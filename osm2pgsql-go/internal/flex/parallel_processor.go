package flex

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/kevinramage/osm2pgsql-go/internal/config"
	"github.com/kevinramage/osm2pgsql-go/internal/logger"
	"github.com/kevinramage/osm2pgsql-go/internal/proj"
	"github.com/kevinramage/osm2pgsql-go/internal/wkb"
)

// ParallelProcessor handles Lua-based OSM processing with parallel workers
type ParallelProcessor struct {
	cfg           *config.Config
	pool          *pgxpool.Pool
	luaFile       string
	numWorkers    int
	batchSize     int
	tables        *TableRegistry

	// Workers
	workers    []*luaWorker
	inputChan  chan *OSMObject
	resultChan chan workerResult
	errorChan  chan error

	// Batch buffers per table
	batchBuffers map[string]*batchBuffer
	bufferMu     sync.Mutex

	// Stats
	nodesProcessed     atomic.Int64
	waysProcessed      atomic.Int64
	relationsProcessed atomic.Int64
	rowsInserted       atomic.Int64
	batchesInserted    atomic.Int64
}

// luaWorker is a single Lua processing worker
type luaWorker struct {
	id          int
	runtime     *Runtime
	transformer *proj.Transformer
	encoder     *wkb.Encoder
}

// workerResult holds the result from a worker
type workerResult struct {
	objType string
	rows    []Row
	obj     *OSMObject
	err     error
}

// batchBuffer holds rows to be inserted in a batch
type batchBuffer struct {
	table   *Table
	rows    [][]interface{}
	columns []string
	mu      sync.Mutex
}

// NewParallelProcessor creates a new parallel Flex processor
func NewParallelProcessor(cfg *config.Config, pool *pgxpool.Pool, luaFile string, numWorkers int) (*ParallelProcessor, error) {
	if numWorkers <= 0 {
		numWorkers = cfg.Workers
	}
	if numWorkers <= 0 {
		numWorkers = 4
	}

	// Load Lua file once to get table definitions
	masterRuntime := NewRuntime(cfg.Projection, cfg.DBSchema)
	if err := masterRuntime.LoadFile(luaFile); err != nil {
		masterRuntime.Close()
		return nil, fmt.Errorf("failed to load Lua file: %w", err)
	}
	tables := masterRuntime.Tables()
	masterRuntime.Close()

	pp := &ParallelProcessor{
		cfg:          cfg,
		pool:         pool,
		luaFile:      luaFile,
		numWorkers:   numWorkers,
		batchSize:    10000, // Configurable batch size
		tables:       tables,
		inputChan:    make(chan *OSMObject, numWorkers*100),
		resultChan:   make(chan workerResult, numWorkers*100),
		errorChan:    make(chan error, numWorkers),
		batchBuffers: make(map[string]*batchBuffer),
	}

	// Initialize batch buffers for each table
	for _, table := range tables.All() {
		columns := make([]string, 0, len(table.Columns))
		for _, col := range table.Columns {
			if !col.CreateOnly {
				columns = append(columns, col.Name)
			}
		}
		pp.batchBuffers[table.Name] = &batchBuffer{
			table:   table,
			rows:    make([][]interface{}, 0, pp.batchSize),
			columns: columns,
		}
	}

	return pp, nil
}

// Start initializes workers and begins processing
func (pp *ParallelProcessor) Start(ctx context.Context) error {
	log := logger.Get()

	// Create workers
	pp.workers = make([]*luaWorker, pp.numWorkers)
	for i := 0; i < pp.numWorkers; i++ {
		runtime := NewRuntime(pp.cfg.Projection, pp.cfg.DBSchema)
		if err := runtime.LoadFile(pp.luaFile); err != nil {
			// Clean up already created workers
			for j := 0; j < i; j++ {
				pp.workers[j].runtime.Close()
			}
			return fmt.Errorf("failed to create worker %d: %w", i, err)
		}

		transformer, _ := proj.NewTransformer(proj.SRID4326, pp.cfg.Projection)
		pp.workers[i] = &luaWorker{
			id:          i,
			runtime:     runtime,
			transformer: transformer,
			encoder:     wkb.NewEncoderWithSRID(4096, pp.cfg.Projection),
		}
	}

	log.Info("Started parallel Lua workers", zap.Int("workers", pp.numWorkers))

	// Start worker goroutines
	var wg sync.WaitGroup
	for _, worker := range pp.workers {
		wg.Add(1)
		go func(w *luaWorker) {
			defer wg.Done()
			pp.workerLoop(ctx, w)
		}(worker)
	}

	// Start result collector
	go pp.collectResults(ctx)

	// Start periodic batch flusher
	go pp.periodicFlush(ctx)

	// Wait for all workers to complete in a separate goroutine
	go func() {
		wg.Wait()
		close(pp.resultChan)
	}()

	return nil
}

// workerLoop is the main processing loop for a worker
func (pp *ParallelProcessor) workerLoop(ctx context.Context, w *luaWorker) {
	for {
		select {
		case <-ctx.Done():
			return
		case obj, ok := <-pp.inputChan:
			if !ok {
				return
			}

			result := workerResult{
				objType: obj.Type,
				obj:     obj,
			}

			// Process through Lua
			var err error
			switch obj.Type {
			case "node":
				if w.runtime.HasProcessNode() {
					err = w.runtime.ProcessNode(obj)
				}
			case "way":
				if w.runtime.HasProcessWay() {
					err = w.runtime.ProcessWay(obj)
				}
			case "relation":
				if w.runtime.HasProcessRelation() {
					err = w.runtime.ProcessRelation(obj)
				}
			}

			if err != nil {
				result.err = err
			} else {
				// Collect rows and build geometries
				rows := w.runtime.CollectRows()
				for i := range rows {
					if rows[i].GeomWKB == nil {
						rows[i].GeomWKB = pp.buildGeometry(w, obj)
					}
				}
				result.rows = rows
			}

			select {
			case pp.resultChan <- result:
			case <-ctx.Done():
				return
			}
		}
	}
}

// buildGeometry builds WKB geometry from the object using worker's encoder
func (pp *ParallelProcessor) buildGeometry(w *luaWorker, obj *OSMObject) []byte {
	switch obj.Type {
	case "node":
		x, y := w.transformer.Transform(obj.Lon, obj.Lat)
		return w.encoder.EncodePoint(x, y)

	case "way":
		if len(obj.Coords) < 4 {
			return nil
		}
		coords := make([]float64, len(obj.Coords))
		copy(coords, obj.Coords)
		w.transformer.TransformCoords(coords)

		if obj.IsClosed && len(coords) >= 8 {
			return w.encoder.EncodePolygon(coords)
		}
		return w.encoder.EncodeLineString(coords)

	case "relation":
		if len(obj.Coords) < 8 {
			return nil
		}
		coords := make([]float64, len(obj.Coords))
		copy(coords, obj.Coords)
		w.transformer.TransformCoords(coords)
		return w.encoder.EncodePolygon(coords)
	}

	return nil
}

// collectResults collects results from workers and batches them
func (pp *ParallelProcessor) collectResults(ctx context.Context) {
	log := logger.Get()

	for {
		select {
		case <-ctx.Done():
			return
		case result, ok := <-pp.resultChan:
			if !ok {
				return
			}

			if result.err != nil {
				log.Warn("Worker error", zap.Error(result.err))
				continue
			}

			// Update stats
			switch result.objType {
			case "node":
				pp.nodesProcessed.Add(1)
			case "way":
				pp.waysProcessed.Add(1)
			case "relation":
				pp.relationsProcessed.Add(1)
			}

			// Add rows to batch buffers
			for _, row := range result.rows {
				pp.addToBatch(ctx, row, result.obj)
			}
		}
	}
}

// addToBatch adds a row to the appropriate batch buffer
func (pp *ParallelProcessor) addToBatch(ctx context.Context, row Row, obj *OSMObject) {
	buffer, ok := pp.batchBuffers[row.TableName]
	if !ok {
		return
	}

	buffer.mu.Lock()
	defer buffer.mu.Unlock()

	// Build row values
	values := make([]interface{}, len(buffer.columns))
	for i, colName := range buffer.columns {
		if colName == "osm_id" {
			values[i] = obj.ID
		} else if colName == "osm_type" {
			values[i] = obj.Type
		} else if colName == buffer.table.GeomColumn {
			values[i] = row.GeomWKB
		} else if v, exists := row.Values[colName]; exists {
			values[i] = v
		} else {
			values[i] = nil
		}
	}

	buffer.rows = append(buffer.rows, values)

	// Flush if batch is full
	if len(buffer.rows) >= pp.batchSize {
		pp.flushBuffer(ctx, buffer)
	}
}

// flushBuffer flushes a batch buffer to the database
func (pp *ParallelProcessor) flushBuffer(ctx context.Context, buffer *batchBuffer) {
	if len(buffer.rows) == 0 {
		return
	}

	rows := buffer.rows
	buffer.rows = make([][]interface{}, 0, pp.batchSize)

	// Use COPY for bulk insert
	fullName := fmt.Sprintf("%s.%s", buffer.table.Schema, buffer.table.Name)

	copyCount, err := pp.pool.CopyFrom(
		ctx,
		pgx.Identifier{buffer.table.Schema, buffer.table.Name},
		buffer.columns,
		pgx.CopyFromRows(rows),
	)

	if err != nil {
		log := logger.Get()
		log.Error("Batch insert failed",
			zap.String("table", fullName),
			zap.Int("rows", len(rows)),
			zap.Error(err))
		return
	}

	pp.rowsInserted.Add(copyCount)
	pp.batchesInserted.Add(1)
}

// periodicFlush periodically flushes all buffers
func (pp *ParallelProcessor) periodicFlush(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pp.FlushAll(ctx)
		}
	}
}

// FlushAll flushes all batch buffers
func (pp *ParallelProcessor) FlushAll(ctx context.Context) {
	for _, buffer := range pp.batchBuffers {
		buffer.mu.Lock()
		pp.flushBuffer(ctx, buffer)
		buffer.mu.Unlock()
	}
}

// Submit submits an object for processing
func (pp *ParallelProcessor) Submit(obj *OSMObject) {
	pp.inputChan <- obj
}

// Close shuts down the processor and flushes remaining data
func (pp *ParallelProcessor) Close(ctx context.Context) {
	// Close input channel to signal workers to stop
	close(pp.inputChan)

	// Wait a bit for workers to finish
	time.Sleep(100 * time.Millisecond)

	// Flush remaining batches
	pp.FlushAll(ctx)

	// Close workers
	for _, w := range pp.workers {
		if w != nil && w.runtime != nil {
			w.runtime.Close()
		}
	}
}

// Tables returns the defined tables
func (pp *ParallelProcessor) Tables() []*Table {
	return pp.tables.All()
}

// EnsureTables creates the output tables in PostgreSQL
func (pp *ParallelProcessor) EnsureTables(ctx context.Context, dropExisting bool) error {
	log := logger.Get()

	for _, table := range pp.tables.All() {
		fullName := fmt.Sprintf("%s.%s", table.Schema, table.Name)

		if dropExisting {
			_, err := pp.pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", fullName))
			if err != nil {
				return fmt.Errorf("failed to drop table %s: %w", fullName, err)
			}
		}

		sql := pp.buildCreateTableSQL(table)
		log.Debug("Creating table", zap.String("table", fullName))

		_, err := pp.pool.Exec(ctx, sql)
		if err != nil {
			return fmt.Errorf("failed to create table %s: %w", fullName, err)
		}

		log.Info("Created table", zap.String("table", fullName))
	}

	return nil
}

// buildCreateTableSQL generates CREATE TABLE SQL
func (pp *ParallelProcessor) buildCreateTableSQL(table *Table) string {
	fullName := fmt.Sprintf("%s.%s", table.Schema, table.Name)
	sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n", fullName)

	var columns []string
	for _, col := range table.Columns {
		colDef := fmt.Sprintf("  %s %s", col.Name, col.Type.String())

		if col.Type >= ColumnTypePoint && col.Type <= ColumnTypeGeometryCollection {
			srid := col.SRID
			if srid == 0 {
				srid = table.SRID
			}
			if srid != 0 {
				typeName := col.Type.String()
				if len(typeName) > 10 && typeName[:8] == "GEOMETRY" {
					geomType := typeName[9 : len(typeName)-1]
					colDef = fmt.Sprintf("  %s GEOMETRY(%s, %d)", col.Name, geomType, srid)
				}
			}
		}

		if col.NotNull {
			colDef += " NOT NULL"
		}
		columns = append(columns, colDef)
	}

	sql += joinStrings(columns, ",\n")
	sql += "\n)"

	return sql
}

// CreateIndexes creates indexes on the tables
func (pp *ParallelProcessor) CreateIndexes(ctx context.Context) error {
	log := logger.Get()

	for _, table := range pp.tables.All() {
		fullName := fmt.Sprintf("%s.%s", table.Schema, table.Name)

		if table.GeomColumn != "" {
			indexName := fmt.Sprintf("%s_%s_idx", table.Name, table.GeomColumn)
			sql := fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s USING GIST (%s)",
				indexName, fullName, table.GeomColumn)

			log.Debug("Creating index", zap.String("index", indexName))
			_, err := pp.pool.Exec(ctx, sql)
			if err != nil {
				return fmt.Errorf("failed to create index %s: %w", indexName, err)
			}
		}

		for _, colName := range table.Indexes {
			indexName := fmt.Sprintf("%s_%s_idx", table.Name, colName)
			sql := fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (%s)",
				indexName, fullName, colName)

			log.Debug("Creating index", zap.String("index", indexName))
			_, err := pp.pool.Exec(ctx, sql)
			if err != nil {
				return fmt.Errorf("failed to create index %s: %w", indexName, err)
			}
		}

		if table.ClusterOn != "" {
			indexName := fmt.Sprintf("%s_%s_idx", table.Name, table.ClusterOn)
			sql := fmt.Sprintf("CLUSTER %s USING %s", fullName, indexName)
			log.Debug("Clustering table", zap.String("table", fullName))
			_, _ = pp.pool.Exec(ctx, sql)
		}
	}

	return nil
}

// Stats returns processing statistics
func (pp *ParallelProcessor) Stats() ProcessorStats {
	return ProcessorStats{
		NodesProcessed:     pp.nodesProcessed.Load(),
		WaysProcessed:      pp.waysProcessed.Load(),
		RelationsProcessed: pp.relationsProcessed.Load(),
		RowsInserted:       pp.rowsInserted.Load(),
	}
}

// HasProcessNode returns true if process_node is defined
func (pp *ParallelProcessor) HasProcessNode() bool {
	if len(pp.workers) > 0 && pp.workers[0] != nil {
		return pp.workers[0].runtime.HasProcessNode()
	}
	return false
}

// HasProcessWay returns true if process_way is defined
func (pp *ParallelProcessor) HasProcessWay() bool {
	if len(pp.workers) > 0 && pp.workers[0] != nil {
		return pp.workers[0].runtime.HasProcessWay()
	}
	return false
}

// HasProcessRelation returns true if process_relation is defined
func (pp *ParallelProcessor) HasProcessRelation() bool {
	if len(pp.workers) > 0 && pp.workers[0] != nil {
		return pp.workers[0].runtime.HasProcessRelation()
	}
	return false
}
