package loader

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/parquet/file"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"
	"go.uber.org/zap"

	"github.com/kevinramage/osm2pgsql-go/internal/config"
	"github.com/kevinramage/osm2pgsql-go/internal/logger"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Stats holds loader statistics
type Stats struct {
	RowsLoaded int64
}

// Loader loads Parquet files into PostgreSQL
type Loader struct {
	cfg           *config.Config
	pool          *pgxpool.Pool
	dropExisting  bool
	createIndexes bool
}

// NewLoader creates a new PostgreSQL loader
func NewLoader(cfg *config.Config, dropExisting, createIndexes bool) (*Loader, error) {
	// Connect to PostgreSQL
	poolConfig, err := pgxpool.ParseConfig(cfg.ConnectionString())
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	poolConfig.MaxConns = int32(cfg.Workers)

	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	return &Loader{
		cfg:           cfg,
		pool:          pool,
		dropExisting:  dropExisting,
		createIndexes: createIndexes,
	}, nil
}

// Close closes connections
func (l *Loader) Close() error {
	l.pool.Close()
	return nil
}

// Run executes the load
func (l *Loader) Run() (*Stats, error) {
	ctx := context.Background()
	stats := &Stats{}

	// Ensure PostGIS extension is available
	if _, err := l.pool.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS postgis"); err != nil {
		return nil, fmt.Errorf("failed to create PostGIS extension: %w", err)
	}

	// Create schema if needed
	if l.cfg.DBSchema != "public" {
		if _, err := l.pool.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", l.cfg.DBSchema)); err != nil {
			return nil, fmt.Errorf("failed to create schema: %w", err)
		}
	}

	// Load each geometry type
	tables := []struct {
		name   string
		source string
	}{
		{"planet_osm_point", "points.parquet"},
		{"planet_osm_line", "lines.parquet"},
		{"planet_osm_polygon", "polygons.parquet"},
	}

	log := logger.Get()

	// Phase 1: Load all tables in parallel (without indexes)
	type loadResult struct {
		tableName string
		count     int64
		err       error
	}

	resultChan := make(chan loadResult, len(tables))
	var tablesToIndex []string

	for _, table := range tables {
		sourcePath := filepath.Join(l.cfg.OutputDir, table.source)

		// Check if source file exists
		if _, err := os.Stat(sourcePath); os.IsNotExist(err) {
			log.Debug("Skipping table (no source file)", zap.String("table", table.name))
			continue
		}

		fullTableName := fmt.Sprintf("%s.%s", l.cfg.DBSchema, table.name)
		tablesToIndex = append(tablesToIndex, fullTableName)

		go func(tableName, fullName, source string) {
			log.Info("Loading table", zap.String("table", tableName))
			count, err := l.loadTableData(ctx, fullName, source)
			resultChan <- loadResult{tableName: tableName, count: count, err: err}
		}(table.name, fullTableName, sourcePath)
	}

	// Collect load results
	for i := 0; i < len(tablesToIndex); i++ {
		result := <-resultChan
		if result.err != nil {
			return nil, fmt.Errorf("failed to load %s: %w", result.tableName, result.err)
		}
		stats.RowsLoaded += result.count
		log.Info("Table loaded", zap.String("table", result.tableName), zap.Int64("rows", result.count))
	}

	// Phase 2: Create all indexes in parallel
	if l.createIndexes && len(tablesToIndex) > 0 {
		log.Info("Creating indexes in parallel", zap.Int("tables", len(tablesToIndex)))
		errChan := make(chan error, len(tablesToIndex)*2) // 2 indexes per table

		for _, tableName := range tablesToIndex {
			go func(tbl string) {
				errChan <- l.createTableIndexesParallel(ctx, tbl)
			}(tableName)
		}

		// Wait for all index creations
		for i := 0; i < len(tablesToIndex); i++ {
			if err := <-errChan; err != nil {
				return nil, fmt.Errorf("failed to create indexes: %w", err)
			}
		}
		log.Info("All indexes created")
	}

	return stats, nil
}

// loadTableData loads data without creating indexes (for parallel loading)
func (l *Loader) loadTableData(ctx context.Context, tableName, parquetPath string) (int64, error) {
	conn, err := l.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	// Drop existing table if requested
	if l.dropExisting {
		if _, err := conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", tableName)); err != nil {
			return 0, fmt.Errorf("failed to drop table: %w", err)
		}
	}

	// Create UNLOGGED table for faster loading
	createSQL := fmt.Sprintf(`
		CREATE UNLOGGED TABLE IF NOT EXISTS %s (
			osm_id BIGINT NOT NULL,
			osm_type CHAR(1) NOT NULL,
			tags JSONB,
			geom GEOMETRY(Geometry, 4326)
		)
	`, tableName)

	if _, err := conn.Exec(ctx, createSQL); err != nil {
		return 0, fmt.Errorf("failed to create table: %w", err)
	}

	// Truncate if table existed and we're not dropping
	if !l.dropExisting {
		if _, err := conn.Exec(ctx, fmt.Sprintf("TRUNCATE %s", tableName)); err != nil {
			// Ignore error - table might be new
		}
	}

	// Use COPY for bulk loading
	count, err := l.copyFromParquet(ctx, conn.Conn(), tableName, parquetPath)
	if err != nil {
		return 0, err
	}

	// Convert to logged table
	if _, err := conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s SET LOGGED", tableName)); err != nil {
		// Ignore error if it fails
	}

	return count, nil
}

// createTableIndexesParallel creates indexes using its own connection
func (l *Loader) createTableIndexesParallel(ctx context.Context, tableName string) error {
	conn, err := l.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	// Set high maintenance_work_mem for this session
	if _, err := conn.Exec(ctx, "SET maintenance_work_mem = '2GB'"); err != nil {
		// Ignore error
	}

	// Extract just the table name for index naming
	shortName := tableName
	if l.cfg.DBSchema != "" {
		shortName = tableName[len(l.cfg.DBSchema)+1:]
	}

	// Create GIST index (this is the slow one)
	gistIdx := fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s_geom_idx ON %s USING GIST (geom)",
		shortName, tableName)
	if _, err := conn.Exec(ctx, gistIdx); err != nil {
		return err
	}

	// Create B-tree index on osm_id
	btreeIdx := fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s_osm_id_idx ON %s (osm_id)",
		shortName, tableName)
	if _, err := conn.Exec(ctx, btreeIdx); err != nil {
		return err
	}

	// Analyze table for query planner
	if _, err := conn.Exec(ctx, fmt.Sprintf("ANALYZE %s", tableName)); err != nil {
		return err
	}

	return nil
}

func (l *Loader) copyFromParquet(ctx context.Context, conn *pgx.Conn, tableName, parquetPath string) (int64, error) {
	// Open Parquet file using Arrow
	f, err := os.Open(parquetPath)
	if err != nil {
		return 0, fmt.Errorf("failed to open parquet file: %w", err)
	}
	defer f.Close()

	pf, err := file.NewParquetReader(f)
	if err != nil {
		return 0, fmt.Errorf("failed to create parquet reader: %w", err)
	}
	defer pf.Close()

	// Create Arrow reader
	arrowReader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to create arrow reader: %w", err)
	}

	// Read entire table
	tbl, err := arrowReader.ReadTable(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to read table: %w", err)
	}
	defer tbl.Release()

	if tbl.NumRows() == 0 {
		return 0, nil
	}

	// Check schema to determine if WKB or WKT
	schema := tbl.Schema()
	isWKB := false
	for _, field := range schema.Fields() {
		if field.Name == "geom_wkb" {
			isWKB = true
			break
		}
	}

	// Use a transaction for the COPY
	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Create a temporary table for COPY
	tempTable := "osm_load_tmp"

	var tempTableSQL string
	if isWKB {
		tempTableSQL = fmt.Sprintf(`
			DROP TABLE IF EXISTS %s;
			CREATE TEMP TABLE %s (
				osm_id BIGINT,
				osm_type CHAR(1),
				tags TEXT,
				geom_wkb BYTEA
			) ON COMMIT DROP
		`, tempTable, tempTable)
	} else {
		tempTableSQL = fmt.Sprintf(`
			DROP TABLE IF EXISTS %s;
			CREATE TEMP TABLE %s (
				osm_id BIGINT,
				osm_type CHAR(1),
				tags TEXT,
				geom_wkt TEXT
			) ON COMMIT DROP
		`, tempTable, tempTable)
	}

	if _, err := tx.Exec(ctx, tempTableSQL); err != nil {
		return 0, fmt.Errorf("failed to create temp table: %w", err)
	}

	// Create channel for streaming rows
	rowChan := make(chan []interface{}, 10000)
	var count atomic.Int64

	// Start goroutine to read from table and send to channel
	go func() {
		defer close(rowChan)

		// Get columns as chunked arrays
		osmIDCol := tbl.Column(0).Data()
		osmTypeCol := tbl.Column(1).Data()
		tagsCol := tbl.Column(2).Data()
		geomCol := tbl.Column(3).Data()

		// Iterate through all chunks
		numChunks := len(osmIDCol.Chunks())
		for chunkIdx := 0; chunkIdx < numChunks; chunkIdx++ {
			idChunk := osmIDCol.Chunk(chunkIdx).(*array.Int64)
			typeChunk := osmTypeCol.Chunk(chunkIdx).(*array.String)
			tagsChunk := tagsCol.Chunk(chunkIdx).(*array.String)

			chunkLen := idChunk.Len()
			for i := 0; i < chunkLen; i++ {
				osmID := idChunk.Value(i)
				osmType := typeChunk.Value(i)
				tags := tagsChunk.Value(i)

				var geomData interface{}
				if isWKB {
					geomData = geomCol.Chunk(chunkIdx).(*array.Binary).Value(i)
				} else {
					geomData = geomCol.Chunk(chunkIdx).(*array.String).Value(i)
				}

				rowChan <- []interface{}{osmID, osmType, tags, geomData}
				count.Add(1)
			}
		}
	}()

	// COPY data into temp table
	var copyColumns []string
	if isWKB {
		copyColumns = []string{"osm_id", "osm_type", "tags", "geom_wkb"}
	} else {
		copyColumns = []string{"osm_id", "osm_type", "tags", "geom_wkt"}
	}

	copyCount, err := tx.CopyFrom(
		ctx,
		pgx.Identifier{tempTable},
		copyColumns,
		&rowSource{rows: rowChan},
	)
	if err != nil {
		return 0, fmt.Errorf("COPY failed: %w", err)
	}

	// Insert from temp table to final table with geometry conversion
	var insertSQL string
	if isWKB {
		// WKB already includes SRID (EWKB format)
		insertSQL = fmt.Sprintf(`
			INSERT INTO %s (osm_id, osm_type, tags, geom)
			SELECT
				osm_id,
				osm_type,
				tags::jsonb,
				ST_GeomFromWKB(geom_wkb)
			FROM %s
			WHERE geom_wkb IS NOT NULL
		`, tableName, tempTable)
	} else {
		insertSQL = fmt.Sprintf(`
			INSERT INTO %s (osm_id, osm_type, tags, geom)
			SELECT
				osm_id,
				osm_type,
				tags::jsonb,
				ST_GeomFromText(geom_wkt, 4326)
			FROM %s
			WHERE geom_wkt IS NOT NULL
		`, tableName, tempTable)
	}

	if _, err := tx.Exec(ctx, insertSQL); err != nil {
		return 0, fmt.Errorf("failed to insert from temp table: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("failed to commit: %w", err)
	}

	return copyCount, nil
}

// rowSource implements pgx.CopyFromSource for streaming rows
type rowSource struct {
	rows    <-chan []interface{}
	current []interface{}
}

func (r *rowSource) Next() bool {
	row, ok := <-r.rows
	if !ok {
		return false
	}
	r.current = row
	return true
}

func (r *rowSource) Values() ([]interface{}, error) {
	return r.current, nil
}

func (r *rowSource) Err() error {
	return nil
}
