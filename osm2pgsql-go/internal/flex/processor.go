package flex

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/kevinramage/osm2pgsql-go/internal/config"
	"github.com/kevinramage/osm2pgsql-go/internal/logger"
	"github.com/kevinramage/osm2pgsql-go/internal/proj"
	"github.com/kevinramage/osm2pgsql-go/internal/wkb"
)

// Processor handles Lua-based OSM processing
type Processor struct {
	cfg         *config.Config
	runtime     *Runtime
	pool        *pgxpool.Pool
	transformer *proj.Transformer
	encoder     *wkb.Encoder
	mu          sync.Mutex

	// Stats
	nodesProcessed     int64
	waysProcessed      int64
	relationsProcessed int64
	rowsInserted       int64
}

// NewProcessor creates a new Flex processor
func NewProcessor(cfg *config.Config, pool *pgxpool.Pool, luaFile string) (*Processor, error) {
	runtime := NewRuntime(cfg.Projection, cfg.DBSchema)

	if err := runtime.LoadFile(luaFile); err != nil {
		runtime.Close()
		return nil, fmt.Errorf("failed to load Lua file: %w", err)
	}

	transformer, _ := proj.NewTransformer(proj.SRID4326, cfg.Projection)

	return &Processor{
		cfg:         cfg,
		runtime:     runtime,
		pool:        pool,
		transformer: transformer,
		encoder:     wkb.NewEncoderWithSRID(4096, cfg.Projection),
	}, nil
}

// Close releases resources
func (p *Processor) Close() {
	if p.runtime != nil {
		p.runtime.Close()
	}
}

// Tables returns the defined tables
func (p *Processor) Tables() []*Table {
	return p.runtime.Tables().All()
}

// EnsureTables creates the output tables in PostgreSQL
func (p *Processor) EnsureTables(ctx context.Context, dropExisting bool) error {
	log := logger.Get()

	for _, table := range p.runtime.Tables().All() {
		fullName := fmt.Sprintf("%s.%s", table.Schema, table.Name)

		if dropExisting {
			_, err := p.pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", fullName))
			if err != nil {
				return fmt.Errorf("failed to drop table %s: %w", fullName, err)
			}
		}

		// Build CREATE TABLE statement
		sql := p.buildCreateTableSQL(table)
		log.Debug("Creating table", zap.String("table", fullName), zap.String("sql", sql))

		_, err := p.pool.Exec(ctx, sql)
		if err != nil {
			return fmt.Errorf("failed to create table %s: %w", fullName, err)
		}

		log.Info("Created table", zap.String("table", fullName))
	}

	return nil
}

// buildCreateTableSQL generates CREATE TABLE SQL for a table definition
func (p *Processor) buildCreateTableSQL(table *Table) string {
	fullName := fmt.Sprintf("%s.%s", table.Schema, table.Name)

	sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n", fullName)

	var columns []string
	for _, col := range table.Columns {
		colDef := fmt.Sprintf("  %s %s", col.Name, col.Type.String())

		// Add SRID for geometry columns
		if col.Type >= ColumnTypePoint && col.Type <= ColumnTypeGeometryCollection {
			srid := col.SRID
			if srid == 0 {
				srid = table.SRID
			}
			if srid != 0 {
				// Replace GEOMETRY(Type) with GEOMETRY(Type, SRID)
				typeName := col.Type.String()
				if len(typeName) > 10 && typeName[:8] == "GEOMETRY" {
					// Extract the geometry type from GEOMETRY(Type)
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

// joinStrings joins strings with a separator
func joinStrings(strs []string, sep string) string {
	if len(strs) == 0 {
		return ""
	}
	result := strs[0]
	for _, s := range strs[1:] {
		result += sep + s
	}
	return result
}

// CreateIndexes creates indexes on the tables
func (p *Processor) CreateIndexes(ctx context.Context) error {
	log := logger.Get()

	for _, table := range p.runtime.Tables().All() {
		fullName := fmt.Sprintf("%s.%s", table.Schema, table.Name)

		// Create geometry index if table has geometry column
		if table.GeomColumn != "" {
			indexName := fmt.Sprintf("%s_%s_idx", table.Name, table.GeomColumn)
			sql := fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s USING GIST (%s)",
				indexName, fullName, table.GeomColumn)

			log.Debug("Creating index", zap.String("index", indexName))
			_, err := p.pool.Exec(ctx, sql)
			if err != nil {
				return fmt.Errorf("failed to create index %s: %w", indexName, err)
			}
		}

		// Create other indexes
		for _, colName := range table.Indexes {
			indexName := fmt.Sprintf("%s_%s_idx", table.Name, colName)
			sql := fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (%s)",
				indexName, fullName, colName)

			log.Debug("Creating index", zap.String("index", indexName))
			_, err := p.pool.Exec(ctx, sql)
			if err != nil {
				return fmt.Errorf("failed to create index %s: %w", indexName, err)
			}
		}

		// Cluster if specified
		if table.ClusterOn != "" {
			indexName := fmt.Sprintf("%s_%s_idx", table.Name, table.ClusterOn)
			sql := fmt.Sprintf("CLUSTER %s USING %s", fullName, indexName)
			log.Debug("Clustering table", zap.String("table", fullName), zap.String("index", indexName))
			_, err := p.pool.Exec(ctx, sql)
			if err != nil {
				log.Warn("Failed to cluster table", zap.String("table", fullName), zap.Error(err))
			}
		}
	}

	return nil
}

// ProcessNode processes a node through the Lua runtime
func (p *Processor) ProcessNode(ctx context.Context, obj *OSMObject) error {
	if !p.runtime.HasProcessNode() {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.runtime.ProcessNode(obj); err != nil {
		return err
	}

	p.nodesProcessed++

	// Collect and insert rows
	return p.insertRows(ctx, obj)
}

// ProcessWay processes a way through the Lua runtime
func (p *Processor) ProcessWay(ctx context.Context, obj *OSMObject) error {
	if !p.runtime.HasProcessWay() {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.runtime.ProcessWay(obj); err != nil {
		return err
	}

	p.waysProcessed++

	// Collect and insert rows
	return p.insertRows(ctx, obj)
}

// ProcessRelation processes a relation through the Lua runtime
func (p *Processor) ProcessRelation(ctx context.Context, obj *OSMObject) error {
	if !p.runtime.HasProcessRelation() {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.runtime.ProcessRelation(obj); err != nil {
		return err
	}

	p.relationsProcessed++

	// Collect and insert rows
	return p.insertRows(ctx, obj)
}

// insertRows inserts pending rows from Lua processing
func (p *Processor) insertRows(ctx context.Context, obj *OSMObject) error {
	rows := p.runtime.CollectRows()
	if len(rows) == 0 {
		return nil
	}

	for _, row := range rows {
		table := p.runtime.Tables().Get(row.TableName)
		if table == nil {
			continue
		}

		// Build geometry if needed
		if row.GeomWKB == nil && table.GeomColumn != "" {
			row.GeomWKB = p.buildGeometry(obj)
		}

		if err := p.insertRow(ctx, table, row, obj); err != nil {
			return err
		}
		p.rowsInserted++
	}

	return nil
}

// buildGeometry builds WKB geometry from the object
func (p *Processor) buildGeometry(obj *OSMObject) []byte {
	switch obj.Type {
	case "node":
		x, y := p.transformer.Transform(obj.Lon, obj.Lat)
		return p.encoder.EncodePoint(x, y)

	case "way":
		if len(obj.Coords) < 4 {
			return nil
		}
		// Transform coordinates
		coords := make([]float64, len(obj.Coords))
		copy(coords, obj.Coords)
		p.transformer.TransformCoords(coords)

		if obj.IsClosed && len(coords) >= 8 {
			return p.encoder.EncodePolygon(coords)
		}
		return p.encoder.EncodeLineString(coords)

	case "relation":
		// For relations, we need multipolygon assembly
		// This would require access to way geometries
		// Return nil for now - full implementation would need way cache
		return nil
	}

	return nil
}

// insertRow inserts a single row into the database
func (p *Processor) insertRow(ctx context.Context, table *Table, row Row, obj *OSMObject) error {
	fullName := fmt.Sprintf("%s.%s", table.Schema, table.Name)

	// Build column list and values
	var columns []string
	var placeholders []string
	var values []interface{}
	paramIdx := 1

	for _, col := range table.Columns {
		if col.CreateOnly {
			continue
		}

		columns = append(columns, col.Name)
		placeholders = append(placeholders, fmt.Sprintf("$%d", paramIdx))

		// Get value
		var value interface{}
		if col.Name == "osm_id" {
			value = obj.ID
		} else if col.Name == "osm_type" {
			value = obj.Type
		} else if col.Name == table.GeomColumn {
			value = row.GeomWKB
		} else if v, ok := row.Values[col.Name]; ok {
			value = v
		} else {
			value = nil
		}

		values = append(values, value)
		paramIdx++
	}

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		fullName,
		joinStrings(columns, ", "),
		joinStrings(placeholders, ", "))

	_, err := p.pool.Exec(ctx, sql, values...)
	return err
}

// Stats returns processing statistics
func (p *Processor) Stats() ProcessorStats {
	return ProcessorStats{
		NodesProcessed:     p.nodesProcessed,
		WaysProcessed:      p.waysProcessed,
		RelationsProcessed: p.relationsProcessed,
		RowsInserted:       p.rowsInserted,
	}
}

// ProcessorStats holds processor statistics
type ProcessorStats struct {
	NodesProcessed     int64
	WaysProcessed      int64
	RelationsProcessed int64
	RowsInserted       int64
}

// HasProcessNode returns true if process_node is defined
func (p *Processor) HasProcessNode() bool {
	return p.runtime.HasProcessNode()
}

// HasProcessWay returns true if process_way is defined
func (p *Processor) HasProcessWay() bool {
	return p.runtime.HasProcessWay()
}

// HasProcessRelation returns true if process_relation is defined
func (p *Processor) HasProcessRelation() bool {
	return p.runtime.HasProcessRelation()
}
