package pipeline

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/kevinramage/osm2pgsql-go/internal/config"
	"github.com/kevinramage/osm2pgsql-go/internal/logger"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// StreamingLoader loads geometries from channels into PostgreSQL
type StreamingLoader struct {
	cfg  *config.Config
	pool *pgxpool.Pool
}

// NewStreamingLoader creates a new streaming PostgreSQL loader
func NewStreamingLoader(cfg *config.Config) (*StreamingLoader, error) {
	poolConfig, err := pgxpool.ParseConfig(cfg.ConnectionString())
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	// Need at least 4 connections: 3 loaders + 1 for setup/indexes
	minConns := cfg.Workers
	if minConns < 4 {
		minConns = 4
	}
	poolConfig.MaxConns = int32(minConns)

	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	return &StreamingLoader{
		cfg:  cfg,
		pool: pool,
	}, nil
}

// Close closes all database connections
func (l *StreamingLoader) Close() error {
	l.pool.Close()
	return nil
}

// EnsureSchema creates the PostGIS extension and schema if needed
func (l *StreamingLoader) EnsureSchema(ctx context.Context) error {
	if _, err := l.pool.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS postgis"); err != nil {
		return fmt.Errorf("failed to create PostGIS extension: %w", err)
	}

	if l.cfg.DBSchema != "public" {
		if _, err := l.pool.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", l.cfg.DBSchema)); err != nil {
			return fmt.Errorf("failed to create schema: %w", err)
		}
	}

	return nil
}

// PrepareTable creates or truncates a table for loading
func (l *StreamingLoader) PrepareTable(ctx context.Context, tableName string, dropExisting bool) error {
	conn, err := l.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	fullTableName := fmt.Sprintf("%s.%s", l.cfg.DBSchema, tableName)

	if dropExisting {
		if _, err := conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", fullTableName)); err != nil {
			return fmt.Errorf("failed to drop table: %w", err)
		}
	}

	createSQL := fmt.Sprintf(`
		CREATE UNLOGGED TABLE IF NOT EXISTS %s (
			osm_id BIGINT NOT NULL,
			osm_type CHAR(1) NOT NULL,
			tags JSONB,
			geom GEOMETRY(Geometry, 4326)
		)
	`, fullTableName)

	if _, err := conn.Exec(ctx, createSQL); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	if !dropExisting {
		if _, err := conn.Exec(ctx, fmt.Sprintf("TRUNCATE %s", fullTableName)); err != nil {
			// Ignore error - table might be new
		}
	}

	return nil
}

// LoadStream consumes geometry records from a channel and loads them into PostgreSQL
func (l *StreamingLoader) LoadStream(ctx context.Context, tableName string, records <-chan GeometryRecord) (int64, error) {
	log := logger.Get()
	fullTableName := fmt.Sprintf("%s.%s", l.cfg.DBSchema, tableName)

	conn, err := l.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	log.Info("Starting stream load", zap.String("table", tableName))

	// Use COPY protocol for bulk loading directly to final table
	// PostGIS accepts raw EWKB bytes for geometry columns
	count, err := l.copyFromChannel(ctx, conn.Conn(), l.cfg.DBSchema, tableName, records)
	if err != nil {
		return 0, err
	}

	// Convert to logged table
	if _, err := conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s SET LOGGED", fullTableName)); err != nil {
		// Ignore error if it fails
	}

	log.Info("Stream load complete", zap.String("table", tableName), zap.Int64("rows", count))
	return count, nil
}

// copyFromChannel uses PostgreSQL COPY to bulk load directly from a channel
// PostGIS accepts raw EWKB bytes directly into geometry columns via COPY
func (l *StreamingLoader) copyFromChannel(ctx context.Context, conn *pgx.Conn, schema, tableName string, records <-chan GeometryRecord) (int64, error) {
	// Create channel-based row source
	rowChan := make(chan []interface{}, 10000)

	// Goroutine to convert GeometryRecords to row slices
	// Note: Tags are JSON strings, GeomWKB is raw EWKB bytes that PostGIS accepts directly
	go func() {
		defer close(rowChan)
		for record := range records {
			select {
			case rowChan <- []interface{}{record.OsmID, record.OsmType, record.Tags, record.GeomWKB}:
			case <-ctx.Done():
				return
			}
		}
	}()

	// COPY directly to final table - PostGIS accepts EWKB bytes for geometry columns
	copyCount, err := conn.CopyFrom(
		ctx,
		pgx.Identifier{schema, tableName},
		[]string{"osm_id", "osm_type", "tags", "geom"},
		&rowSource{rows: rowChan},
	)
	if err != nil {
		return 0, fmt.Errorf("COPY failed: %w", err)
	}

	return copyCount, nil
}

// CreateIndexes creates spatial and ID indexes on a table
func (l *StreamingLoader) CreateIndexes(ctx context.Context, tableName string) error {
	log := logger.Get()
	fullTableName := fmt.Sprintf("%s.%s", l.cfg.DBSchema, tableName)

	conn, err := l.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	// Set high maintenance_work_mem for this session
	if _, err := conn.Exec(ctx, "SET maintenance_work_mem = '2GB'"); err != nil {
		// Ignore error
	}

	log.Info("Creating indexes", zap.String("table", tableName))

	// Create GIST index
	gistIdx := fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s_geom_idx ON %s USING GIST (geom)",
		tableName, fullTableName)
	if _, err := conn.Exec(ctx, gistIdx); err != nil {
		return fmt.Errorf("failed to create GIST index: %w", err)
	}

	// Create B-tree index on osm_id
	btreeIdx := fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s_osm_id_idx ON %s (osm_id)",
		tableName, fullTableName)
	if _, err := conn.Exec(ctx, btreeIdx); err != nil {
		return fmt.Errorf("failed to create B-tree index: %w", err)
	}

	// Analyze table for query planner
	if _, err := conn.Exec(ctx, fmt.Sprintf("ANALYZE %s", fullTableName)); err != nil {
		return fmt.Errorf("failed to analyze table: %w", err)
	}

	log.Info("Indexes created", zap.String("table", tableName))
	return nil
}

// rowSource implements pgx.CopyFromSource for streaming rows from a channel
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
