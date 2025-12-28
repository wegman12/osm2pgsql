package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kevinramage/osm2pgsql-go/internal/config"
	"github.com/kevinramage/osm2pgsql-go/internal/logger"
)

// LiveLoadStats tracks real-time loading statistics
type LiveLoadStats struct {
	PointsLoaded   atomic.Int64
	LinesLoaded    atomic.Int64
	PolygonsLoaded atomic.Int64
	StartTime      time.Time
}

// GetStats returns current load statistics
func (s *LiveLoadStats) GetStats() (points, lines, polygons int64) {
	return s.PointsLoaded.Load(), s.LinesLoaded.Load(), s.PolygonsLoaded.Load()
}

// GetRates returns rows per second for each table
func (s *LiveLoadStats) GetRates() (pointsRate, linesRate, polygonsRate float64) {
	elapsed := time.Since(s.StartTime).Seconds()
	if elapsed < 0.1 {
		return 0, 0, 0
	}
	return float64(s.PointsLoaded.Load()) / elapsed,
		float64(s.LinesLoaded.Load()) / elapsed,
		float64(s.PolygonsLoaded.Load()) / elapsed
}

// StreamingLoader loads geometries from channels into PostgreSQL
type StreamingLoader struct {
	cfg       *config.Config
	pool      *pgxpool.Pool
	liveStats *LiveLoadStats
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

	// If hstore is enabled, register hstore type after each connection is established
	if cfg.Hstore {
		poolConfig.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
			// Query hstore type OID and register it
			var oid uint32
			err := conn.QueryRow(ctx, "SELECT oid FROM pg_type WHERE typname = 'hstore'").Scan(&oid)
			if err != nil {
				return fmt.Errorf("failed to get hstore OID: %w", err)
			}
			conn.TypeMap().RegisterType(&pgtype.Type{
				Name:  "hstore",
				OID:   oid,
				Codec: pgtype.HstoreCodec{},
			})
			return nil
		}
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	return &StreamingLoader{
		cfg:       cfg,
		pool:      pool,
		liveStats: &LiveLoadStats{StartTime: time.Now()},
	}, nil
}

// LiveStats returns the live loading statistics
func (l *StreamingLoader) LiveStats() *LiveLoadStats {
	return l.liveStats
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

	// Create hstore extension if hstore mode is enabled
	if l.cfg.Hstore {
		if _, err := l.pool.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS hstore"); err != nil {
			return fmt.Errorf("failed to create hstore extension: %w", err)
		}
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

	// Use configured projection SRID for geometry column
	srid := l.cfg.Projection
	if srid == 0 {
		srid = 4326 // Default to WGS84
	}

	// Determine tags column type (hstore or JSONB)
	tagsType := "JSONB"
	if l.cfg.Hstore {
		tagsType = "hstore"
	}

	// Build tablespace clause if specified
	tablespaceClause := ""
	if l.cfg.TablespaceMain != "" {
		tablespaceClause = fmt.Sprintf(" TABLESPACE %s", l.cfg.TablespaceMain)
	}

	// Build CREATE TABLE statement with optional extra attribute columns
	var createSQL string
	if l.cfg.ExtraAttributes {
		createSQL = fmt.Sprintf(`
			CREATE UNLOGGED TABLE IF NOT EXISTS %s (
				osm_id BIGINT NOT NULL,
				osm_type CHAR(1) NOT NULL,
				tags %s,
				geom GEOMETRY(Geometry, %d),
				osm_version INTEGER,
				osm_changeset BIGINT,
				osm_timestamp TIMESTAMPTZ,
				osm_user TEXT,
				osm_uid INTEGER
			)%s
		`, fullTableName, tagsType, srid, tablespaceClause)
	} else {
		createSQL = fmt.Sprintf(`
			CREATE UNLOGGED TABLE IF NOT EXISTS %s (
				osm_id BIGINT NOT NULL,
				osm_type CHAR(1) NOT NULL,
				tags %s,
				geom GEOMETRY(Geometry, %d)
			)%s
		`, fullTableName, tagsType, srid, tablespaceClause)
	}

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

	// Determine which counter to update based on table name
	var counter *atomic.Int64
	switch tableName {
	case "planet_osm_point":
		counter = &l.liveStats.PointsLoaded
	case "planet_osm_line":
		counter = &l.liveStats.LinesLoaded
	case "planet_osm_polygon":
		counter = &l.liveStats.PolygonsLoaded
	}

	// Use COPY protocol for bulk loading directly to final table
	// PostGIS accepts raw EWKB bytes for geometry columns
	count, err := l.copyFromChannelWithStats(ctx, conn.Conn(), l.cfg.DBSchema, tableName, records, counter, l.cfg.ExtraAttributes, l.cfg.Hstore)
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

// copyFromChannelWithStats uses PostgreSQL COPY to bulk load directly from a channel
// PostGIS accepts raw EWKB bytes directly into geometry columns via COPY
// Optionally updates a counter for live statistics
func (l *StreamingLoader) copyFromChannelWithStats(ctx context.Context, conn *pgx.Conn, schema, tableName string, records <-chan GeometryRecord, counter *atomic.Int64, extraAttributes bool, useHstore bool) (int64, error) {
	// Create channel-based row source
	rowChan := make(chan []interface{}, 10000)

	// Goroutine to convert GeometryRecords to row slices
	// Note: Tags are JSON strings, GeomWKB is raw EWKB bytes that PostGIS accepts directly
	go func() {
		defer close(rowChan)
		for record := range records {
			// Convert tags to appropriate format
			var tags interface{}
			if useHstore {
				tags = jsonToHstore(record.Tags)
			} else {
				tags = record.Tags
			}

			var row []interface{}
			if extraAttributes {
				// Include extra metadata columns
				row = []interface{}{
					record.OsmID,
					record.OsmType,
					tags,
					record.GeomWKB,
					record.Version,
					record.Changeset,
					record.Timestamp,
					record.User,
					record.UID,
				}
			} else {
				row = []interface{}{record.OsmID, record.OsmType, tags, record.GeomWKB}
			}
			select {
			case rowChan <- row:
				// Update live counter if provided
				if counter != nil {
					counter.Add(1)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Build column list based on whether extra attributes are enabled
	columns := []string{"osm_id", "osm_type", "tags", "geom"}
	if extraAttributes {
		columns = append(columns, "osm_version", "osm_changeset", "osm_timestamp", "osm_user", "osm_uid")
	}

	// COPY directly to final table - PostGIS accepts EWKB bytes for geometry columns
	copyCount, err := conn.CopyFrom(
		ctx,
		pgx.Identifier{schema, tableName},
		columns,
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

	// Build tablespace clause for indexes if specified
	tablespaceClause := ""
	if l.cfg.TablespaceIndex != "" {
		tablespaceClause = fmt.Sprintf(" TABLESPACE %s", l.cfg.TablespaceIndex)
	}

	log.Info("Creating indexes", zap.String("table", tableName))

	// Create GIST index
	gistIdx := fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s_geom_idx ON %s USING GIST (geom)%s",
		tableName, fullTableName, tablespaceClause)
	if _, err := conn.Exec(ctx, gistIdx); err != nil {
		return fmt.Errorf("failed to create GIST index: %w", err)
	}

	// Create B-tree index on osm_id
	btreeIdx := fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s_osm_id_idx ON %s (osm_id)%s",
		tableName, fullTableName, tablespaceClause)
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

// jsonToHstore converts a JSON string to pgtype.Hstore for proper pgx encoding
func jsonToHstore(jsonStr string) pgtype.Hstore {
	if jsonStr == "" || jsonStr == "{}" {
		return pgtype.Hstore{}
	}

	// Parse JSON to map
	var tags map[string]string
	if err := json.Unmarshal([]byte(jsonStr), &tags); err != nil {
		return pgtype.Hstore{}
	}

	if len(tags) == 0 {
		return pgtype.Hstore{}
	}

	// Convert to pgtype.Hstore format (map[string]*string)
	result := make(pgtype.Hstore, len(tags))
	for k, v := range tags {
		vCopy := v
		result[k] = &vCopy
	}

	return result
}
