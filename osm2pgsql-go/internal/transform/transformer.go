package transform

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	"go.uber.org/zap"

	"github.com/kevinramage/osm2pgsql-go/internal/config"
	"github.com/kevinramage/osm2pgsql-go/internal/logger"
	_ "github.com/marcboeker/go-duckdb"
)

// Stats holds transformation statistics
type Stats struct {
	Points   int64
	Lines    int64
	Polygons int64
}

// Transformer uses DuckDB to build geometries from Parquet files
type Transformer struct {
	cfg *config.Config
	db  *sql.DB
}

// NewTransformer creates a new DuckDB transformer
func NewTransformer(cfg *config.Config) (*Transformer, error) {
	// Open DuckDB with memory limit
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB: %w", err)
	}

	// Use a conservative memory limit (40% of specified) to leave room for OS and other processes
	// DuckDB will spill to disk when this limit is reached
	memLimit := cfg.MemoryMB * 40 / 100
	if memLimit < 4000 {
		memLimit = 4000 // Minimum 4GB
	}

	// Configure DuckDB for performance with disk spilling
	configs := []string{
		fmt.Sprintf("SET memory_limit='%dMB'", memLimit),
		fmt.Sprintf("SET threads=%d", cfg.Workers),
		fmt.Sprintf("SET temp_directory='%s'", filepath.Join(cfg.OutputDir, "duckdb_tmp")),
		"SET enable_progress_bar=true",
		"SET preserve_insertion_order=false", // Allows more parallel execution
		"INSTALL spatial",
		"LOAD spatial",
	}

	for _, c := range configs {
		if _, err := db.Exec(c); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to configure DuckDB (%s): %w", c, err)
		}
	}

	return &Transformer{
		cfg: cfg,
		db:  db,
	}, nil
}

// Close closes the DuckDB connection
func (t *Transformer) Close() error {
	return t.db.Close()
}

// Run executes the transformation
func (t *Transformer) Run() (*Stats, error) {
	stats := &Stats{}

	// Create temp directory for DuckDB spilling
	tmpDir := filepath.Join(t.cfg.OutputDir, "duckdb_tmp")
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create temp directory: %w", err)
	}

	// Create views for Parquet files
	if err := t.createViews(); err != nil {
		return nil, err
	}

	log := logger.Get()

	// Build point geometries (from nodes with tags)
	log.Info("Building point geometries")
	points, err := t.buildPoints()
	if err != nil {
		return nil, fmt.Errorf("failed to build points: %w", err)
	}
	stats.Points = points
	log.Info("Created points", zap.Int64("count", points))

	// Build line geometries (from ways)
	log.Info("Building line geometries")
	lines, err := t.buildLines()
	if err != nil {
		return nil, fmt.Errorf("failed to build lines: %w", err)
	}
	stats.Lines = lines
	log.Info("Created lines", zap.Int64("count", lines))

	// Build polygon geometries (from closed ways and relations)
	log.Info("Building polygon geometries")
	polygons, err := t.buildPolygons()
	if err != nil {
		return nil, fmt.Errorf("failed to build polygons: %w", err)
	}
	stats.Polygons = polygons
	log.Info("Created polygons", zap.Int64("count", polygons))

	return stats, nil
}

func (t *Transformer) createViews() error {
	views := map[string]string{
		"nodes":            filepath.Join(t.cfg.OutputDir, "nodes.parquet"),
		"ways":             filepath.Join(t.cfg.OutputDir, "ways.parquet"),
		"way_nodes":        filepath.Join(t.cfg.OutputDir, "way_nodes.parquet"),
		"relations":        filepath.Join(t.cfg.OutputDir, "relations.parquet"),
		"relation_members": filepath.Join(t.cfg.OutputDir, "relation_members.parquet"),
	}

	for name, path := range views {
		sql := fmt.Sprintf("CREATE VIEW %s AS SELECT * FROM read_parquet('%s')", name, path)
		if _, err := t.db.Exec(sql); err != nil {
			return fmt.Errorf("failed to create view %s: %w", name, err)
		}
	}

	return nil
}

func (t *Transformer) buildPoints() (int64, error) {
	outputPath := filepath.Join(t.cfg.OutputDir, "points.parquet")

	// Points are nodes with meaningful tags (not just metadata)
	// We filter out nodes that are just way vertices
	// Output geometry as WKT text for compatibility
	query := fmt.Sprintf(`
		COPY (
			SELECT
				n.id AS osm_id,
				'N' AS osm_type,
				n.tags,
				ST_AsText(ST_Point(n.lon, n.lat)) AS geom_wkt
			FROM nodes n
			WHERE n.tags != '{}'
			  AND n.tags NOT LIKE '%%"created_by"%%'
		) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)
	`, outputPath)

	result, err := t.db.Exec(query)
	if err != nil {
		return 0, err
	}

	count, _ := result.RowsAffected()
	return count, nil
}

func (t *Transformer) buildLines() (int64, error) {
	outputPath := filepath.Join(t.cfg.OutputDir, "lines.parquet")

	// Build linestrings from ways by joining with nodes
	// This is the key join operation that was the bottleneck in osm2pgsql
	// Output geometry as WKT text for compatibility
	query := fmt.Sprintf(`
		COPY (
			WITH way_coords AS (
				SELECT
					wn.way_id,
					wn.seq,
					n.lon,
					n.lat
				FROM way_nodes wn
				JOIN nodes n ON wn.node_id = n.id
			),
			way_geoms AS (
				SELECT
					way_id,
					ST_MakeLine(
						list(ST_Point(lon, lat) ORDER BY seq)
					) AS geom
				FROM way_coords
				GROUP BY way_id
				HAVING count(*) >= 2
			)
			SELECT
				w.id AS osm_id,
				'W' AS osm_type,
				w.tags,
				ST_AsText(wg.geom) AS geom_wkt
			FROM ways w
			JOIN way_geoms wg ON w.id = wg.way_id
			WHERE NOT ST_IsClosed(wg.geom)
			   OR w.tags NOT LIKE '%%"area"%%'
		) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)
	`, outputPath)

	result, err := t.db.Exec(query)
	if err != nil {
		return 0, err
	}

	count, _ := result.RowsAffected()
	return count, nil
}

func (t *Transformer) buildPolygons() (int64, error) {
	outputPath := filepath.Join(t.cfg.OutputDir, "polygons.parquet")

	// Build polygons from closed ways
	// For simplicity, we're treating closed ways as polygons
	// A full implementation would also handle multipolygon relations
	// Output geometry as WKT text for compatibility
	query := fmt.Sprintf(`
		COPY (
			WITH way_coords AS (
				SELECT
					wn.way_id,
					wn.seq,
					n.lon,
					n.lat
				FROM way_nodes wn
				JOIN nodes n ON wn.node_id = n.id
			),
			way_geoms AS (
				SELECT
					way_id,
					ST_MakeLine(
						list(ST_Point(lon, lat) ORDER BY seq)
					) AS geom
				FROM way_coords
				GROUP BY way_id
				HAVING count(*) >= 4
			)
			SELECT
				w.id AS osm_id,
				'W' AS osm_type,
				w.tags,
				ST_AsText(ST_MakePolygon(wg.geom)) AS geom_wkt
			FROM ways w
			JOIN way_geoms wg ON w.id = wg.way_id
			WHERE ST_IsClosed(wg.geom)
			  AND ST_NPoints(wg.geom) >= 4
		) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)
	`, outputPath)

	result, err := t.db.Exec(query)
	if err != nil {
		return 0, err
	}

	count, _ := result.RowsAffected()
	return count, nil
}
