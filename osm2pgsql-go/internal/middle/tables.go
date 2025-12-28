package middle

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kevinramage/osm2pgsql-go/internal/config"
	"github.com/kevinramage/osm2pgsql-go/internal/logger"
	"go.uber.org/zap"
)

// MiddleStore manages the "middle tables" that store raw OSM data
// These tables enable incremental updates by tracking dependencies
type MiddleStore struct {
	cfg  *config.Config
	pool *pgxpool.Pool

	// Statistics
	NodesInserted     atomic.Int64
	WaysInserted      atomic.Int64
	RelationsInserted atomic.Int64
}

// NewMiddleStore creates a new middle table store
func NewMiddleStore(cfg *config.Config, pool *pgxpool.Pool) *MiddleStore {
	return &MiddleStore{
		cfg:  cfg,
		pool: pool,
	}
}

// EnsureTables creates the middle tables if they don't exist
func (m *MiddleStore) EnsureTables(ctx context.Context, dropExisting bool) error {
	log := logger.Get()

	tables := []struct {
		name   string
		schema string
	}{
		{
			name: "planet_osm_nodes",
			schema: `
				CREATE UNLOGGED TABLE IF NOT EXISTS %s.planet_osm_nodes (
					id BIGINT PRIMARY KEY,
					lat INTEGER NOT NULL,
					lon INTEGER NOT NULL,
					tags JSONB
				)%s`,
		},
		{
			name: "planet_osm_ways",
			schema: `
				CREATE UNLOGGED TABLE IF NOT EXISTS %s.planet_osm_ways (
					id BIGINT PRIMARY KEY,
					nodes BIGINT[] NOT NULL,
					tags JSONB
				)%s`,
		},
		{
			name: "planet_osm_rels",
			schema: `
				CREATE UNLOGGED TABLE IF NOT EXISTS %s.planet_osm_rels (
					id BIGINT PRIMARY KEY,
					members JSONB NOT NULL,
					tags JSONB
				)%s`,
		},
	}

	// Build tablespace clause
	tablespaceClause := ""
	if m.cfg.TablespaceMain != "" {
		tablespaceClause = fmt.Sprintf(" TABLESPACE %s", m.cfg.TablespaceMain)
	}

	for _, t := range tables {
		fullName := fmt.Sprintf("%s.%s", m.cfg.DBSchema, t.name)

		if dropExisting {
			log.Info("Dropping middle table", zap.String("table", t.name))
			if _, err := m.pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", fullName)); err != nil {
				return fmt.Errorf("failed to drop table %s: %w", t.name, err)
			}
		}

		log.Info("Creating middle table", zap.String("table", t.name))
		sql := fmt.Sprintf(t.schema, m.cfg.DBSchema, tablespaceClause)
		if _, err := m.pool.Exec(ctx, sql); err != nil {
			return fmt.Errorf("failed to create table %s: %w", t.name, err)
		}
	}

	return nil
}

// LoadNodes bulk inserts nodes from a channel into planet_osm_nodes
func (m *MiddleStore) LoadNodes(ctx context.Context, nodes <-chan RawNode) (int64, error) {
	log := logger.Get()
	log.Info("Starting middle table node load")

	conn, err := m.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	// Convert channel to row source
	rowChan := make(chan []interface{}, 10000)
	go func() {
		defer close(rowChan)
		for node := range nodes {
			var tagsJSON []byte
			if len(node.Tags) > 0 {
				tagsJSON, _ = json.Marshal(node.Tags)
			}

			row := []interface{}{node.ID, node.Lat, node.Lon, tagsJSON}
			select {
			case rowChan <- row:
				m.NodesInserted.Add(1)
			case <-ctx.Done():
				return
			}
		}
	}()

	count, err := conn.Conn().CopyFrom(
		ctx,
		pgx.Identifier{m.cfg.DBSchema, "planet_osm_nodes"},
		[]string{"id", "lat", "lon", "tags"},
		&rowSource{rows: rowChan},
	)
	if err != nil {
		return 0, fmt.Errorf("COPY to planet_osm_nodes failed: %w", err)
	}

	// Convert to logged table
	fullName := fmt.Sprintf("%s.planet_osm_nodes", m.cfg.DBSchema)
	if _, err := conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s SET LOGGED", fullName)); err != nil {
		// Ignore error
	}

	log.Info("Middle table node load complete", zap.Int64("rows", count))
	return count, nil
}

// LoadWays bulk inserts ways from a channel into planet_osm_ways
func (m *MiddleStore) LoadWays(ctx context.Context, ways <-chan RawWay) (int64, error) {
	log := logger.Get()
	log.Info("Starting middle table way load")

	conn, err := m.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	rowChan := make(chan []interface{}, 10000)
	go func() {
		defer close(rowChan)
		for way := range ways {
			var tagsJSON []byte
			if len(way.Tags) > 0 {
				tagsJSON, _ = json.Marshal(way.Tags)
			}

			row := []interface{}{way.ID, way.Nodes, tagsJSON}
			select {
			case rowChan <- row:
				m.WaysInserted.Add(1)
			case <-ctx.Done():
				return
			}
		}
	}()

	count, err := conn.Conn().CopyFrom(
		ctx,
		pgx.Identifier{m.cfg.DBSchema, "planet_osm_ways"},
		[]string{"id", "nodes", "tags"},
		&rowSource{rows: rowChan},
	)
	if err != nil {
		return 0, fmt.Errorf("COPY to planet_osm_ways failed: %w", err)
	}

	// Convert to logged table
	fullName := fmt.Sprintf("%s.planet_osm_ways", m.cfg.DBSchema)
	if _, err := conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s SET LOGGED", fullName)); err != nil {
		// Ignore error
	}

	log.Info("Middle table way load complete", zap.Int64("rows", count))
	return count, nil
}

// LoadRelations bulk inserts relations from a channel into planet_osm_rels
func (m *MiddleStore) LoadRelations(ctx context.Context, relations <-chan RawRelation) (int64, error) {
	log := logger.Get()
	log.Info("Starting middle table relation load")

	conn, err := m.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	rowChan := make(chan []interface{}, 10000)
	go func() {
		defer close(rowChan)
		for rel := range relations {
			membersJSON, _ := json.Marshal(rel.Members)
			var tagsJSON []byte
			if len(rel.Tags) > 0 {
				tagsJSON, _ = json.Marshal(rel.Tags)
			}

			row := []interface{}{rel.ID, membersJSON, tagsJSON}
			select {
			case rowChan <- row:
				m.RelationsInserted.Add(1)
			case <-ctx.Done():
				return
			}
		}
	}()

	count, err := conn.Conn().CopyFrom(
		ctx,
		pgx.Identifier{m.cfg.DBSchema, "planet_osm_rels"},
		[]string{"id", "members", "tags"},
		&rowSource{rows: rowChan},
	)
	if err != nil {
		return 0, fmt.Errorf("COPY to planet_osm_rels failed: %w", err)
	}

	// Convert to logged table
	fullName := fmt.Sprintf("%s.planet_osm_rels", m.cfg.DBSchema)
	if _, err := conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s SET LOGGED", fullName)); err != nil {
		// Ignore error
	}

	log.Info("Middle table relation load complete", zap.Int64("rows", count))
	return count, nil
}

// CreateIndexes creates indexes on middle tables for dependency lookups
func (m *MiddleStore) CreateIndexes(ctx context.Context) error {
	log := logger.Get()
	log.Info("Creating middle table indexes")

	conn, err := m.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	// Set high maintenance_work_mem
	if _, err := conn.Exec(ctx, "SET maintenance_work_mem = '2GB'"); err != nil {
		// Ignore
	}

	// Build tablespace clause
	tablespaceClause := ""
	if m.cfg.TablespaceIndex != "" {
		tablespaceClause = fmt.Sprintf(" TABLESPACE %s", m.cfg.TablespaceIndex)
	}

	indexes := []struct {
		name string
		sql  string
	}{
		{
			name: "planet_osm_ways_nodes_idx",
			sql: fmt.Sprintf(
				"CREATE INDEX IF NOT EXISTS planet_osm_ways_nodes_idx ON %s.planet_osm_ways USING GIN (nodes)%s",
				m.cfg.DBSchema, tablespaceClause),
		},
	}

	for _, idx := range indexes {
		log.Info("Creating index", zap.String("name", idx.name))
		if _, err := conn.Exec(ctx, idx.sql); err != nil {
			return fmt.Errorf("failed to create index %s: %w", idx.name, err)
		}
	}

	// Analyze tables
	for _, table := range []string{"planet_osm_nodes", "planet_osm_ways", "planet_osm_rels"} {
		fullName := fmt.Sprintf("%s.%s", m.cfg.DBSchema, table)
		if _, err := conn.Exec(ctx, fmt.Sprintf("ANALYZE %s", fullName)); err != nil {
			return fmt.Errorf("failed to analyze %s: %w", table, err)
		}
	}

	log.Info("Middle table indexes created")
	return nil
}

// GetNode retrieves a node by ID
func (m *MiddleStore) GetNode(ctx context.Context, id int64) (*RawNode, error) {
	var node RawNode
	var tagsJSON []byte

	err := m.pool.QueryRow(ctx,
		fmt.Sprintf("SELECT id, lat, lon, tags FROM %s.planet_osm_nodes WHERE id = $1", m.cfg.DBSchema),
		id,
	).Scan(&node.ID, &node.Lat, &node.Lon, &tagsJSON)

	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if len(tagsJSON) > 0 {
		json.Unmarshal(tagsJSON, &node.Tags)
	}

	return &node, nil
}

// GetWay retrieves a way by ID
func (m *MiddleStore) GetWay(ctx context.Context, id int64) (*RawWay, error) {
	var way RawWay
	var tagsJSON []byte

	err := m.pool.QueryRow(ctx,
		fmt.Sprintf("SELECT id, nodes, tags FROM %s.planet_osm_ways WHERE id = $1", m.cfg.DBSchema),
		id,
	).Scan(&way.ID, &way.Nodes, &tagsJSON)

	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if len(tagsJSON) > 0 {
		json.Unmarshal(tagsJSON, &way.Tags)
	}

	return &way, nil
}

// GetWaysForNode finds all ways that contain a given node ID
func (m *MiddleStore) GetWaysForNode(ctx context.Context, nodeID int64) ([]int64, error) {
	rows, err := m.pool.Query(ctx,
		fmt.Sprintf("SELECT id FROM %s.planet_osm_ways WHERE nodes @> ARRAY[$1]::bigint[]", m.cfg.DBSchema),
		nodeID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var wayIDs []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		wayIDs = append(wayIDs, id)
	}

	return wayIDs, rows.Err()
}

// GetRelationsForMember finds all relations that contain a given member
func (m *MiddleStore) GetRelationsForMember(ctx context.Context, memberType string, memberRef int64) ([]int64, error) {
	// Query JSONB array for matching member
	rows, err := m.pool.Query(ctx,
		fmt.Sprintf(`
			SELECT id FROM %s.planet_osm_rels
			WHERE members @> '[{"Type": %q, "Ref": %d}]'::jsonb
		`, m.cfg.DBSchema, memberType, memberRef),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var relIDs []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		relIDs = append(relIDs, id)
	}

	return relIDs, rows.Err()
}

// GetRelation retrieves a relation by ID
func (m *MiddleStore) GetRelation(ctx context.Context, id int64) (*RawRelation, error) {
	var rel RawRelation
	var membersJSON, tagsJSON []byte

	err := m.pool.QueryRow(ctx,
		fmt.Sprintf("SELECT id, members, tags FROM %s.planet_osm_rels WHERE id = $1", m.cfg.DBSchema),
		id,
	).Scan(&rel.ID, &membersJSON, &tagsJSON)

	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	json.Unmarshal(membersJSON, &rel.Members)
	if len(tagsJSON) > 0 {
		json.Unmarshal(tagsJSON, &rel.Tags)
	}

	return &rel, nil
}

// UpdateNode updates or inserts a node
func (m *MiddleStore) UpdateNode(ctx context.Context, node *RawNode) error {
	var tagsJSON []byte
	if len(node.Tags) > 0 {
		tagsJSON, _ = json.Marshal(node.Tags)
	}

	_, err := m.pool.Exec(ctx,
		fmt.Sprintf(`
			INSERT INTO %s.planet_osm_nodes (id, lat, lon, tags)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (id) DO UPDATE SET lat = $2, lon = $3, tags = $4
		`, m.cfg.DBSchema),
		node.ID, node.Lat, node.Lon, tagsJSON,
	)
	return err
}

// UpdateWay updates or inserts a way
func (m *MiddleStore) UpdateWay(ctx context.Context, way *RawWay) error {
	var tagsJSON []byte
	if len(way.Tags) > 0 {
		tagsJSON, _ = json.Marshal(way.Tags)
	}

	_, err := m.pool.Exec(ctx,
		fmt.Sprintf(`
			INSERT INTO %s.planet_osm_ways (id, nodes, tags)
			VALUES ($1, $2, $3)
			ON CONFLICT (id) DO UPDATE SET nodes = $2, tags = $3
		`, m.cfg.DBSchema),
		way.ID, way.Nodes, tagsJSON,
	)
	return err
}

// UpdateRelation updates or inserts a relation
func (m *MiddleStore) UpdateRelation(ctx context.Context, rel *RawRelation) error {
	membersJSON, _ := json.Marshal(rel.Members)
	var tagsJSON []byte
	if len(rel.Tags) > 0 {
		tagsJSON, _ = json.Marshal(rel.Tags)
	}

	_, err := m.pool.Exec(ctx,
		fmt.Sprintf(`
			INSERT INTO %s.planet_osm_rels (id, members, tags)
			VALUES ($1, $2, $3)
			ON CONFLICT (id) DO UPDATE SET members = $2, tags = $3
		`, m.cfg.DBSchema),
		rel.ID, membersJSON, tagsJSON,
	)
	return err
}

// DeleteNode removes a node from middle tables
func (m *MiddleStore) DeleteNode(ctx context.Context, id int64) error {
	_, err := m.pool.Exec(ctx,
		fmt.Sprintf("DELETE FROM %s.planet_osm_nodes WHERE id = $1", m.cfg.DBSchema),
		id,
	)
	return err
}

// DeleteWay removes a way from middle tables
func (m *MiddleStore) DeleteWay(ctx context.Context, id int64) error {
	_, err := m.pool.Exec(ctx,
		fmt.Sprintf("DELETE FROM %s.planet_osm_ways WHERE id = $1", m.cfg.DBSchema),
		id,
	)
	return err
}

// DeleteRelation removes a relation from middle tables
func (m *MiddleStore) DeleteRelation(ctx context.Context, id int64) error {
	_, err := m.pool.Exec(ctx,
		fmt.Sprintf("DELETE FROM %s.planet_osm_rels WHERE id = $1", m.cfg.DBSchema),
		id,
	)
	return err
}

// DropTables drops all middle tables
func (m *MiddleStore) DropTables(ctx context.Context) error {
	log := logger.Get()
	log.Info("Dropping middle tables")

	for _, table := range []string{"planet_osm_nodes", "planet_osm_ways", "planet_osm_rels"} {
		fullName := fmt.Sprintf("%s.%s", m.cfg.DBSchema, table)
		if _, err := m.pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", fullName)); err != nil {
			return fmt.Errorf("failed to drop %s: %w", table, err)
		}
	}

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
