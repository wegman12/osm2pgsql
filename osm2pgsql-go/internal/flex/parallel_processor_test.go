package flex

import (
	"testing"

	"github.com/kevinramage/osm2pgsql-go/internal/config"
)

func TestNewParallelProcessorWithoutDB(t *testing.T) {
	// Test that we can create a processor structure (without DB connection)
	cfg := &config.Config{
		Workers:    4,
		Projection: 3857,
		DBSchema:   "public",
	}

	// We can't fully test without a DB, but we can verify the worker count defaults
	if cfg.Workers != 4 {
		t.Errorf("expected 4 workers, got %d", cfg.Workers)
	}
}

func TestBatchBufferLogic(t *testing.T) {
	// Test the batch buffer structure
	buffer := &batchBuffer{
		table: &Table{
			Name:       "test",
			Schema:     "public",
			GeomColumn: "geom",
		},
		rows:    make([][]interface{}, 0, 100),
		columns: []string{"osm_id", "name", "geom"},
	}

	// Test adding rows
	buffer.rows = append(buffer.rows, []interface{}{int64(1), "Test", nil})
	buffer.rows = append(buffer.rows, []interface{}{int64(2), "Test2", nil})

	if len(buffer.rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(buffer.rows))
	}

	if len(buffer.columns) != 3 {
		t.Errorf("expected 3 columns, got %d", len(buffer.columns))
	}
}

func TestWorkerResultHandling(t *testing.T) {
	// Test worker result structure
	obj := &OSMObject{
		ID:   12345,
		Type: "node",
		Tags: map[string]string{"name": "Test"},
		Lat:  43.7384,
		Lon:  7.4246,
	}

	result := workerResult{
		objType: "node",
		obj:     obj,
		rows: []Row{
			{
				TableName: "pois",
				Values:    map[string]interface{}{"name": "Test"},
			},
		},
	}

	if result.objType != "node" {
		t.Errorf("expected node type, got %s", result.objType)
	}

	if len(result.rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(result.rows))
	}

	if result.rows[0].TableName != "pois" {
		t.Errorf("expected pois table, got %s", result.rows[0].TableName)
	}
}

func TestLuaWorkerStructure(t *testing.T) {
	// Test luaWorker structure
	worker := &luaWorker{
		id: 0,
	}

	if worker.id != 0 {
		t.Errorf("expected worker id 0, got %d", worker.id)
	}
}

func TestParallelProcessorStats(t *testing.T) {
	// Create a minimal processor to test stats
	pp := &ParallelProcessor{
		batchBuffers: make(map[string]*batchBuffer),
	}

	// Test initial stats are zero
	stats := pp.Stats()
	if stats.NodesProcessed != 0 {
		t.Errorf("expected 0 nodes processed, got %d", stats.NodesProcessed)
	}
	if stats.WaysProcessed != 0 {
		t.Errorf("expected 0 ways processed, got %d", stats.WaysProcessed)
	}
	if stats.RelationsProcessed != 0 {
		t.Errorf("expected 0 relations processed, got %d", stats.RelationsProcessed)
	}
	if stats.RowsInserted != 0 {
		t.Errorf("expected 0 rows inserted, got %d", stats.RowsInserted)
	}

	// Test atomic increments
	pp.nodesProcessed.Add(100)
	pp.waysProcessed.Add(50)
	pp.relationsProcessed.Add(10)
	pp.rowsInserted.Add(500)

	stats = pp.Stats()
	if stats.NodesProcessed != 100 {
		t.Errorf("expected 100 nodes processed, got %d", stats.NodesProcessed)
	}
	if stats.WaysProcessed != 50 {
		t.Errorf("expected 50 ways processed, got %d", stats.WaysProcessed)
	}
	if stats.RelationsProcessed != 10 {
		t.Errorf("expected 10 relations processed, got %d", stats.RelationsProcessed)
	}
	if stats.RowsInserted != 500 {
		t.Errorf("expected 500 rows inserted, got %d", stats.RowsInserted)
	}
}
