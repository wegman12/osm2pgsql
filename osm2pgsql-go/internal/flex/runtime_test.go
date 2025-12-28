package flex

import (
	"testing"
)

func TestNewRuntime(t *testing.T) {
	runtime := NewRuntime(4326, "public")
	defer runtime.Close()

	if runtime.L == nil {
		t.Error("Lua state should not be nil")
	}
	if runtime.tables == nil {
		t.Error("tables registry should not be nil")
	}
}

func TestRuntimeDefineTable(t *testing.T) {
	runtime := NewRuntime(4326, "public")
	defer runtime.Close()

	// Define a simple table via Lua
	luaCode := `
		local roads = osm2pgsql.define_table({
			name = 'roads',
			ids = { type = 'way' },
			columns = {
				{ column = 'name', type = 'text' },
				{ column = 'highway', type = 'text' },
				{ column = 'geom', type = 'linestring' }
			}
		})
	`

	if err := runtime.LoadString(luaCode); err != nil {
		t.Fatalf("Lua execution failed: %v", err)
	}

	// Verify table was registered
	tables := runtime.Tables()
	if tables.Get("roads") == nil {
		t.Error("roads table should be registered")
	}

	roads := tables.Get("roads")
	if roads.Name != "roads" {
		t.Errorf("table name = %q, want %q", roads.Name, "roads")
	}
	if roads.Schema != "public" {
		t.Errorf("table schema = %q, want %q", roads.Schema, "public")
	}

	// Check columns - should have osm_id + 3 defined columns
	expectedCols := 4 // osm_id + name + highway + geom
	if len(roads.Columns) != expectedCols {
		t.Errorf("column count = %d, want %d", len(roads.Columns), expectedCols)
	}

	if roads.GeomColumn != "geom" {
		t.Errorf("geom column = %q, want %q", roads.GeomColumn, "geom")
	}
}

func TestRuntimeProcessCallbacks(t *testing.T) {
	runtime := NewRuntime(4326, "public")
	defer runtime.Close()

	// Define process callbacks
	luaCode := `
		local pois = osm2pgsql.define_table({
			name = 'pois',
			ids = { type = 'node' },
			columns = {
				{ column = 'name', type = 'text' },
				{ column = 'geom', type = 'point' }
			}
		})

		function osm2pgsql.process_node(object)
			if object.tags.amenity then
				pois:insert({
					name = object.tags.name
				})
			end
		end
	`

	if err := runtime.LoadString(luaCode); err != nil {
		t.Fatalf("Lua execution failed: %v", err)
	}

	// Check that process_node is defined
	if !runtime.HasProcessNode() {
		t.Error("process_node should be defined")
	}
	if runtime.HasProcessWay() {
		t.Error("process_way should not be defined")
	}
	if runtime.HasProcessRelation() {
		t.Error("process_relation should not be defined")
	}

	// Test processing a node
	obj := &OSMObject{
		ID:   12345,
		Type: "node",
		Tags: map[string]string{
			"amenity": "restaurant",
			"name":    "Test Restaurant",
		},
		Lat: 43.7384,
		Lon: 7.4246,
	}

	if err := runtime.ProcessNode(obj); err != nil {
		t.Fatalf("ProcessNode failed: %v", err)
	}

	// Collect pending rows
	rows := runtime.CollectRows()
	if len(rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(rows))
	}

	if rows[0].TableName != "pois" {
		t.Errorf("table name = %q, want %q", rows[0].TableName, "pois")
	}
	if rows[0].Values["name"] != "Test Restaurant" {
		t.Errorf("name = %q, want %q", rows[0].Values["name"], "Test Restaurant")
	}
}

func TestParseColumnType(t *testing.T) {
	tests := []struct {
		input    string
		expected ColumnType
		wantErr  bool
	}{
		{"text", ColumnTypeText, false},
		{"string", ColumnTypeText, false},
		{"int", ColumnTypeInt, false},
		{"integer", ColumnTypeInt, false},
		{"bigint", ColumnTypeBigInt, false},
		{"real", ColumnTypeReal, false},
		{"float", ColumnTypeReal, false},
		{"bool", ColumnTypeBoolean, false},
		{"boolean", ColumnTypeBoolean, false},
		{"json", ColumnTypeJSON, false},
		{"jsonb", ColumnTypeJSONB, false},
		{"hstore", ColumnTypeHstore, false},
		{"point", ColumnTypePoint, false},
		{"linestring", ColumnTypeLineString, false},
		{"polygon", ColumnTypePolygon, false},
		{"multipolygon", ColumnTypeMultiPolygon, false},
		{"geometry", ColumnTypeGeometry, false},
		{"unknown_type_xyz", ColumnTypeText, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			colType, err := ParseColumnType(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if colType != tt.expected {
				t.Errorf("ParseColumnType(%q) = %v, want %v", tt.input, colType, tt.expected)
			}
		})
	}
}

func TestColumnTypeString(t *testing.T) {
	tests := []struct {
		colType  ColumnType
		expected string
	}{
		{ColumnTypeText, "TEXT"},
		{ColumnTypeInt, "INTEGER"},
		{ColumnTypeBigInt, "BIGINT"},
		{ColumnTypeReal, "REAL"},
		{ColumnTypeBoolean, "BOOLEAN"},
		{ColumnTypeJSON, "JSON"},
		{ColumnTypeJSONB, "JSONB"},
		{ColumnTypeHstore, "HSTORE"},
		{ColumnTypeTimestamp, "TIMESTAMP WITH TIME ZONE"},
		{ColumnTypeGeometry, "GEOMETRY"},
		{ColumnTypePoint, "GEOMETRY(Point)"},
		{ColumnTypeLineString, "GEOMETRY(LineString)"},
		{ColumnTypePolygon, "GEOMETRY(Polygon)"},
		{ColumnTypeMultiPolygon, "GEOMETRY(MultiPolygon)"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			got := tt.colType.String()
			if got != tt.expected {
				t.Errorf("ColumnType.String() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestOSMObjectHelpers(t *testing.T) {
	obj := &OSMObject{
		ID:   12345,
		Type: "node",
		Tags: map[string]string{
			"name":    "Test",
			"amenity": "restaurant",
		},
	}

	// Test Tag
	if obj.Tag("name") != "Test" {
		t.Errorf("Tag(name) = %q, want %q", obj.Tag("name"), "Test")
	}
	if obj.Tag("missing") != "" {
		t.Errorf("Tag(missing) = %q, want empty", obj.Tag("missing"))
	}

	// Test HasTag
	if !obj.HasTag("name") {
		t.Error("HasTag(name) should return true")
	}
	if obj.HasTag("missing") {
		t.Error("HasTag(missing) should return false")
	}

	// Test HasTags
	if !obj.HasTags("name", "missing") {
		t.Error("HasTags(name, missing) should return true")
	}
	if obj.HasTags("missing1", "missing2") {
		t.Error("HasTags(missing1, missing2) should return false")
	}

	// Test TagCount
	if obj.TagCount() != 2 {
		t.Errorf("TagCount() = %d, want 2", obj.TagCount())
	}

	// Test geometry
	if obj.HasGeometry() {
		t.Error("HasGeometry should return false initially")
	}
	obj.SetGeometry([]byte{1, 2, 3}, "point")
	if !obj.HasGeometry() {
		t.Error("HasGeometry should return true after SetGeometry")
	}
	wkb, geomType := obj.Geometry()
	if len(wkb) != 3 {
		t.Errorf("wkb length = %d, want 3", len(wkb))
	}
	if geomType != "point" {
		t.Errorf("geomType = %q, want %q", geomType, "point")
	}
}
