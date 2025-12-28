package flex

import (
	"fmt"
	"time"
)

// ColumnType represents the type of a table column
type ColumnType int

const (
	ColumnTypeText ColumnType = iota
	ColumnTypeInt
	ColumnTypeBigInt
	ColumnTypeReal
	ColumnTypeBoolean
	ColumnTypeJSON
	ColumnTypeJSONB
	ColumnTypeHstore
	ColumnTypeTimestamp
	ColumnTypeGeometry
	ColumnTypePoint
	ColumnTypeLineString
	ColumnTypePolygon
	ColumnTypeMultiPoint
	ColumnTypeMultiLineString
	ColumnTypeMultiPolygon
	ColumnTypeGeometryCollection
	ColumnTypeArea // Special: stores area in specified units
)

// String returns the SQL type name
func (ct ColumnType) String() string {
	switch ct {
	case ColumnTypeText:
		return "TEXT"
	case ColumnTypeInt:
		return "INTEGER"
	case ColumnTypeBigInt:
		return "BIGINT"
	case ColumnTypeReal:
		return "REAL"
	case ColumnTypeBoolean:
		return "BOOLEAN"
	case ColumnTypeJSON:
		return "JSON"
	case ColumnTypeJSONB:
		return "JSONB"
	case ColumnTypeHstore:
		return "HSTORE"
	case ColumnTypeTimestamp:
		return "TIMESTAMP WITH TIME ZONE"
	case ColumnTypeGeometry:
		return "GEOMETRY"
	case ColumnTypePoint:
		return "GEOMETRY(Point)"
	case ColumnTypeLineString:
		return "GEOMETRY(LineString)"
	case ColumnTypePolygon:
		return "GEOMETRY(Polygon)"
	case ColumnTypeMultiPoint:
		return "GEOMETRY(MultiPoint)"
	case ColumnTypeMultiLineString:
		return "GEOMETRY(MultiLineString)"
	case ColumnTypeMultiPolygon:
		return "GEOMETRY(MultiPolygon)"
	case ColumnTypeGeometryCollection:
		return "GEOMETRY(GeometryCollection)"
	case ColumnTypeArea:
		return "REAL"
	default:
		return "TEXT"
	}
}

// ParseColumnType parses a column type string
func ParseColumnType(s string) (ColumnType, error) {
	switch s {
	case "text", "string":
		return ColumnTypeText, nil
	case "int", "integer", "int4":
		return ColumnTypeInt, nil
	case "bigint", "int8":
		return ColumnTypeBigInt, nil
	case "real", "float", "double", "float8":
		return ColumnTypeReal, nil
	case "bool", "boolean":
		return ColumnTypeBoolean, nil
	case "json":
		return ColumnTypeJSON, nil
	case "jsonb":
		return ColumnTypeJSONB, nil
	case "hstore":
		return ColumnTypeHstore, nil
	case "timestamp", "timestamptz":
		return ColumnTypeTimestamp, nil
	case "geometry":
		return ColumnTypeGeometry, nil
	case "point":
		return ColumnTypePoint, nil
	case "linestring":
		return ColumnTypeLineString, nil
	case "polygon":
		return ColumnTypePolygon, nil
	case "multipoint":
		return ColumnTypeMultiPoint, nil
	case "multilinestring":
		return ColumnTypeMultiLineString, nil
	case "multipolygon":
		return ColumnTypeMultiPolygon, nil
	case "geometrycollection":
		return ColumnTypeGeometryCollection, nil
	case "area":
		return ColumnTypeArea, nil
	default:
		return ColumnTypeText, fmt.Errorf("unknown column type: %s", s)
	}
}

// Column represents a table column definition
type Column struct {
	Name       string
	Type       ColumnType
	NotNull    bool
	CreateOnly bool // Only used during table creation, not for inserts
	SRID       int  // For geometry columns
}

// Table represents an output table definition
type Table struct {
	Name        string
	Schema      string
	Columns     []Column
	GeomColumn  string // Name of the geometry column (if any)
	SRID        int    // SRID for geometry column
	Indexes     []string
	ClusterOn   string
	DataColumns []string // Non-geometry columns that receive data
}

// TableRegistry holds all defined tables
type TableRegistry struct {
	tables map[string]*Table
}

// NewTableRegistry creates a new table registry
func NewTableRegistry() *TableRegistry {
	return &TableRegistry{
		tables: make(map[string]*Table),
	}
}

// Register adds a table to the registry
func (r *TableRegistry) Register(t *Table) {
	r.tables[t.Name] = t
}

// Get returns a table by name
func (r *TableRegistry) Get(name string) *Table {
	return r.tables[name]
}

// All returns all registered tables
func (r *TableRegistry) All() []*Table {
	tables := make([]*Table, 0, len(r.tables))
	for _, t := range r.tables {
		tables = append(tables, t)
	}
	return tables
}

// Row represents a row to be inserted into a table
type Row struct {
	TableName string
	Values    map[string]interface{}
	GeomWKB   []byte
}

// OSMObject represents an OSM object passed to Lua callbacks
type OSMObject struct {
	ID        int64
	Type      string // "node", "way", "relation"
	Version   int
	Timestamp time.Time
	Changeset int64
	UID       int
	User      string
	Tags      map[string]string

	// Node-specific
	Lat float64
	Lon float64

	// Way-specific
	NodeRefs []int64
	IsClosed bool

	// Relation-specific
	Members []RelationMember

	// Computed geometry (set by as_point, as_linestring, etc.)
	geometry     []byte
	geometryType string

	// Coordinate data for geometry building
	Coords []float64 // [lon, lat, lon, lat, ...]
}

// RelationMember represents a member of a relation
type RelationMember struct {
	Type string // "n", "w", "r"
	Ref  int64
	Role string
}

// Tag returns the value of a tag, or empty string if not found
func (o *OSMObject) Tag(key string) string {
	return o.Tags[key]
}

// HasTag returns true if the object has the specified tag
func (o *OSMObject) HasTag(key string) bool {
	_, ok := o.Tags[key]
	return ok
}

// HasTags returns true if the object has any of the specified tags
func (o *OSMObject) HasTags(keys ...string) bool {
	for _, key := range keys {
		if _, ok := o.Tags[key]; ok {
			return true
		}
	}
	return false
}

// TagCount returns the number of tags
func (o *OSMObject) TagCount() int {
	return len(o.Tags)
}

// SetGeometry sets the computed geometry
func (o *OSMObject) SetGeometry(wkb []byte, geomType string) {
	o.geometry = wkb
	o.geometryType = geomType
}

// Geometry returns the computed geometry
func (o *OSMObject) Geometry() ([]byte, string) {
	return o.geometry, o.geometryType
}

// HasGeometry returns true if geometry has been computed
func (o *OSMObject) HasGeometry() bool {
	return o.geometry != nil
}
