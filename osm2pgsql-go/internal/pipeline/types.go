package pipeline

import (
	"time"

	"github.com/kevinramage/osm2pgsql-go/internal/middle"
)

// GeometryRecord represents a single geometry for streaming between extractor and loader
type GeometryRecord struct {
	OsmID   int64
	OsmType string // "N", "W", or "R"
	Tags    string // JSON string
	GeomWKB []byte

	// Extra attributes (optional, only populated when ExtraAttributes is enabled)
	Version   int
	Changeset int64
	Timestamp time.Time
	User      string
	UID       int
}

// GeometryStreams holds the three output channels from the streaming extractor
type GeometryStreams struct {
	Points   <-chan GeometryRecord
	Lines    <-chan GeometryRecord
	Polygons <-chan GeometryRecord
	Errors   <-chan error

	// Raw OSM data streams (only populated when SlimMode is enabled)
	RawNodes     <-chan middle.RawNode
	RawWays      <-chan middle.RawWay
	RawRelations <-chan middle.RawRelation
}

// ExtractStats holds extraction statistics
type ExtractStats struct {
	Nodes     int64
	Ways      int64
	Relations int64
	BytesRead int64
}

// LoadStats holds loading statistics per table
type LoadStats struct {
	Table      string
	RowsLoaded int64
}

// ImportStats holds combined import statistics
type ImportStats struct {
	Extract    ExtractStats
	PointsLoad LoadStats
	LinesLoad  LoadStats
	PolysLoad  LoadStats
	TotalRows  int64
}
