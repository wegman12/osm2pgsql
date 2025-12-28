package middle

import "time"

// RawNode represents an OSM node as stored in middle tables
// Coordinates are stored as scaled integers (lat/lon × 10^7) for compact storage
type RawNode struct {
	ID        int64
	Lat       int32 // scaled: lat * 10^7
	Lon       int32 // scaled: lon * 10^7
	Tags      map[string]string
	Version   int32
	Changeset int64
	Timestamp time.Time
	User      string
	UID       int32
}

// RawWay represents an OSM way as stored in middle tables
type RawWay struct {
	ID        int64
	Nodes     []int64 // ordered node ID array
	Tags      map[string]string
	Version   int32
	Changeset int64
	Timestamp time.Time
	User      string
	UID       int32
}

// RelationMember represents a member of an OSM relation
type RelationMember struct {
	Type string // "n" = node, "w" = way, "r" = relation
	Ref  int64
	Role string
}

// RawRelation represents an OSM relation as stored in middle tables
type RawRelation struct {
	ID        int64
	Members   []RelationMember
	Tags      map[string]string
	Version   int32
	Changeset int64
	Timestamp time.Time
	User      string
	UID       int32
}

// ScaleCoord converts a float64 lat/lon to scaled integer (× 10^7)
func ScaleCoord(coord float64) int32 {
	return int32(coord * 1e7)
}

// UnscaleCoord converts a scaled integer back to float64
func UnscaleCoord(scaled int32) float64 {
	return float64(scaled) / 1e7
}
