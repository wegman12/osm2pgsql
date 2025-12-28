package osc

import (
	"time"

	"github.com/kevinramage/osm2pgsql-go/internal/middle"
)

// Action represents the type of change in an OSC file
type Action string

const (
	ActionCreate Action = "create"
	ActionModify Action = "modify"
	ActionDelete Action = "delete"
)

// Change represents a single OSM change from an OSC file
type Change struct {
	Action Action
	Type   string      // "node", "way", "relation"
	Node   *middle.RawNode
	Way    *middle.RawWay
	Relation *middle.RawRelation
}

// NodeChange holds the parsed node data from OSC
type NodeChange struct {
	ID        int64
	Lat       float64
	Lon       float64
	Tags      map[string]string
	Version   int32
	Changeset int64
	Timestamp time.Time
	User      string
	UID       int32
}

// WayChange holds the parsed way data from OSC
type WayChange struct {
	ID        int64
	NodeRefs  []int64
	Tags      map[string]string
	Version   int32
	Changeset int64
	Timestamp time.Time
	User      string
	UID       int32
}

// RelationMemberChange holds a relation member from OSC
type RelationMemberChange struct {
	Type string // "node", "way", "relation"
	Ref  int64
	Role string
}

// RelationChange holds the parsed relation data from OSC
type RelationChange struct {
	ID        int64
	Members   []RelationMemberChange
	Tags      map[string]string
	Version   int32
	Changeset int64
	Timestamp time.Time
	User      string
	UID       int32
}

// Stats tracks OSC parsing statistics
type Stats struct {
	NodesCreated     int64
	NodesModified    int64
	NodesDeleted     int64
	WaysCreated      int64
	WaysModified     int64
	WaysDeleted      int64
	RelationsCreated int64
	RelationsModified int64
	RelationsDeleted int64
}

// Total returns total number of changes
func (s *Stats) Total() int64 {
	return s.NodesCreated + s.NodesModified + s.NodesDeleted +
		s.WaysCreated + s.WaysModified + s.WaysDeleted +
		s.RelationsCreated + s.RelationsModified + s.RelationsDeleted
}
