package osc

import (
	"context"
	"strings"
	"testing"
)

func TestParseOSC(t *testing.T) {
	oscData := `<?xml version="1.0" encoding="UTF-8"?>
<osmChange version="0.6" generator="test">
  <create>
    <node id="1" lat="43.7384" lon="7.4246" version="1" changeset="123" timestamp="2024-01-15T12:00:00Z" user="testuser" uid="1">
      <tag k="name" v="Test Node"/>
      <tag k="amenity" v="cafe"/>
    </node>
    <way id="100" version="1" changeset="124">
      <nd ref="1"/>
      <nd ref="2"/>
      <nd ref="3"/>
      <tag k="highway" v="primary"/>
    </way>
  </create>
  <modify>
    <node id="2" lat="43.7390" lon="7.4250" version="2">
      <tag k="name" v="Modified Node"/>
    </node>
    <relation id="200" version="2">
      <member type="way" ref="100" role="outer"/>
      <member type="way" ref="101" role="inner"/>
      <tag k="type" v="multipolygon"/>
    </relation>
  </modify>
  <delete>
    <node id="999"/>
    <way id="998"/>
  </delete>
</osmChange>`

	parser := NewParser()
	ctx := context.Background()
	changes, errChan := parser.ParseReader(ctx, strings.NewReader(oscData))

	var allChanges []Change
	for change := range changes {
		allChanges = append(allChanges, change)
	}

	// Check for errors
	for err := range errChan {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	// Verify counts
	stats := parser.Stats()
	if stats.NodesCreated != 1 {
		t.Errorf("expected 1 node created, got %d", stats.NodesCreated)
	}
	if stats.NodesModified != 1 {
		t.Errorf("expected 1 node modified, got %d", stats.NodesModified)
	}
	if stats.NodesDeleted != 1 {
		t.Errorf("expected 1 node deleted, got %d", stats.NodesDeleted)
	}
	if stats.WaysCreated != 1 {
		t.Errorf("expected 1 way created, got %d", stats.WaysCreated)
	}
	if stats.WaysDeleted != 1 {
		t.Errorf("expected 1 way deleted, got %d", stats.WaysDeleted)
	}
	if stats.RelationsModified != 1 {
		t.Errorf("expected 1 relation modified, got %d", stats.RelationsModified)
	}

	// Verify we got all changes
	if len(allChanges) != 6 {
		t.Errorf("expected 6 changes, got %d", len(allChanges))
	}

	// Verify first node
	if len(allChanges) > 0 {
		change := allChanges[0]
		if change.Action != ActionCreate {
			t.Errorf("expected create action, got %s", change.Action)
		}
		if change.Type != "node" {
			t.Errorf("expected node type, got %s", change.Type)
		}
		if change.Node == nil {
			t.Error("expected node data")
		} else {
			if change.Node.ID != 1 {
				t.Errorf("expected node ID 1, got %d", change.Node.ID)
			}
			if change.Node.Tags["name"] != "Test Node" {
				t.Errorf("expected name 'Test Node', got '%s'", change.Node.Tags["name"])
			}
		}
	}

	// Verify way
	for _, change := range allChanges {
		if change.Type == "way" && change.Action == ActionCreate {
			if change.Way == nil {
				t.Error("expected way data")
			} else {
				if change.Way.ID != 100 {
					t.Errorf("expected way ID 100, got %d", change.Way.ID)
				}
				if len(change.Way.Nodes) != 3 {
					t.Errorf("expected 3 node refs, got %d", len(change.Way.Nodes))
				}
			}
		}
	}

	// Verify relation
	for _, change := range allChanges {
		if change.Type == "relation" && change.Action == ActionModify {
			if change.Relation == nil {
				t.Error("expected relation data")
			} else {
				if change.Relation.ID != 200 {
					t.Errorf("expected relation ID 200, got %d", change.Relation.ID)
				}
				if len(change.Relation.Members) != 2 {
					t.Errorf("expected 2 members, got %d", len(change.Relation.Members))
				}
				if change.Relation.Members[0].Type != "w" {
					t.Errorf("expected member type 'w', got '%s'", change.Relation.Members[0].Type)
				}
			}
		}
	}
}
