package osc

import (
	"compress/gzip"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/kevinramage/osm2pgsql-go/internal/middle"
)

// Parser parses OSC (OSM Change) files
type Parser struct {
	stats Stats
}

// NewParser creates a new OSC parser
func NewParser() *Parser {
	return &Parser{}
}

// Stats returns parsing statistics
func (p *Parser) Stats() Stats {
	return p.stats
}

// ParseFile parses an OSC file and streams changes to a channel
// Supports both plain XML and gzip-compressed files
func (p *Parser) ParseFile(ctx context.Context, filename string) (<-chan Change, <-chan error) {
	changes := make(chan Change, 1000)
	errChan := make(chan error, 1)

	go func() {
		defer close(changes)
		defer close(errChan)

		f, err := os.Open(filename)
		if err != nil {
			errChan <- fmt.Errorf("failed to open OSC file: %w", err)
			return
		}
		defer f.Close()

		var reader io.Reader = f

		// Check if gzip compressed
		if strings.HasSuffix(filename, ".gz") {
			gzReader, err := gzip.NewReader(f)
			if err != nil {
				errChan <- fmt.Errorf("failed to create gzip reader: %w", err)
				return
			}
			defer gzReader.Close()
			reader = gzReader
		}

		if err := p.parse(ctx, reader, changes); err != nil {
			errChan <- err
		}
	}()

	return changes, errChan
}

// ParseReader parses OSC data from a reader
func (p *Parser) ParseReader(ctx context.Context, reader io.Reader) (<-chan Change, <-chan error) {
	changes := make(chan Change, 1000)
	errChan := make(chan error, 1)

	go func() {
		defer close(changes)
		defer close(errChan)

		if err := p.parse(ctx, reader, changes); err != nil {
			errChan <- err
		}
	}()

	return changes, errChan
}

// parse performs the actual XML parsing
func (p *Parser) parse(ctx context.Context, reader io.Reader, changes chan<- Change) error {
	decoder := xml.NewDecoder(reader)
	var currentAction Action

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("XML parse error: %w", err)
		}

		switch se := token.(type) {
		case xml.StartElement:
			switch se.Name.Local {
			case "create":
				currentAction = ActionCreate
			case "modify":
				currentAction = ActionModify
			case "delete":
				currentAction = ActionDelete
			case "node":
				node, err := p.parseNode(decoder, se, currentAction)
				if err != nil {
					return err
				}
				if node != nil {
					change := Change{
						Action: currentAction,
						Type:   "node",
						Node:   node,
					}
					select {
					case changes <- change:
						p.updateStats(currentAction, "node")
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			case "way":
				way, err := p.parseWay(decoder, se, currentAction)
				if err != nil {
					return err
				}
				if way != nil {
					change := Change{
						Action: currentAction,
						Type:   "way",
						Way:    way,
					}
					select {
					case changes <- change:
						p.updateStats(currentAction, "way")
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			case "relation":
				rel, err := p.parseRelation(decoder, se, currentAction)
				if err != nil {
					return err
				}
				if rel != nil {
					change := Change{
						Action:   currentAction,
						Type:     "relation",
						Relation: rel,
					}
					select {
					case changes <- change:
						p.updateStats(currentAction, "relation")
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			}
		}
	}

	return nil
}

// parseNode parses a node element
func (p *Parser) parseNode(decoder *xml.Decoder, start xml.StartElement, action Action) (*middle.RawNode, error) {
	node := &middle.RawNode{
		Tags: make(map[string]string),
	}

	// Parse attributes
	for _, attr := range start.Attr {
		switch attr.Name.Local {
		case "id":
			id, _ := strconv.ParseInt(attr.Value, 10, 64)
			node.ID = id
		case "lat":
			lat, _ := strconv.ParseFloat(attr.Value, 64)
			node.Lat = middle.ScaleCoord(lat)
		case "lon":
			lon, _ := strconv.ParseFloat(attr.Value, 64)
			node.Lon = middle.ScaleCoord(lon)
		case "version":
			v, _ := strconv.ParseInt(attr.Value, 10, 32)
			node.Version = int32(v)
		case "changeset":
			cs, _ := strconv.ParseInt(attr.Value, 10, 64)
			node.Changeset = cs
		case "timestamp":
			t, _ := time.Parse(time.RFC3339, attr.Value)
			node.Timestamp = t
		case "user":
			node.User = attr.Value
		case "uid":
			uid, _ := strconv.ParseInt(attr.Value, 10, 32)
			node.UID = int32(uid)
		}
	}

	// For delete actions, we only need the ID
	if action == ActionDelete {
		// Skip to end of node element
		for {
			token, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			if ee, ok := token.(xml.EndElement); ok && ee.Name.Local == "node" {
				break
			}
		}
		return node, nil
	}

	// Parse child elements (tags)
	for {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}

		switch se := token.(type) {
		case xml.StartElement:
			if se.Name.Local == "tag" {
				var k, v string
				for _, attr := range se.Attr {
					switch attr.Name.Local {
					case "k":
						k = attr.Value
					case "v":
						v = attr.Value
					}
				}
				if k != "" {
					node.Tags[k] = v
				}
			}
		case xml.EndElement:
			if se.Name.Local == "node" {
				return node, nil
			}
		}
	}
}

// parseWay parses a way element
func (p *Parser) parseWay(decoder *xml.Decoder, start xml.StartElement, action Action) (*middle.RawWay, error) {
	way := &middle.RawWay{
		Nodes: make([]int64, 0, 100),
		Tags:  make(map[string]string),
	}

	// Parse attributes
	for _, attr := range start.Attr {
		switch attr.Name.Local {
		case "id":
			id, _ := strconv.ParseInt(attr.Value, 10, 64)
			way.ID = id
		case "version":
			v, _ := strconv.ParseInt(attr.Value, 10, 32)
			way.Version = int32(v)
		case "changeset":
			cs, _ := strconv.ParseInt(attr.Value, 10, 64)
			way.Changeset = cs
		case "timestamp":
			t, _ := time.Parse(time.RFC3339, attr.Value)
			way.Timestamp = t
		case "user":
			way.User = attr.Value
		case "uid":
			uid, _ := strconv.ParseInt(attr.Value, 10, 32)
			way.UID = int32(uid)
		}
	}

	// For delete actions, we only need the ID
	if action == ActionDelete {
		for {
			token, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			if ee, ok := token.(xml.EndElement); ok && ee.Name.Local == "way" {
				break
			}
		}
		return way, nil
	}

	// Parse child elements (nd refs and tags)
	for {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}

		switch se := token.(type) {
		case xml.StartElement:
			switch se.Name.Local {
			case "nd":
				for _, attr := range se.Attr {
					if attr.Name.Local == "ref" {
						ref, _ := strconv.ParseInt(attr.Value, 10, 64)
						way.Nodes = append(way.Nodes, ref)
					}
				}
			case "tag":
				var k, v string
				for _, attr := range se.Attr {
					switch attr.Name.Local {
					case "k":
						k = attr.Value
					case "v":
						v = attr.Value
					}
				}
				if k != "" {
					way.Tags[k] = v
				}
			}
		case xml.EndElement:
			if se.Name.Local == "way" {
				return way, nil
			}
		}
	}
}

// parseRelation parses a relation element
func (p *Parser) parseRelation(decoder *xml.Decoder, start xml.StartElement, action Action) (*middle.RawRelation, error) {
	rel := &middle.RawRelation{
		Members: make([]middle.RelationMember, 0, 10),
		Tags:    make(map[string]string),
	}

	// Parse attributes
	for _, attr := range start.Attr {
		switch attr.Name.Local {
		case "id":
			id, _ := strconv.ParseInt(attr.Value, 10, 64)
			rel.ID = id
		case "version":
			v, _ := strconv.ParseInt(attr.Value, 10, 32)
			rel.Version = int32(v)
		case "changeset":
			cs, _ := strconv.ParseInt(attr.Value, 10, 64)
			rel.Changeset = cs
		case "timestamp":
			t, _ := time.Parse(time.RFC3339, attr.Value)
			rel.Timestamp = t
		case "user":
			rel.User = attr.Value
		case "uid":
			uid, _ := strconv.ParseInt(attr.Value, 10, 32)
			rel.UID = int32(uid)
		}
	}

	// For delete actions, we only need the ID
	if action == ActionDelete {
		for {
			token, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			if ee, ok := token.(xml.EndElement); ok && ee.Name.Local == "relation" {
				break
			}
		}
		return rel, nil
	}

	// Parse child elements (members and tags)
	for {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}

		switch se := token.(type) {
		case xml.StartElement:
			switch se.Name.Local {
			case "member":
				member := middle.RelationMember{}
				for _, attr := range se.Attr {
					switch attr.Name.Local {
					case "type":
						// Convert "node"/"way"/"relation" to "n"/"w"/"r"
						switch attr.Value {
						case "node":
							member.Type = "n"
						case "way":
							member.Type = "w"
						case "relation":
							member.Type = "r"
						default:
							member.Type = attr.Value
						}
					case "ref":
						ref, _ := strconv.ParseInt(attr.Value, 10, 64)
						member.Ref = ref
					case "role":
						member.Role = attr.Value
					}
				}
				rel.Members = append(rel.Members, member)
			case "tag":
				var k, v string
				for _, attr := range se.Attr {
					switch attr.Name.Local {
					case "k":
						k = attr.Value
					case "v":
						v = attr.Value
					}
				}
				if k != "" {
					rel.Tags[k] = v
				}
			}
		case xml.EndElement:
			if se.Name.Local == "relation" {
				return rel, nil
			}
		}
	}
}

// updateStats updates parsing statistics
func (p *Parser) updateStats(action Action, objType string) {
	switch objType {
	case "node":
		switch action {
		case ActionCreate:
			p.stats.NodesCreated++
		case ActionModify:
			p.stats.NodesModified++
		case ActionDelete:
			p.stats.NodesDeleted++
		}
	case "way":
		switch action {
		case ActionCreate:
			p.stats.WaysCreated++
		case ActionModify:
			p.stats.WaysModified++
		case ActionDelete:
			p.stats.WaysDeleted++
		}
	case "relation":
		switch action {
		case ActionCreate:
			p.stats.RelationsCreated++
		case ActionModify:
			p.stats.RelationsModified++
		case ActionDelete:
			p.stats.RelationsDeleted++
		}
	}
}
