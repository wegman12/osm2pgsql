package flex

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/kevinramage/osm2pgsql-go/internal/config"
	"github.com/kevinramage/osm2pgsql-go/internal/logger"
	"github.com/kevinramage/osm2pgsql-go/internal/nodeindex"
	"github.com/paulmach/osm"
	"github.com/paulmach/osm/osmpbf"
)

// FlexExtractor processes PBF files using Lua flex styles
type FlexExtractor struct {
	cfg           *config.Config
	luaFile       string
	channelBuffer int

	// Node index for coordinate lookups
	nodeIndex     *nodeindex.MmapIndex
	nodeIndexPath string

	// Way geometry cache for relation processing
	wayCache     sync.Map
	wayCacheSize atomic.Int64

	stats FlexExtractStats
}

// FlexExtractStats holds extraction statistics
type FlexExtractStats struct {
	BytesRead int64
	Nodes     int64
	Ways      int64
	Relations int64
}

// FlexStreams holds output channels from flex extraction
type FlexStreams struct {
	Objects <-chan *OSMObject
	Errors  <-chan error
}

// NewFlexExtractor creates a new Flex-based PBF extractor
func NewFlexExtractor(cfg *config.Config, luaFile string, channelBuffer int) (*FlexExtractor, error) {
	// Determine node index path
	var nodeIndexPath string
	if cfg.FlatNodesFile != "" {
		nodeIndexPath = cfg.FlatNodesFile
		if err := os.MkdirAll(filepath.Dir(nodeIndexPath), 0755); err != nil {
			return nil, fmt.Errorf("failed to create flat nodes directory: %w", err)
		}
	} else {
		if err := os.MkdirAll(cfg.OutputDir, 0755); err != nil {
			return nil, err
		}
		nodeIndexPath = filepath.Join(cfg.OutputDir, "node_index.bin")
	}

	if channelBuffer <= 0 {
		channelBuffer = 50000
	}

	return &FlexExtractor{
		cfg:           cfg,
		luaFile:       luaFile,
		channelBuffer: channelBuffer,
		nodeIndexPath: nodeIndexPath,
	}, nil
}

// Close cleans up resources
func (e *FlexExtractor) Close() error {
	if e.nodeIndex != nil {
		e.nodeIndex.Close()
		e.nodeIndex = nil
	}
	// Remove the node index file after we're done
	if e.cfg.FlatNodesFile == "" {
		os.Remove(e.nodeIndexPath)
	}
	return nil
}

// Stats returns extraction statistics
func (e *FlexExtractor) Stats() *FlexExtractStats {
	return &e.stats
}

// Run executes the two-pass extraction and returns OSM object stream
func (e *FlexExtractor) Run(ctx context.Context) (*FlexStreams, error) {
	log := logger.Get()

	f, err := os.Open(e.cfg.InputFile)
	if err != nil {
		return nil, err
	}

	// Get file size for progress reporting
	fileInfo, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	e.stats.BytesRead = fileInfo.Size()

	// Pass 1: Build node index
	log.Info("Pass 1: Building node coordinate index")
	start := time.Now()
	nodeCount, err := e.buildNodeIndex(ctx, f)
	if err != nil {
		f.Close()
		return nil, err
	}
	e.stats.Nodes = nodeCount
	log.Info("Pass 1 complete", zap.Int64("nodes", nodeCount), zap.Duration("duration", time.Since(start).Round(time.Second)))

	// Seek back to beginning for pass 2
	if _, err := f.Seek(0, 0); err != nil {
		f.Close()
		return nil, err
	}

	// Pass 2: Stream OSM objects with coordinates resolved
	log.Info("Pass 2: Streaming OSM objects with Flex processing")
	streams := e.streamObjects(ctx, f)

	return streams, nil
}

// buildNodeIndex performs pass 1: indexing all node coordinates
func (e *FlexExtractor) buildNodeIndex(ctx context.Context, f *os.File) (int64, error) {
	log := logger.Get()

	idx, err := nodeindex.NewMmapIndex(e.nodeIndexPath)
	if err != nil {
		return 0, err
	}
	e.nodeIndex = idx

	scanner := osmpbf.New(ctx, f, runtime.NumCPU())
	defer scanner.Close()

	var count atomic.Int64
	bbox := e.cfg.BBox

	// Progress ticker
	tickerCtx, cancelTicker := context.WithCancel(ctx)
	defer cancelTicker()
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-tickerCtx.Done():
				return
			case <-ticker.C:
				bytesScanned := scanner.FullyScannedBytes()
				pct := float64(bytesScanned) / float64(e.stats.BytesRead) * 100
				log.Debug("Node indexing progress",
					zap.Int64("nodes", count.Load()),
					zap.String("percent", fmt.Sprintf("%.1f%%", pct)))
			}
		}
	}()

	for scanner.Scan() {
		obj := scanner.Object()
		switch n := obj.(type) {
		case *osm.Node:
			idx.Put(int64(n.ID), n.Lat, n.Lon)
			if bbox == nil || !bbox.IsSet || bbox.Contains(n.Lat, n.Lon) {
				count.Add(1)
			}
		case *osm.Way:
			// Stop at first way - we only need nodes in pass 1
			return count.Load(), nil
		}
	}

	if err := scanner.Err(); err != nil && err != io.EOF {
		return 0, err
	}

	return count.Load(), nil
}

// streamObjects performs pass 2: streaming OSM objects
func (e *FlexExtractor) streamObjects(ctx context.Context, f *os.File) *FlexStreams {
	log := logger.Get()

	objChan := make(chan *OSMObject, e.channelBuffer)
	errChan := make(chan error, 1)

	var wayCount, relCount atomic.Int64
	bbox := e.cfg.BBox

	scanner := osmpbf.New(ctx, f, runtime.NumCPU())

	// Progress ticker
	tickerCtx, cancelTicker := context.WithCancel(ctx)

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-tickerCtx.Done():
				return
			case <-ticker.C:
				bytesScanned := scanner.FullyScannedBytes()
				pct := float64(bytesScanned) / float64(e.stats.BytesRead) * 100
				log.Debug("Flex processing progress",
					zap.Int64("ways", wayCount.Load()),
					zap.Int64("relations", relCount.Load()),
					zap.String("percent", fmt.Sprintf("%.1f%%", pct)))
			}
		}
	}()

	go func() {
		defer f.Close()
		defer scanner.Close()
		defer cancelTicker()
		defer close(objChan)
		defer close(errChan)

		for scanner.Scan() {
			select {
			case <-ctx.Done():
				return
			default:
			}

			obj := scanner.Object()
			switch o := obj.(type) {
			case *osm.Node:
				// Skip nodes outside bbox
				if bbox != nil && bbox.IsSet && !bbox.Contains(o.Lat, o.Lon) {
					continue
				}

				osmObj := &OSMObject{
					ID:        int64(o.ID),
					Type:      "node",
					Version:   o.Version,
					Timestamp: o.Timestamp,
					Changeset: int64(o.ChangesetID),
					UID:       int(o.UserID),
					User:      o.User,
					Tags:      tagsToMap(o.Tags),
					Lat:       o.Lat,
					Lon:       o.Lon,
				}

				select {
				case objChan <- osmObj:
				case <-ctx.Done():
					return
				}

			case *osm.Way:
				wayCount.Add(1)

				// Build coordinate array from node references
				coords := make([]float64, 0, len(o.Nodes)*2)
				nodeRefs := make([]int64, 0, len(o.Nodes))
				valid := true
				inBBox := bbox == nil || !bbox.IsSet

				for _, nodeRef := range o.Nodes {
					lat, lon, ok := e.nodeIndex.Get(int64(nodeRef.ID))
					if !ok {
						valid = false
						break
					}
					coords = append(coords, lon, lat)
					nodeRefs = append(nodeRefs, int64(nodeRef.ID))
					if !inBBox && bbox.Contains(lat, lon) {
						inBBox = true
					}
				}

				if !valid || len(coords) < 4 || !inBBox {
					continue
				}

				// Cache way coordinates for relation processing
				e.wayCache.Store(int64(o.ID), coords)
				e.wayCacheSize.Add(1)

				isClosed := len(o.Nodes) >= 4 && o.Nodes[0].ID == o.Nodes[len(o.Nodes)-1].ID

				osmObj := &OSMObject{
					ID:        int64(o.ID),
					Type:      "way",
					Version:   o.Version,
					Timestamp: o.Timestamp,
					Changeset: int64(o.ChangesetID),
					UID:       int(o.UserID),
					User:      o.User,
					Tags:      tagsToMap(o.Tags),
					NodeRefs:  nodeRefs,
					IsClosed:  isClosed,
					Coords:    coords,
				}

				select {
				case objChan <- osmObj:
				case <-ctx.Done():
					return
				}

			case *osm.Relation:
				relCount.Add(1)

				members := make([]RelationMember, len(o.Members))
				for i, m := range o.Members {
					var memberType string
					switch m.Type {
					case osm.TypeNode:
						memberType = "n"
					case osm.TypeWay:
						memberType = "w"
					case osm.TypeRelation:
						memberType = "r"
					}
					members[i] = RelationMember{
						Type: memberType,
						Ref:  int64(m.Ref),
						Role: m.Role,
					}
				}

				osmObj := &OSMObject{
					ID:        int64(o.ID),
					Type:      "relation",
					Version:   o.Version,
					Timestamp: o.Timestamp,
					Changeset: int64(o.ChangesetID),
					UID:       int(o.UserID),
					User:      o.User,
					Tags:      tagsToMap(o.Tags),
					Members:   members,
				}

				// For multipolygon relations, try to build geometry
				if isMultipolygon(o.Tags) {
					coords := e.assembleRelationGeometry(o)
					if coords != nil {
						osmObj.Coords = coords
					}
				}

				select {
				case objChan <- osmObj:
				case <-ctx.Done():
					return
				}
			}
		}

		if err := scanner.Err(); err != nil && err != io.EOF {
			select {
			case errChan <- err:
			default:
			}
		}

		// Update stats
		e.stats.Ways = wayCount.Load()
		e.stats.Relations = relCount.Load()

		// Clear way cache
		e.wayCache = sync.Map{}
		e.wayCacheSize.Store(0)

		log.Info("Pass 2 complete",
			zap.Int64("ways", wayCount.Load()),
			zap.Int64("relations", relCount.Load()))
	}()

	return &FlexStreams{
		Objects: objChan,
		Errors:  errChan,
	}
}

// assembleRelationGeometry builds coordinates for multipolygon relations
func (e *FlexExtractor) assembleRelationGeometry(rel *osm.Relation) []float64 {
	// Collect outer way coordinates
	var allCoords []float64

	for _, member := range rel.Members {
		if member.Type != osm.TypeWay {
			continue
		}
		if member.Role != "outer" && member.Role != "" {
			continue
		}

		if coords, ok := e.wayCache.Load(int64(member.Ref)); ok {
			allCoords = append(allCoords, coords.([]float64)...)
		}
	}

	return allCoords
}

// tagsToMap converts OSM tags to a map
func tagsToMap(tags osm.Tags) map[string]string {
	m := make(map[string]string, len(tags))
	for _, tag := range tags {
		m[tag.Key] = tag.Value
	}
	return m
}

// isMultipolygon checks if relation is a multipolygon
func isMultipolygon(tags osm.Tags) bool {
	for _, tag := range tags {
		if tag.Key == "type" {
			return tag.Value == "multipolygon" || tag.Value == "boundary"
		}
	}
	return false
}
