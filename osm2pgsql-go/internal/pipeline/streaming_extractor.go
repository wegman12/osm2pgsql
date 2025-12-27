package pipeline

import (
	"context"
	"encoding/json"
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
	"github.com/kevinramage/osm2pgsql-go/internal/wkb"
	"github.com/paulmach/osm"
	"github.com/paulmach/osm/osmpbf"
)

// StreamingExtractor reads PBF files and streams geometries to channels
type StreamingExtractor struct {
	cfg           *config.Config
	channelBuffer int

	// Node index for coordinate lookups
	nodeIndex     *nodeindex.MmapIndex
	nodeIndexPath string

	stats ExtractStats
}

// NewStreamingExtractor creates a new streaming PBF extractor
func NewStreamingExtractor(cfg *config.Config, channelBuffer int) (*StreamingExtractor, error) {
	// Create output directory for node index
	if err := os.MkdirAll(cfg.OutputDir, 0755); err != nil {
		return nil, err
	}

	nodeIndexPath := filepath.Join(cfg.OutputDir, "node_index.bin")

	if channelBuffer <= 0 {
		channelBuffer = 50000
	}

	return &StreamingExtractor{
		cfg:           cfg,
		channelBuffer: channelBuffer,
		nodeIndexPath: nodeIndexPath,
	}, nil
}

// Close cleans up resources
func (e *StreamingExtractor) Close() error {
	if e.nodeIndex != nil {
		e.nodeIndex.Close()
		e.nodeIndex = nil
	}
	// Remove the node index file after we're done
	os.Remove(e.nodeIndexPath)
	return nil
}

// Stats returns extraction statistics
func (e *StreamingExtractor) Stats() *ExtractStats {
	return &e.stats
}

// Run executes the two-pass extraction and returns geometry streams
// Pass 1 runs synchronously (node indexing must complete first)
// Pass 2 runs in the background, streaming results to the returned channels
func (e *StreamingExtractor) Run(ctx context.Context) (*GeometryStreams, error) {
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

	// Pass 1: Build node index (must complete before Pass 2)
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

	// Reopen node index for reading
	e.nodeIndex, err = nodeindex.OpenMmapIndex(e.nodeIndexPath)
	if err != nil {
		f.Close()
		return nil, err
	}

	// Pass 2: Stream geometries to channels (runs in background)
	log.Info("Pass 2: Building geometries (streaming to loaders)")
	streams := e.buildGeometriesStreaming(ctx, f)

	return streams, nil
}

// buildNodeIndex performs pass 1: indexing all node coordinates
func (e *StreamingExtractor) buildNodeIndex(ctx context.Context, f *os.File) (int64, error) {
	log := logger.Get()

	// Create mmap index
	idx, err := nodeindex.NewMmapIndex(e.nodeIndexPath)
	if err != nil {
		return 0, err
	}
	defer idx.Close()

	scanner := osmpbf.New(ctx, f, runtime.NumCPU())
	defer scanner.Close()

	var count atomic.Int64

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
				log.Debug("Node indexing progress", zap.Int64("nodes", count.Load()))
			}
		}
	}()

	for scanner.Scan() {
		obj := scanner.Object()
		switch n := obj.(type) {
		case *osm.Node:
			idx.Put(int64(n.ID), n.Lat, n.Lon)
			count.Add(1)
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

// buildGeometriesStreaming performs pass 2: streaming geometries to channels
func (e *StreamingExtractor) buildGeometriesStreaming(ctx context.Context, f *os.File) *GeometryStreams {
	log := logger.Get()
	numWorkers := e.cfg.Workers
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
	}

	// Output channels with configurable buffer
	pointsChan := make(chan GeometryRecord, e.channelBuffer)
	linesChan := make(chan GeometryRecord, e.channelBuffer)
	polygonsChan := make(chan GeometryRecord, e.channelBuffer)
	errChan := make(chan error, 1)

	// Internal work distribution channels
	nodeChan := make(chan *osm.Node, 10000)
	wayChan := make(chan *osm.Way, 10000)
	resultChan := make(chan geometryResult, 10000)

	var wayCount, relCount atomic.Int64
	var wg sync.WaitGroup

	// Start worker goroutines for processing ways
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			encoder := wkb.NewEncoder(1024)
			coords := make([]float64, 0, 2000)

			for way := range wayChan {
				wayCount.Add(1)
				coords = coords[:0]

				// Build coordinate array from node references
				valid := true
				for _, nodeRef := range way.Nodes {
					lat, lon, ok := e.nodeIndex.Get(int64(nodeRef.ID))
					if !ok {
						valid = false
						break
					}
					coords = append(coords, lon, lat)
				}

				if !valid || len(coords) < 4 {
					continue
				}

				tags := tagsToJSON(way.Tags)
				isClosed := len(way.Nodes) >= 4 && way.Nodes[0].ID == way.Nodes[len(way.Nodes)-1].ID

				if isClosed && isArea(way.Tags) {
					wkbBytes := encoder.EncodePolygon(coords)
					resultChan <- geometryResult{
						osmID:    int64(way.ID),
						osmType:  "W",
						tags:     tags,
						geomWKB:  append([]byte(nil), wkbBytes...),
						geomType: 2,
					}
				} else {
					wkbBytes := encoder.EncodeLineString(coords)
					resultChan <- geometryResult{
						osmID:    int64(way.ID),
						osmType:  "W",
						tags:     tags,
						geomWKB:  append([]byte(nil), wkbBytes...),
						geomType: 1,
					}
				}
			}
		}()
	}

	// Start worker for processing nodes (points)
	wg.Add(1)
	go func() {
		defer wg.Done()
		encoder := wkb.NewEncoder(64)

		for node := range nodeChan {
			if len(node.Tags) > 0 && hasMeaningfulTags(node.Tags) {
				wkbBytes := encoder.EncodePoint(node.Lon, node.Lat)
				resultChan <- geometryResult{
					osmID:    int64(node.ID),
					osmType:  "N",
					tags:     tagsToJSON(node.Tags),
					geomWKB:  append([]byte(nil), wkbBytes...),
					geomType: 0,
				}
			}
		}
	}()

	// Start router goroutine: routes results to typed output channels
	go func() {
		defer close(pointsChan)
		defer close(linesChan)
		defer close(polygonsChan)

		for result := range resultChan {
			record := GeometryRecord{
				OsmID:   result.osmID,
				OsmType: result.osmType,
				Tags:    result.tags,
				GeomWKB: result.geomWKB,
			}
			switch result.geomType {
			case 0:
				select {
				case pointsChan <- record:
				case <-ctx.Done():
					return
				}
			case 1:
				select {
				case linesChan <- record:
				case <-ctx.Done():
					return
				}
			case 2:
				select {
				case polygonsChan <- record:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

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
				log.Debug("Geometry building progress",
					zap.Int64("ways", wayCount.Load()),
					zap.Int64("relations", relCount.Load()))
			}
		}
	}()

	// Start PBF scanner goroutine
	go func() {
		defer f.Close()
		defer cancelTicker()
		defer close(errChan) // Always close error channel when done

		scanner := osmpbf.New(ctx, f, runtime.NumCPU())
		defer scanner.Close()

		for scanner.Scan() {
			obj := scanner.Object()
			switch o := obj.(type) {
			case *osm.Node:
				select {
				case nodeChan <- o:
				case <-ctx.Done():
					close(nodeChan)
					close(wayChan)
					return
				}
			case *osm.Way:
				select {
				case wayChan <- o:
				case <-ctx.Done():
					close(nodeChan)
					close(wayChan)
					return
				}
			case *osm.Relation:
				relCount.Add(1)
				// TODO: Handle multipolygon relations
			}
		}

		if err := scanner.Err(); err != nil && err != io.EOF {
			select {
			case errChan <- err:
			default:
			}
		}

		// Close input channels and wait for workers
		close(nodeChan)
		close(wayChan)
		wg.Wait()

		// Close result channel (triggers router goroutine to close output channels)
		close(resultChan)

		// Update stats
		e.stats.Ways = wayCount.Load()
		e.stats.Relations = relCount.Load()

		log.Info("Pass 2 complete",
			zap.Int64("ways", wayCount.Load()),
			zap.Int64("relations", relCount.Load()))
	}()

	return &GeometryStreams{
		Points:   pointsChan,
		Lines:    linesChan,
		Polygons: polygonsChan,
		Errors:   errChan,
	}
}

// geometryResult holds the result of processing a single geometry (internal)
type geometryResult struct {
	osmID    int64
	osmType  string
	tags     string
	geomWKB  []byte
	geomType int // 0=point, 1=line, 2=polygon
}

// hasMeaningfulTags checks if tags contain more than just metadata
func hasMeaningfulTags(tags osm.Tags) bool {
	dominated := map[string]bool{
		"created_by": true,
		"source":     true,
		"note":       true,
		"fixme":      true,
		"FIXME":      true,
	}

	for _, tag := range tags {
		if !dominated[tag.Key] {
			return true
		}
	}
	return false
}

// tagsToJSON converts OSM tags to JSON string
func tagsToJSON(tags osm.Tags) string {
	if len(tags) == 0 {
		return "{}"
	}
	m := make(map[string]string, len(tags))
	for _, tag := range tags {
		m[tag.Key] = tag.Value
	}
	b, _ := json.Marshal(m)
	return string(b)
}

// isArea checks if a closed way should be treated as a polygon
func isArea(tags osm.Tags) bool {
	for _, tag := range tags {
		if tag.Key == "area" {
			return tag.Value == "yes"
		}
	}

	areaKeys := map[string]bool{
		"building":    true,
		"landuse":     true,
		"natural":     true,
		"leisure":     true,
		"amenity":     true,
		"shop":        true,
		"tourism":     true,
		"man_made":    true,
		"waterway":    false,
		"highway":     false,
		"barrier":     false,
		"railway":     false,
	}

	for _, tag := range tags {
		if isArea, exists := areaKeys[tag.Key]; exists {
			return isArea
		}
	}

	return false
}
