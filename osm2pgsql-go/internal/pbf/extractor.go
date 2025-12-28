package pbf

import (
	"context"
	"encoding/json"
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
	"github.com/kevinramage/osm2pgsql-go/internal/parquet"
	"github.com/kevinramage/osm2pgsql-go/internal/wkb"
	"github.com/paulmach/osm"
	"github.com/paulmach/osm/osmpbf"
)

// Stats holds extraction statistics
type Stats struct {
	Nodes     int64
	Ways      int64
	Relations int64
	BytesRead int64
}

// Extractor reads PBF files and writes to Parquet with geometries
type Extractor struct {
	cfg *config.Config

	// Node index for coordinate lookups
	nodeIndex     *nodeindex.MmapIndex
	nodeIndexPath string

	stats Stats
}

// NewExtractor creates a new PBF extractor
func NewExtractor(cfg *config.Config) (*Extractor, error) {
	// Create output directory
	if err := os.MkdirAll(cfg.OutputDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	nodeIndexPath := filepath.Join(cfg.OutputDir, "node_index.bin")

	return &Extractor{
		cfg:           cfg,
		nodeIndexPath: nodeIndexPath,
	}, nil
}

// Close cleans up resources
func (e *Extractor) Close() error {
	if e.nodeIndex != nil {
		e.nodeIndex.Close()
		e.nodeIndex = nil
	}
	// Remove the node index file after we're done
	os.Remove(e.nodeIndexPath)
	return nil
}

// Run executes the two-pass extraction
func (e *Extractor) Run() (*Stats, error) {
	log := logger.Get()

	f, err := os.Open(e.cfg.InputFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Get file size for progress reporting
	fileInfo, err := f.Stat()
	if err != nil {
		return nil, err
	}
	e.stats.BytesRead = fileInfo.Size()

	// Pass 1: Build node index (parallelized)
	log.Info("Pass 1: Building node coordinate index")
	start := time.Now()
	nodeCount, err := e.buildNodeIndexParallel(f)
	if err != nil {
		return nil, err
	}
	e.stats.Nodes = nodeCount
	log.Info("Pass 1 complete", zap.Int64("nodes", nodeCount), zap.Duration("duration", time.Since(start).Round(time.Second)))

	// Seek back to beginning for pass 2
	if _, err := f.Seek(0, 0); err != nil {
		return nil, err
	}

	// Reopen node index for reading
	e.nodeIndex, err = nodeindex.OpenMmapIndex(e.nodeIndexPath)
	if err != nil {
		return nil, err
	}

	// Pass 2: Process ways and relations, building geometries (parallelized)
	log.Info("Pass 2: Building geometries (parallel with WKB)")
	start = time.Now()
	wayCount, relCount, err := e.buildGeometriesParallel(f)
	if err != nil {
		return nil, err
	}
	e.stats.Ways = wayCount
	e.stats.Relations = relCount
	log.Info("Pass 2 complete", zap.Int64("ways", wayCount), zap.Int64("relations", relCount), zap.Duration("duration", time.Since(start).Round(time.Second)))

	return &e.stats, nil
}

// buildNodeIndexParallel performs pass 1 with parallel processing
func (e *Extractor) buildNodeIndexParallel(f *os.File) (int64, error) {
	log := logger.Get()

	// Create mmap index
	idx, err := nodeindex.NewMmapIndex(e.nodeIndexPath)
	if err != nil {
		return 0, err
	}
	defer idx.Close()

	scanner := osmpbf.New(context.Background(), f, runtime.NumCPU())
	defer scanner.Close()

	var count atomic.Int64

	// Progress ticker
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				log.Debug("[extractor.go] Node indexing progress", zap.Int64("nodes", count.Load()))
			}
		}
	}()

	// The osmpbf scanner already decodes in parallel
	// Mmap writes are thread-safe for unique node IDs (each writes to unique offset)
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

// geometryResult holds the result of processing a single way
type geometryResult struct {
	osmID    int64
	osmType  string
	tags     string
	geomWKB  []byte
	geomType int // 0=point, 1=line, 2=polygon
}

// buildGeometriesParallel performs pass 2 with parallel worker goroutines
func (e *Extractor) buildGeometriesParallel(f *os.File) (int64, int64, error) {
	log := logger.Get()
	numWorkers := e.cfg.Workers
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
	}

	// Channels for work distribution
	nodeChan := make(chan *osm.Node, 10000)
	wayChan := make(chan *osm.Way, 10000)
	resultChan := make(chan geometryResult, 10000)

	var nodeCount, wayCount, relCount atomic.Int64
	var wg sync.WaitGroup

	// Start worker goroutines for processing ways
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			encoder := wkb.NewEncoder(1024)
			coords := make([]float64, 0, 2000) // Pre-allocated coordinate buffer

			for way := range wayChan {
				wayCount.Add(1)
				coords = coords[:0] // Reset without reallocating

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

				if !valid || len(coords) < 4 { // Need at least 2 points
					continue
				}

				tags := tagsToJSON(way.Tags)
				isClosed := len(way.Nodes) >= 4 && way.Nodes[0].ID == way.Nodes[len(way.Nodes)-1].ID

				if isClosed && isArea(way.Tags) {
					// Polygon
					wkbBytes := encoder.EncodePolygon(coords)
					resultChan <- geometryResult{
						osmID:    int64(way.ID),
						osmType:  "W",
						tags:     tags,
						geomWKB:  append([]byte(nil), wkbBytes...), // Copy to avoid reuse issues
						geomType: 2,
					}
				} else {
					// LineString
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
		}(i)
	}

	// Start worker for processing nodes (points)
	wg.Add(1)
	go func() {
		defer wg.Done()
		encoder := wkb.NewEncoder(64)

		for node := range nodeChan {
			nodeCount.Add(1)
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

	// Start result writer goroutine
	var writerErr error
	var writerWg sync.WaitGroup
	writerWg.Add(1)
	go func() {
		defer writerWg.Done()

		pointWriter, err := parquet.NewWKBGeometryWriter(filepath.Join(e.cfg.OutputDir, "points.parquet"), e.cfg.BatchSize)
		if err != nil {
			writerErr = err
			return
		}
		defer pointWriter.Close()

		lineWriter, err := parquet.NewWKBGeometryWriter(filepath.Join(e.cfg.OutputDir, "lines.parquet"), e.cfg.BatchSize)
		if err != nil {
			writerErr = err
			return
		}
		defer lineWriter.Close()

		polygonWriter, err := parquet.NewWKBGeometryWriter(filepath.Join(e.cfg.OutputDir, "polygons.parquet"), e.cfg.BatchSize)
		if err != nil {
			writerErr = err
			return
		}
		defer polygonWriter.Close()

		for result := range resultChan {
			switch result.geomType {
			case 0:
				pointWriter.Write(result.osmID, result.osmType, result.tags, result.geomWKB)
			case 1:
				lineWriter.Write(result.osmID, result.osmType, result.tags, result.geomWKB)
			case 2:
				polygonWriter.Write(result.osmID, result.osmType, result.tags, result.geomWKB)
			}
		}
	}()

	// Progress ticker
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				log.Debug("Geometry building progress",
					zap.Int64("nodes", nodeCount.Load()),
					zap.Int64("ways", wayCount.Load()),
					zap.Int64("relations", relCount.Load()))
			}
		}
	}()

	// Read PBF and distribute work
	scanner := osmpbf.New(context.Background(), f, runtime.NumCPU())
	defer scanner.Close()

	for scanner.Scan() {
		obj := scanner.Object()
		switch o := obj.(type) {
		case *osm.Node:
			nodeChan <- o
		case *osm.Way:
			wayChan <- o
		case *osm.Relation:
			relCount.Add(1)
			// TODO: Handle multipolygon relations
		}
	}

	if err := scanner.Err(); err != nil && err != io.EOF {
		close(nodeChan)
		close(wayChan)
		cancel()
		return 0, 0, err
	}

	// Close input channels and wait for workers
	close(nodeChan)
	close(wayChan)
	wg.Wait()

	// Close result channel and wait for writer
	close(resultChan)
	writerWg.Wait()
	cancel()

	if writerErr != nil {
		return 0, 0, writerErr
	}

	return wayCount.Load(), relCount.Load(), nil
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
	// Explicit area tag
	for _, tag := range tags {
		if tag.Key == "area" {
			return tag.Value == "yes"
		}
	}

	// Tags that imply area
	areaKeys := map[string]bool{
		"building": true,
		"landuse":  true,
		"natural":  true,
		"leisure":  true,
		"amenity":  true,
		"shop":     true,
		"tourism":  true,
		"man_made": true,
		"waterway": false, // rivers are lines even if closed
		"highway":  false, // roundabouts are lines
		"barrier":  false,
		"railway":  false,
	}

	for _, tag := range tags {
		if isArea, exists := areaKeys[tag.Key]; exists {
			return isArea
		}
	}

	return false
}
