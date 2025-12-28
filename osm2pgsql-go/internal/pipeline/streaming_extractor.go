package pipeline

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
	"github.com/kevinramage/osm2pgsql-go/internal/proj"
	"github.com/kevinramage/osm2pgsql-go/internal/style"
	"github.com/kevinramage/osm2pgsql-go/internal/wkb"
	"github.com/paulmach/osm"
	"github.com/paulmach/osm/osmpbf"
)

// StreamingExtractor reads PBF files and streams geometries to channels
type StreamingExtractor struct {
	cfg           *config.Config
	channelBuffer int

	// Node index for coordinate lookups
	nodeIndex      *nodeindex.MmapIndex
	nodeIndexPath  string
	keepNodeIndex  bool // If true, don't delete node index on close (for --flat-nodes)

	// Way geometry cache for relation processing
	// Maps way ID to flat coordinate array [lon1, lat1, lon2, lat2, ...]
	wayCache     sync.Map
	wayCacheSize atomic.Int64

	// Style filters for tag-based filtering
	pointFilter   *style.Filter
	lineFilter    *style.Filter
	polygonFilter *style.Filter

	stats ExtractStats
}

// NewStreamingExtractor creates a new streaming PBF extractor
func NewStreamingExtractor(cfg *config.Config, channelBuffer int) (*StreamingExtractor, error) {
	// Determine node index path
	var nodeIndexPath string
	if cfg.FlatNodesFile != "" {
		// Use specified flat nodes file path
		nodeIndexPath = cfg.FlatNodesFile
		// Ensure parent directory exists
		if err := os.MkdirAll(filepath.Dir(nodeIndexPath), 0755); err != nil {
			return nil, fmt.Errorf("failed to create flat nodes directory: %w", err)
		}
	} else {
		// Use default path in output directory
		if err := os.MkdirAll(cfg.OutputDir, 0755); err != nil {
			return nil, err
		}
		nodeIndexPath = filepath.Join(cfg.OutputDir, "node_index.bin")
	}

	if channelBuffer <= 0 {
		channelBuffer = 50000
	}

	// Load style configuration if provided
	var styleCfg *style.Config
	if cfg.StyleFile != "" {
		var err error
		styleCfg, err = style.LoadConfig(cfg.StyleFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load style config: %w", err)
		}
	} else {
		styleCfg = style.DefaultConfig()
	}

	// Create filters
	var pointFilter, lineFilter, polygonFilter *style.Filter
	if styleCfg.Points != nil {
		pointFilter = style.NewFilter(styleCfg.Points)
	} else {
		pointFilter = style.NewFilter(nil)
	}
	if styleCfg.Lines != nil {
		lineFilter = style.NewFilter(styleCfg.Lines)
	} else {
		lineFilter = style.NewFilter(nil)
	}
	if styleCfg.Polygons != nil {
		polygonFilter = style.NewFilter(styleCfg.Polygons)
	} else {
		polygonFilter = style.NewFilter(nil)
	}

	return &StreamingExtractor{
		cfg:           cfg,
		channelBuffer: channelBuffer,
		nodeIndexPath: nodeIndexPath,
		keepNodeIndex: cfg.FlatNodesFile != "", // Keep if user specified a path
		pointFilter:   pointFilter,
		lineFilter:    lineFilter,
		polygonFilter: polygonFilter,
	}, nil
}

// Close cleans up resources
func (e *StreamingExtractor) Close() error {
	if e.nodeIndex != nil {
		e.nodeIndex.Close()
		e.nodeIndex = nil
	}
	// Remove the node index file after we're done (unless user specified --flat-nodes to keep it)
	if !e.keepNodeIndex {
		os.Remove(e.nodeIndexPath)
	}
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

	// Node index is already open from Pass 1 (kept open to avoid filesystem sync issues)

	// Pass 2: Stream geometries to channels (runs in background)
	log.Info("Pass 2: Building geometries (streaming to loaders)")
	streams := e.buildGeometriesStreaming(ctx, f)

	return streams, nil
}

// buildNodeIndex performs pass 1: indexing all node coordinates
func (e *StreamingExtractor) buildNodeIndex(ctx context.Context, f *os.File) (int64, error) {
	log := logger.Get()

	// Create mmap index and store it in the extractor for use in Pass 2
	// We keep it open instead of closing/reopening to avoid filesystem sync issues
	idx, err := nodeindex.NewMmapIndex(e.nodeIndexPath)
	if err != nil {
		return 0, err
	}
	// Store the index for Pass 2 - don't close it here
	e.nodeIndex = idx

	scanner := osmpbf.New(ctx, f, runtime.NumCPU())
	defer scanner.Close()

	var count atomic.Int64

	// Progress tracker using file size
	progress := NewProgressTracker(e.stats.BytesRead, "node indexing")

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
				p := progress.Calculate(count.Load(), bytesScanned)
				log.Debug("Node indexing progress",
					zap.Int64("nodes", count.Load()),
					zap.String("processed", FormatBytes(bytesScanned)),
					zap.String("total", FormatBytes(e.stats.BytesRead)),
					zap.String("percent", fmt.Sprintf("%.1f%%", p.Percentage)),
					zap.String("throughput", FormatThroughput(p.Throughput)),
					zap.String("eta", FormatETA(p.ETA)))
			}
		}
	}()

	// Get bbox filter (may be nil)
	bbox := e.cfg.BBox

	for scanner.Scan() {
		obj := scanner.Object()
		switch n := obj.(type) {
		case *osm.Node:
			// Always index nodes (needed for way geometry building)
			// but track if node is in bbox for filtering later
			idx.Put(int64(n.ID), n.Lat, n.Lon)
			// Only count nodes in bbox for stats
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
	relationChan := make(chan *osm.Relation, 1000)
	resultChan := make(chan geometryResult, 10000)

	var wayCount, relCount atomic.Int64
	var wg sync.WaitGroup

	// Get bbox filter (may be nil)
	bbox := e.cfg.BBox

	// Create transformer for projection
	transformer, _ := proj.NewTransformer(proj.SRID4326, e.cfg.Projection)

	// Start worker goroutines for processing ways
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			encoder := wkb.NewEncoderWithSRID(1024, e.cfg.Projection)
			coords := make([]float64, 0, 2000)

			for way := range wayChan {
				wayCount.Add(1)
				coords = coords[:0]

				// Build coordinate array from node references
				valid := true
				inBBox := bbox == nil || !bbox.IsSet // If no bbox, all are "in"
				for _, nodeRef := range way.Nodes {
					lat, lon, ok := e.nodeIndex.Get(int64(nodeRef.ID))
					if !ok {
						valid = false
						break
					}
					coords = append(coords, lon, lat)
					// Check if any node is in bbox
					if !inBBox && bbox.Contains(lat, lon) {
						inBBox = true
					}
				}

				// Skip if invalid or no nodes in bbox
				if !valid || len(coords) < 4 || !inBBox {
					continue
				}

				// Cache way coordinates for relation processing
				// Make a copy since coords slice is reused
				coordsCopy := make([]float64, len(coords))
				copy(coordsCopy, coords)
				e.wayCache.Store(int64(way.ID), coordsCopy)
				e.wayCacheSize.Add(1)

				tags := tagsToJSON(way.Tags)
				tagMap := tagsToMap(way.Tags)
				isClosed := len(way.Nodes) >= 4 && way.Nodes[0].ID == way.Nodes[len(way.Nodes)-1].ID

				// Transform coordinates if needed (after bbox check, before encoding)
				transformer.TransformCoords(coords)

				if isClosed && isArea(way.Tags) {
					// Apply polygon filter
					if e.polygonFilter.HasFilter() && !e.polygonFilter.Match(tagMap) {
						continue
					}
					wkbBytes := encoder.EncodePolygon(coords)
					result := geometryResult{
						osmID:    int64(way.ID),
						osmType:  "W",
						tags:     tags,
						geomWKB:  append([]byte(nil), wkbBytes...),
						geomType: 2,
					}
					if e.cfg.ExtraAttributes {
						result.version = way.Version
						result.changeset = int64(way.ChangesetID)
						result.timestamp = way.Timestamp
						if way.User != "" {
							result.user = way.User
							result.uid = int(way.UserID)
						}
					}
					resultChan <- result
				} else {
					// Apply line filter
					if e.lineFilter.HasFilter() && !e.lineFilter.Match(tagMap) {
						continue
					}
					wkbBytes := encoder.EncodeLineString(coords)
					result := geometryResult{
						osmID:    int64(way.ID),
						osmType:  "W",
						tags:     tags,
						geomWKB:  append([]byte(nil), wkbBytes...),
						geomType: 1,
					}
					if e.cfg.ExtraAttributes {
						result.version = way.Version
						result.changeset = int64(way.ChangesetID)
						result.timestamp = way.Timestamp
						if way.User != "" {
							result.user = way.User
							result.uid = int(way.UserID)
						}
					}
					resultChan <- result
				}
			}
		}()
	}

	// Start worker for processing nodes (points)
	wg.Add(1)
	go func() {
		defer wg.Done()
		encoder := wkb.NewEncoderWithSRID(64, e.cfg.Projection)

		for node := range nodeChan {
			// Skip nodes outside bbox (check in WGS84)
			if bbox != nil && bbox.IsSet && !bbox.Contains(node.Lat, node.Lon) {
				continue
			}

			if len(node.Tags) > 0 && hasMeaningfulTags(node.Tags) {
				// Apply style filter
				if e.pointFilter.HasFilter() && !e.pointFilter.Match(tagsToMap(node.Tags)) {
					continue
				}

				// Transform coordinates if needed
				x, y := transformer.Transform(node.Lon, node.Lat)
				wkbBytes := encoder.EncodePoint(x, y)

				result := geometryResult{
					osmID:    int64(node.ID),
					osmType:  "N",
					tags:     tagsToJSON(node.Tags),
					geomWKB:  append([]byte(nil), wkbBytes...),
					geomType: 0,
				}

				// Add extra attributes if enabled
				if e.cfg.ExtraAttributes {
					result.version = node.Version
					result.changeset = int64(node.ChangesetID)
					result.timestamp = node.Timestamp
					if node.User != "" {
						result.user = node.User
						result.uid = int(node.UserID)
					}
				}

				resultChan <- result
			}
		}
	}()

	// Start worker for processing relations (multipolygons)
	wg.Add(1)
	go func() {
		defer wg.Done()
		encoder := wkb.NewEncoderWithSRID(4096, e.cfg.Projection)

		for rel := range relationChan {
			relCount.Add(1)

			// Only handle multipolygon relations
			if !isMultipolygonRelation(rel) {
				continue
			}

			// Collect outer and inner member ways
			var outerWays, innerWays []int64
			for _, member := range rel.Members {
				if member.Type != osm.TypeWay {
					continue
				}
				switch member.Role {
				case "outer", "":
					outerWays = append(outerWays, int64(member.Ref))
				case "inner":
					innerWays = append(innerWays, int64(member.Ref))
				}
			}

			if len(outerWays) == 0 {
				continue
			}

			// Build multipolygon from member ways
			polygons := e.assembleMultipolygon(outerWays, innerWays, bbox)
			if len(polygons) == 0 {
				continue
			}

			// Transform coordinates if needed
			for _, poly := range polygons {
				for _, ring := range poly {
					transformer.TransformCoords(ring)
				}
			}

			var wkbBytes []byte
			if len(polygons) == 1 && len(polygons[0]) == 1 {
				// Simple polygon (single outer ring, no holes)
				wkbBytes = encoder.EncodePolygon(polygons[0][0])
			} else if len(polygons) == 1 {
				// Polygon with holes
				wkbBytes = encoder.EncodePolygonWithRings(polygons[0])
			} else {
				// MultiPolygon
				wkbBytes = encoder.EncodeMultiPolygon(polygons)
			}

			if wkbBytes != nil {
				// Apply polygon filter to relations
				if e.polygonFilter.HasFilter() && !e.polygonFilter.Match(tagsToMap(rel.Tags)) {
					continue
				}
				result := geometryResult{
					osmID:    int64(rel.ID),
					osmType:  "R",
					tags:     tagsToJSON(rel.Tags),
					geomWKB:  append([]byte(nil), wkbBytes...),
					geomType: 2,
				}
				if e.cfg.ExtraAttributes {
					result.version = rel.Version
					result.changeset = int64(rel.ChangesetID)
					result.timestamp = rel.Timestamp
					if rel.User != "" {
						result.user = rel.User
						result.uid = int(rel.UserID)
					}
				}
				resultChan <- result
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
				OsmID:     result.osmID,
				OsmType:   result.osmType,
				Tags:      result.tags,
				GeomWKB:   result.geomWKB,
				Version:   result.version,
				Changeset: result.changeset,
				Timestamp: result.timestamp,
				User:      result.user,
				UID:       result.uid,
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

	// Create scanner before goroutines so ticker can access byte progress
	scanner := osmpbf.New(ctx, f, runtime.NumCPU())

	// Progress tracker using file size
	progress := NewProgressTracker(e.stats.BytesRead, "geometry building")

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
				ways := wayCount.Load()
				rels := relCount.Load()
				bytesScanned := scanner.FullyScannedBytes()
				p := progress.Calculate(ways+rels, bytesScanned)
				log.Debug("Geometry building progress",
					zap.Int64("ways", ways),
					zap.Int64("relations", rels),
					zap.String("processed", FormatBytes(bytesScanned)),
					zap.String("total", FormatBytes(e.stats.BytesRead)),
					zap.String("percent", fmt.Sprintf("%.1f%%", p.Percentage)),
					zap.String("throughput", FormatThroughput(p.Throughput)),
					zap.String("eta", FormatETA(p.ETA)))
			}
		}
	}()

	// Start PBF scanner goroutine
	go func() {
		defer f.Close()
		defer scanner.Close()
		defer cancelTicker()
		defer close(errChan) // Always close error channel when done

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
				select {
				case relationChan <- o:
				case <-ctx.Done():
					close(nodeChan)
					close(wayChan)
					close(relationChan)
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

		// Close input channels and wait for workers
		close(nodeChan)
		close(wayChan)
		close(relationChan)
		wg.Wait()

		// Clear way cache after relation processing is complete
		e.wayCache = sync.Map{}
		e.wayCacheSize.Store(0)

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
	osmID     int64
	osmType   string
	tags      string
	geomWKB   []byte
	geomType  int // 0=point, 1=line, 2=polygon
	version   int
	changeset int64
	timestamp time.Time
	user      string
	uid       int
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
	m := tagsToMap(tags)
	b, _ := json.Marshal(m)
	return string(b)
}

// tagsToMap converts OSM tags to a map
func tagsToMap(tags osm.Tags) map[string]string {
	m := make(map[string]string, len(tags))
	for _, tag := range tags {
		m[tag.Key] = tag.Value
	}
	return m
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

// isMultipolygonRelation checks if a relation is a multipolygon or boundary
func isMultipolygonRelation(rel *osm.Relation) bool {
	for _, tag := range rel.Tags {
		if tag.Key == "type" {
			return tag.Value == "multipolygon" || tag.Value == "boundary"
		}
	}
	return false
}

// assembleMultipolygon builds multipolygon geometry from member ways
// Returns a slice of polygons, each polygon is a slice of rings (outer + inner)
// Each ring is a flat coordinate array [lon1, lat1, lon2, lat2, ...]
func (e *StreamingExtractor) assembleMultipolygon(outerWays, innerWays []int64, bbox *config.BBox) [][][]float64 {
	// Build outer rings from outer ways
	outerRings := e.assembleRings(outerWays)
	if len(outerRings) == 0 {
		return nil
	}

	// Build inner rings from inner ways
	innerRings := e.assembleRings(innerWays)

	// Check bbox filter - at least one ring must intersect bbox
	if bbox != nil && bbox.IsSet {
		hasInBBox := false
		for _, ring := range outerRings {
			if ringIntersectsBBox(ring, bbox) {
				hasInBBox = true
				break
			}
		}
		if !hasInBBox {
			return nil
		}
	}

	// Simple case: single outer ring
	if len(outerRings) == 1 {
		polygon := [][]float64{outerRings[0]}
		// Add any inner rings that are contained by this outer ring
		for _, inner := range innerRings {
			if ringContainedBy(inner, outerRings[0]) {
				polygon = append(polygon, inner)
			}
		}
		return [][][]float64{polygon}
	}

	// Multiple outer rings = multipolygon
	// Assign inner rings to their containing outer rings
	var polygons [][][]float64
	usedInners := make([]bool, len(innerRings))

	for _, outer := range outerRings {
		polygon := [][]float64{outer}
		for i, inner := range innerRings {
			if !usedInners[i] && ringContainedBy(inner, outer) {
				polygon = append(polygon, inner)
				usedInners[i] = true
			}
		}
		polygons = append(polygons, polygon)
	}

	return polygons
}

// assembleRings connects way segments into closed rings
// Returns fully closed rings only
func (e *StreamingExtractor) assembleRings(wayIDs []int64) [][]float64 {
	if len(wayIDs) == 0 {
		return nil
	}

	// Collect way coordinates
	ways := make([][]float64, 0, len(wayIDs))
	for _, wayID := range wayIDs {
		if coords, ok := e.wayCache.Load(wayID); ok {
			ways = append(ways, coords.([]float64))
		}
	}

	if len(ways) == 0 {
		return nil
	}

	// If single way and already closed, return it directly
	if len(ways) == 1 {
		coords := ways[0]
		if len(coords) >= 8 && coords[0] == coords[len(coords)-2] && coords[1] == coords[len(coords)-1] {
			return [][]float64{coords}
		}
		return nil
	}

	// Connect ways into rings
	var rings [][]float64
	used := make([]bool, len(ways))

	for {
		// Find first unused way to start a new ring
		startIdx := -1
		for i, u := range used {
			if !u {
				startIdx = i
				break
			}
		}
		if startIdx == -1 {
			break
		}

		// Start building a ring
		ring := make([]float64, 0, 1000)
		ring = append(ring, ways[startIdx]...)
		used[startIdx] = true

		// Keep connecting until ring is closed or we can't find more ways
		for {
			if len(ring) >= 4 {
				// Check if ring is closed
				if ring[0] == ring[len(ring)-2] && ring[1] == ring[len(ring)-1] {
					break
				}
			}

			// Find a way that connects to the end of current ring
			endLon, endLat := ring[len(ring)-2], ring[len(ring)-1]
			found := false

			for i, w := range ways {
				if used[i] || len(w) < 4 {
					continue
				}

				// Check if start of way matches end of ring
				if w[0] == endLon && w[1] == endLat {
					// Append way (skip first point to avoid duplicate)
					ring = append(ring, w[2:]...)
					used[i] = true
					found = true
					break
				}

				// Check if end of way matches end of ring (reverse needed)
				if w[len(w)-2] == endLon && w[len(w)-1] == endLat {
					// Append reversed way (skip last point to avoid duplicate)
					for j := len(w) - 4; j >= 0; j -= 2 {
						ring = append(ring, w[j], w[j+1])
					}
					used[i] = true
					found = true
					break
				}
			}

			if !found {
				break
			}
		}

		// Check if ring is valid (closed and has enough points)
		if len(ring) >= 8 && ring[0] == ring[len(ring)-2] && ring[1] == ring[len(ring)-1] {
			rings = append(rings, ring)
		}
	}

	return rings
}

// ringIntersectsBBox checks if any point in the ring is within the bbox
func ringIntersectsBBox(ring []float64, bbox *config.BBox) bool {
	for i := 0; i < len(ring); i += 2 {
		if bbox.Contains(ring[i+1], ring[i]) { // lat, lon
			return true
		}
	}
	return false
}

// ringContainedBy performs a simple point-in-polygon test
// Uses ray casting algorithm with first point of inner ring
func ringContainedBy(inner, outer []float64) bool {
	if len(inner) < 2 || len(outer) < 6 {
		return false
	}

	// Test first point of inner ring
	testLon, testLat := inner[0], inner[1]

	// Ray casting algorithm
	inside := false
	j := len(outer)/2 - 1
	for i := 0; i < len(outer)/2; i++ {
		xi, yi := outer[i*2], outer[i*2+1]
		xj, yj := outer[j*2], outer[j*2+1]

		if ((yi > testLat) != (yj > testLat)) &&
			(testLon < (xj-xi)*(testLat-yi)/(yj-yi)+xi) {
			inside = !inside
		}
		j = i
	}

	return inside
}
