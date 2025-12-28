package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kevinramage/osm2pgsql-go/internal/config"
	"github.com/kevinramage/osm2pgsql-go/internal/expire"
	"github.com/kevinramage/osm2pgsql-go/internal/logger"
	"github.com/kevinramage/osm2pgsql-go/internal/middle"
	"github.com/kevinramage/osm2pgsql-go/internal/osc"
	"github.com/kevinramage/osm2pgsql-go/internal/proj"
	"github.com/kevinramage/osm2pgsql-go/internal/wkb"
)

// AppendStats tracks append processing statistics
type AppendStats struct {
	NodesProcessed     int64
	WaysProcessed      int64
	RelationsProcessed int64
	WaysRebuilt        int64
	RelationsRebuilt   int64
	PointsUpdated      int64
	LinesUpdated       int64
	PolygonsUpdated    int64
	Duration           time.Duration
}

// AppendProcessor handles incremental updates from OSC files
type AppendProcessor struct {
	cfg         *config.Config
	pool        *pgxpool.Pool
	middleStore *middle.MiddleStore
	transformer *proj.Transformer

	// Pending sets for cascading updates
	pendingWays      map[int64]bool
	pendingRelations map[int64]bool

	// Tile expiry tracking
	expireTracker *expire.Tracker
}

// NewAppendProcessor creates a new append processor
func NewAppendProcessor(cfg *config.Config, pool *pgxpool.Pool, middleStore *middle.MiddleStore) *AppendProcessor {
	transformer, _ := proj.NewTransformer(proj.SRID4326, cfg.Projection)

	// Create expire tracker if expire output is configured
	var tracker *expire.Tracker
	if cfg.ExpireOutput != "" {
		tracker = expire.NewTracker(cfg.ExpireMinZoom, cfg.ExpireMaxZoom)
	}

	return &AppendProcessor{
		cfg:              cfg,
		pool:             pool,
		middleStore:      middleStore,
		transformer:      transformer,
		pendingWays:      make(map[int64]bool),
		pendingRelations: make(map[int64]bool),
		expireTracker:    tracker,
	}
}

// ExpireTracker returns the expire tracker (for writing output after processing)
func (p *AppendProcessor) ExpireTracker() *expire.Tracker {
	return p.expireTracker
}

// ProcessChanges applies changes from an OSC file
func (p *AppendProcessor) ProcessChanges(ctx context.Context, changes <-chan osc.Change) (*AppendStats, error) {
	log := logger.Get()
	stats := &AppendStats{}
	start := time.Now()

	log.Info("Processing OSC changes")

	// Process all changes
	for change := range changes {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		var err error
		switch change.Type {
		case "node":
			err = p.processNodeChange(ctx, change, stats)
		case "way":
			err = p.processWayChange(ctx, change, stats)
		case "relation":
			err = p.processRelationChange(ctx, change, stats)
		}

		if err != nil {
			return nil, fmt.Errorf("failed to process %s change: %w", change.Type, err)
		}
	}

	log.Info("Processed direct changes",
		zap.Int64("nodes", stats.NodesProcessed),
		zap.Int64("ways", stats.WaysProcessed),
		zap.Int64("relations", stats.RelationsProcessed))

	// Rebuild pending ways (affected by node changes)
	if len(p.pendingWays) > 0 {
		log.Info("Rebuilding affected ways", zap.Int("count", len(p.pendingWays)))
		for wayID := range p.pendingWays {
			if err := p.rebuildWay(ctx, wayID, stats); err != nil {
				log.Warn("Failed to rebuild way", zap.Int64("id", wayID), zap.Error(err))
			}
		}
	}

	// Rebuild pending relations (affected by way changes)
	if len(p.pendingRelations) > 0 {
		log.Info("Rebuilding affected relations", zap.Int("count", len(p.pendingRelations)))
		for relID := range p.pendingRelations {
			if err := p.rebuildRelation(ctx, relID, stats); err != nil {
				log.Warn("Failed to rebuild relation", zap.Int64("id", relID), zap.Error(err))
			}
		}
	}

	stats.Duration = time.Since(start)

	log.Info("Append processing complete",
		zap.Int64("ways_rebuilt", stats.WaysRebuilt),
		zap.Int64("relations_rebuilt", stats.RelationsRebuilt),
		zap.Int64("points_updated", stats.PointsUpdated),
		zap.Int64("lines_updated", stats.LinesUpdated),
		zap.Int64("polygons_updated", stats.PolygonsUpdated),
		zap.Duration("duration", stats.Duration))

	return stats, nil
}

// processNodeChange handles a node create/modify/delete
func (p *AppendProcessor) processNodeChange(ctx context.Context, change osc.Change, stats *AppendStats) error {
	node := change.Node
	if node == nil {
		return nil
	}
	stats.NodesProcessed++

	// Expire tiles for the node's location
	if p.expireTracker != nil {
		lat := middle.UnscaleCoord(node.Lat)
		lon := middle.UnscaleCoord(node.Lon)
		p.expireTracker.ExpirePoint(lat, lon)
	}

	switch change.Action {
	case osc.ActionCreate, osc.ActionModify:
		// Update middle table
		if err := p.middleStore.UpdateNode(ctx, node); err != nil {
			return err
		}

		// Update point geometry if node has meaningful tags
		if len(node.Tags) > 0 && hasMeaningfulNodeTags(node.Tags) {
			if err := p.updatePointGeometry(ctx, node); err != nil {
				return err
			}
			stats.PointsUpdated++
		}

		// Find affected ways and mark for rebuild
		wayIDs, err := p.middleStore.GetWaysForNode(ctx, node.ID)
		if err != nil {
			return err
		}
		for _, wayID := range wayIDs {
			p.pendingWays[wayID] = true
		}

	case osc.ActionDelete:
		// Delete from middle table
		if err := p.middleStore.DeleteNode(ctx, node.ID); err != nil {
			return err
		}

		// Delete from output table
		if err := p.deleteFromOutput(ctx, "planet_osm_point", node.ID, "N"); err != nil {
			return err
		}

		// Find affected ways and mark for rebuild
		wayIDs, err := p.middleStore.GetWaysForNode(ctx, node.ID)
		if err != nil {
			return err
		}
		for _, wayID := range wayIDs {
			p.pendingWays[wayID] = true
		}
	}

	return nil
}

// processWayChange handles a way create/modify/delete
func (p *AppendProcessor) processWayChange(ctx context.Context, change osc.Change, stats *AppendStats) error {
	way := change.Way
	if way == nil {
		return nil
	}
	stats.WaysProcessed++

	switch change.Action {
	case osc.ActionCreate, osc.ActionModify:
		// Update middle table
		if err := p.middleStore.UpdateWay(ctx, way); err != nil {
			return err
		}

		// Rebuild way geometry
		if err := p.rebuildWayDirect(ctx, way, stats); err != nil {
			return err
		}

		// Find affected relations and mark for rebuild
		relIDs, err := p.middleStore.GetRelationsForMember(ctx, "w", way.ID)
		if err != nil {
			return err
		}
		for _, relID := range relIDs {
			p.pendingRelations[relID] = true
		}

	case osc.ActionDelete:
		// Delete from middle table
		if err := p.middleStore.DeleteWay(ctx, way.ID); err != nil {
			return err
		}

		// Delete from output tables (could be line or polygon)
		if err := p.deleteFromOutput(ctx, "planet_osm_line", way.ID, "W"); err != nil {
			return err
		}
		if err := p.deleteFromOutput(ctx, "planet_osm_polygon", way.ID, "W"); err != nil {
			return err
		}

		// Find affected relations and mark for rebuild
		relIDs, err := p.middleStore.GetRelationsForMember(ctx, "w", way.ID)
		if err != nil {
			return err
		}
		for _, relID := range relIDs {
			p.pendingRelations[relID] = true
		}
	}

	return nil
}

// processRelationChange handles a relation create/modify/delete
func (p *AppendProcessor) processRelationChange(ctx context.Context, change osc.Change, stats *AppendStats) error {
	rel := change.Relation
	if rel == nil {
		return nil
	}
	stats.RelationsProcessed++

	switch change.Action {
	case osc.ActionCreate, osc.ActionModify:
		// Update middle table
		if err := p.middleStore.UpdateRelation(ctx, rel); err != nil {
			return err
		}

		// Rebuild relation geometry if it's a multipolygon
		if isMultipolygonTags(rel.Tags) {
			if err := p.rebuildRelationDirect(ctx, rel, stats); err != nil {
				return err
			}
		}

	case osc.ActionDelete:
		// Delete from middle table
		if err := p.middleStore.DeleteRelation(ctx, rel.ID); err != nil {
			return err
		}

		// Delete from output table
		if err := p.deleteFromOutput(ctx, "planet_osm_polygon", rel.ID, "R"); err != nil {
			return err
		}
	}

	return nil
}

// rebuildWay rebuilds a way's geometry from middle tables
func (p *AppendProcessor) rebuildWay(ctx context.Context, wayID int64, stats *AppendStats) error {
	way, err := p.middleStore.GetWay(ctx, wayID)
	if err != nil {
		return err
	}
	if way == nil {
		return nil // Way was deleted
	}

	return p.rebuildWayDirect(ctx, way, stats)
}

// rebuildWayDirect rebuilds geometry for a way
func (p *AppendProcessor) rebuildWayDirect(ctx context.Context, way *middle.RawWay, stats *AppendStats) error {
	// Get node coordinates
	coords := make([]float64, 0, len(way.Nodes)*2)
	for _, nodeID := range way.Nodes {
		node, err := p.middleStore.GetNode(ctx, nodeID)
		if err != nil {
			return err
		}
		if node == nil {
			return nil // Missing node, can't build geometry
		}
		coords = append(coords, middle.UnscaleCoord(node.Lon), middle.UnscaleCoord(node.Lat))
	}

	if len(coords) < 4 {
		return nil // Not enough points
	}

	// Expire tiles for the way's bounding box (before coordinate transformation)
	if p.expireTracker != nil {
		p.expireTracker.ExpireCoords(coords)
	}

	// Transform coordinates
	p.transformer.TransformCoords(coords)

	// Determine geometry type
	isClosed := len(way.Nodes) >= 4 && way.Nodes[0] == way.Nodes[len(way.Nodes)-1]
	isAreaTag := isAreaTags(way.Tags)

	encoder := wkb.NewEncoderWithSRID(1024, p.cfg.Projection)
	tagsJSON, _ := json.Marshal(way.Tags)

	// Delete existing geometry first
	if err := p.deleteFromOutput(ctx, "planet_osm_line", way.ID, "W"); err != nil {
		return err
	}
	if err := p.deleteFromOutput(ctx, "planet_osm_polygon", way.ID, "W"); err != nil {
		return err
	}

	if isClosed && isAreaTag {
		// Insert as polygon
		wkbBytes := encoder.EncodePolygon(coords)
		if err := p.insertGeometry(ctx, "planet_osm_polygon", way.ID, "W", string(tagsJSON), wkbBytes); err != nil {
			return err
		}
		stats.PolygonsUpdated++
	} else {
		// Insert as line
		wkbBytes := encoder.EncodeLineString(coords)
		if err := p.insertGeometry(ctx, "planet_osm_line", way.ID, "W", string(tagsJSON), wkbBytes); err != nil {
			return err
		}
		stats.LinesUpdated++
	}

	stats.WaysRebuilt++
	return nil
}

// rebuildRelation rebuilds a relation's geometry from middle tables
func (p *AppendProcessor) rebuildRelation(ctx context.Context, relID int64, stats *AppendStats) error {
	rel, err := p.middleStore.GetRelation(ctx, relID)
	if err != nil {
		return err
	}
	if rel == nil {
		return nil // Relation was deleted
	}

	if !isMultipolygonTags(rel.Tags) {
		return nil // Not a multipolygon
	}

	return p.rebuildRelationDirect(ctx, rel, stats)
}

// rebuildRelationDirect rebuilds geometry for a relation
func (p *AppendProcessor) rebuildRelationDirect(ctx context.Context, rel *middle.RawRelation, stats *AppendStats) error {
	// Collect outer and inner ways
	var outerWayIDs, innerWayIDs []int64
	for _, member := range rel.Members {
		if member.Type != "w" {
			continue
		}
		switch member.Role {
		case "outer", "":
			outerWayIDs = append(outerWayIDs, member.Ref)
		case "inner":
			innerWayIDs = append(innerWayIDs, member.Ref)
		}
	}

	if len(outerWayIDs) == 0 {
		return nil
	}

	// Build rings from ways
	outerRings, err := p.buildRingsFromWays(ctx, outerWayIDs)
	if err != nil {
		return err
	}
	if len(outerRings) == 0 {
		return nil
	}

	innerRings, err := p.buildRingsFromWays(ctx, innerWayIDs)
	if err != nil {
		return err
	}

	// Expire tiles for all rings (before coordinate transformation)
	if p.expireTracker != nil {
		for _, ring := range outerRings {
			p.expireTracker.ExpireCoords(ring)
		}
		for _, ring := range innerRings {
			p.expireTracker.ExpireCoords(ring)
		}
	}

	// Transform all ring coordinates
	for _, ring := range outerRings {
		p.transformer.TransformCoords(ring)
	}
	for _, ring := range innerRings {
		p.transformer.TransformCoords(ring)
	}

	// Build polygons
	var polygons [][][]float64
	if len(outerRings) == 1 {
		polygon := [][]float64{outerRings[0]}
		polygon = append(polygon, innerRings...)
		polygons = append(polygons, polygon)
	} else {
		// Multiple outer rings - simple assignment
		for _, outer := range outerRings {
			polygons = append(polygons, [][]float64{outer})
		}
	}

	// Encode geometry
	encoder := wkb.NewEncoderWithSRID(4096, p.cfg.Projection)
	var wkbBytes []byte
	if len(polygons) == 1 && len(polygons[0]) == 1 {
		wkbBytes = encoder.EncodePolygon(polygons[0][0])
	} else if len(polygons) == 1 {
		wkbBytes = encoder.EncodePolygonWithRings(polygons[0])
	} else {
		wkbBytes = encoder.EncodeMultiPolygon(polygons)
	}

	if wkbBytes == nil {
		return nil
	}

	tagsJSON, _ := json.Marshal(rel.Tags)

	// Delete existing and insert new
	if err := p.deleteFromOutput(ctx, "planet_osm_polygon", rel.ID, "R"); err != nil {
		return err
	}
	if err := p.insertGeometry(ctx, "planet_osm_polygon", rel.ID, "R", string(tagsJSON), wkbBytes); err != nil {
		return err
	}

	stats.RelationsRebuilt++
	stats.PolygonsUpdated++
	return nil
}

// buildRingsFromWays builds coordinate rings from way IDs
func (p *AppendProcessor) buildRingsFromWays(ctx context.Context, wayIDs []int64) ([][]float64, error) {
	if len(wayIDs) == 0 {
		return nil, nil
	}

	// Get coordinates for each way
	ways := make([][]float64, 0, len(wayIDs))
	for _, wayID := range wayIDs {
		way, err := p.middleStore.GetWay(ctx, wayID)
		if err != nil {
			return nil, err
		}
		if way == nil {
			continue
		}

		coords := make([]float64, 0, len(way.Nodes)*2)
		for _, nodeID := range way.Nodes {
			node, err := p.middleStore.GetNode(ctx, nodeID)
			if err != nil {
				return nil, err
			}
			if node == nil {
				break // Missing node
			}
			coords = append(coords, middle.UnscaleCoord(node.Lon), middle.UnscaleCoord(node.Lat))
		}

		if len(coords) >= 4 {
			ways = append(ways, coords)
		}
	}

	if len(ways) == 0 {
		return nil, nil
	}

	// If single way and closed, return directly
	if len(ways) == 1 {
		coords := ways[0]
		if len(coords) >= 8 && coords[0] == coords[len(coords)-2] && coords[1] == coords[len(coords)-1] {
			return [][]float64{coords}, nil
		}
		return nil, nil
	}

	// Try to connect ways into rings (simplified version)
	var rings [][]float64
	used := make([]bool, len(ways))

	for {
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

		ring := make([]float64, 0, 1000)
		ring = append(ring, ways[startIdx]...)
		used[startIdx] = true

		// Try to close the ring
		for attempts := 0; attempts < len(ways); attempts++ {
			if len(ring) >= 4 && ring[0] == ring[len(ring)-2] && ring[1] == ring[len(ring)-1] {
				break
			}

			endLon, endLat := ring[len(ring)-2], ring[len(ring)-1]
			found := false

			for i, w := range ways {
				if used[i] || len(w) < 4 {
					continue
				}

				if w[0] == endLon && w[1] == endLat {
					ring = append(ring, w[2:]...)
					used[i] = true
					found = true
					break
				}

				if w[len(w)-2] == endLon && w[len(w)-1] == endLat {
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

		if len(ring) >= 8 && ring[0] == ring[len(ring)-2] && ring[1] == ring[len(ring)-1] {
			rings = append(rings, ring)
		}
	}

	return rings, nil
}

// updatePointGeometry updates a point geometry in the output table
func (p *AppendProcessor) updatePointGeometry(ctx context.Context, node *middle.RawNode) error {
	// Delete existing
	if err := p.deleteFromOutput(ctx, "planet_osm_point", node.ID, "N"); err != nil {
		return err
	}

	// Transform coordinates
	lon := middle.UnscaleCoord(node.Lon)
	lat := middle.UnscaleCoord(node.Lat)
	x, y := p.transformer.Transform(lon, lat)

	// Encode point
	encoder := wkb.NewEncoderWithSRID(64, p.cfg.Projection)
	wkbBytes := encoder.EncodePoint(x, y)

	tagsJSON, _ := json.Marshal(node.Tags)
	return p.insertGeometry(ctx, "planet_osm_point", node.ID, "N", string(tagsJSON), wkbBytes)
}

// deleteFromOutput deletes a geometry from an output table
func (p *AppendProcessor) deleteFromOutput(ctx context.Context, table string, osmID int64, osmType string) error {
	sql := fmt.Sprintf("DELETE FROM %s.%s WHERE osm_id = $1 AND osm_type = $2", p.cfg.DBSchema, table)
	_, err := p.pool.Exec(ctx, sql, osmID, osmType)
	return err
}

// insertGeometry inserts a geometry into an output table
func (p *AppendProcessor) insertGeometry(ctx context.Context, table string, osmID int64, osmType, tags string, geomWKB []byte) error {
	sql := fmt.Sprintf("INSERT INTO %s.%s (osm_id, osm_type, tags, geom) VALUES ($1, $2, $3, $4)", p.cfg.DBSchema, table)
	_, err := p.pool.Exec(ctx, sql, osmID, osmType, tags, geomWKB)
	return err
}

// hasMeaningfulNodeTags checks if node tags are meaningful (not just metadata)
func hasMeaningfulNodeTags(tags map[string]string) bool {
	dominated := map[string]bool{
		"created_by": true,
		"source":     true,
		"note":       true,
		"fixme":      true,
		"FIXME":      true,
	}

	for k := range tags {
		if !dominated[k] {
			return true
		}
	}
	return false
}

// isAreaTags checks if tags indicate an area
func isAreaTags(tags map[string]string) bool {
	if v, ok := tags["area"]; ok {
		return v == "yes"
	}

	areaKeys := map[string]bool{
		"building": true,
		"landuse":  true,
		"natural":  true,
		"leisure":  true,
		"amenity":  true,
		"shop":     true,
		"tourism":  true,
		"man_made": true,
	}

	for k := range tags {
		if areaKeys[k] {
			return true
		}
	}

	return false
}

// isMultipolygonTags checks if tags indicate a multipolygon relation
func isMultipolygonTags(tags map[string]string) bool {
	if t, ok := tags["type"]; ok {
		return t == "multipolygon" || t == "boundary"
	}
	return false
}
