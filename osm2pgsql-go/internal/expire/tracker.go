package expire

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"sync"

	"go.uber.org/zap"

	"github.com/kevinramage/osm2pgsql-go/internal/logger"
)

// Tracker tracks tiles that need to be expired (re-rendered)
type Tracker struct {
	mu       sync.Mutex
	tiles    map[string]Tile // Deduplicated tiles by key
	minZoom  int
	maxZoom  int
	enabled  bool
}

// NewTracker creates a new tile expiry tracker
func NewTracker(minZoom, maxZoom int) *Tracker {
	return &Tracker{
		tiles:   make(map[string]Tile),
		minZoom: minZoom,
		maxZoom: maxZoom,
		enabled: true,
	}
}

// Disable turns off tile tracking (for when no expire output is needed)
func (t *Tracker) Disable() {
	t.enabled = false
}

// IsEnabled returns whether tracking is enabled
func (t *Tracker) IsEnabled() bool {
	return t.enabled
}

// ExpirePoint marks tiles containing a point as expired
func (t *Tracker) ExpirePoint(lat, lon float64) {
	if !t.enabled {
		return
	}

	tiles := GetAffectedTilesForPoint(lat, lon, t.minZoom, t.maxZoom)
	t.addTiles(tiles)
}

// ExpireBBox marks tiles intersecting a bounding box as expired
func (t *Tracker) ExpireBBox(bbox BBox) {
	if !t.enabled {
		return
	}

	if !bbox.IsValid() {
		return
	}

	tiles := GetAffectedTiles(bbox, t.minZoom, t.maxZoom)
	t.addTiles(tiles)
}

// ExpireCoords marks tiles for a coordinate array as expired
// Coords format: [lon, lat, lon, lat, ...]
func (t *Tracker) ExpireCoords(coords []float64) {
	if !t.enabled || len(coords) < 2 {
		return
	}

	bbox := NewBBoxFromCoords(coords)
	t.ExpireBBox(bbox)
}

// addTiles adds tiles to the tracker (thread-safe, deduplicated)
func (t *Tracker) addTiles(tiles []Tile) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, tile := range tiles {
		key := tile.Key()
		if _, exists := t.tiles[key]; !exists {
			t.tiles[key] = tile
		}
	}
}

// Count returns the number of unique expired tiles
func (t *Tracker) Count() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.tiles)
}

// CountByZoom returns the count of tiles at each zoom level
func (t *Tracker) CountByZoom() map[int]int {
	t.mu.Lock()
	defer t.mu.Unlock()

	counts := make(map[int]int)
	for _, tile := range t.tiles {
		counts[tile.Z]++
	}
	return counts
}

// GetTiles returns all expired tiles
func (t *Tracker) GetTiles() []Tile {
	t.mu.Lock()
	defer t.mu.Unlock()

	tiles := make([]Tile, 0, len(t.tiles))
	for _, tile := range t.tiles {
		tiles = append(tiles, tile)
	}

	// Sort tiles for consistent output
	sort.Slice(tiles, func(i, j int) bool {
		if tiles[i].Z != tiles[j].Z {
			return tiles[i].Z < tiles[j].Z
		}
		if tiles[i].X != tiles[j].X {
			return tiles[i].X < tiles[j].X
		}
		return tiles[i].Y < tiles[j].Y
	})

	return tiles
}

// Clear removes all tracked tiles
func (t *Tracker) Clear() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tiles = make(map[string]Tile)
}

// WriteToFile writes expired tiles to a file in z/x/y format
func (t *Tracker) WriteToFile(filename string) error {
	log := logger.Get()

	tiles := t.GetTiles()
	if len(tiles) == 0 {
		log.Info("No tiles to expire")
		return nil
	}

	f, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create expire file: %w", err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	for _, tile := range tiles {
		fmt.Fprintln(w, tile.String())
	}

	if err := w.Flush(); err != nil {
		return fmt.Errorf("failed to write expire file: %w", err)
	}

	// Log summary by zoom level
	counts := t.CountByZoom()
	zoomFields := make([]zap.Field, 0, len(counts)+1)
	zoomFields = append(zoomFields, zap.String("file", filename))

	zooms := make([]int, 0, len(counts))
	for z := range counts {
		zooms = append(zooms, z)
	}
	sort.Ints(zooms)

	for _, z := range zooms {
		zoomFields = append(zoomFields, zap.Int(fmt.Sprintf("z%d", z), counts[z]))
	}
	zoomFields = append(zoomFields, zap.Int("total", len(tiles)))

	log.Info("Wrote expire tiles", zoomFields...)

	return nil
}

// AppendToFile appends expired tiles to an existing file
func (t *Tracker) AppendToFile(filename string) error {
	tiles := t.GetTiles()
	if len(tiles) == 0 {
		return nil
	}

	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open expire file: %w", err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	for _, tile := range tiles {
		fmt.Fprintln(w, tile.String())
	}

	return w.Flush()
}

// Stats returns statistics about tracked tiles
type Stats struct {
	TotalTiles   int
	TilesByZoom  map[int]int
	MinZoom      int
	MaxZoom      int
}

// GetStats returns statistics about tracked tiles
func (t *Tracker) GetStats() Stats {
	counts := t.CountByZoom()

	stats := Stats{
		TotalTiles:  t.Count(),
		TilesByZoom: counts,
		MinZoom:     t.minZoom,
		MaxZoom:     t.maxZoom,
	}

	return stats
}
