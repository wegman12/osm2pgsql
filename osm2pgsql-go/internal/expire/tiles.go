package expire

import (
	"fmt"
	"math"
)

// Tile represents a map tile at a specific zoom level
type Tile struct {
	Z int // Zoom level
	X int // X coordinate (column)
	Y int // Y coordinate (row)
}

// String returns the tile in z/x/y format
func (t Tile) String() string {
	return fmt.Sprintf("%d/%d/%d", t.Z, t.X, t.Y)
}

// Key returns a unique string key for the tile (for deduplication)
func (t Tile) Key() string {
	return t.String()
}

// BBox represents a geographic bounding box
type BBox struct {
	MinLon, MinLat, MaxLon, MaxLat float64
}

// IsValid checks if the bounding box is valid
func (b BBox) IsValid() bool {
	return b.MinLon <= b.MaxLon && b.MinLat <= b.MaxLat &&
		b.MinLon >= -180 && b.MaxLon <= 180 &&
		b.MinLat >= -90 && b.MaxLat <= 90
}

// Expand expands the bounding box to include another bbox
func (b *BBox) Expand(other BBox) {
	if other.MinLon < b.MinLon {
		b.MinLon = other.MinLon
	}
	if other.MaxLon > b.MaxLon {
		b.MaxLon = other.MaxLon
	}
	if other.MinLat < b.MinLat {
		b.MinLat = other.MinLat
	}
	if other.MaxLat > b.MaxLat {
		b.MaxLat = other.MaxLat
	}
}

// ExpandPoint expands the bounding box to include a point
func (b *BBox) ExpandPoint(lat, lon float64) {
	if lon < b.MinLon {
		b.MinLon = lon
	}
	if lon > b.MaxLon {
		b.MaxLon = lon
	}
	if lat < b.MinLat {
		b.MinLat = lat
	}
	if lat > b.MaxLat {
		b.MaxLat = lat
	}
}

// NewBBoxFromPoint creates a bbox from a single point
func NewBBoxFromPoint(lat, lon float64) BBox {
	return BBox{
		MinLon: lon,
		MaxLon: lon,
		MinLat: lat,
		MaxLat: lat,
	}
}

// NewBBoxFromCoords creates a bbox from a coordinate array [lon, lat, lon, lat, ...]
func NewBBoxFromCoords(coords []float64) BBox {
	if len(coords) < 2 {
		return BBox{}
	}

	bbox := BBox{
		MinLon: coords[0],
		MaxLon: coords[0],
		MinLat: coords[1],
		MaxLat: coords[1],
	}

	for i := 2; i < len(coords); i += 2 {
		lon, lat := coords[i], coords[i+1]
		if lon < bbox.MinLon {
			bbox.MinLon = lon
		}
		if lon > bbox.MaxLon {
			bbox.MaxLon = lon
		}
		if lat < bbox.MinLat {
			bbox.MinLat = lat
		}
		if lat > bbox.MaxLat {
			bbox.MaxLat = lat
		}
	}

	return bbox
}

// Web Mercator constants
const (
	// Maximum latitude for Web Mercator (approximately 85.051129°)
	MaxMercatorLat = 85.0511287798
	// Minimum latitude for Web Mercator
	MinMercatorLat = -85.0511287798
)

// LatLonToTile converts latitude/longitude to tile coordinates at a given zoom level
// Uses the standard Web Mercator tile scheme (OSM/Google style)
func LatLonToTile(lat, lon float64, zoom int) Tile {
	// Clamp latitude to valid Web Mercator range
	if lat > MaxMercatorLat {
		lat = MaxMercatorLat
	}
	if lat < MinMercatorLat {
		lat = MinMercatorLat
	}

	// Clamp longitude to valid range
	if lon < -180 {
		lon = -180
	}
	if lon > 180 {
		lon = 180
	}

	n := float64(int(1) << zoom) // 2^zoom

	// Calculate X tile coordinate
	x := int((lon + 180.0) / 360.0 * n)
	if x >= int(n) {
		x = int(n) - 1
	}

	// Calculate Y tile coordinate using Mercator projection
	latRad := lat * math.Pi / 180.0
	y := int((1.0 - math.Log(math.Tan(latRad)+1.0/math.Cos(latRad))/math.Pi) / 2.0 * n)
	if y >= int(n) {
		y = int(n) - 1
	}
	if y < 0 {
		y = 0
	}

	return Tile{Z: zoom, X: x, Y: y}
}

// TileRange represents a range of tiles at a specific zoom level
type TileRange struct {
	Z          int
	MinX, MaxX int
	MinY, MaxY int
}

// BBoxToTileRange converts a bounding box to a range of tiles at a given zoom level
func BBoxToTileRange(bbox BBox, zoom int) TileRange {
	// Get corner tiles
	// Note: In tile coordinates, Y increases downward (north to south)
	topLeft := LatLonToTile(bbox.MaxLat, bbox.MinLon, zoom)
	bottomRight := LatLonToTile(bbox.MinLat, bbox.MaxLon, zoom)

	return TileRange{
		Z:    zoom,
		MinX: topLeft.X,
		MaxX: bottomRight.X,
		MinY: topLeft.Y,  // Northern tiles have smaller Y
		MaxY: bottomRight.Y,
	}
}

// TileCount returns the number of tiles in the range
func (r TileRange) TileCount() int {
	return (r.MaxX - r.MinX + 1) * (r.MaxY - r.MinY + 1)
}

// Tiles returns all tiles in the range
func (r TileRange) Tiles() []Tile {
	tiles := make([]Tile, 0, r.TileCount())
	for x := r.MinX; x <= r.MaxX; x++ {
		for y := r.MinY; y <= r.MaxY; y++ {
			tiles = append(tiles, Tile{Z: r.Z, X: x, Y: y})
		}
	}
	return tiles
}

// GetAffectedTiles returns all tiles affected by a bounding box across zoom levels
func GetAffectedTiles(bbox BBox, minZoom, maxZoom int) []Tile {
	if !bbox.IsValid() {
		return nil
	}

	var tiles []Tile
	for z := minZoom; z <= maxZoom; z++ {
		tileRange := BBoxToTileRange(bbox, z)
		tiles = append(tiles, tileRange.Tiles()...)
	}
	return tiles
}

// GetAffectedTilesForPoint returns all tiles affected by a point across zoom levels
func GetAffectedTilesForPoint(lat, lon float64, minZoom, maxZoom int) []Tile {
	tiles := make([]Tile, 0, maxZoom-minZoom+1)
	for z := minZoom; z <= maxZoom; z++ {
		tiles = append(tiles, LatLonToTile(lat, lon, z))
	}
	return tiles
}
