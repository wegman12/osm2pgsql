package expire

import (
	"testing"
)

func TestLatLonToTile(t *testing.T) {
	tests := []struct {
		name     string
		lat, lon float64
		zoom     int
		wantX    int
		wantY    int
	}{
		{
			name: "London at zoom 10",
			lat:  51.5074,
			lon:  -0.1278,
			zoom: 10,
			wantX: 511,
			wantY: 340,
		},
		{
			name: "Monaco at zoom 12",
			lat:  43.7384,
			lon:  7.4246,
			zoom: 12,
			wantX: 2132,
			wantY: 1493,
		},
		{
			name: "New York at zoom 10",
			lat:  40.7128,
			lon:  -74.0060,
			zoom: 10,
			wantX: 301,
			wantY: 385,
		},
		{
			name: "Origin at zoom 0",
			lat:  0,
			lon:  0,
			zoom: 0,
			wantX: 0,
			wantY: 0,
		},
		{
			name: "Origin at zoom 1",
			lat:  0,
			lon:  0,
			zoom: 1,
			wantX: 1,
			wantY: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tile := LatLonToTile(tt.lat, tt.lon, tt.zoom)
			if tile.X != tt.wantX || tile.Y != tt.wantY {
				t.Errorf("LatLonToTile(%f, %f, %d) = (%d, %d), want (%d, %d)",
					tt.lat, tt.lon, tt.zoom, tile.X, tile.Y, tt.wantX, tt.wantY)
			}
		})
	}
}

func TestBBoxToTileRange(t *testing.T) {
	// Monaco bounding box
	bbox := BBox{
		MinLon: 7.409,
		MinLat: 43.724,
		MaxLon: 7.440,
		MaxLat: 43.752,
	}

	tileRange := BBoxToTileRange(bbox, 14)

	// Verify we get a reasonable range
	if tileRange.TileCount() < 1 {
		t.Error("expected at least 1 tile")
	}
	if tileRange.TileCount() > 100 {
		t.Errorf("expected fewer than 100 tiles, got %d", tileRange.TileCount())
	}

	// Verify zoom level
	if tileRange.Z != 14 {
		t.Errorf("expected zoom 14, got %d", tileRange.Z)
	}
}

func TestGetAffectedTiles(t *testing.T) {
	bbox := NewBBoxFromPoint(43.7384, 7.4246)

	tiles := GetAffectedTiles(bbox, 10, 12)

	// Should have tiles for zoom 10, 11, 12
	if len(tiles) != 3 {
		t.Errorf("expected 3 tiles (one per zoom), got %d", len(tiles))
	}

	// Verify each zoom level is represented
	zooms := make(map[int]bool)
	for _, tile := range tiles {
		zooms[tile.Z] = true
	}
	for z := 10; z <= 12; z++ {
		if !zooms[z] {
			t.Errorf("expected tile at zoom %d", z)
		}
	}
}

func TestTileString(t *testing.T) {
	tile := Tile{Z: 12, X: 2144, Y: 1501}
	expected := "12/2144/1501"
	if tile.String() != expected {
		t.Errorf("expected %s, got %s", expected, tile.String())
	}
}

func TestBBoxFromCoords(t *testing.T) {
	// Simple line: 3 points
	coords := []float64{
		7.409, 43.724,
		7.420, 43.740,
		7.440, 43.752,
	}

	bbox := NewBBoxFromCoords(coords)

	if bbox.MinLon != 7.409 {
		t.Errorf("expected MinLon 7.409, got %f", bbox.MinLon)
	}
	if bbox.MaxLon != 7.440 {
		t.Errorf("expected MaxLon 7.440, got %f", bbox.MaxLon)
	}
	if bbox.MinLat != 43.724 {
		t.Errorf("expected MinLat 43.724, got %f", bbox.MinLat)
	}
	if bbox.MaxLat != 43.752 {
		t.Errorf("expected MaxLat 43.752, got %f", bbox.MaxLat)
	}
}
