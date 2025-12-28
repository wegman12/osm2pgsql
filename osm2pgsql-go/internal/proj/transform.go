package proj

import (
	"fmt"
	"math"
)

// SRID constants for common projections
const (
	SRID4326 = 4326 // WGS84 (lat/lon)
	SRID3857 = 3857 // Web Mercator
)

// Transformer handles coordinate transformations between projections
type Transformer struct {
	SourceSRID int
	TargetSRID int
}

// NewTransformer creates a transformer from source to target SRID
func NewTransformer(sourceSRID, targetSRID int) (*Transformer, error) {
	// Validate supported projections
	if sourceSRID != SRID4326 {
		return nil, fmt.Errorf("unsupported source SRID: %d (only 4326 supported)", sourceSRID)
	}
	if targetSRID != SRID4326 && targetSRID != SRID3857 {
		return nil, fmt.Errorf("unsupported target SRID: %d (only 4326 and 3857 supported)", targetSRID)
	}

	return &Transformer{
		SourceSRID: sourceSRID,
		TargetSRID: targetSRID,
	}, nil
}

// Transform converts a coordinate from source to target projection
// Input: lon, lat in source projection
// Output: x, y in target projection
func (t *Transformer) Transform(lon, lat float64) (x, y float64) {
	if t.SourceSRID == t.TargetSRID {
		return lon, lat
	}

	// 4326 -> 3857 (WGS84 to Web Mercator)
	if t.SourceSRID == SRID4326 && t.TargetSRID == SRID3857 {
		return lonLatToWebMercator(lon, lat)
	}

	// No transformation available, return as-is
	return lon, lat
}

// TransformCoords transforms a flat coordinate array in place
// coords format: [lon1, lat1, lon2, lat2, ...]
func (t *Transformer) TransformCoords(coords []float64) {
	if t.SourceSRID == t.TargetSRID {
		return
	}

	for i := 0; i < len(coords); i += 2 {
		coords[i], coords[i+1] = t.Transform(coords[i], coords[i+1])
	}
}

// NeedsTransform returns true if transformation is required
func (t *Transformer) NeedsTransform() bool {
	return t.SourceSRID != t.TargetSRID
}

// Web Mercator constants
const (
	// Semi-major axis of WGS84 ellipsoid in meters
	earthRadius = 6378137.0
	// Maximum extent of Web Mercator
	maxExtent = 20037508.342789244
)

// lonLatToWebMercator converts WGS84 (lon, lat) to Web Mercator (x, y)
func lonLatToWebMercator(lon, lat float64) (x, y float64) {
	// Clamp latitude to avoid infinity at poles
	if lat > 85.06 {
		lat = 85.06
	} else if lat < -85.06 {
		lat = -85.06
	}

	// Convert longitude to x (meters)
	x = lon * maxExtent / 180.0

	// Convert latitude to y (meters)
	// y = R * ln(tan(π/4 + φ/2))
	latRad := lat * math.Pi / 180.0
	y = math.Log(math.Tan(math.Pi/4.0+latRad/2.0)) * earthRadius

	return x, y
}

// ParseSRID parses a projection string to SRID
// Accepts: "4326", "3857", "EPSG:4326", "EPSG:3857"
func ParseSRID(s string) (int, error) {
	switch s {
	case "4326", "EPSG:4326":
		return SRID4326, nil
	case "3857", "EPSG:3857":
		return SRID3857, nil
	default:
		return 0, fmt.Errorf("unsupported projection: %s (supported: 4326, 3857)", s)
	}
}
