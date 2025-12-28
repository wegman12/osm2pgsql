package wkb

import (
	"encoding/binary"
	"math"
)

// WKB type constants (ISO SQL/MM specification)
const (
	wkbPoint        = 1
	wkbLineString   = 2
	wkbPolygon      = 3
	wkbMultiPolygon = 6

	// SRID flag for EWKB (PostGIS extended WKB)
	wkbSRIDFlag = 0x20000000
)

// Common SRID constants
const (
	SRID4326 = 4326 // WGS84
	SRID3857 = 3857 // Web Mercator
)

// Encoder encodes geometries to WKB format
// Uses little-endian byte order and includes SRID (EWKB format)
type Encoder struct {
	buf  []byte
	srid uint32
}

// NewEncoder creates a new WKB encoder with pre-allocated buffer and default SRID 4326
func NewEncoder(initialSize int) *Encoder {
	return &Encoder{
		buf:  make([]byte, 0, initialSize),
		srid: SRID4326,
	}
}

// NewEncoderWithSRID creates a new WKB encoder with specified SRID
func NewEncoderWithSRID(initialSize int, srid int) *Encoder {
	return &Encoder{
		buf:  make([]byte, 0, initialSize),
		srid: uint32(srid),
	}
}

// SRID returns the encoder's current SRID
func (e *Encoder) SRID() int {
	return int(e.srid)
}

// Reset clears the buffer for reuse
func (e *Encoder) Reset() {
	e.buf = e.buf[:0]
}

// Bytes returns the encoded WKB bytes
func (e *Encoder) Bytes() []byte {
	return e.buf
}

// EncodePoint encodes a point as EWKB with SRID
func (e *Encoder) EncodePoint(lon, lat float64) []byte {
	e.Reset()
	// Total size: 1 (byte order) + 4 (type+srid flag) + 4 (srid) + 16 (2 doubles) = 25 bytes
	e.ensureCapacity(25)

	// Byte order (little-endian)
	e.buf = append(e.buf, 0x01)

	// Type with SRID flag
	e.appendUint32(wkbPoint | wkbSRIDFlag)

	// SRID
	e.appendUint32(e.srid)

	// Coordinates (X=lon, Y=lat)
	e.appendFloat64(lon)
	e.appendFloat64(lat)

	return e.buf
}

// EncodeLineString encodes a linestring as EWKB with SRID
// coords is a flat array of [lon1, lat1, lon2, lat2, ...]
func (e *Encoder) EncodeLineString(coords []float64) []byte {
	e.Reset()
	numPoints := len(coords) / 2
	// Size: 1 + 4 + 4 + 4 + (numPoints * 16)
	e.ensureCapacity(13 + numPoints*16)

	// Byte order (little-endian)
	e.buf = append(e.buf, 0x01)

	// Type with SRID flag
	e.appendUint32(wkbLineString | wkbSRIDFlag)

	// SRID
	e.appendUint32(e.srid)

	// Number of points
	e.appendUint32(uint32(numPoints))

	// Coordinates
	for i := 0; i < len(coords); i += 2 {
		e.appendFloat64(coords[i])   // lon
		e.appendFloat64(coords[i+1]) // lat
	}

	return e.buf
}

// EncodePolygon encodes a polygon as EWKB with SRID
// coords is a flat array of [lon1, lat1, lon2, lat2, ...] for the outer ring
// (interior rings not yet supported)
func (e *Encoder) EncodePolygon(coords []float64) []byte {
	e.Reset()
	numPoints := len(coords) / 2
	// Size: 1 + 4 + 4 + 4 (num rings) + 4 (ring size) + (numPoints * 16)
	e.ensureCapacity(17 + numPoints*16)

	// Byte order (little-endian)
	e.buf = append(e.buf, 0x01)

	// Type with SRID flag
	e.appendUint32(wkbPolygon | wkbSRIDFlag)

	// SRID
	e.appendUint32(e.srid)

	// Number of rings (just outer ring for now)
	e.appendUint32(1)

	// Number of points in the ring
	e.appendUint32(uint32(numPoints))

	// Coordinates
	for i := 0; i < len(coords); i += 2 {
		e.appendFloat64(coords[i])   // lon
		e.appendFloat64(coords[i+1]) // lat
	}

	return e.buf
}

// EncodePolygonWithRings encodes a polygon with outer ring and optional inner rings (holes)
// Each ring is a flat array of [lon1, lat1, lon2, lat2, ...]
// rings[0] is outer ring, rings[1:] are inner rings (holes)
func (e *Encoder) EncodePolygonWithRings(rings [][]float64) []byte {
	e.Reset()
	if len(rings) == 0 {
		return nil
	}

	// Calculate total size
	totalPoints := 0
	for _, ring := range rings {
		totalPoints += len(ring) / 2
	}
	// Size: 1 + 4 + 4 + 4 (num rings) + len(rings)*4 (ring sizes) + (totalPoints * 16)
	e.ensureCapacity(13 + len(rings)*4 + totalPoints*16)

	// Byte order (little-endian)
	e.buf = append(e.buf, 0x01)

	// Type with SRID flag
	e.appendUint32(wkbPolygon | wkbSRIDFlag)

	// SRID
	e.appendUint32(e.srid)

	// Number of rings
	e.appendUint32(uint32(len(rings)))

	// Each ring
	for _, ring := range rings {
		numPoints := len(ring) / 2
		e.appendUint32(uint32(numPoints))
		for i := 0; i < len(ring); i += 2 {
			e.appendFloat64(ring[i])   // lon
			e.appendFloat64(ring[i+1]) // lat
		}
	}

	return e.buf
}

// EncodeMultiPolygon encodes multiple polygons as a MultiPolygon EWKB
// Each polygon is represented as a slice of rings (outer + inner rings)
func (e *Encoder) EncodeMultiPolygon(polygons [][][]float64) []byte {
	e.Reset()
	if len(polygons) == 0 {
		return nil
	}

	// Calculate total size (rough estimate for capacity)
	totalPoints := 0
	totalRings := 0
	for _, poly := range polygons {
		totalRings += len(poly)
		for _, ring := range poly {
			totalPoints += len(ring) / 2
		}
	}
	// Header: 1 + 4 + 4 + 4 (num polys)
	// Per polygon: 1 + 4 + 4 (num rings) + rings*4 + points*16
	e.ensureCapacity(13 + len(polygons)*9 + totalRings*4 + totalPoints*16)

	// Byte order (little-endian)
	e.buf = append(e.buf, 0x01)

	// Type with SRID flag
	e.appendUint32(wkbMultiPolygon | wkbSRIDFlag)

	// SRID
	e.appendUint32(e.srid)

	// Number of polygons
	e.appendUint32(uint32(len(polygons)))

	// Each polygon (without SRID - embedded polygons don't have SRID)
	for _, poly := range polygons {
		// Byte order
		e.buf = append(e.buf, 0x01)
		// Type (no SRID flag for embedded geometries)
		e.appendUint32(wkbPolygon)
		// Number of rings
		e.appendUint32(uint32(len(poly)))
		// Each ring
		for _, ring := range poly {
			numPoints := len(ring) / 2
			e.appendUint32(uint32(numPoints))
			for i := 0; i < len(ring); i += 2 {
				e.appendFloat64(ring[i])   // lon
				e.appendFloat64(ring[i+1]) // lat
			}
		}
	}

	return e.buf
}

func (e *Encoder) ensureCapacity(n int) {
	if cap(e.buf) < n {
		e.buf = make([]byte, 0, n)
	}
}

func (e *Encoder) appendUint32(v uint32) {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, v)
	e.buf = append(e.buf, b...)
}

func (e *Encoder) appendFloat64(v float64) {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, math.Float64bits(v))
	e.buf = append(e.buf, b...)
}
