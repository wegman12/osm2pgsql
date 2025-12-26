package wkb

import (
	"encoding/binary"
	"math"
)

// WKB type constants (ISO SQL/MM specification)
const (
	wkbPoint      = 1
	wkbLineString = 2
	wkbPolygon    = 3

	// SRID flag for EWKB (PostGIS extended WKB)
	wkbSRIDFlag = 0x20000000
)

// SRID for WGS84
const SRID4326 = 4326

// Encoder encodes geometries to WKB format
// Uses little-endian byte order and includes SRID (EWKB format)
type Encoder struct {
	buf []byte
}

// NewEncoder creates a new WKB encoder with pre-allocated buffer
func NewEncoder(initialSize int) *Encoder {
	return &Encoder{
		buf: make([]byte, 0, initialSize),
	}
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
	e.appendUint32(SRID4326)

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
	e.appendUint32(SRID4326)

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
	e.appendUint32(SRID4326)

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
