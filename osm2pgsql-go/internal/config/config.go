package config

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// BBox represents a geographic bounding box
type BBox struct {
	MinLon, MinLat, MaxLon, MaxLat float64
	IsSet                          bool
}

// Contains checks if a point is within the bounding box
func (b *BBox) Contains(lat, lon float64) bool {
	if !b.IsSet {
		return true
	}
	return lon >= b.MinLon && lon <= b.MaxLon && lat >= b.MinLat && lat <= b.MaxLat
}

// ParseBBox parses a bbox string in format "minlon,minlat,maxlon,maxlat"
func ParseBBox(s string) (*BBox, error) {
	if s == "" {
		return &BBox{IsSet: false}, nil
	}

	parts := strings.Split(s, ",")
	if len(parts) != 4 {
		return nil, fmt.Errorf("bbox must have 4 values: minlon,minlat,maxlon,maxlat")
	}

	var coords [4]float64
	for i, p := range parts {
		v, err := strconv.ParseFloat(strings.TrimSpace(p), 64)
		if err != nil {
			return nil, fmt.Errorf("invalid bbox coordinate %q: %w", p, err)
		}
		coords[i] = v
	}

	bbox := &BBox{
		MinLon: coords[0],
		MinLat: coords[1],
		MaxLon: coords[2],
		MaxLat: coords[3],
		IsSet:  true,
	}

	// Validate
	if bbox.MinLon > bbox.MaxLon {
		return nil, fmt.Errorf("minlon (%f) must be <= maxlon (%f)", bbox.MinLon, bbox.MaxLon)
	}
	if bbox.MinLat > bbox.MaxLat {
		return nil, fmt.Errorf("minlat (%f) must be <= maxlat (%f)", bbox.MinLat, bbox.MaxLat)
	}

	return bbox, nil
}

// Config holds the global configuration for the import process
type Config struct {
	// Input settings
	InputFile string
	BBox      *BBox // Geographic bounding box filter

	// Output settings
	OutputDir  string
	Projection int    // Target SRID (4326 or 3857)
	StyleFile  string // Path to style YAML file for tag filtering

	// Database settings
	DBHost     string
	DBPort     int
	DBName     string
	DBUser     string
	DBPassword string
	DBSchema   string

	// Processing settings
	Workers    int
	BatchSize  int
	MemoryMB   int

	// Feature flags
	SkipNodes       bool
	SkipWays        bool
	SkipRelations   bool
	Verbose         bool
	ExtraAttributes bool   // Include changeset, timestamp, version, user columns
	Hstore          bool   // Use hstore instead of JSONB for tags
	FlatNodesFile   string // Path to flat nodes file (alternative to mmap)

	// Slim mode (middle tables for incremental updates)
	SlimMode   bool // Enable middle table storage
	AppendMode bool // Apply changes instead of full import
	DropMiddle bool // Drop middle tables after import

	// Tile expiry settings
	ExpireOutput  string // Path to expire tiles output file
	ExpireMinZoom int    // Minimum zoom level for tile expiry
	ExpireMaxZoom int    // Maximum zoom level for tile expiry

	// Tablespace settings
	TablespaceMain  string // Tablespace for main tables
	TablespaceIndex string // Tablespace for indexes

	// Logging and metrics
	LogFile         string        // Path to log file (empty = no file logging)
	MetricsInterval time.Duration // Interval for system metrics logging
}

// DefaultConfig returns a configuration with sensible defaults
func DefaultConfig() *Config {
	return &Config{
		OutputDir:  "./osm_data",
		Projection: 4326, // WGS84 by default
		DBHost:     "localhost",
		DBPort:     5432,
		DBName:     "osm",
		DBUser:     "postgres",
		DBPassword: "",
		DBSchema:   "public",
		Workers:         runtime.NumCPU(),
		BatchSize:       100000,
		MemoryMB:        50000, // 50GB default for DuckDB
		Verbose:         false,
		LogFile:         "",             // No file logging by default
		MetricsInterval: 30 * time.Second, // Log system metrics every 30 seconds
	}
}

// ConnectionString returns a PostgreSQL connection string
func (c *Config) ConnectionString() string {
	connStr := fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s sslmode=disable",
		c.DBHost, c.DBPort, c.DBName, c.DBUser,
	)
	if c.DBPassword != "" {
		connStr += fmt.Sprintf(" password=%s", c.DBPassword)
	}
	return connStr
}

// Validate checks that the configuration is valid
func (c *Config) Validate() error {
	if c.InputFile == "" {
		return fmt.Errorf("input file is required")
	}
	if c.Workers < 1 {
		return fmt.Errorf("workers must be at least 1")
	}
	if c.BatchSize < 1000 {
		return fmt.Errorf("batch size must be at least 1000")
	}
	return nil
}
