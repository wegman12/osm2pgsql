package config

import (
	"fmt"
	"runtime"
)

// Config holds the global configuration for the import process
type Config struct {
	// Input settings
	InputFile string

	// Output settings
	OutputDir string

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
	SkipNodes     bool
	SkipWays      bool
	SkipRelations bool
	Verbose       bool
}

// DefaultConfig returns a configuration with sensible defaults
func DefaultConfig() *Config {
	return &Config{
		OutputDir:  "./osm_data",
		DBHost:     "localhost",
		DBPort:     5432,
		DBName:     "osm",
		DBUser:     "postgres",
		DBPassword: "",
		DBSchema:   "public",
		Workers:    runtime.NumCPU(),
		BatchSize:  100000,
		MemoryMB:   50000, // 50GB default for DuckDB
		Verbose:    false,
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
