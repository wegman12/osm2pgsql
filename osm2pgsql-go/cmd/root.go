package cmd

import (
	"os"

	"github.com/kevinramage/osm2pgsql-go/internal/config"
	"github.com/kevinramage/osm2pgsql-go/internal/logger"
	"github.com/spf13/cobra"
)

var (
	cfg     = config.DefaultConfig()
	verbose bool
)

var rootCmd = &cobra.Command{
	Use:   "osm2pgsql-go",
	Short: "High-performance OSM to PostgreSQL importer",
	Long: `osm2pgsql-go is a fast, parallel OSM data importer that uses
columnar processing (Parquet + DuckDB) to achieve significantly faster
import times compared to traditional row-by-row processing.

The import process has three stages:
  1. extract  - Parse PBF file and write to Parquet files (parallel)
  2. transform - Use DuckDB to join nodes with ways and build geometries
  3. load     - Bulk load geometries into PostgreSQL/PostGIS

You can run all stages with 'import' or run them individually.`,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		cfg.Verbose = verbose
		logger.Init(verbose)
	},
}

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	// Global flags
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Enable verbose output")
	rootCmd.PersistentFlags().StringVarP(&cfg.OutputDir, "output-dir", "o", cfg.OutputDir, "Directory for intermediate Parquet files")
	rootCmd.PersistentFlags().IntVarP(&cfg.Workers, "workers", "j", cfg.Workers, "Number of parallel workers")

	// Database flags (persistent so they're available to all subcommands)
	rootCmd.PersistentFlags().StringVar(&cfg.DBHost, "db-host", cfg.DBHost, "PostgreSQL host")
	rootCmd.PersistentFlags().IntVar(&cfg.DBPort, "db-port", cfg.DBPort, "PostgreSQL port")
	rootCmd.PersistentFlags().StringVarP(&cfg.DBName, "db-name", "d", cfg.DBName, "PostgreSQL database name")
	rootCmd.PersistentFlags().StringVarP(&cfg.DBUser, "db-user", "U", cfg.DBUser, "PostgreSQL user")
	rootCmd.PersistentFlags().StringVarP(&cfg.DBPassword, "db-password", "W", cfg.DBPassword, "PostgreSQL password")
	rootCmd.PersistentFlags().StringVar(&cfg.DBSchema, "db-schema", cfg.DBSchema, "PostgreSQL schema")
}

func exitWithError(msg string, err error) {
	if err != nil {
		logger.Error(msg, "error", err)
	} else {
		logger.Error(msg)
	}
	os.Exit(1)
}
