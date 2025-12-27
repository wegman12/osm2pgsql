package cmd

import (
	"time"

	"go.uber.org/zap"

	"github.com/kevinramage/osm2pgsql-go/internal/logger"
	"github.com/kevinramage/osm2pgsql-go/internal/transform"
	"github.com/spf13/cobra"
)

var transformCmd = &cobra.Command{
	Use:   "transform",
	Short: "Transform extracted data into geometries using DuckDB",
	Long: `Use DuckDB to join nodes with ways and construct PostGIS geometries.

This stage performs:
  1. Hash join of way_nodes with nodes to resolve coordinates
  2. Aggregation of coordinates into linestrings/polygons
  3. Tag filtering and transformation
  4. Output to geometry Parquet files ready for PostgreSQL

DuckDB provides fast, parallel SQL execution with automatic memory management.`,
	Run: runTransform,
}

func init() {
	rootCmd.AddCommand(transformCmd)

	transformCmd.Flags().IntVar(&cfg.MemoryMB, "memory", cfg.MemoryMB, "Memory limit for DuckDB in MB")
}

func runTransform(cmd *cobra.Command, args []string) {
	log := logger.Get()
	log.Info("Starting geometry transformation",
		zap.String("input_dir", cfg.OutputDir),
		zap.Int("memory_mb", cfg.MemoryMB),
		zap.Int("workers", cfg.Workers),
	)

	start := time.Now()

	transformer, err := transform.NewTransformer(cfg)
	if err != nil {
		exitWithError("failed to create transformer", err)
	}
	defer transformer.Close()

	stats, err := transformer.Run()
	if err != nil {
		exitWithError("transformation failed", err)
	}

	elapsed := time.Since(start)

	log.Info("Transformation complete",
		zap.Duration("duration", elapsed.Round(time.Second)),
		zap.Int64("points", stats.Points),
		zap.Int64("lines", stats.Lines),
		zap.Int64("polygons", stats.Polygons),
	)
}
