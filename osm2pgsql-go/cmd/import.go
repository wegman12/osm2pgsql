package cmd

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/kevinramage/osm2pgsql-go/internal/logger"
	"github.com/kevinramage/osm2pgsql-go/internal/pipeline"
	"github.com/spf13/cobra"
)

var (
	channelBuffer int
)

var importCmd = &cobra.Command{
	Use:   "import <input.osm.pbf>",
	Short: "Run full import pipeline (extract → load)",
	Long: `Run the complete OSM import pipeline with pipelined extraction and loading:

  1. Pass 1: Stream nodes into memory-mapped index (O(1) lookup)
  2. Pass 2: Stream ways/relations, build geometries, load directly to PostgreSQL

The pipelined architecture starts loading points while ways are still being
processed, significantly reducing total import time compared to sequential
extraction and loading.`,
	Args: cobra.ExactArgs(1),
	Run:  runImport,
}

func init() {
	rootCmd.AddCommand(importCmd)

	importCmd.Flags().BoolVar(&createIndexes, "create-indexes", true, "Create spatial indexes after loading")
	importCmd.Flags().BoolVar(&dropExisting, "drop-existing", false, "Drop existing tables before loading")
	importCmd.Flags().IntVar(&channelBuffer, "channel-buffer", 50000, "Buffer size for geometry channels")
}

func runImport(cmd *cobra.Command, args []string) {
	cfg.InputFile = args[0]
	log := logger.Get()

	if err := cfg.Validate(); err != nil {
		exitWithError("invalid configuration", err)
	}

	totalStart := time.Now()

	log.Info("Starting osm2pgsql-go pipelined import",
		zap.String("input", cfg.InputFile),
		zap.String("output", cfg.DBHost+":"+string(rune(cfg.DBPort))+"/"+cfg.DBName),
		zap.Int("workers", cfg.Workers),
		zap.Int("channel_buffer", channelBuffer),
	)

	// Create pipeline coordinator
	pipeCfg := pipeline.CoordinatorConfig{
		ChannelBuffer: channelBuffer,
		DropExisting:  dropExisting,
		CreateIndexes: createIndexes,
	}

	coordinator, err := pipeline.NewCoordinator(cfg, pipeCfg)
	if err != nil {
		exitWithError("failed to create pipeline", err)
	}
	defer coordinator.Close()

	// Run pipelined import
	ctx := context.Background()
	stats, err := coordinator.Run(ctx)
	if err != nil {
		exitWithError("import failed", err)
	}

	// Summary
	totalElapsed := time.Since(totalStart)

	log.Info("Import complete",
		zap.Duration("total_time", totalElapsed.Round(time.Second)),
		zap.Int64("nodes", stats.Extract.Nodes),
		zap.Int64("ways", stats.Extract.Ways),
		zap.Int64("relations", stats.Extract.Relations),
		zap.Int64("points", stats.PointsLoad.RowsLoaded),
		zap.Int64("lines", stats.LinesLoad.RowsLoaded),
		zap.Int64("polygons", stats.PolysLoad.RowsLoaded),
		zap.Int64("total_rows", stats.TotalRows),
		zap.Float64("throughput_mb_s", float64(stats.Extract.BytesRead)/(1024*1024)/totalElapsed.Seconds()),
	)
}
