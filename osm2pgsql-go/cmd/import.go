package cmd

import (
	"time"

	"github.com/kevinramage/osm2pgsql-go/internal/loader"
	"github.com/kevinramage/osm2pgsql-go/internal/logger"
	"github.com/kevinramage/osm2pgsql-go/internal/pbf"
	"github.com/spf13/cobra"
)

var (
	keepIntermediate bool
)

var importCmd = &cobra.Command{
	Use:   "import <input.osm.pbf>",
	Short: "Run full import pipeline (extract → load)",
	Long: `Run the complete OSM import pipeline:

  1. Extract: Parse PBF file, build geometries using mmap node index
  2. Load: Bulk load geometries into PostgreSQL

The new architecture uses a two-pass approach:
  - Pass 1: Stream nodes into memory-mapped index (O(1) lookup)
  - Pass 2: Stream ways, lookup coords from mmap, build geometries

This eliminates the expensive DuckDB join and provides 10-100x speedup.`,
	Args: cobra.ExactArgs(1),
	Run:  runImport,
}

func init() {
	rootCmd.AddCommand(importCmd)

	importCmd.Flags().IntVar(&cfg.BatchSize, "batch-size", cfg.BatchSize, "Rows per Parquet row group")
	importCmd.Flags().BoolVar(&createIndexes, "create-indexes", true, "Create spatial indexes after loading")
	importCmd.Flags().BoolVar(&dropExisting, "drop-existing", false, "Drop existing tables before loading")
	importCmd.Flags().BoolVar(&keepIntermediate, "keep-intermediate", false, "Keep intermediate Parquet files")
}

func runImport(cmd *cobra.Command, args []string) {
	cfg.InputFile = args[0]
	log := logger.Get()

	if err := cfg.Validate(); err != nil {
		exitWithError("invalid configuration", err)
	}

	totalStart := time.Now()

	log.Info("Starting osm2pgsql-go import",
		"input", cfg.InputFile,
		"output", cfg.DBHost+":"+string(rune(cfg.DBPort))+"/"+cfg.DBName,
		"workers", cfg.Workers,
	)

	// Stage 1: Extract (two-pass with mmap)
	log.Info("Stage 1: Extract (two-pass with mmap node index)")

	extractStart := time.Now()

	extractor, err := pbf.NewExtractor(cfg)
	if err != nil {
		exitWithError("failed to create extractor", err)
	}

	extractStats, err := extractor.Run()
	extractor.Close()
	if err != nil {
		exitWithError("extraction failed", err)
	}

	extractElapsed := time.Since(extractStart)
	log.Info("Extraction complete",
		"nodes", extractStats.Nodes,
		"ways", extractStats.Ways,
		"relations", extractStats.Relations,
		"duration", extractElapsed.Round(time.Second),
		"throughput_mb_s", float64(extractStats.BytesRead)/(1024*1024)/extractElapsed.Seconds(),
	)

	// Stage 2: Load
	log.Info("Stage 2: Load (PostgreSQL bulk insert)")

	loadStart := time.Now()

	ldr, err := loader.NewLoader(cfg, dropExisting, createIndexes)
	if err != nil {
		exitWithError("failed to create loader", err)
	}

	loadStats, err := ldr.Run()
	ldr.Close()
	if err != nil {
		exitWithError("load failed", err)
	}

	loadElapsed := time.Since(loadStart)
	log.Info("Load complete",
		"rows", loadStats.RowsLoaded,
		"duration", loadElapsed.Round(time.Second),
	)

	// Summary
	totalElapsed := time.Since(totalStart)

	log.Info("Import complete",
		"total_time", totalElapsed.Round(time.Second),
		"extract_time", extractElapsed.Round(time.Second),
		"extract_pct", 100*extractElapsed.Seconds()/totalElapsed.Seconds(),
		"load_time", loadElapsed.Round(time.Second),
		"load_pct", 100*loadElapsed.Seconds()/totalElapsed.Seconds(),
		"throughput_mb_s", float64(extractStats.BytesRead)/(1024*1024)/totalElapsed.Seconds(),
	)
}
