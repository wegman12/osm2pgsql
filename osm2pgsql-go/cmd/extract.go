package cmd

import (
	"time"

	"go.uber.org/zap"

	"github.com/kevinramage/osm2pgsql-go/internal/logger"
	"github.com/kevinramage/osm2pgsql-go/internal/pbf"
	"github.com/spf13/cobra"
)

var extractCmd = &cobra.Command{
	Use:   "extract <input.osm.pbf>",
	Short: "Extract OSM data from PBF to Parquet files",
	Long: `Parse an OSM PBF file in parallel and write the data to Parquet files.

This stage extracts:
  - nodes.parquet     (id, lat, lon, tags)
  - ways.parquet      (id, tags)
  - way_nodes.parquet (way_id, seq, node_id)
  - relations.parquet (id, tags)
  - relation_members.parquet (relation_id, seq, type, ref, role)

The PBF file is processed in parallel blocks for maximum throughput.`,
	Args: cobra.ExactArgs(1),
	Run:  runExtract,
}

func init() {
	rootCmd.AddCommand(extractCmd)

	extractCmd.Flags().IntVar(&cfg.BatchSize, "batch-size", cfg.BatchSize, "Rows per Parquet row group")
	extractCmd.Flags().BoolVar(&cfg.SkipNodes, "skip-nodes", false, "Skip node extraction")
	extractCmd.Flags().BoolVar(&cfg.SkipWays, "skip-ways", false, "Skip way extraction")
	extractCmd.Flags().BoolVar(&cfg.SkipRelations, "skip-relations", false, "Skip relation extraction")
}

func runExtract(cmd *cobra.Command, args []string) {
	cfg.InputFile = args[0]
	log := logger.Get()

	if err := cfg.Validate(); err != nil {
		exitWithError("invalid configuration", err)
	}

	log.Info("Starting PBF extraction",
		zap.String("input", cfg.InputFile),
		zap.String("output", cfg.OutputDir),
		zap.Int("workers", cfg.Workers),
	)

	start := time.Now()

	extractor, err := pbf.NewExtractor(cfg)
	if err != nil {
		exitWithError("failed to create extractor", err)
	}
	defer extractor.Close()

	stats, err := extractor.Run()
	if err != nil {
		exitWithError("extraction failed", err)
	}

	elapsed := time.Since(start)

	log.Info("Extraction complete",
		zap.Duration("duration", elapsed.Round(time.Second)),
		zap.Int64("nodes", stats.Nodes),
		zap.Int64("ways", stats.Ways),
		zap.Int64("relations", stats.Relations),
		zap.Float64("throughput_mb_s", float64(stats.BytesRead)/(1024*1024)/elapsed.Seconds()),
	)
}
