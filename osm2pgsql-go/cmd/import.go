package cmd

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/kevinramage/osm2pgsql-go/internal/config"
	"github.com/kevinramage/osm2pgsql-go/internal/logger"
	"github.com/kevinramage/osm2pgsql-go/internal/pipeline"
	"github.com/kevinramage/osm2pgsql-go/internal/proj"
	"github.com/spf13/cobra"
)

var (
	channelBuffer   int
	bboxStr         string
	projectionStr   string
	styleFile       string
	extraAttributes bool
	hstore          bool
	flatNodesFile   string
	tablespaceMain  string
	tablespaceIndex string
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
	importCmd.Flags().StringVarP(&bboxStr, "bbox", "b", "", "Bounding box filter: minlon,minlat,maxlon,maxlat")
	importCmd.Flags().StringVarP(&projectionStr, "projection", "E", "4326", "Target projection SRID (4326 or 3857)")
	importCmd.Flags().StringVarP(&styleFile, "style", "S", "", "Style YAML file for tag filtering")
	importCmd.Flags().BoolVar(&extraAttributes, "extra-attributes", false, "Include changeset, timestamp, version, user columns")
	importCmd.Flags().BoolVar(&hstore, "hstore", false, "Use hstore instead of JSONB for tags column")
	importCmd.Flags().StringVar(&flatNodesFile, "flat-nodes", "", "Path to flat nodes file (faster for large imports)")
	importCmd.Flags().StringVar(&tablespaceMain, "tablespace-main", "", "Tablespace for main tables")
	importCmd.Flags().StringVar(&tablespaceIndex, "tablespace-index", "", "Tablespace for indexes")
}

func runImport(cmd *cobra.Command, args []string) {
	cfg.InputFile = args[0]
	log := logger.Get()

	// Parse bounding box if provided
	if bboxStr != "" {
		bbox, err := config.ParseBBox(bboxStr)
		if err != nil {
			exitWithError("invalid bbox", err)
		}
		cfg.BBox = bbox
	}

	// Parse projection
	srid, err := proj.ParseSRID(projectionStr)
	if err != nil {
		exitWithError("invalid projection", err)
	}
	cfg.Projection = srid

	// Set style file
	cfg.StyleFile = styleFile

	// Set extra attributes
	cfg.ExtraAttributes = extraAttributes

	// Set hstore mode
	cfg.Hstore = hstore

	// Set flat nodes file
	cfg.FlatNodesFile = flatNodesFile

	// Set tablespace settings
	cfg.TablespaceMain = tablespaceMain
	cfg.TablespaceIndex = tablespaceIndex

	if err := cfg.Validate(); err != nil {
		exitWithError("invalid configuration", err)
	}

	totalStart := time.Now()

	// Build log fields
	logFields := []zap.Field{
		zap.String("input", cfg.InputFile),
		zap.String("output", fmt.Sprintf("%s:%d/%s", cfg.DBHost, cfg.DBPort, cfg.DBName)),
		zap.Int("workers", cfg.Workers),
		zap.Int("channel_buffer", channelBuffer),
		zap.Int("projection", cfg.Projection),
	}
	if cfg.BBox != nil && cfg.BBox.IsSet {
		logFields = append(logFields, zap.String("bbox",
			fmt.Sprintf("%.4f,%.4f,%.4f,%.4f", cfg.BBox.MinLon, cfg.BBox.MinLat, cfg.BBox.MaxLon, cfg.BBox.MaxLat)))
	}
	if cfg.StyleFile != "" {
		logFields = append(logFields, zap.String("style", cfg.StyleFile))
	}
	log.Info("Starting osm2pgsql-go pipelined import", logFields...)

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
