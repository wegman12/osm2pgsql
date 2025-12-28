package cmd

import (
	"context"
	"fmt"
	"strings"
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
	slimMode        bool
	appendMode      bool
	dropMiddle      bool
	expireOutput    string
	expireMinZoom   int
	expireMaxZoom   int
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
	importCmd.Flags().BoolVar(&slimMode, "slim", false, "Enable slim mode (store raw OSM data for incremental updates)")
	importCmd.Flags().BoolVar(&appendMode, "append", false, "Apply OSC file as update (requires existing slim tables)")
	importCmd.Flags().BoolVar(&dropMiddle, "drop", false, "Drop slim tables after import")
	importCmd.Flags().StringVarP(&expireOutput, "expire-output", "e", "", "Path to expire tiles output file")
	importCmd.Flags().IntVar(&expireMinZoom, "expire-min-zoom", 1, "Minimum zoom level for tile expiry")
	importCmd.Flags().IntVar(&expireMaxZoom, "expire-max-zoom", 18, "Maximum zoom level for tile expiry")
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

	// Set slim mode settings
	cfg.SlimMode = slimMode
	cfg.AppendMode = appendMode
	cfg.DropMiddle = dropMiddle

	// Set expire settings
	cfg.ExpireOutput = expireOutput
	cfg.ExpireMinZoom = expireMinZoom
	cfg.ExpireMaxZoom = expireMaxZoom

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
	if cfg.SlimMode {
		logFields = append(logFields, zap.Bool("slim", true))
	}
	if cfg.AppendMode {
		logFields = append(logFields, zap.Bool("append", true))
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

	ctx := context.Background()

	// Check if append mode
	if cfg.AppendMode {
		// Append mode: apply OSC changes
		appendStats, err := coordinator.RunAppend(ctx, cfg.InputFile)
		if err != nil {
			exitWithError("append failed", err)
		}

		totalElapsed := time.Since(totalStart)
		log.Info("Append complete",
			zap.Duration("total_time", totalElapsed.Round(time.Second)),
			zap.Int64("nodes_processed", appendStats.NodesProcessed),
			zap.Int64("ways_processed", appendStats.WaysProcessed),
			zap.Int64("relations_processed", appendStats.RelationsProcessed),
			zap.Int64("ways_rebuilt", appendStats.WaysRebuilt),
			zap.Int64("relations_rebuilt", appendStats.RelationsRebuilt),
			zap.Int64("points_updated", appendStats.PointsUpdated),
			zap.Int64("lines_updated", appendStats.LinesUpdated),
			zap.Int64("polygons_updated", appendStats.PolygonsUpdated),
		)
		return
	}

	// Check if using Lua Flex style
	isLuaStyle := strings.HasSuffix(strings.ToLower(cfg.StyleFile), ".lua")
	if isLuaStyle {
		// Flex mode: use Lua style for custom table definitions
		log.Info("Using Lua Flex style", zap.String("style", cfg.StyleFile))
		flexStats, err := coordinator.RunFlex(ctx, cfg.StyleFile)
		if err != nil {
			exitWithError("flex import failed", err)
		}

		totalElapsed := time.Since(totalStart)
		log.Info("Flex import complete",
			zap.Duration("total_time", totalElapsed.Round(time.Second)),
			zap.Int64("nodes_processed", flexStats.NodesProcessed),
			zap.Int64("ways_processed", flexStats.WaysProcessed),
			zap.Int64("relations_processed", flexStats.RelationsProcessed),
			zap.Int64("rows_inserted", flexStats.RowsInserted),
			zap.Strings("tables", flexStats.Tables),
			zap.Float64("throughput_mb_s", float64(flexStats.BytesRead)/(1024*1024)/totalElapsed.Seconds()),
		)
		return
	}

	// Normal import mode
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
