package pipeline

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/kevinramage/osm2pgsql-go/internal/config"
	"github.com/kevinramage/osm2pgsql-go/internal/flex"
	"github.com/kevinramage/osm2pgsql-go/internal/logger"
	"github.com/kevinramage/osm2pgsql-go/internal/metrics"
	"github.com/kevinramage/osm2pgsql-go/internal/middle"
	"github.com/kevinramage/osm2pgsql-go/internal/osc"
)

// CoordinatorConfig holds pipeline-specific configuration
type CoordinatorConfig struct {
	ChannelBuffer int
	DropExisting  bool
	CreateIndexes bool
}

// Coordinator orchestrates the pipelined import
type Coordinator struct {
	cfg         *config.Config
	pipeCfg     CoordinatorConfig
	extractor   *StreamingExtractor
	loader      *StreamingLoader
	middleStore *middle.MiddleStore // Only set when SlimMode is enabled
}

// NewCoordinator creates a new pipeline coordinator
func NewCoordinator(cfg *config.Config, pipeCfg CoordinatorConfig) (*Coordinator, error) {
	if pipeCfg.ChannelBuffer <= 0 {
		pipeCfg.ChannelBuffer = 50000
	}

	extractor, err := NewStreamingExtractor(cfg, pipeCfg.ChannelBuffer)
	if err != nil {
		return nil, fmt.Errorf("failed to create extractor: %w", err)
	}

	loader, err := NewStreamingLoader(cfg)
	if err != nil {
		extractor.Close()
		return nil, fmt.Errorf("failed to create loader: %w", err)
	}

	coord := &Coordinator{
		cfg:       cfg,
		pipeCfg:   pipeCfg,
		extractor: extractor,
		loader:    loader,
	}

	// Create MiddleStore when slim mode is enabled (shares loader's pool)
	if cfg.SlimMode {
		coord.middleStore = middle.NewMiddleStore(cfg, loader.Pool())
	}

	return coord, nil
}

// Close cleans up resources
func (c *Coordinator) Close() error {
	var errs []error
	if c.extractor != nil {
		if err := c.extractor.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if c.loader != nil {
		if err := c.loader.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// Run executes the pipelined import
func (c *Coordinator) Run(ctx context.Context) (*ImportStats, error) {
	log := logger.Get()
	stats := &ImportStats{}

	// Start metrics collection in background if interval is set
	if c.cfg.MetricsInterval > 0 {
		metricsCtx, cancelMetrics := context.WithCancel(ctx)
		defer cancelMetrics()

		collector := metrics.NewCollector(c.cfg.MetricsInterval, log)
		go collector.Start(metricsCtx)
		log.Info("System metrics collection started",
			zap.Duration("interval", c.cfg.MetricsInterval))
	}

	// Ensure PostGIS and schema exist
	if err := c.loader.EnsureSchema(ctx); err != nil {
		return nil, err
	}

	// Prepare tables before starting extraction
	tables := []string{"planet_osm_point", "planet_osm_line", "planet_osm_polygon"}
	for _, table := range tables {
		if err := c.loader.PrepareTable(ctx, table, c.pipeCfg.DropExisting); err != nil {
			return nil, fmt.Errorf("failed to prepare table %s: %w", table, err)
		}
	}

	// Prepare middle tables when slim mode is enabled
	if c.cfg.SlimMode && c.middleStore != nil {
		log.Info("Slim mode enabled - preparing middle tables")
		if err := c.middleStore.EnsureTables(ctx, c.pipeCfg.DropExisting); err != nil {
			return nil, fmt.Errorf("failed to prepare middle tables: %w", err)
		}
	}

	// Start extraction (Pass 1 runs synchronously, Pass 2 starts in background)
	extractStart := time.Now()
	streams, err := c.extractor.Run(ctx)
	if err != nil {
		return nil, fmt.Errorf("extraction failed: %w", err)
	}

	// Reset live stats start time
	c.loader.liveStats.StartTime = time.Now()

	// Start live progress reporter
	progressCtx, cancelProgress := context.WithCancel(ctx)
	defer cancelProgress()
	go c.reportLiveProgress(progressCtx)

	// Create errgroup for concurrent loading
	g, gctx := errgroup.WithContext(ctx)

	// Track load stats
	var pointsCount, linesCount, polysCount int64

	// Start points loader
	g.Go(func() error {
		count, err := c.loader.LoadStream(gctx, "planet_osm_point", streams.Points)
		if err != nil {
			return fmt.Errorf("points load failed: %w", err)
		}
		pointsCount = count
		return nil
	})

	// Start lines loader
	g.Go(func() error {
		count, err := c.loader.LoadStream(gctx, "planet_osm_line", streams.Lines)
		if err != nil {
			return fmt.Errorf("lines load failed: %w", err)
		}
		linesCount = count
		return nil
	})

	// Start polygons loader
	g.Go(func() error {
		count, err := c.loader.LoadStream(gctx, "planet_osm_polygon", streams.Polygons)
		if err != nil {
			return fmt.Errorf("polygons load failed: %w", err)
		}
		polysCount = count
		return nil
	})

	// Monitor for extraction errors
	g.Go(func() error {
		select {
		case err := <-streams.Errors:
			if err != nil {
				return fmt.Errorf("extraction error: %w", err)
			}
		case <-gctx.Done():
		}
		return nil
	})

	// Start middle table loaders when slim mode is enabled
	var middleNodesCount, middleWaysCount, middleRelsCount int64
	if c.cfg.SlimMode && c.middleStore != nil && streams.RawNodes != nil {
		// Load raw nodes to middle table
		g.Go(func() error {
			count, err := c.middleStore.LoadNodes(gctx, streams.RawNodes)
			if err != nil {
				return fmt.Errorf("middle nodes load failed: %w", err)
			}
			middleNodesCount = count
			return nil
		})

		// Load raw ways to middle table
		g.Go(func() error {
			count, err := c.middleStore.LoadWays(gctx, streams.RawWays)
			if err != nil {
				return fmt.Errorf("middle ways load failed: %w", err)
			}
			middleWaysCount = count
			return nil
		})

		// Load raw relations to middle table
		g.Go(func() error {
			count, err := c.middleStore.LoadRelations(gctx, streams.RawRelations)
			if err != nil {
				return fmt.Errorf("middle relations load failed: %w", err)
			}
			middleRelsCount = count
			return nil
		})
	}

	// Wait for all loaders to complete
	if err := g.Wait(); err != nil {
		return nil, err
	}

	extractElapsed := time.Since(extractStart)

	// Populate stats from extractor
	extractStats := c.extractor.Stats()
	stats.Extract = *extractStats
	stats.PointsLoad = LoadStats{Table: "planet_osm_point", RowsLoaded: pointsCount}
	stats.LinesLoad = LoadStats{Table: "planet_osm_line", RowsLoaded: linesCount}
	stats.PolysLoad = LoadStats{Table: "planet_osm_polygon", RowsLoaded: polysCount}
	stats.TotalRows = pointsCount + linesCount + polysCount

	logFields := []zap.Field{
		zap.Int64("nodes", stats.Extract.Nodes),
		zap.Int64("ways", stats.Extract.Ways),
		zap.Int64("relations", stats.Extract.Relations),
		zap.Int64("points", pointsCount),
		zap.Int64("lines", linesCount),
		zap.Int64("polygons", polysCount),
		zap.Duration("duration", extractElapsed.Round(time.Second)),
	}
	if c.cfg.SlimMode {
		logFields = append(logFields,
			zap.Int64("middle_nodes", middleNodesCount),
			zap.Int64("middle_ways", middleWaysCount),
			zap.Int64("middle_relations", middleRelsCount),
		)
	}
	log.Info("Extraction and loading complete", logFields...)

	// Create indexes in parallel
	if c.pipeCfg.CreateIndexes {
		indexStart := time.Now()
		log.Info("Creating indexes in parallel")

		ig, igctx := errgroup.WithContext(ctx)
		for _, table := range tables {
			table := table // capture for goroutine
			ig.Go(func() error {
				return c.loader.CreateIndexes(igctx, table)
			})
		}

		if err := ig.Wait(); err != nil {
			return nil, fmt.Errorf("index creation failed: %w", err)
		}

		log.Info("All indexes created", zap.Duration("duration", time.Since(indexStart).Round(time.Second)))
	}

	// Create middle table indexes when slim mode is enabled
	if c.cfg.SlimMode && c.middleStore != nil && c.pipeCfg.CreateIndexes {
		log.Info("Creating middle table indexes")
		if err := c.middleStore.CreateIndexes(ctx); err != nil {
			return nil, fmt.Errorf("middle table index creation failed: %w", err)
		}
	}

	// Drop middle tables if --drop flag is set
	if c.cfg.DropMiddle && c.middleStore != nil {
		log.Info("Dropping middle tables (--drop flag)")
		if err := c.middleStore.DropTables(ctx); err != nil {
			return nil, fmt.Errorf("failed to drop middle tables: %w", err)
		}
	}

	return stats, nil
}

// reportLiveProgress periodically logs live loading progress
func (c *Coordinator) reportLiveProgress(ctx context.Context) {
	log := logger.Get()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	var lastPoints, lastLines, lastPolys int64
	lastTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			points, lines, polys := c.loader.liveStats.GetStats()
			now := time.Now()
			elapsed := now.Sub(lastTime).Seconds()

			// Calculate instantaneous rates (last 5 seconds)
			var pointsRate, linesRate, polysRate float64
			if elapsed > 0 {
				pointsRate = float64(points-lastPoints) / elapsed
				linesRate = float64(lines-lastLines) / elapsed
				polysRate = float64(polys-lastPolys) / elapsed
			}

			// Get channel buffer utilization
			wayCacheSize := c.extractor.wayCacheSize.Load()

			total := points + lines + polys
			totalRate := pointsRate + linesRate + polysRate

			log.Info("Loading progress",
				zap.Int64("points", points),
				zap.Int64("lines", lines),
				zap.Int64("polygons", polys),
				zap.Int64("total", total),
				zap.String("points_rate", FormatThroughput(pointsRate)),
				zap.String("lines_rate", FormatThroughput(linesRate)),
				zap.String("polys_rate", FormatThroughput(polysRate)),
				zap.String("total_rate", FormatThroughput(totalRate)),
				zap.Int64("way_cache", wayCacheSize),
			)

			lastPoints, lastLines, lastPolys = points, lines, polys
			lastTime = now
		}
	}
}

// RunAppend applies changes from an OSC file to existing data
func (c *Coordinator) RunAppend(ctx context.Context, oscFile string) (*AppendStats, error) {
	log := logger.Get()

	// Verify slim mode is enabled (required for append)
	if c.middleStore == nil {
		return nil, fmt.Errorf("append mode requires slim mode (--slim) to be used during initial import")
	}

	log.Info("Starting append mode", zap.String("osc_file", oscFile))

	// Parse OSC file
	parser := osc.NewParser()
	changes, errChan := parser.ParseFile(ctx, oscFile)

	// Create append processor
	processor := NewAppendProcessor(c.cfg, c.loader.Pool(), c.middleStore)

	// Process changes with error handling
	var parseErr error
	go func() {
		for err := range errChan {
			if err != nil {
				parseErr = err
			}
		}
	}()

	stats, err := processor.ProcessChanges(ctx, changes)
	if err != nil {
		return nil, fmt.Errorf("append processing failed: %w", err)
	}

	if parseErr != nil {
		return nil, fmt.Errorf("OSC parsing failed: %w", parseErr)
	}

	// Write expire tiles if configured
	if c.cfg.ExpireOutput != "" {
		tracker := processor.ExpireTracker()
		if tracker != nil {
			if err := tracker.WriteToFile(c.cfg.ExpireOutput); err != nil {
				return nil, fmt.Errorf("failed to write expire tiles: %w", err)
			}
		}
	}

	// Log parser stats
	parserStats := parser.Stats()
	log.Info("OSC file parsed",
		zap.Int64("nodes_created", parserStats.NodesCreated),
		zap.Int64("nodes_modified", parserStats.NodesModified),
		zap.Int64("nodes_deleted", parserStats.NodesDeleted),
		zap.Int64("ways_created", parserStats.WaysCreated),
		zap.Int64("ways_modified", parserStats.WaysModified),
		zap.Int64("ways_deleted", parserStats.WaysDeleted),
		zap.Int64("relations_created", parserStats.RelationsCreated),
		zap.Int64("relations_modified", parserStats.RelationsModified),
		zap.Int64("relations_deleted", parserStats.RelationsDeleted),
		zap.Int64("total", parserStats.Total()),
	)

	return stats, nil
}

// FlexStats holds statistics from Flex mode import
type FlexStats struct {
	BytesRead          int64
	NodesProcessed     int64
	WaysProcessed      int64
	RelationsProcessed int64
	RowsInserted       int64
	Tables             []string
}

// RunFlex executes a Lua Flex style import with parallel processing
func (c *Coordinator) RunFlex(ctx context.Context, luaFile string) (*FlexStats, error) {
	log := logger.Get()

	// Start metrics collection in background
	if c.cfg.MetricsInterval > 0 {
		metricsCtx, cancelMetrics := context.WithCancel(ctx)
		defer cancelMetrics()

		collector := metrics.NewCollector(c.cfg.MetricsInterval, log)
		go collector.Start(metricsCtx)
	}

	// Create parallel Flex processor with worker pool
	numWorkers := c.cfg.Workers
	if numWorkers <= 0 {
		numWorkers = 4
	}

	processor, err := flex.NewParallelProcessor(c.cfg, c.loader.Pool(), luaFile, numWorkers)
	if err != nil {
		return nil, fmt.Errorf("failed to create flex processor: %w", err)
	}

	// Log defined tables
	tables := processor.Tables()
	tableNames := make([]string, len(tables))
	for i, t := range tables {
		tableNames[i] = t.Name
		log.Info("Flex table defined",
			zap.String("table", t.Name),
			zap.Int("columns", len(t.Columns)),
			zap.String("geom_column", t.GeomColumn))
	}

	// Ensure schema exists
	if err := c.loader.EnsureSchema(ctx); err != nil {
		return nil, err
	}

	// Create output tables
	if err := processor.EnsureTables(ctx, c.pipeCfg.DropExisting); err != nil {
		return nil, fmt.Errorf("failed to create tables: %w", err)
	}

	// Start the parallel processor workers
	if err := processor.Start(ctx); err != nil {
		return nil, fmt.Errorf("failed to start processor: %w", err)
	}

	// Create Flex extractor
	extractor, err := flex.NewFlexExtractor(c.cfg, luaFile, c.pipeCfg.ChannelBuffer)
	if err != nil {
		processor.Close(ctx)
		return nil, fmt.Errorf("failed to create flex extractor: %w", err)
	}
	defer extractor.Close()

	// Run extraction
	extractStart := time.Now()
	streams, err := extractor.Run(ctx)
	if err != nil {
		processor.Close(ctx)
		return nil, fmt.Errorf("extraction failed: %w", err)
	}

	// Process objects through parallel Lua workers
	log.Info("Processing OSM objects through parallel Lua workers",
		zap.Int("workers", numWorkers))

	// Create errgroup for concurrent processing
	g, gctx := errgroup.WithContext(ctx)

	// Submit objects to the parallel processor
	g.Go(func() error {
		for obj := range streams.Objects {
			select {
			case <-gctx.Done():
				return gctx.Err()
			default:
				processor.Submit(obj)
			}
		}
		return nil
	})

	// Monitor for extraction errors
	g.Go(func() error {
		for err := range streams.Errors {
			if err != nil {
				return fmt.Errorf("extraction error: %w", err)
			}
		}
		return nil
	})

	// Wait for extraction to complete
	if err := g.Wait(); err != nil {
		processor.Close(ctx)
		return nil, err
	}

	// Close processor and flush remaining batches
	processor.Close(ctx)

	extractElapsed := time.Since(extractStart)
	procStats := processor.Stats()

	log.Info("Flex processing complete",
		zap.Int64("nodes_processed", procStats.NodesProcessed),
		zap.Int64("ways_processed", procStats.WaysProcessed),
		zap.Int64("relations_processed", procStats.RelationsProcessed),
		zap.Int64("rows_inserted", procStats.RowsInserted),
		zap.Duration("duration", extractElapsed.Round(time.Second)))

	// Create indexes
	if c.pipeCfg.CreateIndexes {
		indexStart := time.Now()
		log.Info("Creating indexes")
		if err := processor.CreateIndexes(ctx); err != nil {
			return nil, fmt.Errorf("index creation failed: %w", err)
		}
		log.Info("Indexes created", zap.Duration("duration", time.Since(indexStart).Round(time.Second)))
	}

	return &FlexStats{
		BytesRead:          extractor.Stats().BytesRead,
		NodesProcessed:     procStats.NodesProcessed,
		WaysProcessed:      procStats.WaysProcessed,
		RelationsProcessed: procStats.RelationsProcessed,
		RowsInserted:       procStats.RowsInserted,
		Tables:             tableNames,
	}, nil
}
