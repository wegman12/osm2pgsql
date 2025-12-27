package pipeline

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/kevinramage/osm2pgsql-go/internal/config"
	"github.com/kevinramage/osm2pgsql-go/internal/logger"
)

// CoordinatorConfig holds pipeline-specific configuration
type CoordinatorConfig struct {
	ChannelBuffer int
	DropExisting  bool
	CreateIndexes bool
}

// Coordinator orchestrates the pipelined import
type Coordinator struct {
	cfg      *config.Config
	pipeCfg  CoordinatorConfig
	extractor *StreamingExtractor
	loader    *StreamingLoader
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

	return &Coordinator{
		cfg:       cfg,
		pipeCfg:   pipeCfg,
		extractor: extractor,
		loader:    loader,
	}, nil
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

	// Start extraction (Pass 1 runs synchronously, Pass 2 starts in background)
	extractStart := time.Now()
	streams, err := c.extractor.Run(ctx)
	if err != nil {
		return nil, fmt.Errorf("extraction failed: %w", err)
	}

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

	log.Info("Extraction and loading complete",
		zap.Int64("nodes", stats.Extract.Nodes),
		zap.Int64("ways", stats.Extract.Ways),
		zap.Int64("relations", stats.Extract.Relations),
		zap.Int64("points", pointsCount),
		zap.Int64("lines", linesCount),
		zap.Int64("polygons", polysCount),
		zap.Duration("duration", extractElapsed.Round(time.Second)),
	)

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

	return stats, nil
}
