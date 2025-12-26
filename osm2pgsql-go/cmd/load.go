package cmd

import (
	"time"

	"github.com/kevinramage/osm2pgsql-go/internal/loader"
	"github.com/kevinramage/osm2pgsql-go/internal/logger"
	"github.com/spf13/cobra"
)

var (
	createIndexes bool
	dropExisting  bool
)

var loadCmd = &cobra.Command{
	Use:   "load",
	Short: "Load transformed geometries into PostgreSQL",
	Long: `Bulk load geometry Parquet files into PostgreSQL/PostGIS.

This stage:
  1. Creates target tables (planet_osm_point, planet_osm_line, planet_osm_polygon)
  2. Uses COPY for high-speed bulk loading
  3. Optionally creates spatial indexes

The loader uses parallel COPY streams for maximum throughput.`,
	Run: runLoad,
}

func init() {
	rootCmd.AddCommand(loadCmd)

	loadCmd.Flags().BoolVar(&createIndexes, "create-indexes", true, "Create spatial indexes after loading")
	loadCmd.Flags().BoolVar(&dropExisting, "drop-existing", false, "Drop existing tables before loading")
}

func runLoad(cmd *cobra.Command, args []string) {
	log := logger.Get()
	log.Info("Starting PostgreSQL load",
		"input_dir", cfg.OutputDir,
		"database", cfg.DBName,
		"host", cfg.DBHost,
		"port", cfg.DBPort,
		"user", cfg.DBUser,
		"schema", cfg.DBSchema,
	)

	start := time.Now()

	ldr, err := loader.NewLoader(cfg, dropExisting, createIndexes)
	if err != nil {
		exitWithError("failed to create loader", err)
	}
	defer ldr.Close()

	stats, err := ldr.Run()
	if err != nil {
		exitWithError("load failed", err)
	}

	elapsed := time.Since(start)

	log.Info("Load complete",
		"duration", elapsed.Round(time.Second),
		"rows", stats.RowsLoaded,
		"throughput_rows_s", float64(stats.RowsLoaded)/elapsed.Seconds(),
	)
}
