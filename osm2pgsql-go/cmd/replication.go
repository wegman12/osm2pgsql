package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/kevinramage/osm2pgsql-go/internal/logger"
	"github.com/kevinramage/osm2pgsql-go/internal/pipeline"
	"github.com/kevinramage/osm2pgsql-go/internal/replication"
	"github.com/spf13/cobra"
)

var (
	replicationSource   string
	replicationInterval time.Duration
	maxUpdates          int
	catchUp             bool
)

var replicationCmd = &cobra.Command{
	Use:   "replication",
	Short: "Manage OSM replication for incremental updates",
	Long: `Manage OSM replication to keep your database in sync with OpenStreetMap.

Replication sources include:
  - planet-minute, planet-hour, planet-day (OpenStreetMap planet)
  - geofabrik/<region> (e.g., geofabrik/monaco, geofabrik/germany)
  - Custom URL (https://your-server/replication)

Examples:
  # Initialize replication from Geofabrik Monaco
  osm2pgsql-go replication init --source geofabrik/monaco

  # Check replication status
  osm2pgsql-go replication status

  # Apply a single update
  osm2pgsql-go replication update

  # Start continuous replication
  osm2pgsql-go replication start --interval 5m`,
}

var replicationInitCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize replication from a source",
	Long: `Initialize replication by downloading the current state from the source.

This command:
  1. Connects to the replication source
  2. Downloads the current state file (sequence number and timestamp)
  3. Saves the state locally for future updates

After initialization, use 'replication update' or 'replication start' to apply updates.`,
	Run: runReplicationInit,
}

var replicationStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show current replication status",
	Long: `Display the current replication status including:
  - Local sequence number and timestamp
  - Remote sequence number and timestamp
  - Number of updates behind
  - Time lag`,
	Run: runReplicationStatus,
}

var replicationUpdateCmd = &cobra.Command{
	Use:   "update",
	Short: "Apply the next replication update",
	Long: `Fetch and apply the next pending replication update.

This command:
  1. Checks for available updates
  2. Downloads the next OSC (change) file
  3. Applies the changes to the database
  4. Updates the local state

Use --catch-up to apply all pending updates until caught up.`,
	Run: runReplicationUpdate,
}

var replicationStartCmd = &cobra.Command{
	Use:   "start",
	Short: "Start continuous replication",
	Long: `Start a continuous replication loop that:
  1. Checks for new updates periodically
  2. Downloads and applies available updates
  3. Continues until interrupted (Ctrl+C)

Use --interval to control how often to check for updates.
Use --max-updates to limit the number of updates to apply (0 = unlimited).`,
	Run: runReplicationStart,
}

var replicationListCmd = &cobra.Command{
	Use:   "list-sources",
	Short: "List available replication sources",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Available replication sources:")
		fmt.Println()
		for _, source := range replication.ListSources() {
			fmt.Println(source)
		}
	},
}

func init() {
	rootCmd.AddCommand(replicationCmd)

	// Add subcommands
	replicationCmd.AddCommand(replicationInitCmd)
	replicationCmd.AddCommand(replicationStatusCmd)
	replicationCmd.AddCommand(replicationUpdateCmd)
	replicationCmd.AddCommand(replicationStartCmd)
	replicationCmd.AddCommand(replicationListCmd)

	// Common flags for all replication commands
	replicationCmd.PersistentFlags().StringVar(&replicationSource, "source", "", "Replication source (e.g., geofabrik/monaco, planet-minute)")

	// Update command flags
	replicationUpdateCmd.Flags().BoolVar(&catchUp, "catch-up", false, "Apply all pending updates until caught up")

	// Start command flags
	replicationStartCmd.Flags().DurationVar(&replicationInterval, "interval", 5*time.Minute, "Interval between update checks")
	replicationStartCmd.Flags().IntVar(&maxUpdates, "max-updates", 0, "Maximum number of updates to apply (0 = unlimited)")
}

func getReplicator() (*replication.Replicator, error) {
	if replicationSource == "" {
		return nil, fmt.Errorf("--source is required")
	}

	source, err := replication.ParseSource(replicationSource)
	if err != nil {
		return nil, fmt.Errorf("invalid source %q: %w", replicationSource, err)
	}

	return replication.NewReplicator(cfg, source)
}

func runReplicationInit(cmd *cobra.Command, args []string) {
	log := logger.Get()

	replicator, err := getReplicator()
	if err != nil {
		exitWithError("failed to create replicator", err)
	}

	ctx := context.Background()
	if err := replicator.Init(ctx); err != nil {
		exitWithError("failed to initialize replication", err)
	}

	state := replicator.State()
	log.Info("Replication initialized",
		zap.String("source", replicationSource),
		zap.Int64("sequence", state.SequenceNumber),
		zap.Time("timestamp", state.Timestamp))

	fmt.Printf("Replication initialized successfully!\n")
	fmt.Printf("Source: %s\n", replicationSource)
	fmt.Printf("Sequence: %d\n", state.SequenceNumber)
	fmt.Printf("Timestamp: %s\n", state.Timestamp.Format(time.RFC3339))
}

func runReplicationStatus(cmd *cobra.Command, args []string) {
	log := logger.Get()

	replicator, err := getReplicator()
	if err != nil {
		exitWithError("failed to create replicator", err)
	}

	ctx := context.Background()
	status, err := replicator.GetStatus(ctx)
	if err != nil {
		exitWithError("failed to get status", err)
	}

	log.Info("Replication status",
		zap.String("source", status.Source),
		zap.Int64("local_sequence", status.LocalSequence),
		zap.Time("local_timestamp", status.LocalTimestamp),
		zap.Int64("remote_sequence", status.RemoteSequence),
		zap.Int64("behind", status.Behind),
		zap.Duration("lag", status.Lag))

	fmt.Print(status.String())
}

func runReplicationUpdate(cmd *cobra.Command, args []string) {
	log := logger.Get()

	replicator, err := getReplicator()
	if err != nil {
		exitWithError("failed to create replicator", err)
	}

	// Load existing state
	if err := replicator.LoadState(); err != nil {
		exitWithError("failed to load state", err)
	}

	ctx := context.Background()
	updatesApplied := 0

	for {
		// Check for updates
		hasUpdates, behind, err := replicator.CheckForUpdates(ctx)
		if err != nil {
			exitWithError("failed to check for updates", err)
		}

		if !hasUpdates {
			if updatesApplied == 0 {
				log.Info("Already up to date")
				fmt.Println("Already up to date.")
			} else {
				log.Info("Caught up after applying updates",
					zap.Int("updates_applied", updatesApplied))
				fmt.Printf("Caught up! Applied %d updates.\n", updatesApplied)
			}
			return
		}

		log.Info("Updates available",
			zap.Int64("behind", behind))

		// Fetch next update
		oscPath, nextState, err := replicator.FetchNextUpdate(ctx)
		if err != nil {
			exitWithError("failed to fetch update", err)
		}
		if oscPath == "" {
			log.Warn("Update not yet available, try again later")
			return
		}

		// Apply the update
		if err := applyOSCUpdate(ctx, oscPath); err != nil {
			exitWithError("failed to apply update", err)
		}

		// Update state
		if err := replicator.UpdateState(nextState); err != nil {
			exitWithError("failed to update state", err)
		}

		updatesApplied++
		log.Info("Applied update",
			zap.Int64("sequence", nextState.SequenceNumber),
			zap.Time("timestamp", nextState.Timestamp))

		// If not catching up, stop after one update
		if !catchUp {
			fmt.Printf("Applied update %d (timestamp: %s)\n",
				nextState.SequenceNumber,
				nextState.Timestamp.Format(time.RFC3339))
			return
		}
	}
}

func runReplicationStart(cmd *cobra.Command, args []string) {
	log := logger.Get()

	replicator, err := getReplicator()
	if err != nil {
		exitWithError("failed to create replicator", err)
	}

	// Load existing state
	if err := replicator.LoadState(); err != nil {
		exitWithError("failed to load state", err)
	}

	// Set up signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Info("Received signal, shutting down", zap.String("signal", sig.String()))
		cancel()
	}()

	log.Info("Starting continuous replication",
		zap.String("source", replicationSource),
		zap.Duration("interval", replicationInterval),
		zap.Int("max_updates", maxUpdates))

	fmt.Printf("Starting continuous replication from %s\n", replicationSource)
	fmt.Printf("Checking every %s (press Ctrl+C to stop)\n", replicationInterval)

	updatesApplied := 0
	ticker := time.NewTicker(replicationInterval)
	defer ticker.Stop()

	// Do an immediate check
	checkAndApply := func() bool {
		// Check for updates
		hasUpdates, behind, err := replicator.CheckForUpdates(ctx)
		if err != nil {
			log.Error("Failed to check for updates", zap.Error(err))
			return true // Continue loop
		}

		if !hasUpdates {
			log.Debug("No updates available")
			return true
		}

		log.Info("Updates available", zap.Int64("behind", behind))

		// Apply all available updates
		for hasUpdates {
			select {
			case <-ctx.Done():
				return false
			default:
			}

			// Fetch next update
			oscPath, nextState, err := replicator.FetchNextUpdate(ctx)
			if err != nil {
				log.Error("Failed to fetch update", zap.Error(err))
				return true
			}
			if oscPath == "" {
				break
			}

			// Apply the update
			if err := applyOSCUpdate(ctx, oscPath); err != nil {
				log.Error("Failed to apply update", zap.Error(err))
				return true
			}

			// Update state
			if err := replicator.UpdateState(nextState); err != nil {
				log.Error("Failed to update state", zap.Error(err))
				return true
			}

			updatesApplied++
			log.Info("Applied update",
				zap.Int64("sequence", nextState.SequenceNumber),
				zap.Time("timestamp", nextState.Timestamp),
				zap.Int("total_applied", updatesApplied))

			// Check max updates limit
			if maxUpdates > 0 && updatesApplied >= maxUpdates {
				log.Info("Reached max updates limit", zap.Int("max", maxUpdates))
				return false
			}

			// Check if more updates available
			hasUpdates, _, err = replicator.CheckForUpdates(ctx)
			if err != nil {
				log.Error("Failed to check for updates", zap.Error(err))
				return true
			}
		}

		return true
	}

	// Initial check
	if !checkAndApply() {
		return
	}

	// Continuous loop
	for {
		select {
		case <-ctx.Done():
			log.Info("Replication stopped",
				zap.Int("total_updates_applied", updatesApplied))
			fmt.Printf("\nReplication stopped. Applied %d updates total.\n", updatesApplied)
			return

		case <-ticker.C:
			if !checkAndApply() {
				log.Info("Replication complete",
					zap.Int("total_updates_applied", updatesApplied))
				fmt.Printf("Replication complete. Applied %d updates total.\n", updatesApplied)
				return
			}
		}
	}
}

// applyOSCUpdate applies an OSC file to the database
func applyOSCUpdate(ctx context.Context, oscPath string) error {
	log := logger.Get()
	log.Debug("Applying OSC update", zap.String("path", oscPath))

	// Set the input file to the OSC path and enable append mode
	cfg.InputFile = oscPath
	cfg.AppendMode = true

	// Create pipeline coordinator
	pipeCfg := pipeline.CoordinatorConfig{
		ChannelBuffer: 10000,
		DropExisting:  false,
		CreateIndexes: false,
	}

	coordinator, err := pipeline.NewCoordinator(cfg, pipeCfg)
	if err != nil {
		return fmt.Errorf("failed to create coordinator: %w", err)
	}
	defer coordinator.Close()

	// Run append
	stats, err := coordinator.RunAppend(ctx, oscPath)
	if err != nil {
		return fmt.Errorf("failed to apply OSC: %w", err)
	}

	log.Info("OSC update applied",
		zap.Int64("nodes", stats.NodesProcessed),
		zap.Int64("ways", stats.WaysProcessed),
		zap.Int64("relations", stats.RelationsProcessed),
		zap.Int64("ways_rebuilt", stats.WaysRebuilt),
		zap.Int64("relations_rebuilt", stats.RelationsRebuilt))

	return nil
}
