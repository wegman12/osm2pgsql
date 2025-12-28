package replication

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"

	"github.com/kevinramage/osm2pgsql-go/internal/config"
	"github.com/kevinramage/osm2pgsql-go/internal/logger"
)

// Replicator manages the replication process
type Replicator struct {
	cfg       *config.Config
	source    *Source
	fetcher   *Fetcher
	stateFile string
	state     *State
}

// NewReplicator creates a new replicator
func NewReplicator(cfg *config.Config, source *Source) (*Replicator, error) {
	// Create cache directory
	cacheDir := filepath.Join(cfg.OutputDir, "replication")
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create cache directory: %w", err)
	}

	stateFile := filepath.Join(cfg.OutputDir, "replication.state")

	return &Replicator{
		cfg:       cfg,
		source:    source,
		fetcher:   NewFetcher(source, cacheDir),
		stateFile: stateFile,
	}, nil
}

// Init initializes replication by fetching the current state from the source
func (r *Replicator) Init(ctx context.Context) error {
	log := logger.Get()

	// Fetch current state from source
	state, err := r.fetcher.FetchCurrentState(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch current state: %w", err)
	}

	// Save state locally
	if err := WriteStateFile(r.stateFile, state); err != nil {
		return fmt.Errorf("failed to write state file: %w", err)
	}

	r.state = state

	log.Info("Replication initialized",
		zap.String("source", r.source.Name),
		zap.Int64("sequence", state.SequenceNumber),
		zap.Time("timestamp", state.Timestamp))

	return nil
}

// LoadState loads the local replication state
func (r *Replicator) LoadState() error {
	state, err := ParseStateFile(r.stateFile)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("replication not initialized - run 'replication init' first")
		}
		return fmt.Errorf("failed to load state: %w", err)
	}
	r.state = state
	return nil
}

// SaveState saves the current state to disk
func (r *Replicator) SaveState() error {
	if r.state == nil {
		return fmt.Errorf("no state to save")
	}
	return WriteStateFile(r.stateFile, r.state)
}

// State returns the current replication state
func (r *Replicator) State() *State {
	return r.state
}

// CheckForUpdates checks if new updates are available
func (r *Replicator) CheckForUpdates(ctx context.Context) (bool, int64, error) {
	if r.state == nil {
		return false, 0, fmt.Errorf("state not loaded")
	}

	currentState, err := r.fetcher.FetchCurrentState(ctx)
	if err != nil {
		return false, 0, err
	}

	if currentState.SequenceNumber > r.state.SequenceNumber {
		behind := currentState.SequenceNumber - r.state.SequenceNumber
		return true, behind, nil
	}

	return false, 0, nil
}

// FetchNextUpdate fetches the next update file
// Returns the path to the OSC file, or empty string if no update available
func (r *Replicator) FetchNextUpdate(ctx context.Context) (string, *State, error) {
	if r.state == nil {
		return "", nil, fmt.Errorf("state not loaded")
	}

	nextSeq := r.state.SequenceNumber + 1

	// Fetch the OSC file
	oscPath, err := r.fetcher.FetchSequenceData(ctx, nextSeq)
	if err != nil {
		return "", nil, err
	}
	if oscPath == "" {
		return "", nil, nil // No update available yet
	}

	// Fetch the state for this sequence
	nextState, err := r.fetcher.FetchSequenceState(ctx, nextSeq)
	if err != nil {
		return "", nil, err
	}
	if nextState == nil {
		// Use current state with incremented sequence
		nextState = &State{
			SequenceNumber: nextSeq,
			Timestamp:      time.Now().UTC(),
		}
	}

	return oscPath, nextState, nil
}

// UpdateState updates the local state after successfully applying an update
func (r *Replicator) UpdateState(newState *State) error {
	r.state = newState
	return r.SaveState()
}

// GetStatus returns a status summary
func (r *Replicator) GetStatus(ctx context.Context) (*Status, error) {
	if r.state == nil {
		if err := r.LoadState(); err != nil {
			return nil, err
		}
	}

	status := &Status{
		Source:         r.source.Name,
		SourceURL:      r.source.BaseURL,
		LocalSequence:  r.state.SequenceNumber,
		LocalTimestamp: r.state.Timestamp,
	}

	// Try to get remote state
	remoteState, err := r.fetcher.FetchCurrentState(ctx)
	if err == nil {
		status.RemoteSequence = remoteState.SequenceNumber
		status.RemoteTimestamp = remoteState.Timestamp
		status.Behind = remoteState.SequenceNumber - r.state.SequenceNumber
		status.Lag = remoteState.Timestamp.Sub(r.state.Timestamp)
	}

	return status, nil
}

// Status represents the current replication status
type Status struct {
	Source          string
	SourceURL       string
	LocalSequence   int64
	LocalTimestamp  time.Time
	RemoteSequence  int64
	RemoteTimestamp time.Time
	Behind          int64
	Lag             time.Duration
}

// String returns a human-readable status
func (s *Status) String() string {
	str := fmt.Sprintf("Source: %s\n", s.Source)
	str += fmt.Sprintf("URL: %s\n", s.SourceURL)
	str += fmt.Sprintf("Local sequence: %d\n", s.LocalSequence)
	str += fmt.Sprintf("Local timestamp: %s\n", s.LocalTimestamp.Format(time.RFC3339))

	if s.RemoteSequence > 0 {
		str += fmt.Sprintf("Remote sequence: %d\n", s.RemoteSequence)
		str += fmt.Sprintf("Remote timestamp: %s\n", s.RemoteTimestamp.Format(time.RFC3339))
		str += fmt.Sprintf("Behind: %d sequences\n", s.Behind)
		str += fmt.Sprintf("Lag: %s\n", s.Lag.Round(time.Second))
	}

	return str
}
