package replication

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"

	"github.com/kevinramage/osm2pgsql-go/internal/logger"
)

// Fetcher downloads replication files from a source
type Fetcher struct {
	source     *Source
	client     *http.Client
	cacheDir   string
	maxRetries int
	retryDelay time.Duration
}

// NewFetcher creates a new replication fetcher
func NewFetcher(source *Source, cacheDir string) *Fetcher {
	return &Fetcher{
		source: source,
		client: &http.Client{
			Timeout: 60 * time.Second,
		},
		cacheDir:   cacheDir,
		maxRetries: 3,
		retryDelay: 5 * time.Second,
	}
}

// Source returns the replication source
func (f *Fetcher) Source() *Source {
	return f.source
}

// FetchCurrentState fetches the current replication state from the source
func (f *Fetcher) FetchCurrentState(ctx context.Context) (*State, error) {
	log := logger.Get()
	url := f.source.StateURL()

	log.Debug("Fetching current state", zap.String("url", url))

	resp, err := f.fetchWithRetry(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch state: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	state, err := ParseState(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to parse state: %w", err)
	}

	log.Debug("Fetched current state",
		zap.Int64("sequence", state.SequenceNumber),
		zap.Time("timestamp", state.Timestamp))

	return state, nil
}

// FetchSequenceState fetches the state for a specific sequence number
func (f *Fetcher) FetchSequenceState(ctx context.Context, seq int64) (*State, error) {
	url := f.source.SequenceStateURL(seq)

	resp, err := f.fetchWithRetry(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch sequence state: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil // Sequence doesn't exist yet
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return ParseState(resp.Body)
}

// FetchSequenceData fetches the OSC data for a specific sequence number
// Returns the path to the downloaded file (in cache dir) or empty string if not found
func (f *Fetcher) FetchSequenceData(ctx context.Context, seq int64) (string, error) {
	log := logger.Get()
	url := f.source.SequenceDataURL(seq)
	path := SequenceToPath(seq)

	// Create cache directory structure
	cacheFile := filepath.Join(f.cacheDir, path+".osc.gz")
	cacheDir := filepath.Dir(cacheFile)
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create cache directory: %w", err)
	}

	// Check if already cached
	if _, err := os.Stat(cacheFile); err == nil {
		log.Debug("Using cached OSC file", zap.String("path", cacheFile))
		return cacheFile, nil
	}

	log.Debug("Fetching OSC data", zap.Int64("sequence", seq), zap.String("url", url))

	resp, err := f.fetchWithRetry(ctx, url)
	if err != nil {
		return "", fmt.Errorf("failed to fetch OSC data: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return "", nil // Sequence doesn't exist yet
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// Write to cache file
	tmpFile := cacheFile + ".tmp"
	out, err := os.Create(tmpFile)
	if err != nil {
		return "", fmt.Errorf("failed to create cache file: %w", err)
	}

	_, err = io.Copy(out, resp.Body)
	out.Close()
	if err != nil {
		os.Remove(tmpFile)
		return "", fmt.Errorf("failed to write cache file: %w", err)
	}

	// Rename to final name
	if err := os.Rename(tmpFile, cacheFile); err != nil {
		os.Remove(tmpFile)
		return "", fmt.Errorf("failed to rename cache file: %w", err)
	}

	log.Debug("Downloaded OSC data", zap.Int64("sequence", seq), zap.String("path", cacheFile))
	return cacheFile, nil
}

// fetchWithRetry performs an HTTP GET with retries
func (f *Fetcher) fetchWithRetry(ctx context.Context, url string) (*http.Response, error) {
	var lastErr error

	for attempt := 0; attempt <= f.maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(f.retryDelay):
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("User-Agent", "osm2pgsql-go/1.0")

		resp, err := f.client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}

		// Don't retry on 404 or success
		if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusOK {
			return resp, nil
		}

		// Retry on server errors
		if resp.StatusCode >= 500 {
			resp.Body.Close()
			lastErr = fmt.Errorf("server error: %d", resp.StatusCode)
			continue
		}

		return resp, nil
	}

	return nil, fmt.Errorf("max retries exceeded: %w", lastErr)
}

// CleanCache removes old cached files
func (f *Fetcher) CleanCache(keepSequences int) error {
	// This is a simple cleanup - in production you might want something more sophisticated
	// For now, we'll just leave files in place
	return nil
}

// GetCachePath returns the path where a sequence would be cached
func (f *Fetcher) GetCachePath(seq int64) string {
	path := SequenceToPath(seq)
	return filepath.Join(f.cacheDir, path+".osc.gz")
}
