package pipeline

import (
	"fmt"
	"time"
)

// ProgressTracker tracks progress for long-running operations
type ProgressTracker struct {
	totalBytes  int64
	startTime   time.Time
	description string
}

// NewProgressTracker creates a new progress tracker
func NewProgressTracker(totalBytes int64, description string) *ProgressTracker {
	return &ProgressTracker{
		totalBytes:  totalBytes,
		startTime:   time.Now(),
		description: description,
	}
}

// Progress holds current progress information
type Progress struct {
	Current     int64
	Total       int64
	Percentage  float64
	Elapsed     time.Duration
	ETA         time.Duration
	Throughput  float64 // units per second
	Description string
}

// Calculate returns current progress metrics given the current count and bytes processed
func (p *ProgressTracker) Calculate(currentCount int64, bytesProcessed int64) Progress {
	elapsed := time.Since(p.startTime)

	var percentage float64
	var eta time.Duration

	if p.totalBytes > 0 && bytesProcessed > 0 {
		percentage = float64(bytesProcessed) / float64(p.totalBytes) * 100
		if percentage > 0 && percentage < 100 {
			// Estimate remaining time based on bytes processed
			bytesPerSecond := float64(bytesProcessed) / elapsed.Seconds()
			remainingBytes := p.totalBytes - bytesProcessed
			if bytesPerSecond > 0 {
				eta = time.Duration(float64(remainingBytes)/bytesPerSecond) * time.Second
			}
		}
	}

	// Calculate throughput (items per second)
	var throughput float64
	if elapsed.Seconds() > 0 {
		throughput = float64(currentCount) / elapsed.Seconds()
	}

	return Progress{
		Current:     currentCount,
		Total:       p.totalBytes,
		Percentage:  percentage,
		Elapsed:     elapsed.Round(time.Second),
		ETA:         eta.Round(time.Second),
		Throughput:  throughput,
		Description: p.description,
	}
}

// FormatETA formats the ETA duration in a human-readable format
func FormatETA(d time.Duration) string {
	if d <= 0 {
		return "calculating..."
	}

	d = d.Round(time.Second)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second

	if h > 0 {
		return fmt.Sprintf("%dh %dm %ds", h, m, s)
	}
	if m > 0 {
		return fmt.Sprintf("%dm %ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}

// FormatThroughput formats throughput as human-readable items per second
func FormatThroughput(itemsPerSec float64) string {
	if itemsPerSec >= 1_000_000 {
		return fmt.Sprintf("%.1fM/s", itemsPerSec/1_000_000)
	}
	if itemsPerSec >= 1_000 {
		return fmt.Sprintf("%.1fK/s", itemsPerSec/1_000)
	}
	return fmt.Sprintf("%.0f/s", itemsPerSec)
}

// FormatBytes formats bytes in a human-readable format
func FormatBytes(bytes int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)

	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.1f GB", float64(bytes)/GB)
	case bytes >= MB:
		return fmt.Sprintf("%.1f MB", float64(bytes)/MB)
	case bytes >= KB:
		return fmt.Sprintf("%.1f KB", float64(bytes)/KB)
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}
