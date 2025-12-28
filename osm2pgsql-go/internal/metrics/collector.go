package metrics

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/mem"
	"github.com/shirou/gopsutil/v4/process"
	"go.uber.org/zap"
)

// SystemMetrics holds current system metrics snapshot
type SystemMetrics struct {
	CPUPercent        float64 // System-wide CPU usage (0-100%)
	ProcessCPUPercent float64 // This process CPU usage (0-100% per core, can exceed 100% on multi-core)
	IOWaitPercent     float64 // CPU time waiting for I/O (high = I/O bound)
	MemoryUsedGB      float64
	MemoryTotalGB     float64
	MemoryPercent     float64
	DiskReadMBps      float64
	DiskWriteMBps     float64
	DiskBusyPercent   float64 // Percentage of time disk is busy
	Timestamp         time.Time
}

// Collector periodically collects and logs system metrics
type Collector struct {
	interval      time.Duration
	logger        *zap.Logger
	proc          *process.Process
	lastDiskStats map[string]disk.IOCountersStat
	lastDiskTime  time.Time
	lastCPUTimes  cpu.TimesStat
	hasCPUTimes   bool
	mu            sync.RWMutex
	lastMetrics   *SystemMetrics
}

// NewCollector creates a new metrics collector
func NewCollector(interval time.Duration, logger *zap.Logger) *Collector {
	if interval < time.Second {
		interval = 30 * time.Second
	}

	// Get handle to current process for CPU tracking
	proc, _ := process.NewProcess(int32(os.Getpid()))

	return &Collector{
		interval: interval,
		logger:   logger,
		proc:     proc,
	}
}

// Start begins periodic metrics collection. Returns when context is cancelled.
func (c *Collector) Start(ctx context.Context) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	// Collect first sample immediately (initializes disk baseline)
	c.collect()

	for {
		select {
		case <-ctx.Done():
			c.logger.Debug("Metrics collection stopped")
			return
		case <-ticker.C:
			c.collect()
		}
	}
}

// GetMetrics returns the last collected metrics
func (c *Collector) GetMetrics() *SystemMetrics {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastMetrics
}

// collect gathers current system metrics and logs them
func (c *Collector) collect() {
	metrics := &SystemMetrics{
		Timestamp: time.Now(),
	}

	// System-wide CPU percentage
	cpuPercent, err := cpu.Percent(0, false)
	if err == nil && len(cpuPercent) > 0 {
		metrics.CPUPercent = cpuPercent[0]
	}

	// Process-specific CPU percentage
	if c.proc != nil {
		if procCPU, err := c.proc.Percent(0); err == nil {
			metrics.ProcessCPUPercent = procCPU
		}
	}

	// I/O wait percentage (from CPU times)
	metrics.IOWaitPercent = c.calculateIOWait()

	// Memory usage
	vmem, err := mem.VirtualMemory()
	if err == nil {
		metrics.MemoryPercent = vmem.UsedPercent
		metrics.MemoryUsedGB = float64(vmem.Used) / (1024 * 1024 * 1024)
		metrics.MemoryTotalGB = float64(vmem.Total) / (1024 * 1024 * 1024)
	}

	// Disk I/O rates and utilization
	readRate, writeRate, busyPct := c.calculateDiskMetrics()
	metrics.DiskReadMBps = readRate
	metrics.DiskWriteMBps = writeRate
	metrics.DiskBusyPercent = busyPct

	c.mu.Lock()
	c.lastMetrics = metrics
	c.mu.Unlock()

	// Log metrics
	c.logger.Info("System metrics",
		zap.Float64("sys_cpu", metrics.CPUPercent),
		zap.Float64("proc_cpu", metrics.ProcessCPUPercent),
		zap.Float64("iowait", metrics.IOWaitPercent),
		zap.Float64("mem_pct", metrics.MemoryPercent),
		zap.String("mem_used", formatGB(metrics.MemoryUsedGB)),
		zap.String("disk_r", formatMBps(metrics.DiskReadMBps)),
		zap.String("disk_w", formatMBps(metrics.DiskWriteMBps)),
		zap.Float64("disk_busy", metrics.DiskBusyPercent),
	)
}

// calculateIOWait calculates the I/O wait percentage from CPU times
func (c *Collector) calculateIOWait() float64 {
	times, err := cpu.Times(false) // false = aggregate across all CPUs
	if err != nil || len(times) == 0 {
		return 0
	}

	current := times[0]

	if !c.hasCPUTimes {
		c.lastCPUTimes = current
		c.hasCPUTimes = true
		return 0
	}

	// Calculate deltas
	last := c.lastCPUTimes
	totalDelta := (current.User - last.User) +
		(current.System - last.System) +
		(current.Idle - last.Idle) +
		(current.Iowait - last.Iowait) +
		(current.Irq - last.Irq) +
		(current.Softirq - last.Softirq) +
		(current.Steal - last.Steal)

	iowaitDelta := current.Iowait - last.Iowait

	c.lastCPUTimes = current

	if totalDelta <= 0 {
		return 0
	}

	return (iowaitDelta / totalDelta) * 100
}

// calculateDiskMetrics calculates disk read/write rates and busy percentage
func (c *Collector) calculateDiskMetrics() (readMBps, writeMBps, busyPct float64) {
	counters, err := disk.IOCounters()
	if err != nil {
		return 0, 0, 0
	}

	now := time.Now()

	// First call - initialize baseline
	if c.lastDiskStats == nil {
		c.lastDiskStats = make(map[string]disk.IOCountersStat)
		for name, counter := range counters {
			c.lastDiskStats[name] = counter
		}
		c.lastDiskTime = now
		return 0, 0, 0
	}

	elapsed := now.Sub(c.lastDiskTime).Seconds()
	if elapsed < 0.1 {
		return 0, 0, 0
	}
	elapsedMs := elapsed * 1000

	var totalReadDelta, totalWriteDelta uint64
	var totalIOTimeDelta uint64

	for name, counter := range counters {
		if last, ok := c.lastDiskStats[name]; ok {
			// Handle counter wrapping
			if counter.ReadBytes >= last.ReadBytes {
				totalReadDelta += counter.ReadBytes - last.ReadBytes
			}
			if counter.WriteBytes >= last.WriteBytes {
				totalWriteDelta += counter.WriteBytes - last.WriteBytes
			}
			// IoTime is in milliseconds
			if counter.IoTime >= last.IoTime {
				totalIOTimeDelta += counter.IoTime - last.IoTime
			}
		}
	}

	// Update baseline
	c.lastDiskStats = make(map[string]disk.IOCountersStat)
	for name, counter := range counters {
		c.lastDiskStats[name] = counter
	}
	c.lastDiskTime = now

	// Convert to MB/s
	readMBps = float64(totalReadDelta) / elapsed / (1024 * 1024)
	writeMBps = float64(totalWriteDelta) / elapsed / (1024 * 1024)

	// Disk busy percentage (IoTime is cumulative ms spent doing I/O)
	// For multiple disks, this can exceed 100% if they're all busy
	if elapsedMs > 0 {
		busyPct = float64(totalIOTimeDelta) / elapsedMs * 100
		if busyPct > 100 {
			busyPct = 100 // Cap at 100% for single-disk interpretation
		}
	}

	return readMBps, writeMBps, busyPct
}

// formatGB formats gigabytes with one decimal place
func formatGB(gb float64) string {
	return zap.Float64("", gb).String[:0] + formatFloat(gb) + " GB"
}

// formatMBps formats MB/s with one decimal place
func formatMBps(mbps float64) string {
	return formatFloat(mbps) + " MB/s"
}

// formatFloat formats a float with one decimal place
func formatFloat(f float64) string {
	if f < 0.1 {
		return "0.0"
	}
	// Simple formatting without fmt to avoid import
	whole := int(f)
	frac := int((f - float64(whole)) * 10)
	if frac < 0 {
		frac = 0
	}
	return intToStr(whole) + "." + intToStr(frac)
}

// intToStr converts an int to string without fmt
func intToStr(n int) string {
	if n == 0 {
		return "0"
	}
	if n < 0 {
		n = -n
	}
	digits := make([]byte, 0, 10)
	for n > 0 {
		digits = append(digits, byte('0'+n%10))
		n /= 10
	}
	// Reverse
	for i, j := 0, len(digits)-1; i < j; i, j = i+1, j-1 {
		digits[i], digits[j] = digits[j], digits[i]
	}
	return string(digits)
}
