package nodeindex

import (
	"encoding/binary"
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

const (
	// Each node entry: lat (int32) + lon (int32) = 8 bytes
	// Using fixed-point: value * 1e7 to store as int32
	entrySize = 8
	// Maximum node ID we support (10 billion should be enough)
	maxNodeID = 10_000_000_000
)

// MmapIndex is a memory-mapped node coordinate index
// Node coordinates are stored at offset = nodeID * 8
// This gives O(1) lookup for any node ID
type MmapIndex struct {
	file   *os.File
	data   []byte
	size   int64
	writer bool
}

// NewMmapIndex creates a new mmap index for writing
func NewMmapIndex(path string) (*MmapIndex, error) {
	// Calculate size needed (10B nodes * 8 bytes = 80GB address space)
	// But we'll use sparse file, so actual disk usage is only for written nodes
	size := int64(maxNodeID) * entrySize

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create mmap file: %w", err)
	}

	// Truncate to full size (creates sparse file on Linux)
	if err := f.Truncate(size); err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to truncate file: %w", err)
	}

	// Memory map the file
	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to mmap file: %w", err)
	}

	return &MmapIndex{
		file:   f,
		data:   data,
		size:   size,
		writer: true,
	}, nil
}

// OpenMmapIndex opens an existing mmap index for reading
func OpenMmapIndex(path string) (*MmapIndex, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open mmap file: %w", err)
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	size := info.Size()

	// Memory map the file read-only
	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to mmap file: %w", err)
	}

	return &MmapIndex{
		file:   f,
		data:   data,
		size:   size,
		writer: false,
	}, nil
}

// Put stores a node's coordinates
func (m *MmapIndex) Put(nodeID int64, lat, lon float64) {
	if nodeID < 0 || nodeID >= maxNodeID {
		return // Ignore out of range
	}

	offset := nodeID * entrySize

	// Convert to fixed-point (7 decimal places)
	latInt := int32(lat * 1e7)
	lonInt := int32(lon * 1e7)

	// Write directly to mmap
	binary.LittleEndian.PutUint32(m.data[offset:], uint32(latInt))
	binary.LittleEndian.PutUint32(m.data[offset+4:], uint32(lonInt))
}

// Get retrieves a node's coordinates
// Returns (0, 0, false) if the node doesn't exist
func (m *MmapIndex) Get(nodeID int64) (lat, lon float64, ok bool) {
	if nodeID < 0 || nodeID >= maxNodeID {
		return 0, 0, false
	}

	offset := nodeID * entrySize
	if offset+entrySize > m.size {
		return 0, 0, false
	}

	latInt := int32(binary.LittleEndian.Uint32(m.data[offset:]))
	lonInt := int32(binary.LittleEndian.Uint32(m.data[offset+4:]))

	// Check if node was written (0,0 is a valid location, but very rare)
	// We'll accept this edge case for simplicity
	if latInt == 0 && lonInt == 0 {
		return 0, 0, false
	}

	lat = float64(latInt) / 1e7
	lon = float64(lonInt) / 1e7
	return lat, lon, true
}

// Sync flushes changes to disk
func (m *MmapIndex) Sync() error {
	// Force msync to ensure data is persisted to disk
	// This is especially important for large sparse files
	_, _, errno := syscall.Syscall(syscall.SYS_MSYNC,
		uintptr(unsafe.Pointer(&m.data[0])),
		uintptr(len(m.data)),
		uintptr(syscall.MS_SYNC))
	if errno != 0 {
		return errno
	}
	return nil
}

// Close closes the mmap index
func (m *MmapIndex) Close() error {
	if err := syscall.Munmap(m.data); err != nil {
		m.file.Close()
		return err
	}
	return m.file.Close()
}
