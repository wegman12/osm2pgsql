package replication

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

// State represents the replication state
type State struct {
	SequenceNumber int64
	Timestamp      time.Time
}

// String returns the state in a human-readable format
func (s State) String() string {
	return fmt.Sprintf("Sequence: %d, Timestamp: %s", s.SequenceNumber, s.Timestamp.Format(time.RFC3339))
}

// ParseState parses a state.txt file content
// Format:
//
//	#comment line
//	sequenceNumber=12345
//	timestamp=2024-01-15T12\:00\:00Z
func ParseState(r io.Reader) (*State, error) {
	state := &State{}
	scanner := bufio.NewScanner(r)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Parse key=value
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		switch key {
		case "sequenceNumber":
			seq, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid sequence number: %w", err)
			}
			state.SequenceNumber = seq

		case "timestamp":
			// Unescape colons (OSM state files escape colons as \:)
			value = strings.ReplaceAll(value, `\:`, ":")
			// Also handle backslash before colons without the escape
			value = strings.ReplaceAll(value, `\\`, `\`)

			// Try multiple timestamp formats
			var t time.Time
			var err error
			formats := []string{
				time.RFC3339,
				"2006-01-02T15:04:05Z",
				"2006-01-02T15\\:04\\:05Z",
				"2006-01-02 15:04:05",
			}
			for _, format := range formats {
				t, err = time.Parse(format, value)
				if err == nil {
					break
				}
			}
			if err != nil {
				return nil, fmt.Errorf("invalid timestamp %q: %w", value, err)
			}
			state.Timestamp = t
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading state: %w", err)
	}

	return state, nil
}

// ParseStateFile reads and parses a state file from disk
func ParseStateFile(filename string) (*State, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return ParseState(f)
}

// WriteState writes a state to a writer
func WriteState(w io.Writer, state *State) error {
	// Format timestamp with escaped colons (OSM style)
	ts := state.Timestamp.UTC().Format("2006-01-02T15:04:05Z")
	ts = strings.ReplaceAll(ts, ":", `\:`)

	_, err := fmt.Fprintf(w, "# osm2pgsql-go replication state\n")
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "sequenceNumber=%d\n", state.SequenceNumber)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "timestamp=%s\n", ts)
	return err
}

// WriteStateFile writes a state to a file
func WriteStateFile(filename string, state *State) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	return WriteState(f, state)
}

// SequenceToPath converts a sequence number to a path like "000/000/001"
// This is the standard OSM replication directory structure
func SequenceToPath(seq int64) string {
	// Sequence is split into 3-digit groups: AAA/BBB/CCC
	// e.g., sequence 1234567 -> 001/234/567
	return fmt.Sprintf("%03d/%03d/%03d",
		seq/1000000,
		(seq/1000)%1000,
		seq%1000)
}

// PathToSequence converts a path like "000/000/001" back to a sequence number
func PathToSequence(path string) (int64, error) {
	// Remove any extension
	path = strings.TrimSuffix(path, ".osc.gz")
	path = strings.TrimSuffix(path, ".state.txt")

	parts := strings.Split(path, "/")
	if len(parts) != 3 {
		return 0, fmt.Errorf("invalid path format: %s", path)
	}

	var seq int64
	for i, part := range parts {
		n, err := strconv.ParseInt(part, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid path component %q: %w", part, err)
		}
		switch i {
		case 0:
			seq += n * 1000000
		case 1:
			seq += n * 1000
		case 2:
			seq += n
		}
	}

	return seq, nil
}
