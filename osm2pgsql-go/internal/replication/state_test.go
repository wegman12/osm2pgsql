package replication

import (
	"strings"
	"testing"
	"time"
)

func TestParseState(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantSeq int64
		wantTS  time.Time
		wantErr bool
	}{
		{
			name: "standard OSM state file",
			input: `#Sat Jan 15 12:00:00 UTC 2024
sequenceNumber=12345
timestamp=2024-01-15T12\:00\:00Z`,
			wantSeq: 12345,
			wantTS:  time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC),
			wantErr: false,
		},
		{
			name: "state with extra whitespace",
			input: `  # comment
  sequenceNumber = 67890
  timestamp = 2024-06-20T08\:30\:00Z  `,
			wantSeq: 67890,
			wantTS:  time.Date(2024, 6, 20, 8, 30, 0, 0, time.UTC),
			wantErr: false,
		},
		{
			name: "unescaped timestamp",
			input: `sequenceNumber=100
timestamp=2024-03-10T15:45:00Z`,
			wantSeq: 100,
			wantTS:  time.Date(2024, 3, 10, 15, 45, 0, 0, time.UTC),
			wantErr: false,
		},
		{
			name:    "invalid sequence number",
			input:   "sequenceNumber=abc\ntimestamp=2024-01-01T00:00:00Z",
			wantErr: true,
		},
		{
			name:    "invalid timestamp",
			input:   "sequenceNumber=100\ntimestamp=invalid",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state, err := ParseState(strings.NewReader(tt.input))
			if tt.wantErr {
				if err == nil {
					t.Error("expected error but got none")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if state.SequenceNumber != tt.wantSeq {
				t.Errorf("SequenceNumber = %d, want %d", state.SequenceNumber, tt.wantSeq)
			}
			if !state.Timestamp.Equal(tt.wantTS) {
				t.Errorf("Timestamp = %v, want %v", state.Timestamp, tt.wantTS)
			}
		})
	}
}

func TestSequenceToPath(t *testing.T) {
	tests := []struct {
		seq  int64
		want string
	}{
		{0, "000/000/000"},
		{1, "000/000/001"},
		{999, "000/000/999"},
		{1000, "000/001/000"},
		{1234, "000/001/234"},
		{12345, "000/012/345"},
		{123456, "000/123/456"},
		{1234567, "001/234/567"},
		{12345678, "012/345/678"},
		{6321543, "006/321/543"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := SequenceToPath(tt.seq)
			if got != tt.want {
				t.Errorf("SequenceToPath(%d) = %q, want %q", tt.seq, got, tt.want)
			}
		})
	}
}

func TestPathToSequence(t *testing.T) {
	tests := []struct {
		path    string
		want    int64
		wantErr bool
	}{
		{"000/000/000", 0, false},
		{"000/000/001", 1, false},
		{"000/001/234", 1234, false},
		{"001/234/567", 1234567, false},
		{"006/321/543", 6321543, false},
		{"006/321/543.osc.gz", 6321543, false},
		{"006/321/543.state.txt", 6321543, false},
		{"invalid", 0, true},
		{"000/000", 0, true},
		{"abc/def/ghi", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			got, err := PathToSequence(tt.path)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error but got none")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("PathToSequence(%q) = %d, want %d", tt.path, got, tt.want)
			}
		})
	}
}

func TestSequencePathRoundTrip(t *testing.T) {
	// Test that conversion is reversible
	sequences := []int64{0, 1, 100, 1000, 12345, 123456, 1234567, 9999999}

	for _, seq := range sequences {
		path := SequenceToPath(seq)
		got, err := PathToSequence(path)
		if err != nil {
			t.Fatalf("PathToSequence(%q) error: %v", path, err)
		}
		if got != seq {
			t.Errorf("round trip failed: %d -> %q -> %d", seq, path, got)
		}
	}
}
