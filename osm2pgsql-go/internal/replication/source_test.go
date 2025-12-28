package replication

import (
	"strings"
	"testing"
	"time"
)

func TestParseSource(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		wantName    string
		wantBaseURL string
		wantErr     bool
	}{
		{
			name:        "planet minute",
			input:       "planet-minute",
			wantName:    "planet-minute",
			wantBaseURL: "https://planet.openstreetmap.org/replication/minute",
		},
		{
			name:        "planet minute alternate",
			input:       "minute",
			wantName:    "planet-minute",
			wantBaseURL: "https://planet.openstreetmap.org/replication/minute",
		},
		{
			name:        "planet hour",
			input:       "planet-hour",
			wantName:    "planet-hour",
			wantBaseURL: "https://planet.openstreetmap.org/replication/hour",
		},
		{
			name:        "planet day",
			input:       "day",
			wantName:    "planet-day",
			wantBaseURL: "https://planet.openstreetmap.org/replication/day",
		},
		{
			name:        "geofabrik monaco",
			input:       "geofabrik/monaco",
			wantName:    "geofabrik/monaco",
			wantBaseURL: "https://download.geofabrik.de/europe/monaco-updates",
		},
		{
			name:        "geofabrik germany",
			input:       "geofabrik/germany",
			wantName:    "geofabrik/germany",
			wantBaseURL: "https://download.geofabrik.de/europe/germany-updates",
		},
		{
			name:        "shortcut monaco",
			input:       "monaco",
			wantName:    "geofabrik/monaco",
			wantBaseURL: "https://download.geofabrik.de/europe/monaco-updates",
		},
		{
			name:        "custom URL",
			input:       "https://my-server.com/replication",
			wantName:    "custom",
			wantBaseURL: "https://my-server.com/replication",
		},
		{
			name:        "custom URL with trailing slash",
			input:       "https://my-server.com/replication/",
			wantName:    "custom",
			wantBaseURL: "https://my-server.com/replication",
		},
		{
			name:    "unknown source",
			input:   "unknown-source-xyz",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source, err := ParseSource(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error but got none")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if source.Name != tt.wantName {
				t.Errorf("Name = %q, want %q", source.Name, tt.wantName)
			}
			if source.BaseURL != tt.wantBaseURL {
				t.Errorf("BaseURL = %q, want %q", source.BaseURL, tt.wantBaseURL)
			}
		})
	}
}

func TestSourceURLs(t *testing.T) {
	source := SourcePlanetMinute

	// Test state URL
	stateURL := source.StateURL()
	expected := "https://planet.openstreetmap.org/replication/minute/state.txt"
	if stateURL != expected {
		t.Errorf("StateURL() = %q, want %q", stateURL, expected)
	}

	// Test sequence state URL
	seqStateURL := source.SequenceStateURL(1234567)
	expected = "https://planet.openstreetmap.org/replication/minute/001/234/567.state.txt"
	if seqStateURL != expected {
		t.Errorf("SequenceStateURL(1234567) = %q, want %q", seqStateURL, expected)
	}

	// Test sequence data URL
	seqDataURL := source.SequenceDataURL(1234567)
	expected = "https://planet.openstreetmap.org/replication/minute/001/234/567.osc.gz"
	if seqDataURL != expected {
		t.Errorf("SequenceDataURL(1234567) = %q, want %q", seqDataURL, expected)
	}
}

func TestGetGeofabrikSource(t *testing.T) {
	tests := []struct {
		region       string
		wantContains string
		wantInterval time.Duration
	}{
		{"monaco", "europe/monaco-updates", 24 * time.Hour},
		{"germany", "europe/germany-updates", 24 * time.Hour},
		{"us", "north-america/us-updates", 24 * time.Hour},
		{"japan", "asia/japan-updates", 24 * time.Hour},
		{"australia", "australia-oceania/australia-updates", 24 * time.Hour},
	}

	for _, tt := range tests {
		t.Run(tt.region, func(t *testing.T) {
			source, err := GetGeofabrikSource(tt.region)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if !strings.Contains(source.BaseURL, tt.wantContains) {
				t.Errorf("BaseURL %q does not contain %q", source.BaseURL, tt.wantContains)
			}
			if source.UpdateInterval != tt.wantInterval {
				t.Errorf("UpdateInterval = %v, want %v", source.UpdateInterval, tt.wantInterval)
			}
		})
	}
}

func TestListSources(t *testing.T) {
	sources := ListSources()

	// Should have planet sources
	found := false
	for _, s := range sources {
		if strings.Contains(s, "planet-minute") {
			found = true
			break
		}
	}
	if !found {
		t.Error("ListSources() should include planet-minute")
	}

	// Should have geofabrik sources
	found = false
	for _, s := range sources {
		if strings.Contains(s, "geofabrik/") {
			found = true
			break
		}
	}
	if !found {
		t.Error("ListSources() should include geofabrik sources")
	}
}
