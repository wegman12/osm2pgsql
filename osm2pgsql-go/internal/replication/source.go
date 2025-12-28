package replication

import (
	"fmt"
	"strings"
	"time"
)

// Source represents a replication data source
type Source struct {
	Name        string
	BaseURL     string        // Base URL for replication files
	UpdateInterval time.Duration // Expected update interval
	Description string
}

// StateURL returns the URL for the current state file
func (s *Source) StateURL() string {
	return s.BaseURL + "/state.txt"
}

// SequenceStateURL returns the URL for a specific sequence's state file
func (s *Source) SequenceStateURL(seq int64) string {
	path := SequenceToPath(seq)
	return fmt.Sprintf("%s/%s.state.txt", s.BaseURL, path)
}

// SequenceDataURL returns the URL for a specific sequence's OSC file
func (s *Source) SequenceDataURL(seq int64) string {
	path := SequenceToPath(seq)
	return fmt.Sprintf("%s/%s.osc.gz", s.BaseURL, path)
}

// Predefined replication sources
var (
	// Planet OSM - minutely updates
	SourcePlanetMinute = &Source{
		Name:           "planet-minute",
		BaseURL:        "https://planet.openstreetmap.org/replication/minute",
		UpdateInterval: 1 * time.Minute,
		Description:    "OpenStreetMap planet minutely updates",
	}

	// Planet OSM - hourly updates
	SourcePlanetHour = &Source{
		Name:           "planet-hour",
		BaseURL:        "https://planet.openstreetmap.org/replication/hour",
		UpdateInterval: 1 * time.Hour,
		Description:    "OpenStreetMap planet hourly updates",
	}

	// Planet OSM - daily updates
	SourcePlanetDay = &Source{
		Name:           "planet-day",
		BaseURL:        "https://planet.openstreetmap.org/replication/day",
		UpdateInterval: 24 * time.Hour,
		Description:    "OpenStreetMap planet daily updates",
	}
)

// Geofabrik regions with their replication URLs
var geofabrikRegions = map[string]string{
	// Europe
	"europe":              "europe",
	"germany":             "europe/germany",
	"france":              "europe/france",
	"italy":               "europe/italy",
	"spain":               "europe/spain",
	"united-kingdom":      "europe/great-britain",
	"great-britain":       "europe/great-britain",
	"netherlands":         "europe/netherlands",
	"belgium":             "europe/belgium",
	"switzerland":         "europe/switzerland",
	"austria":             "europe/austria",
	"poland":              "europe/poland",
	"monaco":              "europe/monaco",

	// Americas
	"north-america":       "north-america",
	"us":                  "north-america/us",
	"usa":                 "north-america/us",
	"canada":              "north-america/canada",
	"mexico":              "north-america/mexico",
	"south-america":       "south-america",
	"brazil":              "south-america/brazil",

	// Asia
	"asia":                "asia",
	"japan":               "asia/japan",
	"china":               "asia/china",
	"india":               "asia/india",

	// Africa
	"africa":              "africa",

	// Oceania
	"oceania":             "australia-oceania",
	"australia":           "australia-oceania/australia",
	"new-zealand":         "australia-oceania/new-zealand",
}

// GetGeofabrikSource returns a replication source for a Geofabrik region
func GetGeofabrikSource(region string) (*Source, error) {
	region = strings.ToLower(strings.TrimSpace(region))

	path, ok := geofabrikRegions[region]
	if !ok {
		// Try using the region directly as a path
		path = region
	}

	return &Source{
		Name:           fmt.Sprintf("geofabrik/%s", region),
		BaseURL:        fmt.Sprintf("https://download.geofabrik.de/%s-updates", path),
		UpdateInterval: 24 * time.Hour, // Geofabrik typically updates daily
		Description:    fmt.Sprintf("Geofabrik %s daily updates", region),
	}, nil
}

// ParseSource parses a source string and returns a Source
// Formats:
//   - "planet-minute", "planet-hour", "planet-day"
//   - "geofabrik/monaco", "geofabrik/germany", etc.
//   - Custom URL: "https://example.com/replication"
func ParseSource(s string) (*Source, error) {
	s = strings.TrimSpace(s)

	// Check predefined planet sources
	switch strings.ToLower(s) {
	case "planet-minute", "planet/minute", "minute":
		return SourcePlanetMinute, nil
	case "planet-hour", "planet/hour", "hour":
		return SourcePlanetHour, nil
	case "planet-day", "planet/day", "day":
		return SourcePlanetDay, nil
	}

	// Check for Geofabrik format
	if strings.HasPrefix(strings.ToLower(s), "geofabrik/") {
		region := strings.TrimPrefix(s, "geofabrik/")
		region = strings.TrimPrefix(region, "Geofabrik/")
		return GetGeofabrikSource(region)
	}

	// Check for direct URL
	if strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://") {
		return &Source{
			Name:           "custom",
			BaseURL:        strings.TrimSuffix(s, "/"),
			UpdateInterval: 1 * time.Hour, // Default to hourly
			Description:    "Custom replication source",
		}, nil
	}

	// Try as Geofabrik region name
	if _, ok := geofabrikRegions[strings.ToLower(s)]; ok {
		return GetGeofabrikSource(s)
	}

	return nil, fmt.Errorf("unknown replication source: %s", s)
}

// ListSources returns a list of all predefined sources
func ListSources() []string {
	sources := []string{
		"planet-minute - OpenStreetMap planet minutely updates",
		"planet-hour   - OpenStreetMap planet hourly updates",
		"planet-day    - OpenStreetMap planet daily updates",
	}

	sources = append(sources, "")
	sources = append(sources, "Geofabrik regions (use as geofabrik/<region>):")

	for region := range geofabrikRegions {
		sources = append(sources, fmt.Sprintf("  geofabrik/%s", region))
	}

	return sources
}
