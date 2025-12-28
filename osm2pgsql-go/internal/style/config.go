package style

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config represents the style configuration for filtering OSM data
type Config struct {
	// Points configuration for point geometries
	Points *FilterConfig `yaml:"points,omitempty"`
	// Lines configuration for line geometries
	Lines *FilterConfig `yaml:"lines,omitempty"`
	// Polygons configuration for polygon geometries
	Polygons *FilterConfig `yaml:"polygons,omitempty"`
}

// FilterConfig defines filtering rules for a geometry type
type FilterConfig struct {
	// Include specifies which tag keys/values to include
	// If empty, all tags are included (no filtering)
	Include map[string][]string `yaml:"include,omitempty"`
	// Exclude specifies which tag keys/values to exclude
	// Applied after include rules
	Exclude map[string][]string `yaml:"exclude,omitempty"`
	// RequireAny specifies that at least one of these tags must be present
	// If empty, no requirement
	RequireAny []string `yaml:"require_any,omitempty"`
}

// LoadConfig loads a style configuration from a YAML file
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read style file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse style YAML: %w", err)
	}

	return &cfg, nil
}

// DefaultConfig returns a configuration that includes everything
func DefaultConfig() *Config {
	return &Config{}
}

// Filter checks if tags match the filter configuration
type Filter struct {
	cfg *FilterConfig
}

// NewFilter creates a filter from configuration
func NewFilter(cfg *FilterConfig) *Filter {
	if cfg == nil {
		return &Filter{cfg: &FilterConfig{}}
	}
	return &Filter{cfg: cfg}
}

// Match checks if the given tags match the filter rules
// Returns true if the feature should be included
func (f *Filter) Match(tags map[string]string) bool {
	if f.cfg == nil {
		return true
	}

	// Check require_any - at least one tag must be present
	if len(f.cfg.RequireAny) > 0 {
		found := false
		for _, key := range f.cfg.RequireAny {
			if _, ok := tags[key]; ok {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// Check include rules
	if len(f.cfg.Include) > 0 {
		matched := false
		for key, values := range f.cfg.Include {
			if tagValue, ok := tags[key]; ok {
				// If no specific values, any value matches
				if len(values) == 0 {
					matched = true
					break
				}
				// Check if tag value is in allowed list
				for _, v := range values {
					if v == tagValue || v == "*" {
						matched = true
						break
					}
				}
				if matched {
					break
				}
			}
		}
		if !matched {
			return false
		}
	}

	// Check exclude rules
	if len(f.cfg.Exclude) > 0 {
		for key, values := range f.cfg.Exclude {
			if tagValue, ok := tags[key]; ok {
				// If no specific values, exclude any with this key
				if len(values) == 0 {
					return false
				}
				// Check if tag value is in excluded list
				for _, v := range values {
					if v == tagValue || v == "*" {
						return false
					}
				}
			}
		}
	}

	return true
}

// MatchOSMTags is a convenience method for osm.Tags
func (f *Filter) MatchOSMTags(tags interface{ Map() map[string]string }) bool {
	return f.Match(tags.Map())
}

// HasFilter returns true if filtering is enabled
func (f *Filter) HasFilter() bool {
	if f.cfg == nil {
		return false
	}
	return len(f.cfg.Include) > 0 || len(f.cfg.Exclude) > 0 || len(f.cfg.RequireAny) > 0
}
