package flex

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	lua "github.com/yuin/gopher-lua"
)

// Tag transform helper functions for Lua scripts

var (
	// Regex patterns
	whitespaceRegex = regexp.MustCompile(`\s+`)
	numericRegex    = regexp.MustCompile(`^-?\d+(\.\d+)?$`)
)

// RegisterTransforms registers all tag transform functions in the Lua state
func RegisterTransforms(L *lua.LState) {
	// Create osm2pgsql.transforms table
	transforms := L.NewTable()

	// String transforms
	L.SetField(transforms, "trim", L.NewFunction(luaTrim))
	L.SetField(transforms, "lower", L.NewFunction(luaLower))
	L.SetField(transforms, "upper", L.NewFunction(luaUpper))
	L.SetField(transforms, "clean_spaces", L.NewFunction(luaCleanSpaces))
	L.SetField(transforms, "truncate", L.NewFunction(luaTruncate))

	// Type parsing
	L.SetField(transforms, "parse_int", L.NewFunction(luaParseInt))
	L.SetField(transforms, "parse_real", L.NewFunction(luaParseReal))
	L.SetField(transforms, "parse_bool", L.NewFunction(luaParseBool))
	L.SetField(transforms, "parse_direction", L.NewFunction(luaParseDirection))
	L.SetField(transforms, "parse_layer", L.NewFunction(luaParseLayer))

	// Name handling
	L.SetField(transforms, "get_name", L.NewFunction(luaGetName))
	L.SetField(transforms, "get_name_localized", L.NewFunction(luaGetNameLocalized))

	// Tag formatting
	L.SetField(transforms, "tags_to_json", L.NewFunction(luaTagsToJSON))
	L.SetField(transforms, "tags_to_hstore", L.NewFunction(luaTagsToHstore))
	L.SetField(transforms, "filter_tags", L.NewFunction(luaFilterTags))

	// Road/way helpers
	L.SetField(transforms, "calc_z_order", L.NewFunction(luaCalcZOrder))
	L.SetField(transforms, "is_area", L.NewFunction(luaIsArea))

	// Register under osm2pgsql
	osm2pgsql := L.GetGlobal("osm2pgsql")
	if osm2pgsql == lua.LNil {
		osm2pgsql = L.NewTable()
		L.SetGlobal("osm2pgsql", osm2pgsql)
	}
	L.SetField(osm2pgsql.(*lua.LTable), "transforms", transforms)

	// Also register common functions at top level for convenience
	L.SetGlobal("trim", L.NewFunction(luaTrim))
	L.SetGlobal("parse_int", L.NewFunction(luaParseInt))
	L.SetGlobal("parse_bool", L.NewFunction(luaParseBool))
	L.SetGlobal("get_name", L.NewFunction(luaGetName))
}

// luaTrim trims whitespace from a string
func luaTrim(L *lua.LState) int {
	s := L.CheckString(1)
	L.Push(lua.LString(strings.TrimSpace(s)))
	return 1
}

// luaLower converts string to lowercase
func luaLower(L *lua.LState) int {
	s := L.CheckString(1)
	L.Push(lua.LString(strings.ToLower(s)))
	return 1
}

// luaUpper converts string to uppercase
func luaUpper(L *lua.LState) int {
	s := L.CheckString(1)
	L.Push(lua.LString(strings.ToUpper(s)))
	return 1
}

// luaCleanSpaces normalizes whitespace (collapse multiple spaces, trim)
func luaCleanSpaces(L *lua.LState) int {
	s := L.CheckString(1)
	cleaned := whitespaceRegex.ReplaceAllString(s, " ")
	cleaned = strings.TrimSpace(cleaned)
	L.Push(lua.LString(cleaned))
	return 1
}

// luaTruncate truncates string to max length
func luaTruncate(L *lua.LState) int {
	s := L.CheckString(1)
	maxLen := L.CheckInt(2)

	runes := []rune(s)
	if len(runes) <= maxLen {
		L.Push(lua.LString(s))
	} else {
		L.Push(lua.LString(string(runes[:maxLen])))
	}
	return 1
}

// luaParseInt parses string to integer with optional default
func luaParseInt(L *lua.LState) int {
	s := L.CheckString(1)
	defaultVal := int64(0)
	if L.GetTop() >= 2 {
		defaultVal = L.CheckInt64(2)
	}

	s = strings.TrimSpace(s)
	if val, err := strconv.ParseInt(s, 10, 64); err == nil {
		L.Push(lua.LNumber(val))
	} else {
		// Try parsing as float and truncate
		if fval, err := strconv.ParseFloat(s, 64); err == nil {
			L.Push(lua.LNumber(int64(fval)))
		} else {
			L.Push(lua.LNumber(defaultVal))
		}
	}
	return 1
}

// luaParseReal parses string to float with optional default
func luaParseReal(L *lua.LState) int {
	s := L.CheckString(1)
	defaultVal := float64(0)
	if L.GetTop() >= 2 {
		defaultVal = float64(L.CheckNumber(2))
	}

	s = strings.TrimSpace(s)
	if val, err := strconv.ParseFloat(s, 64); err == nil {
		L.Push(lua.LNumber(val))
	} else {
		L.Push(lua.LNumber(defaultVal))
	}
	return 1
}

// luaParseBool parses various boolean representations
func luaParseBool(L *lua.LState) int {
	s := L.CheckString(1)
	s = strings.ToLower(strings.TrimSpace(s))

	switch s {
	case "yes", "true", "1", "on":
		L.Push(lua.LTrue)
	case "no", "false", "0", "off", "":
		L.Push(lua.LFalse)
	default:
		// Check if it's a non-empty value (often means true in OSM)
		if s != "" {
			L.Push(lua.LTrue)
		} else {
			L.Push(lua.LFalse)
		}
	}
	return 1
}

// luaParseDirection parses oneway/direction tags
// Returns: 1 (forward), -1 (backward), 0 (both/none)
func luaParseDirection(L *lua.LState) int {
	s := L.CheckString(1)
	s = strings.ToLower(strings.TrimSpace(s))

	switch s {
	case "yes", "true", "1":
		L.Push(lua.LNumber(1))
	case "-1", "reverse", "backward":
		L.Push(lua.LNumber(-1))
	default:
		L.Push(lua.LNumber(0))
	}
	return 1
}

// luaParseLayer parses layer tag with clamping
func luaParseLayer(L *lua.LState) int {
	s := L.CheckString(1)
	s = strings.TrimSpace(s)

	layer := int64(0)
	if val, err := strconv.ParseInt(s, 10, 64); err == nil {
		layer = val
	}

	// Clamp to reasonable range
	if layer < -10 {
		layer = -10
	} else if layer > 10 {
		layer = 10
	}

	L.Push(lua.LNumber(layer))
	return 1
}

// luaGetName gets the best name from tags
// Priority: name, then int_name, then name:en
func luaGetName(L *lua.LState) int {
	tags := L.CheckTable(1)

	// Try name first
	if name := L.GetField(tags, "name"); name != lua.LNil {
		if s := lua.LVAsString(name); s != "" {
			L.Push(lua.LString(s))
			return 1
		}
	}

	// Try int_name
	if name := L.GetField(tags, "int_name"); name != lua.LNil {
		if s := lua.LVAsString(name); s != "" {
			L.Push(lua.LString(s))
			return 1
		}
	}

	// Try name:en
	if name := L.GetField(tags, "name:en"); name != lua.LNil {
		if s := lua.LVAsString(name); s != "" {
			L.Push(lua.LString(s))
			return 1
		}
	}

	L.Push(lua.LNil)
	return 1
}

// luaGetNameLocalized gets name in specific language with fallback
// Usage: get_name_localized(tags, "de") -> name:de or name
func luaGetNameLocalized(L *lua.LState) int {
	tags := L.CheckTable(1)
	lang := L.CheckString(2)

	// Try localized name first
	localKey := "name:" + lang
	if name := L.GetField(tags, localKey); name != lua.LNil {
		if s := lua.LVAsString(name); s != "" {
			L.Push(lua.LString(s))
			return 1
		}
	}

	// Fallback to default name
	if name := L.GetField(tags, "name"); name != lua.LNil {
		if s := lua.LVAsString(name); s != "" {
			L.Push(lua.LString(s))
			return 1
		}
	}

	L.Push(lua.LNil)
	return 1
}

// luaTagsToJSON converts tags table to JSON string
func luaTagsToJSON(L *lua.LState) int {
	tags := L.CheckTable(1)

	m := make(map[string]string)
	tags.ForEach(func(k, v lua.LValue) {
		key := lua.LVAsString(k)
		val := lua.LVAsString(v)
		if key != "" {
			m[key] = val
		}
	})

	jsonBytes, err := json.Marshal(m)
	if err != nil {
		L.Push(lua.LString("{}"))
	} else {
		L.Push(lua.LString(string(jsonBytes)))
	}
	return 1
}

// luaTagsToHstore converts tags table to PostgreSQL hstore string
func luaTagsToHstore(L *lua.LState) int {
	tags := L.CheckTable(1)

	var parts []string
	tags.ForEach(func(k, v lua.LValue) {
		key := lua.LVAsString(k)
		val := lua.LVAsString(v)
		if key != "" {
			// Escape quotes
			key = strings.ReplaceAll(key, `"`, `\"`)
			val = strings.ReplaceAll(val, `"`, `\"`)
			parts = append(parts, `"`+key+`"=>"`+val+`"`)
		}
	})

	L.Push(lua.LString(strings.Join(parts, ", ")))
	return 1
}

// luaFilterTags filters tags keeping only specified keys
// Usage: filter_tags(tags, {"name", "highway", "ref"})
func luaFilterTags(L *lua.LState) int {
	tags := L.CheckTable(1)
	keepKeys := L.CheckTable(2)

	// Build set of keys to keep
	keep := make(map[string]bool)
	keepKeys.ForEach(func(_, v lua.LValue) {
		if s := lua.LVAsString(v); s != "" {
			keep[s] = true
		}
	})

	// Create filtered table
	result := L.NewTable()
	tags.ForEach(func(k, v lua.LValue) {
		key := lua.LVAsString(k)
		if keep[key] {
			L.SetField(result, key, v)
		}
	})

	L.Push(result)
	return 1
}

// luaCalcZOrder calculates z_order for roads based on highway/railway tags
func luaCalcZOrder(L *lua.LState) int {
	tags := L.CheckTable(1)

	z := 0

	// Highway z_order values
	highwayZOrder := map[string]int{
		"motorway":      380,
		"motorway_link": 375,
		"trunk":         370,
		"trunk_link":    365,
		"primary":       360,
		"primary_link":  355,
		"secondary":     350,
		"secondary_link": 345,
		"tertiary":      340,
		"tertiary_link": 335,
		"residential":   330,
		"unclassified":  330,
		"road":          330,
		"living_street": 320,
		"pedestrian":    310,
		"service":       300,
		"track":         290,
		"path":          280,
		"footway":       280,
		"cycleway":      280,
		"bridleway":     280,
		"steps":         270,
	}

	// Railway z_order values
	railwayZOrder := map[string]int{
		"rail":       440,
		"subway":     420,
		"tram":       410,
		"light_rail": 430,
		"narrow_gauge": 420,
		"monorail":   420,
	}

	// Check highway
	if hw := L.GetField(tags, "highway"); hw != lua.LNil {
		if val, ok := highwayZOrder[lua.LVAsString(hw)]; ok {
			z = val
		} else {
			z = 300 // default highway
		}
	}

	// Check railway
	if rw := L.GetField(tags, "railway"); rw != lua.LNil {
		if val, ok := railwayZOrder[lua.LVAsString(rw)]; ok {
			if val > z {
				z = val
			}
		}
	}

	// Layer adjustment
	if layer := L.GetField(tags, "layer"); layer != lua.LNil {
		if layerVal, err := strconv.Atoi(lua.LVAsString(layer)); err == nil {
			z += layerVal * 10
		}
	}

	// Bridge adjustment
	if bridge := L.GetField(tags, "bridge"); bridge != lua.LNil {
		s := lua.LVAsString(bridge)
		if s != "" && s != "no" {
			z += 100
		}
	}

	// Tunnel adjustment
	if tunnel := L.GetField(tags, "tunnel"); tunnel != lua.LNil {
		s := lua.LVAsString(tunnel)
		if s != "" && s != "no" {
			z -= 100
		}
	}

	L.Push(lua.LNumber(z))
	return 1
}

// luaIsArea checks if tags indicate an area
func luaIsArea(L *lua.LState) int {
	tags := L.CheckTable(1)
	isClosed := true
	if L.GetTop() >= 2 {
		isClosed = L.CheckBool(2)
	}

	if !isClosed {
		L.Push(lua.LFalse)
		return 1
	}

	// Explicit area tag
	if area := L.GetField(tags, "area"); area != lua.LNil {
		s := strings.ToLower(lua.LVAsString(area))
		if s == "yes" {
			L.Push(lua.LTrue)
			return 1
		}
		if s == "no" {
			L.Push(lua.LFalse)
			return 1
		}
	}

	// Tags that typically indicate areas
	areaTags := []string{
		"building", "landuse", "natural", "water", "waterway",
		"leisure", "amenity", "shop", "tourism", "place",
	}

	for _, tag := range areaTags {
		if v := L.GetField(tags, tag); v != lua.LNil && lua.LVAsString(v) != "" {
			L.Push(lua.LTrue)
			return 1
		}
	}

	L.Push(lua.LFalse)
	return 1
}

// CleanTagKey removes characters not suitable for column names
func CleanTagKey(key string) string {
	var result strings.Builder
	for _, r := range key {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			result.WriteRune(r)
		} else if r == ':' || r == '-' {
			result.WriteRune('_')
		}
	}
	return result.String()
}
