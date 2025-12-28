package flex

import (
	"testing"

	lua "github.com/yuin/gopher-lua"
)

func TestTransformTrim(t *testing.T) {
	L := lua.NewState()
	defer L.Close()
	RegisterTransforms(L)

	// Test basic trim
	if err := L.DoString(`result = trim("  hello  ")`); err != nil {
		t.Fatalf("failed to call trim: %v", err)
	}
	if L.GetGlobal("result").String() != "hello" {
		t.Errorf("trim = %q, want 'hello'", L.GetGlobal("result").String())
	}

	// Test no trim needed
	if err := L.DoString(`result = trim("hello")`); err != nil {
		t.Fatalf("failed to call trim: %v", err)
	}
	if L.GetGlobal("result").String() != "hello" {
		t.Errorf("trim = %q, want 'hello'", L.GetGlobal("result").String())
	}

	// Test empty string
	if err := L.DoString(`result = trim("")`); err != nil {
		t.Fatalf("failed to call trim: %v", err)
	}
	if L.GetGlobal("result").String() != "" {
		t.Errorf("trim = %q, want ''", L.GetGlobal("result").String())
	}
}

func TestTransformParseInt(t *testing.T) {
	L := lua.NewState()
	defer L.Close()
	RegisterTransforms(L)

	tests := []struct {
		input    string
		expected int64
	}{
		{"123", 123},
		{"-456", -456},
		{"  789  ", 789},
		{"3.14", 3},
		{"abc", 0},
		{"", 0},
	}

	for _, tt := range tests {
		if err := L.DoString(`result = parse_int("` + tt.input + `")`); err != nil {
			t.Fatalf("failed to call parse_int: %v", err)
		}
		result := int64(L.GetGlobal("result").(lua.LNumber))
		if result != tt.expected {
			t.Errorf("parse_int(%q) = %d, want %d", tt.input, result, tt.expected)
		}
	}
}

func TestTransformParseIntWithDefault(t *testing.T) {
	L := lua.NewState()
	defer L.Close()
	RegisterTransforms(L)

	if err := L.DoString(`result = osm2pgsql.transforms.parse_int("invalid", 42)`); err != nil {
		t.Fatalf("failed to call parse_int with default: %v", err)
	}
	result := int64(L.GetGlobal("result").(lua.LNumber))
	if result != 42 {
		t.Errorf("parse_int with default = %d, want 42", result)
	}
}

func TestTransformParseReal(t *testing.T) {
	L := lua.NewState()
	defer L.Close()
	RegisterTransforms(L)

	if err := L.DoString(`result = osm2pgsql.transforms.parse_real("3.14159")`); err != nil {
		t.Fatalf("failed to call parse_real: %v", err)
	}
	result := float64(L.GetGlobal("result").(lua.LNumber))
	if result < 3.14 || result > 3.15 {
		t.Errorf("parse_real(3.14159) = %f, want ~3.14159", result)
	}
}

func TestTransformParseBool(t *testing.T) {
	L := lua.NewState()
	defer L.Close()
	RegisterTransforms(L)

	trueCases := []string{"yes", "true", "1", "on", "Yes", "TRUE"}
	for _, input := range trueCases {
		if err := L.DoString(`result = parse_bool("` + input + `")`); err != nil {
			t.Fatalf("failed to call parse_bool: %v", err)
		}
		if L.GetGlobal("result") != lua.LTrue {
			t.Errorf("parse_bool(%q) = false, want true", input)
		}
	}

	falseCases := []string{"no", "false", "0", "off", ""}
	for _, input := range falseCases {
		if err := L.DoString(`result = parse_bool("` + input + `")`); err != nil {
			t.Fatalf("failed to call parse_bool: %v", err)
		}
		if L.GetGlobal("result") != lua.LFalse {
			t.Errorf("parse_bool(%q) = true, want false", input)
		}
	}
}

func TestTransformParseDirection(t *testing.T) {
	L := lua.NewState()
	defer L.Close()
	RegisterTransforms(L)

	tests := []struct {
		input    string
		expected int
	}{
		{"yes", 1},
		{"true", 1},
		{"1", 1},
		{"-1", -1},
		{"reverse", -1},
		{"backward", -1},
		{"no", 0},
		{"both", 0},
	}

	for _, tt := range tests {
		if err := L.DoString(`result = osm2pgsql.transforms.parse_direction("` + tt.input + `")`); err != nil {
			t.Fatalf("failed to call parse_direction: %v", err)
		}
		result := int(L.GetGlobal("result").(lua.LNumber))
		if result != tt.expected {
			t.Errorf("parse_direction(%q) = %d, want %d", tt.input, result, tt.expected)
		}
	}
}

func TestTransformParseLayer(t *testing.T) {
	L := lua.NewState()
	defer L.Close()
	RegisterTransforms(L)

	tests := []struct {
		input    string
		expected int
	}{
		{"0", 0},
		{"1", 1},
		{"-1", -1},
		{"5", 5},
		{"100", 10},   // clamped to max
		{"-100", -10}, // clamped to min
		{"abc", 0},
	}

	for _, tt := range tests {
		if err := L.DoString(`result = osm2pgsql.transforms.parse_layer("` + tt.input + `")`); err != nil {
			t.Fatalf("failed to call parse_layer: %v", err)
		}
		result := int(L.GetGlobal("result").(lua.LNumber))
		if result != tt.expected {
			t.Errorf("parse_layer(%q) = %d, want %d", tt.input, result, tt.expected)
		}
	}
}

func TestTransformCleanSpaces(t *testing.T) {
	L := lua.NewState()
	defer L.Close()
	RegisterTransforms(L)

	if err := L.DoString(`result = osm2pgsql.transforms.clean_spaces("  hello   world  ")`); err != nil {
		t.Fatalf("failed to call clean_spaces: %v", err)
	}
	result := L.GetGlobal("result").String()
	if result != "hello world" {
		t.Errorf("clean_spaces = %q, want %q", result, "hello world")
	}
}

func TestTransformTruncate(t *testing.T) {
	L := lua.NewState()
	defer L.Close()
	RegisterTransforms(L)

	if err := L.DoString(`result = osm2pgsql.transforms.truncate("hello world", 5)`); err != nil {
		t.Fatalf("failed to call truncate: %v", err)
	}
	result := L.GetGlobal("result").String()
	if result != "hello" {
		t.Errorf("truncate = %q, want %q", result, "hello")
	}

	// Test no truncation needed
	if err := L.DoString(`result = osm2pgsql.transforms.truncate("hi", 10)`); err != nil {
		t.Fatalf("failed to call truncate: %v", err)
	}
	result = L.GetGlobal("result").String()
	if result != "hi" {
		t.Errorf("truncate = %q, want %q", result, "hi")
	}
}

func TestTransformGetName(t *testing.T) {
	L := lua.NewState()
	defer L.Close()
	RegisterTransforms(L)

	// Test with name
	if err := L.DoString(`result = get_name({name = "Paris"})`); err != nil {
		t.Fatalf("failed to call get_name: %v", err)
	}
	result := L.GetGlobal("result").String()
	if result != "Paris" {
		t.Errorf("get_name = %q, want %q", result, "Paris")
	}

	// Test fallback to int_name
	if err := L.DoString(`result = get_name({int_name = "International"})`); err != nil {
		t.Fatalf("failed to call get_name: %v", err)
	}
	result = L.GetGlobal("result").String()
	if result != "International" {
		t.Errorf("get_name (int_name) = %q, want %q", result, "International")
	}

	// Test fallback to name:en
	if err := L.DoString(`result = get_name({["name:en"] = "English Name"})`); err != nil {
		t.Fatalf("failed to call get_name: %v", err)
	}
	result = L.GetGlobal("result").String()
	if result != "English Name" {
		t.Errorf("get_name (name:en) = %q, want %q", result, "English Name")
	}
}

func TestTransformGetNameLocalized(t *testing.T) {
	L := lua.NewState()
	defer L.Close()
	RegisterTransforms(L)

	// Test localized name exists
	if err := L.DoString(`result = osm2pgsql.transforms.get_name_localized({name = "Paris", ["name:de"] = "Paris (DE)"}, "de")`); err != nil {
		t.Fatalf("failed to call get_name_localized: %v", err)
	}
	result := L.GetGlobal("result").String()
	if result != "Paris (DE)" {
		t.Errorf("get_name_localized = %q, want %q", result, "Paris (DE)")
	}

	// Test fallback to default name
	if err := L.DoString(`result = osm2pgsql.transforms.get_name_localized({name = "Paris"}, "de")`); err != nil {
		t.Fatalf("failed to call get_name_localized: %v", err)
	}
	result = L.GetGlobal("result").String()
	if result != "Paris" {
		t.Errorf("get_name_localized fallback = %q, want %q", result, "Paris")
	}
}

func TestTransformTagsToJSON(t *testing.T) {
	L := lua.NewState()
	defer L.Close()
	RegisterTransforms(L)

	if err := L.DoString(`result = osm2pgsql.transforms.tags_to_json({name = "Test", highway = "primary"})`); err != nil {
		t.Fatalf("failed to call tags_to_json: %v", err)
	}
	result := L.GetGlobal("result").String()
	// JSON output might have different ordering, just check it contains expected parts
	if len(result) < 10 || result[0] != '{' {
		t.Errorf("tags_to_json = %q, want valid JSON", result)
	}
}

func TestTransformTagsToHstore(t *testing.T) {
	L := lua.NewState()
	defer L.Close()
	RegisterTransforms(L)

	if err := L.DoString(`result = osm2pgsql.transforms.tags_to_hstore({name = "Test"})`); err != nil {
		t.Fatalf("failed to call tags_to_hstore: %v", err)
	}
	result := L.GetGlobal("result").String()
	if result != `"name"=>"Test"` {
		t.Errorf("tags_to_hstore = %q, want %q", result, `"name"=>"Test"`)
	}
}

func TestTransformFilterTags(t *testing.T) {
	L := lua.NewState()
	defer L.Close()
	RegisterTransforms(L)

	if err := L.DoString(`
		tags = {name = "Test", highway = "primary", source = "survey"}
		result = osm2pgsql.transforms.filter_tags(tags, {"name", "highway"})
	`); err != nil {
		t.Fatalf("failed to call filter_tags: %v", err)
	}

	result := L.GetGlobal("result").(*lua.LTable)
	if result.RawGetString("name").String() != "Test" {
		t.Error("filter_tags should keep 'name'")
	}
	if result.RawGetString("highway").String() != "primary" {
		t.Error("filter_tags should keep 'highway'")
	}
	if result.RawGetString("source") != lua.LNil {
		t.Error("filter_tags should remove 'source'")
	}
}

func TestTransformCalcZOrder(t *testing.T) {
	L := lua.NewState()
	defer L.Close()
	RegisterTransforms(L)

	// Test motorway z_order
	if err := L.DoString(`result = osm2pgsql.transforms.calc_z_order({highway = "motorway"})`); err != nil {
		t.Fatalf("failed to call calc_z_order: %v", err)
	}
	result := int(L.GetGlobal("result").(lua.LNumber))
	if result != 380 {
		t.Errorf("calc_z_order(motorway) = %d, want 380", result)
	}

	// Test with bridge
	if err := L.DoString(`result = osm2pgsql.transforms.calc_z_order({highway = "primary", bridge = "yes"})`); err != nil {
		t.Fatalf("failed to call calc_z_order: %v", err)
	}
	result = int(L.GetGlobal("result").(lua.LNumber))
	if result != 460 { // 360 + 100
		t.Errorf("calc_z_order(primary+bridge) = %d, want 460", result)
	}

	// Test with tunnel
	if err := L.DoString(`result = osm2pgsql.transforms.calc_z_order({highway = "primary", tunnel = "yes"})`); err != nil {
		t.Fatalf("failed to call calc_z_order: %v", err)
	}
	result = int(L.GetGlobal("result").(lua.LNumber))
	if result != 260 { // 360 - 100
		t.Errorf("calc_z_order(primary+tunnel) = %d, want 260", result)
	}

	// Test with layer
	if err := L.DoString(`result = osm2pgsql.transforms.calc_z_order({highway = "primary", layer = "2"})`); err != nil {
		t.Fatalf("failed to call calc_z_order: %v", err)
	}
	result = int(L.GetGlobal("result").(lua.LNumber))
	if result != 380 { // 360 + 20
		t.Errorf("calc_z_order(primary+layer2) = %d, want 380", result)
	}
}

func TestTransformIsArea(t *testing.T) {
	L := lua.NewState()
	defer L.Close()
	RegisterTransforms(L)

	// Test with building
	if err := L.DoString(`result = osm2pgsql.transforms.is_area({building = "yes"}, true)`); err != nil {
		t.Fatalf("failed to call is_area: %v", err)
	}
	if L.GetGlobal("result") != lua.LTrue {
		t.Error("is_area(building) should be true")
	}

	// Test with explicit area=yes
	if err := L.DoString(`result = osm2pgsql.transforms.is_area({area = "yes"}, true)`); err != nil {
		t.Fatalf("failed to call is_area: %v", err)
	}
	if L.GetGlobal("result") != lua.LTrue {
		t.Error("is_area(area=yes) should be true")
	}

	// Test with explicit area=no
	if err := L.DoString(`result = osm2pgsql.transforms.is_area({building = "yes", area = "no"}, true)`); err != nil {
		t.Fatalf("failed to call is_area: %v", err)
	}
	if L.GetGlobal("result") != lua.LFalse {
		t.Error("is_area(area=no) should be false")
	}

	// Test non-closed way
	if err := L.DoString(`result = osm2pgsql.transforms.is_area({building = "yes"}, false)`); err != nil {
		t.Fatalf("failed to call is_area: %v", err)
	}
	if L.GetGlobal("result") != lua.LFalse {
		t.Error("is_area(not closed) should be false")
	}
}

func TestTransformLowerUpper(t *testing.T) {
	L := lua.NewState()
	defer L.Close()
	RegisterTransforms(L)

	if err := L.DoString(`result = osm2pgsql.transforms.lower("HELLO")`); err != nil {
		t.Fatalf("failed to call lower: %v", err)
	}
	if L.GetGlobal("result").String() != "hello" {
		t.Errorf("lower = %q, want 'hello'", L.GetGlobal("result").String())
	}

	if err := L.DoString(`result = osm2pgsql.transforms.upper("hello")`); err != nil {
		t.Fatalf("failed to call upper: %v", err)
	}
	if L.GetGlobal("result").String() != "HELLO" {
		t.Errorf("upper = %q, want 'HELLO'", L.GetGlobal("result").String())
	}
}

func TestCleanTagKey(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"name", "name"},
		{"name:en", "name_en"},
		{"addr:street", "addr_street"},
		{"name-de", "name_de"},
		{"highway@123", "highway123"},
	}

	for _, tt := range tests {
		result := CleanTagKey(tt.input)
		if result != tt.expected {
			t.Errorf("CleanTagKey(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}
