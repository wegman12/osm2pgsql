# Lua Flex API Reference

osm2pgsql-go supports Lua scripts for defining custom output tables and processing OSM data. This API is compatible with osm2pgsql's Flex output.

## Table of Contents

- [Getting Started](#getting-started)
- [Defining Tables](#defining-tables)
- [Processing Callbacks](#processing-callbacks)
- [Object Properties](#object-properties)
- [Geometry Methods](#geometry-methods)
- [Transform Helpers](#transform-helpers)
- [Complete Examples](#complete-examples)

## Getting Started

Create a Lua file with table definitions and processing callbacks:

```lua
-- my-style.lua

-- Define output table
local pois = osm2pgsql.define_table({
    name = 'pois',
    ids = { type = 'node' },
    columns = {
        { column = 'name', type = 'text' },
        { column = 'amenity', type = 'text' },
        { column = 'geom', type = 'point' },
    }
})

-- Process nodes
function osm2pgsql.process_node(object)
    if object.tags.amenity then
        pois:insert({
            name = object.tags.name,
            amenity = object.tags.amenity,
            geom = object:as_point()
        })
    end
end
```

Run with:
```bash
osm2pgsql-go import -d mydb -S my-style.lua data.osm.pbf
```

## Defining Tables

### osm2pgsql.define_table(definition)

Creates an output table and returns a table object for inserting rows.

```lua
local roads = osm2pgsql.define_table({
    -- Required: table name
    name = 'roads',

    -- Optional: schema (defaults to config --db-schema)
    schema = 'public',

    -- Required: ID type determines which objects can be inserted
    ids = { type = 'way' },  -- 'node', 'way', 'relation', 'area', 'any'

    -- Required: column definitions
    columns = {
        { column = 'name', type = 'text' },
        { column = 'highway', type = 'text' },
        { column = 'lanes', type = 'int', not_null = true },
        { column = 'geom', type = 'linestring', srid = 3857 },
    },

    -- Optional: additional indexes
    indexes = {
        { column = 'highway' },
    },

    -- Optional: cluster table on this column
    cluster = 'geom',
})
```

### ID Types

| Type | Description | Adds Columns |
|------|-------------|--------------|
| `node` | Only nodes | `osm_id` |
| `way` | Only ways | `osm_id` |
| `relation` | Only relations | `osm_id` |
| `area` | Ways and relations | `osm_id`, `osm_type` |
| `any` | All object types | `osm_id`, `osm_type` |

### Column Types

| Type | PostgreSQL | Description |
|------|------------|-------------|
| `text`, `string` | TEXT | Variable-length string |
| `int`, `integer` | INTEGER | 32-bit integer |
| `bigint` | BIGINT | 64-bit integer |
| `real`, `float` | REAL | Floating-point number |
| `bool`, `boolean` | BOOLEAN | True/false value |
| `json` | JSON | JSON data |
| `jsonb` | JSONB | Binary JSON (recommended) |
| `hstore` | HSTORE | Key-value pairs |
| `timestamp` | TIMESTAMP WITH TIME ZONE | Date/time |
| `point` | GEOMETRY(Point) | Point geometry |
| `linestring` | GEOMETRY(LineString) | Line geometry |
| `polygon` | GEOMETRY(Polygon) | Polygon geometry |
| `multilinestring` | GEOMETRY(MultiLineString) | Multi-line geometry |
| `multipolygon` | GEOMETRY(MultiPolygon) | Multi-polygon geometry |
| `geometry` | GEOMETRY | Any geometry type |
| `geometrycollection` | GEOMETRY(GeometryCollection) | Collection |

### Column Options

```lua
{
    column = 'name',       -- Column name (required)
    type = 'text',         -- Column type (required)
    not_null = true,       -- Add NOT NULL constraint
    create_only = true,    -- Don't include in INSERT (for serial, etc.)
    srid = 3857,           -- Override default SRID for geometry
}
```

### table:insert(row)

Insert a row into the table:

```lua
roads:insert({
    name = object.tags.name,
    highway = object.tags.highway,
    lanes = tonumber(object.tags.lanes) or 1,
    geom = object:as_linestring()
})
```

## Processing Callbacks

Define these functions to process OSM objects:

### osm2pgsql.process_node(object)

Called for each node in the PBF file.

```lua
function osm2pgsql.process_node(object)
    if object.tags.amenity then
        pois:insert({
            name = object.tags.name,
            amenity = object.tags.amenity,
            geom = object:as_point()
        })
    end
end
```

### osm2pgsql.process_way(object)

Called for each way in the PBF file.

```lua
function osm2pgsql.process_way(object)
    if object.tags.highway then
        roads:insert({
            name = object.tags.name,
            highway = object.tags.highway,
            geom = object:as_linestring()
        })
    end

    if object.tags.building and object.is_closed then
        buildings:insert({
            name = object.tags.name,
            geom = object:as_polygon()
        })
    end
end
```

### osm2pgsql.process_relation(object)

Called for each relation in the PBF file.

```lua
function osm2pgsql.process_relation(object)
    if object.tags.type == 'multipolygon' and object.tags.building then
        buildings:insert({
            name = object.tags.name,
            geom = object:as_multipolygon()
        })
    end
end
```

## Object Properties

### Common Properties

| Property | Type | Description |
|----------|------|-------------|
| `object.id` | number | OSM object ID |
| `object.type` | string | `"node"`, `"way"`, or `"relation"` |
| `object.version` | number | Object version |
| `object.changeset` | number | Changeset ID |
| `object.uid` | number | User ID |
| `object.user` | string | Username |
| `object.tags` | table | Tag key-value pairs |

### Node Properties

| Property | Type | Description |
|----------|------|-------------|
| `object.lat` | number | Latitude in degrees |
| `object.lon` | number | Longitude in degrees |

### Way Properties

| Property | Type | Description |
|----------|------|-------------|
| `object.is_closed` | boolean | True if first node == last node |
| `object.nodes` | table | Array of node IDs |

### Relation Properties

| Property | Type | Description |
|----------|------|-------------|
| `object.members` | table | Array of member objects |

Member objects have:
- `member.type` - `"n"` (node), `"w"` (way), or `"r"` (relation)
- `member.ref` - Referenced object ID
- `member.role` - Member role string

## Geometry Methods

### object:as_point()

Convert a node to a point geometry.

```lua
function osm2pgsql.process_node(object)
    pois:insert({ geom = object:as_point() })
end
```

### object:as_linestring()

Convert a way to a linestring geometry.

```lua
function osm2pgsql.process_way(object)
    if object.tags.highway then
        roads:insert({ geom = object:as_linestring() })
    end
end
```

### object:as_polygon()

Convert a closed way to a polygon geometry.

```lua
function osm2pgsql.process_way(object)
    if object.is_closed and object.tags.building then
        buildings:insert({ geom = object:as_polygon() })
    end
end
```

### object:as_multilinestring()

Convert a relation to a multilinestring geometry.

```lua
function osm2pgsql.process_relation(object)
    if object.tags.type == 'route' then
        routes:insert({ geom = object:as_multilinestring() })
    end
end
```

### object:as_multipolygon()

Convert a relation to a multipolygon geometry.

```lua
function osm2pgsql.process_relation(object)
    if object.tags.type == 'multipolygon' then
        areas:insert({ geom = object:as_multipolygon() })
    end
end
```

### object:as_area()

Automatically choose polygon or multipolygon based on object type.

```lua
function osm2pgsql.process_way(object)
    if object.is_closed then
        areas:insert({ geom = object:as_area() })
    end
end

function osm2pgsql.process_relation(object)
    if object.tags.type == 'multipolygon' then
        areas:insert({ geom = object:as_area() })
    end
end
```

### object:grab_tag(key)

Get a tag value and remove it from the tags table.

```lua
local name = object:grab_tag('name')  -- Returns value, removes from tags
local remaining_tags = object.tags     -- No longer contains 'name'
```

## Transform Helpers

osm2pgsql-go provides built-in functions for common transformations.

### String Transforms

```lua
local t = osm2pgsql.transforms

-- Remove leading/trailing whitespace
t.trim("  hello  ")  -- "hello"

-- Convert case
t.lower("ROAD")      -- "road"
t.upper("road")      -- "ROAD"

-- Normalize whitespace (collapse multiple spaces)
t.clean_spaces("a   b  c")  -- "a b c"

-- Truncate to max length
t.truncate("hello world", 5)  -- "hello"
```

### Type Parsing

```lua
local t = osm2pgsql.transforms

-- Parse integer (with optional default)
t.parse_int("42")           -- 42
t.parse_int("invalid", 0)   -- 0

-- Parse float (with optional default)
t.parse_real("3.14")        -- 3.14
t.parse_real("invalid", 0)  -- 0.0

-- Parse boolean (yes/no/true/false/1/0)
t.parse_bool("yes")     -- true
t.parse_bool("no")      -- false
t.parse_bool("1")       -- true

-- Parse oneway direction
t.parse_direction("yes")      -- 1 (forward)
t.parse_direction("-1")       -- -1 (backward)
t.parse_direction("reverse")  -- -1
t.parse_direction("no")       -- 0

-- Parse layer with clamping (-10 to 10)
t.parse_layer("5")    -- 5
t.parse_layer("100")  -- 10 (clamped)
t.parse_layer("-50")  -- -10 (clamped)
```

### Name Handling

```lua
local t = osm2pgsql.transforms

-- Get best available name (tries: name, int_name, name:en)
get_name(object.tags)

-- Get localized name with fallback
t.get_name_localized(object.tags, "de")  -- name:de or name
t.get_name_localized(object.tags, "fr")  -- name:fr or name
```

### Tag Formatting

```lua
local t = osm2pgsql.transforms

-- Convert tags to JSON string
t.tags_to_json(object.tags)
-- '{"name":"Paris","highway":"primary"}'

-- Convert tags to PostgreSQL hstore format
t.tags_to_hstore(object.tags)
-- '"name"=>"Paris", "highway"=>"primary"'

-- Filter tags to keep only specified keys
local filtered = t.filter_tags(object.tags, {"name", "highway"})
```

### Road/Way Helpers

```lua
local t = osm2pgsql.transforms

-- Calculate z_order for rendering (handles bridge/tunnel/layer)
local z = t.calc_z_order(object.tags)
-- motorway = 380, primary = 360, residential = 330
-- +100 for bridge, -100 for tunnel
-- +10 per layer level

-- Check if tags indicate an area (for closed ways)
if t.is_area(object.tags, object.is_closed) then
    -- Process as polygon
end
```

### Convenience Globals

These common functions are also available as globals:

```lua
trim(s)              -- osm2pgsql.transforms.trim
parse_int(s, def)    -- osm2pgsql.transforms.parse_int
parse_bool(s)        -- osm2pgsql.transforms.parse_bool
get_name(tags)       -- osm2pgsql.transforms.get_name
```

## Complete Examples

### Simple POI Extraction

```lua
local pois = osm2pgsql.define_table({
    name = 'pois',
    ids = { type = 'node' },
    columns = {
        { column = 'name', type = 'text' },
        { column = 'category', type = 'text' },
        { column = 'tags', type = 'jsonb' },
        { column = 'geom', type = 'point' },
    }
})

function osm2pgsql.process_node(object)
    local tags = object.tags
    local category = tags.amenity or tags.shop or tags.tourism

    if category then
        pois:insert({
            name = get_name(tags),
            category = category,
            tags = osm2pgsql.transforms.tags_to_json(tags),
            geom = object:as_point()
        })
    end
end
```

### Road Network with Z-Order

```lua
local t = osm2pgsql.transforms

local roads = osm2pgsql.define_table({
    name = 'roads',
    ids = { type = 'way' },
    columns = {
        { column = 'name', type = 'text' },
        { column = 'ref', type = 'text' },
        { column = 'highway', type = 'text' },
        { column = 'oneway', type = 'int' },
        { column = 'layer', type = 'int' },
        { column = 'z_order', type = 'int' },
        { column = 'geom', type = 'linestring' },
    }
})

function osm2pgsql.process_way(object)
    if object.tags.highway then
        roads:insert({
            name = t.clean_spaces(object.tags.name or ""),
            ref = t.truncate(object.tags.ref or "", 20),
            highway = t.lower(object.tags.highway),
            oneway = t.parse_direction(object.tags.oneway or ""),
            layer = t.parse_layer(object.tags.layer or "0"),
            z_order = t.calc_z_order(object.tags),
            geom = object:as_linestring()
        })
    end
end
```

### Buildings with Area Calculation

```lua
local t = osm2pgsql.transforms

local buildings = osm2pgsql.define_table({
    name = 'buildings',
    ids = { type = 'area' },
    columns = {
        { column = 'name', type = 'text' },
        { column = 'building_type', type = 'text' },
        { column = 'levels', type = 'int' },
        { column = 'height', type = 'real' },
        { column = 'geom', type = 'geometry' },
    }
})

local function process_building(object, geom)
    buildings:insert({
        name = get_name(object.tags),
        building_type = t.lower(object.tags.building),
        levels = parse_int(object.tags["building:levels"] or "0"),
        height = t.parse_real(object.tags.height or "0"),
        geom = geom
    })
end

function osm2pgsql.process_way(object)
    if object.tags.building and object.is_closed then
        process_building(object, object:as_polygon())
    end
end

function osm2pgsql.process_relation(object)
    if object.tags.type == 'multipolygon' and object.tags.building then
        process_building(object, object:as_multipolygon())
    end
end
```

## osm2pgsql Module

### Properties

| Property | Type | Description |
|----------|------|-------------|
| `osm2pgsql.version` | string | Version string |
| `osm2pgsql.mode` | string | Always `"flex"` |
| `osm2pgsql.srid` | number | Target SRID from config |
| `osm2pgsql.stage` | number | Processing stage (1) |

### Functions

| Function | Description |
|----------|-------------|
| `osm2pgsql.define_table(def)` | Define an output table |

### Transforms Table

Access via `osm2pgsql.transforms`:

| Function | Description |
|----------|-------------|
| `trim(s)` | Remove whitespace |
| `lower(s)` | Convert to lowercase |
| `upper(s)` | Convert to uppercase |
| `clean_spaces(s)` | Normalize whitespace |
| `truncate(s, max)` | Limit string length |
| `parse_int(s, [default])` | Parse integer |
| `parse_real(s, [default])` | Parse float |
| `parse_bool(s)` | Parse boolean |
| `parse_direction(s)` | Parse oneway |
| `parse_layer(s)` | Parse layer |
| `get_name(tags)` | Get best name |
| `get_name_localized(tags, lang)` | Get localized name |
| `tags_to_json(tags)` | Convert to JSON |
| `tags_to_hstore(tags)` | Convert to hstore |
| `filter_tags(tags, keys)` | Filter tag keys |
| `calc_z_order(tags)` | Calculate z-order |
| `is_area(tags, closed)` | Check if area |
