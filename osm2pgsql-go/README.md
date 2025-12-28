# osm2pgsql-go

A high-performance OpenStreetMap data importer for PostgreSQL/PostGIS, written in Go.

## Features

- **Fast parallel processing** - Multi-threaded PBF parsing and geometry building
- **Memory-efficient** - Memory-mapped node index for O(1) coordinate lookups
- **Streaming architecture** - Pipelined extraction and loading for reduced import time
- **Lua Flex support** - Custom table definitions and tag transformations
- **Incremental updates** - Slim mode with OSC change file support
- **Tile expiry** - Generate tile expiry lists for cache invalidation
- **Replication** - Built-in support for continuous OSM updates

## Installation

### From Source

```bash
# Clone the repository
git clone https://github.com/kevinramage/osm2pgsql-go.git
cd osm2pgsql-go

# Build
go build -o osm2pgsql-go .

# Install (optional)
go install
```

### Requirements

- Go 1.21+
- PostgreSQL 12+ with PostGIS 3.0+
- Sufficient disk space for intermediate files

## Quick Start

### Basic Import

```bash
# Import an OSM PBF file
osm2pgsql-go import -d gis -U postgres monaco-latest.osm.pbf

# With Web Mercator projection (for tile rendering)
osm2pgsql-go import -d gis -E 3857 monaco-latest.osm.pbf

# With bounding box filter
osm2pgsql-go import -d gis -b "7.4,43.7,7.5,43.8" monaco-latest.osm.pbf
```

### Using Lua Flex Styles

```bash
# Import with custom Lua style
osm2pgsql-go import -d gis -S examples/simple.lua monaco-latest.osm.pbf

# Using the OpenStreetMap Carto style
osm2pgsql-go import -d gis -S examples/openstreetmap-carto.lua planet.osm.pbf
```

### Incremental Updates

```bash
# Initial import with slim mode
osm2pgsql-go import -d gis --slim monaco-latest.osm.pbf

# Apply updates
osm2pgsql-go import -d gis --append changes.osc.gz
```

## Command Reference

### Global Options

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--verbose` | `-v` | false | Enable verbose output |
| `--output-dir` | `-o` | `.` | Directory for intermediate files |
| `--workers` | `-j` | CPU count | Number of parallel workers |
| `--log-file` | | | Path to log file (JSON format) |
| `--metrics-interval` | | 30s | Interval for metrics logging |

### Database Options

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--db-host` | | localhost | PostgreSQL host |
| `--db-port` | | 5432 | PostgreSQL port |
| `--db-name` | `-d` | osm | Database name |
| `--db-user` | `-U` | postgres | Database user |
| `--db-password` | `-W` | | Database password |
| `--db-schema` | | public | Target schema |

### Import Options

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--projection` | `-E` | 4326 | Target SRID (4326 or 3857) |
| `--bbox` | `-b` | | Bounding box: minlon,minlat,maxlon,maxlat |
| `--style` | `-S` | | Style file (YAML or Lua) |
| `--slim` | | false | Enable slim mode for updates |
| `--append` | | false | Apply OSC changes |
| `--drop` | | false | Drop slim tables after import |
| `--flat-nodes` | | | Path to flat nodes file |
| `--create-indexes` | | true | Create spatial indexes |
| `--drop-existing` | | false | Drop existing tables |
| `--extra-attributes` | | false | Include metadata columns |
| `--hstore` | | false | Use hstore instead of JSONB |
| `--expire-output` | `-e` | | Tile expiry output file |
| `--expire-min-zoom` | | 1 | Minimum zoom for expiry |
| `--expire-max-zoom` | | 18 | Maximum zoom for expiry |

### Replication Commands

```bash
# Initialize replication
osm2pgsql-go replication init -d gis --server https://planet.openstreetmap.org/replication/day

# Check replication status
osm2pgsql-go replication status -d gis

# Apply single update
osm2pgsql-go replication update -d gis

# Start continuous updates
osm2pgsql-go replication start -d gis --interval 1m

# List available replication sources
osm2pgsql-go replication list-sources
```

## Lua Flex Styles

osm2pgsql-go supports Lua scripts for custom table definitions and tag processing, compatible with osm2pgsql's Flex output.

### Basic Example

```lua
-- Define a roads table
local roads = osm2pgsql.define_table({
    name = 'roads',
    ids = { type = 'way' },
    columns = {
        { column = 'name', type = 'text' },
        { column = 'highway', type = 'text' },
        { column = 'geom', type = 'linestring' },
    }
})

-- Process ways
function osm2pgsql.process_way(object)
    if object.tags.highway then
        roads:insert({
            name = object.tags.name,
            highway = object.tags.highway,
            geom = object:as_linestring()
        })
    end
end
```

### Available Column Types

| Type | PostgreSQL Type |
|------|-----------------|
| `text`, `string` | TEXT |
| `int`, `integer` | INTEGER |
| `bigint` | BIGINT |
| `real`, `float` | REAL |
| `bool`, `boolean` | BOOLEAN |
| `json` | JSON |
| `jsonb` | JSONB |
| `hstore` | HSTORE |
| `point` | GEOMETRY(Point) |
| `linestring` | GEOMETRY(LineString) |
| `polygon` | GEOMETRY(Polygon) |
| `multipolygon` | GEOMETRY(MultiPolygon) |
| `geometry` | GEOMETRY |

### Geometry Methods

| Method | Description |
|--------|-------------|
| `object:as_point()` | Convert node to point |
| `object:as_linestring()` | Convert way to linestring |
| `object:as_polygon()` | Convert closed way to polygon |
| `object:as_multipolygon()` | Convert relation to multipolygon |
| `object:as_area()` | Auto-detect polygon/multipolygon |
| `object:grab_tag(key)` | Get and remove a tag |

### Transform Helpers

osm2pgsql-go provides built-in functions for common tag transformations:

```lua
local transforms = osm2pgsql.transforms

-- String transforms
transforms.trim(s)              -- Remove whitespace
transforms.lower(s)             -- Convert to lowercase
transforms.upper(s)             -- Convert to uppercase
transforms.clean_spaces(s)      -- Normalize whitespace
transforms.truncate(s, max)     -- Limit string length

-- Type parsing
transforms.parse_int(s, default)    -- Parse integer
transforms.parse_real(s, default)   -- Parse float
transforms.parse_bool(s)            -- Parse yes/no/true/false
transforms.parse_direction(s)       -- Parse oneway (1, -1, 0)
transforms.parse_layer(s)           -- Parse layer with clamping

-- Name handling
get_name(tags)                          -- Get best name
transforms.get_name_localized(tags, lang)  -- Get localized name

-- Tag formatting
transforms.tags_to_json(tags)       -- Convert to JSON string
transforms.tags_to_hstore(tags)     -- Convert to hstore format
transforms.filter_tags(tags, keys)  -- Keep only specified keys

-- Road helpers
transforms.calc_z_order(tags)   -- Calculate render order
transforms.is_area(tags, closed) -- Check if area
```

Convenience globals are also available: `trim`, `parse_int`, `parse_bool`, `get_name`

## Output Tables

### Default Schema (no style file)

| Table | Contents |
|-------|----------|
| `planet_osm_point` | Nodes with tags |
| `planet_osm_line` | Ways as linestrings |
| `planet_osm_polygon` | Closed ways and multipolygons |

### Default Columns

- `osm_id` - OSM object ID
- `osm_type` - Object type (node/way/relation)
- `tags` - All tags as JSONB
- `geom` - PostGIS geometry

With `--extra-attributes`:
- `version` - Object version
- `changeset` - Changeset ID
- `timestamp` - Last modified time
- `uid` - User ID
- `user` - Username

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         PBF File                             │
└─────────────────────────┬───────────────────────────────────┘
                          │
         ┌────────────────┴────────────────┐
         ▼                                 ▼
┌─────────────────┐              ┌─────────────────┐
│  Pass 1: Nodes  │              │  Pass 2: Ways   │
│  → mmap index   │              │  + Relations    │
└────────┬────────┘              └────────┬────────┘
         │                                │
         │    ┌───────────────────────────┘
         │    │
         ▼    ▼
┌─────────────────────────────────────────────────────────────┐
│              Geometry Builder (parallel workers)             │
│  - Coordinate lookup from mmap index                        │
│  - WKB encoding                                             │
│  - Projection transformation                                │
└─────────────────────────┬───────────────────────────────────┘
                          │
         ┌────────────────┼────────────────┐
         ▼                ▼                ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│    Points    │  │    Lines     │  │   Polygons   │
│   Channel    │  │   Channel    │  │   Channel    │
└──────┬───────┘  └──────┬───────┘  └──────┬───────┘
       │                 │                 │
       ▼                 ▼                 ▼
┌─────────────────────────────────────────────────────────────┐
│              PostgreSQL Bulk Loader (COPY)                   │
└─────────────────────────────────────────────────────────────┘
```

## Performance Tips

1. **Use `--flat-nodes`** for imports larger than 1GB
   ```bash
   osm2pgsql-go import -d gis --flat-nodes nodes.cache planet.osm.pbf
   ```

2. **Increase workers** for multi-core systems
   ```bash
   osm2pgsql-go import -d gis -j 16 planet.osm.pbf
   ```

3. **Use Web Mercator** for tile rendering
   ```bash
   osm2pgsql-go import -d gis -E 3857 planet.osm.pbf
   ```

4. **Use tablespaces** on separate SSDs
   ```bash
   osm2pgsql-go import -d gis \
     --tablespace-main fast_ssd \
     --tablespace-index index_ssd \
     planet.osm.pbf
   ```

5. **Tune PostgreSQL** for bulk loading:
   ```sql
   -- During import
   SET maintenance_work_mem = '4GB';
   SET max_wal_size = '10GB';
   SET checkpoint_completion_target = 0.9;
   ```

## Project Structure

```
osm2pgsql-go/
├── cmd/                    # CLI commands
│   ├── root.go            # Root command and global flags
│   ├── import.go          # Import command
│   └── replication.go     # Replication commands
├── internal/
│   ├── config/            # Configuration handling
│   ├── expire/            # Tile expiry calculation
│   ├── flex/              # Lua Flex runtime
│   ├── loader/            # PostgreSQL bulk loader
│   ├── logger/            # Structured logging
│   ├── metrics/           # Performance metrics
│   ├── middle/            # Slim mode middle tables
│   ├── nodeindex/         # Memory-mapped node index
│   ├── osc/               # OSC change file parser
│   ├── pbf/               # PBF file parser
│   ├── pipeline/          # Import pipeline coordinator
│   ├── proj/              # Coordinate projection
│   ├── replication/       # Replication state management
│   ├── style/             # Style file parsing
│   ├── transform/         # Tag transformations
│   └── wkb/               # WKB geometry encoding
├── examples/              # Example Lua styles
│   ├── simple.lua
│   ├── openstreetmap-carto.lua
│   └── transform-helpers.lua
└── main.go
```

## License

MIT License - see LICENSE file for details.

## Acknowledgments

- Inspired by [osm2pgsql](https://osm2pgsql.org/)
- Uses [paulmach/osm](https://github.com/paulmach/osm) for PBF parsing
- Uses [jackc/pgx](https://github.com/jackc/pgx) for PostgreSQL connectivity
- Uses [yuin/gopher-lua](https://github.com/yuin/gopher-lua) for Lua runtime
