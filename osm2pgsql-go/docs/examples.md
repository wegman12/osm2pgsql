# Usage Examples

This document provides practical examples for common osm2pgsql-go use cases.

## Table of Contents

- [Basic Import](#basic-import)
- [Projection and Coordinate Systems](#projection-and-coordinate-systems)
- [Filtering Data](#filtering-data)
- [Incremental Updates](#incremental-updates)
- [Tile Rendering Setup](#tile-rendering-setup)
- [Performance Optimization](#performance-optimization)
- [Replication Setup](#replication-setup)

## Basic Import

### Minimal Import

```bash
# Import with default settings
osm2pgsql-go import -d osm monaco-latest.osm.pbf
```

### Specify Database Connection

```bash
# Full connection details
osm2pgsql-go import \
  --db-host localhost \
  --db-port 5432 \
  --db-name gis \
  --db-user postgres \
  --db-password secret \
  monaco-latest.osm.pbf
```

### Using Environment Variables

```bash
# Set database password via environment
export PGPASSWORD=secret

osm2pgsql-go import -d gis -U postgres monaco-latest.osm.pbf
```

### Import to Specific Schema

```bash
# Import to 'osm' schema
osm2pgsql-go import -d gis --db-schema osm monaco-latest.osm.pbf
```

## Projection and Coordinate Systems

### WGS84 (EPSG:4326)

Default projection, uses latitude/longitude coordinates:

```bash
osm2pgsql-go import -d gis -E 4326 monaco-latest.osm.pbf
```

### Web Mercator (EPSG:3857)

Required for tile rendering with most map servers:

```bash
osm2pgsql-go import -d gis -E 3857 monaco-latest.osm.pbf
```

## Filtering Data

### Bounding Box Filter

Import only data within a geographic area:

```bash
# Monaco area
osm2pgsql-go import -d gis \
  --bbox "7.4,43.72,7.44,43.75" \
  europe-latest.osm.pbf

# Central London
osm2pgsql-go import -d gis \
  --bbox "-0.15,51.5,-0.05,51.52" \
  great-britain-latest.osm.pbf
```

### Using a Style File

Filter and transform tags with YAML style:

```yaml
# style.yaml
tables:
  roads:
    columns:
      - name: name
        type: text
      - name: highway
        type: text
    where: "highway IS NOT NULL"
```

```bash
osm2pgsql-go import -d gis -S style.yaml monaco-latest.osm.pbf
```

### Using Lua Flex

Full control with Lua scripts:

```lua
-- roads-only.lua
local roads = osm2pgsql.define_table({
    name = 'roads',
    ids = { type = 'way' },
    columns = {
        { column = 'name', type = 'text' },
        { column = 'highway', type = 'text' },
        { column = 'geom', type = 'linestring' },
    }
})

function osm2pgsql.process_way(object)
    -- Only import major roads
    local major = {
        motorway = true,
        trunk = true,
        primary = true,
        secondary = true,
    }

    if major[object.tags.highway] then
        roads:insert({
            name = object.tags.name,
            highway = object.tags.highway,
            geom = object:as_linestring()
        })
    end
end
```

```bash
osm2pgsql-go import -d gis -S roads-only.lua monaco-latest.osm.pbf
```

## Incremental Updates

### Initial Import with Slim Mode

Store raw OSM data for future updates:

```bash
osm2pgsql-go import -d gis --slim monaco-latest.osm.pbf
```

### Apply Updates Manually

```bash
# Download change file
wget https://download.geofabrik.de/europe/monaco-updates/000/001/234.osc.gz

# Apply changes
osm2pgsql-go import -d gis --append 234.osc.gz
```

### Drop Slim Tables After Import

Save space if updates aren't needed:

```bash
osm2pgsql-go import -d gis --slim --drop monaco-latest.osm.pbf
```

## Tile Rendering Setup

### For OpenStreetMap Carto

```bash
# Import with Web Mercator projection
osm2pgsql-go import \
  -d gis \
  -E 3857 \
  -S examples/openstreetmap-carto.lua \
  --hstore \
  region.osm.pbf
```

### With Tile Expiry

Generate list of tiles that need re-rendering after updates:

```bash
# Initial import
osm2pgsql-go import \
  -d gis \
  -E 3857 \
  --slim \
  region.osm.pbf

# Apply update with tile expiry
osm2pgsql-go import \
  -d gis \
  --append \
  --expire-output /var/cache/tiles/dirty.list \
  --expire-min-zoom 10 \
  --expire-max-zoom 18 \
  changes.osc.gz
```

### Expire Tiles After Update

```bash
# Using render_expired (mod_tile)
cat /var/cache/tiles/dirty.list | \
  render_expired --map=default --num-threads=4
```

## Performance Optimization

### Parallel Processing

Use more workers for faster processing:

```bash
# Use 16 workers
osm2pgsql-go import -d gis -j 16 planet-latest.osm.pbf
```

### Flat Nodes File

Use memory-mapped file for node coordinates (recommended for large imports):

```bash
# Create flat nodes file
osm2pgsql-go import \
  -d gis \
  --flat-nodes /fast-ssd/nodes.cache \
  planet-latest.osm.pbf
```

### Tablespaces

Spread I/O across multiple disks:

```bash
# Create tablespaces first
psql -d gis -c "CREATE TABLESPACE fast_data LOCATION '/ssd1/pg_data';"
psql -d gis -c "CREATE TABLESPACE fast_index LOCATION '/ssd2/pg_index';"

# Import with tablespaces
osm2pgsql-go import \
  -d gis \
  --tablespace-main fast_data \
  --tablespace-index fast_index \
  planet-latest.osm.pbf
```

### PostgreSQL Tuning

Optimize PostgreSQL for bulk loading:

```sql
-- Before import (as superuser)
ALTER SYSTEM SET maintenance_work_mem = '4GB';
ALTER SYSTEM SET max_wal_size = '10GB';
ALTER SYSTEM SET checkpoint_completion_target = 0.9;
ALTER SYSTEM SET wal_level = minimal;
ALTER SYSTEM SET max_wal_senders = 0;
SELECT pg_reload_conf();

-- After import, restore defaults
ALTER SYSTEM RESET maintenance_work_mem;
ALTER SYSTEM RESET max_wal_size;
ALTER SYSTEM RESET wal_level;
ALTER SYSTEM RESET max_wal_senders;
SELECT pg_reload_conf();
```

### Disable Indexes During Import

```bash
# Import without creating indexes
osm2pgsql-go import -d gis --create-indexes=false planet-latest.osm.pbf

# Create indexes manually afterward
psql -d gis -f create-indexes.sql
```

## Replication Setup

### Initialize Replication

```bash
# Initialize from Geofabrik
osm2pgsql-go replication init \
  -d gis \
  --server https://download.geofabrik.de/europe/monaco-updates

# Or from planet.openstreetmap.org
osm2pgsql-go replication init \
  -d gis \
  --server https://planet.openstreetmap.org/replication/day
```

### Check Status

```bash
osm2pgsql-go replication status -d gis
```

Output:
```
Replication Status:
  Server: https://download.geofabrik.de/europe/monaco-updates
  Current sequence: 1234
  Last update: 2024-01-15 10:30:00 UTC
  Server sequence: 1240
  Behind by: 6 updates
```

### Single Update

```bash
osm2pgsql-go replication update -d gis
```

### Continuous Updates

```bash
# Update every minute
osm2pgsql-go replication start -d gis --interval 1m

# Update every hour with tile expiry
osm2pgsql-go replication start \
  -d gis \
  --interval 1h \
  --expire-output /var/cache/tiles/dirty.list
```

### Available Replication Sources

```bash
osm2pgsql-go replication list-sources
```

Output:
```
Available replication sources:

Planet (OpenStreetMap.org):
  Minutely: https://planet.openstreetmap.org/replication/minute
  Hourly:   https://planet.openstreetmap.org/replication/hour
  Daily:    https://planet.openstreetmap.org/replication/day

Geofabrik (Regional extracts):
  https://download.geofabrik.de/{region}-updates
  Examples:
    europe-updates
    europe/germany-updates
    europe/monaco-updates
```

## Complete Workflow Examples

### Tile Server Setup

```bash
#!/bin/bash
# setup-tile-server.sh

DB_NAME=gis
REGION=europe/monaco

# Create database
createdb $DB_NAME
psql -d $DB_NAME -c "CREATE EXTENSION postgis; CREATE EXTENSION hstore;"

# Download data
wget https://download.geofabrik.de/$REGION-latest.osm.pbf

# Import with slim mode for updates
osm2pgsql-go import \
  -d $DB_NAME \
  -E 3857 \
  -S openstreetmap-carto.lua \
  --slim \
  --hstore \
  --flat-nodes /var/cache/osm/nodes.cache \
  -j $(nproc) \
  $REGION-latest.osm.pbf

# Initialize replication
osm2pgsql-go replication init \
  -d $DB_NAME \
  --server https://download.geofabrik.de/$REGION-updates

# Start background updates
osm2pgsql-go replication start \
  -d $DB_NAME \
  --interval 1h \
  --expire-output /var/cache/tiles/dirty.list &
```

### Data Analysis Setup

```lua
-- analysis.lua - Extract data for analysis

local poi_counts = osm2pgsql.define_table({
    name = 'poi_analysis',
    ids = { type = 'node' },
    columns = {
        { column = 'category', type = 'text' },
        { column = 'subcategory', type = 'text' },
        { column = 'name', type = 'text' },
        { column = 'city', type = 'text' },
        { column = 'geom', type = 'point' },
    }
})

function osm2pgsql.process_node(object)
    local tags = object.tags

    -- Categorize POIs
    local category, subcategory

    if tags.amenity then
        category = 'amenity'
        subcategory = tags.amenity
    elseif tags.shop then
        category = 'shop'
        subcategory = tags.shop
    elseif tags.tourism then
        category = 'tourism'
        subcategory = tags.tourism
    elseif tags.leisure then
        category = 'leisure'
        subcategory = tags.leisure
    else
        return
    end

    poi_counts:insert({
        category = category,
        subcategory = subcategory,
        name = get_name(tags),
        city = tags["addr:city"],
        geom = object:as_point()
    })
end
```

```bash
# Import for analysis
osm2pgsql-go import -d analysis -S analysis.lua region.osm.pbf

# Query results
psql -d analysis -c "
  SELECT category, subcategory, COUNT(*) as count
  FROM poi_analysis
  GROUP BY category, subcategory
  ORDER BY count DESC
  LIMIT 20;
"
```
