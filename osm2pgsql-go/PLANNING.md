# osm2pgsql-go Development Plan

## Goal
Build a high-performance OSM to PostgreSQL importer as a faster alternative to osm2pgsql.
- **Target**: Process 84GB planet file in under 1 hour
- **Machine**: 64GB RAM, 6 cores (12 threads), PostgreSQL on NVME

---

## Current Status

### Performance Benchmarks

| Dataset | Size | Time | Throughput |
|---------|------|------|------------|
| Monaco | 657KB | ~1s | - |
| Netherlands | 1.3GB | 2m 39s | 8.2 MB/s |
| Planet | 84GB | ~3h 40m | ~6.5 MB/s |

### Implemented Features ✓

- [x] PBF input parsing (parallel via `paulmach/osm`)
- [x] Two-pass mmap-based node index (O(1) coordinate lookups)
- [x] Parallel way processing with worker goroutines
- [x] WKB/EWKB geometry encoding (SRID 4326)
- [x] PostgreSQL COPY protocol for bulk loading
- [x] Pipelined extraction + loading (overlapped phases)
- [x] Direct EWKB to PostGIS (no ST_GeomFromWKB overhead)
- [x] Parallel table loading (3 geometry tables concurrent)
- [x] Parallel index creation (GIST + B-tree)
- [x] UNLOGGED tables during load
- [x] System metrics logging (CPU, memory, disk I/O)
- [x] File logging with rotation
- [x] Configurable log verbosity

---

## Missing Features (Prioritized)

### Priority 1: Core Functionality Gaps

#### 1.1 Multipolygon Relations
**Status**: Relations are counted but not processed
**Impact**: Complex areas (lakes with islands, buildings with courtyards, forests with clearings) are missing

**Implementation**:
- Parse relation members (outer/inner roles)
- Collect way geometries for each member
- Assemble into MultiPolygon with holes using ring assembly algorithm
- Handle nested relations

**Files to modify**:
- `internal/pipeline/streaming_extractor.go` - Add relation processing
- `internal/wkb/encoder.go` - Add `EncodeMultiPolygon()` method

**Estimated complexity**: Medium-High

#### 1.2 Projection Support
**Status**: Only WGS84 (EPSG:4326) supported
**Impact**: Many tile servers expect Web Mercator (EPSG:3857)

**Implementation**:
- Add `--projection` / `-E` flag (4326, 3857, or custom EPSG)
- Integrate PROJ library or use pure-Go reprojection for common cases
- Transform coordinates before WKB encoding

**Files to modify**:
- `internal/config/config.go` - Add Projection field
- `cmd/root.go` - Add flag
- New: `internal/proj/transform.go` - Coordinate transformation
- `internal/wkb/encoder.go` - Apply transform before encoding

**Estimated complexity**: Medium

#### 1.3 Tag Filtering / Style Configuration
**Status**: All tags stored as JSONB, no filtering
**Impact**: Database larger than needed, no control over what gets imported

**Implementation options**:
1. **Simple**: Built-in presets (e.g., `--style roads`, `--style buildings`)
2. **Medium**: YAML/JSON config file for tag rules
3. **Full**: Lua scripting (like osm2pgsql flex output)

**Recommended approach**: Start with YAML config, add Lua later if needed

```yaml
# Example config
tables:
  roads:
    type: line
    filter:
      highway: ["primary", "secondary", "tertiary", "residential"]
    columns:
      - name: name
        type: text
      - name: highway
        type: text
      - name: oneway
        type: boolean
```

**Files to create**:
- `internal/style/config.go` - Style configuration parsing
- `internal/style/filter.go` - Tag filtering logic

**Estimated complexity**: Medium (YAML), High (Lua)

### Priority 2: Usability Improvements

#### 2.1 Progress Reporting Improvements
**Status**: Basic progress with ETA
**Impact**: Hard to monitor long imports

**Implementation**:
- Add rows/second counters per table
- Show channel buffer utilization (detect bottlenecks)
- Memory usage tracking
- Optional progress bar mode

**Files to modify**:
- `internal/pipeline/streaming_extractor.go`
- `internal/pipeline/streaming_loader.go`
- `internal/pipeline/progress.go`

**Estimated complexity**: Low

#### 2.2 Resume/Checkpoint Support
**Status**: No checkpointing
**Impact**: Failed imports must restart from scratch

**Implementation**:
- Save progress state periodically (node index position, last way ID)
- On restart, check for checkpoint and resume
- Verify data consistency before resuming

**Files to create**:
- `internal/checkpoint/checkpoint.go`

**Estimated complexity**: Medium

#### 2.3 Bounding Box Filter
**Status**: No geographic filtering
**Impact**: Must import entire file even for regional extracts

**Implementation**:
- Add `--bbox minlon,minlat,maxlon,maxlat` flag
- Filter nodes outside bbox during Pass 1
- Filter ways that have no nodes in bbox

**Files to modify**:
- `internal/config/config.go`
- `cmd/root.go`
- `internal/pipeline/streaming_extractor.go`

**Estimated complexity**: Low

### Priority 3: Advanced Features

#### 3.1 Incremental Updates (Slim Mode)
**Status**: Full import only
**Impact**: Can't apply daily/hourly OSM diffs

**Implementation**:
- Store raw OSM data in "middle" tables
- Parse OSC (change) files
- Track dependencies (way→nodes, relation→ways)
- Apply changes incrementally

**Files to create**:
- `internal/updates/` - Change file parsing and application
- `internal/middle/` - Raw data storage for updates

**Estimated complexity**: High

#### 3.2 Tile Expiry
**Status**: Not implemented
**Impact**: Can't integrate with tile rendering pipelines

**Implementation**:
- Track which tiles are affected by changes
- Output dirty tile list for tile servers
- Support multiple zoom levels

**Files to create**:
- `internal/expire/tiles.go`

**Estimated complexity**: Medium

#### 3.3 Additional Metadata Columns
**Status**: Only osm_id, osm_type, tags, geom
**Impact**: No access to changeset, timestamp, version, user

**Implementation**:
- Add `--extra-attributes` flag
- Extend table schema with optional columns
- Parse additional fields from PBF

**Files to modify**:
- `internal/pipeline/streaming_loader.go` - Extended schema
- `internal/pipeline/types.go` - Additional fields

**Estimated complexity**: Low

#### 3.4 Multiple Output Tables
**Status**: Fixed 3 tables (point, line, polygon)
**Impact**: Can't create thematic tables (roads, buildings, POIs separately)

**Implementation**:
- Allow style config to define multiple output tables
- Route geometries to appropriate tables based on tags
- Support table-specific schemas

**Depends on**: 1.3 (Tag Filtering / Style Configuration)

**Estimated complexity**: Medium

---

## Implementation Roadmap

### Phase 1: Core Improvements (Next)
1. **Multipolygon Relations** - Fill the biggest functionality gap
2. **Bounding Box Filter** - Quick win, useful for testing
3. **Progress Improvements** - Better visibility into bottlenecks

### Phase 2: Flexibility
4. **Projection Support** - Enable 3857 output
5. **Tag Filtering (YAML)** - Control what gets imported
6. **Extra Metadata Columns** - Optional timestamp/version

### Phase 3: Production Features
7. **Resume/Checkpoint** - Reliability for large imports
8. **Multiple Output Tables** - Thematic data organization

### Phase 4: Advanced (Future)
9. **Incremental Updates** - Major undertaking
10. **Tile Expiry** - Rendering pipeline integration
11. **Lua Scripting** - Full flexibility

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Import Pipeline                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────┐    ┌──────────────┐    ┌───────────────────────┐  │
│  │  PBF     │───▶│  Pass 1:     │───▶│  Mmap Node Index      │  │
│  │  Reader  │    │  Node Index  │    │  (80GB sparse file)   │  │
│  └──────────┘    └──────────────┘    └───────────────────────┘  │
│       │                                        │                 │
│       ▼                                        ▼                 │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Pass 2: Geometry Building (Parallel Workers)            │   │
│  │  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐            │   │
│  │  │Worker 1│ │Worker 2│ │Worker 3│ │Worker N│            │   │
│  │  │  WKB   │ │  WKB   │ │  WKB   │ │  WKB   │            │   │
│  │  └────┬───┘ └────┬───┘ └────┬───┘ └────┬───┘            │   │
│  └───────┼──────────┼──────────┼──────────┼─────────────────┘   │
│          │          │          │          │                      │
│          ▼          ▼          ▼          ▼                      │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │              Geometry Channels (buffered)                 │   │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐         │   │
│  │  │   Points    │ │    Lines    │ │  Polygons   │         │   │
│  │  └──────┬──────┘ └──────┬──────┘ └──────┬──────┘         │   │
│  └─────────┼───────────────┼───────────────┼─────────────────┘   │
│            │               │               │                     │
│            ▼               ▼               ▼                     │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │           PostgreSQL COPY Loaders (Parallel)              │   │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐         │   │
│  │  │ osm_point   │ │  osm_line   │ │ osm_polygon │         │   │
│  │  │   COPY      │ │    COPY     │ │    COPY     │         │   │
│  │  └─────────────┘ └─────────────┘ └─────────────┘         │   │
│  └──────────────────────────────────────────────────────────┘   │
│                              │                                   │
│                              ▼                                   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │           Index Creation (Parallel per table)             │   │
│  │           GIST (geom) + B-tree (osm_id)                   │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Key Files

| File | Purpose |
|------|---------|
| `cmd/import.go` | Main import command |
| `internal/pipeline/coordinator.go` | Orchestrates extraction + loading |
| `internal/pipeline/streaming_extractor.go` | Two-pass PBF processing |
| `internal/pipeline/streaming_loader.go` | PostgreSQL COPY loading |
| `internal/nodeindex/mmap.go` | Memory-mapped node coordinates |
| `internal/wkb/encoder.go` | EWKB geometry encoding |
| `internal/metrics/collector.go` | System metrics collection |
| `internal/logger/logger.go` | Logging with file output |

---

## Test Commands

```bash
# Monaco (quick test, 657KB)
./osm2pgsql-go import --db-host localhost --db-port 5412 \
  -U kevin -W 'PASSWORD' -d geoserver_db \
  --drop-existing testdata/monaco.osm.pbf

# Netherlands (benchmark, 1.3GB)
./osm2pgsql-go import --db-host localhost --db-port 5412 \
  -U kevin -W 'PASSWORD' -d geoserver_db \
  --drop-existing --log-file import.log --metrics-interval 10s \
  testdata/netherlands.osm.pbf

# With verbose logging
./osm2pgsql-go import -v --db-host localhost --db-port 5412 \
  -U kevin -W 'PASSWORD' -d geoserver_db \
  --drop-existing testdata/monaco.osm.pbf
```

---

## PostgreSQL Tuning

Recommended settings for import performance:

```conf
shared_buffers = 8GB
work_mem = 512MB
maintenance_work_mem = 4GB
max_wal_size = 10GB
checkpoint_completion_target = 0.9
synchronous_commit = off  # During import only
```
