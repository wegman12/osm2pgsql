# osm2pgsql-go Development Status

## Goal
Build a high-performance OSM to PostgreSQL importer to replace the slow original osm2pgsql tool.
- Target: Process 84GB planet file in under 1 hour
- Machine: 64GB RAM, 6 cores (12 threads), PostgreSQL in Docker on NVME

## Architecture
Two-pass mmap-based approach:
1. **Pass 1**: Stream nodes into memory-mapped index file (O(1) coordinate lookups)
2. **Pass 2**: Stream ways/relations, lookup coords from mmap, build WKT geometries, write to Parquet
3. **Load**: Parallel bulk load Parquet files into PostgreSQL using COPY protocol

## Completed Tasks

### 1. Core Implementation
- [x] Cobra CLI structure with subcommands (import, extract, transform, load)
- [x] Mmap-based node index (`internal/nodeindex/mmap.go`)
- [x] PBF extractor with two-pass geometry building (`internal/pbf/extractor.go`)
- [x] Parquet writer for geometries (`internal/parquet/`)
- [x] PostgreSQL loader with COPY protocol (`internal/loader/loader.go`)

### 2. Performance Optimizations
- [x] PostgreSQL COPY protocol for bulk loading
- [x] Parallel table loading (all 3 tables load concurrently)
- [x] Parallel index creation (GIST and B-tree indexes created in parallel)
- [x] String building optimization (strings.Builder instead of fmt.Sprintf)
- [x] PostgreSQL tuning config created at `/home/kevin/services/geoserver/postgresql.conf`

### 3. Logging
- [x] Added zap structured logging (`internal/logger/logger.go`)
- [x] Updated all cmd/*.go files to use logger
- [x] Updated internal/pbf/extractor.go to use logger
- [x] Updated internal/loader/loader.go to use logger
- [x] Updated internal/transform/transformer.go to use logger

## Baseline Performance (Netherlands 1.3GB)
- **Total time**: 13 minutes 12 seconds
- **Extract**: 5m8s (38.9%) - 135M nodes, 18M ways, 209K relations
- **Load**: 8m4s (61.1%) - 26M rows loaded
- **Projected planet time**: ~14 hours (need to get under 1 hour)

## Current State

### Just Completed
- Zap logging integration across all files
- Build compiles successfully

### Blocked On
- PostgreSQL container has permission issue with custom config file
- Container is running but falling back to default config
- Need to restart shell with docker group permissions

### PostgreSQL Config Issue
The custom config at `/home/kevin/services/geoserver/postgresql.conf` has permission denied error.
Config file permissions are 644 (should be readable), but the postgres container user may not have access.

To fix after shell restart:
```bash
# Check container service name
docker compose -f /home/kevin/services/geoserver/compose.yaml ps

# Restart postgres container
docker compose -f /home/kevin/services/geoserver/compose.yaml restart <service-name>

# Verify connection
PGPASSWORD=postgres psql -h localhost -p 5412 -U kevin -d geoserver_db -c "SELECT version();"
```

## Next Steps

### Immediate
1. Fix PostgreSQL container permissions and restart
2. Test parallel loading with new zap logging
3. Measure performance improvement from parallel loading

### Future Optimizations
1. Add multipolygon relation handling (currently skipped)
2. Consider WKB instead of WKT for geometry transfer
3. Profile and optimize bottlenecks
4. Test with larger regions (Germany, Europe, Planet)

## Key Files
- `cmd/import.go` - Main import pipeline
- `internal/pbf/extractor.go` - Two-pass PBF extraction
- `internal/loader/loader.go` - Parallel PostgreSQL loading
- `internal/nodeindex/mmap.go` - Memory-mapped node coordinate index
- `internal/logger/logger.go` - Zap logging wrapper
- `/home/kevin/services/geoserver/postgresql.conf` - PostgreSQL tuning
- `/home/kevin/services/geoserver/compose.yaml` - Docker compose config

## Test Command
```bash
./osm2pgsql-go import --drop-existing -v testdata/netherlands.osm.pbf
```
