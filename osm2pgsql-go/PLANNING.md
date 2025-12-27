# osm2pgsql-go Development Status

## Goal
Build a high-performance OSM to PostgreSQL importer to replace the slow original osm2pgsql tool.
- Target: Process 84GB planet file in under 1 hour
- Machine: 64GB RAM, 6 cores (12 threads), PostgreSQL in Docker on NVME

## Current Performance

### Netherlands 1.3GB Benchmark
| Metric | Baseline | Sequential | Pipelined+TempTable | Pipelined+DirectCOPY |
|--------|----------|------------|---------------------|----------------------|
| Total Time | 13m 12s | 8m 41s | 6m 31s | **2m 39s** |
| Pass 1 | - | 25s | 25s | 25s |
| Pass 2 + Load | - | 8m 4s | 6m 6s | 2m 16s |
| Index Creation | - | ~1m 30s | 24s | 23s |
| Throughput | ~2.7 MB/s | 6.2 MB/s | 3.3 MB/s | **8.2 MB/s** |
| vs Baseline | - | 34% faster | 50% faster | **80% faster** |

### Breakdown (Current)
- **Pass 1** (node indexing): 25s for 135M nodes
- **Pass 2** (geometry building + streaming): 1m 29s for 18M ways, 209K relations
- **Loading** (overlapped with Pass 2, direct COPY):
  - Lines: 2.2M rows in 5s
  - Points: 12M rows in 17s
  - Polygons: 11.9M rows in 23s
- **Index creation**: 23s (GIST + B-tree, parallel)

### Key Optimization
PostGIS accepts raw EWKB bytes directly via COPY - no temp table or ST_GeomFromWKB() needed!

## Architecture
Two-pass mmap-based approach with pipelined streaming:

1. **Pass 1**: Stream nodes into 80GB memory-mapped index file (O(1) coordinate lookups)
2. **Pass 2 + Load**: Parallel workers process ways/relations, lookup coords from mmap, build WKB geometries, stream directly to PostgreSQL via channels (no intermediate Parquet files)

The pipelined architecture starts loading points while ways are still being processed, overlapping extraction and loading phases for faster total import time.

## Completed Optimizations

- [x] Mmap-based node index for O(1) coordinate lookups
- [x] Parallel way/relation processing with worker goroutines
- [x] WKB (binary) geometry encoding instead of WKT (text)
- [x] PostgreSQL COPY protocol for bulk loading
- [x] Parallel table loading (all 3 geometry tables load concurrently)
- [x] Parallel index creation after data load
- [x] UNLOGGED tables during load, converted to LOGGED after
- [x] Fixed log formatting (switched to base zap.Logger with typed fields)
- [x] **Pipelined extraction and loading** - Extraction streams directly to loaders via channels, no intermediate files
- [x] **Direct EWKB COPY** - PostGIS accepts raw EWKB bytes directly in COPY, eliminating temp table and ST_GeomFromWKB overhead

## Next Steps

### Future Optimizations
1. Add multipolygon relation handling (currently skipped)
2. Profile memory usage during large imports
3. Test with larger regions (Germany, Europe, Planet)

## Key Files
- `cmd/import.go` - Main import pipeline (uses pipelined architecture)
- `internal/pipeline/` - Pipelined extraction and loading:
  - `coordinator.go` - Orchestrates parallel extraction and loading
  - `streaming_extractor.go` - Streams geometries to channels
  - `streaming_loader.go` - Loads from channels to PostgreSQL
  - `types.go` - Shared types (GeometryRecord, stats)
- `internal/pbf/extractor.go` - Two-pass PBF extraction (for standalone `extract` command)
- `internal/loader/loader.go` - Parquet-based loading (for standalone `load` command)
- `internal/nodeindex/mmap.go` - Memory-mapped node coordinate index
- `internal/wkb/encoder.go` - EWKB geometry encoder with SRID 4326
- `internal/logger/logger.go` - Zap logging wrapper

## Test Commands
```bash
# Monaco (quick test, 657KB)
./osm2pgsql-go import --db-host localhost --db-port 5412 -U kevin -W 'PASSWORD' -d geoserver_db --drop-existing testdata/monaco.osm.pbf

# Netherlands (benchmark, 1.3GB)
./osm2pgsql-go import --db-host localhost --db-port 5412 -U kevin -W 'PASSWORD' -d geoserver_db --drop-existing testdata/netherlands.osm.pbf
```

## PostgreSQL Config
Custom tuning at `/home/kevin/services/geoserver/postgresql.conf`:
- shared_buffers: 8GB
- work_mem: 512MB
- maintenance_work_mem: 4GB
- max_wal_size: 10GB
- checkpoint_completion_target: 0.9
