# osm2pgsql-go Development Status

## Goal
Build a high-performance OSM to PostgreSQL importer to replace the slow original osm2pgsql tool.
- Target: Process 84GB planet file in under 1 hour
- Machine: 64GB RAM, 6 cores (12 threads), PostgreSQL in Docker on NVME

## Current Performance

### Netherlands 1.3GB Benchmark
| Metric | Baseline | Sequential | Pipelined | Improvement |
|--------|----------|------------|-----------|-------------|
| Total Time | 13m 12s | 8m 41s | **6m 31s** | **50% faster** |
| Pass 1 | - | 25s | 25s | Same |
| Pass 2 + Load | - | 8m 4s | 6m 6s | 24% faster |
| Index Creation | - | ~1m 30s | 24s | 62% faster |
| Throughput | ~2.7 MB/s | 6.2 MB/s | 3.3 MB/s | - |

### Breakdown (Pipelined)
- **Pass 1** (node indexing): 25s for 135M nodes
- **Pass 2** (geometry building + streaming): 1m 25s for 18M ways, 209K relations
- **Loading** (overlapped with Pass 2):
  - Lines: 2.2M rows in 46s
  - Points: 12M rows in 3m 23s (bottleneck)
  - Polygons: 11.9M rows in 3m 15s
- **Index creation**: 24s (GIST + B-tree, parallel)

### Bottleneck Analysis
PostgreSQL loading is now the primary bottleneck. Extraction completes ~4 minutes before loading finishes.
The ST_GeomFromWKB conversion runs per-row during INSERT and is CPU-bound on the PostgreSQL side.

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

## Next Steps

### High Priority - PostgreSQL Loading Optimization
The bottleneck is now PostgreSQL's ST_GeomFromWKB conversion. Options to investigate:

1. **Batch INSERT with unnest()** - Instead of row-by-row INSERT, use array-based batch insertion:
   ```sql
   INSERT INTO table SELECT unnest($1::bigint[]), unnest($2::bytea[])...
   ```

2. **Parallel COPY per table** - Split each table's data into chunks and load with multiple connections

3. **Direct EWKB insertion** - Since WKB already includes SRID, might be able to cast directly:
   ```sql
   INSERT INTO table (geom) VALUES ($1::geometry)
   ```
   (Requires testing if PostgreSQL accepts raw EWKB as geometry type)

4. **Partitioned loading** - Create multiple temp tables, INSERT in parallel, then combine

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
