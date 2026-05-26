# Optimizing Spatial Joins with Iceberg Z-Ordering

In production lakehouse architectures, spatial join failures rarely stem from raw compute exhaustion. They originate from cross-partition shuffle skew. When joining high-cardinality vector layers (cadastral parcels, sensor footprints, road networks) against time-series telemetry or raster tilesets, standard Iceberg partitioning by ingestion timestamp or administrative region fails to localize spatial predicates. The query planner defaults to broadcast or sort-merge joins that trigger full table scans, materializing intermediate datasets that exceed executor memory limits, saturate network I/O, and breach SLAs. The engineering objective is deterministic: enforce spatial locality at the file level using multi-dimensional Z-ordering to enable aggressive predicate pruning and eliminate unnecessary shuffle.

## The Partition Blindness Failure Mode

Traditional [Spatial Partitioning & Indexing Strategies](/spatial-partitioning-indexing-strategies/) rely on hierarchical grids, temporal buckets, or categorical boundaries that rarely align with runtime spatial query patterns. When a join executes `ST_Intersects(a.geometry, b.geometry)`, the optimizer cannot map bounding box overlap to physical file boundaries if partitioning is purely temporal or categorical. Iceberg’s metadata layer tracks column-level min/max statistics per data file, but native geometry types lack scalar bounds. Without spatial clustering, every join degenerates into a Cartesian product across partitions, forcing the execution engine to deserialize and evaluate geometries that fall entirely outside the target region. This partition blindness manifests as sustained high CPU utilization on worker nodes, manifest file bloat, and unpredictable query latency.

## Configuring Z-Order as a Sort Primitive

Z-ordering maps multi-dimensional spatial coordinates into a single scalar value by interleaving the binary representations of X and Y dimensions. In Iceberg, this is applied during write-time clustering or scheduled compaction. By materializing a Z-value column (e.g., `z_geom`) derived from the centroid or bounding box of each geometry record, spatial locality is transformed into a linear sort key. The [Z-Ordering for Geospatial Queries](/spatial-partitioning-indexing-strategies/z-ordering-for-geospatial-queries/) methodology ensures that spatially proximate features land in the same Parquet row groups, enabling the query planner to prune files using simple range predicates on the Z-column instead of evaluating complex spatial functions at runtime.

### Step 1: Compute Deterministic Z-Values
Iceberg does not natively compute spatial hashes during ingestion. You must derive the Z-value explicitly before writing. Use a deterministic bit-interleaving UDF or leverage Apache Sedona’s `ST_ZOrder` for reproducibility:

```sql
-- Compute Z-value from centroid coordinates (Spark SQL)
CREATE OR REPLACE TEMPORARY VIEW spatial_prepped AS
SELECT 
  id,
  geometry,
  -- Bit-interleave X and Y centroids (normalized to 32-bit unsigned)
  zorder(
    cast((st_x(st_centroid(geometry)) + 180.0) * 1e6 AS bigint),
    cast((st_y(st_centroid(geometry)) + 90.0) * 1e6 AS bigint)
  ) AS z_geom
FROM raw_vector_feed;
```

### Step 2: Register Sort Order in Iceberg Metadata
Define the Z-column as the primary sort key. Iceberg uses this metadata to guide file layout during writes and compaction.

```sql
ALTER TABLE prod.spatial_assets SET TBLPROPERTIES (
  'write.sort-order' = 'z_geom ASC',
  'write.sort-order.z_geom.null-order' = 'nulls-last'
);
```

For Spark 3.3+ with Iceberg 1.3+, you can also enforce sort order at the DataFrame level:
```python
df.writeTo("prod.spatial_assets") \
  .sortWithinPartitions("z_geom") \
  .append()
```

## Production Compaction & Metadata Alignment

Z-ordering degrades as data accumulates. Without scheduled compaction, file boundaries diverge from the sort key, reintroducing shuffle skew. Implement a daily compaction job that rewrites small files and re-clusters Z-values:

```sql
CALL catalog.system.rewrite_data_files(
  'prod.spatial_assets',
  strategy => 'sort',
  sort_order => 'z_geom ASC',
  options => map('min-input-files', '10', 'target-file-size-bytes', '1073741824')
);
```

**Critical Parameters:**
- `target-file-size-bytes`: Align with your object storage optimal read block size (typically `1GB` for S3/GCS).
- `spark.sql.adaptive.enabled=true`: Enables Adaptive Query Execution (AQE) to dynamically coalesce skewed partitions post-shuffle.
- `spark.sql.adaptive.skewJoin.enabled=true`: Splits skewed partitions into smaller tasks.
- `iceberg.metadata.metrics.column.z_geom`: Ensure min/max statistics are tracked for the Z-column. Iceberg tracks this by default, but verify via `DESCRIBE EXTENDED prod.spatial_assets`.

## Debugging Predicate Pruning & Resolving Skew

### Failure Mode 1: Full Table Scan Despite Z-Order
**Symptom:** `EXPLAIN` shows `FileScan parquet` with `PartitionFilters: []` and `DataFilters: []` for `z_geom`.
**Root Cause:** Missing sort order metadata or query predicate does not reference the Z-column.
**Resolution:** 
1. Verify sort order registration: `SHOW TBLPROPERTIES prod.spatial_assets ('write.sort-order')`
2. Rewrite query to explicitly filter on Z-range before spatial evaluation:
```sql
WITH pruned AS (
  SELECT * FROM prod.spatial_assets 
  WHERE z_geom BETWEEN zorder(min_x, min_y) AND zorder(max_x, max_y)
)
SELECT * FROM pruned a 
JOIN telemetry b ON ST_Intersects(a.geometry, b.footprint);
```

### Failure Mode 2: Executor OOM on Join Stage
**Symptom:** `java.lang.OutOfMemoryError: Java heap space` during `SortMergeJoin` or `BroadcastHashJoin`.
**Root Cause:** Z-order clustering is misaligned with join keys, or broadcast threshold is exceeded.
**Resolution:**
1. Disable broadcast for large spatial tables: `spark.sql.autoBroadcastJoinThreshold=-1`
2. Increase shuffle partitions to match data skew: `spark.sql.shuffle.partitions=400`
3. Enable AQE skew handling and set partition size target:
```properties
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.sql.adaptive.skewJoin.enabled=true
spark.sql.adaptive.advisoryPartitionSizeInBytes=134217728
```
4. Validate row group alignment using Parquet metadata inspection:
```bash
parquet-tools meta s3://bucket/path/to/file.parquet | grep -A 5 "z_geom"
```
Ensure `min` and `max` values are tightly bounded per row group. Wide ranges indicate poor clustering.

### Failure Mode 3: Manifest File Bloat & Metadata Latency
**Symptom:** `iceberg.metadata.refresh-interval-ms` triggers frequent catalog calls; query planning exceeds 30s.
**Root Cause:** High write frequency without compaction creates thousands of small manifests.
**Resolution:** 
- Run `CALL catalog.system.expire_snapshots('prod.spatial_assets', older_than => TIMESTAMP '2024-01-01 00:00:00')`
- Schedule `rewrite_data_files` to target `max-concurrent-file-group-rewrites=5`
- Set `iceberg.metadata.metrics.column.z_geom` to `true` (default) and verify via `iceberg.metadata.refresh-interval-ms=60000`

## Production Checklist
- [ ] Z-column computed deterministically from normalized coordinates
- [ ] `write.sort-order` registered in Iceberg table properties
- [ ] Compaction job scheduled daily with `strategy => 'sort'`
- [ ] AQE and skew join handling enabled in Spark config
- [ ] Query predicates explicitly reference `z_geom` range before `ST_Intersects`
- [ ] Manifest count monitored; threshold alert set at `>5000` per snapshot

For authoritative Iceberg configuration references, consult the official [Apache Iceberg Sort Order documentation](https://iceberg.apache.org/docs/latest/spark-configuration/#sort-order) and [Spark AQE performance tuning guidelines](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution).