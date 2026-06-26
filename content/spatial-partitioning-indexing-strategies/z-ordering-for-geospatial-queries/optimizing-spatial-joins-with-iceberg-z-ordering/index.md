# Optimizing Spatial Joins with Iceberg Z-Ordering

In production lakehouse architectures, spatial join failures rarely stem from raw compute exhaustion. They originate from cross-partition shuffle skew. When joining high-cardinality vector layers (cadastral parcels, sensor footprints, road networks) against time-series telemetry or raster tilesets, standard Iceberg partitioning by ingestion timestamp or administrative region fails to localize spatial predicates. The query planner defaults to broadcast or sort-merge joins that trigger full table scans, materializing intermediate datasets that exceed executor memory limits, saturate network I/O, and breach SLAs. The engineering objective is deterministic: enforce spatial locality at the file level using multi-dimensional Z-ordering to enable aggressive predicate pruning and eliminate unnecessary shuffle.

## The Partition Blindness Failure Mode

Traditional [Spatial Partitioning & Indexing Strategies](/spatial-partitioning-indexing-strategies/) rely on hierarchical grids, temporal buckets, or categorical boundaries that rarely align with runtime spatial query patterns. When a join executes `ST_Intersects(a.geometry, b.geometry)`, the optimizer cannot map bounding box overlap to physical file boundaries if partitioning is purely temporal or categorical. Iceberg's metadata layer tracks column-level min/max statistics per data file, but geometry columns stored as raw `BINARY` (WKB) lack scalar bounds. Without spatial clustering on explicit bounding box columns, every join degenerates into a Cartesian product across partitions, forcing the execution engine to deserialize and evaluate geometries that fall entirely outside the target region.

## Configuring Z-Order as a Sort Primitive

Z-ordering maps multi-dimensional spatial coordinates into a single scalar sort key by interleaving the binary representations of X and Y dimensions. In Iceberg, this is applied via scheduled compaction. By materializing a scalar Z-value column (or equivalent bbox columns sorted together), spatial locality is transformed into a linear sort key. The [Z-Ordering for Geospatial Queries](/spatial-partitioning-indexing-strategies/z-ordering-for-geospatial-queries/) methodology ensures that spatially proximate features land in the same Parquet row groups, enabling the query planner to prune files using simple range predicates on the bounding box columns instead of evaluating complex spatial functions at runtime.

### Step 1: Compute Deterministic Bounding Box Columns

Extract min/max bounding box coordinates during ingestion. This is the primary vehicle for Z-order clustering in Iceberg:

```sql
-- Compute bbox columns from geometry (Spark SQL with Apache Sedona)
CREATE OR REPLACE TEMPORARY VIEW spatial_prepped AS
SELECT
  id,
  geometry,
  ST_XMin(ST_GeomFromWKB(geometry)) AS bbox_min_x,
  ST_YMin(ST_GeomFromWKB(geometry)) AS bbox_min_y,
  ST_XMax(ST_GeomFromWKB(geometry)) AS bbox_max_x,
  ST_YMax(ST_GeomFromWKB(geometry)) AS bbox_max_y
FROM raw_vector_feed;
```

### Step 2: Register Sort Order in Iceberg Metadata

Define the bbox columns as the primary sort key. Iceberg uses this metadata to guide file layout during writes and compaction.

```sql
ALTER TABLE prod.spatial_assets SET TBLPROPERTIES (
  'write.sort-order' = 'bbox_min_x ASC, bbox_min_y ASC, bbox_max_x ASC, bbox_max_y ASC'
);
```

For Spark with Iceberg 1.3+, you can also enforce sort order at the DataFrame level:
```python
df.sortWithinPartitions("bbox_min_x", "bbox_min_y", "bbox_max_x", "bbox_max_y") \
  .writeTo("prod.spatial_assets") \
  .append()
```

## Production Compaction & Metadata Alignment

Z-ordering degrades as data accumulates. Without scheduled compaction, file boundaries diverge from the sort key, reintroducing shuffle skew. Implement a daily compaction job that rewrites small files and re-clusters on bbox columns:

```sql
CALL catalog.system.rewrite_data_files(
  table => 'prod.spatial_assets',
  strategy => 'sort',
  sort_order => 'bbox_min_x ASC, bbox_min_y ASC, bbox_max_x ASC, bbox_max_y ASC',
  options => map('min-input-files', '10', 'target-file-size-bytes', '536870912')
);
```

**Critical Parameters:**
- `target-file-size-bytes`: Set to `536870912` (512MB) for S3/GCS optimal read block size.
- `spark.sql.adaptive.enabled=true`: Enables Adaptive Query Execution (AQE) to dynamically coalesce skewed partitions post-shuffle.
- `spark.sql.adaptive.skewJoin.enabled=true`: Splits skewed partitions into smaller tasks.
- Verify bbox column statistics are tracked: `DESCRIBE EXTENDED prod.spatial_assets` should show min/max values for `bbox_min_x`, `bbox_max_x`, etc.

## Debugging Predicate Pruning & Resolving Skew

### Failure Mode 1: Full Table Scan Despite Sort Order
**Symptom:** `EXPLAIN` shows `FileScan parquet` with `PartitionFilters: []` and `DataFilters: []` for bbox columns.
**Root Cause:** Missing sort order metadata or query predicate does not reference the bbox columns.
**Resolution:**
1. Verify sort order registration: `SHOW TBLPROPERTIES prod.spatial_assets ('write.sort-order')`
2. Rewrite query to explicitly filter on bbox range before spatial evaluation:
```sql
WITH pruned AS (
  SELECT * FROM prod.spatial_assets
  WHERE bbox_min_x >= -74.1 AND bbox_max_x <= -73.8
    AND bbox_min_y >= 40.6  AND bbox_max_y <= 40.9
)
SELECT * FROM pruned a
JOIN telemetry b ON ST_Intersects(
  ST_GeomFromWKB(a.geometry),
  ST_GeomFromWKB(b.footprint)
);
```

### Failure Mode 2: Executor OOM on Join Stage
**Symptom:** `java.lang.OutOfMemoryError: Java heap space` during `SortMergeJoin` or `BroadcastHashJoin`.
**Root Cause:** Z-order clustering is misaligned with join keys, or broadcast threshold is exceeded.
**Resolution:**
1. Disable broadcast for large spatial tables: `spark.sql.autoBroadcastJoinThreshold=-1`
2. Increase shuffle partitions to match data skew: `spark.sql.shuffle.partitions=400`
3. Enable AQE skew handling:
```properties
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.sql.adaptive.skewJoin.enabled=true
spark.sql.adaptive.advisoryPartitionSizeInBytes=134217728
```
4. Validate row group alignment using Parquet metadata inspection:
```bash
parquet-tools meta s3://bucket/path/to/file.parquet | grep -A 5 "bbox_min_x"
```
Ensure `min` and `max` values are tightly bounded per row group. Wide ranges indicate poor clustering.

### Failure Mode 3: Manifest File Bloat & Metadata Latency
**Symptom:** Query planning exceeds 30s; `table.refresh()` triggers frequent catalog calls.
**Root Cause:** High write frequency without compaction creates thousands of small manifests.
**Resolution:**
- Run `CALL catalog.system.expire_snapshots('prod.spatial_assets', older_than => TIMESTAMPADD(DAY, -30, CURRENT_TIMESTAMP))`
- Schedule `rewrite_data_files` to target `max-concurrent-file-group-rewrites=5`
- Enable `write.metadata.delete-after-commit.enabled=true` to limit manifest accumulation

## Production Checklist
- [ ] Bbox columns (`bbox_min_x`, `bbox_min_y`, `bbox_max_x`, `bbox_max_y`) materialized as `DOUBLE NOT NULL`
- [ ] `write.sort-order` registered in Iceberg table properties on bbox columns
- [ ] Compaction job scheduled daily with `strategy => 'sort'`
- [ ] AQE and skew join handling enabled in Spark config
- [ ] Query predicates explicitly reference bbox range before `ST_Intersects`
- [ ] Manifest count monitored; threshold alert set at >5,000 per snapshot

For authoritative Iceberg configuration references, consult the official [Apache Iceberg Sort Order documentation](https://iceberg.apache.org/docs/latest/spark-configuration/#sort-order) and [Spark AQE performance tuning guidelines](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution).
