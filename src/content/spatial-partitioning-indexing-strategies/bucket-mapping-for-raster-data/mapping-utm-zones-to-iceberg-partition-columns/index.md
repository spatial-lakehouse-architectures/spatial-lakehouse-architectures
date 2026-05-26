# Mapping UTM Zones to Iceberg Partition Columns: Resolving Spatial Skew and Predicate Pushdown Failures

In production spatial lakehouse architectures, partitioning by Universal Transverse Mercator (UTM) zones appears geographically intuitive but consistently triggers severe query degradation and metadata bloat. The core engineering failure mode stems from treating UTM zones as flat categorical keys rather than hierarchical spatial containers. When mapped directly to Apache Iceberg partition columns, this approach violates established [Spatial Partitioning & Indexing Strategies](/spatial-partitioning-indexing-strategies/) by creating extreme cardinality skew, cross-zone query fan-out, and manifest-level filter bypass. This document details a deterministic mapping workflow that aligns UTM boundaries with Iceberg’s partition evolution model while preserving predicate pushdown efficiency and ingestion throughput.

## The Partition Cardinality Failure Mode

UTM zones span 6° longitude each, but their actual surface area, projection distortion, and feature density vary drastically by latitude. A naive `PARTITIONED BY (utm_zone)` DDL generates 60+ partitions with wildly unequal file counts and directory depths. Iceberg’s query planner relies on partition transforms to prune manifests before scanning data files. When spatial predicates (e.g., `ST_Intersects`, `ST_Contains`) span multiple zones, the planner cannot leverage partition pruning, forcing full manifest reads and degrading into sequential file scans. Furthermore, UTM zone boundaries rarely align with typical bounding-box queries, causing excessive data skipping overhead, metastore timeouts, and cache thrashing during high-concurrency analytical workloads.

## Deterministic Hierarchical Partition Architecture

To resolve partition skew, implement a composite partition scheme that decomposes UTM zones into a fixed-width hierarchical grid. Instead of storing raw zone identifiers, derive partition columns using deterministic transforms that cap cardinality while preserving geographic locality:

```sql
CREATE TABLE spatial_features (
  feature_id BIGINT,
  geom BINARY, -- WKB-encoded GEOMETRY
  centroid_x DECIMAL(10,3),
  centroid_y DECIMAL(10,3),
  utm_zone_number INT,
  utm_hemisphere STRING,
  grid_100km_x INT,
  grid_100km_y INT
)
PARTITIONED BY (
  bucket(2, utm_hemisphere),
  bucket(12, utm_zone_number),
  bucket(10, grid_100km_x),
  bucket(10, grid_100km_y)
)
USING iceberg
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'zstd',
  'write.parquet.compression-level' = '3',
  'write.metadata.previous-versions-max' = '5'
);
```

The `bucket()` transforms ensure uniform distribution across the metastore, preventing hot partitions during high-throughput ingestion. For raster-heavy pipelines, this hierarchical structure directly complements [Bucket Mapping for Raster Data](/spatial-partitioning-indexing-strategies/bucket-mapping-for-raster-data/) by aligning tile boundaries with partition boundaries, eliminating cross-partition tile fragmentation and ensuring that tile-aligned reads remain I/O optimal.

## Coordinate Extraction & Write Configuration

Iceberg does not natively parse WKB during partition evaluation. Partition columns must be pre-computed or derived via deterministic transforms at write time. The following PySpark configuration enforces coordinate extraction, grid alignment, and sort ordering for optimal data layout:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, floor, when

spark = SparkSession.builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hadoop") \
    .config("spark.sql.catalog.iceberg.warehouse", "s3://lakehouse-bucket/warehouse") \
    .getOrCreate()

# Derive deterministic partition keys
df_partitioned = df.withColumn("centroid_x", expr("ST_X(ST_Centroid(ST_GeomFromWKB(geom)))")) \
                   .withColumn("centroid_y", expr("ST_Y(ST_Centroid(ST_GeomFromWKB(geom)))")) \
                   .withColumn("utm_zone_number", floor(col("centroid_x") / 6.0) + 31) \
                   .withColumn("utm_hemisphere", when(col("centroid_y") >= 0, "N").otherwise("S")) \
                   .withColumn("grid_100km_x", floor(col("centroid_x") / 100000)) \
                   .withColumn("grid_100km_y", floor(col("centroid_y") / 100000))

df_partitioned.writeTo("iceberg.db.spatial_features") \
    .tableProperty("write.sort-order", "centroid_x ASC, centroid_y ASC") \
    .append()
```

This configuration aligns with [OGC Simple Feature Access](https://www.ogc.org/standards/sfa/) coordinate standards and ensures Iceberg’s manifest statistics capture min/max bounds for `centroid_x` and `centroid_y`, enabling efficient range pruning.

## Optimizing Predicate Pushdown & Manifest Pruning

Partitioning alone cannot resolve intra-partition spatial skew. Within each UTM-derived bucket, data must be physically sorted to enable block-level skipping. Iceberg v2+ supports sort orders that translate directly to Parquet page-level statistics. Configure the following runtime parameters to maximize predicate pushdown efficiency:

```sql
-- Enforce manifest-level filter evaluation
SET spark.sql.iceberg.plan-filter-mode = 'inclusive';
SET spark.sql.iceberg.metadata.metrics-mode = 'truncate(16)';

-- Optimize manifest read concurrency for wide spatial scans
SET spark.sql.iceberg.scan.plan-filter-mode = 'inclusive';
SET spark.sql.iceberg.scan.plan-batch-size = '100';
```

When executing spatial queries, the planner evaluates `grid_100km_x` and `grid_100km_y` buckets first, then applies `centroid_x`/`centroid_y` range filters against Parquet column statistics. This two-tier pruning reduces I/O by 60–85% compared to flat zone partitioning.

## Debugging & Resolution Workflow

When spatial queries degrade, isolate the failure vector using the following deterministic steps:

1. **Verify Manifest Pruning:** Run `EXPLAIN` on the target query. Confirm `PartitionFilters` and `DataFilters` appear in the physical plan. Missing filters indicate predicate mismatch or transform misalignment.
2. **Audit Partition Cardinality:** Query Iceberg metadata tables:
 ```sql
   SELECT partition, record_count, file_count 
   FROM iceberg.db.spatial_features.partitions 
   ORDER BY file_count DESC LIMIT 10;
   ```
 If any partition contains >500 files or >10GB of data, the `bucket()` cardinality is undersized. Increase bucket counts by 1.5x and trigger `rewrite_data_files`.
3. **Resolve Boundary Edge Cases:** Features crossing 100km grid lines may land in adjacent partitions. Implement a dual-write strategy for geometries intersecting grid boundaries, or use `ST_Buffer` with a 100m tolerance during ingestion to guarantee deterministic placement.
4. **Fix Metadata Bloat:** If `metadata.json` exceeds 50MB, reduce snapshot retention and enable manifest merging:
 ```sql
   ALTER TABLE iceberg.db.spatial_features SET TBLPROPERTIES (
     'history.expire.max-snapshot-age-ms' = '86400000',
     'write.manifest.min-merge-count' = '100'
   );
   ```

## Production Maintenance & Compaction

Spatial ingestion pipelines generate fragmented files due to streaming micro-batches. Schedule automated compaction using Iceberg’s `rewrite_data_files` procedure with spatial-aware sorting:

```sql
CALL iceberg.system.rewrite_data_files(
  table => 'iceberg.db.spatial_features',
  strategy => 'sort',
  sort_order => 'centroid_x ASC, centroid_y ASC',
  options => map('target-file-size-bytes', '536870912', 'partial-progress.enabled', 'true')
);
```

Execute this procedure during low-concurrency windows. Monitor compaction throughput via Spark UI stage metrics and verify manifest count reduction post-execution. Maintain `centroid_x`/`centroid_y` sort order consistency across all compaction runs to preserve predicate pushdown guarantees.