# Mapping UTM Zones to Iceberg Partition Columns: Resolving Spatial Skew and Predicate Pushdown Failures

In production spatial lakehouse architectures, partitioning by Universal Transverse Mercator (UTM) zones appears geographically intuitive but consistently triggers severe query degradation and metadata bloat. The core engineering failure mode stems from treating UTM zones as flat categorical keys rather than hierarchical spatial containers. When mapped directly to Apache Iceberg partition columns, this approach violates established [Spatial Partitioning & Indexing Strategies](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/) by creating extreme cardinality skew, cross-zone query fan-out, and manifest-level filter bypass. This document details a deterministic mapping workflow that aligns UTM boundaries with Iceberg's partition evolution model while preserving predicate pushdown efficiency and ingestion throughput.

## The Partition Cardinality Failure Mode

UTM zones span 6° longitude each, but their actual surface area, projection distortion, and feature density vary drastically by latitude. A naive `PARTITIONED BY (utm_zone)` DDL generates 60 zones with wildly unequal file counts and directory depths. Iceberg's query planner relies on partition transforms to prune manifests before scanning data files. When spatial predicates (e.g., `ST_Intersects`, `ST_Contains`) span multiple zones, the planner cannot leverage partition pruning, forcing full manifest reads and degrading into sequential file scans. Furthermore, UTM zone boundaries rarely align with typical bounding-box queries, causing excessive data skipping overhead, metastore timeouts, and cache thrashing during high-concurrency analytical workloads.

## Deterministic Hierarchical Partition Architecture

To resolve partition skew, implement a composite partition scheme that decomposes UTM zones into a fixed-width hierarchical grid. Instead of storing raw zone identifiers, derive partition columns using deterministic `bucket()` transforms that cap cardinality while preserving geographic locality:

```sql
CREATE TABLE spatial_features (
  feature_id      BIGINT,
  geom            BINARY,           -- WKB-encoded geometry
  centroid_x      DOUBLE,           -- longitude in degrees (EPSG:4326)
  centroid_y      DOUBLE,           -- latitude in degrees (EPSG:4326)
  utm_zone_number INT,              -- 1–60
  utm_hemisphere  STRING,           -- 'N' or 'S'
  grid_1deg_x     INT,              -- floor(centroid_x) as integer degree
  grid_1deg_y     INT               -- floor(centroid_y) as integer degree
)
USING iceberg
PARTITIONED BY (
  bucket(2, utm_hemisphere),
  bucket(12, utm_zone_number),
  bucket(10, grid_1deg_x),
  bucket(10, grid_1deg_y)
)
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'zstd',
  'write.parquet.compression-level' = '3',
  'write.metadata.previous-versions-max' = '5'
);
```

**Note on bucket syntax:** Iceberg's `bucket(N, column)` transform takes the number of buckets as the first argument and the column name as the second. The `grid_1deg_x` / `grid_1deg_y` columns (integer degree cells) provide a ~111km grid that is coarser than raw easting/northing, preventing hot-partition explosions at fine resolutions.

The `bucket()` transforms ensure uniform distribution across the metastore. For raster-heavy pipelines, this hierarchical structure directly complements [Bucket Mapping for Raster Data](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/bucket-mapping-for-raster-data/) by aligning tile boundaries with partition boundaries.

## Coordinate Extraction & Write Configuration

Iceberg does not natively parse WKB during partition evaluation. Partition columns must be pre-computed at write time. The following PySpark configuration enforces coordinate extraction, grid alignment, and sort ordering for optimal data layout:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, floor, when, udf
from pyspark.sql.types import IntegerType, DoubleType
import shapely.wkb

spark = SparkSession.builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hadoop") \
    .config("spark.sql.catalog.iceberg.warehouse", "s3://lakehouse-bucket/warehouse") \
    .getOrCreate()

@udf(DoubleType())
def wkb_centroid_x(wkb_bytes: bytes) -> float:
    if not wkb_bytes:
        return None
    return shapely.wkb.loads(wkb_bytes).centroid.x

@udf(DoubleType())
def wkb_centroid_y(wkb_bytes: bytes) -> float:
    if not wkb_bytes:
        return None
    return shapely.wkb.loads(wkb_bytes).centroid.y

# Derive deterministic partition keys from WKB centroid in EPSG:4326
df_partitioned = df \
    .withColumn("centroid_x", wkb_centroid_x(col("geom"))) \
    .withColumn("centroid_y", wkb_centroid_y(col("geom"))) \
    .withColumn("utm_zone_number",
        floor(col("centroid_x") / 6.0).cast(IntegerType()) + 31) \
    .withColumn("utm_hemisphere",
        when(col("centroid_y") >= 0, "N").otherwise("S")) \
    .withColumn("grid_1deg_x", floor(col("centroid_x")).cast(IntegerType())) \
    .withColumn("grid_1deg_y", floor(col("centroid_y")).cast(IntegerType()))

df_partitioned.writeTo("iceberg.db.spatial_features") \
    .option("write.sort-order", "centroid_x ASC, centroid_y ASC") \
    .append()
```

This configuration aligns with [OGC Simple Feature Access](https://www.ogc.org/standard/simple-feature-access/) coordinate standards and ensures Iceberg's manifest statistics capture min/max bounds for `centroid_x` and `centroid_y`, enabling efficient range pruning.

## Optimizing Predicate Pushdown & Manifest Pruning

Partitioning alone cannot resolve intra-partition spatial skew. Within each UTM-derived bucket, data must be physically sorted to enable block-level skipping. Iceberg v2+ supports sort orders that translate directly to Parquet page-level statistics. Configure these runtime parameters to maximize predicate pushdown efficiency:

```sql
-- Force manifest-level filter evaluation (Iceberg Spark config)
SET spark.sql.catalog.iceberg.io-impl = org.apache.iceberg.aws.s3.S3FileIO;

-- Optimize manifest read concurrency for wide spatial scans
SET spark.sql.iceberg.scan.plan-batch-size = 100;
```

When executing spatial queries, the planner evaluates `grid_1deg_x` and `grid_1deg_y` buckets first, then applies `centroid_x`/`centroid_y` range filters against Parquet column statistics. This two-tier pruning reduces I/O by 60–85% compared to flat zone partitioning.

## Debugging & Resolution Workflow

When spatial queries degrade, isolate the failure vector using the following deterministic steps:

1. **Verify Manifest Pruning:** Run `EXPLAIN FORMATTED` on the target query. Confirm `PartitionFilters` and `DataFilters` appear in the physical plan. Missing filters indicate predicate mismatch or transform misalignment.
2. **Audit Partition Cardinality:** Query Iceberg metadata tables:
   ```sql
   SELECT partition, record_count, file_count
   FROM iceberg.db.spatial_features.partitions
   ORDER BY file_count DESC LIMIT 10;
   ```
   If any partition contains >500 files or >10GB of data, the `bucket()` cardinality is undersized. Increase bucket counts by 1.5x and trigger `rewrite_data_files`.
3. **Resolve Boundary Edge Cases:** Features crossing 1-degree grid lines may land in adjacent partitions. Implement a dual-write strategy for geometries intersecting grid boundaries, or accept a small amount of cross-partition fan-out as an acceptable trade-off.
4. **Fix Metadata Bloat:** If `metadata.json` exceeds 50MB, reduce snapshot retention and enable manifest merging:
   ```sql
   ALTER TABLE iceberg.db.spatial_features SET TBLPROPERTIES (
     'history.expire.max-snapshot-age-ms' = '86400000',
     'write.manifest.min-merge-count' = '100'
   );
   ```

## Production Maintenance & Compaction

Spatial ingestion pipelines generate fragmented files due to streaming micro-batches. Schedule automated compaction using Iceberg's `rewrite_data_files` procedure with spatial-aware sorting:

```sql
CALL iceberg.system.rewrite_data_files(
  table => 'iceberg.db.spatial_features',
  strategy => 'sort',
  sort_order => 'centroid_x ASC, centroid_y ASC',
  options => map('target-file-size-bytes', '536870912', 'partial-progress.enabled', 'true')
);
```

Execute this procedure during low-concurrency windows. Monitor compaction throughput via Spark UI stage metrics and verify manifest count reduction post-execution. Maintain `centroid_x`/`centroid_y` sort order consistency across all compaction runs to preserve predicate pushdown guarantees.

## Why UTM Zones Are an Awkward Partition Key

UTM is a projection family rather than a single system: sixty longitudinal zones, each with its own coordinate system, north and south variants, and coordinates in metres that repeat between zones. Using it as a partition key inherits every one of those properties.

<figure class="diagram">
<svg viewBox="0 0 692 256" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="UTM zone strips shown against a data extent that crosses three zones, illustrating that coordinates repeat across zones so a numeric range predicate matches unrelated data unless the zone is part of the key">
<rect x="0" y="0" width="692" height="256" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">The same easting value exists in every zone</text>
<rect x="80" y="62" width="120" height="120" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="1.5"/>
<rect x="200" y="62" width="120" height="120" fill="#e6f0ea" stroke="#2f6e49" stroke-width="1.5"/>
<rect x="320" y="62" width="120" height="120" fill="#f2e8da" stroke="#9a5a17" stroke-width="1.5"/>
<rect x="440" y="62" width="120" height="120" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="1.5"/>
<rect x="560" y="62" width="120" height="120" fill="#e6f0ea" stroke="#2f6e49" stroke-width="1.5"/>
<text x="140" y="110" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">zone 31</text>
<text x="260" y="110" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">zone 32</text>
<text x="380" y="110" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">zone 33</text>
<text x="500" y="110" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">zone 34</text>
<text x="620" y="110" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">zone 35</text>
<text x="140" y="146" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">E 500 000</text>
<text x="260" y="146" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">E 500 000</text>
<text x="380" y="146" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">E 500 000</text>
<text x="500" y="146" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">E 500 000</text>
<text x="620" y="146" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">E 500 000</text>
<text x="390" y="212" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">A predicate on easting alone matches five unrelated places</text>
<text x="390" y="240" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">The zone must be part of the key, and part of every predicate</text>
</svg>
</figure>

That repetition is the property that trips pipelines up. A bounding-box filter expressed in UTM coordinates without a zone predicate is not merely imprecise — it selects data from every zone at the same relative position, which can be thousands of kilometres away. The zone is not optional context; it is part of the coordinate.

The second awkwardness is that zones are **unequal in data volume**. Zone boundaries follow meridians, so a zone covering ocean holds almost nothing while one covering a dense continental region holds a large share. Partitioning by zone alone therefore produces exactly the skew pattern that a grid was supposed to avoid, and a compound key — zone plus a within-zone bucket — is usually required.

The third is that features **cross zone boundaries**. A road, a river or a flight path spanning a meridian has no single zone, and the choice is between assigning by centroid, splitting the geometry, or duplicating the row. For analysis at zone granularity, splitting is generally correct; for pruning, duplication with a primary flag preserves both.

## When UTM Is Nonetheless the Right Choice

<figure class="diagram">
<svg viewBox="0 0 762 210" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Two situations that justify UTM partitioning: source data already delivered per zone, and analysis that requires metric coordinates for area and distance measurement">
<rect x="0" y="0" width="762" height="210" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Two good reasons to keep the zone in the key</text>
<rect x="30" y="58" width="352" height="140" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="206" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">the source is already zoned</text>
<text x="206" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">satellite products, national survey data</text>
<text x="206" y="142" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">reprojecting on ingest costs accuracy</text>
<text x="206" y="164" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">and throughput for no benefit</text>
<rect x="398" y="58" width="352" height="140" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="574" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">the analysis needs metres</text>
<text x="574" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">area, distance, buffers</text>
<text x="574" y="142" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">metric coordinates avoid a</text>
<text x="574" y="164" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">reprojection on every query</text>
</svg>
</figure>

Both reasons are about avoiding a transformation rather than about the partitioning itself, and that is the honest framing. UTM as a partition key is a consequence of storing data in UTM, and storing data in UTM is justified when the analysis is measurement-heavy or when the source arrives that way and reprojection would degrade it.

Where neither applies, storing in a geographic system and partitioning by a global grid is simpler in every respect. The zone-crossing problem disappears, the coordinate repetition disappears, and the skew is easier to manage. Keep the UTM zone as an attribute column for provenance, and let the partition key be something designed for partitioning.

## Verifying a Zoned Table

<figure class="diagram">
<svg viewBox="0 0 764 202" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three assertions for a UTM partitioned table: coordinates fall inside the declared zone, every query predicate includes the zone, and no single zone dominates the row distribution">
<rect x="0" y="0" width="764" height="202" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Three assertions worth automating</text>
<rect x="26" y="58" width="230" height="132" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="141" y="86" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">coordinates match the zone</text>
<text x="141" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">reproject the centroid back</text>
<text x="141" y="138" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">longitude must land inside</text>
<text x="141" y="158" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">the zone&#8217;s 6-degree strip</text>
<rect x="274" y="58" width="230" height="132" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="389" y="86" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">predicates carry the zone</text>
<text x="389" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">scan the query history</text>
<text x="389" y="138" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">an easting filter without</text>
<text x="389" y="158" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">a zone filter is a bug</text>
<rect x="522" y="58" width="230" height="132" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="637" y="86" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">zone distribution</text>
<text x="637" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">rows per zone</text>
<text x="637" y="138" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">a dominant zone needs a</text>
<text x="637" y="158" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">second key dimension</text>
</svg>
</figure>

The middle check is the one with the most leverage and the least obvious implementation. Query history is available in every engine that matters, and a weekly scan for predicates touching the easting or northing columns without a zone predicate finds the queries that are silently reading five zones' worth of data. Those queries return results that look plausible, which is why nobody reports them.

Where the check finds them, the fix is usually a view that takes a geographic bounding box, resolves it to the affected zones, and emits both the zone list and the per-zone coordinate ranges. Callers then work in the coordinate system they think in, and the zone bookkeeping happens once in a place that can be tested.

Ship the view alongside the table from the first day rather than adding it after the first incident, because a convention that arrives late competes with queries people have already written and copied.

For the broader question of whether a projected coordinate system belongs in the storage layer at all, see [CRS management pipelines](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/crs-management-pipelines/), which covers the canonical-system argument and the cases that genuinely warrant an exception.
