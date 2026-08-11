# Z-Ordering for Geospatial Queries

## Architectural Positioning & Core Mechanics

Z-ordering functions as a fine-grained, file-level clustering mechanism that sits directly above coarse directory partitioning in modern lakehouse stacks. By mapping multi-dimensional geospatial coordinates into a single, linear sort key via a space-filling curve (Morton/Z-curve), it ensures that spatially proximate records are physically co-located within Parquet or Delta files. This architecture dramatically reduces I/O for bounding-box filters, proximity searches, and spatial predicates. While foundational [Spatial Partitioning & Indexing Strategies](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/) reduce scan scope at the directory level, Z-ordering operates within those partitions to maximize data-skipping efficiency.

The core algorithm interleaves the binary representations of coordinate dimensions. For a 2D point `(x, y)`, the engine extracts bits from each dimension and alternates them (`x₀, y₀, x₁, y₁, ...`). The resulting Z-value preserves spatial locality: points close in geographic space yield numerically adjacent sort keys. Query engines generate file-level min/max statistics on the clustered columns, enabling [Predicate Pushdown Optimization](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/predicate-pushdown-optimization/) to bypass entire files when the query envelope falls outside the stored value ranges.

## CRS Selection & Spatial Parameterization

Z-ordering effectiveness is highly sensitive to coordinate reference system (CRS) selection. Raw latitude/longitude (EPSG:4326) introduces non-uniform spatial distortion near the poles, degrading locality preservation for global datasets. For regional or continental workloads, project coordinates to a metric CRS (e.g., UTM zones like EPSG:32633 or EPSG:3857) before applying Z-ordering.

**Explicit Spatial Parameters in Practice:**
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

# 1. Project to metric CRS for uniform spatial locality.
# Assumes Apache Sedona is configured via spark.sql.extensions.
# ST_Transform requires the source and target EPSG codes.
df = spark.read.parquet("s3://raw-gis/iot-telemetry/")
df_projected = df \
    .withColumn("utm_x",
        spark.sql("SELECT ST_X(ST_Transform(ST_Point(lon, lat), 'EPSG:4326', 'EPSG:32633'))").collect()[0][0]
    )
# In practice, use a UDF or Sedona SQL expressions registered on the session:
# df_projected = df.withColumn(
#     "utm_x", expr("ST_X(ST_Transform(ST_Point(lon, lat, 4326), 32633))")
# ).withColumn(
#     "utm_y", expr("ST_Y(ST_Transform(ST_Point(lon, lat, 4326), 32633))")
# )
```

Always store the original CRS alongside projected coordinates to maintain geodetic integrity for downstream GIS consumers. Reference authoritative CRS definitions via the [EPSG Geodetic Parameter Dataset](https://epsg.org/) when validating transformation matrices.

## Format-Specific Implementation

Lakehouse engines diverge in how they materialize and maintain Z-ordering. Production deployments must account for write amplification, compaction cadence, and metadata overhead.

### Apache Iceberg

Iceberg enforces Z-ordering as a deterministic table property applied during data file rewriting. The engine does not auto-cluster during streaming writes; maintenance requires explicit data file rewriting.

```sql
-- DDL: Define sort order on projected coordinates
CREATE TABLE analytics.gis_vehicle_tracks (
  track_id  STRING,
  event_ts  TIMESTAMP,
  utm_x     DOUBLE,
  utm_y     DOUBLE,
  payload   MAP<STRING, STRING>
)
USING iceberg
PARTITIONED BY (days(event_ts))
TBLPROPERTIES (
  'write.sort-order' = 'utm_x ASC, utm_y ASC',
  'write.target-file-size-bytes' = '134217728'  -- 128MB
);
```

```sql
-- Compaction with explicit sort strategy via stored procedure
CALL system.rewrite_data_files(
  table => 'analytics.gis_vehicle_tracks',
  strategy => 'sort',
  sort_order => 'utm_x ASC, utm_y ASC',
  options => map('target-file-size-bytes', '134217728')
);
```

See official configuration details at [Apache Iceberg Sort Order Documentation](https://iceberg.apache.org/docs/latest/spark-configuration/#write-sort-order).

### Delta Lake

Delta applies Z-ordering as a post-write compaction operation via `OPTIMIZE`. The schema remains unchanged; clustering is materialized during `OPTIMIZE`.

```sql
-- Apply Z-ordering to existing Delta table
OPTIMIZE analytics.gis_vehicle_tracks
ZORDER BY (utm_x, utm_y)
WHERE event_ts >= '2024-01-01';
```

Delta's automated data-skipping layer tightly integrates with the Z-ordered column statistics. Monitor `delta.targetFileSize` and `spark.databricks.delta.optimize.maxThreads` to bound resource consumption. Reference [Delta Lake Z-Ordering Documentation](https://docs.delta.io/latest/optimizations-oss.html#z-ordering-multi-dimensional-clustering) for engine-specific tuning.

## Layering with Partitioning & Retention Policies

Z-ordering is not a partitioning replacement. Without a coarse partitioning strategy, engines must sort the entire dataset during compaction, causing OOM failures and excessive shuffle. Combine time-based or region-based partitioning with Z-ordering to bound sort scope.

**Recommended Partition Bounds:**
- **Temporal:** `PARTITIONED BY (days(event_ts))`
- **Spatial (Optional):** `PARTITIONED BY (utm_zone_bucket)` for multi-continental datasets
- **Retention:** Enforce snapshot/file retention to prevent metadata bloat.
  - Delta: `delta.deletedFileRetentionDuration = 'interval 30 days'`
  - Iceberg: `'history.expire.max-snapshot-age-ms' = '2592000000'` (30 days)

When partition bounds align with query patterns, Z-ordering operates efficiently within narrow file groups. For deeper partitioning topology guidance, review [Spatial Partitioning Schemes](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/spatial-partitioning-schemes/).

## CI/CD Automation & Operational Guardrails

Production Z-ordering requires scheduled, idempotent compaction pipelines:

```yaml
name: Lakehouse Z-Order Compaction
on:
  schedule:
    - cron: '0 2 * * *'  # Daily at 02:00 UTC
  workflow_dispatch:

jobs:
  optimize-spatial:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Setup Spark & Delta
        run: |
          pip install pyspark==3.5.5 delta-spark==3.3.0
      - name: Run Compaction
        env:
          AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
          AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
        run: |
          spark-submit \
            --packages io.delta:delta-spark_2.12:3.3.0 \
            --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
            --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
            optimize_zorder.py
```

**Guardrails:**
- Limit concurrent compaction jobs to avoid metadata lock contention.
- Set `spark.sql.files.maxPartitionBytes` to `134217728` (128MB) to prevent oversized Z-ordered files.
- Emit CloudWatch/Prometheus metrics for `files_rewritten`, `bytes_skipped`, and `compaction_duration_ms`.

## Troubleshooting & Performance Tuning

| Symptom | Root Cause | Resolution Path |
|---------|------------|-----------------|
| Low data-skipping ratio (<40%) | Stale min/max stats after bulk upserts | Run `OPTIMIZE ... ZORDER BY` (Delta) or `rewrite_data_files` (Iceberg) immediately after large batch loads. |
| High write amplification during compaction | Z-ordering applied to high-cardinality non-spatial columns | Restrict `ZORDER BY` to 2–3 spatial columns. Remove categorical IDs or timestamps from the sort order. |
| Query returns incorrect spatial results | CRS mismatch between stored data and query filter | Verify query envelope uses the same projection as the Z-ordered column. Transform query bounds to the table's CRS before execution. |
| Compaction OOMs | Partition scope too large or target file size misconfigured | Reduce partition granularity (e.g., switch from monthly to daily). Lower `write.target-file-size-bytes` to `67108864` (64MB). |
| Join performance degradation | Z-ordering not aligned with join keys | For spatial joins, align Z-order columns with the driving table's geometry bounding box columns. See [Optimizing spatial joins with Iceberg Z-ordering](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/z-ordering-for-geospatial-queries/optimizing-spatial-joins-with-iceberg-z-ordering/) for join-specific clustering strategies. |

**Validation Checklist Before Production Rollout:**
- [ ] Confirm CRS consistency across ingestion, Z-ordering, and query layers.
- [ ] Verify partition bounds match query filter cardinality (aim for 100MB–500MB per partition).
- [ ] Benchmark `EXPLAIN` plans to confirm file-level skipping triggers on spatial predicates.
- [ ] Schedule automated `OPTIMIZE` / `rewrite_data_files` jobs aligned with data ingestion SLAs.
- [ ] Monitor metadata store growth; enforce snapshot/file retention policies.

## What a Space-Filling Curve Actually Buys

Z-ordering is frequently described as "sorting by two columns at once", which undersells what it does and hides why it sometimes disappoints.

<figure class="diagram">
<svg viewBox="0 0 706 278" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Comparison of lexicographic sorting by x then y, where a query window intersects many disjoint ranges, against Z-order interleaving where the same window maps to a small number of contiguous ranges">
<rect x="0" y="0" width="706" height="278" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Why interleaving beats sorting by x then y</text>
<text x="196" y="62" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#9a5a17">sort by x, then y</text>
<rect x="86" y="78" width="220" height="160" fill="#ffffff" stroke="#9a5a17" stroke-width="2"/>
<line x1="114" y1="78" x2="114" y2="238" stroke="#eaddc8" stroke-width="1"/>
<line x1="142" y1="78" x2="142" y2="238" stroke="#eaddc8" stroke-width="1"/>
<line x1="170" y1="78" x2="170" y2="238" stroke="#eaddc8" stroke-width="1"/>
<line x1="198" y1="78" x2="198" y2="238" stroke="#eaddc8" stroke-width="1"/>
<line x1="226" y1="78" x2="226" y2="238" stroke="#eaddc8" stroke-width="1"/>
<line x1="254" y1="78" x2="254" y2="238" stroke="#eaddc8" stroke-width="1"/>
<line x1="282" y1="78" x2="282" y2="238" stroke="#eaddc8" stroke-width="1"/>
<rect x="142" y="120" width="84" height="70" fill="none" stroke="#0e6e7d" stroke-width="2.5"/>
<rect x="142" y="120" width="28" height="70" fill="#f2e8da"/>
<rect x="170" y="120" width="28" height="70" fill="#f2e8da"/>
<rect x="198" y="120" width="28" height="70" fill="#f2e8da"/>
<text x="196" y="262" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">3 columns hit, each read end to end</text>
<text x="584" y="62" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#2f6e49">Z-order interleaved</text>
<rect x="474" y="78" width="220" height="160" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<rect x="474" y="78" width="55" height="40" fill="#e6f0ea" stroke="#d7e8de" stroke-width="1"/>
<rect x="529" y="78" width="55" height="40" fill="#e6f0ea" stroke="#d7e8de" stroke-width="1"/>
<rect x="584" y="78" width="55" height="40" fill="#e6f0ea" stroke="#d7e8de" stroke-width="1"/>
<rect x="639" y="78" width="55" height="40" fill="#e6f0ea" stroke="#d7e8de" stroke-width="1"/>
<rect x="474" y="118" width="55" height="40" fill="#e6f0ea" stroke="#d7e8de" stroke-width="1"/>
<rect x="529" y="118" width="55" height="40" fill="#d7e8de" stroke="#2f6e49" stroke-width="1"/>
<rect x="584" y="118" width="55" height="40" fill="#d7e8de" stroke="#2f6e49" stroke-width="1"/>
<rect x="639" y="118" width="55" height="40" fill="#e6f0ea" stroke="#d7e8de" stroke-width="1"/>
<rect x="474" y="158" width="55" height="40" fill="#e6f0ea" stroke="#d7e8de" stroke-width="1"/>
<rect x="529" y="158" width="55" height="40" fill="#d7e8de" stroke="#2f6e49" stroke-width="1"/>
<rect x="584" y="158" width="55" height="40" fill="#d7e8de" stroke="#2f6e49" stroke-width="1"/>
<rect x="639" y="158" width="55" height="40" fill="#e6f0ea" stroke="#d7e8de" stroke-width="1"/>
<rect x="474" y="198" width="55" height="40" fill="#e6f0ea" stroke="#d7e8de" stroke-width="1"/>
<rect x="529" y="198" width="55" height="40" fill="#e6f0ea" stroke="#d7e8de" stroke-width="1"/>
<rect x="584" y="198" width="55" height="40" fill="#e6f0ea" stroke="#d7e8de" stroke-width="1"/>
<rect x="639" y="198" width="55" height="40" fill="#e6f0ea" stroke="#d7e8de" stroke-width="1"/>
<rect x="529" y="118" width="110" height="80" fill="none" stroke="#0e6e7d" stroke-width="2.5"/>
<text x="584" y="262" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">4 blocks hit, and only those blocks</text>
</svg>
</figure>

Lexicographic sorting privileges the first column absolutely. Every row with a given x value is adjacent regardless of its y, so a query window that spans a narrow band of x reads the full y extent for each of those x values — the shaded columns in the left panel. The data is sorted, the statistics are tight in one dimension, and the pruning is one-dimensional.

Interleaving the bits of the two coordinates produces an ordering in which nearby points in *both* dimensions are nearby in the sequence. The query window maps to a small number of contiguous ranges instead of one range per x value, and file-level statistics become genuinely two-dimensional. That is the entire mechanism, and it explains both the benefit and its limits.

The limits are worth stating plainly. The curve has discontinuities — points that are adjacent on the ground can be far apart in the ordering when they fall on opposite sides of a major bit boundary — so pruning is good but never perfect, and a query window straddling such a boundary reads more than its area suggests. Hilbert ordering has fewer of these jumps than Morton ordering and is correspondingly better where the engine offers it. And the benefit falls off as more columns are interleaved: with two columns the ordering is strong, with five it is weak in every dimension, which is why Z-ordering on a long column list usually disappoints.

## Choosing What to Interleave

The columns given to a Z-order are the whole design, and three rules cover almost every case.

**Interleave coordinates, not identifiers.** The ordering only produces locality if nearby values are semantically nearby. Two bounding-box minimum values that differ slightly describe adjacent features; two asset identifiers that differ slightly describe unrelated assets. Including an identifier consumes bits and returns nothing.

**Interleave two columns, occasionally three.** Two coordinates is the canonical case. A third dimension — time, or elevation — is defensible when it appears in nearly every query, but it dilutes the spatial dimensions and is usually better expressed as a partition than as another curve dimension.

**Interleave the minimum, not the centroid.** For point data the distinction is empty. For polygons it matters: the minimum corner is what file statistics record, so ordering by the same values the statistics track keeps the two consistent. Ordering by centroid while pruning on minimum produces files whose statistics are looser than the ordering deserves.

One further consideration applies to data with very uneven precision. Coordinates stored as doubles carry more precision than the data warrants — survey-grade positions and phone GPS positions land in the same column — and the low-order bits of a noisy coordinate are effectively random. Interleaving them wastes curve resolution on noise. Rounding coordinates to the precision the data actually has, before computing the ordering, produces measurably tighter clustering at no cost to accuracy that anyone can observe.

## Measuring Whether the Ordering Is Working

A sort order is a claim about physical layout, and the claim is verifiable directly from file statistics without running a single query.

<figure class="diagram">
<svg viewBox="0 0 696 260" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Clustering quality measured as the ratio of the sum of per-file bounding box areas to the area of the table extent, illustrated with a poorly clustered table where every file box covers the whole extent and a well clustered one where boxes tile the extent">
<rect x="0" y="0" width="696" height="260" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Clustering quality, read straight from file statistics</text>
<text x="196" y="60" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#9a5a17">overlap factor ≈ 40</text>
<rect x="96" y="74" width="200" height="130" fill="#ffffff" stroke="#9a5a17" stroke-width="2"/>
<rect x="104" y="80" width="184" height="118" fill="none" stroke="#9a5a17" stroke-width="1.2"/>
<rect x="110" y="86" width="176" height="110" fill="none" stroke="#9a5a17" stroke-width="1.2"/>
<rect x="116" y="92" width="168" height="102" fill="none" stroke="#9a5a17" stroke-width="1.2"/>
<rect x="122" y="98" width="160" height="94" fill="none" stroke="#9a5a17" stroke-width="1.2"/>
<text x="196" y="226" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">every file&#8217;s box covers the extent</text>
<text x="196" y="244" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">no file can ever be skipped</text>
<text x="584" y="60" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#2f6e49">overlap factor ≈ 1.3</text>
<rect x="484" y="74" width="200" height="130" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<rect x="490" y="80" width="94" height="60" fill="#e6f0ea" stroke="#2f6e49" stroke-width="1.2"/>
<rect x="588" y="80" width="90" height="60" fill="#e6f0ea" stroke="#2f6e49" stroke-width="1.2"/>
<rect x="490" y="144" width="94" height="54" fill="#e6f0ea" stroke="#2f6e49" stroke-width="1.2"/>
<rect x="588" y="144" width="90" height="54" fill="#e6f0ea" stroke="#2f6e49" stroke-width="1.2"/>
<text x="584" y="226" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">file boxes tile the extent</text>
<text x="584" y="244" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">a window reads one or two files</text>
</svg>
</figure>

The metric is the **overlap factor**: sum the areas of every file's bounding box and divide by the area of the table's overall extent. A perfectly clustered table scores close to one, because the file boxes tile the extent with little overlap. A table written in arrival order scores close to the file count, because every box covers everything.

It is computable from metadata alone — Iceberg's `files` metadata table and Delta's log both expose per-file min and max for the bbox columns — so it costs nothing to track and it answers the question that query timings answer only indirectly. A table whose overlap factor has risen from 1.4 to 9 over a month has been accumulating unsorted appends, and that is actionable before anybody complains.

```sql
-- Iceberg 1.4+. Overlap factor from the files metadata table; no data is read.
WITH f AS (
  SELECT
    (upper_bounds['bbox_max_x'] - lower_bounds['bbox_min_x']) *
    (upper_bounds['bbox_max_y'] - lower_bounds['bbox_min_y']) AS box_area,
    lower_bounds['bbox_min_x'] AS minx, upper_bounds['bbox_max_x'] AS maxx,
    lower_bounds['bbox_min_y'] AS miny, upper_bounds['bbox_max_y'] AS maxy
  FROM lakehouse.spatial.telemetry.files
)
SELECT sum(box_area) /
       ((max(maxx) - min(minx)) * (max(maxy) - min(miny))) AS overlap_factor
FROM f;
```

Track it per partition rather than table-wide where the table is partitioned, because a single badly-clustered partition is invisible in the aggregate and is exactly the one that will be slow.

## The Cost Side of Sorting

Sorting is not free, and the cost lands on the write path, which is the path with the least headroom in a streaming pipeline.

A global sort requires a full shuffle, and its cost scales worse than linearly with data volume because of the exchange. On a large table this is the single most expensive maintenance operation available, and running it too frequently costs more than the queries it accelerates. The usual mitigation is to sort **within partitions** rather than globally: a partition-local sort needs no cross-partition exchange, parallelises perfectly, and produces almost all of the benefit when the partition key already provides coarse locality.

The second cost is **write amplification during compaction**. Re-sorting a partition rewrites every file in it, so a daily re-sort of a table with ninety days of live data rewrites ninety times more bytes than the day's ingest. Scope the maintenance to partitions that changed, and let historical partitions settle permanently once they stop receiving writes — which is the great advantage of a time-partitioned layout, since yesterday's partition is finished and never needs touching again.

The third cost is **contention**. A sort rewrite holds a long-running commit against partitions the streaming writer may also target, and the loser retries. Restricting maintenance to closed partitions eliminates this entirely; where that is impossible, cap the rewrite duration so a conflict costs a short retry rather than an hour of thrashing.

A workable schedule for a typical telemetry table: sort the current day's partition every few hours with a small file-count threshold so the work stays incremental, run one final sort on the day's partition after it closes, and never touch it again. Historical re-sorts then happen only when the sort columns themselves change, which should be rare.

## Interaction With Partitioning and Retention

Sort order does not exist in isolation. It composes with the partition key and with the retention lifecycle, and the three together determine whether the layout stays healthy without constant attention.

<figure class="diagram">
<svg viewBox="0 0 758 244" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Lifecycle of a time partitioned spatial table showing the hot partition receiving unsorted appends and frequent incremental sorts, the recent partition sorted once on close, and the cold partition never rewritten again">
<rect x="0" y="0" width="758" height="244" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Sorting effort concentrates on the newest partition only</text>
<rect x="34" y="66" width="220" height="120" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="144" y="94" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">today</text>
<text x="144" y="120" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">unsorted appends arriving</text>
<text x="144" y="142" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">incremental sort every few hours</text>
<text x="144" y="166" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">overlap factor oscillates</text>
<rect x="280" y="66" width="220" height="120" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="390" y="94" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">yesterday</text>
<text x="390" y="120" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">partition closed</text>
<text x="390" y="142" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">one final full sort</text>
<text x="390" y="166" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">overlap factor settles near 1</text>
<rect x="526" y="66" width="220" height="120" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="636" y="94" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">older than a week</text>
<text x="636" y="120" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">immutable in practice</text>
<text x="636" y="142" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">never rewritten again</text>
<text x="636" y="166" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">zero ongoing cost</text>
<text x="390" y="228" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">A time partition makes sorting a bounded, finite job instead of a perpetual one</text>
</svg>
</figure>

This is the strongest practical argument for a time dimension in the partition key even on tables where queries rarely filter by time. Without it, every sort rewrite is global and the cost grows with the whole table forever. With it, the cost is proportional to one day of data, permanently, no matter how many years accumulate.

Retention interacts with the same mechanism. Expiring old snapshots frees the files that historical rewrites would otherwise pin, and skipping the rewrites in the first place means fewer historical snapshots to expire. The three settings — partition granularity, sort schedule and retention window — should be chosen together, because a change to any one of them changes the maintenance cost of the other two, and tuning them independently is how platforms end up with a compaction job that never finishes.

## Common Mistakes That Waste the Ordering

Four errors account for most of the cases where Z-ordering is configured correctly and delivers nothing measurable.

**Sorting on columns nobody filters.** Ordering by `bbox_min_x, bbox_min_y` helps only queries that reference those columns. A caller filtering exclusively on `ST_Intersects` against the geometry gets no benefit at all, because the engine cannot connect the geometry predicate to the sorted columns. The ordering and the query convention have to be designed together.

**Re-sorting a table that is already partitioned finely enough.** When each partition holds a single file, sorting within the partition changes nothing — there is nothing to prune between. The effort is wasted, and the symptom is a compaction job that runs for hours with no measurable improvement in query times. Check the files-per-partition count before scheduling a sort.

**Interleaving a column with vastly different scale.** Combining a longitude in degrees with a projected northing in metres puts one column's significant bits far above the other's, so the ordering is effectively one-dimensional. Normalise the columns to a common range before interleaving, or interleave columns that already share units.

**Assuming the sort survives a merge.** A `MERGE` or an upsert writes new files that are locally sorted at best and often not sorted at all. On a table receiving daily merges, ordering quality decays on a timescale of days regardless of how carefully the initial sort was done. Either schedule the re-sort at the same cadence as the merges, or use a clustering mechanism that maintains the ordering incrementally.

The unifying diagnosis for all four is the overlap-factor metric described above. It is cheap, it is computed from metadata, and it will show a table that is nominally sorted and physically not — which is the state every one of these mistakes produces.

Treat the ordering as one component of the layout rather than as a tuning knob to be turned when queries feel slow. It works when the partition key gives it a bounded scope, when the derived columns give the planner something to compare, and when the maintenance schedule keeps it fresh — and it does very little on its own.
