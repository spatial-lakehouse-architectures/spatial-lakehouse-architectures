# Optimizing Spatial Joins with Iceberg Z-Ordering

In production lakehouse architectures, spatial join failures rarely stem from raw compute exhaustion. They originate from cross-partition shuffle skew. When joining high-cardinality vector layers (cadastral parcels, sensor footprints, road networks) against time-series telemetry or raster tilesets, standard Iceberg partitioning by ingestion timestamp or administrative region fails to localize spatial predicates. The query planner defaults to broadcast or sort-merge joins that trigger full table scans, materializing intermediate datasets that exceed executor memory limits, saturate network I/O, and breach SLAs. The engineering objective is deterministic: enforce spatial locality at the file level using multi-dimensional Z-ordering to enable aggressive predicate pruning and eliminate unnecessary shuffle.

## The Partition Blindness Failure Mode

Traditional [Spatial Partitioning & Indexing Strategies](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/) rely on hierarchical grids, temporal buckets, or categorical boundaries that rarely align with runtime spatial query patterns. When a join executes `ST_Intersects(a.geometry, b.geometry)`, the optimizer cannot map bounding box overlap to physical file boundaries if partitioning is purely temporal or categorical. Iceberg's metadata layer tracks column-level min/max statistics per data file, but geometry columns stored as raw `BINARY` (WKB) lack scalar bounds. Without spatial clustering on explicit bounding box columns, every join degenerates into a Cartesian product across partitions, forcing the execution engine to deserialize and evaluate geometries that fall entirely outside the target region.

## Configuring Z-Order as a Sort Primitive

Z-ordering maps multi-dimensional spatial coordinates into a single scalar sort key by interleaving the binary representations of X and Y dimensions. In Iceberg, this is applied via scheduled compaction. By materializing a scalar Z-value column (or equivalent bbox columns sorted together), spatial locality is transformed into a linear sort key. The [Z-Ordering for Geospatial Queries](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/z-ordering-for-geospatial-queries/) methodology ensures that spatially proximate features land in the same Parquet row groups, enabling the query planner to prune files using simple range predicates on the bounding box columns instead of evaluating complex spatial functions at runtime.

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

## Why a Join Benefits More Than a Filter

Sorting helps a filtered scan by letting the reader skip files. It helps a join by a second, larger mechanism: it makes the two sides of the join *co-located*, so that matching partitions of the two inputs can be paired without a full exchange.

<figure class="diagram">
<svg viewBox="0 0 751 278" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="A spatial join between two unsorted tables requires each partition of one side to be compared against every partition of the other, while two tables sorted on the same curve pair partition to partition">
<defs>
<marker id="zj-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#2f6e49"/></marker>
</defs>
<rect x="0" y="0" width="751" height="278" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Co-location turns a many-to-many exchange into pairs</text>
<text x="196" y="62" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#9a5a17">unsorted: every pair compared</text>
<rect x="90" y="80" width="46" height="30" fill="#f2e8da" stroke="#9a5a17" stroke-width="1.5"/>
<rect x="146" y="80" width="46" height="30" fill="#f2e8da" stroke="#9a5a17" stroke-width="1.5"/>
<rect x="202" y="80" width="46" height="30" fill="#f2e8da" stroke="#9a5a17" stroke-width="1.5"/>
<rect x="258" y="80" width="46" height="30" fill="#f2e8da" stroke="#9a5a17" stroke-width="1.5"/>
<rect x="90" y="200" width="46" height="30" fill="#f2e8da" stroke="#9a5a17" stroke-width="1.5"/>
<rect x="146" y="200" width="46" height="30" fill="#f2e8da" stroke="#9a5a17" stroke-width="1.5"/>
<rect x="202" y="200" width="46" height="30" fill="#f2e8da" stroke="#9a5a17" stroke-width="1.5"/>
<rect x="258" y="200" width="46" height="30" fill="#f2e8da" stroke="#9a5a17" stroke-width="1.5"/>
<line x1="113" y1="110" x2="113" y2="200" stroke="#9a5a17" stroke-width="1"/>
<line x1="113" y1="110" x2="169" y2="200" stroke="#9a5a17" stroke-width="1"/>
<line x1="113" y1="110" x2="225" y2="200" stroke="#9a5a17" stroke-width="1"/>
<line x1="113" y1="110" x2="281" y2="200" stroke="#9a5a17" stroke-width="1"/>
<line x1="169" y1="110" x2="113" y2="200" stroke="#9a5a17" stroke-width="1"/>
<line x1="169" y1="110" x2="169" y2="200" stroke="#9a5a17" stroke-width="1"/>
<line x1="169" y1="110" x2="225" y2="200" stroke="#9a5a17" stroke-width="1"/>
<line x1="169" y1="110" x2="281" y2="200" stroke="#9a5a17" stroke-width="1"/>
<line x1="225" y1="110" x2="113" y2="200" stroke="#9a5a17" stroke-width="1"/>
<line x1="225" y1="110" x2="169" y2="200" stroke="#9a5a17" stroke-width="1"/>
<line x1="225" y1="110" x2="225" y2="200" stroke="#9a5a17" stroke-width="1"/>
<line x1="225" y1="110" x2="281" y2="200" stroke="#9a5a17" stroke-width="1"/>
<line x1="281" y1="110" x2="113" y2="200" stroke="#9a5a17" stroke-width="1"/>
<line x1="281" y1="110" x2="169" y2="200" stroke="#9a5a17" stroke-width="1"/>
<line x1="281" y1="110" x2="225" y2="200" stroke="#9a5a17" stroke-width="1"/>
<line x1="281" y1="110" x2="281" y2="200" stroke="#9a5a17" stroke-width="1"/>
<text x="196" y="262" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">shuffle volume grows with the product</text>
<text x="584" y="62" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#2f6e49">both sorted on the same curve</text>
<rect x="478" y="80" width="46" height="30" fill="#e6f0ea" stroke="#2f6e49" stroke-width="1.5"/>
<rect x="534" y="80" width="46" height="30" fill="#e6f0ea" stroke="#2f6e49" stroke-width="1.5"/>
<rect x="590" y="80" width="46" height="30" fill="#e6f0ea" stroke="#2f6e49" stroke-width="1.5"/>
<rect x="646" y="80" width="46" height="30" fill="#e6f0ea" stroke="#2f6e49" stroke-width="1.5"/>
<rect x="478" y="200" width="46" height="30" fill="#e6f0ea" stroke="#2f6e49" stroke-width="1.5"/>
<rect x="534" y="200" width="46" height="30" fill="#e6f0ea" stroke="#2f6e49" stroke-width="1.5"/>
<rect x="590" y="200" width="46" height="30" fill="#e6f0ea" stroke="#2f6e49" stroke-width="1.5"/>
<rect x="646" y="200" width="46" height="30" fill="#e6f0ea" stroke="#2f6e49" stroke-width="1.5"/>
<line x1="501" y1="110" x2="501" y2="200" stroke="#2f6e49" stroke-width="2.5" marker-end="url(#zj-arrow)"/>
<line x1="557" y1="110" x2="557" y2="200" stroke="#2f6e49" stroke-width="2.5" marker-end="url(#zj-arrow)"/>
<line x1="613" y1="110" x2="613" y2="200" stroke="#2f6e49" stroke-width="2.5" marker-end="url(#zj-arrow)"/>
<line x1="669" y1="110" x2="669" y2="200" stroke="#2f6e49" stroke-width="2.5" marker-end="url(#zj-arrow)"/>
<text x="584" y="262" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">shuffle volume grows with the data, not the product</text>
</svg>
</figure>

This is why a sorted layout produces a larger speedup on joins than on filters, and why it is worth sorting both sides of a frequently-joined pair on the same columns with the same curve. The saving is in the exchange, which is the part of a distributed join that does not parallelise away.

The caveat is that co-location only helps when the engine knows about it. Some planners detect matching sort orders and elide the shuffle; others do not and will shuffle regardless. Check the plan for an exchange operator before and after, because sorting both sides and still shuffling costs the sort with none of the benefit — in which case an explicit spatial partitioner, or a broadcast of the smaller side, is the better lever.

## Sorting the Right Side of the Join

<figure class="diagram">
<svg viewBox="0 0 764 210" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three join shapes and where sorting effort belongs: small polygon side broadcast so only the large side needs sorting, two large sides where both need the same sort order, and a repeated join against a reference table which justifies sorting it once">
<rect x="0" y="0" width="764" height="210" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Where the sorting effort belongs</text>
<rect x="26" y="58" width="230" height="140" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="141" y="86" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">small side broadcast</text>
<text x="141" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">thousands of polygons</text>
<text x="141" y="138" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">sort the large side only</text>
<text x="141" y="162" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">the small side is in memory</text>
<text x="141" y="184" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">cheapest case by far</text>
<rect x="274" y="58" width="230" height="140" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="389" y="86" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">large versus large</text>
<text x="389" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">both sides at scale</text>
<text x="389" y="138" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">sort both, same columns,</text>
<text x="389" y="162" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">same curve, same resolution</text>
<text x="389" y="184" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">check the exchange is elided</text>
<rect x="522" y="58" width="230" height="140" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="637" y="86" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">repeated reference join</text>
<text x="637" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">same table, many queries</text>
<text x="637" y="138" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">sort it once, permanently</text>
<text x="637" y="162" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">cost amortises immediately</text>
<text x="637" y="184" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#6a3d9a">easiest win available</text>
</svg>
</figure>

The left-hand case covers the majority of production spatial joins — telemetry against administrative boundaries, events against service areas, observations against a fixed set of regions — and it is worth checking whether a join really is large-versus-large before investing in sorting both sides. A polygon table of a few thousand rows fits comfortably in memory on every executor, and broadcasting it removes the exchange entirely, which is a bigger saving than any sort order can deliver.

## Keeping the Sort Order Alive

<figure class="diagram">
<svg viewBox="0 0 732 228" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Decay of clustering quality between compaction runs, showing the overlap factor rising as unsorted appends accumulate and dropping back after each scheduled rewrite">
<rect x="0" y="0" width="732" height="228" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Clustering decays between rewrites — schedule accordingly</text>
<line x1="80" y1="180" x2="720" y2="180" stroke="#33707d" stroke-width="1.5"/>
<line x1="80" y1="60" x2="80" y2="180" stroke="#33707d" stroke-width="1.5"/>
<text x="64" y="66" text-anchor="end" font-family="sans-serif" font-size="11" fill="#33707d">high</text>
<text x="64" y="180" text-anchor="end" font-family="sans-serif" font-size="11" fill="#33707d">1.0</text>
<text x="400" y="212" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">time &#8594;</text>
<path d="M90 172 L200 96 L202 168 L312 92 L314 168 L424 90 L426 168 L536 94 L538 168 L648 92 L650 168 L714 130" fill="none" stroke="#9a5a17" stroke-width="2.5"/>
<text x="252" y="76" font-family="sans-serif" font-size="11" fill="#9a5a17">appends raise the overlap factor</text>
<text x="252" y="196" font-family="sans-serif" font-size="11" fill="#2f6e49">each rewrite returns it near 1</text>
</svg>
</figure>

The sawtooth is normal and it is the reason the rewrite cadence matters more than the rewrite quality. A perfect sort run monthly leaves the table poorly clustered for most of the month; a good-enough sort run every few hours on the active partition keeps the average close to the peak. Where the format offers incremental clustering that maintains the ordering as data arrives, it flattens the curve entirely and is worth adopting for any continuously written table.

Set the cadence from the measurement rather than from a convention. A table whose overlap factor rises from 1.2 to 3 in six hours needs a six-hourly rewrite; one that takes a week to reach the same point needs a weekly one. Both numbers come from the same one-line metadata query, and running it for a fortnight before fixing the schedule produces a cadence that fits the workload instead of one inherited from a template.

Record the cadence and the observed overlap factor as table properties so the next person to look at the table inherits both the setting and the reason for it, rather than a maintenance job whose schedule appears arbitrary.

The pattern generalises to every maintenance setting on a spatial table: measure the property the setting is meant to preserve, choose the cadence from the measured decay, and store both alongside the data so the decision remains auditable.

A schedule chosen this way tends to survive staff changes, because the reasoning is recoverable from the table rather than from whoever configured it.

For the mechanics of the sort itself and how it interacts with partition granularity, see [Z-ordering for geospatial queries](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/z-ordering-for-geospatial-queries/).
That page also covers the overlap-factor metric referenced throughout this guide, and the query that computes it from table metadata alone.
