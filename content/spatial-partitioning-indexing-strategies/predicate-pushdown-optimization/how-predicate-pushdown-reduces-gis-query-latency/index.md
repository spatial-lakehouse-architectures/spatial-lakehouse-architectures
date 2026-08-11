# How Predicate Pushdown Reduces GIS Query Latency in Spatial Lakehouse Architectures

In spatial data lakehouse deployments, query latency is predominantly driven by compute-side geometry evaluation. When a query engine receives a geospatial filter such as `ST_Intersects`, `ST_Contains`, or a bounding-box constraint, the default execution path materializes entire Parquet files into executor memory before applying spatial predicates. On multi-terabyte vector datasets, this triggers excessive network I/O, executor OOM crashes, and query timeouts. The engineering objective is to eliminate full-file materialization by translating spatial predicates into storage-level pruning conditions through systematic [Predicate Pushdown Optimization](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/predicate-pushdown-optimization/). This shifts geometry evaluation from the compute layer to the metadata and file-scan layer, reducing latency by 80–99% in production workloads.

## Execution Path: Compute-Side vs. Storage-Side Evaluation

Open table formats decouple compute from storage, but spatial UDFs traditionally assume tight coupling. Without pushdown, the query planner treats `ST_Intersects(geom, bbox)` as a post-scan filter. The engine reads every row, deserializes WKB/WKT payloads, constructs in-memory geometry objects, and evaluates spatial relationships row-by-row. This approach routinely consumes 10–15x more I/O than necessary and forces Spark/Trino executors to spill to disk.

Storage-side pushdown intercepts the logical plan during the physical planning phase. The optimizer extracts the query bounding box, translates it into numeric range predicates on explicit bbox columns, and pushes those ranges into Iceberg manifest files or Delta transaction logs. Only data files whose spatial envelopes intersect the query bbox are scheduled for scan. Pushdown efficiency is directly proportional to how well the physical layout aligns with spatial locality, which is why [Spatial Partitioning & Indexing Strategies](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/) dictate baseline performance.

## Configuration: Schema Design & Metadata Generation

Native spatial functions are opaque to most query optimizers. To enable deterministic pushdown, you must materialize bounding box coordinates as explicit numeric columns and rely on column-level min/max statistics.

### 1. Schema Definition with Envelope Columns
```sql
CREATE TABLE parcels_spatial (
    parcel_id  STRING,
    geom       BINARY,    -- WKB
    bbox_min_x DOUBLE,
    bbox_max_x DOUBLE,
    bbox_min_y DOUBLE,
    bbox_max_y DOUBLE
) USING DELTA
PARTITIONED BY (region_code);
```

### 2. Ingestion Pipeline (PySpark)
Compute envelope columns during write to ensure statistics are captured at the file level.
```python
from pyspark.sql.functions import expr

df_with_bbox = df \
    .withColumn("bbox_min_x", expr("ST_XMin(ST_GeomFromWKB(geom))")) \
    .withColumn("bbox_max_x", expr("ST_XMax(ST_GeomFromWKB(geom))")) \
    .withColumn("bbox_min_y", expr("ST_YMin(ST_GeomFromWKB(geom))")) \
    .withColumn("bbox_max_y", expr("ST_YMax(ST_GeomFromWKB(geom))"))

df_with_bbox.write \
    .mode("overwrite") \
    .format("delta") \
    .save("s3://lakehouse/parcels_spatial")
```

### 3. Z-Ordering for Multi-Dimensional Locality
Apply Z-ordering on envelope columns to cluster spatially adjacent records within files. This maximizes data skipping effectiveness.
```sql
-- Delta Lake
OPTIMIZE parcels_spatial ZORDER BY (bbox_min_x, bbox_max_x, bbox_min_y, bbox_max_y);

-- Iceberg (via stored procedure)
CALL spark_catalog.system.rewrite_data_files(
  table => 'default.parcels_spatial',
  strategy => 'sort',
  sort_order => 'bbox_min_x ASC, bbox_min_y ASC, bbox_max_x ASC, bbox_max_y ASC'
);
```

## Query Engine Integration & Optimizer Tuning

Pushdown requires explicit optimizer configuration. The query engine must be instructed to collect and utilize min/max statistics for range predicates.

### Spark Configuration
```properties
spark.sql.optimizer.dynamicPartitionPruning.enabled=true
spark.sql.parquet.filterPushdown=true
spark.sql.adaptive.enabled=true
spark.sql.files.maxPartitionBytes=134217728
```

For Delta, statistics collection is automatic during write. Ensure `delta.dataSkippingNumIndexedCols` covers all envelope columns (default is 32, which is sufficient for 4 bbox columns).

### Query Pattern for Guaranteed Pushdown
Avoid wrapping envelope columns in UDFs. Use direct numeric comparisons that the optimizer can translate to `PushedFilters`.
```sql
SELECT parcel_id, geom
FROM parcels_spatial
WHERE bbox_min_x <= 100.5
  AND bbox_max_x >= 98.2
  AND bbox_min_y <= 45.1
  AND bbox_max_y >= 43.8
  AND ST_Intersects(
        ST_GeomFromWKB(geom),
        ST_GeomFromText('POLYGON((98.2 43.8, 100.5 43.8, 100.5 45.1, 98.2 45.1, 98.2 43.8))')
      );
```

## Debugging: Validating Pushdown & Resolving Failures

Pushdown failures manifest as full table scans despite spatial filters. Validate and resolve using the following steps.

### Step 1: Verify Physical Plan Pushdown
Run `EXPLAIN FORMATTED` and inspect the `PushedFilters` array in the `FileScan` node.
```sql
EXPLAIN FORMATTED SELECT * FROM parcels_spatial WHERE bbox_min_x <= 100.5 AND bbox_max_x >= 98.2;
```
**Expected Output:**
```
PushedFilters: [IsNotNull(bbox_min_x), LessThanOrEqual(bbox_min_x,100.5), GreaterThanOrEqual(bbox_max_x,98.2)]
```
If `PushedFilters: []` appears, the optimizer cannot map the predicate to column statistics.

### Step 2: Diagnose Missing Statistics
Verify metadata contains accurate bounds:
```sql
-- Iceberg
SELECT file_path, record_count, lower_bounds, upper_bounds
FROM default.parcels_spatial.files
LIMIT 10;

-- Delta
DESCRIBE DETAIL parcels_spatial;
```
**Resolution:** If `lower_bounds`/`upper_bounds` are null for bbox columns, re-run `OPTIMIZE` or `rewrite_data_files` with stats collection enabled.

### Step 3: Resolve UDF Opacity
If `ST_Intersects` blocks pushdown, isolate the envelope filter as a pre-scan step. The engine will skip files first, then apply the exact geometry evaluation only on the pruned dataset.
```sql
WITH pruned AS (
  SELECT * FROM parcels_spatial
  WHERE bbox_min_x <= 100.5 AND bbox_max_x >= 98.2
    AND bbox_min_y <= 45.1  AND bbox_max_y >= 43.8
)
SELECT * FROM pruned
WHERE ST_Intersects(
  ST_GeomFromWKB(geom),
  ST_GeomFromText('POLYGON(...)')
);
```

### Step 4: Address Manifest/Transaction Log Bloat
Aggressive Z-ordering or frequent small writes inflate metadata, slowing manifest parsing.
- **Delta:** Run `VACUUM parcels_spatial RETAIN 168 HOURS;` and then `OPTIMIZE parcels_spatial;`
- **Iceberg:** Execute `CALL spark_catalog.system.rewrite_manifests('default.parcels_spatial');`

Monitor manifest size. Target fewer than 500MB of manifest files per partition to maintain sub-second planning latency.

## Production Validation Checklist
1. Envelope columns are `DOUBLE` type and populated during write.
2. `EXPLAIN FORMATTED` confirms `PushedFilters` on all four bbox columns.
3. File scan metrics show `Files Read << Total Files`.
4. Executor memory utilization drops below 60% during spatial joins.
5. Manifest parsing latency < 2s for tables > 100k files.

Implementing these configurations eliminates compute-side geometry materialization. By translating spatial predicates into numeric range filters and aligning physical layout with query patterns, lakehouse architectures achieve deterministic sub-second latency on multi-terabyte GIS datasets.

## The Latency Budget, Broken Down

"Faster" is not a useful target. Breaking the latency of a spatial query into its constituent parts makes it obvious which optimisation is worth doing and which is noise.

<figure class="diagram">
<svg viewBox="0 0 765 288" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Latency breakdown of the same spatial query in two configurations, showing planning, storage requests, decode and predicate evaluation before and after materialising bounding box columns and sorting the table">
<rect x="0" y="0" width="765" height="288" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Same query, same result, two layouts</text>
<text x="72" y="72" font-family="sans-serif" font-size="12" font-weight="700" fill="#9a5a17">before: 42 s</text>
<rect x="72" y="84" width="30" height="34" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="1.5"/>
<rect x="102" y="84" width="290" height="34" fill="#f2e8da" stroke="#9a5a17" stroke-width="1.5"/>
<rect x="392" y="84" width="200" height="34" fill="#e6f0ea" stroke="#2f6e49" stroke-width="1.5"/>
<rect x="592" y="84" width="120" height="34" fill="#faf8fc" stroke="#6a3d9a" stroke-width="1.5"/>
<text x="87" y="140" text-anchor="middle" font-family="sans-serif" font-size="10" fill="#0d3b45">plan</text>
<text x="247" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">storage reads</text>
<text x="492" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">WKB decode</text>
<text x="652" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">predicate</text>
<text x="72" y="196" font-family="sans-serif" font-size="12" font-weight="700" fill="#2f6e49">after: 1.6 s</text>
<rect x="72" y="208" width="34" height="34" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="1.5"/>
<rect x="106" y="208" width="66" height="34" fill="#f2e8da" stroke="#9a5a17" stroke-width="1.5"/>
<rect x="172" y="208" width="42" height="34" fill="#e6f0ea" stroke="#2f6e49" stroke-width="1.5"/>
<rect x="214" y="208" width="26" height="34" fill="#faf8fc" stroke="#6a3d9a" stroke-width="1.5"/>
<text x="390" y="232" font-family="sans-serif" font-size="11" fill="#3d5a63">the bars are to the same scale — the work itself disappeared</text>
<text x="390" y="272" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">Planning grew slightly; everything downstream of it collapsed</text>
</svg>
</figure>

Two things in this breakdown are worth noticing. First, **planning time increases** when pushdown works, because the planner is now doing real work evaluating statistics against a predicate. That increase is trivial in absolute terms and is the price of everything else falling away — but it means "total planning time went up" is not, on its own, a regression.

Second, decode and predicate evaluation shrink **proportionally** to the rows that survive, not to the bytes skipped. That is why materialising bounding-box columns helps even on a table that is already well partitioned: partition pruning removes files, and the numeric predicate removes rows within the files that remain, and the two compound.

The practical target is that storage reads should dominate the profile of a well-tuned spatial query, and they should be small. A profile where decode dominates means rows are being decoded that a numeric predicate could have eliminated. A profile where planning dominates means too many partitions. A profile where the exact predicate dominates means the candidate set is too large, which is a join-strategy problem rather than a layout one.

## Reading the Profile in Practice

<figure class="diagram">
<svg viewBox="0 0 766 254" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Diagnostic table mapping the dominant phase of a query profile to its likely cause and the corresponding fix">
<rect x="0" y="0" width="766" height="254" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Dominant phase &#8594; likely cause &#8594; fix</text>
<rect x="26" y="52" width="200" height="34" rx="6" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="1.5"/>
<text x="126" y="75" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">dominant phase</text>
<rect x="234" y="52" width="250" height="34" rx="6" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="1.5"/>
<text x="359" y="75" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">cause</text>
<rect x="492" y="52" width="262" height="34" rx="6" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="1.5"/>
<text x="623" y="75" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">fix</text>
<text x="36" y="114" font-family="sans-serif" font-size="12" fill="#0d3b45">planning</text>
<text x="244" y="114" font-family="sans-serif" font-size="12" fill="#33707d">too many partitions</text>
<text x="502" y="114" font-family="sans-serif" font-size="12" fill="#2f6e49">coarsen the partition key</text>
<line x1="26" y1="128" x2="754" y2="128" stroke="#cfe3e7" stroke-width="1.5"/>
<text x="36" y="156" font-family="sans-serif" font-size="12" fill="#0d3b45">storage reads</text>
<text x="244" y="156" font-family="sans-serif" font-size="12" fill="#33707d">files not pruned</text>
<text x="502" y="156" font-family="sans-serif" font-size="12" fill="#2f6e49">add the bbox predicate</text>
<line x1="26" y1="170" x2="754" y2="170" stroke="#cfe3e7" stroke-width="1.5"/>
<text x="36" y="198" font-family="sans-serif" font-size="12" fill="#0d3b45">decode</text>
<text x="244" y="198" font-family="sans-serif" font-size="12" fill="#33707d">rows survive that should not</text>
<text x="502" y="198" font-family="sans-serif" font-size="12" fill="#2f6e49">sort the files; check statistics</text>
<line x1="26" y1="212" x2="754" y2="212" stroke="#cfe3e7" stroke-width="1.5"/>
<text x="36" y="238" font-family="sans-serif" font-size="12" fill="#0d3b45">exact predicate</text>
<text x="244" y="238" font-family="sans-serif" font-size="12" fill="#33707d">candidate set too large</text>
<text x="502" y="238" font-family="sans-serif" font-size="12" fill="#2f6e49">change the join strategy</text>
</svg>
</figure>

Work the table top to bottom. Each row's fix is cheaper than the one below it, and fixing a lower row while an upper one is still dominant produces an improvement too small to measure — which is the usual reason a tuning effort feels unproductive.

## What Pushdown Cannot Fix

<figure class="diagram">
<svg viewBox="0 0 764 202" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three query shapes that pushdown cannot accelerate: an aggregation over the whole extent, a join whose candidate set is genuinely large, and a query whose selectivity is low because the data really is everywhere the filter looks">
<rect x="0" y="0" width="764" height="202" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Three cases where the answer is not more pruning</text>
<rect x="26" y="58" width="230" height="132" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="141" y="86" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">global aggregation</text>
<text x="141" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">&#8220;count by region, worldwide&#8221;</text>
<text x="141" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">every file is genuinely needed</text>
<text x="141" y="162" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">fix: pre-aggregate</text>
<rect x="274" y="58" width="230" height="132" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="389" y="86" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">large genuine candidate set</text>
<text x="389" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">dense points, dense polygons</text>
<text x="389" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">the pairs really do overlap</text>
<text x="389" y="162" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">fix: join strategy, more compute</text>
<rect x="522" y="58" width="230" height="132" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="637" y="86" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">low selectivity by nature</text>
<text x="637" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">the window covers most data</text>
<text x="637" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">nothing to skip</text>
<text x="637" y="162" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">fix: narrow the question</text>
</svg>
</figure>

Recognising these early saves a lot of wasted tuning. A query that legitimately needs most of the table is not a pushdown failure, and no layout change will help it — the answer is either a pre-computed aggregate, more compute, or a conversation about whether the question can be narrowed. The distinguishing test is simple: compare the bytes read against the bytes the answer actually depends on. When the ratio is close to one, the query is already optimal and the remaining cost is real work.

## Summary

Pushdown for spatial queries is not a feature to enable; it is a property that emerges when three things are true at once. The table carries numeric columns that describe where each row is. Those columns have statistics the planner can read. And the query mentions them, so the planner has something to compare against. Remove any one and the mechanism disappears silently, leaving a query that returns correct answers at full-scan cost. The measurements described above exist to tell you which of the three has gone missing, and the ordering of the fixes exists because working them out of order produces improvements too small to notice. Everything else — engine selection, cluster sizing, cache warming — moves the numbers by tens of percent, while this moves them by orders of magnitude.
 Do the layout work first, every time, and measure it before touching anything else.

For the layout mechanics behind each of these fixes, see [spatial partitioning schemes](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/spatial-partitioning-schemes/) for key selection and [Z-ordering for geospatial queries](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/z-ordering-for-geospatial-queries/) for the sort order that makes within-file statistics useful.

Both are prerequisites rather than alternatives: the sort order is what makes file statistics tight enough to be worth consulting, and the partition key is what keeps the number of files small enough for the planner to consult them quickly.

Neither one alone produces the collapse in latency shown above; together they routinely do.
