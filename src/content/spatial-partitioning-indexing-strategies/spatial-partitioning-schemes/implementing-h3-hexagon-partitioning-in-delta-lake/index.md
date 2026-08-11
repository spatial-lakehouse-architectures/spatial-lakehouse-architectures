# Implementing H3 Hexagon Partitioning in Delta Lake

High-frequency spatial telemetry, mobility grids, and raster tile streams consistently degrade in Delta Lake deployments when partitioned directly by fine-grained H3 identifiers. The deterministic failure mode involves transaction log bloat, small-file proliferation, and complete predicate pushdown failure. This guide provides a production-grade configuration workflow to align H3 spatial locality with Delta Lake's storage engine, query optimizer, and metadata lifecycle.

## 1. Partition Architecture & Storage Alignment

Direct partitioning by `h3_index` at resolution 8 or 9 generates ~86 million or ~691 million potential directory paths, respectively. Delta's metadata engine cannot sustain this cardinality: each micro-batch appends thousands of partition entries, inflating `_delta_log` size, degrading checkpoint throughput, and forcing the Catalyst planner into full table scans. The root cause is Delta's lack of native spatial type awareness; string H3 partitions do not map to spatial bounding box filters without explicit statistical correlation.

The production fix decouples physical storage layout from logical spatial indexing:
- **Physical Partition Key:** `h3_parent_res5` (or res 6). Coarse hexagons limit directory cardinality to approximately 2 million (res 5) or 14 million (res 6) globally, of which only a fraction will be populated for any given dataset.
- **Logical Sort Key:** `ZORDER BY h3_index` within partitions. Preserves spatial locality while enabling Delta's data skipping engine to prune files via min/max statistics on the H3 integer value.
- **Statistical Columns:** Materialize `h3_min_lat`, `h3_max_lat`, `h3_min_lon`, `h3_max_lon` as native `DOUBLE` columns. Unlike legacy [Spatial Partitioning Schemes](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/spatial-partitioning-schemes/) that force rigid directory trees, this hybrid layout allows the query planner to skip irrelevant hex clusters at the partition level and prune individual Parquet files via columnar statistics.

## 2. Production Write Pipeline

Deploy the table with explicit Delta properties to control auto-optimization, checkpoint frequency, and data skipping scope. H3 indices must be stored as `BIGINT` (64-bit signed integer) for efficient Z-ORDER statistics. The h3-py library (version 4.x) represents H3 cell IDs as strings by default; convert to integer using `h3.str_to_int()`.

```sql
CREATE TABLE IF NOT EXISTS mobility.h3_telemetry
USING DELTA
PARTITIONED BY (h3_parent_res5)
LOCATION 's3://data-lake/mobility/h3_telemetry/'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.dataSkippingNumIndexedCols' = '8',
  'delta.checkpointInterval' = '10',
  'delta.enableDeletionVectors' = 'true'
);
```

Ingest pipeline (PySpark with h3-py 4.x):
```python
from pyspark.sql.functions import udf, col
from pyspark.sql.types import LongType, StringType
import h3  # h3-py 4.x

@udf(LongType())
def compute_h3_int(lat: float, lon: float, res: int = 9) -> int:
    """Returns H3 cell ID as a 64-bit integer for efficient Z-ORDER statistics."""
    cell_str = h3.latlng_to_cell(lat, lon, res)
    return h3.str_to_int(cell_str)

@udf(StringType())
def compute_h3_parent_str(lat: float, lon: float, child_res: int = 9, parent_res: int = 5) -> str:
    """Returns coarse H3 parent cell as a string for partition column."""
    child = h3.latlng_to_cell(lat, lon, child_res)
    return h3.cell_to_parent(child, parent_res)

# Precompute bounds via static lookup table for performance
bounds_df = spark.read.parquet("s3://ref-data/h3_bounds_res9.parquet")

(ingest_df
  .withColumn("h3_index", compute_h3_int(col("lat"), col("lon")))
  .withColumn("h3_parent_res5", compute_h3_parent_str(col("lat"), col("lon")))
  .join(bounds_df, "h3_index", "left")
  .select(
    col("event_id"), col("ts"), col("h3_index"), col("h3_parent_res5"),
    col("min_lat"), col("max_lat"), col("min_lon"), col("max_lon")
  )
  .write
  .format("delta")
  .partitionBy("h3_parent_res5")
  .mode("append")
  .save("s3://data-lake/mobility/h3_telemetry/")
)
```

Execute targeted Z-ORDER immediately after batch ingestion to align file layouts:
```sql
OPTIMIZE mobility.h3_telemetry
ZORDER BY (h3_index)
WHERE h3_parent_res5 IN (
  SELECT DISTINCT h3_parent_res5 FROM mobility.h3_telemetry
  WHERE ts >= current_date() - INTERVAL '1' DAY
);
```

## 3. Predicate Pushdown & Query Optimization

Delta Lake relies on min/max column statistics stored in Parquet footers and the transaction log. Spatial predicates must be rewritten to leverage these statistics. A raw `ST_Contains` UDF cannot be pushed down; instead, filter on the precomputed bounds and H3 integer range.

```sql
EXPLAIN (COST, FORMATTED)
SELECT * FROM mobility.h3_telemetry
WHERE h3_index BETWEEN 599686042433355775 AND 599686042433355800
  AND min_lat <= 37.7749 AND max_lat >= 37.7749
  AND min_lon <= -122.4194 AND max_lon >= -122.4194
  AND ts >= '2024-10-01T00:00:00Z';
```

Verify the execution plan for `PartitionFilters` and `DataFilters`. Successful pushdown will show:
- `PartitionFilters: [h3_parent_res5 = '85283473fffffff']` (coarse hex string)
- `DataFilters: [h3_index >= ..., h3_index <= ..., min_lat <= ..., max_lat >= ...]`

Ensure `spark.sql.adaptive.enabled` is `true` and `delta.autoOptimize.optimizeWrite` is active to prevent small-file generation during streaming micro-batches. Reference the official [Delta Lake Z-ORDER documentation](https://docs.delta.io/latest/optimizations-oss.html#z-ordering-multi-dimensional-clustering) for hash distribution parameters.

## 4. Compaction, Metadata & Vacuum Cycles

H3 workloads generate high write amplification. Configure automated compaction and metadata cleanup to maintain query latency:

```sql
-- Targeted compaction for a specific partition
OPTIMIZE mobility.h3_telemetry
WHERE h3_parent_res5 = '85283473fffffff'
ZORDER BY (h3_index);

-- Metadata cleanup (retain 7 days for time-travel)
VACUUM mobility.h3_telemetry RETAIN 168 HOURS;
```

Set Spark session properties to control file sizing and checkpoint overhead:
```properties
spark.databricks.delta.optimizeWrite.enabled=true
spark.databricks.delta.autoCompact.enabled=true
spark.sql.files.maxPartitionBytes=268435456
spark.databricks.delta.checkpointInterval=10
spark.sql.adaptive.coalescePartitions.enabled=true
```

## 5. Debugging Pushdown Failures & File Skew

**Failure Mode:** Query planner ignores spatial filters, triggering full table scans despite correct partitioning.
**Diagnosis Steps:**
1. Run `DESCRIBE DETAIL mobility.h3_telemetry` to verify `minValues` and `maxValues` are populated for `h3_index` and bound columns.
2. Check `_delta_log` size. If >5GB, checkpoint frequency is too low or partition cardinality is unbounded.
3. Verify `h3_index` data type. String storage breaks Z-ORDER statistics. The `BIGINT` cast is required.

**Resolution:**
```sql
-- Force statistics recomputation after schema fix
OPTIMIZE mobility.h3_telemetry ZORDER BY (h3_index);
```

**File Skew Mitigation:** High-traffic hexagons (e.g., urban cores) generate disproportionate file counts. Apply salting for extreme hotspots:
```python
from pyspark.sql.functions import col, expr

# Add salt column for skewed partitions (100 sub-buckets per parent hex)
df.withColumn("h3_salt", (col("h3_index") % 100).cast("int")) \
  .write \
  .format("delta") \
  .partitionBy("h3_parent_res5", "h3_salt") \
  .mode("append") \
  .save("s3://data-lake/mobility/h3_telemetry_salted/")
```

Monitor skew via `SELECT h3_parent_res5, COUNT(*) AS row_count FROM mobility.h3_telemetry GROUP BY 1 ORDER BY 2 DESC LIMIT 10`. Rebalance partitions exceeding 2x the median row count using targeted `OPTIMIZE ... ZORDER BY (h3_index, h3_salt)`.

For authoritative reference on H3 cell hierarchy and resolution scaling, consult the [H3 Core Library Overview](https://h3geo.org/docs/core-library/overview). Validate query execution plans using Apache Spark's [EXPLAIN Syntax](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-explain.html) to confirm predicate propagation.

## Why Hexagons Behave Differently From Squares

The choice of a hexagonal grid over a square one is usually justified with a claim about neighbour distances, and the claim is true but incomplete. Three properties matter for a partitioned table.

<figure class="diagram">
<svg viewBox="0 0 721 272" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Comparison of a square grid cell with eight neighbours at two different distances against a hexagonal cell with six neighbours all equidistant, and the consequence for expanding a query window into a cell list">
<rect x="0" y="0" width="721" height="272" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Neighbour geometry, and why it shows up in query plans</text>
<text x="196" y="62" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#9a5a17">square: 8 neighbours, 2 distances</text>
<rect x="146" y="78" width="50" height="50" fill="#f2e8da" stroke="#9a5a17" stroke-width="1.5"/>
<rect x="196" y="78" width="50" height="50" fill="#f2e8da" stroke="#9a5a17" stroke-width="1.5"/>
<rect x="246" y="78" width="50" height="50" fill="#f2e8da" stroke="#9a5a17" stroke-width="1.5"/>
<rect x="146" y="128" width="50" height="50" fill="#f2e8da" stroke="#9a5a17" stroke-width="1.5"/>
<rect x="196" y="128" width="50" height="50" fill="#eaddc8" stroke="#9a5a17" stroke-width="2.5"/>
<rect x="246" y="128" width="50" height="50" fill="#f2e8da" stroke="#9a5a17" stroke-width="1.5"/>
<rect x="146" y="178" width="50" height="50" fill="#f2e8da" stroke="#9a5a17" stroke-width="1.5"/>
<rect x="196" y="178" width="50" height="50" fill="#f2e8da" stroke="#9a5a17" stroke-width="1.5"/>
<rect x="246" y="178" width="50" height="50" fill="#f2e8da" stroke="#9a5a17" stroke-width="1.5"/>
<text x="221" y="256" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">edge neighbours are closer than corner ones</text>
<text x="584" y="62" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#2f6e49">hexagon: 6 neighbours, 1 distance</text>
<path d="M584 100 l30 17 v34 l-30 17 l-30 -17 v-34 z" fill="#d7e8de" stroke="#2f6e49" stroke-width="2.5"/>
<path d="M584 32 l30 17 v34 l-30 17 l-30 -17 v-34 z" fill="#e6f0ea" stroke="#2f6e49" stroke-width="1.5" transform="translate(0,68)"/>
<path d="M644 134 l30 17 v34 l-30 17 l-30 -17 v-34 z" fill="#e6f0ea" stroke="#2f6e49" stroke-width="1.5"/>
<path d="M524 134 l30 17 v34 l-30 17 l-30 -17 v-34 z" fill="#e6f0ea" stroke="#2f6e49" stroke-width="1.5"/>
<path d="M644 66 l30 17 v34 l-30 17 l-30 -17 v-34 z" fill="#e6f0ea" stroke="#2f6e49" stroke-width="1.5"/>
<path d="M524 66 l30 17 v34 l-30 17 l-30 -17 v-34 z" fill="#e6f0ea" stroke="#2f6e49" stroke-width="1.5"/>
<path d="M584 168 l30 17 v34 l-30 17 l-30 -17 v-34 z" fill="#e6f0ea" stroke="#2f6e49" stroke-width="1.5"/>
<text x="584" y="256" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">every neighbour is the same distance away</text>
</svg>
</figure>

The uniform neighbour distance matters for **radius queries**. Expanding a circular window into a set of cells is a ring expansion, and with hexagons a ring of radius *k* is a well-defined set whose members are all roughly equidistant from the centre. With squares, the same expansion produces a set whose corner members are 40% further away, so either the cell set is larger than the radius requires or the coverage is uneven. On a partitioned table this shows up directly as extra partitions read.

The second property is **area consistency**. Hexagonal cells at a given resolution vary in area much less than square cells of fixed angular size, whose ground area shrinks with the cosine of latitude. For a dataset spanning a wide latitude range, that alone can be the difference between partitions within a factor of two and partitions varying by a factor of five.

The third property cuts the other way and deserves the same emphasis. **Hexagons do not nest exactly.** A cell at resolution 6 is not the union of seven cells at resolution 7; the children overlap the parent's edges. That means a mixed-resolution layout needs care — containment is approximate at boundaries — and it means aggregating a fine-resolution count to a coarse cell is an approximation rather than an identity. For counting and partitioning this is harmless; for exact area-weighted aggregation it is not, and a square or triangular grid that nests exactly is the better choice there.

## Practical Delta Considerations

Two details of Delta specifically affect a hexagonal layout, and both are easy to get wrong at table creation.

The cell identifier must be a `BIGINT` and it must be inside the statistics window. H3 identifiers are 64-bit values, and storing them as strings — which the common Python helpers return by default — both inflates the column and makes range predicates useless. Convert explicitly at write time, and place the column early in the schema so it falls inside `dataSkippingNumIndexedCols`.

Partitioning directly on a fine-resolution cell will produce a directory explosion. A resolution-8 partition over a country is hundreds of thousands of directories, and Delta's Hive-style layout puts each in its own path. The pattern that works is to partition on a coarse cell — resolution 4 or 5 — and to keep the fine cell as an ordinary clustered column, so pruning happens at two levels without the directory count becoming unmanageable.

```sql
-- Delta 3.x. Coarse cell partitions the table; the fine cell clusters within it.
CREATE TABLE lakehouse.spatial.rides (
  ride_id     BIGINT,
  event_ts    TIMESTAMP,
  h3_r5       BIGINT,        -- partition: a few thousand distinct values
  h3_r9       BIGINT,        -- clustered: high cardinality, prunes within files
  bbox_min_x  DOUBLE, bbox_min_y DOUBLE,
  bbox_max_x  DOUBLE, bbox_max_y DOUBLE,
  geom_wkb    BINARY
) USING DELTA
PARTITIONED BY (h3_r5);

OPTIMIZE lakehouse.spatial.rides ZORDER BY (h3_r9, event_ts);
```

Verify both levels are working before declaring the layout done: a query filtering only `h3_r5` should touch a handful of directories, and one adding `h3_r9` should read materially fewer files within them. If the second filter changes nothing, the clustering has not been applied or has decayed since the last `OPTIMIZE`.

## Handling Cells That Straddle the Data

Two edge cases show up on every hexagonal deployment and both have tidy answers.

<figure class="diagram">
<svg viewBox="0 0 762 250" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Two edge cases for hexagonal partitioning: a polygon overlapping several cells which must either be assigned once or duplicated, and the pentagon cells that appear at the twelve icosahedron vertices">
<rect x="0" y="0" width="762" height="250" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Two cases every hexagonal layout meets</text>
<rect x="30" y="58" width="352" height="180" rx="8" fill="#ffffff" stroke="#0e6e7d" stroke-width="2"/>
<text x="206" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">a polygon spanning cells</text>
<path d="M150 120 l26 15 v30 l-26 15 l-26 -15 v-30 z" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="1.5"/>
<path d="M202 120 l26 15 v30 l-26 15 l-26 -15 v-30 z" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="1.5"/>
<path d="M254 120 l26 15 v30 l-26 15 l-26 -15 v-30 z" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="1.5"/>
<path d="M170 130 L262 138 L250 176 L164 168 Z" fill="none" stroke="#9a5a17" stroke-width="2.5"/>
<text x="206" y="208" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">assign by centroid, or duplicate per cell</text>
<text x="206" y="226" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">centroid keeps counts exact; duplication keeps pruning exact</text>
<rect x="398" y="58" width="352" height="180" rx="8" fill="#ffffff" stroke="#6a3d9a" stroke-width="2"/>
<text x="574" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">the twelve pentagons</text>
<path d="M574 118 l30 22 l-11 35 h-38 l-11 -35 z" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2.5"/>
<text x="574" y="208" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">smaller area, different neighbour count</text>
<text x="574" y="226" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">mostly in ocean, but assume nothing</text>
</svg>
</figure>

For **polygons spanning several cells**, the choice is between assigning each feature to one cell by its centroid, or writing one row per overlapping cell. Centroid assignment keeps row counts honest and makes aggregation straightforward, at the cost that a query scoped to a cell can miss a polygon whose centroid sits next door but whose body extends into the window. Duplication makes pruning exact and requires every consumer to deduplicate. For reference boundaries queried by containment, duplication with an explicit `is_primary` flag is usually the better trade; for point-like features the question does not arise.

The **pentagons** are a genuine property of the system rather than an implementation quirk: mapping an icosahedron onto a sphere leaves twelve vertices where a hexagon cannot close, and those cells are pentagonal with a smaller area and five neighbours instead of six. They are deliberately positioned over ocean, which means most datasets never touch one — and a global dataset will. Code that assumes six neighbours will produce a subtly wrong ring expansion there, so use the library's neighbour function rather than an arithmetic shortcut, and include a pentagon cell in the test fixtures so the assumption is exercised.

## Verifying the Layout After the First Load

<figure class="diagram">
<svg viewBox="0 0 764 202" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three post-load checks for a hexagonal Delta table: partition directory count, distribution of rows across cells, and whether a cell scoped query prunes to a small number of files">
<rect x="0" y="0" width="764" height="202" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Three checks before calling the layout done</text>
<rect x="26" y="58" width="230" height="132" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="141" y="86" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">directory count</text>
<text x="141" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">distinct h3_r5 values</text>
<text x="141" y="138" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">expect thousands,</text>
<text x="141" y="158" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">not hundreds of thousands</text>
<rect x="274" y="58" width="230" height="132" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="389" y="86" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">row distribution</text>
<text x="389" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">max ÷ median rows per cell</text>
<text x="389" y="138" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">under 4× is healthy,</text>
<text x="389" y="158" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">over 10× needs splitting</text>
<rect x="522" y="58" width="230" height="132" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="637" y="86" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">pruning proof</text>
<text x="637" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">files scanned ÷ files total</text>
<text x="637" y="138" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">a cell-scoped query</text>
<text x="637" y="158" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">should read under 2%</text>
</svg>
</figure>

All three are single queries against metadata, and running them on the first day of a new table catches the mistakes that otherwise surface months later as unexplained slowness. Record the numbers somewhere durable, because the useful signal is not the value on day one but the drift from it.
