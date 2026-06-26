# Implementing H3 Hexagon Partitioning in Delta Lake

High-frequency spatial telemetry, mobility grids, and raster tile streams consistently degrade in Delta Lake deployments when partitioned directly by fine-grained H3 identifiers. The deterministic failure mode involves transaction log bloat, small-file proliferation, and complete predicate pushdown failure. This guide provides a production-grade configuration workflow to align H3 spatial locality with Delta Lake's storage engine, query optimizer, and metadata lifecycle.

## 1. Partition Architecture & Storage Alignment

Direct partitioning by `h3_index` at resolution 8 or 9 generates ~86 million or ~691 million potential directory paths, respectively. Delta's metadata engine cannot sustain this cardinality: each micro-batch appends thousands of partition entries, inflating `_delta_log` size, degrading checkpoint throughput, and forcing the Catalyst planner into full table scans. The root cause is Delta's lack of native spatial type awareness; string H3 partitions do not map to spatial bounding box filters without explicit statistical correlation.

The production fix decouples physical storage layout from logical spatial indexing:
- **Physical Partition Key:** `h3_parent_res5` (or res 6). Coarse hexagons limit directory cardinality to approximately 2 million (res 5) or 14 million (res 6) globally, of which only a fraction will be populated for any given dataset.
- **Logical Sort Key:** `ZORDER BY h3_index` within partitions. Preserves spatial locality while enabling Delta's data skipping engine to prune files via min/max statistics on the H3 integer value.
- **Statistical Columns:** Materialize `h3_min_lat`, `h3_max_lat`, `h3_min_lon`, `h3_max_lon` as native `DOUBLE` columns. Unlike legacy [Spatial Partitioning Schemes](/spatial-partitioning-indexing-strategies/spatial-partitioning-schemes/) that force rigid directory trees, this hybrid layout allows the query planner to skip irrelevant hex clusters at the partition level and prune individual Parquet files via columnar statistics.

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
