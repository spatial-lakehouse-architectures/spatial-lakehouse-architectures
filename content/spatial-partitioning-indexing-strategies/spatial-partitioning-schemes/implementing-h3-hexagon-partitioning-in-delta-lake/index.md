# Implementing H3 Hexagon Partitioning in Delta Lake

High-frequency spatial telemetry, mobility grids, and raster tile streams consistently degrade in Delta Lake deployments when partitioned directly by fine-grained H3 identifiers. The deterministic failure mode involves transaction log bloat, small-file proliferation, and complete predicate pushdown failure. This guide provides a production-grade configuration workflow to align H3 spatial locality with Delta Lake’s storage engine, query optimizer, and metadata lifecycle.

## 1. Partition Architecture & Storage Alignment

Direct partitioning by `h3_index` at resolution 8 or 9 generates ~1.5 billion potential directory paths. Delta’s metadata engine cannot sustain this cardinality: each micro-batch appends thousands of partition entries, inflating `_delta_log` size, degrading checkpoint throughput, and forcing the Catalyst planner into full table scans. The root cause is Delta’s lack of native spatial type awareness; string or integer H3 partitions do not map to spatial bounding box filters without explicit statistical correlation.

The production fix decouples physical storage layout from logical spatial indexing:
- **Physical Partition Key:** `h3_parent_res5` (or res 6). Coarse hexagons align with cloud object storage block boundaries (~128–256MB), limiting directory cardinality to ~1.2M (res 5) or ~22M (res 6).
- **Logical Sort Key:** `ZORDER BY h3_index_int` within partitions. Preserves spatial locality while enabling Delta’s data skipping engine to prune files via min/max statistics.
- **Statistical Columns:** Materialize `h3_min_lat`, `h3_max_lat`, `h3_min_lon`, `h3_max_lon` as native `DOUBLE` columns. Unlike legacy [Spatial Partitioning Schemes](/spatial-partitioning-indexing-strategies/spatial-partitioning-schemes/) that force rigid directory trees, this hybrid layout allows the query planner to skip irrelevant hex clusters at the partition level and prune individual Parquet files via columnar statistics.

## 2. Production Write Pipeline

Deploy the table with explicit Delta properties to control auto-optimization, checkpoint frequency, and data skipping scope. H3 indices must be stored as `BIGINT` (64-bit unsigned) for efficient Z-ORDER hashing and statistical computation.

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

Ingest pipeline (PySpark):
```python
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import LongType, DoubleType
import h3

@udf(LongType())
def compute_h3(lat, lon, res=9):
    return h3.latlng_to_cell(lat, lon, res)

@udf(LongType())
def compute_parent(h3_idx, parent_res=5):
    return h3.cell_to_parent(h3_idx, parent_res)

# Precompute bounds via static lookup table for performance
bounds_df = spark.read.parquet("s3://ref-data/h3_bounds_res9.parquet")

(ingest_df
  .withColumn("h3_index", compute_h3(col("lat"), col("lon"), lit(9)))
  .withColumn("h3_parent_res5", compute_parent(col("h3_index"), lit(5)))
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
WHERE h3_parent_res5 IN (SELECT DISTINCT h3_parent_res5 FROM mobility.h3_telemetry WHERE ts >= current_date() - INTERVAL '1' DAY);
```

## 3. Predicate Pushdown & Query Optimization

Delta Lake relies on min/max column statistics stored in Parquet footers and the transaction log. Spatial predicates must be rewritten to leverage these statistics. A raw `ST_Contains` UDF cannot be pushed down; instead, filter on the precomputed bounds and H3 range.

```sql
EXPLAIN (COST, FORMATTED)
SELECT * FROM mobility.h3_telemetry
WHERE h3_index BETWEEN 599686042433355775 AND 599686042433355800
  AND min_lat <= 37.7749 AND max_lat >= 37.7749
  AND min_lon <= -122.4194 AND max_lon >= -122.4194
  AND ts >= '2024-10-01T00:00:00Z';
```

Verify the execution plan for `PartitionFilters` and `DataFilters`. Successful pushdown will show:
- `PartitionFilters: [h3_parent_res5 = 589686042433355775]`
- `DataFilters: [h3_index >= ..., h3_index <= ..., min_lat <= ..., max_lat >= ...]`

Modern [Spatial Partitioning & Indexing Strategies](/spatial-partitioning-indexing-strategies/) mandate explicit column-level statistics for non-native spatial types. Ensure `spark.sql.adaptive.enabled` is `true` and `spark.databricks.delta.optimizeWrite.enabled` is active to prevent small-file generation during streaming micro-batches. Reference the official [Delta Lake Z-ORDER documentation](https://docs.delta.io/latest/optimizations-oss.html#z-ordering) for hash distribution parameters.

## 4. Compaction, Metadata & Vacuum Cycles

H3 workloads generate high write amplification. Configure automated compaction and metadata cleanup to maintain query latency:

```bash
# Targeted compaction for skewed partitions
OPTIMIZE mobility.h3_telemetry
WHERE h3_parent_res5 = '589686042433355775'
ZORDER BY (h3_index);

# Metadata cleanup (retain 7 days for time-travel)
VACUUM mobility.h3_telemetry RETAIN 168 HOURS;
```

Set Spark session properties to control file sizing and checkpoint overhead:
```properties
spark.databricks.delta.optimizeWrite.enabled=true
spark.databricks.delta.autoCompact.enabled=true
spark.sql.files.maxPartitionBytes=268435456  # 256MB
spark.databricks.delta.checkpointInterval=10
spark.sql.adaptive.coalescePartitions.enabled=true
```

## 5. Debugging Pushdown Failures & File Skew

**Failure Mode:** Query planner ignores spatial filters, triggering full table scans despite correct partitioning.
**Diagnosis Steps:**
1. Run `DESCRIBE DETAIL mobility.h3_telemetry` to verify `minValues` and `maxValues` are populated for `h3_index` and bound columns.
2. Check `_delta_log` size. If >5GB, checkpoint frequency is too low or partition cardinality is unbounded.
3. Verify `h3_index` data type. String storage breaks Z-ORDER hashing and statistical min/max computation. Cast to `BIGINT`.

**Resolution:**
```sql
-- Force statistics recomputation
OPTIMIZE mobility.h3_telemetry ZORDER BY (h3_index);

-- If stats are missing, rewrite affected partitions
INSERT OVERWRITE mobility.h3_telemetry PARTITION (h3_parent_res5)
SELECT * FROM mobility.h3_telemetry WHERE h3_parent_res5 = '589686042433355775';
```

**File Skew Mitigation:** High-traffic hexagons (e.g., urban cores) generate disproportionate file counts. Apply dynamic partition pruning and salting for extreme hotspots:
```python
# Add salt column for skewed partitions
df.withColumn("h3_salt", (col("h3_index") % 100))
  .partitionBy("h3_parent_res5", "h3_salt")
```
Monitor skew via `SELECT h3_parent_res5, COUNT(*) AS file_count FROM mobility.h3_telemetry GROUP BY 1 ORDER BY 2 DESC LIMIT 10`. Rebalance partitions exceeding 2x the median file count using targeted `OPTIMIZE` with `ZORDER BY (h3_index, h3_salt)`.

For authoritative reference on H3 cell hierarchy and resolution scaling, consult the [H3 Core Library Overview](https://h3geo.org/docs/core-library/overview). Validate query execution plans using Apache Spark’s [EXPLAIN Syntax](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-explain.html) to confirm predicate propagation.