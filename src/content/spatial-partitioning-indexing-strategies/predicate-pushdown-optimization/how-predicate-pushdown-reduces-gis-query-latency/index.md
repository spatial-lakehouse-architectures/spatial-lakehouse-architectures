# How Predicate Pushdown Reduces GIS Query Latency in Spatial Lakehouse Architectures

In spatial data lakehouse deployments, query latency is predominantly driven by compute-side geometry evaluation. When a query engine receives a geospatial filter such as `ST_Intersects`, `ST_Contains`, or a bounding-box constraint, the default execution path materializes entire Parquet/ORC files into executor memory before applying spatial predicates. On multi-terabyte vector datasets, this triggers excessive network I/O, executor OOM crashes, and query timeouts. The engineering objective is to eliminate full-file materialization by translating spatial predicates into storage-level pruning conditions through systematic [Predicate Pushdown Optimization](/spatial-partitioning-indexing-strategies/predicate-pushdown-optimization/). This shifts geometry evaluation from the compute layer to the metadata and file-scan layer, reducing latency by 80–99% in production workloads.

## Execution Path: Compute-Side vs. Storage-Side Evaluation

Open table formats decouple compute from storage, but spatial UDFs traditionally assume tight coupling. Without pushdown, the query planner treats `ST_Intersects(geom, bbox)` as a post-scan filter. The engine reads every row, deserializes WKB/WKT payloads, constructs in-memory geometry objects, and evaluates spatial relationships row-by-row. This approach routinely consumes 10–15x more I/O than necessary and forces Spark/Trino executors to spill to disk.

Storage-side pushdown intercepts the logical plan during the physical planning phase. The optimizer extracts the query bounding box, translates it into numeric range predicates, and pushes those ranges into Iceberg manifest files or Delta transaction logs. Only data files whose spatial envelopes intersect the query bbox are scheduled for scan. Pushdown efficiency is directly proportional to how well the physical layout aligns with spatial locality, which is why [Spatial Partitioning & Indexing Strategies](/spatial-partitioning-indexing-strategies/) dictate baseline performance.

## Configuration: Schema Design & Metadata Generation

Native spatial functions are opaque to most query optimizers. To enable deterministic pushdown, you must materialize bounding box coordinates as numeric columns and rely on column-level min/max statistics.

### 1. Schema Definition with Envelope Columns
```sql
CREATE TABLE parcels_spatial (
    parcel_id STRING,
    geom BINARY,
    bbox_min_x DOUBLE,
    bbox_max_x DOUBLE,
    bbox_min_y DOUBLE,
    bbox_max_y DOUBLE
) USING DELTA
PARTITIONED BY (region_code);
```

### 2. Ingestion Pipeline (PySpark)
Compute envelope columns during write to ensure statistics are captured at the file level. Disable opaque UDF wrapping in the write path.
```python
from pyspark.sql.functions import expr

df_with_bbox = df.withColumn("bbox_min_x", expr("ST_XMin(geom)")) \
                 .withColumn("bbox_max_x", expr("ST_XMax(geom)")) \
                 .withColumn("bbox_min_y", expr("ST_YMin(geom)")) \
                 .withColumn("bbox_max_y", expr("ST_YMax(geom)"))

df_with_bbox.write \
    .mode("overwrite") \
    .format("delta") \
    .option("delta.dataSkippingNumIndexedCols", "4") \
    .save("s3://lakehouse/parcels_spatial")
```

### 3. Z-Ordering for Multi-Dimensional Locality
Apply Z-ordering on envelope columns to cluster spatially adjacent records within files. This maximizes data skipping effectiveness.
```sql
-- Delta Lake
OPTIMIZE parcels_spatial ZORDER BY (bbox_min_x, bbox_max_x, bbox_min_y, bbox_max_y);

-- Iceberg (via Spark)
CALL spark_catalog.system.rewrite_data_files(
  table => 'default.parcels_spatial',
  strategy => 'sort',
  sort_order => 'bbox_min_x, bbox_max_x, bbox_min_y, bbox_max_y'
);
```

## Query Engine Integration & Optimizer Tuning

Pushdown requires explicit optimizer configuration. The query engine must be instructed to collect and utilize min/max statistics for range predicates.

### Spark Configuration
```properties
spark.sql.optimizer.dynamicPartitionPruning.enabled=true
spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly=false
spark.databricks.delta.stats.collect=true
spark.sql.iceberg.plan-mode=batch
spark.sql.files.maxPartitionBytes=128m
```

### Query Pattern for Guaranteed Pushdown
Avoid wrapping envelope columns in UDFs. Use direct numeric comparisons that the optimizer can translate to `PushedFilters`.
```sql
SELECT parcel_id, geom
FROM parcels_spatial
WHERE bbox_min_x <= 100.5
  AND bbox_max_x >= 98.2
  AND bbox_min_y <= 45.1
  AND bbox_max_y >= 43.8
  AND ST_Intersects(geom, ST_GeomFromText('POLYGON((98.2 43.8, 100.5 43.8, 100.5 45.1, 98.2 45.1, 98.2 43.8))'));
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
Iceberg and Delta require explicit stats collection. Verify metadata:
```sql
-- Iceberg
SELECT file_path, record_count, column_sizes, value_counts, null_value_counts, lower_bounds, upper_bounds
FROM default.parcels_spatial.files;

-- Delta
DESCRIBE DETAIL parcels_spatial;
SHOW TBLPROPERTIES parcels_spatial;
```
**Resolution:** If `lower_bounds`/`upper_bounds` are null, re-run `OPTIMIZE` or `REWRITE DATA FILES` with stats collection enabled. Ensure `delta.dataSkippingNumIndexedCols` covers all envelope columns.

### Step 3: Resolve UDF Opacity
If `ST_Intersects` blocks pushdown, isolate the envelope filter as a pre-scan step. The engine will skip files first, then apply the exact geometry evaluation only on the pruned dataset.
```sql
WITH pruned AS (
  SELECT * FROM parcels_spatial
  WHERE bbox_min_x <= 100.5 AND bbox_max_x >= 98.2
    AND bbox_min_y <= 45.1 AND bbox_max_y >= 43.8
)
SELECT * FROM pruned
WHERE ST_Intersects(geom, ST_GeomFromText('POLYGON(...)'));
```

### Step 4: Address Manifest/Transaction Log Bloat
Aggressive Z-ordering or frequent small writes inflate metadata, slowing manifest parsing.
- **Delta:** Run `VACUUM parcels_spatial RETAIN 168 HOURS;` and `OPTIMIZE parcels_spatial;`
- **Iceberg:** Execute `CALL spark_catalog.system.rewrite_manifests('default.parcels_spatial');`
Monitor manifest size via `spark.sql.iceberg.plan-mode=batch` logs. Target <500MB manifest files per partition.

## Production Validation Checklist
1. Envelope columns are `DOUBLE` type and populated during write.
2. `EXPLAIN` confirms `PushedFilters` on all four bbox columns.
3. File scan metrics show `Files Read << Total Files`.
4. Executor memory utilization drops below 60% during spatial joins.
5. Manifest parsing latency < 2s for tables > 100k files.

Implementing these configurations eliminates compute-side geometry materialization. By translating spatial predicates into numeric range filters and aligning physical layout with query patterns, lakehouse architectures achieve deterministic sub-second latency on multi-terabyte GIS datasets.