# Predicate Pushdown Optimization

Predicate pushdown is the foundational execution strategy that shifts filtering logic from distributed compute engines down to the object storage metadata layer. In spatial data lakehouses built on Apache Iceberg or Delta Lake, this mechanism determines whether a geospatial workload scans terabytes of raw Parquet files or reads only the relevant spatial partitions. As a core execution mechanism within the broader [Spatial Partitioning & Indexing Strategies](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/) framework, pushdown optimization directly dictates query latency, compute cost, and concurrency limits for GIS analytics. Without it, spatial predicates degrade into full-table scans, negating the economic and performance advantages of the lakehouse paradigm.

## Core Mechanics and Spatial Metadata Translation

Spatial predicates (`ST_Intersects`, `ST_Contains`, `ST_DWithin`) introduce execution complexity because geometry types are typically serialized as binary WKB or custom spatial encodings. Traditional relational pushdown relies on scalar column statistics, but spatial engines must translate geometric bounds into file-level metadata filters. When a query specifies a geographic bounding box, the execution planner evaluates min/max coordinate statistics stored in table manifests. If a file's spatial envelope does not overlap the query window, the file is skipped entirely before deserialization.

This early pruning requires strict coordinate reference system (CRS) alignment. Engines evaluate pushdown against the stored CRS (typically `EPSG:4326` for lat/lon). Mismatched CRS values between query predicates and table metadata force full deserialization and runtime reprojection, bypassing pushdown entirely. The planner also handles topology-aware predicates by approximating complex geometries with their minimum bounding rectangles (MBRs) during the initial filter pass, deferring exact geometric computation to the post-filter stage. For authoritative specifications on spatial predicate evaluation and WKB serialization, consult the [OGC Simple Features Access Standard](https://www.ogc.org/publications/standard/sfa/).

## Format-Specific Execution: Iceberg vs. Delta Lake

The effectiveness of spatial pushdown depends heavily on the underlying table format's metadata architecture and data skipping capabilities.

**Apache Iceberg** leverages a hierarchical manifest system where each manifest file stores per-file statistics, including custom spatial bounds when configured via auxiliary metadata columns. Iceberg's hidden partitioning allows engineers to partition by upstream-computed spatial keys (e.g., geohash strings or H3 cell IDs) without exposing those columns in the query schema. The engine evaluates predicates directly against these manifests, enabling aggressive file skipping. Configuration requires `spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions` and careful handling of null geometries. Trade-off: Iceberg's manifest pruning is highly efficient but requires proactive metadata maintenance; stale manifests lead to degraded skip rates.

**Delta Lake** relies on the `_delta_log` and built-in data skipping indexes that automatically track min/max/null counts for all columns. Spatial pushdown in Delta is most effective when combined with multi-dimensional clustering. While Delta does not natively support hidden spatial partition transforms, it compensates through `ZORDER BY` on derived spatial columns (e.g., `bbox_min_x`, `bbox_min_y`). This approach co-locates spatially adjacent records within Parquet row groups, allowing the engine to skip entire blocks during predicate evaluation. For implementation details on Delta's skipping architecture, refer to [Delta Lake Data Skipping Documentation](https://docs.delta.io/latest/delta-batch.html#data-skipping).

When designing partition layouts, engineers must balance granularity with metadata overhead. Over-partitioning creates excessive manifest files, increasing planner latency and diminishing pushdown returns. A comprehensive breakdown of optimal partition boundaries and clustering dimensions is available in the [Spatial Partitioning Schemes](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/spatial-partitioning-schemes/) guide. Additionally, applying [Z-Ordering for Geospatial Queries](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/z-ordering-for-geospatial-queries/) alongside pushdown ensures that spatial predicates hit contiguous Parquet blocks rather than scattered files.

## Production Implementation: Configuration & Validation

The following examples demonstrate production-ready configuration, query execution, and CI validation for spatial predicate pushdown.

### PySpark Configuration & Table Creation
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

spark = SparkSession.builder \
    .appName("SpatialPushdownConfig") \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.parquet.filterPushdown", "true") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Explicit spatial parameters
CRS = "EPSG:4326"
RETENTION_DAYS = 90

# Create Iceberg table with bounding box columns for predicate pushdown
spark.sql("""
    CREATE TABLE IF NOT EXISTS analytics.spatial_events (
        event_id  STRING,
        timestamp TIMESTAMP,
        geometry  BINARY,
        x_min     DOUBLE,
        y_min     DOUBLE,
        x_max     DOUBLE,
        y_max     DOUBLE
    ) USING iceberg
    PARTITIONED BY (bucket(16, event_id))
    LOCATION 's3://lakehouse-prod/analytics/spatial_events'
""")

# Write with explicit spatial bounds extraction
df = spark.read.parquet("s3://ingest-raw/events/")
df.withColumn("x_min", expr("ST_XMin(ST_GeomFromWKB(geometry))")) \
  .withColumn("y_min", expr("ST_YMin(ST_GeomFromWKB(geometry))")) \
  .withColumn("x_max", expr("ST_XMax(ST_GeomFromWKB(geometry))")) \
  .withColumn("y_max", expr("ST_YMax(ST_GeomFromWKB(geometry))")) \
  .writeTo("analytics.spatial_events") \
  .append()
```

### SQL Query with Pushdown Verification
```sql
-- Verify pushdown via EXPLAIN before execution.
-- The bbox filter pushes down to manifest; ST_Intersects runs post-filter.
EXPLAIN FORMATTED
SELECT event_id, timestamp, geometry
FROM analytics.spatial_events
WHERE x_min >= -122.5 AND x_max <= -122.0
  AND y_min >= 37.5  AND y_max <= 38.0
  AND ST_Intersects(
        ST_GeomFromWKB(geometry),
        ST_GeomFromText('POLYGON((-122.5 37.5, -122.0 37.5, -122.0 38.0, -122.5 38.0, -122.5 37.5))', 4326)
      );
```

### CI/CD Validation Pipeline (GitHub Actions)
```yaml
name: Validate Spatial Pushdown
on:
  push:
    branches: [main]
jobs:
  validate-pushdown:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Run Pushdown Validation
        run: |
          pip install pyspark pyiceberg
          python -c "
          from pyspark.sql import SparkSession
          spark = SparkSession.builder \
              .config('spark.sql.extensions',
                      'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions') \
              .getOrCreate()
          plan = spark.sql(
              'EXPLAIN FORMATTED SELECT * FROM analytics.spatial_events '
              'WHERE x_min >= -122.5 AND x_max <= -122.0'
          ).collect()[0][0]
          assert 'PushedFilters' in plan, 'Predicate pushdown failed'
          print('Pushdown validation passed.')
          "
```

## Troubleshooting & Operational Guardrails

Even with correct configuration, spatial pushdown can silently degrade. Use the following diagnostic paths to maintain production reliability:

1. **Stale Manifest Statistics:** Iceberg and Delta rely on accurate min/max bounds. If `rewrite_data_files` or `OPTIMIZE` jobs run infrequently, file-level stats drift from actual geometry distributions. Schedule metadata compaction daily and monitor `num_files_skipped` metrics in query execution logs.
2. **Null Geometry Handling:** Pushdown engines treat `NULL` bounds conservatively. If a table contains unprocessed or malformed geometries, the planner may bypass skipping to avoid false negatives. Implement strict ingestion validation and route invalid records to a quarantine table before spatial column extraction.
3. **CRS Mismatch & Unit Drift:** Queries using metric predicates (`ST_DWithin` in meters) against lat/lon tables (`EPSG:4326`) bypass pushdown because the engine cannot safely convert units at the metadata layer. Standardize all stored bounds to a single CRS and perform unit conversions post-filter.
4. **Over-Partitioning & Small Files:** Partitioning by high-cardinality spatial hashes (e.g., Geohash at level 12+) creates thousands of tiny Parquet files. The planner spends more time reading manifests than skipping data. Enforce a minimum file size of 128MB and use `VACUUM` / `expire_snapshots` to prevent metadata bloat.
5. **Query Plan Inspection:** Always run `EXPLAIN FORMATTED` before deploying spatial workloads. Look for `PushedFilters` containing `x_min`, `y_min`, or bounding box predicates. If the plan shows `Filter` at the `Project` or `Scan` level without storage-layer predicates, pushdown is disabled or unsupported for that predicate type.

For detailed metrics on how early pruning impacts end-to-end GIS query execution, review [How predicate pushdown reduces GIS query latency](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/predicate-pushdown-optimization/how-predicate-pushdown-reduces-gis-query-latency/).
