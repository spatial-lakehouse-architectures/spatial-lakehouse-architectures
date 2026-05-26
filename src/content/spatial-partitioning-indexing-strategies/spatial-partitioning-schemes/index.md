# Spatial Partitioning Schemes

Geospatial workloads in modern data lakehouses demand partitioning strategies that respect topological adjacency rather than lexical ordering. Traditional range or hash partitioning on coordinate columns produces severe data skew, triggers the small-files problem, and degrades join performance. Within the broader [Spatial Partitioning & Indexing Strategies](/spatial-partitioning-indexing-strategies/) framework, spatial partitioning schemes translate geographic primitives into deterministic storage layouts, enabling directory pruning, metadata efficiency, and predictable query latency. This guide details production-grade configuration, format-specific trade-offs, and operational maintenance for deploying spatial partitioning across Delta Lake and Apache Iceberg.

## Architectural Foundations

Spatial partitioning maps geometries to discrete storage units using hierarchical grids or space-filling curves. The choice of scheme directly impacts I/O patterns, transaction log overhead, and compaction frequency.

| Scheme | Topology | Best Use Case | Lakehouse Fit |
|--------|----------|---------------|---------------|
| **H3 / S2 Grids** | Hexagonal / Quadtree | Global telemetry, IoT trajectories, regional aggregations | Delta (directory-based), Iceberg (partition transforms) |
| **Z-Order / Hilbert** | Space-filling curve | Bounding-box scans, KNN, multi-dimensional clustering | In-file clustering (requires compaction) |
| **QuadTree / R-Tree** | Hierarchical bounding boxes | High-precision cadastral, cadastral parcel joins | External index files (less native to Parquet) |

Delta Lake materializes partitions as physical directories. Over-partitioning at fine grid resolutions (e.g., H3 resolution 10+) generates millions of directories, inflating `LIST` operations and fragmenting the `_delta_log`. Apache Iceberg abstracts partitioning via transform functions, storing partition metadata in manifests rather than directories, which scales better to high cardinality but requires careful spec versioning.

## Operational Configuration & Parameters

Production deployments must enforce explicit spatial contracts before ingestion begins.

### Coordinate Reference System (CRS) & Bounds
Always standardize to **EPSG:4326 (WGS84)** at ingestion. Projected CRS values (e.g., EPSG:3857) introduce distortion that breaks spatial partition boundaries. Define explicit global or regional bounds to prevent null-partition drift:
- **Global:** `[-180.0, -90.0, 180.0, 90.0]`
- **Regional (CONUS):** `[-125.0, 24.5, -66.9, 49.3]`

### Partition Resolution & File Sizing
Target **128MB–1GB** uncompressed Parquet files post-compaction. For H3, resolution `7` (~5 km² cells) typically yields optimal cardinality for continental-scale datasets. Higher resolutions (8–10) should only be used when query predicates consistently filter to sub-kilometer extents.

### Retention Policies
Enforce time-based retention alongside spatial partitions to prevent metadata bloat. A standard contract:
```sql
ALTER TABLE geospatial.events SET TBLPROPERTIES (
  'delta.logRetentionDuration' = 'interval 30 days',
  'delta.deletedFileRetentionDuration' = 'interval 7 days',
  'max_partition_retention_days' = '365'
);
```

## Production Implementation Patterns

### PySpark: H3 Partitioning in Delta Lake
The following pipeline ingests WGS84 trajectories, computes H3 cells, and writes partitioned Delta tables with explicit schema enforcement.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, lit, year, month
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType

spark = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

schema = StructType([
    StructField("device_id", StringType(), False),
    StructField("lat", DoubleType(), False),
    StructField("lon", DoubleType(), False),
    StructField("ts", TimestampType(), False)
])

# Ingest raw telemetry
df = spark.read.schema(schema).json("s3://raw-telemetry/2024-10/")

# Compute H3 partition key (resolution 7) and enforce CRS bounds
df_spatial = df.filter(
    (col("lat") >= -90.0) & (col("lat") <= 90.0) &
    (col("lon") >= -180.0) & (col("lon") <= 180.0)
).withColumn("h3_res7", expr("h3_latlng_to_cell(lat, lon, 7)")) \
 .withColumn("year", year(col("ts"))) \
 .withColumn("month", month(col("ts")))

df_spatial.write \
    .format("delta") \
    .partitionBy("h3_res7", "year", "month") \
    .option("mergeSchema", "false") \
    .mode("append") \
    .save("s3://lakehouse/geospatial/events")
```
For a complete breakdown of directory pruning behavior and transaction log optimization, review [Implementing H3 hexagon partitioning in Delta Lake](/spatial-partitioning-indexing-strategies/spatial-partitioning-schemes/implementing-h3-hexagon-partitioning-in-delta-lake/).

### Spark SQL: Apache Iceberg Partition Transforms
Iceberg handles spatial partitioning via metadata transforms, avoiding directory proliferation. Use `bucket` or `truncate` transforms alongside spatial UDFs.

```sql
CREATE TABLE iceberg.geospatial.events (
    device_id STRING,
    lat DOUBLE,
    lon DOUBLE,
    ts TIMESTAMP,
    h3_res7 STRING
) USING iceberg
PARTITIONED BY (
    bucket(128, h3_res7),
    days(ts)
)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.metadata.previous-versions-max' = '10'
);

INSERT INTO iceberg.geospatial.events
SELECT 
    device_id, lat, lon, ts,
    h3_latlng_to_cell(lat, lon, 7) AS h3_res7
FROM raw.telemetry_stream
WHERE ts >= '2024-01-01';
```

## Clustering & Query Optimization

Partitioning alone cannot resolve intra-file spatial locality. Pair coarse partitions with in-file clustering to minimize row-group scans. Applying [Z-Ordering for Geospatial Queries](/spatial-partitioning-indexing-strategies/z-ordering-for-geospatial-queries/) interleaves longitude and latitude bits, ensuring adjacent coordinates share Parquet row groups. This requires scheduled `OPTIMIZE` or `REWRITE DATA` jobs to maintain clustering efficiency as data ages.

For Iceberg, leverage `SORT BY` during writes:
```sql
INSERT INTO iceberg.geospatial.events
SELECT * FROM staging.events
SORT BY h3_res7, lon, lat;
```

## Maintenance, Compaction & Troubleshooting

### Manifest Bloat & Checkpoint Fragmentation
Monitor partition cardinality weekly. If `h3_res7` cardinality exceeds 50,000 per day, consider:
1. Upgrading to Iceberg v2 partition specs
2. Implementing dynamic partition pruning via query predicates
3. Running daily compaction to merge small files (`delta.autoOptimize.optimizeWrite = true`)

### Predicate Pushdown Debugging
Spatial predicates fail to prune when column statistics lack envelope bounds. Ensure your query engine leverages [Predicate Pushdown Optimization](/spatial-partitioning-indexing-strategies/predicate-pushdown-optimization/) by exposing min/max coordinates in Parquet metadata. In Spark, validate pushdown with:

```sql
EXPLAIN EXTENDED
SELECT * FROM geospatial.events
WHERE ST_Contains(ST_PolygonFromText('POLYGON((-120 35, -115 35, -115 40, -120 40, -120 35))'), ST_Point(lon, lat));
```
If the plan shows `AlwaysTrue` or `Filter` without `PartitionFilters`, verify:
- `spark.sql.parquet.filterPushdown=true`
- Spatial UDFs are registered as deterministic
- Null-handling is explicit in `ST_Point` wrappers
- GeoParquet metadata aligns with [OGC GeoParquet Specification](https://geoparquet.org/)

### CI/CD Validation Pipeline
Enforce spatial contracts before merging pipeline changes. The following GitHub Actions workflow validates partition cardinality and schema drift:

```yaml
name: spatial-partition-validation
on: [pull_request]
jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Run Partition Cardinality Check
        run: |
          spark-sql -e "
          SELECT 
            COUNT(DISTINCT h3_res7) AS unique_cells,
            COUNT(*) AS total_rows,
            CASE WHEN COUNT(DISTINCT h3_res7) > 50000 THEN 'FAIL' ELSE 'PASS' END AS status
          FROM geospatial.events 
          WHERE ts >= current_date() - INTERVAL 1 DAY;
          " > /tmp/partition_report.json
      - name: Assert Threshold
        run: |
          jq -e '.status == "PASS"' /tmp/partition_report.json || exit 1
```

## Operational Checklist
- [ ] Standardize all inputs to EPSG:4326 before partition computation
- [ ] Cap grid resolution to prevent directory/manifest explosion
- [ ] Schedule weekly compaction and Z-order rewrites
- [ ] Validate spatial UDF determinism and null handling
- [ ] Monitor `spark.sql.files.maxPartitionBytes` and adjust to 128MB–1GB targets
- [ ] Enforce schema contracts via CI/CD cardinality thresholds

Adhering to these patterns ensures spatial partitioning scales predictably across petabyte-scale lakehouses, minimizing I/O overhead while maintaining strict query latency SLAs.