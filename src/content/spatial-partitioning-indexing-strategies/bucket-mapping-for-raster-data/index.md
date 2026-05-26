# Bucket Mapping for Raster Data

Raster datasets—satellite imagery, digital elevation models (DEMs), LiDAR derivatives, and climate reanalysis grids—introduce distinct storage and query challenges in modern data lakehouses. Unlike vector geometries, rasters are inherently grid-aligned, frequently exceed multi-terabyte scales, and exhibit highly localized, bounding-box-driven access patterns. Bucket mapping translates continuous spatial coordinates into discrete, query-optimized partition directories, enabling efficient predicate pushdown, metadata pruning, and predictable I/O behavior. This technique operates as a specialized implementation layer within broader [Spatial Partitioning & Indexing Strategies](/spatial-partitioning-indexing-strategies/), where physical storage layout must align tightly with downstream analytical and GIS processing workloads.

## Deterministic Coordinate Transformation & CRS Alignment

Effective bucket mapping begins with deterministic coordinate transformation. Raw geographic coordinates (WGS84, EPSG:4326) introduce severe distortion and uneven bucket sizes at higher latencies, making them unsuitable for direct partitioning. Production pipelines must project raster extents into a metric-aligned coordinate reference system (CRS) before computing bucket identifiers.

For continental-scale ingestion, Universal Transverse Mercator (UTM) zones establish a natural, meter-based grid. The ingestion pipeline derives the UTM zone identifier, truncates easting/northing values to a fixed tile size, and hashes them into partition columns. See [Mapping UTM zones to Iceberg partition columns](/spatial-partitioning-indexing-strategies/bucket-mapping-for-raster-data/mapping-utm-zones-to-iceberg-partition-columns/) for detailed schema evolution patterns.

**Explicit Production Parameters:**
- **Target CRS:** `EPSG:32633` (UTM Zone 33N)
- **Tile Size:** `2000m × 2000m` (aligns with typical 1024×1024 GeoTIFF block boundaries)
- **Bucket Formula:** `bucket_id = CONCAT(FLOOR(easting / 2000), '_', FLOOR(northing / 2000))`
- **Partition Bounds:** `easting ∈ [100000, 900000]`, `northing ∈ [1100000, 9200000]`

This deterministic mapping ensures that adjacent spatial tiles map to predictable directory paths, preventing coordinate drift from propagating into consumer queries.

## Partition Hierarchy & Directory Layout

Architecting the partition hierarchy requires balancing directory depth against query selectivity. [Spatial Partitioning Schemes](/spatial-partitioning-indexing-strategies/spatial-partitioning-schemes/) outlines the trade-offs between coarse administrative boundaries, hierarchical quadtrees, and flat spatial hashing. For raster workloads, a two-tier partition strategy consistently delivers optimal query planner performance:

1. **Coarse Partition:** `utm_zone`, `acquisition_year`, `sensor_type`
2. **Bucket Partition:** `spatial_bucket` (string-encoded grid cell)

This structure prevents metadata explosion while maintaining high pruning efficiency for regional or temporal queries. Target file sizes should align with cloud storage block limits (typically 128MB–512MB per Parquet/GeoParquet file) to avoid excessive `LIST` API calls during manifest reads. Over-partitioning below 64MB/file triggers metadata bloat, while under-partitioning above 2GB/file degrades parallel read throughput.

## Production Implementation Patterns

### PySpark Ingestion Pipeline
The following snippet demonstrates coordinate projection, bucket derivation, and Iceberg table writes with explicit partition specs:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, floor, concat_ws, lit
import pyspark.sql.types as T

spark = SparkSession.builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
    .getOrCreate()

# Load raw raster catalog (GeoTIFF paths + bounding boxes in EPSG:4326)
raw_df = spark.read.parquet("s3://raw-catalog/landsat_metadata/")

# Project to UTM 33N (EPSG:32633) and compute bucket IDs
bucket_df = raw_df.withColumn(
    "spatial_bucket",
    concat_ws(
        "_",
        floor(col("easting_utm33n") / 2000).cast(T.IntegerType()),
        floor(col("northing_utm33n") / 2000).cast(T.IntegerType())
    )
)

# Write with hidden partitioning
bucket_df.write.format("iceberg") \
    .option("path", "s3://lakehouse/raster/landsat/") \
    .partitionBy("utm_zone", "acquisition_year", "spatial_bucket") \
    .mode("append") \
    .save()
```

### SQL DDL & Query Pruning
Define the table schema and verify partition pruning via `EXPLAIN`:

```sql
CREATE TABLE lakehouse.raster.landsat_bucketed (
    raster_path STRING,
    sensor_type STRING,
    acquisition_date DATE,
    utm_zone INT,
    easting DOUBLE,
    northing DOUBLE,
    spatial_bucket STRING
) USING iceberg
PARTITIONED BY (utm_zone, year(acquisition_date), spatial_bucket);

-- Query engine will prune partitions matching bucket range
EXPLAIN SELECT COUNT(*) FROM lakehouse.raster.landsat_bucketed
WHERE spatial_bucket BETWEEN '450_5200' AND '455_5205'
AND acquisition_date >= '2023-01-01';
```

### CI/CD Validation Step
Validate partition structure before merging pipeline changes:

```yaml
# .github/workflows/validate-partitions.yml
name: Validate Raster Partition Schema
on: [pull_request]
jobs:
  check-partitions:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Run partition validator
        run: |
          python scripts/validate_bucket_schema.py \
            --catalog-path s3://lakehouse/raster/landsat/ \
            --expected-tile-size 2000 \
            --crs EPSG:32633 \
            --max-partitions-per-year 15000
```

## Query Execution & Pruning Mechanics

Bucket mapping enables the query planner to translate spatial predicates directly into directory scans. When a bounding box intersects multiple tiles, the engine computes the overlapping `spatial_bucket` range, reads only the relevant manifest entries, and skips non-matching partitions entirely. This reduces I/O by 60–85% compared to unpartitioned lakehouse scans.

For multi-dimensional workloads combining spatial, temporal, and spectral filters, bucket mapping pairs effectively with [Z-Ordering for Geospatial Queries](/spatial-partitioning-indexing-strategies/z-ordering-for-geospatial-queries/). While bucket mapping handles coarse directory pruning, Z-ordering optimizes file-level data layout within those directories, minimizing the number of Parquet row groups scanned per query.

## Operational Guardrails & Troubleshooting

### Common Failure Modes
| Symptom | Root Cause | Remediation |
|---------|------------|-------------|
| Query planner scans 100% of partitions | CRS mismatch between ingestion and query layer | Standardize all pipelines to a single EPSG code; validate with `ST_Transform` checks |
| Metadata bloat (>500k partitions) | Tile size too small or over-partitioning | Increase `tile_size` to 4000m+; consolidate historical data using `OPTIMIZE`/`REWRITE` |
| Severe partition skew | Coastal/urban rasters span multiple UTM zones | Implement zone-edge buffering; route edge tiles to a dedicated `utm_zone=99` fallback partition |
| `FileNotFoundException` on read | Stale manifest after external deletion | Run `CALL lakehouse.system.expire_snapshots()` and refresh catalog metadata |

### Retention & Lifecycle Policies
Raster archives require strict lifecycle management to control storage costs. Implement time-based retention aligned with data utility:
- **Operational Satellite Imagery:** `retention_days = 1095` (3-year rolling)
- **Climate Reanalysis Grids:** `retention_days = 3650` (10-year archival)
- **LiDAR Point Clouds:** `retention_days = 7300` (20-year compliance)

Automate cleanup using scheduled Spark jobs:
```sql
CALL lakehouse.system.expire_snapshots(
    table => 'lakehouse.raster.landsat_bucketed',
    older_than => TIMESTAMP '2021-01-01 00:00:00',
    retain_last => 5
);
```

### Debugging Workflow
1. **Verify Bucket Alignment:** Cross-check `spatial_bucket` values against known tile boundaries using the [GDAL Raster Data Model](https://gdal.org/user/raster_data_model.html) reference.
2. **Inspect Manifests:** Query `lakehouse.raster.landsat_bucketed.metadata` to confirm partition distribution matches expected spatial density.
3. **Profile Query Plans:** Run `EXPLAIN (FORMAT JSON)` to verify `PartitionPruning` predicates are applied before `FileScan`.
4. **Validate CRS Consistency:** Ensure all upstream producers reference the official [EPSG Geodetic Parameter Dataset](https://epsg.org/) to prevent silent coordinate drift.

Bucket mapping transforms unstructured raster sprawl into deterministic, query-ready storage layouts. By enforcing strict CRS alignment, calibrated tile sizes, and automated lifecycle policies, platform teams can deliver sub-second spatial pruning at petabyte scale while maintaining full compatibility with downstream GIS and analytical engines.