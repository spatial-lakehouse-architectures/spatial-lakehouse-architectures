# Iceberg Spatial Type Support: Production Architecture & Operational Guide

Implementing spatial workloads on open table formats requires deliberate alignment between storage semantics, query engines, and metadata management. Within the broader [Spatial Lakehouse Fundamentals & Architecture](/spatial-lakehouse-fundamentals-architecture/) framework, Iceberg's spatial type support operates at the intersection of columnar storage optimization and geospatial predicate evaluation. Unlike proprietary GIS databases that embed spatial indexes directly into storage layers, Iceberg delegates filtering to manifest-level statistics, deterministic partition transforms, and engine-level UDFs. This architectural shift eliminates vendor lock-in but demands explicit configuration at the table, partition, and pipeline layers to prevent full-table scans on geometry columns.

## Schema Design & CRS Enforcement

Iceberg's V3 specification introduces native `geometry` and `geography` types, but engine support is still uneven, so most production deployments continue to treat spatial columns as structured binary or string representations. Geometry payloads are stored as `BINARY` (WKB) columns; query engines such as Spark with Apache Sedona or Trino with its geospatial plugin interpret these payloads at runtime. To guarantee cross-engine interoperability, enforce strict Coordinate Reference System (CRS) validation at ingestion. Platform architects should standardize on EPSG:4326 (WGS84) for global datasets, storing the CRS identifier alongside the geometry to prevent silent coordinate drift during downstream joins.

Schema evolution is handled through Iceberg's explicit metadata tracking. When adding or altering spatial columns, leverage the format's backward-compatible type promotion rules to avoid breaking consumers. For detailed versioning strategies, consult the [Open Table Format Versioning](/spatial-lakehouse-fundamentals-architecture/open-table-format-versioning/) guidelines to align snapshot progression with spatial schema migrations.

**PySpark: Schema Definition with CRS Validation**
```python
from pyspark.sql.types import StructType, StructField, BinaryType, StringType, TimestampType
from pyspark.sql.functions import col, expr, lit

schema = StructType([
    StructField("feature_id", StringType(), False),
    StructField("geometry_wkb", BinaryType(), True),   # WKB, not WKT string
    StructField("crs", StringType(), True),
    StructField("ingested_at", TimestampType(), True)
])

df = spark.read.json("s3://raw-gis-data/")
# Validate CRS and reject non-conforming records
validated_df = df.filter(col("crs") == lit("EPSG:4326"))
validated_df.writeTo("catalog.gis.features").using("iceberg").createOrReplace()
```

## Spatial Partitioning & Write-Time Sorting

Iceberg lacks native spatial partition transforms (no built-in `ST_Geohash` transform in the core spec), but production workloads achieve efficient data skipping by combining upstream geohashing with Iceberg's `bucket()` transform alongside temporal partitioning. This co-locates spatially adjacent records, enabling bounding-box filters to prune manifest files before scanning.

**SQL: Table Creation with Spatial Bucketing & Sort Order**
```sql
CREATE TABLE catalog.gis.traffic_zones (
  zone_id BIGINT,
  boundary BINARY,  -- WKB-encoded geometry
  updated_at TIMESTAMP,
  geohash STRING,   -- computed upstream, e.g. via ST_GeoHash(boundary, 6)
  bbox_min_x DOUBLE,
  bbox_min_y DOUBLE,
  bbox_max_x DOUBLE,
  bbox_max_y DOUBLE
) USING iceberg
PARTITIONED BY (bucket(64, geohash), days(updated_at))
TBLPROPERTIES (
  'write.sort-order' = 'geohash ASC, updated_at DESC',
  'write.parquet.compression-codec' = 'zstd',
  'write.target-file-size-bytes' = '536870912'
);
```

Ingestion pipelines must guarantee that `geohash` is computed deterministically (e.g., using a spatial UDF that calls `ST_GeoHash(boundary, precision => 6)`). If upstream writes are unsorted, schedule `rewrite_data_files` with a spatial sort order to maintain clustering efficiency. Unlike [Delta Lake Geometry Handling](/spatial-lakehouse-fundamentals-architecture/delta-lake-geometry-handling/), which relies on post-write `ZORDER BY` optimization, Iceberg's `write.sort-order` enforces clustering at write time, reducing compaction overhead but requiring strict pipeline discipline.

## Predicate Pushdown & Manifest Statistics

Spatial filtering performance depends on the accuracy of min/max statistics captured in Iceberg manifest files. For binary geometry columns, engines must extract bounding box (BBOX) coordinates during write and store them as separate `DOUBLE` columns to enable manifest-level skipping. Query engines will push down predicates on `bbox_min_x`, `bbox_max_x`, etc., skipping Parquet files whose bounding boxes do not intersect the query window.

**Debugging Pushdown Failures:**
1. **Verify bounding box columns exist and are typed `DOUBLE`**: The Iceberg manifest stores per-column min/max; if bbox columns are absent, the engine cannot prune files.
2. **Inspect manifest stats**: `SELECT file_path, lower_bounds, upper_bounds FROM catalog.gis.traffic_zones.files LIMIT 10;`
3. **Check for `null` bounds**: If bbox columns contain nulls, the engine defaults to full-table scans. Enforce `NOT NULL` constraints on bbox columns and reject records that fail extraction.
4. **Engine version**: Spatial predicate pushdown via bbox columns works in Spark 3.3+ with Iceberg 1.1+. Confirm with `EXPLAIN` that bbox predicates appear in `PushedFilters`.

## Maintenance, Retention & CI/CD Validation

Spatial tables generate higher metadata churn due to frequent geometry updates and partition splits. Implement automated maintenance to control manifest bloat and enforce snapshot retention.

**SQL: Automated Maintenance & Retention Policy**
```sql
-- Rewrite small files and sort spatial buckets
CALL catalog.system.rewrite_data_files(
  table => 'catalog.gis.traffic_zones',
  strategy => 'sort',
  sort_order => 'geohash ASC'
);

-- Expire old snapshots (retain last 5, remove anything older than a timestamp)
CALL catalog.system.expire_snapshots(
  table => 'catalog.gis.traffic_zones',
  older_than => TIMESTAMPADD(DAY, -7, CURRENT_TIMESTAMP),
  retain_last => 5
);
```

For CI/CD pipelines, validate spatial integrity before production promotion:

```yaml
name: Validate Iceberg Spatial Tables
on: [push]
jobs:
  spatial-validation:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Install dependencies
        run: pip install pyiceberg pyarrow shapely pytest
      - name: Run spatial extent tests
        run: pytest tests/test_spatial_integrity.py -v
```

When ingesting unstructured spatial payloads, refer to [How to store GeoJSON in Apache Iceberg tables](/spatial-lakehouse-fundamentals-architecture/iceberg-spatial-type-support/how-to-store-geojson-in-apache-iceberg-tables/) for serialization patterns that preserve topology without inflating Parquet row groups.

## Troubleshooting Production Anomalies

| Symptom | Root Cause | Resolution |
|---|---|---|
| Full-table scans on spatial joins | Missing bbox columns or unsorted writes | Add `bbox_min_x/y`, `bbox_max_x/y` as `DOUBLE NOT NULL`; run `rewrite_data_files` with spatial sort |
| Metadata bloat (>10GB) | High-frequency micro-batches with spatial updates | Increase `write.target-file-size-bytes` to 512MB; consolidate snapshots daily |
| CRS mismatch in downstream BI | Implicit coordinate transformation during read | Enforce `ST_Transform()` in view layer; standardize on EPSG:4326 at ingestion |
| Partition skew | Geohash precision too high/low for dataset extent | Adjust geohash precision to 5–7; monitor bucket distribution via `SELECT count(*) FROM ... GROUP BY bucket(64, geohash)` |

## Operational Readiness Checklist

- [ ] Enforce EPSG:4326 at ingestion; reject non-compliant payloads via pipeline validation.
- [ ] Store geometry as `BINARY` (WKB); include explicit `bbox_min_x/y`, `bbox_max_x/y` columns for predicate pushdown.
- [ ] Configure `write.sort-order` to match spatial bucketing strategy.
- [ ] Schedule `rewrite_data_files` every 6 hours for high-velocity spatial streams.
- [ ] Set snapshot retention to align with time-travel SLA (minimum 7 days for debugging).
- [ ] Validate bbox statistics post-write using manifest inspection.

Productionizing Iceberg Spatial Type Support requires deliberate trade-offs between write-time sorting, manifest statistics, and maintenance cadence. By enforcing CRS standards, implementing deterministic spatial partitioning, and automating compaction, platform teams can achieve sub-second spatial predicate pushdown without sacrificing open-format interoperability. Align pipeline configurations with the [Apache Iceberg Specification](https://iceberg.apache.org/spec/) and validate against [OGC Simple Features](https://www.ogc.org/standards/sfs) compliance to ensure long-term spatial data integrity across cloud-native architectures.
