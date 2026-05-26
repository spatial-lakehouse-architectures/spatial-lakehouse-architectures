# Iceberg Spatial Type Support: Production Architecture & Operational Guide

Implementing spatial workloads on open table formats requires deliberate alignment between storage semantics, query engines, and metadata management. Within the broader [Spatial Lakehouse Fundamentals & Architecture](/spatial-lakehouse-fundamentals-architecture/) framework, Iceberg’s spatial type support operates at the intersection of columnar storage optimization and geospatial predicate evaluation. Unlike proprietary GIS databases that embed spatial indexes directly into storage layers, Iceberg delegates filtering to manifest-level statistics, deterministic partition transforms, and engine-level UDFs. This architectural shift eliminates vendor lock-in but demands explicit configuration at the table, partition, and pipeline layers to prevent full-table scans on geometry columns.

## Schema Design & CRS Enforcement

Iceberg’s core specification treats spatial columns as structured binary or string representations, relying on query engines (Spark, Trino, PyIceberg) to interpret geometry payloads. To guarantee cross-engine interoperability, enforce strict Coordinate Reference System (CRS) validation at ingestion. Platform architects should standardize on EPSG:4326 (WGS84) for global datasets and EPSG:3857 for projected analytics, storing the CRS identifier alongside the geometry to prevent silent coordinate drift during downstream joins.

Schema evolution is handled through Iceberg’s explicit metadata tracking. When adding or altering spatial columns, leverage the format’s backward-compatible type promotion rules to avoid breaking consumers. For detailed versioning strategies, consult the [Open Table Format Versioning](/spatial-lakehouse-fundamentals-architecture/open-table-format-versioning/) guidelines to align snapshot progression with spatial schema migrations.

**PySpark: Schema Definition with CRS Validation**
```python
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col, expr

schema = StructType([
    StructField("feature_id", StringType(), False),
    StructField("boundary_wkt", StringType(), True),
    StructField("crs", StringType(), True),
    StructField("ingested_at", TimestampType(), True)
])

df = spark.read.json("s3://raw-gis-data/")
validated_df = df.withColumn("crs", expr("CASE WHEN crs = 'EPSG:4326' THEN crs ELSE 'EPSG:4326' END"))
validated_df.writeTo("catalog.gis.features").using("iceberg").createOrReplace()
```

## Spatial Partitioning & Write-Time Sorting

Iceberg lacks native spatial partition transforms, but production workloads achieve efficient data skipping by combining geohashing with deterministic bucketing. The recommended pattern generates a spatial hash upstream, then applies Iceberg’s `bucket()` transform alongside temporal partitioning. This co-locates spatially adjacent records, enabling bounding-box filters to prune manifest files before scanning.

**SQL: Table Creation with Spatial Bucketing & Sort Order**
```sql
CREATE TABLE catalog.gis.traffic_zones (
  zone_id BIGINT,
  boundary GEOMETRY,
  updated_at TIMESTAMP,
  geohash STRING
) USING iceberg
PARTITIONED BY (bucket(64, geohash), days(updated_at))
TBLPROPERTIES (
  'write.sort-order' = 'geohash ASC, updated_at DESC',
  'write.parquet.compression-codec' = 'zstd',
  'write.target-file-size-bytes' = '536870912'
);
```

Ingestion pipelines must guarantee that `geohash` is computed deterministically (e.g., `ST_GeoHash(boundary, precision=6)`). If upstream writes are unsorted, schedule `rewrite_data_files` with a spatial sort order to maintain clustering efficiency. Unlike [Delta Lake Geometry Handling](/spatial-lakehouse-fundamentals-architecture/delta-lake-geometry-handling/), which relies on post-write `ZORDER BY` optimization, Iceberg’s `write.sort-order` enforces clustering at write time, reducing compaction overhead but requiring strict pipeline discipline.

## Predicate Pushdown & Manifest Statistics

Spatial filtering performance depends entirely on the accuracy of min/max statistics captured in Iceberg manifest files. For geometry types, engines must extract bounding box (BBOX) coordinates during serialization. When using Parquet, ensure the `lower_bound` and `upper_bound` metadata fields contain normalized min/max X/Y values. Query engines will automatically push down predicates like `ST_Intersects(boundary, ST_GeomFromText('POLYGON(...)'))` if the BBOX overlaps the filter bounds.

**Debugging Pushdown Failures:**
1. **Verify engine compatibility:** Spark 3.4+ and Trino 400+ include native Iceberg spatial predicate pushdown. Older versions fall back to full scans.
2. **Inspect manifest stats:** `SELECT * FROM catalog.gis.traffic_zones.metadata.files WHERE file_path LIKE '%manifest%'`
3. **Check for `null` bounds:** If geometry serialization strips BBOX metadata, the engine defaults to full-table scans. Force BBOX extraction in the write path using `ST_Envelope()` or equivalent engine functions.

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

-- Expire old snapshots (retain 7 days, 24h grace period)
CALL catalog.system.expire_snapshots(
  table => 'catalog.gis.traffic_zones',
  older_than => TIMESTAMP '2024-01-01 00:00:00',
  retain_last => 5
);
```

For CI/CD pipelines, validate spatial integrity before production promotion. The following GitHub Actions workflow runs schema validation and spatial extent checks:

```yaml
name: Validate Iceberg Spatial Tables
on: [push]
jobs:
  spatial-validation:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Install dependencies
        run: pip install geopandas pyiceberg pytest
      - name: Run spatial extent tests
        run: |
          pytest tests/test_spatial_integrity.py \
            --table=catalog.gis.traffic_zones \
            --expected-crs=EPSG:4326 \
            --bbox-bounds=-180,-90,180,90
```

When ingesting unstructured spatial payloads, refer to [How to store GeoJSON in Apache Iceberg tables](/spatial-lakehouse-fundamentals-architecture/iceberg-spatial-type-support/how-to-store-geojson-in-apache-iceberg-tables/) for serialization patterns that preserve topology without inflating Parquet row groups.

## Troubleshooting Production Anomalies

| Symptom | Root Cause | Resolution |
|---|---|---|
| Full-table scans on spatial joins | Missing BBOX stats or unsorted writes | Run `rewrite_data_files` with spatial sort; verify engine pushdown support |
| Metadata bloat (>10GB) | High-frequency micro-batches with spatial updates | Increase `write.target-file-size-bytes` to 512MB; consolidate snapshots daily |
| CRS mismatch in downstream BI | Implicit coordinate transformation during read | Enforce `ST_Transform()` in view layer; standardize on EPSG:4326 at ingestion |
| Partition skew | Geohash precision too high/low for dataset extent | Adjust geohash precision to 5-7; monitor `bucket()` distribution via `SELECT count(*) GROUP BY bucket(64, geohash)` |

## Operational Readiness Checklist

- [ ] Enforce EPSG:4326 at ingestion; reject non-compliant payloads via pipeline validation.
- [ ] Configure `write.sort-order` to match spatial bucketing strategy.
- [ ] Schedule `rewrite_data_files` every 6 hours for high-velocity spatial streams.
- [ ] Set snapshot retention to 7 days to balance time-travel needs with metadata storage costs.
- [ ] Validate BBOX statistics post-write using `DESCRIBE EXTENDED` or manifest inspection.

Productionizing Iceberg Spatial Type Support requires deliberate trade-offs between write-time sorting, manifest statistics, and maintenance cadence. By enforcing CRS standards, implementing deterministic spatial partitioning, and automating compaction, platform teams can achieve sub-second spatial predicate pushdown without sacrificing open-format interoperability. Align pipeline configurations with the [Apache Iceberg Specification](https://iceberg.apache.org/spec/) and validate against [OGC Simple Features](https://www.ogc.org/standards/sfs) compliance to ensure long-term spatial data integrity across cloud-native architectures.