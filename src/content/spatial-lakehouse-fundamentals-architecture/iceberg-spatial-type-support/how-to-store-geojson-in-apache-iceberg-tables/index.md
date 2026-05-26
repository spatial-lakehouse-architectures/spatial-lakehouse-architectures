# How to store GeoJSON in Apache Iceberg tables

Storing raw GeoJSON payloads in a lakehouse table degrades query performance, breaks vectorized execution, and introduces uncontrolled schema drift. The production objective is to normalize incoming GeoJSON into deterministic, columnar primitives that enable manifest-level spatial pruning while preserving downstream GIS compatibility. This guide details the exact configuration, encoding, and optimization steps required to operationalize GeoJSON ingestion in Apache Iceberg.

## 1. Deterministic Schema & WKB Encoding

Iceberg’s type system currently lacks a native `GEOMETRY` primitive. Persisting JSON strings forces runtime deserialization on every scan, starving the optimizer of column statistics. The production standard is to decompose GeoJSON into Well-Known Binary (WKB) and explicit bounding box columns. WKB is endian-safe, compact, and directly consumable by spatial UDFs without JSON parsing overhead.

```sql
CREATE TABLE gis.assets (
    asset_id STRING NOT NULL,
    geom_wkb BINARY NOT NULL,
    bbox_min_x DOUBLE,
    bbox_max_x DOUBLE,
    bbox_min_y DOUBLE,
    bbox_max_y DOUBLE,
    srid INT DEFAULT 4326,
    properties MAP<STRING, STRING>,
    ingestion_ts TIMESTAMP NOT NULL,
    partition_date DATE
)
USING iceberg
PARTITIONED BY (partition_date, bucket(16, asset_id))
LOCATION 's3://lakehouse-prod/gis/assets'
TBLPROPERTIES (
    'write.parquet.compression-codec' = 'zstd',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '10'
);
```

**Key Configuration Notes:**
- `BINARY` stores raw WKB bytes. Avoid `STRING` or `JSON` types.
- Bounding box columns (`bbox_min_x`, etc.) must be typed as `DOUBLE` to enable range-based manifest pruning.
- `properties` uses `MAP<STRING, STRING>` for residual attributes. Critical keys should be promoted to explicit typed columns to prevent downstream type coercion failures.
- Table properties enforce ZSTD compression and aggressive metadata cleanup to prevent manifest bloat on high-frequency spatial upserts.

## 2. Partitioning & Write Configuration

Spatial data requires partitioning strategies that align with query access patterns. Date-based partitioning combined with hash distribution prevents small-file proliferation and balances write parallelism. Avoid partitioning directly on coordinate values; it creates sparse partitions and degrades compaction efficiency.

Configure Spark/Iceberg write parameters to guarantee deterministic file layout:

```python
spark.conf.set("spark.sql.iceberg.vectorization.enabled", "true")
spark.conf.set("spark.sql.iceberg.planning.preserve-data-grouping", "true")
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

For high-throughput ingestion, set `write.distribution-mode=hash` to co-locate related geometries and minimize cross-node shuffles during spatial joins. Enable `write.parquet.page-size-bytes=1048576` to optimize column chunk alignment for spatial UDF scans.

## 3. Idempotent Ingestion Pipeline (PySpark)

The ingestion layer must parse GeoJSON, compute WKB, extract bounding boxes, and write to Iceberg in a single transactional pass. Use `ST_GeomFromGeoJSON` and `ST_AsBinary` equivalents via spatial libraries (GeoPandas, Sedona, or GeoTrellis) before DataFrame construction.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_timestamp, lit
import json
import struct

# Pseudocode for WKB + BBOX extraction (replace with Sedona/GeoPandas UDFs in prod)
def normalize_geojson(row):
    geom = json.loads(row["geojson"])
    coords = geom["geometry"]["coordinates"]
    # Flatten to compute bbox (implementation depends on geometry type)
    xs = [c[0] for c in flatten_coords(coords)]
    ys = [c[1] for c in flatten_coords(coords)]
    bbox = (min(xs), max(xs), min(ys), max(ys))
    wkb = geom_to_wkb(geom["geometry"])  # Use shapely.wkb.dumps()
    return (row["id"], wkb, bbox[0], bbox[1], bbox[2], bbox[3], row.get("props", {}))

df = spark.read.json("s3://ingest-bucket/daily_geojson/*.json")
normalized = df.rdd.map(normalize_geojson).toDF([
    "asset_id", "geom_wkb", "bbox_min_x", "bbox_max_x", 
    "bbox_min_y", "bbox_max_y", "properties"
])

(normalized
 .withColumn("ingestion_ts", current_timestamp())
 .withColumn("partition_date", to_date(current_timestamp()))
 .writeTo("gis.assets")
 .option("mergeSchema", "false")
 .append())
```

**Critical Constraint:** Disable `mergeSchema` on spatial tables. Uncontrolled property evolution breaks manifest statistics and invalidates downstream spatial indexes. Schema changes must be applied via explicit `ALTER TABLE` DDL.

## 4. Query Optimization & Manifest Pruning

Spatial predicate pushdown in Iceberg relies on manifest-level min/max statistics. When querying, always filter on the extracted bounding box columns before invoking spatial UDFs. This allows the query planner to skip Parquet files entirely.

```sql
SELECT asset_id, geom_wkb
FROM gis.assets
WHERE partition_date = '2024-11-01'
  AND bbox_min_x <= -74.0060
  AND bbox_max_x >= -73.9800
  AND bbox_min_y <= 40.7128
  AND bbox_max_y >= 40.7500
  AND ST_Intersects(ST_GeomFromWKB(geom_wkb), ST_PolygonFromText('POLYGON(...)'));
```

The bounding box filter executes during file planning, while `ST_Intersects` runs only on candidate rows. This two-stage evaluation reduces I/O by 80–95% on tables exceeding 100M rows. For sustained performance, run `CALL spark_catalog.system.rewrite_data_files('gis.assets', strategy => 'sort', sort_order => 'bbox_min_x ASC, bbox_min_y ASC')` weekly to cluster spatially adjacent records and improve Z-order-like locality.

## 5. Failure Modes & Resolution Matrix

| Symptom | Root Cause | Resolution |
|---------|------------|------------|
| Full table scan on `ST_Contains` | Missing or untyped bbox columns; query planner cannot push predicates | Add `bbox_min/max_x/y` as `DOUBLE`; rewrite queries to filter bbox before UDF execution |
| `Invalid WKB` or endian mismatch errors | Mixed-endian WKB generation across ingestion nodes | Enforce little-endian WKB output (`shapely.wkb.dumps(geom, hex=False)`); validate with `ST_IsValid(ST_GeomFromWKB(...))` |
| Manifest corruption / slow metadata reads | Excessive `previous-versions-max` or disabled metadata cleanup | Set `write.metadata.previous-versions-max=10` and `write.metadata.delete-after-commit.enabled=true` |
| Schema drift on `properties` map | Ingesting unvalidated JSON with dynamic keys | Promote high-cardinality keys to explicit columns; enforce `NOT NULL` constraints; disable `mergeSchema` |
| Vectorized execution disabled | Parquet column alignment mismatch or unsupported UDFs | Verify `spark.sql.iceberg.vectorization.enabled=true`; ensure spatial UDFs are registered as deterministic and non-blocking |

Tracking [Iceberg Spatial Type Support](/spatial-lakehouse-fundamentals-architecture/iceberg-spatial-type-support/) is required for future migration to native geometry primitives, but current production deployments must rely on WKB + bbox normalization to guarantee deterministic query planning. This architecture aligns with core [Spatial Lakehouse Fundamentals & Architecture](/spatial-lakehouse-fundamentals-architecture/) principles to prevent compute lock-in and maintain manifest-level statistics integrity.

For specification compliance, reference the [OGC Simple Features Specification](https://www.ogc.org/standards/sfa) for WKB encoding rules and the [Apache Iceberg Specification](https://iceberg.apache.org/spec/) for manifest statistics generation. Implement strict schema validation at the ingestion boundary, enforce deterministic partitioning, and validate spatial UDF determinism before promoting to production.