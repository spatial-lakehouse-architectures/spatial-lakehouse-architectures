# How to store GeoJSON in Apache Iceberg tables

Storing raw GeoJSON payloads in a lakehouse table degrades query performance, breaks vectorized execution, and introduces uncontrolled schema drift. The production objective is to normalize incoming GeoJSON into deterministic, columnar primitives that enable manifest-level spatial pruning while preserving downstream GIS compatibility. This guide details the exact configuration, encoding, and optimization steps required to operationalize GeoJSON ingestion in Apache Iceberg.

## 1. Deterministic Schema & WKB Encoding

Iceberg's type system does not include a native `GEOMETRY` primitive; geometry is stored as `BINARY` (WKB). Persisting raw GeoJSON strings forces runtime deserialization on every scan and starves the optimizer of column statistics. The production standard is to decompose GeoJSON into Well-Known Binary (WKB) and explicit bounding box columns. WKB is endian-safe, compact, and directly consumable by spatial UDFs without JSON parsing overhead.

```sql
CREATE TABLE gis.assets (
    asset_id STRING NOT NULL,
    geom_wkb BINARY NOT NULL,
    bbox_min_x DOUBLE,
    bbox_max_x DOUBLE,
    bbox_min_y DOUBLE,
    bbox_max_y DOUBLE,
    srid INT,
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
- `BINARY` stores raw WKB bytes. Avoid `STRING` or JSON-typed columns for geometry.
- Bounding box columns (`bbox_min_x`, etc.) must be typed as `DOUBLE` to enable range-based manifest pruning.
- `srid` stores the numeric EPSG code (e.g., `4326`). Store this as an integer, not a string, for type safety.
- `properties` uses `MAP<STRING, STRING>` for residual attributes. Critical keys should be promoted to explicit typed columns to prevent downstream type coercion failures.
- Table properties enforce ZSTD compression and aggressive metadata cleanup to prevent manifest bloat on high-frequency spatial upserts.

## 2. Partitioning & Write Configuration

Spatial data requires partitioning strategies that align with query access patterns. Date-based partitioning combined with hash distribution on `asset_id` prevents small-file proliferation and balances write parallelism. Avoid partitioning directly on coordinate values; it creates sparse partitions and degrades compaction efficiency.

Configure Spark write parameters to guarantee deterministic file layout:

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
# Iceberg vectorized reader (Iceberg 1.3+, Spark 3.3+)
spark.conf.set("spark.sql.iceberg.vectorization.enabled", "true")
```

For high-throughput ingestion, the default `write.distribution-mode=hash` co-locates related geometries and minimizes cross-node shuffles during spatial joins. Use `write.parquet.page-size-bytes=1048576` to optimize column chunk alignment for spatial UDF scans.

## 3. Idempotent Ingestion Pipeline (PySpark)

The ingestion layer must parse GeoJSON, compute WKB, extract bounding boxes, and write to Iceberg in a single transactional pass. The example below uses `shapely` for geometry parsing:

```python
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_timestamp, udf, lit
from pyspark.sql.types import BinaryType, DoubleType, StructType, StructField
import shapely.geometry
import shapely.wkb

def flatten_coords(geom_type, coords):
    """Flatten nested coordinate arrays to a flat list of (x, y) tuples."""
    if geom_type in ("Point",):
        return [coords]
    if geom_type in ("LineString", "MultiPoint"):
        return coords
    if geom_type in ("Polygon",):
        return [pt for ring in coords for pt in ring]
    if geom_type in ("MultiLineString",):
        return [pt for line in coords for pt in line]
    if geom_type in ("MultiPolygon",):
        return [pt for poly in coords for ring in poly for pt in ring]
    return []

def parse_geojson_feature(feature_json: str):
    """Returns (wkb_bytes, min_x, max_x, min_y, max_y) or None on failure."""
    try:
        feat = json.loads(feature_json)
        geom_dict = feat.get("geometry") or feat  # support both Feature and bare geometry
        geom = shapely.geometry.shape(geom_dict)
        wkb_bytes = shapely.wkb.dumps(geom, include_srid=False)
        bx = geom.bounds  # (minx, miny, maxx, maxy)
        return wkb_bytes, bx[0], bx[2], bx[1], bx[3]
    except Exception:
        return None

spark = SparkSession.builder.getOrCreate()

raw_df = spark.read.text("s3://ingest-bucket/daily_geojson/*.json")

# UDFs for bbox + wkb extraction
@udf(BinaryType())
def to_wkb(feature_json):
    result = parse_geojson_feature(feature_json)
    return result[0] if result else None

@udf(DoubleType())
def to_min_x(feature_json):
    result = parse_geojson_feature(feature_json)
    return result[1] if result else None

@udf(DoubleType())
def to_max_x(feature_json):
    result = parse_geojson_feature(feature_json)
    return result[2] if result else None

@udf(DoubleType())
def to_min_y(feature_json):
    result = parse_geojson_feature(feature_json)
    return result[3] if result else None

@udf(DoubleType())
def to_max_y(feature_json):
    result = parse_geojson_feature(feature_json)
    return result[4] if result else None

normalized = raw_df \
    .withColumn("geom_wkb",   to_wkb(col("value"))) \
    .withColumn("bbox_min_x", to_min_x(col("value"))) \
    .withColumn("bbox_max_x", to_max_x(col("value"))) \
    .withColumn("bbox_min_y", to_min_y(col("value"))) \
    .withColumn("bbox_max_y", to_max_y(col("value"))) \
    .withColumn("srid",       lit(4326)) \
    .withColumn("ingestion_ts",   current_timestamp()) \
    .withColumn("partition_date", to_date(current_timestamp())) \
    .filter(col("geom_wkb").isNotNull()) \
    .drop("value")

(normalized
 .writeTo("gis.assets")
 .option("mergeSchema", "false")
 .append())
```

**Critical Constraint:** Disable `mergeSchema` on spatial tables. Uncontrolled property evolution breaks manifest statistics and invalidates downstream spatial indexes. Schema changes must be applied via explicit `ALTER TABLE` DDL.

## 4. Query Optimization & Manifest Pruning

Spatial predicate pushdown in Iceberg relies on manifest-level min/max statistics from the bounding box columns. When querying, always filter on the extracted bounding box columns before invoking spatial UDFs. This allows the query planner to skip Parquet files entirely.

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

The bounding box filter executes during file planning, while `ST_Intersects` runs only on candidate rows. This two-stage evaluation reduces I/O by 80–95% on tables exceeding 100M rows. For sustained performance, run compaction weekly to cluster spatially adjacent records:

```sql
CALL spark_catalog.system.rewrite_data_files(
  table => 'gis.assets',
  strategy => 'sort',
  sort_order => 'bbox_min_x ASC, bbox_min_y ASC'
);
```

## 5. Failure Modes & Resolution Matrix

| Symptom | Root Cause | Resolution |
|---------|------------|------------|
| Full table scan on `ST_Contains` | Missing or untyped bbox columns; query planner cannot push predicates | Add `bbox_min/max_x/y` as `DOUBLE NOT NULL`; rewrite queries to filter bbox before UDF execution |
| `Invalid WKB` or endian mismatch errors | Mixed-endian WKB generation across ingestion nodes | Enforce little-endian WKB output (`shapely.wkb.dumps(geom, little_endian=True)`); validate with `ST_IsValid(ST_GeomFromWKB(...))` |
| Manifest corruption / slow metadata reads | `previous-versions-max` too high or metadata cleanup disabled | Set `write.metadata.previous-versions-max=10` and `write.metadata.delete-after-commit.enabled=true` |
| Schema drift on `properties` map | Ingesting unvalidated JSON with dynamic keys | Promote high-cardinality keys to explicit columns; enforce `NOT NULL` constraints; disable `mergeSchema` |
| Vectorized execution disabled | Parquet column alignment mismatch or unsupported UDFs | Verify `spark.sql.iceberg.vectorization.enabled=true`; ensure spatial UDFs are registered as deterministic |

Tracking [Iceberg Spatial Type Support](/spatial-lakehouse-fundamentals-architecture/iceberg-spatial-type-support/) is required for future migration to native geometry primitives, but current production deployments must rely on WKB + bbox normalization to guarantee deterministic query planning. This architecture aligns with core [Spatial Lakehouse Fundamentals & Architecture](/spatial-lakehouse-fundamentals-architecture/) principles to prevent compute lock-in and maintain manifest-level statistics integrity.

For specification compliance, reference the [OGC Simple Features Specification](https://www.ogc.org/standards/sfa) for WKB encoding rules and the [Apache Iceberg Specification](https://iceberg.apache.org/spec/) for manifest statistics generation. Implement strict schema validation at the ingestion boundary, enforce deterministic partitioning, and validate spatial UDF determinism before promoting to production.
