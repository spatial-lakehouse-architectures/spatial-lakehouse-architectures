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

Tracking [Iceberg Spatial Type Support](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/iceberg-spatial-type-support/) is required for future migration to native geometry primitives, but current production deployments must rely on WKB + bbox normalization to guarantee deterministic query planning. This architecture aligns with core [Spatial Lakehouse Fundamentals & Architecture](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/) principles to prevent compute lock-in and maintain manifest-level statistics integrity.

For specification compliance, reference the [OGC Simple Features Specification](https://www.ogc.org/standards/sfa) for WKB encoding rules and the [Apache Iceberg Specification](https://iceberg.apache.org/spec/) for manifest statistics generation. Implement strict schema validation at the ingestion boundary, enforce deterministic partitioning, and validate spatial UDF determinism before promoting to production.

## Why Not Just Store the GeoJSON String

The shortcut is tempting and it appears in almost every first iteration: keep a `STRING` column holding the raw GeoJSON, parse it when needed, and move on. It works, at small scale, and it degrades in four specific ways that are worth naming because each one arrives at a different moment.

<figure class="diagram">
<svg viewBox="0 0 762 284" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Four costs of storing raw GeoJSON strings compared with normalised WKB and derived columns: storage size, parse cost per query, absence of usable statistics, and schema drift from nested properties">
<rect x="0" y="0" width="762" height="284" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Raw GeoJSON string versus normalised columns</text>
<rect x="30" y="56" width="352" height="100" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="206" y="82" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">size</text>
<text x="206" y="106" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">JSON text: 3–6× the WKB bytes</text>
<text x="206" y="130" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">compresses well, still reads slower</text>
<rect x="398" y="56" width="352" height="100" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="574" y="82" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">parse cost</text>
<text x="574" y="106" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">every query re-parses every row</text>
<text x="574" y="130" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">WKB decode is an order faster</text>
<rect x="30" y="172" width="352" height="100" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="206" y="198" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">no usable statistics</text>
<text x="206" y="222" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">min/max of a JSON string is lexical</text>
<text x="206" y="246" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">nothing can be pruned by location</text>
<rect x="398" y="172" width="352" height="100" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="574" y="198" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">schema drift</text>
<text x="574" y="222" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">properties vary row to row</text>
<text x="574" y="246" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">no contract, no validation point</text>
</svg>
</figure>

The third box is the one that decides the argument. A lakehouse table's whole performance model rests on per-file statistics, and a JSON string produces statistics that are lexicographic — the minimum and maximum are the alphabetically first and last strings in the file, which carry no spatial meaning whatsoever. No engine can prune on them, so every location-filtered query becomes a full scan regardless of how well the table is otherwise laid out. Normalising to WKB plus derived numeric columns is not an optimisation on top of a working design; it is what makes the design work at all.

The fourth box matters more slowly but hurts longer. GeoJSON `properties` is an open object, so a producer can add, rename or drop fields without any signal, and a pipeline that unpacks it lazily will find the shape has changed only when a downstream query returns nulls. Declaring the attribute columns explicitly at ingest turns that into a validation failure at the boundary, which is where it can still be fixed cheaply.

## Handling the Parts of GeoJSON That Do Not Map Cleanly

Three constructs in the format have no direct equivalent in a flat columnar table, and each needs a decision made once rather than per pipeline.

<figure class="diagram">
<svg viewBox="0 0 758 258" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three GeoJSON constructs and their normalisation: feature collections become rows, nested property objects become explicit columns or a JSON blob, and the optional bbox member is recomputed rather than trusted">
<defs>
<marker id="gj-map-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#0e6e7d"/></marker>
</defs>
<rect x="0" y="0" width="758" height="258" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Normalising the awkward parts</text>
<rect x="34" y="58" width="264" height="52" rx="8" fill="#ffffff" stroke="#0e6e7d" stroke-width="2"/>
<text x="166" y="90" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">FeatureCollection</text>
<rect x="446" y="58" width="300" height="52" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="596" y="90" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">one row per feature, id preserved</text>
<line x1="298" y1="84" x2="446" y2="84" stroke="#0e6e7d" stroke-width="2" marker-end="url(#gj-map-arrow)"/>
<rect x="34" y="126" width="264" height="52" rx="8" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<text x="166" y="158" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">properties &#123; … &#125;</text>
<rect x="446" y="126" width="300" height="52" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="596" y="151" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">declared columns + a JSON overflow</text>
<text x="596" y="169" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">nothing is silently dropped</text>
<line x1="298" y1="152" x2="446" y2="152" stroke="#2f6e49" stroke-width="2" marker-end="url(#gj-map-arrow)"/>
<rect x="34" y="194" width="264" height="52" rx="8" fill="#ffffff" stroke="#9a5a17" stroke-width="2"/>
<text x="166" y="226" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">bbox member (optional)</text>
<rect x="446" y="194" width="300" height="52" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="596" y="219" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">recomputed, never trusted</text>
<text x="596" y="237" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">producers often leave it stale</text>
<line x1="298" y1="220" x2="446" y2="220" stroke="#9a5a17" stroke-width="2" marker-end="url(#gj-map-arrow)"/>
</svg>
</figure>

The overflow column is the detail that saves the most grief. Declaring the properties you care about as typed columns gives validation and pruning; keeping everything else in a single JSON column means an unexpected new field is preserved rather than lost, and can be promoted to a real column later without re-ingesting the source. Dropping unknown properties silently is the alternative most pipelines implement by accident, and it is unrecoverable once the source has rotated its files away.

The stale `bbox` member deserves its own warning. It is optional, producers frequently emit it once and never update it after an edit, and a pipeline that trusts it inherits a bounding box that does not contain its geometry — which quietly breaks pruning by excluding files that should have matched. Always recompute from the geometry, and if the source `bbox` disagrees by more than a rounding tolerance, log it: a systematic disagreement is useful evidence that the upstream editing tool is not maintaining it.

## Verifying the Normalisation Round-Trips

The final check is that a feature which entered as GeoJSON can leave as equivalent GeoJSON. This catches precision loss, coordinate order mistakes and dimensionality changes in one assertion, and it is fast enough to run on every batch as a sample.

<figure class="diagram">
<svg viewBox="0 0 768 196" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Round trip verification: GeoJSON parsed to a geometry, encoded to WKB, stored, read back, and re-serialised to GeoJSON, with an equality check at a stated tolerance closing the loop">
<defs>
<marker id="gj-rt-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#6a3d9a"/></marker>
</defs>
<rect x="0" y="0" width="768" height="196" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Close the loop on a sample of every batch</text>
<rect x="24" y="66" width="146" height="60" rx="8" fill="#ffffff" stroke="#6a3d9a" stroke-width="2"/>
<text x="97" y="102" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">source GeoJSON</text>
<rect x="212" y="66" width="146" height="60" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="285" y="102" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">WKB in Iceberg</text>
<rect x="400" y="66" width="146" height="60" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="473" y="102" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">read back</text>
<rect x="588" y="66" width="168" height="60" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="672" y="94" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">re-serialised GeoJSON</text>
<text x="672" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">compare at 1e-9</text>
<line x1="170" y1="96" x2="212" y2="96" stroke="#6a3d9a" stroke-width="2" marker-end="url(#gj-rt-arrow)"/>
<line x1="358" y1="96" x2="400" y2="96" stroke="#6a3d9a" stroke-width="2" marker-end="url(#gj-rt-arrow)"/>
<line x1="546" y1="96" x2="588" y2="96" stroke="#6a3d9a" stroke-width="2" marker-end="url(#gj-rt-arrow)"/>
<text x="390" y="180" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">A failure here is always a pipeline bug, never a data problem</text>
</svg>
</figure>

Compare with a geometric equality test at an explicit tolerance rather than with string comparison. Two GeoJSON documents describing the same polygon can differ in whitespace, in coordinate precision, in ring orientation and in the starting vertex, and all four of those differences are meaningless. A tolerance of `1e-9` degrees is roughly a tenth of a millimetre and is a sensible default for data stored at double precision.
