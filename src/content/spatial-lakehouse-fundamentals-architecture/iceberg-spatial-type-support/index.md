# Iceberg Spatial Type Support: Production Architecture & Operational Guide

Implementing spatial workloads on open table formats requires deliberate alignment between storage semantics, query engines, and metadata management. Within the broader [Spatial Lakehouse Fundamentals & Architecture](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/) framework, Iceberg's spatial type support operates at the intersection of columnar storage optimization and geospatial predicate evaluation. Unlike proprietary GIS databases that embed spatial indexes directly into storage layers, Iceberg delegates filtering to manifest-level statistics, deterministic partition transforms, and engine-level UDFs. This architectural shift eliminates vendor lock-in but demands explicit configuration at the table, partition, and pipeline layers to prevent full-table scans on geometry columns.

## Schema Design & CRS Enforcement

Iceberg's V3 specification introduces native `geometry` and `geography` types, but engine support is still uneven, so most production deployments continue to treat spatial columns as structured binary or string representations. Geometry payloads are stored as `BINARY` (WKB) columns; query engines such as Spark with Apache Sedona or Trino with its geospatial plugin interpret these payloads at runtime. To guarantee cross-engine interoperability, enforce strict Coordinate Reference System (CRS) validation at ingestion. Platform architects should standardize on EPSG:4326 (WGS84) for global datasets, storing the CRS identifier alongside the geometry to prevent silent coordinate drift during downstream joins.

Schema evolution is handled through Iceberg's explicit metadata tracking. When adding or altering spatial columns, leverage the format's backward-compatible type promotion rules to avoid breaking consumers. For detailed versioning strategies, consult the [Open Table Format Versioning](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/open-table-format-versioning/) guidelines to align snapshot progression with spatial schema migrations.

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

## Where Spatial Metadata Lives in an Iceberg Table

Iceberg's advantage for spatial work is not that it understands geometry — for most deployments it does not — but that its metadata layer is rich enough to carry the numbers that make geometry fast, and to carry them at three separate granularities that the planner uses in sequence.

<figure class="diagram">
<svg viewBox="0 0 700 324" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Iceberg metadata hierarchy for spatial pruning: table metadata holds the schema and partition spec, the manifest list holds per-manifest partition summaries, each manifest holds per-file bounding box statistics, and the data files hold row groups">
<defs>
<marker id="ice-meta-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#0e6e7d"/></marker>
</defs>
<rect x="0" y="0" width="700" height="324" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Four levels, each pruning before the next is read</text>
<rect x="230" y="50" width="320" height="54" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="390" y="72" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">table metadata (one JSON)</text>
<text x="390" y="92" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">schema, partition spec, sort order, properties</text>
<rect x="184" y="122" width="412" height="54" rx="8" fill="#ffffff" stroke="#0e6e7d" stroke-width="2"/>
<text x="390" y="144" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">manifest list (one per snapshot)</text>
<text x="390" y="164" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">partition value ranges per manifest — skips whole manifests</text>
<rect x="138" y="194" width="504" height="54" rx="8" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<text x="390" y="216" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">manifests (many)</text>
<text x="390" y="236" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">per-file lower/upper bounds for bbox_min_x … bbox_max_y</text>
<rect x="92" y="266" width="596" height="46" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="390" y="295" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">data files — row groups, then WKB decode</text>
<line x1="390" y1="104" x2="390" y2="122" stroke="#0e6e7d" stroke-width="2" marker-end="url(#ice-meta-arrow)"/>
<line x1="390" y1="176" x2="390" y2="194" stroke="#0e6e7d" stroke-width="2" marker-end="url(#ice-meta-arrow)"/>
<line x1="390" y1="248" x2="390" y2="266" stroke="#0e6e7d" stroke-width="2" marker-end="url(#ice-meta-arrow)"/>
</svg>
</figure>

The **manifest list** level is the one with no equivalent in a flat design, and on large tables it is the difference between planning in 200 milliseconds and planning in thirty seconds. Each manifest carries the range of partition values it covers, so a query scoped to two grid cells reads two manifests out of four hundred. This level only helps if the partition column is the one the query filters on — which is why a derived grid identifier as a partition field is worth so much more in Iceberg than a bare geometry column ever could be.

The **manifest** level carries per-file lower and upper bounds for every column Iceberg tracks, and those bounds are how bounding-box pruning happens. Two properties govern whether they exist: `write.metadata.metrics.default` (commonly `truncate(16)`, which is useless for doubles) and per-column overrides. Set the bbox columns to `full` explicitly. A table whose bbox columns are tracked at `counts` will plan perfectly and read everything.

```sql
-- Iceberg 1.4+. Without these properties the bbox columns get no usable bounds.
ALTER TABLE lakehouse.spatial.telemetry SET TBLPROPERTIES (
  'write.metadata.metrics.column.bbox_min_x' = 'full',
  'write.metadata.metrics.column.bbox_min_y' = 'full',
  'write.metadata.metrics.column.bbox_max_x' = 'full',
  'write.metadata.metrics.column.bbox_max_y' = 'full',
  'write.metadata.metrics.column.geom_wkb'   = 'none'
);
```

Setting the geometry column itself to `none` is deliberate. Iceberg would otherwise store min/max of the WKB bytes, which is meaningless, consumes manifest space, and on wide geometries can bloat the manifest enough to slow planning measurably.

## Hidden Partitioning and Spatial Transforms

Iceberg's hidden partitioning is the feature that most changes day-to-day spatial ergonomics, and it is also the one most often used incorrectly.

In a Hive-style layout, a query must filter on the partition column by name, so callers have to know that the table is partitioned by `h3_r5` and compute that value themselves. Iceberg instead records a *transform* from a source column to a partition value, and the planner applies the same transform to predicates automatically. A filter on the source column prunes partitions without the caller knowing the partitioning exists.

For time this works beautifully: `days(event_ts)` means a filter on `event_ts` prunes days. For space, there is a gap — Iceberg's built-in transforms are `identity`, `bucket`, `truncate`, `year/month/day/hour`, and none of them computes a spatial cell. So the honest pattern is a **materialised cell column with an identity transform**: derive `h3_r5` at write time, partition by `identity(h3_r5)`, and accept that queries must reference it.

The way to keep that from leaking into every query is a view that does the derivation once:

```sql
-- Callers filter on a bounding box; the view supplies the partition predicate.
CREATE VIEW lakehouse.spatial.telemetry_geo AS
SELECT *, h3_cell_to_boundary_wkb(h3_r5) AS cell_geom
FROM lakehouse.spatial.telemetry;
```

`bucket(N, ...)` deserves a specific warning in spatial contexts. It is a hash, so bucketing on a coordinate or a cell identifier destroys locality entirely: adjacent cells land in unrelated buckets, and a spatial range predicate must read all of them. Bucket transforms are the right tool for high-cardinality identifiers such as `asset_id`, and precisely the wrong tool for anything with spatial meaning.

Finally, **partition evolution is genuinely usable here** in a way it is not elsewhere. Because the transform is recorded per snapshot, a table can move from resolution 4 to resolution 5 without rewriting history, and queries plan correctly against the mixture. This is the cleanest available answer to the density-shift problem, provided the rewrite of old data is scheduled rather than assumed.


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

Ingestion pipelines must guarantee that `geohash` is computed deterministically (e.g., using a spatial UDF that calls `ST_GeoHash(boundary, precision => 6)`). If upstream writes are unsorted, schedule `rewrite_data_files` with a spatial sort order to maintain clustering efficiency. Unlike [Delta Lake Geometry Handling](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/delta-lake-geometry-handling/), which relies on post-write `ZORDER BY` optimization, Iceberg's `write.sort-order` enforces clustering at write time, reducing compaction overhead but requiring strict pipeline discipline.

## The Geometry Type in Iceberg V3, and What to Do Until It Arrives

Iceberg's specification has moved towards first-class geometry and geography types, and the direction of travel matters for decisions being made now — but so does the gap between a specification containing a type and every engine in a production stack supporting it.

A native geometry type changes three things. The schema stops lying: a column declared `geometry` cannot be mistaken for an arbitrary blob, and a writer that emits malformed bytes can be rejected at commit rather than at read. The metadata layer gains spatial semantics: lower and upper bounds become genuine bounding boxes maintained by the format itself, so the manually materialised bbox columns that most of this site recommends become redundant rather than mandatory. And the CRS becomes a property of the column, carried in the schema, so a reader can no longer forget to ask.

That is a materially better world, and it is arriving unevenly. The practical question for a table being designed today is what happens to it during the transition. The answer that ages best is to build as though the type does not exist, because a table with explicit WKB and explicit bbox columns is readable by every engine including future ones, whereas a table using a type that half the stack does not implement is readable by half the stack.

Concretely: keep the geometry in a `binary` column with WKB, keep the four bbox doubles, and keep the CRS in table properties. When the native type is available across every engine that reads the table, migration is a schema change plus a rewrite — mechanical, schedulable, and reversible — rather than an emergency. The bbox columns can then be dropped, or retained as a belt-and-braces measure for engines whose spatial predicate pushdown is less mature than their type support.

The one decision worth making early is **naming**. Call the geometry column `geometry`, not `geom_wkb` or `shape` or `the_geom`, because tooling that discovers spatial columns by convention — including GeoParquet readers and several catalogue crawlers — looks for that name first. A rename during migration is cheap; a rename after fifty downstream queries reference the old name is not.

## Validating That a Spatial Table Is Actually Configured

Every recommendation on this page is a table property or a column that can silently be absent, and a table that is missing them behaves correctly while performing terribly. That combination — correct but slow, with no error anywhere — is exactly what an assertion belongs on.

The check below reads the table's metadata through PyIceberg and fails when the spatial contract is not met. It is fast enough to run on every deployment and against every table in a catalogue nightly, and it catches the two failure modes that account for most of the incidents: a table created by a path that did not apply the properties, and a table whose properties were reset by a tool that recreated it.

```python
# PyIceberg 0.7+. Assert the spatial contract on a table's metadata alone — no data read.
from pyiceberg.catalog import load_catalog

REQUIRED_BBOX = ("bbox_min_x", "bbox_min_y", "bbox_max_x", "bbox_max_y")

def audit_spatial_table(catalog_name: str, identifier: str) -> list[str]:
    table = load_catalog(catalog_name).load_table(identifier)
    schema, props = table.schema(), table.properties
    problems = []

    names = {f.name for f in schema.fields}
    missing = [c for c in REQUIRED_BBOX if c not in names]
    if missing:
        problems.append(f"missing bounding-box columns: {missing}")

    for col in REQUIRED_BBOX:
        key = f"write.metadata.metrics.column.{col}"
        if props.get(key) != "full":
            problems.append(f"{col} metrics are '{props.get(key, 'default')}', expected 'full'")

    if "spatial.crs" not in props:
        problems.append("table does not declare spatial.crs — readers must guess the CRS")

    spec_fields = {f.name for f in table.spec().fields}
    if not spec_fields:
        problems.append("table is unpartitioned — acceptable only below a few hundred GB")

    sort_cols = {f.source_id for f in table.sort_order().fields}
    if not sort_cols:
        problems.append("no sort order declared — row-group statistics will not prune")

    return problems
```

Run it as a catalogue sweep rather than per table. The output is a short list of tables that will disappoint someone, ordered by how much data sits behind each, and it is the most useful half-hour of maintenance available on a spatial platform. Wire the same function into the pull-request pipeline described in [schema validation pipeline for geospatial tables](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/lakehouse-maintenance-automation/schema-validation-pipeline-for-geospatial-tables/) so a table created by new code cannot be merged without the contract in place.

One refinement worth adding once the basics hold: assert that the declared CRS matches the coordinate ranges actually present. A table declaring 4326 whose `bbox_max_x` upper bound is 1,492,245 is misconfigured, and the metadata alone is enough to prove it — no scan required, because Iceberg already stores the bound.


## Predicate Pushdown & Manifest Statistics

Spatial filtering performance depends on the accuracy of min/max statistics captured in Iceberg manifest files. For binary geometry columns, engines must extract bounding box (BBOX) coordinates during write and store them as separate `DOUBLE` columns to enable manifest-level skipping. Query engines will push down predicates on `bbox_min_x`, `bbox_max_x`, etc., skipping Parquet files whose bounding boxes do not intersect the query window.

**Debugging Pushdown Failures:**
1. **Verify bounding box columns exist and are typed `DOUBLE`**: The Iceberg manifest stores per-column min/max; if bbox columns are absent, the engine cannot prune files.
2. **Inspect manifest stats**: `SELECT file_path, lower_bounds, upper_bounds FROM catalog.gis.traffic_zones.files LIMIT 10;`
3. **Check for `null` bounds**: If bbox columns contain nulls, the engine defaults to full-table scans. Enforce `NOT NULL` constraints on bbox columns and reject records that fail extraction.
4. **Engine version**: Spatial predicate pushdown via bbox columns works in Spark 3.3+ with Iceberg 1.1+. Confirm with `EXPLAIN` that bbox predicates appear in `PushedFilters`.

## The Sort Order Is a Table Property, Not a Job Parameter

Iceberg records a declared sort order in table metadata, and writers that honour it produce files whose row groups already have tight bounding boxes. This is distinct from a one-off sort in a rewrite job, and the difference shows up on every append.

<figure class="diagram">
<svg viewBox="0 0 721 264" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Two files holding identical rows: one written in arrival order where every row group spans the full extent, and one written under a declared sort order where each row group covers a compact area">
<rect x="0" y="0" width="721" height="264" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Same rows, same file size, different row-group extents</text>
<text x="196" y="62" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#9a5a17">arrival order</text>
<rect x="86" y="78" width="220" height="150" rx="6" fill="#ffffff" stroke="#9a5a17" stroke-width="2"/>
<rect x="98" y="90" width="196" height="126" fill="#f2e8da" stroke="#9a5a17" stroke-width="1"/>
<rect x="104" y="96" width="184" height="114" fill="none" stroke="#9a5a17" stroke-width="1"/>
<rect x="110" y="102" width="172" height="102" fill="none" stroke="#9a5a17" stroke-width="1"/>
<text x="196" y="158" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">every row group spans</text>
<text x="196" y="176" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">the whole extent</text>
<text x="584" y="62" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#2f6e49">declared sort order</text>
<rect x="474" y="78" width="220" height="150" rx="6" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<rect x="486" y="90" width="62" height="58" fill="#e6f0ea" stroke="#2f6e49" stroke-width="1"/>
<rect x="556" y="90" width="62" height="58" fill="#e6f0ea" stroke="#2f6e49" stroke-width="1"/>
<rect x="626" y="90" width="56" height="58" fill="#e6f0ea" stroke="#2f6e49" stroke-width="1"/>
<rect x="486" y="156" width="62" height="60" fill="#e6f0ea" stroke="#2f6e49" stroke-width="1"/>
<rect x="556" y="156" width="62" height="60" fill="#e6f0ea" stroke="#2f6e49" stroke-width="1"/>
<rect x="626" y="156" width="56" height="60" fill="#e6f0ea" stroke="#2f6e49" stroke-width="1"/>
<text x="584" y="248" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">a selective query reads one or two groups</text>
<text x="196" y="248" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">a selective query reads all of them</text>
</svg>
</figure>

Declare it once with `ALTER TABLE ... WRITE ORDERED BY (bbox_min_x, bbox_min_y)` and every subsequent writer that respects the property produces well-formed files without the job needing to know. Writers that ignore it — some streaming paths do — still produce correct data, which is why the compaction job remains necessary rather than optional.

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

When ingesting unstructured spatial payloads, refer to [How to store GeoJSON in Apache Iceberg tables](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/iceberg-spatial-type-support/how-to-store-geojson-in-apache-iceberg-tables/) for serialization patterns that preserve topology without inflating Parquet row groups.

## Position Deletes, Equality Deletes and Spatial Reads

Merge-on-read tables carry delete files alongside data files, and the two kinds behave very differently under a spatial query.

<figure class="diagram">
<svg viewBox="0 0 752 222" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Position deletes reference specific file offsets and can be applied to only the data files a spatial query reads, while equality deletes apply by predicate and must be evaluated against every scanned file">
<rect x="0" y="0" width="752" height="222" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Delete-file kind decides whether pruning still works</text>
<rect x="40" y="58" width="340" height="152" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="210" y="84" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">position deletes</text>
<text x="210" y="110" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">&#8220;file X, rows 12, 480, 9931&#8221;</text>
<text x="210" y="134" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">scoped to named data files</text>
<text x="210" y="158" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">only loaded for files actually read</text>
<text x="210" y="186" text-anchor="middle" font-family="sans-serif" font-size="11" font-weight="700" fill="#2f6e49">pruning preserved</text>
<rect x="400" y="58" width="340" height="152" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="570" y="84" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">equality deletes</text>
<text x="570" y="110" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">&#8220;every row where asset_id = 77&#8221;</text>
<text x="570" y="134" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">no file scope at all</text>
<text x="570" y="158" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">evaluated against every scanned file</text>
<text x="570" y="186" text-anchor="middle" font-family="sans-serif" font-size="11" font-weight="700" fill="#9a5a17">cost grows with delete count</text>
</svg>
</figure>

For spatial tables the practical guidance is to prefer position deletes and to compact them aggressively. Equality deletes accumulate silently on a streaming upsert path and turn a well-pruned query into one that loads a growing delete set on every read, with no change to the plan that would make the cause obvious.

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

### Keeping the Contract Visible to Callers

A configured table is only half the work; the other half is making the configuration discoverable by the people who write queries against it. Add a table comment that states the CRS, the geometry encoding, the dimensionality and the partition column in one sentence, because that comment surfaces in every catalogue browser, every BI tool's schema panel and every `DESCRIBE` output. A caller who can read "WKB, 4326, 2D, partitioned by h3_r5" without leaving their editor will write a query that prunes; a caller who cannot will write `ST_Intersects` against the raw geometry and wonder why it takes four minutes. Column comments on the four bounding-box columns are worth the same small effort — name them as the pruning columns and say which predicate shape uses them, so the fastest query is also the obvious one.
