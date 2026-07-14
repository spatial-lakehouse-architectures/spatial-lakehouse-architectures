# DuckDB Geospatial Analytics on Lakehouse Tables

DuckDB turns a laptop or a single warehouse node into a capable spatial query engine that reads GeoParquet and Apache Iceberg data straight from object storage, with no cluster to provision and no JVM to tune. Its `spatial` extension ships an R-tree index, a GEOS-backed predicate library, and native GeoParquet readers, while the `httpfs` and `iceberg` extensions let it pull byte ranges directly from S3. For data engineers who spend their day inside a distributed lakehouse but need fast, interactive spatial exploration — ad-hoc `ST_Intersects` filters, tile validation, or pre-aggregation before a heavier job — an embedded engine removes an enormous amount of operational friction. This topic area sits inside [Spatial Query Engines & Compute Optimization](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/) and covers when single-node DuckDB is the right tool, how to wire it to lakehouse storage, and how to keep spatial joins fast when the working set outgrows memory.

<figure class="diagram">
<svg viewBox="0 0 760 300" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="DuckDB single-node engine reading GeoParquet and Iceberg from object storage through httpfs with an in-process R-tree">
<defs>
<marker id="arw-duckdb-cluster" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#0e6e7d"/></marker>
</defs>
<rect x="0" y="0" width="760" height="300" fill="#f7fbfc"/>
<text x="380" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">DuckDB embedded spatial engine over lakehouse storage</text>
<text x="130" y="60" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#33707d">Object storage (S3)</text>
<rect x="30" y="72" width="200" height="60" rx="8" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<text x="130" y="98" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="600" fill="#0d3b45">GeoParquet files</text>
<text x="130" y="117" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">WKB + bbox stats</text>
<rect x="30" y="150" width="200" height="60" rx="8" fill="#ffffff" stroke="#9a5a17" stroke-width="2"/>
<text x="130" y="176" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="600" fill="#0d3b45">Iceberg table</text>
<text x="130" y="195" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">manifests + data</text>
<rect x="300" y="100" width="150" height="90" rx="8" fill="#ffffff" stroke="#0e6e7d" stroke-width="2"/>
<text x="375" y="132" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">httpfs</text>
<text x="375" y="152" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">ranged GET</text>
<text x="375" y="170" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">predicate pushdown</text>
<rect x="520" y="72" width="210" height="140" rx="10" fill="#ffffff" stroke="#0e6e7d" stroke-width="2.5"/>
<text x="625" y="100" text-anchor="middle" font-family="sans-serif" font-size="14" font-weight="700" fill="#0d3b45">DuckDB process</text>
<rect x="545" y="115" width="160" height="34" rx="6" fill="#f7fbfc" stroke="#6a3d9a" stroke-width="1.5"/>
<text x="625" y="137" text-anchor="middle" font-family="sans-serif" font-size="11" font-weight="600" fill="#6a3d9a">R-tree index</text>
<rect x="545" y="158" width="160" height="34" rx="6" fill="#f7fbfc" stroke="#2f6e49" stroke-width="1.5"/>
<text x="625" y="180" text-anchor="middle" font-family="sans-serif" font-size="11" font-weight="600" fill="#2f6e49">GEOS predicates</text>
<line x1="230" y1="102" x2="300" y2="130" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-duckdb-cluster)"/>
<line x1="230" y1="180" x2="300" y2="160" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-duckdb-cluster)"/>
<line x1="450" y1="145" x2="520" y2="145" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-duckdb-cluster)"/>
<text x="380" y="255" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">Only the row groups whose bbox overlaps the query window are fetched — the rest never leave storage</text>
<text x="380" y="278" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">Single process, single node, no shuffle, no coordinator</text>
</svg>
</figure>

## When to use single-node DuckDB

The decision is almost always about data-scan volume versus operational cost, not about correctness — DuckDB's spatial functions are GEOS-backed and produce the same results as PostGIS or Sedona. Reach for it when the *effective* working set (after partition and bbox pruning) fits comfortably in single-node memory or streams from object storage within acceptable latency, and reach for a distributed engine when a single query must repeatedly shuffle tens of billions of geometries. Use the criteria below.

| Criterion | Prefer DuckDB (single node) | Prefer distributed (Sedona / Trino) |
|---|---|---|
| Effective scan after pruning | Up to ~a few hundred GB streamed, working set fits in RAM or spills locally | Multi-TB scans, working set exceeds one node |
| Concurrency | 1–a few interactive analysts / one pipeline task | Many concurrent tenants sharing a Spark cluster |
| Join cardinality | Point-in-polygon, small-to-mid build side that fits memory | Large-vs-large polygon overlays needing shuffle |
| Latency target | Sub-second to low-seconds interactive | Batch minutes-to-hours acceptable |
| Ops budget | No cluster, embed in Python/CI/notebook | Team already running Spark/Trino |
| Data location | GeoParquet or Iceberg in S3, read-mostly | Federated across many catalogs and sources |

A useful heuristic: if the query would fit on one machine in PostGIS, DuckDB will handle it against the lakehouse without the ETL round-trip. For distributed alternatives, see the sibling topic areas [Trino spatial SQL federation](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/trino-spatial-sql-federation/) and [Sedona distributed spatial compute](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/sedona-distributed-spatial-compute/); to pick objectively across all three, the [engine benchmarking and selection](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/engine-benchmarking-selection/) guide walks through a repeatable methodology.

## Prerequisites and environment setup

You need DuckDB 1.0 or later (spatial extension APIs stabilized at 1.0; these examples were validated on 1.1.x). The `spatial` extension provides geometry types and `ST_*` functions; `httpfs` enables `s3://` access; `iceberg` reads Iceberg metadata. Install the Python client and pin versions in CI so extension autoloading is reproducible.

```bash
pip install "duckdb>=1.1.0" pyarrow
```

```python
import duckdb

con = duckdb.connect()  # in-memory; pass a path for a persistent database file

# Extensions are downloaded once and cached under ~/.duckdb/extensions
con.execute("INSTALL spatial;")
con.execute("LOAD spatial;")
con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")
con.execute("INSTALL iceberg;")
con.execute("LOAD iceberg;")

# S3 credentials via the modern secrets API (preferred over legacy SET s3_* pragmas)
con.execute("""
CREATE OR REPLACE SECRET s3_lakehouse (
    TYPE S3,
    PROVIDER credential_chain,
    REGION 'us-east-1'
);
""")

print(con.execute("SELECT version();").fetchone()[0])
```

The `credential_chain` provider resolves AWS credentials the same way the AWS SDK does — environment variables, `~/.aws/credentials`, or an instance/pod IAM role — so no keys are hardcoded. For MinIO or other S3-compatible stores, add `ENDPOINT 'minio:9000'` and `USE_SSL false` to the secret. Set `memory_limit` and `threads` before running heavy queries; both are covered under performance tuning below.

## Step-by-step implementation

### 1. Read GeoParquet directly from object storage

DuckDB's spatial extension recognizes the GeoParquet `geo` metadata and decodes the WKB geometry column automatically through `ST_Read` or, for column-level control, `read_parquet`. Reading GeoParquet natively means you never materialize an intermediate copy.

```python
# GeoParquet with a "geometry" column encoded as WKB per the GeoParquet spec
parquet_glob = "s3://lakehouse/buildings/geoparquet/*.parquet"

con.execute(f"""
CREATE VIEW buildings AS
SELECT
    id,
    height_m,
    ST_GeomFromWKB(geometry) AS geom   -- decode WKB to DuckDB GEOMETRY
FROM read_parquet('{parquet_glob}', hive_partitioning = true);
""")

count = con.execute("SELECT count(*) FROM buildings;").fetchone()[0]
print(f"buildings rows visible: {count}")
```

If the files were written by GeoPandas or Sedona with the standardized WKB encoding, `ST_GeomFromWKB` is a zero-copy reinterpretation. Understanding exactly how that column is laid out — and why `bbox` covering columns matter — is the subject of [GeoParquet encoding standards](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/geoparquet-encoding-standards/).

### 2. Build an R-tree over the geometry

For repeated spatial joins on a table that stays resident, materialize the geometries and create an R-tree index. The R-tree accelerates the bounding-box refinement stage of predicates like `ST_Intersects` and `ST_Within`.

```python
con.execute("""
CREATE TABLE buildings_mat AS SELECT * FROM buildings;
CREATE INDEX buildings_rtree ON buildings_mat USING RTREE (geom);
""")
```

The R-tree only helps when the optimizer can push a bounding-box comparison against the indexed column; keep predicates in the `ST_Intersects(a, b)` form rather than wrapping the indexed geometry in a transform. The concrete join recipe lives in [how to run ST_Intersects in DuckDB on GeoParquet](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/duckdb-geospatial-analytics/how-to-run-st-intersects-in-duckdb-on-geoparquet/).

### 3. Filter with predicate pushdown on bbox

GeoParquet files written with a `bbox` covering column expose per-row-group min/max statistics that Parquet's row-group pruning can exploit. When you filter on those numeric bbox columns, DuckDB skips entire row groups before decoding any geometry — the single biggest lever for scan reduction against object storage.

```python
# Query window (xmin, ymin, xmax, ymax) in the table's CRS
con.execute("""
SELECT count(*) FROM read_parquet('s3://lakehouse/buildings/geoparquet/*.parquet')
WHERE bbox.xmin <= 13.45 AND bbox.xmax >= 13.40
  AND bbox.ymin <= 52.52 AND bbox.ymax >= 52.50;
""")
```

This numeric bbox predicate is what makes lakehouse spatial queries cheap; the mechanics of how row-group and file skipping propagate through the reader are detailed in [predicate pushdown optimization](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/predicate-pushdown-optimization/).

### 4. Read an Iceberg table

The `iceberg` extension resolves the table's current snapshot from a metadata JSON pointer (or a REST catalog) and hands DuckDB the list of data files to scan. Geometry stored as WKB `BLOB` is decoded on read.

```python
con.execute("""
SELECT
    parcel_id,
    ST_Area(ST_GeomFromWKB(geometry)) AS area_m2
FROM iceberg_scan('s3://warehouse/parcels/metadata/v3.metadata.json')
LIMIT 5;
""")
```

The full catalog-and-metadata walkthrough, including REST catalog wiring and snapshot selection, is in [querying Iceberg tables with the DuckDB spatial extension](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/duckdb-geospatial-analytics/querying-iceberg-tables-with-duckdb-spatial-extension/).

## Verification and testing

Never trust that pruning happened — prove it. Use `EXPLAIN ANALYZE` to confirm the R-tree index scan is chosen and that the number of rows scanned matches expectations, and cross-check row counts against a known-good engine.

```python
plan = con.execute("""
EXPLAIN ANALYZE
SELECT count(*)
FROM buildings_mat b
JOIN districts d ON ST_Intersects(b.geom, d.geom)
WHERE d.name = 'Mitte';
""").fetchall()
for row in plan:
    print(row[1])
```

In the output, look for `RTREE_INDEX_SCAN` (or an index-assisted `PIECEWISE_MERGE_JOIN`) on the buildings side rather than a full `SEQ_SCAN` feeding a nested-loop join. Confirm `Rows Scanned` is a small fraction of the table cardinality. For GeoParquet pushdown, run the query with and without the bbox predicate and compare the `read_parquet` operator's reported bytes read; effective pruning shows up as a large drop. A quick geometry-integrity check catches decoding errors early:

```python
bad = con.execute("""
SELECT count(*) FROM buildings_mat
WHERE NOT ST_IsValid(geom);
""").fetchone()[0]
assert bad == 0, f"{bad} invalid geometries after decode"
```

## Performance and tuning

DuckDB's defaults are conservative for a shared laptop; a dedicated ETL node wants explicit limits. The knobs that matter most for spatial workloads:

- `threads`: default is core count. For CPU-bound GEOS predicate evaluation (`ST_Intersects`, `ST_Contains`), leaving it at physical core count is right; oversubscribing hyperthreads gives ~10–20% at best and can hurt on memory-bound scans.
- `memory_limit`: default is ~80% of RAM. Set it explicitly (e.g. `'12GB'`) so spatial joins spill to disk deterministically instead of getting OOM-killed. When the build side of a spatial join exceeds this, DuckDB spills to `temp_directory` — point that at fast local NVMe, not a network mount.
- `preserve_insertion_order`: set to `false` for large scans to let the engine reorder freely and cut peak memory.
- Partial reads: keep GeoParquet row groups in the 64–128 MB range. Row groups that are too large defeat bbox pruning granularity; too small inflate metadata overhead over `httpfs`.

```python
con.execute("SET threads = 8;")
con.execute("SET memory_limit = '12GB';")
con.execute("SET temp_directory = '/mnt/nvme/duckdb_spill';")
con.execute("SET preserve_insertion_order = false;")
```

Concrete expectations on modern hardware: a point-in-polygon join of ~10M points against ~50k polygons with an R-tree completes in low single-digit seconds on 8 cores; the same without an index degrades to a full cross-check and can be 50–100x slower. Against S3, the dominant cost is round-trip latency, so a query touching many tiny files will be latency-bound regardless of CPU — compact GeoParquet to fewer, larger files first. When a spatial join's build side is genuinely larger than node memory even after pruning, that is the signal to move to [Sedona distributed spatial compute](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/sedona-distributed-spatial-compute/) rather than fight the spill.

For raw scan throughput, prefer numeric bbox predicates over geometry predicates in the `WHERE` clause: the numeric form prunes at the row-group level before any WKB is decoded, whereas a geometry predicate can only filter after decode. Combine both — a cheap bbox pre-filter followed by an exact `ST_Intersects` refinement — to get pruning and correctness together.

## Common errors and fixes

| Symptom | Root cause | Fix |
|---|---|---|
| `Catalog Error: Table Function "iceberg_scan" does not exist` | `iceberg` extension not loaded, or version predates Iceberg support | `INSTALL iceberg; LOAD iceberg;` on DuckDB ≥ 1.0; for pinned builds add `SET unsafe_enable_version_guessing=true` only if metadata pointer resolution fails |
| `IO Error: Connection error ... 403` on `s3://` | Credentials not resolved or wrong region | Create an S3 secret with `PROVIDER credential_chain` and correct `REGION`; verify the IAM role can `s3:GetObject` on the prefix |
| Spatial join runs but ignores the R-tree (full `SEQ_SCAN` in plan) | Indexed geometry wrapped in a function (e.g. `ST_Transform`) so bbox can't be extracted | Pre-transform into a stored column, index that; keep the predicate as `ST_Intersects(indexed_geom, param)` |
| `Out of Memory Error` during a large spatial join | Build side exceeds `memory_limit`, spill dir on slow/absent disk | Lower `memory_limit` to force early spill, set `temp_directory` to local NVMe, or pre-filter with a bbox predicate to shrink the build side |
| GeoParquet reads but `geom` is `BLOB`, `ST_*` errors | Column still raw WKB, not decoded to GEOMETRY | Wrap with `ST_GeomFromWKB(geometry)`; confirm the file's `geo` metadata declares WKB encoding |

DuckDB's spatial engine rewards a workflow of prune-then-refine: filter on bbox statistics, decode only what survives, and index anything you join repeatedly. Wired to GeoParquet and Iceberg through `httpfs`, it gives interactive spatial SQL over the same lakehouse tables your distributed jobs write — without a Spark cluster in the loop. For the authoritative function reference and extension internals, consult the official [DuckDB spatial extension documentation](https://duckdb.org/docs/stable/extensions/spatial/overview) and the [GeoParquet specification](https://geoparquet.org/releases/v1.1.0/); for the geometry predicate semantics themselves, the [OGC Simple Features standard](https://www.ogc.org/standards/sfa/) is the source of record.
