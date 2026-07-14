# Spatial Query Engines & Compute Optimization for Lakehouse Architectures

The compute plane is where a spatial lakehouse either delivers sub-second geospatial SQL or collapses into full-table scans and out-of-memory shuffles. This section is for data engineers and platform architects who have already landed geometry into open table formats and now need to choose and tune the engine that runs `ST_Intersects`, `ST_DWithin`, and large spatial joins against that storage. The decisive property of the modern stack is that the engine is decoupled from the table: DuckDB, Trino, and Apache Sedona on Spark all read the same Iceberg, Delta Lake, and GeoParquet files, but they consume spatial statistics, evaluate predicates, and distribute work in profoundly different ways. Picking the wrong one — or configuring the right one badly — is the most common cause of slow spatial analytics at scale.

Because the storage and catalog planes are shared, engine selection is not a lock-in decision the way a monolithic spatial database is. You can run DuckDB for interactive exploration, Trino for federated dashboards, and Sedona for nightly batch joins over the *same* GeoParquet-backed Iceberg tables. The cost of that flexibility is that each engine has its own `ST_*` dialect, its own approach to bounding-box pruning, and its own geometry serialization contract. The rest of this page walks the trade-offs, the mechanics of predicate pushdown per engine, three runnable production patterns, the failure modes that recur across teams, and an operational readiness checklist.

<figure class="diagram">
<svg viewBox="0 0 760 300" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three spatial query engines reading a shared open-table storage and catalog plane">
<title>Decoupled engine-over-open-table compute model</title>
<desc>DuckDB, Trino, and Apache Sedona on Spark all read the same GeoParquet files registered in Iceberg and Delta Lake tables through a shared catalog that exposes bounding-box statistics for predicate pushdown.</desc>
<rect x="0" y="0" width="760" height="300" fill="#f7fbfc"/>
<text x="380" y="26" text-anchor="middle" font-family="sans-serif" font-size="15" fill="#0d3b45" font-weight="bold">Decoupled compute over one open-table storage plane</text>
<!-- Engine row -->
<rect x="40" y="46" width="200" height="60" rx="6" fill="#ffffff" stroke="#0e6e7d" stroke-width="2"/>
<text x="140" y="72" text-anchor="middle" font-family="sans-serif" font-size="14" fill="#0d3b45" font-weight="bold">DuckDB</text>
<text x="140" y="92" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">single-node, embedded</text>
<rect x="280" y="46" width="200" height="60" rx="6" fill="#ffffff" stroke="#6a3d9a" stroke-width="2"/>
<text x="380" y="72" text-anchor="middle" font-family="sans-serif" font-size="14" fill="#0d3b45" font-weight="bold">Trino</text>
<text x="380" y="92" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">federated MPP SQL</text>
<rect x="520" y="46" width="200" height="60" rx="6" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<text x="620" y="72" text-anchor="middle" font-family="sans-serif" font-size="14" fill="#0d3b45" font-weight="bold">Sedona / Spark</text>
<text x="620" y="92" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">distributed joins</text>
<!-- arrows down to catalog -->
<defs>
<marker id="arw-sqe" markerWidth="9" markerHeight="9" refX="7" refY="4" orient="auto"><path d="M0,0 L8,4 L0,8 z" fill="#33707d"/></marker>
</defs>
<line x1="140" y1="106" x2="140" y2="140" stroke="#33707d" stroke-width="1.6" marker-end="url(#arw-sqe)"/>
<line x1="380" y1="106" x2="380" y2="140" stroke="#33707d" stroke-width="1.6" marker-end="url(#arw-sqe)"/>
<line x1="620" y1="106" x2="620" y2="140" stroke="#33707d" stroke-width="1.6" marker-end="url(#arw-sqe)"/>
<!-- Catalog plane -->
<rect x="40" y="146" width="680" height="52" rx="6" fill="#ffffff" stroke="#9a5a17" stroke-width="2"/>
<text x="380" y="168" text-anchor="middle" font-family="sans-serif" font-size="13" fill="#0d3b45" font-weight="bold">Catalog plane — Iceberg / Delta metadata + bbox min/max statistics</text>
<text x="380" y="187" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">manifests, snapshots, CRS metadata, file-level bounding boxes for pruning</text>
<line x1="380" y1="198" x2="380" y2="230" stroke="#33707d" stroke-width="1.6" marker-end="url(#arw-sqe)"/>
<!-- Storage plane -->
<rect x="40" y="236" width="680" height="52" rx="6" fill="#ffffff" stroke="#cfe3e7" stroke-width="2"/>
<text x="380" y="258" text-anchor="middle" font-family="sans-serif" font-size="13" fill="#0d3b45" font-weight="bold">Storage plane — GeoParquet (WKB geometry column) on S3 / ADLS / GCS</text>
<text x="380" y="277" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">one physical copy of the data, read by every engine above</text>
</svg>
</figure>

## Core design decisions and trade-offs

Choosing a spatial engine is a workload-shape decision, not a preference. The three mainstream open-source options occupy distinct points on the axes of data size, concurrency, deployment complexity, and spatial-function maturity. The comparison below states each option's sweet spot and an explicit recommendation.

**1. DuckDB with the `spatial` extension — single-node, embedded, GeoParquet-native.** DuckDB (spatial extension ≥ 1.0) runs in-process with zero cluster to operate. Its spatial extension is built on GEOS and GDAL, so `ST_*` coverage is broad and OGC-faithful, and it reads GeoParquet directly with native Arrow-backed columnar scans. It vectorizes predicate evaluation and can push bounding-box filters down to Parquet row groups. The hard ceiling is memory and cores on one machine: it excels up to tens of gigabytes of geometry, or larger with careful projection and predicate pushdown, but has no shuffle across nodes for a billion-by-billion spatial join.

- *Pros:* trivial deployment, fastest cold-start for interactive analysis, first-class GeoParquet and `read_parquet` support, excellent for notebooks and CI validation.
- *Cons:* single-node ceiling; no native distributed join; large joins spill or fail.
- *Recommendation:* **Default choice for interactive exploration, ad-hoc analytics, CI checks, and any dataset that fits comfortably on one node.** See [DuckDB geospatial analytics](/spatial-query-engines-compute/duckdb-geospatial-analytics/) for the extension-loading and Iceberg-attach patterns.

**2. Trino — federated MPP SQL across catalogs.** Trino is a distributed query engine that shines when the query must span multiple catalogs — join an Iceberg spatial table to a PostgreSQL reference table to a Hive-registered raster catalog in one SQL statement — and serve many concurrent BI users. Its geospatial functions are based on Esri Geometry and use a `Geometry` type constructed via `ST_GeomFromBinary` / `ST_GeometryFromText`. Trino distributes work across workers but its spatial join optimizer is less specialized than Sedona's; it benefits enormously from pre-computed bounding-box columns and `KDB`-style spatial partitioning done upstream.

- *Pros:* cross-catalog federation, high concurrency, mature SQL, connects to Iceberg and Delta without moving data.
- *Cons:* narrower and differently-named `ST_*` set; spatial joins need bbox helper columns to be fast; no columnar geometry compute as tight as DuckDB.
- *Recommendation:* **Use for federated dashboards, multi-source spatial SQL, and concurrent interactive querying** where the data lives across several catalogs. See [Trino spatial SQL federation](/spatial-query-engines-compute/trino-spatial-sql-federation/).

**3. Apache Sedona on Spark — distributed spatial joins at scale.** Sedona extends Spark SQL with spatial types, a spatial partitioner, and — critically — spatial-join strategies (broadcast index join, partitioned range join) that no other engine matches for very large joins. When you must join hundreds of millions of points against millions of polygons, Sedona builds spatial partitions and local R-tree/quadtree indexes so the join is not an O(n·m) cartesian shuffle. The price is Spark's operational weight: a standing compute cluster, JVM tuning, and slower cold-start.

- *Pros:* genuine distributed spatial joins, spatial partitioning + indexing, scales to billions of geometries, rich `ST_*` library, reads GeoParquet/Iceberg/Delta.
- *Cons:* cluster to run and tune; heaviest cold-start; shuffle blowups if joins are not broadcast or partitioned correctly.
- *Recommendation:* **The engine for large batch spatial joins and enrichment pipelines that exceed one node.** See [Sedona distributed spatial compute](/spatial-query-engines-compute/sedona-distributed-spatial-compute/).

There is no single winner. The disciplined pattern is to benchmark candidate engines against *your* query shapes and data volumes rather than trusting vendor microbenchmarks; the methodology for that lives in [engine benchmarking and selection](/spatial-query-engines-compute/engine-benchmarking-selection/). As a rule of thumb: fits-on-a-node interactive → DuckDB; federated concurrent SQL → Trino; distributed batch joins → Sedona.

A fourth axis worth weighing is operational cost per query. DuckDB has effectively zero standing cost — it spins up in the process that needs it and dies with it, which makes it ideal for serverless functions and CI runners. Trino carries a standing coordinator-plus-workers cluster that amortizes across high concurrency; it is expensive if it sits idle but cheap per query under load, which is exactly the profile of an interactive analytics tier. Sedona inherits Spark's job-submission overhead — tens of seconds of cluster acquisition and DAG scheduling before the first byte is read — so it is uneconomical for sub-second interactive queries but the only viable option once a single join exceeds one machine's memory. Mapping these cost curves onto your actual query mix, rather than a single representative query, is what prevents a team from over-provisioning a Spark cluster for workloads DuckDB would answer in-process, or from hammering a single DuckDB process with a join that will never fit.

## Geometry serialization and the shared contract

Because all three engines read the same files, the serialization contract at the storage layer is what makes the decoupled model work — or breaks it silently. The lingua franca is Well-Known Binary (WKB) geometry stored in a Parquet column, described by GeoParquet metadata that records the geometry encoding, the geometry column name, and the CRS. Getting this metadata right is a storage-plane concern covered in depth under [GeoParquet encoding standards](/spatial-lakehouse-fundamentals-architecture/geoparquet-encoding-standards/), but every engine on the compute plane depends on it.

A minimal GeoParquet file-level metadata block (JSON stored in the Parquet key-value metadata under the `geo` key) looks like this:

```json
{
  "version": "1.1.0",
  "primary_column": "geometry",
  "columns": {
    "geometry": {
      "encoding": "WKB",
      "geometry_types": ["Point", "MultiPolygon"],
      "crs": {
        "type": "GeographicCRS",
        "id": { "authority": "EPSG", "code": 4326 }
      },
      "bbox": [-124.7, 24.5, -66.9, 49.4]
    }
  }
}
```

Two rules matter for the compute plane. First, keep the CRS consistent across every file in a table — mixing EPSG:4326 and EPSG:3857 geometries in one column forces engines to either error or silently produce wrong distances, because none of them will reproject on the fly during a predicate evaluation. In code you pass the numeric literal (`4326`); in metadata and prose you name it as EPSG:4326. Second, materialize explicit bounding-box columns (`bbox_minx`, `bbox_miny`, `bbox_maxx`, `bbox_maxy` as `DOUBLE`) alongside the WKB geometry. The GeoParquet `bbox` metadata is per-file; the four `DOUBLE` columns give the table format's manifests per-file min/max statistics that *every* engine's optimizer can prune on, even engines that do not natively parse GeoParquet's `geo` metadata.

## How each engine consumes predicate pushdown and bbox statistics

Spatial predicate pushdown is the single biggest lever on query latency, and it works differently in each engine. The shared prerequisite — file-level bounding-box statistics — is discussed generally in [predicate pushdown optimization](/spatial-partitioning-indexing-strategies/predicate-pushdown-optimization/); here is how the three engines actually consume it.

**DuckDB** pushes predicates on the four `DOUBLE` bbox columns down to Parquet row groups and skips groups whose min/max ranges do not intersect the query window. It does not (as of the current spatial extension) push a raw `ST_Intersects(geom, ...)` down to file statistics — you must express the coarse filter on the numeric bbox columns and let `ST_Intersects` run only on surviving rows. The idiomatic pattern is a two-stage filter: cheap bbox range predicate first, exact `ST_*` predicate second.

**Trino** prunes at the split level using Iceberg/Delta manifest statistics on the bbox columns. Its cost-based optimizer will reorder a spatial join to broadcast the smaller side, but only if table statistics are present — run `ANALYZE` on Iceberg tables so Trino sees NDV and min/max. Like DuckDB, Trino benefits from an explicit bbox pre-filter; a bare `ST_Contains` on WKB will not prune files by itself.

**Sedona** goes further: beyond manifest-level file pruning on bbox columns, it repartitions both sides of a join by a shared spatial grid and builds an in-memory tree index per partition, so the join predicate is evaluated only against spatially co-located candidates. This is why Sedona wins large joins — the pruning happens not just at file granularity but at the join-algorithm level. It still reads faster when the underlying files are spatially clustered, so upstream spatial partitioning compounds with Sedona's own partitioner.

The common thread: bounding-box statistics in the manifest are the currency of pruning, and none of the engines reproject or reason across CRS boundaries during pushdown. Whether you store geometry in Iceberg or Delta changes *where* those statistics live — the trade-offs are covered in [Iceberg vs Delta Lake for spatial data](/spatial-lakehouse-fundamentals-architecture/open-table-format-versioning/iceberg-vs-delta-lake-for-spatial-data/) — but the compute-plane contract is identical.

It is worth being precise about the two distinct pruning layers, because teams routinely conflate them and then wonder why a query still scans everything. The first layer is *file/split pruning*: the table format's manifest holds per-file min/max for each of the four bbox columns, and the engine's planner discards whole files before any I/O against them. The second layer is *row-group pruning* inside a surviving Parquet file: the file footer holds per-row-group statistics, and a columnar engine skips row groups whose ranges miss the window. DuckDB is strong at the second layer because it reads Parquet natively; Trino and Sedona lean on the first layer via the Iceberg/Delta connector. A query that filters only on `ST_Intersects` and never touches the numeric bbox columns defeats *both* layers — the geometry blob has no manifest or row-group range the planner can reason about — which is why the two-stage filter pattern (numeric bbox predicate first, exact `ST_*` second) is non-negotiable for performance regardless of engine.

## Engine execution mechanics: memory, concurrency, and the join boundary

The trade-off table above is ultimately a consequence of *how* each engine turns SQL into work, and understanding the mechanics makes the recommendations self-evident rather than arbitrary. The decisive question for any spatial workload is where the join happens and what memory it needs.

DuckDB executes as a vectorized, pull-based pipeline entirely within one process's address space. A spatial join hashes or nested-loops the two inputs in RAM (spilling to a temp directory under memory pressure), which is why its ceiling is a function of that one machine's memory and why it is astonishingly fast when both sides fit. There is no network shuffle, no serialization across a wire, and no scheduler overhead — the entire query is a tight loop over Arrow-shaped column batches. This is the property that makes it the right default for the interactive tier and for CI, where predictable low-latency execution against bounded data matters more than horizontal scale.

Trino distributes a query across worker nodes as a tree of stages connected by exchanges. A spatial join is planned as a distributed join whose smaller side the cost-based optimizer may broadcast to every worker; the larger side is partitioned and streamed. The critical dependency is statistics — without them Trino cannot tell which side is smaller and may partition both, producing an expensive redistribution. Because Trino's spatial functions are scalar and its join operators are general-purpose (not spatially aware), it does not build spatial indexes at join time the way Sedona does; its speed on spatial joins comes almost entirely from the bbox pre-filter shrinking the candidate set before the exact predicate runs. This is why federated, high-concurrency SQL over well-partitioned data is its sweet spot and very large geometry-vs-geometry joins are not.

Sedona changes the join algorithm itself. Its `RangeJoin` and `DistanceJoin` strategies repartition both inputs onto a shared spatial grid so that geometries which could possibly match land in the same Spark partition, then build a JTS-based tree index (quadtree or R-tree) per partition and probe it. The result is that a hundred-million-by-ten-million join evaluates each geometry against only its spatial neighbours rather than the full cross-product. The `BROADCAST` variant skips the shuffle of the small side entirely by shipping it to every executor and indexing it once per task. These strategies are the entire reason Sedona exists as a distinct engine, and they are also its main failure surface: if the join does not trigger a spatial strategy — because the predicate is not recognized, or a hint is missing — Spark falls back to a cartesian shuffle and the job dies. The distributed-join details, including how to confirm the spatial strategy fired in the physical plan, are the subject of [Sedona distributed spatial compute](/spatial-query-engines-compute/sedona-distributed-spatial-compute/).

## ST_* function coverage differences

The three engines do not share a spatial-SQL dialect, and this is the sharpest edge for teams that move queries between them. All three implement the OGC Simple Features core — `ST_Intersects`, `ST_Contains`, `ST_Within`, `ST_Distance`, `ST_Area`, `ST_Buffer` — but names, constructors, and semantics drift at the edges.

| Concern | DuckDB `spatial` | Trino | Apache Sedona |
|---|---|---|---|
| Backing library | GEOS + GDAL | Esri Geometry | JTS / GEOS |
| Construct from WKB | `ST_GeomFromWKB` | `ST_GeomFromBinary` | `ST_GeomFromWKB` |
| Construct from text | `ST_GeomFromText` | `ST_GeometryFromText` | `ST_GeomFromWKT` |
| Distance-within test | `ST_DWithin` | no `ST_DWithin`; use `ST_Distance(...) < r` | `ST_DWithin` (metres option) |
| Read GeoParquet | native `ST_Read` / `read_parquet` | via Hive/Iceberg connector | native GeoParquet reader |
| Distance semantics | planar (project first) | planar | planar + spheroidal helpers |

Two practical consequences. First, `ST_Distance` in all three is planar unless you use a spheroidal helper — for metre-accurate distances on EPSG:4326 data, project to an equal-distance CRS or use each engine's geography-aware function, and never assume a raw `ST_Distance` on lon/lat returns metres. Second, the constructor names differ enough that a query authored for Sedona will fail on Trino and vice versa; keep engine-specific SQL in engine-specific files rather than templating one string across all three. The canonical references are the [DuckDB spatial extension documentation](https://duckdb.org/docs/stable/extensions/spatial/overview), the [Trino geospatial functions reference](https://trino.io/docs/current/functions/geospatial.html), and the [Apache Sedona documentation](https://sedona.apache.org/latest/).

## Production implementation patterns

The three blocks below are complete and runnable — one per engine — showing the idiomatic way to execute a spatial query against GeoParquet-backed lakehouse storage. Each uses the two-stage bbox-then-exact filter pattern where it matters. Read them as a set: the same logical operation (constrain to a spatial window, then evaluate an exact predicate) is expressed three ways because the engines differ in constructor names, pushdown surface, and join strategy — which is precisely the portability tax the earlier sections warned about.

### DuckDB: spatial query on GeoParquet with bbox pushdown

```python
import duckdb

# DuckDB spatial extension >= 1.0. Runs in-process, no server.
con = duckdb.connect()
con.execute("INSTALL spatial; LOAD spatial;")
con.execute("INSTALL httpfs; LOAD httpfs;")  # for s3:// GeoParquet

# Query window (a bounding box over the San Francisco Bay Area, EPSG:4326)
minx, miny, maxx, maxy = -122.6, 37.2, -121.7, 38.0

# Two-stage filter: cheap bbox range predicate prunes Parquet row groups,
# exact ST_Intersects runs only on surviving rows.
rows = con.execute(
    """
    SELECT parcel_id,
           ST_Area(geometry) AS area_deg2
    FROM read_parquet('s3://lakehouse-prod/parcels/*.parquet')
    WHERE bbox_maxx >= ?          -- pushed down to row-group min/max
      AND bbox_minx <= ?
      AND bbox_maxy >= ?
      AND bbox_miny <= ?
      AND ST_Intersects(
            geometry,
            ST_MakeEnvelope(?, ?, ?, ?)   -- exact geometry test
          )
    ORDER BY area_deg2 DESC
    LIMIT 100
    """,
    [minx, maxx, miny, maxy, minx, miny, maxx, maxy],
).fetchall()

print(f"matched {len(rows)} parcels")
con.close()
```

In the DuckDB block, note that the bbox predicates and the `ST_Intersects` call reference the *same* window but serve different purposes: the four numeric comparisons are what let DuckDB skip Parquet row groups without deserializing a single geometry, while `ST_MakeEnvelope` builds the exact polygon that the surviving rows are tested against. Dropping the numeric predicates would still return correct results but would force a full geometry scan.

### Trino: spatial SQL join across an Iceberg table and a reference catalog

```sql
-- Trino SQL. Joins an Iceberg spatial table (iceberg catalog) to a
-- PostgreSQL reference table (postgres catalog) in one federated query.
-- Run ANALYZE on the Iceberg table first so the optimizer has statistics.

SELECT r.region_name,
       count(*)                      AS event_count,
       approx_distinct(e.device_id)  AS distinct_devices
FROM iceberg.geospatial.telemetry_events AS e
JOIN postgres.reference.admin_regions AS r
  -- bbox pre-filter prunes Iceberg splits via manifest min/max stats
  ON e.bbox_maxx >= r.min_lon
 AND e.bbox_minx <= r.max_lon
 AND e.bbox_maxy >= r.min_lat
 AND e.bbox_miny <= r.max_lat
  -- exact containment test on surviving candidate rows
 AND ST_Contains(
       ST_GeomFromBinary(r.boundary_wkb),
       ST_GeomFromBinary(e.geometry)
     )
WHERE e.event_date >= DATE '2026-06-01'
GROUP BY r.region_name
ORDER BY event_count DESC;
```

The Trino query is federated: `iceberg.geospatial.telemetry_events` and `postgres.reference.admin_regions` live in different catalogs, and Trino joins them without either dataset being copied into a common store. This is the capability no single-node engine offers. The bbox join conditions do double duty as both the spatial pre-filter and a hint to the optimizer about selectivity, and the `ST_GeomFromBinary` constructors reflect Trino's Esri-Geometry lineage — the same query on Sedona would use `ST_GeomFromWKB`.

### Sedona on PySpark: broadcast spatial join at scale

```python
from pyspark.sql import SparkSession
from sedona.spark import SedonaContext

# Spark 3.5 + Sedona. Packages resolve Sedona + GeoTools at submit time.
config = (
    SedonaContext.builder()
    .appName("sedona-broadcast-join")
    .config(
        "spark.jars.packages",
        "org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.6.1,"
        "org.datasyslab:geotools-wrapper:1.6.1-28.2",
    )
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.extensions",
            "org.apache.sedona.sql.SedonaSqlExtensions")
    .getOrCreate()
)
sedona = SedonaContext.create(config)

# Large point side (hundreds of millions of rows) from GeoParquet
points = (
    sedona.read.format("geoparquet")
    .load("s3://lakehouse-prod/telemetry/")
    .withColumnRenamed("geometry", "pt_geom")
)

# Small polygon side (thousands of regions) — broadcast it
regions = (
    sedona.read.format("geoparquet")
    .load("s3://lakehouse-prod/admin_regions/")
    .withColumnRenamed("geometry", "region_geom")
)
regions.createOrReplaceTempView("regions")
points.createOrReplaceTempView("points")

# BROADCAST hint forces the small side into a broadcast index join,
# avoiding an O(n*m) shuffle. Sedona builds a local tree index per task.
enriched = sedona.sql(
    """
    SELECT /*+ BROADCAST(regions) */
           p.device_id,
           r.region_name
    FROM points AS p
    JOIN regions AS r
      ON ST_Contains(r.region_geom, p.pt_geom)
    """
)

enriched.write.format("iceberg").mode("append").save(
    "glue_catalog.geospatial.telemetry_enriched"
)
```

The Sedona block's `/*+ BROADCAST(regions) */` hint is the single most important line: it forces the small polygon side to be shipped to and indexed on every executor, converting what would be a shuffle-heavy join into a local index probe. The KryoSerializer configuration is not optional — Sedona's geometry objects serialize far more compactly through Kryo than through Java serialization, and omitting it inflates shuffle volume and can itself trigger memory failures. The result is written straight back into an Iceberg table, keeping the enriched output on the same open-table storage plane the inputs came from.

## Failure modes and operational gotchas

These are the recurring production failures on the spatial compute plane. Each is paired with a mitigation.

- **Function-name drift across engines.** A query using `ST_GeomFromWKB` on DuckDB fails on Trino, which expects `ST_GeomFromBinary`; `ST_DWithin` exists in DuckDB and Sedona but not Trino. *Mitigation:* keep engine-specific SQL in separate files, add a CI smoke test per engine, and never string-template one spatial query across all three.
- **Missing spatial statistics.** Without materialized `DOUBLE` bbox columns and up-to-date manifest stats (Trino `ANALYZE`, Iceberg statistics), the optimizer cannot prune and every query becomes a full-table scan. *Mitigation:* enforce bbox columns at write time, run `ANALYZE`/rewrite stats after bulk loads, and assert plan-level pruning in tests.
- **Geometry serialization mismatches.** A column written as EWKB with an embedded SRID, or as GeoArrow while the reader expects WKB, deserializes to garbage or throws. Mixed-CRS columns silently corrupt distance results. *Mitigation:* pin one encoding (WKB) and one CRS per table, validate GeoParquet metadata in CI, and reject writes that violate the contract.
- **Shuffle blowups on distributed joins.** A Sedona join without a `BROADCAST` hint or spatial partitioning degenerates into a cartesian shuffle that fills executor disk and fails with OOM or shuffle-fetch timeouts. *Mitigation:* broadcast the small side, use Sedona's spatial partitioner and index for large-vs-large joins, and cap partition skew.
- **Single-node engine on oversized data.** Pointing DuckDB at a terabyte GeoParquet dataset spills to disk and thrashes because there is no cluster to spread the work. *Mitigation:* size the workload first; route anything beyond one node's memory to Sedona.
- **Planar distance treated as metres.** `ST_Distance` on EPSG:4326 geometry returns degrees, not metres, producing wrong `ST_DWithin` radii. *Mitigation:* project to an equal-distance CRS or use the engine's geography-aware distance function, and unit-test the radius.
- **Stale post-write pruning.** Immediately after a bulk append, manifests may lack fresh min/max stats and queries scan more than expected until compaction/analyze runs. *Mitigation:* trigger statistics collection as the final step of the ingest job.

## Operational readiness checklist

- [ ] Workload sized and mapped to an engine: interactive/fits-on-node → DuckDB, federated concurrent SQL → Trino, distributed batch joins → Sedona.
- [ ] Explicit `DOUBLE` bounding-box columns (`bbox_minx/miny/maxx/maxy`) materialized alongside every WKB geometry column.
- [ ] Single CRS per table enforced (numeric literal in code, EPSG:4326 named in docs); mixed-CRS writes rejected in CI.
- [ ] GeoParquet `geo` metadata validated in CI (encoding = WKB, primary column, CRS code).
- [ ] Manifest/table statistics refreshed after every bulk load (Iceberg stats rewrite, Trino `ANALYZE`).
- [ ] Predicate-pushdown verified in the query plan for each engine — confirm splits/row groups are pruned, not full-scanned.
- [ ] Engine-specific `ST_*` SQL kept in separate files with a per-engine CI smoke test to catch function-name drift.
- [ ] Sedona large joins use `BROADCAST` for small sides and the spatial partitioner + index for large-vs-large joins.
- [ ] Distance queries use a projected or geography-aware function, unit-tested against a known metre distance.
- [ ] Benchmark harness runs candidate engines against representative query shapes and data volumes before production cutover.

The four topic areas under this section drill into each engine and the selection process: [DuckDB geospatial analytics](/spatial-query-engines-compute/duckdb-geospatial-analytics/) for single-node GeoParquet and Iceberg querying, [Trino spatial SQL federation](/spatial-query-engines-compute/trino-spatial-sql-federation/) for cross-catalog joins, [Sedona distributed spatial compute](/spatial-query-engines-compute/sedona-distributed-spatial-compute/) for at-scale broadcast and partitioned joins, and [engine benchmarking and selection](/spatial-query-engines-compute/engine-benchmarking-selection/) for the methodology that turns the trade-offs above into a defensible per-workload decision. Together they close the loop from storage layout to executed spatial SQL.
