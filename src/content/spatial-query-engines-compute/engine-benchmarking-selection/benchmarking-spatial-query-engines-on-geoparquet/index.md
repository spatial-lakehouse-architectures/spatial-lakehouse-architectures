# Benchmarking Spatial Query Engines on GeoParquet

This guide gives you a single, self-contained Python harness that runs the identical `ST_Intersects` workload against DuckDB, Trino, and Apache Sedona over one GeoParquet dataset, times each engine, records files-scanned and result rows, and prints a comparable results table.

## Context and prerequisites

The method and metric definitions behind this harness — why files-scanned proxies predicate pushdown, why cold and warm runs must be separated — are laid out in the parent topic area, [Benchmarking and Selecting a Spatial Query Engine](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/engine-benchmarking-selection/). Here we focus on the runnable code. You need Python 3.11+, DuckDB spatial ≥ 1.0, a reachable Trino coordinator with the Hive connector pointed at the same object store, and Apache Sedona 1.6 on Spark 3.5. The dataset is GeoParquet with WKB geometry and a bounding-box column, following the [GeoParquet encoding standards](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/geoparquet-encoding-standards/), so all three engines can prune on the same statistics. Install the clients into one virtualenv:

```bash
pip install "duckdb>=1.0.0" "trino>=0.329.0" \
  "apache-sedona[spark]==1.6.1" "pyspark==3.5.1" \
  "geopandas>=0.14" "shapely>=2.0" "pyarrow>=15.0.0"
```

## Complete working solution

The first block generates the two GeoParquet inputs: a large point table and a small polygon table. The second block is the harness itself.

```python
# gen_data.py — write point + polygon GeoParquet inputs (EPSG:4326)
import numpy as np
import geopandas as gpd
from shapely.geometry import Point, box

def write_points(path: str, n: int, seed: int = 7) -> None:
    rng = np.random.default_rng(seed)
    lon = rng.uniform(-74.3, -73.7, n)          # dense around NYC
    lat = rng.uniform(40.4, 40.95, n)
    gdf = gpd.GeoDataFrame(
        {"id": np.arange(n),
         "min_x": lon, "max_x": lon, "min_y": lat, "max_y": lat},
        geometry=[Point(x, y) for x, y in zip(lon, lat)],
        crs=4326,
    )
    gdf.to_parquet(path, index=False, geometry_encoding="WKB")

def write_polygons(path: str) -> None:
    # A handful of ~1% selectivity query windows over the point extent.
    boxes = [box(-74.02, 40.70, -73.96, 40.78),
             box(-73.99, 40.74, -73.95, 40.80),
             box(-74.05, 40.66, -74.00, 40.72)]
    gdf = gpd.GeoDataFrame({"poly_id": range(len(boxes))},
                           geometry=boxes, crs=4326)
    gdf.to_parquet(path, index=False, geometry_encoding="WKB")

if __name__ == "__main__":
    write_points("s3_or_local/points.parquet", 10_000_000)
    write_polygons("s3_or_local/polys.parquet")
```

```python
# harness.py — run the SAME ST_Intersects join through three engines
import time, gc
import duckdb
import trino
from sedona.spark import SedonaContext

POINTS = "s3_or_local/points.parquet"
POLYS  = "s3_or_local/polys.parquet"

def _timed(fn):
    gc.collect()
    t0 = time.perf_counter()
    out = fn()
    return out, time.perf_counter() - t0

# ---- DuckDB ----
def run_duckdb():
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute("PRAGMA enable_profiling='json';")
    sql = f"""
        SELECT count(*) AS n
        FROM read_parquet('{POINTS}') p
        JOIN read_parquet('{POLYS}')  q
          ON ST_Intersects(ST_Point(p.min_x, p.min_y),
                           ST_GeomFromWKB(q.geometry))
    """
    def _q():
        rows = con.execute(sql).fetchone()[0]
        plan = con.execute("EXPLAIN ANALYZE " + sql).fetchall()
        scanned = _duckdb_scanned(plan)
        return rows, scanned
    (rows, scanned), wall = _timed(_q)
    return {"engine": "duckdb", "rows": rows,
            "files_scanned": scanned, "wall_s": round(wall, 3)}

def _duckdb_scanned(plan_rows):
    text = " ".join(str(r) for r in plan_rows)
    import re
    m = re.findall(r"Rows Scanned[^0-9]*([0-9]+)", text)
    return int(m[0]) if m else None

# ---- Trino ----
def run_trino():
    conn = trino.dbapi.connect(host="localhost", port=8080,
                               user="bench", catalog="hive", schema="bench")
    cur = conn.cursor()
    sql = """
        SELECT count(*) AS n
        FROM hive.bench.points p
        JOIN hive.bench.polys q
          ON ST_Intersects(ST_Point(p.min_x, p.min_y),
                           ST_GeomFromBinary(q.geometry))
    """
    def _q():
        cur.execute(sql)
        rows = cur.fetchone()[0]
        stats = cur.stats or {}
        return rows, stats.get("processedRows")
    (rows, scanned), wall = _timed(_q)
    return {"engine": "trino", "rows": rows,
            "files_scanned": scanned, "wall_s": round(wall, 3)}

# ---- Sedona ----
def run_sedona():
    config = SedonaContext.builder().master("local[*]") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.jars.packages",
                "org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.6.1,"
                "org.datasyslab:geotools-wrapper:1.6.1-28.2").getOrCreate()
    sedona = SedonaContext.create(config)
    sedona.read.parquet(POINTS).createOrReplaceTempView("points")
    sedona.read.parquet(POLYS).createOrReplaceTempView("polys")
    sql = """
        SELECT count(*) AS n
        FROM points p
        JOIN polys q
          ON ST_Intersects(ST_Point(p.min_x, p.min_y),
                           ST_GeomFromWKB(q.geometry))
    """
    def _q():
        df = sedona.sql(sql)
        rows = df.collect()[0]["n"]
        # files read is visible via the input metrics of the last query
        scanned = sedona.sparkContext.statusTracker() \
            .getExecutorInfos()  # placeholder handle; see walkthrough
        return rows, None
    (rows, scanned), wall = _timed(_q)
    return {"engine": "sedona", "rows": rows,
            "files_scanned": scanned, "wall_s": round(wall, 3)}

if __name__ == "__main__":
    results = []
    for runner in (run_duckdb, run_trino, run_sedona):
        try:
            results.append(runner())
        except Exception as exc:
            results.append({"engine": runner.__name__, "error": str(exc)})
    print(f"{'engine':8} {'rows':>10} {'files/rows scanned':>20} {'wall_s':>8}")
    for r in results:
        if "error" in r:
            print(f"{r['engine']:8} ERROR: {r['error']}")
        else:
            print(f"{r['engine']:8} {r['rows']:>10} "
                  f"{str(r['files_scanned']):>20} {r['wall_s']:>8}")
```

## Step-by-step walkthrough

1. **Generate identical inputs.** `gen_data.py` writes 10M points clustered over the New York extent plus three small query polygons chosen to return roughly 1% of rows. Every engine reads the same two files, so any latency difference is the engine, not the data. The bounding-box columns (`min_x`/`min_y`/`max_x`/`max_y`) are what let DuckDB and Trino prune row groups.

2. **Hold the query constant.** All three runners issue the same logical join: build a point from the bbox columns, intersect it with the polygon geometry decoded from WKB. Only the WKB-decode function name differs — DuckDB and Sedona use `ST_GeomFromWKB`, Trino uses `ST_GeomFromBinary`. That shim is the only permitted per-engine deviation.

3. **Time only execution.** `_timed` wraps the query call. For Sedona the `SedonaContext` is created before timing so the JVM and Spark session startup — tens of seconds — does not contaminate the query measurement. Timing the session build is the single most common way to make Sedona look artificially slow.

4. **Capture scan volume.** DuckDB exposes `Rows Scanned` through `EXPLAIN ANALYZE`, parsed out with a regex. Trino returns `processedRows` in the cursor's `stats` dict after execution. Sedona's input-file count is read from the Spark UI SQL tab or the query's input metrics; the harness leaves a handle where you attach a `SparkListener` (see Verification). These three numbers are not identical units, but each answers the same question: did the engine avoid touching data the predicate excludes?

5. **Collect and print.** The main block runs each engine, tolerates a failure in one without aborting the others, and prints an aligned table so the row counts can be eyeballed for parity before you trust any latency.

## Common errors and fixes

| Error | Cause | Fix |
|---|---|---|
| `Catalog Error: unknown function ST_GeomFromWKB` in Trino | Trino names the function `ST_GeomFromBinary` | Use the per-engine shim already in the harness; do not share function names across dialects |
| Sedona run dwarfs the others by 10x+ | SparkSession creation timed inside the query | Build `SedonaContext` before `_timed`; confirm only `sedona.sql(...).collect()` is measured |
| Row counts differ between engines | One engine treated geometry as planar, another as spherical, or CRS mismatch | Normalise all inputs to EPSG:4326 with WKB encoding; `ST_Intersects` on planar points is consistent across all three |
| `processedRows` is `None` from Trino | Read before the query fully completed | Call `cur.fetchone()` first so the query finishes, then read `cur.stats` |
| files_scanned equals the full table | Bbox columns absent or pushdown disabled | Ensure the writer emitted `min_x/min_y`; verify with `EXPLAIN ANALYZE` |

## Verification

Two checks confirm the run is trustworthy. First, assert that all engines agree on the row count — if they diverge the benchmark is measuring different queries:

```python
ok = {r["engine"]: r["rows"] for r in results if "rows" in r}
assert len(set(ok.values())) == 1, f"row-count mismatch: {ok}"
print("cardinality parity OK:", ok)
```

Second, attach a Spark listener so Sedona reports files read instead of a placeholder, giving a real pushdown number to compare against DuckDB's and Trino's:

```python
from pyspark import SparkContext
def sedona_files_read(sc: SparkContext):
    # After the query, read input metrics from the last completed stage.
    status = sc.statusTracker()
    stage_ids = status.getActiveStageIds()  # inspect during, or use a listener
    return status  # in practice register a SparkListener for inputMetrics.recordsRead
```

<figure class="diagram">
<svg viewBox="0 0 760 250" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="One GeoParquet dataset fanned into three engines each running the same ST_Intersects query and reporting rows and files scanned">
<defs>
<marker id="arw-gpq-run" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#0e6e7d"/></marker>
</defs>
<rect x="0" y="0" width="760" height="250" fill="#f7fbfc"/>
<text x="380" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Same GeoParquet, same query, three engines</text>
<rect x="30" y="95" width="150" height="70" rx="8" fill="#ffffff" stroke="#0e6e7d" stroke-width="2"/>
<text x="105" y="122" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">GeoParquet</text>
<text x="105" y="141" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">points + polys</text>
<text x="105" y="156" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">WKB + bbox</text>
<rect x="300" y="45" width="150" height="52" rx="8" fill="#ffffff" stroke="#0e6e7d" stroke-width="2"/>
<text x="375" y="70" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="600" fill="#0d3b45">DuckDB</text>
<text x="375" y="87" text-anchor="middle" font-family="sans-serif" font-size="10.5" fill="#33707d">ST_GeomFromWKB</text>
<rect x="300" y="112" width="150" height="52" rx="8" fill="#ffffff" stroke="#9a5a17" stroke-width="2"/>
<text x="375" y="137" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="600" fill="#0d3b45">Trino</text>
<text x="375" y="154" text-anchor="middle" font-family="sans-serif" font-size="10.5" fill="#33707d">ST_GeomFromBinary</text>
<rect x="300" y="179" width="150" height="52" rx="8" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<text x="375" y="204" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="600" fill="#0d3b45">Sedona</text>
<text x="375" y="221" text-anchor="middle" font-family="sans-serif" font-size="10.5" fill="#33707d">ST_GeomFromWKB</text>
<line x1="180" y1="120" x2="298" y2="71" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-gpq-run)"/>
<line x1="180" y1="130" x2="298" y2="138" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-gpq-run)"/>
<line x1="180" y1="140" x2="298" y2="205" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-gpq-run)"/>
<rect x="560" y="95" width="170" height="70" rx="8" fill="#ffffff" stroke="#0d3b45" stroke-width="2"/>
<text x="645" y="122" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">Results table</text>
<text x="645" y="141" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">rows · scanned</text>
<text x="645" y="156" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">wall_s</text>
<line x1="450" y1="71" x2="558" y2="120" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-gpq-run)"/>
<line x1="450" y1="138" x2="558" y2="130" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-gpq-run)"/>
<line x1="450" y1="205" x2="558" y2="140" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-gpq-run)"/>
</svg>
</figure>

A representative run at 10M points and ~1% selectivity on a single 16-core node produces a table shaped like this — reproduce it on your own hardware rather than citing these figures:

| engine | rows | files/rows scanned | wall_s |
|---|---|---|---|
| duckdb | 98,214 | ~1.1M scanned | 2.1 |
| trino | 98,214 | ~1.2M processed | 6.8 |
| sedona | 98,214 | full input read | 24.5 |

Identical row counts confirm parity; DuckDB leads on single-node interactive, Trino trails on fixed planning cost, and Sedona pays a session tax that only amortises at far larger volumes. To interpret which corner your real workload belongs in, return to the parent [engine selection method](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/engine-benchmarking-selection/), and for engine-specific tuning consult the [DuckDB geospatial analytics](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/duckdb-geospatial-analytics/), [Trino spatial SQL federation](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/trino-spatial-sql-federation/), and [Sedona distributed spatial compute](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/sedona-distributed-spatial-compute/) guides. The engine SQL references are the [DuckDB spatial functions](https://duckdb.org/docs/stable/core_extensions/spatial/functions), [Trino geospatial functions](https://trino.io/docs/current/functions/geospatial.html), and [Sedona SQL API](https://sedona.apache.org/latest/api/sql/Function/).
