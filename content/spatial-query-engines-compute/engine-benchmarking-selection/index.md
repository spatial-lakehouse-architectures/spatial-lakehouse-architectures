# Benchmarking and Selecting a Spatial Query Engine

Choosing between DuckDB, Trino, and Apache Sedona for a spatial lakehouse workload is not a matter of vendor preference or raw TPC scores — it is an engineering decision that hinges on data volume, geometry complexity, join selectivity, and concurrency. This guide lays out a disciplined, reproducible method for building a representative spatial benchmark, measuring the metrics that actually predict production behaviour (wall-clock latency, predicate pushdown effectiveness, files scanned, peak memory), and mapping the results onto a decision matrix. It is the analytical counterpart to the deeper engine guides under [Spatial Query Engines & Compute Optimization](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/), and it assumes you have already read at least one of the [DuckDB](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/duckdb-geospatial-analytics/), [Trino](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/trino-spatial-sql-federation/), or [Sedona](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/sedona-distributed-spatial-compute/) topic areas.

## When to use this method

Run a structured benchmark before you commit an engine to a workload class — not once, but whenever data volume crosses an order of magnitude or the query shape changes. A single-node engine that dominates at 50M rows can fall off a cliff at 5B; a distributed engine that wins the terabyte join wastes money and adds latency on an interactive dashboard. The table below gives quick heuristics, but the whole point of this page is that you validate them against your own data rather than trusting them blindly.

| Signal | Lean DuckDB | Lean Trino | Lean Sedona |
|---|---|---|---|
| Data fits one fat node (< ~200 GB working set) | Yes | Maybe | No |
| Cross-catalog federation (Iceberg + Delta + Postgres) | No | Yes | Partial |
| Join both sides are billion-row geometry tables | No | Maybe | Yes |
| High interactive concurrency (50+ QPS, small results) | Yes | Yes | No |
| Heavy geometry construction / raster / ML pipeline | No | No | Yes |
| Zero cluster to operate, laptop-to-CI reproducibility | Yes | No | No |

If your workload straddles rows, treat that as a signal to benchmark rather than a reason to guess. The most expensive mistakes in spatial lakehouse design come from picking the engine that was fastest on someone else's slide.

## Architecture of a spatial benchmark harness

A credible benchmark isolates the engine under test behind an identical workload contract: the same GeoParquet dataset, the same logical query, the same success criteria. Everything else — the storage layer, the metrics collector, the result store — is shared. The diagram shows the data path and the decision that falls out of it.

<figure class="diagram">
<svg viewBox="0 0 760 340" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Spatial benchmark harness feeding a decision matrix that maps workload shape to DuckDB, Trino, or Sedona">
<defs>
<marker id="arw-bench" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#0e6e7d"/></marker>
<marker id="arw-bench-green" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#2f6e49"/></marker>
</defs>
<rect x="0" y="0" width="760" height="340" fill="#f7fbfc"/>
<text x="380" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Benchmark harness and engine decision path</text>
<rect x="30" y="55" width="150" height="60" rx="8" fill="#ffffff" stroke="#0e6e7d" stroke-width="2"/>
<text x="105" y="80" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="600" fill="#0d3b45">GeoParquet</text>
<text x="105" y="99" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">WKB + bbox stats</text>
<rect x="30" y="140" width="150" height="60" rx="8" fill="#ffffff" stroke="#0e6e7d" stroke-width="2"/>
<text x="105" y="165" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="600" fill="#0d3b45">Workload spec</text>
<text x="105" y="184" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">ST_Intersects</text>
<rect x="250" y="95" width="160" height="105" rx="8" fill="#ffffff" stroke="#6a3d9a" stroke-width="2"/>
<text x="330" y="120" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">Harness runner</text>
<text x="330" y="141" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">same query,</text>
<text x="330" y="157" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">each engine,</text>
<text x="330" y="173" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">timed + traced</text>
<line x1="180" y1="85" x2="248" y2="120" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-bench)"/>
<line x1="180" y1="170" x2="248" y2="150" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-bench)"/>
<rect x="470" y="55" width="120" height="46" rx="8" fill="#ffffff" stroke="#0e6e7d" stroke-width="2"/>
<text x="530" y="83" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="600" fill="#0d3b45">DuckDB</text>
<rect x="470" y="119" width="120" height="46" rx="8" fill="#ffffff" stroke="#9a5a17" stroke-width="2"/>
<text x="530" y="147" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="600" fill="#0d3b45">Trino</text>
<rect x="470" y="183" width="120" height="46" rx="8" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<text x="530" y="211" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="600" fill="#0d3b45">Sedona</text>
<line x1="410" y1="130" x2="468" y2="80" stroke="#6a3d9a" stroke-width="2" marker-end="url(#arw-bench)"/>
<line x1="410" y1="147" x2="468" y2="142" stroke="#6a3d9a" stroke-width="2" marker-end="url(#arw-bench)"/>
<line x1="410" y1="164" x2="468" y2="204" stroke="#6a3d9a" stroke-width="2" marker-end="url(#arw-bench)"/>
<rect x="640" y="95" width="100" height="105" rx="8" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<text x="690" y="120" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">Metrics</text>
<text x="690" y="140" text-anchor="middle" font-family="sans-serif" font-size="10.5" fill="#33707d">latency</text>
<text x="690" y="156" text-anchor="middle" font-family="sans-serif" font-size="10.5" fill="#33707d">files scanned</text>
<text x="690" y="172" text-anchor="middle" font-family="sans-serif" font-size="10.5" fill="#33707d">peak RSS</text>
<text x="690" y="188" text-anchor="middle" font-family="sans-serif" font-size="10.5" fill="#33707d">pushdown %</text>
<line x1="590" y1="78" x2="638" y2="120" stroke="#2f6e49" stroke-width="2" marker-end="url(#arw-bench-green)"/>
<line x1="590" y1="142" x2="638" y2="147" stroke="#2f6e49" stroke-width="2" marker-end="url(#arw-bench-green)"/>
<line x1="590" y1="206" x2="638" y2="174" stroke="#2f6e49" stroke-width="2" marker-end="url(#arw-bench-green)"/>
<rect x="180" y="255" width="400" height="60" rx="8" fill="#ffffff" stroke="#0d3b45" stroke-width="2"/>
<text x="380" y="280" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">Decision matrix: workload shape → engine</text>
<text x="380" y="300" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">volume × geometry × selectivity × concurrency</text>
<line x1="690" y1="200" x2="500" y2="253" stroke="#0d3b45" stroke-width="1.5" stroke-dasharray="4 3" marker-end="url(#arw-bench)"/>
</svg>
</figure>

The critical property is that the harness never lets an engine cheat. If DuckDB reads a locally cached copy while Sedona reads cold object storage, the numbers are worthless. Pin the storage layer, clear OS page cache between cold runs, and record whether each measurement is warm or cold.

## Prerequisites and environment setup

The harness targets Python 3.11+, DuckDB spatial ≥ 1.0, a reachable Trino coordinator (0.44x / Trino 4xx with the Hive or Iceberg connector), and Apache Sedona 1.6+ on Spark 3.5. Install everything into one virtualenv so the runner can import all three clients in-process; Trino and Sedona reach out to their respective services.

```bash
python -m venv .venv && source .venv/bin/activate
pip install \
  "duckdb>=1.0.0" \
  "trino>=0.329.0" \
  "apache-sedona[spark]==1.6.1" \
  "pyspark==3.5.1" \
  "pyarrow>=15.0.0" \
  "geopandas>=0.14" \
  "shapely>=2.0" \
  "psutil>=5.9"
```

Generate a representative dataset rather than downloading a pristine benchmark set. Real spatial tables have skewed geometry vertex counts, mixed geometry types, and non-uniform spatial density — properties that dominate join cost. The generator below writes GeoParquet with a bounding-box column so every engine can exercise predicate pushdown on the same statistics. GeoParquet layout details are covered in [GeoParquet Encoding Standards](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/geoparquet-encoding-standards/).

```python
import numpy as np
import geopandas as gpd
from shapely.geometry import Point

def make_geoparquet(path: str, n: int, seed: int = 42) -> None:
    """Write n points with realistic clustered density to GeoParquet (EPSG:4326)."""
    rng = np.random.default_rng(seed)
    # Three density clusters + uniform background — mimics urban/rural skew.
    centers = np.array([[-73.98, 40.75], [-0.12, 51.50], [139.69, 35.68]])
    pick = rng.integers(0, len(centers) + 1, size=n)
    lon = np.empty(n); lat = np.empty(n)
    for i in range(n):
        if pick[i] < len(centers):
            lon[i] = centers[pick[i], 0] + rng.normal(0, 0.4)
            lat[i] = centers[pick[i], 1] + rng.normal(0, 0.4)
        else:
            lon[i] = rng.uniform(-180, 180)
            lat[i] = rng.uniform(-60, 70)
    gdf = gpd.GeoDataFrame(
        {"id": np.arange(n)},
        geometry=[Point(x, y) for x, y in zip(lon, lat)],
        crs=4326,
    )
    gdf["min_x"] = lon; gdf["max_x"] = lon
    gdf["min_y"] = lat; gdf["max_y"] = lat
    gdf.to_parquet(path, index=False, geometry_encoding="WKB", write_covering_bbox=False)

if __name__ == "__main__":
    make_geoparquet("points_10m.parquet", 10_000_000)
```

## Step-by-step method

### 1. Fix the workload contract

Pick one query shape and hold it constant across engines. For a spatial lakehouse the canonical shape is a point-in-polygon or `ST_Intersects` join between a large point table and a smaller polygon table. Define it as SQL text with a per-engine dialect shim, because DuckDB, Trino, and Spark SQL disagree on function names (`ST_Intersects` is common; geometry construction is not). Freeze the selectivity by fixing the polygon set — a query that returns 2% of rows stresses an engine very differently from one that returns 40%.

### 2. Decide the metric set

Wall-clock latency alone is a trap. Record all four of:

- **Latency** — median and p95 over repeated runs, cold and warm reported separately.
- **Files/row-groups scanned** — the direct proxy for [predicate pushdown](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/predicate-pushdown-optimization/) effectiveness. An engine that scans every file has no pushdown regardless of how fast it runs.
- **Peak memory (RSS)** — the number that decides whether the engine survives at 10x volume.
- **Result cardinality** — a correctness guard. If two engines disagree on row count, the benchmark is measuring two different queries and every latency comparison is meaningless.

### 3. Instrument each engine identically

Wrap every run in the same timer and memory sampler. The harness measures the client-observed wall time (what a user experiences) and asks each engine for its own scan statistics through native EXPLAIN/query-stats APIs, so pushdown can be attributed rather than guessed.

```python
import time, gc, psutil, os

def timed_run(fn):
    """Return (result, wall_seconds, peak_rss_mb) for a single query invocation."""
    gc.collect()
    proc = psutil.Process(os.getpid())
    rss_before = proc.memory_info().rss
    peak = rss_before
    t0 = time.perf_counter()
    result = fn()
    wall = time.perf_counter() - t0
    peak = max(peak, proc.memory_info().rss)
    return result, wall, (peak - rss_before) / 1e6
```

### 4. Sweep the axes that matter

Run the fixed workload across a grid of the four independent variables. Do not vary them one at a time in isolation — interactions are where engines diverge. A useful minimum grid:

- **Data volume**: 1M, 10M, 100M, 1B rows on the point side.
- **Geometry complexity**: points, then polygons with ~20 and ~500 vertices.
- **Join selectivity**: polygon sets that return roughly 1%, 10%, and 40% of rows.
- **Concurrency**: 1, 8, and 32 simultaneous clients issuing the query.

At each grid cell, run three warm iterations after one discarded cold-cache priming run, and record the median. Store every observation as a row keyed by the grid coordinates so the analysis is a straight groupby, not a spreadsheet archaeology exercise.

### 5. Normalise and store results

Persist results to a single Parquet or DuckDB table with columns `engine, volume, geom_vertices, selectivity, concurrency, warm, latency_s, files_scanned, peak_rss_mb, rows`. This makes the decision matrix a query rather than a judgement call.

```python
import duckdb

def summarise(results_path: str) -> None:
    con = duckdb.connect()
    con.execute(f"CREATE VIEW r AS SELECT * FROM read_parquet('{results_path}')")
    print(con.execute("""
        SELECT engine, volume, selectivity,
               median(latency_s) AS p50_s,
               max(peak_rss_mb)  AS peak_mb,
               any_value(files_scanned) AS files
        FROM r WHERE warm
        GROUP BY engine, volume, selectivity
        ORDER BY volume, selectivity, p50_s
    """).fetchdf().to_string(index=False))
```

## Verification and testing

A benchmark you cannot trust is worse than no benchmark. Three checks separate signal from noise. First, assert result-cardinality parity: every engine must return the same row count for the same grid cell, within a tolerance of zero for exact predicates. If they diverge, a dialect shim is wrong — often a CRS mismatch where one engine treated coordinates as planar and another as spherical. In prose always write EPSG:4326; in code pass the integer `4326`.

Second, confirm the cold/warm split is real. A cold run that matches the warm run means the OS page cache was not cleared and your "cold" number is fiction. On Linux, drop caches between cold runs (`echo 3 > /proc/sys/vm/drop_caches` as root) or run each cold measurement in a fresh container.

Third, verify pushdown attribution against the query plan. For DuckDB, `EXPLAIN ANALYZE` reports `Rows Scanned`; for Trino, the query stats expose `processedInputPositions` and `physicalInputBytes`; for Sedona, the Spark UI SQL tab shows `number of files read` and `pruned` partition counts. If files-scanned does not drop as selectivity tightens, the engine is not pruning on the bbox statistics and you have a partitioning or [Z-ordering](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/z-ordering-for-geospatial-queries/) problem, not an engine problem.

```sql
-- DuckDB: confirm bbox pushdown prunes row groups
EXPLAIN ANALYZE
SELECT count(*)
FROM read_parquet('points_10m.parquet')
WHERE min_x BETWEEN -74.1 AND -73.9
  AND min_y BETWEEN 40.6 AND 40.9;
-- Inspect the 'Rows Scanned' vs total in the profiled plan.
```

## Performance and tuning

Once the raw grid is collected, tune each engine before declaring a winner — an untuned engine loses benchmarks it would win in production. Concrete knobs and their ranges:

- **DuckDB**: set `SET threads TO <physical cores>` and `SET memory_limit='<70% of RAM>'`. DuckDB spills to disk above the limit; leaving it at default often caps a big machine at a fraction of its cores. Expect DuckDB to lead on single-node point-in-polygon up to roughly the point where the working set exceeds RAM, after which spill I/O dominates.
- **Trino**: raise `query.max-memory-per-node` and enable `spatial_partitioning` for large joins. Trino's spatial joins need a `spatial_partitioning()` KDB-tree table to avoid a broadcast that blows up the coordinator. Federation across an Iceberg and a Delta catalog is Trino's structural advantage; measure it, because it is the reason to accept its higher fixed latency. Cross-catalog patterns live in [spatial joins across catalogs with Trino](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/trino-spatial-sql-federation/spatial-joins-across-catalogs-with-trino/).
- **Sedona**: broadcast the small side with `ST_GeomFromWKB` materialised once and use `spark.sql.autoBroadcastJoinThreshold` deliberately; the [broadcast spatial join](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/sedona-distributed-spatial-compute/broadcast-spatial-joins-with-apache-sedona/) is the difference between a fast Sedona run and a shuffle-bound one. Sedona's fixed startup cost (JVM + Spark session) means it almost always loses the 1M-row interactive case and almost always wins the billion-row shuffle-heavy case.

As a rough calibration from a 16-core / 64 GB node against the clustered point generator above: at 10M points and 1% selectivity DuckDB completes an `ST_Intersects` join in low single-digit seconds warm, Trino in the high single digits including planning, and Sedona in tens of seconds dominated by session and shuffle setup. At 1B points those orderings invert once the point side no longer fits in memory. Treat these as shape expectations to reproduce, not as numbers to cite — your hardware and geometry complexity will move them.

The decision matrix that falls out is stable even when the absolute numbers are not: DuckDB owns single-node interactive and embedded/CI workloads; Trino owns federated multi-catalog SQL and moderate-concurrency serving; Sedona owns distributed billion-row joins and geometry-construction-heavy ETL. The benchmark's job is to tell you which corner your real workload lives in, and whether it is close enough to a boundary to justify running two engines.

## Common errors and fixes

| Symptom | Root cause | Fix |
|---|---|---|
| Engines return different row counts for the same cell | Dialect shim used spherical vs planar predicate, or CRS mismatch | Normalise all inputs to EPSG:4326, assert `ST_Intersects` semantics per engine, add a cardinality-parity assertion to the harness |
| "Cold" and "warm" latencies identical | OS page cache not cleared between runs | Drop caches (`vm.drop_caches`) or run each cold measurement in a fresh container/pod |
| Sedona always slowest, even at large volume | JVM/Spark session created inside the timed region | Warm the SparkSession once outside `timed_run`; time only query execution and collect |
| files_scanned does not fall as selectivity tightens | Bbox statistics missing or engine not pushing the predicate down | Write covering-bbox columns, verify via EXPLAIN, revisit partition/Z-order layout |
| Trino join OOMs the coordinator | Implicit broadcast of a large geometry side | Build a `spatial_partitioning()` KDB-tree and partition both sides before the join |
| DuckDB caps at a few cores on a large box | Default thread/memory limits | `SET threads`, `SET memory_limit` to the physical machine size before running |

For the concrete, runnable three-engine harness that produces the results table this method depends on, work through [benchmarking spatial query engines on GeoParquet](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/engine-benchmarking-selection/benchmarking-spatial-query-engines-on-geoparquet/). For authoritative engine references, see the [DuckDB spatial extension docs](https://duckdb.org/docs/stable/core_extensions/spatial/overview), the [Trino geospatial functions](https://trino.io/docs/current/functions/geospatial.html), and the [Apache Sedona SQL reference](https://sedona.apache.org/latest/api/sql/Overview/).
