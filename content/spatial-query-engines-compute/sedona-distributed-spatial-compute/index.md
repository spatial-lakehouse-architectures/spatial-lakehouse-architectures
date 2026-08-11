# Distributed Spatial Compute with Apache Sedona

Apache Sedona (formerly GeoSpark) extends Spark 3.5 with a distributed spatial type system, spatial partitioners, and index-backed join operators, making it the tool of choice when a spatial join is too large for any single machine. Where a single-node engine loads one dataset into memory, Sedona shards billions of geometries across a Spark cluster, builds a distributed spatial index (KDB-tree or quad-tree), and executes range and join queries in parallel. This topic area covers the `SpatialRDD` and Sedona SQL programming models, spatial partitioning and index construction, reading and writing both Apache Iceberg and GeoParquet from Sedona, and the concrete threshold at which distribution beats single-node [DuckDB geospatial analytics](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/duckdb-geospatial-analytics/). It belongs to the [Spatial Query Engines & Compute Optimization](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/) section and complements the SQL-federation approach in [Trino spatial SQL and cross-catalog federation](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/trino-spatial-sql-federation/).

## When to use this

Sedona earns its operational overhead only when the data genuinely exceeds single-node capacity or when the spatial join is quadratic and both sides are large. Below roughly 50–100 GB of geometry, a single-node engine will almost always finish faster because it skips job scheduling and shuffle. The decision is about data size, join cardinality, and whether the output feeds a heavier Spark transformation DAG.

| Signal | Sedona (Spark) | DuckDB | Trino |
|---|---|---|---|
| Both join sides are 100+ GB of geometry | Best | No | Adequate |
| Output feeds an existing Spark ETL DAG | Best | No | No |
| Interactive ad-hoc SQL, seconds matter | Weaker | Best (one node) | Strong |
| Need a distributed spatial index | Yes (KDB/quad-tree) | No | Partial |
| Small data, no cluster available | Overkill | Best | No |

If your large-large spatial join OOMs or runs for hours on a single node, that is the signal to move to Sedona's partitioned, index-backed join.

<figure class="diagram">
<svg viewBox="0 0 742 296" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Sedona distributed spatial join: partition both layers by KDB-tree, build local indexes, join per partition">
<defs>
<marker id="arw-sedona-flow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#0e6e7d"/></marker>
</defs>
<rect x="0" y="0" width="742" height="296" fill="#f7fbfc"/>
<text x="380" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Sedona distributed spatial join on Spark 3.5</text>
<rect x="30" y="55" width="180" height="60" rx="8" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<text x="120" y="81" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="600" fill="#0d3b45">Iceberg / GeoParquet</text>
<text x="120" y="100" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">read to DataFrame</text>
<rect x="290" y="55" width="180" height="60" rx="8" fill="#ffffff" stroke="#0e6e7d" stroke-width="2"/>
<text x="380" y="81" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="600" fill="#0d3b45">KDB-tree partitioner</text>
<text x="380" y="100" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">equal-load grid</text>
<rect x="550" y="55" width="180" height="60" rx="8" fill="#ffffff" stroke="#9a5a17" stroke-width="2"/>
<text x="640" y="81" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="600" fill="#0d3b45">local R-tree index</text>
<text x="640" y="100" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">per partition</text>
<line x1="210" y1="85" x2="290" y2="85" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-sedona-flow)"/>
<line x1="470" y1="85" x2="550" y2="85" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-sedona-flow)"/>
<rect x="120" y="165" width="150" height="55" rx="8" fill="#ffffff" stroke="#6a3d9a" stroke-width="2"/>
<text x="195" y="190" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="600" fill="#0d3b45">partition 0</text>
<text x="195" y="208" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">local join</text>
<rect x="305" y="165" width="150" height="55" rx="8" fill="#ffffff" stroke="#6a3d9a" stroke-width="2"/>
<text x="380" y="190" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="600" fill="#0d3b45">partition 1</text>
<text x="380" y="208" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">local join</text>
<rect x="490" y="165" width="150" height="55" rx="8" fill="#ffffff" stroke="#6a3d9a" stroke-width="2"/>
<text x="565" y="190" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="600" fill="#0d3b45">partition n</text>
<text x="565" y="208" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">local join</text>
<line x1="640" y1="115" x2="565" y2="165" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-sedona-flow)"/>
<line x1="600" y1="115" x2="380" y2="165" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-sedona-flow)"/>
<line x1="560" y1="115" x2="195" y2="165" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-sedona-flow)"/>
<text x="380" y="258" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">Equal-load partitions plus per-partition R-tree turn a quadratic join into parallel local joins</text>
<text x="380" y="280" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">Skew is handled by the KDB-tree, not by broadcasting</text>
</svg>
</figure>

## Prerequisites and environment setup

Pin **Spark 3.5** and a matching Sedona release. Sedona ships as Scala/Java jars plus the `apache-sedona` Python package; the two must agree on Spark and Scala versions. Register the Sedona SQL functions and serializers on the session, and add the Iceberg and Sedona jars to the classpath.

```python
# pip install apache-sedona==1.6.1 pyspark==3.5.1
from sedona.spark import SedonaContext

config = (
    SedonaContext.builder()
    .appName("sedona-spatial-join")
    # Sedona + Iceberg runtime jars (match Spark 3.5 / Scala 2.12)
    .config(
        "spark.jars.packages",
        "org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.6.1,"
        "org.datasyslab:geotools-wrapper:1.6.1-28.2,"
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.0",
    )
    # Sedona geometry serializer (Kryo) — required for shuffle
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator")
    # Iceberg REST catalog
    .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lake.type", "rest")
    .config("spark.sql.catalog.lake.uri", "https://catalog.internal:8181")
    .getOrCreate()
)
sedona = SedonaContext.create(config)
```

Verify registration with `sedona.sql("SELECT ST_Point(0.0, 0.0)").show()`. A `Undefined function ST_Point` error means the serializer/registrator config did not take — the jars and `SedonaContext.create` step are both required.

## Step-by-step implementation

### 1. Read spatial data from Iceberg and GeoParquet

Sedona reads GeoParquet natively and reads Iceberg through the standard Spark catalog, then reconstructs geometries with `ST_GeomFromWKB`. Keep everything in EPSG:4326 lon/lat so downstream predicates are unambiguous.

```python
# Large fact layer from Iceberg (WKB in a binary column)
pings = sedona.sql("""
    SELECT device_id, event_ts, ST_GeomFromWKB(geom_wkb) AS geom
    FROM lake.telemetry.pings
    WHERE event_ts >= TIMESTAMP '2026-07-01 00:00:00'
""")

# Reference layer from GeoParquet (geometry column decoded automatically)
zones = sedona.read.format("geoparquet").load("s3a://ref/zones/")
zones.createOrReplaceTempView("zones")
pings.createOrReplaceTempView("pings")
```

### 2. Let Sedona build the spatial partitioning and index

The Sedona SQL optimizer recognizes an `ST_` predicate in the join condition and injects a distributed spatial join: it partitions both inputs with a KDB-tree (equal-load, skew-aware) and builds a local R-tree per partition. You enable the range-join optimization and set the partition count; you do not hand-write the partitioner.

```python
sedona.conf.set("sedona.join.numpartition", "200")
sedona.conf.set("sedona.join.gridtype", "kdbtree")   # or "quadtree"
sedona.conf.set("sedona.join.indextype", "rtree")

result = sedona.sql("""
    SELECT p.device_id, z.zone_id, p.event_ts
    FROM pings p JOIN zones z
      ON ST_Intersects(p.geom, z.geometry)
""")
result.cache()
```

For the small-reference-layer case where a broadcast is cheaper than a shuffle, use the explicit broadcast hint pattern documented in [broadcast spatial joins with Apache Sedona](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/sedona-distributed-spatial-compute/broadcast-spatial-joins-with-apache-sedona/).

### 3. Write results back to Iceberg

Encode the geometry back to WKB before writing so the Iceberg schema stays engine-neutral and readable by Trino and DuckDB (the encoding contract is covered under [Iceberg spatial type support](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/iceberg-spatial-type-support/)).

```python
from pyspark.sql.functions import expr

(result
   .withColumn("geom_wkb", expr("ST_AsBinary(geom)"))
   .drop("geom")
   .writeTo("lake.telemetry.pings_zoned")
   .using("iceberg")
   .createOrReplace())
```

## Verification and testing

Confirm the optimizer actually chose the distributed spatial join rather than a Cartesian product by inspecting the physical plan; a healthy plan contains a `RangeJoin` (or `DistanceJoin`) node, not `BroadcastNestedLoopJoin` over the full product.

```python
result.explain()   # look for "RangeJoin" and the spatial partitioner
print("rows:", result.count())

# bbox sanity: joined pings must fall within the union bbox of matched zones
result.selectExpr(
    "min(ST_XMin(geom)) minx", "min(ST_YMin(geom)) miny",
    "max(ST_XMax(geom)) maxx", "max(ST_YMax(geom)) maxy"
).show()
```

## Performance and tuning

Sedona performance is dominated by partition balance and index construction cost. Concrete knobs and ranges:

- `sedona.join.numpartition`: target 2–4 partitions per executor core; too few starves parallelism, too many inflates index build overhead. For a 200-core cluster, 400–800 is a reasonable band.
- `sedona.join.gridtype`: use `kdbtree` for skewed data (dense cities, sparse ocean) because it equalizes load; `quadtree` is fine for uniform distributions and builds faster.
- `spark.sql.autoBroadcastJoinThreshold`: raise it (or use an explicit hint) when the reference side is under ~100 MB so Sedona broadcasts instead of shuffling.
- `spark.executor.memory` / `spark.memory.fraction`: geometry objects and R-tree nodes are heap-heavy; budget 8–16 GB per executor for 100M+ geometry joins and enable off-heap if GC pauses dominate.

At the crossover point, a distributed join over ~500 GB with balanced KDB-tree partitions typically runs 3–8x faster than a single node that has to spill; below ~50 GB the single node wins because Sedona's job-startup and shuffle costs are not amortized. Pre-sorting the Iceberg source with [Z-ordering](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/z-ordering-for-geospatial-queries/optimizing-spatial-joins-with-iceberg-z-ordering/) cuts the bytes read before partitioning even begins.

## Common errors and fixes

| Symptom | Root cause | Fix |
|---|---|---|
| `Undefined function ST_GeomFromWKB` | Sedona functions not registered on the session | Call `SedonaContext.create(config)` and set the Kryo serializer + `SedonaKryoRegistrator` |
| Join runs as `BroadcastNestedLoopJoin`, never finishes | Predicate not recognized as a spatial range join | Put a single `ST_Intersects`/`ST_Contains` predicate in the `ON` clause; check `explain()` for `RangeJoin` |
| A few tasks run 100x longer than the rest | Data skew with `quadtree` partitioner | Switch `sedona.join.gridtype` to `kdbtree`; raise `sedona.join.numpartition` |
| Executors OOM during index build | Too many geometries per partition | Increase `numpartition`; raise `spark.executor.memory`; enable spill |
| Downstream engines can't read output geometry | Wrote Sedona `Geometry` type directly | Convert with `ST_AsBinary` to WKB before `writeTo(...).using("iceberg")` |

For authoritative API and configuration reference, consult the [Apache Sedona documentation](https://sedona.apache.org/latest/) and the [Sedona spatial join tuning guide](https://sedona.apache.org/latest/tutorial/sql/#optimize-spatial-join). To decide empirically whether Sedona, Trino, or DuckDB fits a given workload, run the harness in [benchmarking spatial query engines on GeoParquet](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/engine-benchmarking-selection/benchmarking-spatial-query-engines-on-geoparquet/).

## What Distribution Actually Costs

Moving a spatial workload onto a cluster is often described as scaling up. It is more accurately described as trading a memory limit for a network limit, and the trade is only worth making when the new limit is further away.

<figure class="diagram">
<svg viewBox="0 0 774 264" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="The three costs distribution introduces: serialisation of geometry between executors, the shuffle exchange itself, and skew where one partition dominates the runtime">
<rect x="0" y="0" width="774" height="264" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Three costs that only exist once work is distributed</text>
<rect x="26" y="56" width="230" height="196" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="141" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">serialisation</text>
<text x="141" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">geometry objects across</text>
<text x="141" y="134" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">the executor boundary</text>
<text x="141" y="164" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">mitigate: Kryo, and keep</text>
<text x="141" y="184" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">geometry as WKB until needed</text>
<text x="141" y="216" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">often 2–4× with Java serialisation</text>
<rect x="274" y="56" width="230" height="196" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="389" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">the shuffle</text>
<text x="389" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">all-to-all data movement</text>
<text x="389" y="134" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">before the join</text>
<text x="389" y="164" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">mitigate: broadcast the</text>
<text x="389" y="184" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">small side, or partition first</text>
<text x="389" y="216" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">the dominant cost when it happens</text>
<rect x="522" y="56" width="230" height="196" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="637" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">skew</text>
<text x="637" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">one dense cell holds</text>
<text x="637" y="134" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">a third of the work</text>
<text x="637" y="164" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">mitigate: adaptive resolution,</text>
<text x="637" y="184" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">or salt the hot cells</text>
<text x="637" y="216" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">the reason a job takes 4× the median task</text>
</svg>
</figure>

Skew is the cost most specific to spatial work, because spatial data is never uniformly distributed and a partitioner that divides space evenly divides *data* unevenly. A job whose median task finishes in ninety seconds and whose slowest finishes in twelve minutes is not compute-bound; it is waiting on one executor processing a metropolitan area. No amount of additional hardware fixes that, which is why the layout guidance elsewhere on this site — adaptive resolution, salted hot cells — matters as much for distributed compute as it does for storage cost.

Serialisation is the cheapest to fix and the most frequently overlooked. Geometry objects serialise poorly through the default Java path, and configuring Kryo typically shrinks shuffle volume by a factor of two to four on geometry-heavy stages. It is a two-line configuration change with a large effect, and it should be in place before any other tuning is attempted.

## Choosing the Join Strategy Deliberately

<figure class="diagram">
<svg viewBox="0 0 764 244" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three spatial join strategies in Sedona: broadcast index join for a small side, spatially partitioned join for large versus large, and a naive shuffle join which should never be chosen deliberately">
<rect x="0" y="0" width="764" height="244" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Pick the strategy before the cluster size</text>
<rect x="26" y="56" width="230" height="176" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="141" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">broadcast index join</text>
<text x="141" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">small side ships to every executor</text>
<text x="141" y="142" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">no shuffle at all</text>
<text x="141" y="164" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">local tree index per task</text>
<text x="141" y="192" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">use when one side &lt; a few hundred MB</text>
<text x="141" y="216" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">covers most production joins</text>
<rect x="274" y="56" width="230" height="176" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="389" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">partitioned join</text>
<text x="389" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">both sides partitioned by space</text>
<text x="389" y="142" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">one shuffle, then local joins</text>
<text x="389" y="164" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">duplicate geometries at borders</text>
<text x="389" y="192" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">use for large versus large</text>
<text x="389" y="216" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">watch for skew</text>
<rect x="522" y="56" width="230" height="176" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="637" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">naive shuffle join</text>
<text x="637" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">no spatial awareness</text>
<text x="637" y="142" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">candidate set grows with</text>
<text x="637" y="164" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">the product of both sides</text>
<text x="637" y="192" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">this is what you get by default</text>
<text x="637" y="216" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">never the intended answer</text>
</svg>
</figure>

The right-hand column is not a strategy anyone selects; it is what happens when neither of the other two is arranged. That is worth stating plainly because the symptom — a job that runs for hours and eventually fails on executor memory — reads like a capacity problem and gets treated with a larger cluster, which makes it fail more expensively.

Confirm the strategy from the physical plan rather than from the query text. A broadcast hint that the planner declined, because the side it was applied to exceeded the broadcast threshold, silently falls back to the shuffle path with no warning in the output.

## Getting the Cluster Configuration Right

A handful of settings account for most of the difference between a Sedona job that runs well and one that struggles, and they are all decided before the query is written.

**Serialisation.** Register Sedona's Kryo serialiser. This is the highest-value single line in the configuration, and omitting it inflates every shuffle involving geometry.

**Partition count.** The default parallelism is rarely right for spatial data. Too few partitions and each task holds too much geometry; too many and the per-task overhead dominates and the tree index in each is built over too little data to be useful. Sizing for roughly 128 MB of input per task is a reasonable starting point, measured on the geometry-heavy side.

**Broadcast threshold.** The default is tuned for scalar tables and is frequently too low for a boundary table whose few thousand rows are individually large. Raising it deliberately — with a measurement of the actual broadcast size — converts a shuffle join into a broadcast join, which is usually the largest single improvement available.

**Executor memory versus cores.** Geometry work is memory-hungry per task, so the usual instinct to maximise cores per executor works against you. Fewer, larger tasks with more memory each is generally better for spatial joins than many small ones, because the tree index and the candidate buffers are per-task.

Record whatever is chosen in the job definition with a comment explaining the measurement behind it. Spark configurations accumulate by copy-paste, and a setting whose reason is unrecorded is a setting nobody will ever be willing to change.

## Verifying the Job Did What Was Intended

The physical plan is the source of truth, and three things in it are worth checking on every substantive spatial job.

The **join operator** should be the broadcast index join or the spatially partitioned one, not a generic shuffle join with a filter above it. A filter above a join is the plan shape that indicates the spatial predicate was not used as a join condition at all, which turns the operation into a cross product with a post-filter.

The **exchange count** should be zero for a broadcast join and one for a partitioned join. Two or more means something forced an extra shuffle — commonly an aggregation with a different grouping, or a repartition inserted by an earlier step — and each one costs roughly as much as the join itself.

The **task duration distribution** should be tight. A ratio of more than three between the slowest task and the median means skew, and skew is a data-layout problem rather than a Spark problem. Fixing it in the layout is permanent; fixing it with more executors is rented.

Capture all three into the job's own logs rather than reading them from the UI after the fact. A job that records its own plan shape and task distribution gives you a history to compare against when it slows down six months later, which is the moment the information is most valuable and least available.

## When to Move a Job Off the Cluster

The reverse migration is worth revisiting periodically, because the conditions that justified distribution frequently stop holding.

The most common cause is a layout improvement. A job that scanned a terabyte because the table had no usable partition key may scan twenty gigabytes once it does, and twenty gigabytes fits on one machine comfortably. Nobody re-measures, so the cluster keeps running a job that no longer needs it.

The second cause is a change in the join shape. A large-versus-large join that was genuinely distributed becomes a broadcast join when someone aggregates the reference side, and a broadcast join over a reduced dataset frequently runs faster in a single process than in a cluster, because the cluster's fixed overheads — session start-up, task scheduling, shuffle setup — stop being amortised over enough work.

Re-measure after every significant change to either input, and treat "this runs on the cluster" as a decision with an expiry date rather than a property of the job. The cost of checking is one run of the same query in a single-node engine, and the saving when it turns out to be sufficient is the entire cluster.

For the join pattern that covers the majority of production spatial workloads, see [broadcast spatial joins with Apache Sedona](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/sedona-distributed-spatial-compute/broadcast-spatial-joins-with-apache-sedona/), which works through the hint, the threshold and the plan verification in code.

## A Note on Version Alignment

Sedona sits on top of Spark and pulls in a geometry stack of its own, and version mismatches in that stack produce failures that read as application bugs.

The combination that must line up is Spark, Scala, Sedona and the underlying geometry libraries. A Sedona build compiled for one Scala version will not load in a cluster running another, and the error is a class-loading failure rather than anything mentioning versions. Resolve the packages explicitly at submit time with pinned coordinates rather than relying on whatever the cluster image provides, and record the resolved set in the job's own logs.

The second alignment problem is subtler. Geometry results can differ between versions of the underlying library — validity repair, buffer output and overlay vertex ordering have all changed across releases — so an upgrade can change results without changing code. On a platform where geometry hashes are used for reconciliation, that surfaces as every row appearing to have changed. Pin the geometry stack alongside the engine, record the versions as table properties on write, and treat an upgrade as a change requiring a differential test rather than as routine maintenance.

Both problems are cheap to prevent and expensive to diagnose, and they account for a disproportionate share of the time teams lose to a distributed spatial stack in its first months.

Taken together, the configuration, the join strategy and the version discipline account for nearly all of the gap between a Sedona deployment that is a pleasure to operate and one that consumes a team. None of them is exotic, and all of them are decided before the first production job rather than after the first incident.
Both are the kind of decision that costs an afternoon in advance and a fortnight in arrears.
The distinction between the two is usually whether anybody wrote the numbers down.
Write them down.
The numbers outlive the people who measured them; the reasoning does not, unless it is recorded with them.
A configuration without a recorded measurement behind it is a configuration nobody will ever be willing to change.

Record the measurement next to the setting, in the job definition, where the next person to read it will actually be looking.
Anywhere else and it will be lost at the next refactor.
The job definition is the durable place.
 Everything else is a copy waiting to drift.
