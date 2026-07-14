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
<svg viewBox="0 0 760 300" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Sedona distributed spatial join: partition both layers by KDB-tree, build local indexes, join per partition">
<defs>
<marker id="arw-sedona-flow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#0e6e7d"/></marker>
</defs>
<rect x="0" y="0" width="760" height="300" fill="#f7fbfc"/>
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
