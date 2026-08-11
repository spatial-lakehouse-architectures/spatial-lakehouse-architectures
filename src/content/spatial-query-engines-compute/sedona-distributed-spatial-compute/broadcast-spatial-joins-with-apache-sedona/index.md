# Broadcast Spatial Joins with Apache Sedona

This guide gives you a complete PySpark and Apache Sedona recipe that performs a broadcast spatial join — a small reference layer is broadcast to every executor while the large layer stays partitioned — using `ST_Intersects` with a spatial index, plus a physical-plan check that proves the broadcast happened.

## Context and prerequisites

A broadcast spatial join is the right pattern when one side of the join is small (a few thousand administrative or reference polygons, typically under ~100 MB) and the other is enormous. Instead of shuffling both sides across the network by a spatial partitioner, Sedona ships the small side to every executor and probes it with a local index — eliminating the large-side shuffle entirely. This recipe is the concrete companion to the [distributed spatial compute with Apache Sedona](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/sedona-distributed-spatial-compute/) topic area within the [Spatial Query Engines & Compute Optimization](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/) section. You need Spark 3.5, `apache-sedona` 1.6.x matched to that Spark version, and a reference layer that genuinely fits in executor memory; if both sides are large, use the KDB-tree partitioned join from the parent topic instead.

## Complete working solution

```python
# pip install apache-sedona==1.6.1 pyspark==3.5.1
from sedona.spark import SedonaContext
from pyspark.sql.functions import broadcast, expr

# (1) Session with Sedona serializers registered
config = (
    SedonaContext.builder()
    .appName("sedona-broadcast-spatial-join")
    .config(
        "spark.jars.packages",
        "org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.6.1,"
        "org.datasyslab:geotools-wrapper:1.6.1-28.2,"
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.0",
    )
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryo.registrator",
            "org.apache.sedona.core.serde.SedonaKryoRegistrator")
    .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lake.type", "rest")
    .config("spark.sql.catalog.lake.uri", "https://catalog.internal:8181")
    # let Sedona auto-broadcast small spatial sides up to 100 MB
    .config("spark.sql.autoBroadcastJoinThreshold", str(100 * 1024 * 1024))
    .getOrCreate()
)
sedona = SedonaContext.create(config)

# (2) Large partitioned layer: billions of pings from Iceberg
pings = sedona.sql("""
    SELECT device_id, event_ts, ST_GeomFromWKB(geom_wkb) AS geom
    FROM lake.telemetry.pings
    WHERE event_ts >= TIMESTAMP '2026-07-01 00:00:00'
""")

# (3) Small reference layer: a few thousand zone polygons from GeoParquet
zones = (
    sedona.read.format("geoparquet").load("s3a://ref/zones/")
    .selectExpr("zone_id", "geometry AS zone_geom")
)

# (4) Broadcast spatial join: hint the SMALL side; ST_Intersects drives it
joined = (
    pings.join(
        broadcast(zones),
        expr("ST_Intersects(geom, zone_geom)")
    )
    .select("device_id", "event_ts", "zone_id")
)

# (5) Materialize and persist back to Iceberg as WKB
(joined
   .withColumn("geom_present", expr("true"))
   .writeTo("lake.telemetry.pings_zoned")
   .using("iceberg")
   .createOrReplace())

print("joined rows:", joined.count())
```

## Step-by-step walkthrough

1. **Register Sedona on the session (block 1).** The Kryo serializer and `SedonaKryoRegistrator` are mandatory — without them, geometry cannot be serialized for a broadcast and you get a serialization error at the first shuffle or collect. `spark.sql.autoBroadcastJoinThreshold` raised to 100 MB lets the optimizer choose a broadcast automatically for the small side; the explicit `broadcast()` hint in step 4 forces it regardless.

2. **Read the large side (block 2).** `ST_GeomFromWKB` reconstructs geometries from the Iceberg WKB column. The `event_ts` filter is pushed into Iceberg scan planning so the large side is already reduced before the join — the same [predicate pushdown](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/predicate-pushdown-optimization/) leverage described for the SQL engines.

3. **Read the small reference side (block 3).** Sedona reads GeoParquet natively and decodes its geometry column, so no `ST_GeomFromWKB` call is needed here. Aliasing to `zone_geom` avoids a name clash in the join.

4. **Force the broadcast (block 4).** Wrapping `zones` in `broadcast()` tells Spark to ship the entire small layer to every executor and build a local index of it there. Each large-side partition then probes that in-memory index with `ST_Intersects` — no shuffle of the billions of pings, which is the whole point. `ST_Intersects` is the load-bearing predicate; it returns true when the geometries share any point.

5. **Write back as WKB (block 5).** Persisting to Iceberg keeps the table engine-neutral so [Trino](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/trino-spatial-sql-federation/) and DuckDB can read it later.

## Common errors and fixes

| Error | Cause | Fix |
|---|---|---|
| `BroadcastNestedLoopJoin` with no index, extremely slow | Broadcast side too large, or predicate not recognized | Ensure the reference side really is < ~100 MB; keep a single `ST_Intersects` in the join expression |
| `Kryo serialization failed: Buffer overflow` | Geometry serializer not registered, or buffer too small | Set `SedonaKryoRegistrator`; raise `spark.kryo.registrator.buffer.max` |
| `OutOfMemoryError` on executors | Broadcast layer does not actually fit in memory | Fall back to the KDB-tree partitioned join; lower `autoBroadcastJoinThreshold` |
| Join returns too many rows | Overlaps at reference-polygon boundaries counted twice | Deduplicate on `(device_id, event_ts)` or make reference polygons non-overlapping |

## Verification

Prove that Spark chose a broadcast plan (not a shuffle join, not a Cartesian nested loop) and spot-check correctness:

```python
# 1) The physical plan must contain a broadcast + spatial index probe
joined.explain()
# expect: "BroadcastSpatialJoin" (or Broadcast + RangeJoin) and
#         NO plain "SortMergeJoin" / full "BroadcastNestedLoopJoin"

# 2) Correctness: count for one known zone via an independent predicate
from pyspark.sql.functions import expr
one_zone = zones.where("zone_id = 'Z-0042'")
check = pings.join(
    broadcast(one_zone),
    expr("ST_Contains(zone_geom, geom)")
).count()
print("Z-0042 contained pings:", check)
```

The `explain()` output should show the small side under a broadcast exchange feeding a spatial join operator; if you instead see a `SortMergeJoin`, the broadcast hint did not apply and you should confirm the reference side's estimated size.

<figure class="diagram">
<svg viewBox="0 0 732 244" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Small zone layer broadcast to every executor and probed by partitioned pings with a local index">
<defs>
<marker id="arw-sedona-bcast" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#0e6e7d"/></marker>
</defs>
<rect x="0" y="0" width="732" height="244" fill="#f7fbfc"/>
<text x="380" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Broadcast spatial join: ship the small side, keep the big side put</text>
<rect x="300" y="50" width="160" height="55" rx="8" fill="#ffffff" stroke="#6a3d9a" stroke-width="2"/>
<text x="380" y="74" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="600" fill="#0d3b45">zones (small)</text>
<text x="380" y="93" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">broadcast + index</text>
<rect x="40" y="150" width="150" height="55" rx="8" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<text x="115" y="174" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="600" fill="#0d3b45">executor A</text>
<text x="115" y="192" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">pings part 0</text>
<rect x="305" y="150" width="150" height="55" rx="8" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<text x="380" y="174" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="600" fill="#0d3b45">executor B</text>
<text x="380" y="192" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">pings part 1</text>
<rect x="570" y="150" width="150" height="55" rx="8" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<text x="645" y="174" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="600" fill="#0d3b45">executor C</text>
<text x="645" y="192" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">pings part n</text>
<line x1="340" y1="105" x2="130" y2="150" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-sedona-bcast)"/>
<line x1="380" y1="105" x2="380" y2="150" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-sedona-bcast)"/>
<line x1="420" y1="105" x2="630" y2="150" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-sedona-bcast)"/>
<text x="380" y="228" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">Each executor probes its local zone index with ST_Intersects — no large-side shuffle</text>
</svg>
</figure>

To compare this against a shuffle-partitioned join, return to [distributed spatial compute with Apache Sedona](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/sedona-distributed-spatial-compute/); for the SQL-engine equivalent see [spatial joins across catalogs with Trino](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/trino-spatial-sql-federation/spatial-joins-across-catalogs-with-trino/), and to prepare the Iceberg source see [optimizing spatial joins with Iceberg Z-ordering](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/z-ordering-for-geospatial-queries/optimizing-spatial-joins-with-iceberg-z-ordering/). Canonical semantics for the join and broadcast hints are in the [Apache Sedona SQL documentation](https://sedona.apache.org/latest/tutorial/sql/).

## Why the Broadcast Wins

The hint in the recipe above is the whole optimisation, and understanding what it replaces makes the size threshold obvious rather than arbitrary.

<figure class="diagram">
<svg viewBox="0 0 736 272" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="A shuffle join moving both sides across the network against a broadcast join where the small polygon side is copied to every executor and the large side never moves">
<defs>
<marker id="bsj-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#2f6e49"/></marker>
</defs>
<rect x="0" y="0" width="736" height="272" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Move the small side, never the large one</text>
<text x="196" y="62" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#9a5a17">shuffle join</text>
<rect x="70" y="80" width="100" height="40" rx="6" fill="#f2e8da" stroke="#9a5a17" stroke-width="1.5"/>
<text x="120" y="105" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">points</text>
<rect x="222" y="80" width="100" height="40" rx="6" fill="#f2e8da" stroke="#9a5a17" stroke-width="1.5"/>
<text x="272" y="105" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">polygons</text>
<rect x="120" y="176" width="152" height="46" rx="6" fill="#eaddc8" stroke="#9a5a17" stroke-width="2"/>
<text x="196" y="205" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">exchange: both sides</text>
<line x1="120" y1="120" x2="170" y2="176" stroke="#9a5a17" stroke-width="2" marker-end="url(#bsj-arrow)"/>
<line x1="272" y1="120" x2="222" y2="176" stroke="#9a5a17" stroke-width="2" marker-end="url(#bsj-arrow)"/>
<text x="196" y="256" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">network cost scales with the large side</text>
<text x="584" y="62" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#2f6e49">broadcast join</text>
<rect x="458" y="80" width="100" height="40" rx="6" fill="#e6f0ea" stroke="#2f6e49" stroke-width="1.5"/>
<text x="508" y="105" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">points</text>
<rect x="610" y="80" width="100" height="40" rx="6" fill="#d7e8de" stroke="#2f6e49" stroke-width="2"/>
<text x="660" y="105" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">polygons</text>
<rect x="452" y="176" width="112" height="46" rx="6" fill="#e6f0ea" stroke="#2f6e49" stroke-width="1.5"/>
<text x="508" y="205" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">local join</text>
<rect x="600" y="176" width="120" height="46" rx="6" fill="#d7e8de" stroke="#2f6e49" stroke-width="2"/>
<text x="660" y="205" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">copy on each executor</text>
<line x1="508" y1="120" x2="508" y2="176" stroke="#2f6e49" stroke-width="2" marker-end="url(#bsj-arrow)"/>
<line x1="660" y1="120" x2="660" y2="176" stroke="#2f6e49" stroke-width="2" marker-end="url(#bsj-arrow)"/>
<line x1="600" y1="199" x2="564" y2="199" stroke="#2f6e49" stroke-width="2" marker-end="url(#bsj-arrow)"/>
<text x="584" y="256" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">network cost scales with the small side only</text>
</svg>
</figure>

The threshold follows directly: broadcasting is worth it whenever a copy of the small side fits comfortably in each executor's memory alongside its share of the large side. For boundary tables of a few thousand polygons that is almost always true, and the saving is the entire exchange of the large side — frequently hundreds of gigabytes.

The detail that makes it fast rather than merely cheap is the **local index**. Each executor builds a tree over its copy of the small side, so the join becomes an indexed lookup per point rather than a scan of every polygon. Without the index the broadcast still avoids the shuffle and then spends the saving on a nested loop.

## Confirming It Actually Broadcast

<figure class="diagram">
<svg viewBox="0 0 764 210" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three signals in the physical plan that confirm a broadcast index join: the join operator name, an exchange count of zero for the large side, and a broadcast exchange on the small side only">
<rect x="0" y="0" width="764" height="210" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Read the plan, not the hint</text>
<rect x="26" y="58" width="230" height="140" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="141" y="86" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">join operator</text>
<text x="141" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">a broadcast index join,</text>
<text x="141" y="134" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">not a generic join</text>
<text x="141" y="164" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">with a filter above it</text>
<rect x="274" y="58" width="230" height="140" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="389" y="86" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">large side unmoved</text>
<text x="389" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">no exchange beneath it</text>
<text x="389" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">this is the saving</text>
<rect x="522" y="58" width="230" height="140" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="637" y="86" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">small side broadcast</text>
<text x="637" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">one broadcast exchange</text>
<text x="637" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">sized as expected</text>
</svg>
</figure>

A hint that the planner declined — because the estimated side size exceeded the threshold — leaves no trace in the query text and produces a plan without any of these three signals. Since the estimate is frequently wrong for geometry columns, checking the plan rather than trusting the hint is the difference between a job that runs in four minutes and one that runs for two hours.

## When the Small Side Is Not Small Enough

Occasionally the reference side genuinely will not broadcast: a national parcel dataset, a detailed road network, a set of polygons whose vertex counts make a few thousand rows into several gigabytes.

Two reductions usually rescue it. **Simplification** at a tolerance matched to the join's resolution frequently shrinks a boundary set by an order of magnitude while changing no join result at the precision the analysis uses — a parcel boundary simplified to one metre is still exact enough to assign a point that is fifty metres inside it. Keep the exact geometry in the reference table and broadcast the simplified version, joining back for the rows that need precision.

**Pre-filtering** is the other. The broadcast side only needs the features that could match, so restricting it to the extent of the large side before broadcasting removes everything outside — which on a job scoped to one region removes almost all of a national dataset. Computing that extent is one aggregate over the large side and costs a fraction of the exchange it avoids.

When neither works, the fallback is the spatially partitioned join rather than a plain shuffle: partition both sides by the same grid, accept the duplication at cell borders, and join within partitions. It is more machinery and it keeps the network cost proportional to the data rather than to the product. The section overview covers when that trade is the right one, and the layout guidance elsewhere on this site covers keeping the resulting partitions from skewing.

The reduction to try first is almost always pre-filtering, because it costs one aggregate and frequently removes 95% of the reference side on a regionally-scoped job. Simplification is the second because it needs a tolerance decision that somebody has to own, and the partitioned join is the last because it is the only one that adds a permanent piece of machinery to the pipeline.
Try them in that order and most jobs never reach the third.

The pre-filter is also the only one of the three that requires no permanent decision, which makes it the right thing to reach for during an incident as well as during design.
It is also reversible, which the other two are not.
Reach for it first.
 It costs one aggregate and frequently ends the investigation.
