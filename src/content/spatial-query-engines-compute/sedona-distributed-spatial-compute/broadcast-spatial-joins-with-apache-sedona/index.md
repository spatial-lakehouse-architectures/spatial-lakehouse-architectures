# Broadcast Spatial Joins with Apache Sedona

This guide gives you a complete PySpark and Apache Sedona recipe that performs a broadcast spatial join — a small reference layer is broadcast to every executor while the large layer stays partitioned — using `ST_Intersects` with a spatial index, plus a physical-plan check that proves the broadcast happened.

## Context and prerequisites

A broadcast spatial join is the right pattern when one side of the join is small (a few thousand administrative or reference polygons, typically under ~100 MB) and the other is enormous. Instead of shuffling both sides across the network by a spatial partitioner, Sedona ships the small side to every executor and probes it with a local index — eliminating the large-side shuffle entirely. This recipe is the concrete companion to the [distributed spatial compute with Apache Sedona](/spatial-query-engines-compute/sedona-distributed-spatial-compute/) topic area within the [Spatial Query Engines & Compute Optimization](/spatial-query-engines-compute/) section. You need Spark 3.5, `apache-sedona` 1.6.x matched to that Spark version, and a reference layer that genuinely fits in executor memory; if both sides are large, use the KDB-tree partitioned join from the parent topic instead.

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

2. **Read the large side (block 2).** `ST_GeomFromWKB` reconstructs geometries from the Iceberg WKB column. The `event_ts` filter is pushed into Iceberg scan planning so the large side is already reduced before the join — the same [predicate pushdown](/spatial-partitioning-indexing-strategies/predicate-pushdown-optimization/) leverage described for the SQL engines.

3. **Read the small reference side (block 3).** Sedona reads GeoParquet natively and decodes its geometry column, so no `ST_GeomFromWKB` call is needed here. Aliasing to `zone_geom` avoids a name clash in the join.

4. **Force the broadcast (block 4).** Wrapping `zones` in `broadcast()` tells Spark to ship the entire small layer to every executor and build a local index of it there. Each large-side partition then probes that in-memory index with `ST_Intersects` — no shuffle of the billions of pings, which is the whole point. `ST_Intersects` is the load-bearing predicate; it returns true when the geometries share any point.

5. **Write back as WKB (block 5).** Persisting to Iceberg keeps the table engine-neutral so [Trino](/spatial-query-engines-compute/trino-spatial-sql-federation/) and DuckDB can read it later.

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
<svg viewBox="0 0 760 240" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Small zone layer broadcast to every executor and probed by partitioned pings with a local index">
<defs>
<marker id="arw-sedona-bcast" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#0e6e7d"/></marker>
</defs>
<rect x="0" y="0" width="760" height="240" fill="#f7fbfc"/>
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

To compare this against a shuffle-partitioned join, return to [distributed spatial compute with Apache Sedona](/spatial-query-engines-compute/sedona-distributed-spatial-compute/); for the SQL-engine equivalent see [spatial joins across catalogs with Trino](/spatial-query-engines-compute/trino-spatial-sql-federation/spatial-joins-across-catalogs-with-trino/), and to prepare the Iceberg source see [optimizing spatial joins with Iceberg Z-ordering](/spatial-partitioning-indexing-strategies/z-ordering-for-geospatial-queries/optimizing-spatial-joins-with-iceberg-z-ordering/). Canonical semantics for the join and broadcast hints are in the [Apache Sedona SQL documentation](https://sedona.apache.org/latest/tutorial/sql/).
