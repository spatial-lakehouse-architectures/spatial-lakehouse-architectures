# Choosing Between Broadcast and Partitioned Spatial Joins

This guide gives a measurement-driven decision procedure for the two distributed spatial join strategies, with the three numbers that decide it, the code to compute them, and the plan checks that confirm the engine did what you asked.

## Context and prerequisites

The choice is usually made by habit and is usually wrong in one direction: teams either broadcast a side that has grown too large, or run a partitioned join for a case a broadcast would have handled with no shuffle at all. This recipe runs on Spark 3.5 with Sedona and Iceberg 1.4; the anatomy behind the strategies is in [spatial join optimization](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/spatial-join-optimization/), and the Sedona-side mechanics in [broadcast spatial joins with Apache Sedona](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/sedona-distributed-spatial-compute/broadcast-spatial-joins-with-apache-sedona/).

## The decision, in one diagram

<figure class="diagram">
<svg viewBox="0 0 766 300" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Decision path from the serialised size of the smaller side and the skew of the join key to one of three outcomes: broadcast, reduce then broadcast, or spatially partitioned join">
<defs>
<marker id="cbp-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#2f6e49"/></marker>
</defs>
<rect x="0" y="0" width="766" height="300" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Two questions, three answers</text>
<rect x="270" y="50" width="240" height="52" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="390" y="72" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">smaller side, serialised?</text>
<text x="390" y="91" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">measured, not estimated</text>
<rect x="26" y="146" width="216" height="106" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="134" y="174" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">under ~200 MB</text>
<text x="134" y="200" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">broadcast index join</text>
<text x="134" y="224" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">no shuffle at all</text>
<rect x="282" y="146" width="216" height="106" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="390" y="174" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">200 MB – 2 GB</text>
<text x="390" y="200" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">reduce, then broadcast</text>
<text x="390" y="224" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">simplify or pre-filter first</text>
<rect x="538" y="146" width="216" height="106" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="646" y="174" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">over ~2 GB</text>
<text x="646" y="200" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">spatially partitioned join</text>
<text x="646" y="224" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">and check the key skew</text>
<line x1="330" y1="102" x2="170" y2="146" stroke="#2f6e49" stroke-width="2" marker-end="url(#cbp-arrow)"/>
<line x1="390" y1="102" x2="390" y2="146" stroke="#2f6e49" stroke-width="2" marker-end="url(#cbp-arrow)"/>
<line x1="450" y1="102" x2="610" y2="146" stroke="#2f6e49" stroke-width="2" marker-end="url(#cbp-arrow)"/>
<text x="390" y="284" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">The thresholds scale with executor memory; the shape of the decision does not</text>
</svg>
</figure>

The middle branch is the one most often skipped, and it is where most production joins actually belong. A reference layer of a few gigabytes is rarely a few gigabytes of information — it is a few gigabytes of cartographic vertex detail that the join does not use. Simplifying it to the join's resolution, or restricting it to the extent of the other side, routinely brings it under the broadcast threshold and removes the shuffle entirely.

## Complete working solution

```python
from pyspark.sql import functions as F

def measure_sides(spark, small_table: str, large_table: str, key: str) -> dict:
    small = spark.table(small_table)
    small_mb = (small.select(F.sum(F.length("geom_wkb")).alias("b"))
                     .collect()[0]["b"] or 0) / 1e6

    # Vertex detail is what makes a small reference layer large.
    detail = small.selectExpr("percentile_approx(ST_NPoints(ST_GeomFromWKB(geom_wkb)), 0.5) p50",
                              "percentile_approx(ST_NPoints(ST_GeomFromWKB(geom_wkb)), 0.99) p99")
    d = detail.collect()[0]

    # Skew of the join key on the large side, from a 1% sample.
    skew = (spark.table(large_table).sample(0.01)
                 .groupBy(key).count()
                 .selectExpr("max(count) mx",
                             "percentile_approx(count, 0.5) med")
                 .collect()[0])
    key_skew = (skew["mx"] / skew["med"]) if skew["med"] else float("inf")

    if small_mb < 200:
        strategy = "broadcast"
    elif small_mb < 2000:
        strategy = "reduce_then_broadcast"
    else:
        strategy = "partitioned"

    return {"small_side_mb": small_mb, "median_vertices": d["p50"],
            "p99_vertices": d["p99"], "key_skew": key_skew,
            "strategy": strategy,
            "warning": ("key skew will produce a straggler in a partitioned join"
                        if strategy == "partitioned" and key_skew > 4 else None)}
```

```python
# Strategy A — broadcast, after reducing the small side if needed.
regions = spark.table("reference.regions")
if measurements["strategy"] == "reduce_then_broadcast":
    regions = regions.selectExpr(
        "region_id",
        "ST_AsBinary(ST_SimplifyPreserveTopology(ST_GeomFromWKB(geom_wkb), 0.0001)) AS geom_wkb")

result = spark.sql("""
SELECT /*+ BROADCAST(r) */ t.asset_id, r.region_id
FROM   lakehouse.spatial.telemetry t
JOIN   regions r
  ON   ST_Intersects(ST_GeomFromWKB(t.geom_wkb), ST_GeomFromWKB(r.geom_wkb))
""")
```

```python
# Strategy B — spatially partitioned, for genuinely large-versus-large.
spark.conf.set("sedona.join.gridtype", "kdbtree")
spark.conf.set("sedona.join.numpartition", 512)

result = spark.sql("""
SELECT p.parcel_id, b.building_id
FROM   lakehouse.spatial.parcels p
JOIN   lakehouse.spatial.buildings b
  ON   ST_Intersects(ST_GeomFromWKB(p.geom_wkb), ST_GeomFromWKB(b.geom_wkb))
""")
```

## Step-by-step walkthrough

1. **Measure serialised bytes, not rows.** A thousand detailed coastlines and a million points can differ by two orders of magnitude in memory while the row counts suggest the opposite. `sum(length(geom_wkb))` is the number that predicts whether a broadcast fits.

2. **Measure vertex detail separately.** A high median vertex count is the signal that the middle branch applies — the layer is large because of detail rather than because of content, and simplification will reduce it dramatically without changing the join result at the resolution being used.

3. **Measure key skew before choosing the partitioned strategy.** A partitioned join over a skewed key inherits the skew as a straggler, and its advantage over a reduced broadcast can vanish entirely. Where the skew is above about four, fixing the layout comes first.

4. **Simplify to the join's tolerance, not to a round number.** The tolerance should be well below the accuracy the join's answer needs — typically a metre for administrative assignment — so the result is unchanged for every point that is not within a metre of a boundary.

5. **Let the partitioner choose its grid type.** A tree-based partitioner adapts to the data's distribution, which is precisely what a uniform grid fails to do on spatial data. Fixing the partition count is more useful than fixing the grid type.

## Common errors and fixes

| Symptom | Cause | Fix |
|---|---|---|
| Broadcast hint ignored, shuffle appears | Estimated side size exceeded the threshold | Raise `autoBroadcastJoinThreshold` after measuring, or reduce the side |
| Executors run out of memory during broadcast | Side larger in memory than serialised | Measure in-memory footprint too; geometry objects inflate substantially |
| Partitioned join has one very slow task | Key skew | Apply adaptive resolution or salt the hot cells before joining |
| Both strategies are slow | The scan is the problem, not the join | Check bytes read first; layout precedes join tuning |
| Result counts differ between strategies | Duplicate matches at cell borders not deduplicated | Deduplicate on the identifier pair in the partitioned path |

## Verification

<figure class="diagram">
<svg viewBox="0 0 762 222" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Plan signatures confirming each strategy: broadcast shows one broadcast exchange and no exchange under the large side, while partitioned shows exactly one shuffle exchange and balanced task durations">
<rect x="0" y="0" width="762" height="222" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">What each strategy looks like in the plan</text>
<rect x="30" y="58" width="352" height="152" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="206" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">broadcast</text>
<text x="206" y="118" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">BroadcastExchange on the small side</text>
<text x="206" y="142" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">no exchange under the large side</text>
<text x="206" y="166" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">an index join operator, not a filter</text>
<text x="206" y="192" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">broadcast size matches the measurement</text>
<rect x="398" y="58" width="352" height="152" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="574" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">partitioned</text>
<text x="574" y="118" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">exactly one shuffle exchange</text>
<text x="574" y="142" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">partition count as configured</text>
<text x="574" y="166" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">task durations within 3× of median</text>
<text x="574" y="192" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">shuffle bytes proportional to data</text>
</svg>
</figure>

```python
def assert_strategy(df, expected: str):
    plan = df.queryExecution.executedPlan.toString()
    exchanges = plan.count("Exchange")
    if expected == "broadcast":
        assert "BroadcastExchange" in plan, "the hint was declined"
        assert exchanges <= 1, f"unexpected shuffle: {exchanges} exchanges"
    else:
        assert exchanges == 1, f"expected one shuffle, found {exchanges}"
```

Run the assertion in the job rather than checking it by hand. A broadcast that silently degrades to a shuffle — because the reference layer grew past the threshold over six months — is the single most common cause of a spatial job that used to finish in four minutes and now takes two hours, and nothing in the output indicates it.

Record the measured side size alongside the result each run. The trend is what predicts the day the strategy needs revisiting, and having it recorded turns that from a surprise into a scheduled change.

## The Middle Branch in Practice

Reducing a side to make it broadcastable is the highest-leverage move available here, and it has three variants that apply in different situations.

<figure class="diagram">
<svg viewBox="0 0 764 246" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three reductions that bring a reference side under the broadcast threshold: simplification, extent pre filtering, and attribute projection, with typical size reductions">
<rect x="0" y="0" width="764" height="246" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Three reductions, applied in this order</text>
<rect x="26" y="58" width="230" height="176" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="141" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">1. extent pre-filter</text>
<text x="141" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">restrict to the other side&#8217;s extent</text>
<text x="141" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">on a regional job: −90% or more</text>
<text x="141" y="170" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">one aggregate to compute</text>
<text x="141" y="200" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">reversible, no accuracy cost</text>
<rect x="274" y="58" width="230" height="176" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="389" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">2. simplify</text>
<text x="389" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">to the join&#8217;s tolerance</text>
<text x="389" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">typically −85% of vertices</text>
<text x="389" y="170" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">measurable accuracy cost</text>
<text x="389" y="200" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">quantify the disagreement rate</text>
<rect x="522" y="58" width="230" height="176" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="637" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">3. project narrowly</text>
<text x="637" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">identifier and geometry only</text>
<text x="637" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">attributes join back later</text>
<text x="637" y="170" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">small but free</text>
<text x="637" y="200" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">always worth doing</text>
</svg>
</figure>

The extent pre-filter is first because it is free and reversible: computing the large side's bounding box is one aggregate, and restricting the reference layer to it removes everything that could not possibly match. On a job scoped to one region against a national reference layer this alone is often sufficient, and it introduces no accuracy question at all.

Simplification comes second because it does have an accuracy cost, and that cost should be quantified with a disagreement rate rather than accepted implicitly — the technique is covered in [simplifying geometries for analytical layers](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/geometry-validation-and-repair/simplifying-geometries-for-analytical-layers/).

Projection is last because it is small, but it is genuinely free and frequently overlooked: broadcasting a reference table with forty attribute columns ships thirty-nine of them to every executor for no purpose. Join on identifier and geometry, and bring the attributes back with a second, ordinary join on the much smaller result.

## When the Partitioned Join Is Genuinely Required

Three situations leave no reduction available, and for those the partitioned strategy is correct rather than a fallback.

**Both sides are facts.** A join between two large observation datasets — vehicle traces against traffic sensor readings, say — has no small reference side to reduce. Neither side is a lookup, both grow continuously, and the extent filter helps only if the job is regionally scoped.

**The reference layer is genuinely detailed and the detail matters.** A parcel-level join for a legal determination cannot be simplified, and the parcels of a metropolitan area are large in every representation. Here the accuracy requirement removes the middle branch by definition.

**The join is many-to-many with high fan-out.** Overlapping polygon layers — flood zones against land parcels — produce results larger than either input, and a broadcast does not help because the cost is in the candidate evaluation rather than the data movement.

In all three, the work shifts to controlling skew, because a partitioned join over spatial data will be skewed unless the partitioner adapts. Use a tree-based partitioner rather than a uniform grid, set the partition count from the data volume rather than accepting a default, and check the task duration spread after every run — a factor above three means one partition is doing the work of several and the remedy is in the layout rather than in the cluster.

The measurement to record here is shuffle bytes against input bytes. A healthy partitioned join shuffles roughly the size of its inputs once; one shuffling several times that is duplicating features across too many partitions, which points at a partitioner resolution that is too fine for the geometry sizes involved.
Recording it each run gives the trend that says when the partition count needs raising, which otherwise only becomes apparent when a job starts spilling.
The trend also settles arguments about whether a job "used to be faster", which are otherwise unresolvable and consume a surprising amount of time.
For the layout work that makes either strategy affordable in the first place, see [spatial partitioning schemes](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/spatial-partitioning-schemes/) — a join tuned against an unpartitioned table is tuning the wrong layer.
Layout first, strategy second, cluster size last — in that order the work compounds rather than competing.
Reversing it produces a larger cluster running the same badly-shaped join.
