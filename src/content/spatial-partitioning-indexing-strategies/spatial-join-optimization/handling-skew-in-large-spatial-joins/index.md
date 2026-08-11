# Handling Skew in Large Spatial Joins

This guide diagnoses and fixes the straggler task that dominates large spatial joins, using three techniques chosen by what the skew profile actually shows rather than by trial and error.

## Context and prerequisites

Spatial data is never uniformly distributed, so any join partitioned by space inherits that non-uniformity as unequal task sizes. The symptom is a job whose median task finishes in a minute and whose slowest runs for an hour, and whose duration does not improve when executors are added. This recipe uses Spark 3.5 with Sedona; the strategy context is in [spatial join optimization](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/spatial-join-optimization/), and the layout-side remedies in [spatial partitioning schemes](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/spatial-partitioning-schemes/).

## Reading the skew profile

<figure class="diagram">
<svg viewBox="0 0 764 264" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three skew profiles distinguished by shape: a single dominant task, a small group of heavy tasks, and a generally uneven distribution, each pointing at a different cause and remedy">
<rect x="0" y="0" width="764" height="264" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">The shape of the profile names the cause</text>
<rect x="26" y="56" width="230" height="196" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="141" y="84" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">one dominant task</text>
<rect x="46" y="100" width="14" height="14" fill="#9a5a17"/><rect x="66" y="102" width="14" height="12" fill="#9a5a17"/>
<rect x="86" y="98" width="14" height="16" fill="#9a5a17"/><rect x="106" y="101" width="14" height="13" fill="#9a5a17"/>
<rect x="126" y="60" width="14" height="54" fill="#9a5a17"/>
<rect x="146" y="100" width="14" height="14" fill="#9a5a17"/><rect x="166" y="103" width="14" height="11" fill="#9a5a17"/>
<text x="141" y="146" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">one hot cell, stable</text>
<text x="141" y="172" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">remedy: salt that cell</text>
<text x="141" y="200" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">cheapest, most targeted</text>
<text x="141" y="228" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">e.g. a single metropolitan area</text>
<rect x="274" y="56" width="230" height="196" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="389" y="84" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">a heavy group</text>
<rect x="294" y="100" width="14" height="14" fill="#0e6e7d"/><rect x="314" y="80" width="14" height="34" fill="#0e6e7d"/>
<rect x="334" y="72" width="14" height="42" fill="#0e6e7d"/><rect x="354" y="86" width="14" height="28" fill="#0e6e7d"/>
<rect x="374" y="100" width="14" height="14" fill="#0e6e7d"/><rect x="394" y="102" width="14" height="12" fill="#0e6e7d"/>
<rect x="414" y="99" width="14" height="15" fill="#0e6e7d"/>
<text x="389" y="146" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">a dozen dense cells</text>
<text x="389" y="172" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">remedy: adaptive resolution</text>
<text x="389" y="200" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">split those cells deeper</text>
<text x="389" y="228" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">the common case</text>
<rect x="522" y="56" width="230" height="196" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="637" y="84" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">generally uneven</text>
<rect x="542" y="88" width="14" height="26" fill="#2f6e49"/><rect x="562" y="96" width="14" height="18" fill="#2f6e49"/>
<rect x="582" y="82" width="14" height="32" fill="#2f6e49"/><rect x="602" y="100" width="14" height="14" fill="#2f6e49"/>
<rect x="622" y="90" width="14" height="24" fill="#2f6e49"/><rect x="642" y="84" width="14" height="30" fill="#2f6e49"/>
<rect x="662" y="98" width="14" height="16" fill="#2f6e49"/>
<text x="637" y="146" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">no clear outliers</text>
<text x="637" y="172" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">remedy: more partitions</text>
<text x="637" y="200" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">or a tree partitioner</text>
<text x="637" y="228" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">not really skew — granularity</text>
</svg>
</figure>

The right-hand profile is not skew and treating it as such wastes effort. A distribution with a factor of two between the largest and smallest task is normal, and the remedy — if any is needed — is simply more partitions so the scheduler has finer units to balance.

The left and middle profiles are genuine skew, and they differ in the right response: a single stable hot cell is best handled by salting it specifically, while a group of dense cells calls for adaptive resolution across the layer.

## Complete working solution

```python
from pyspark.sql import functions as F

def skew_profile(spark, table: str, key: str, sample: float = 0.01) -> dict:
    counts = (spark.table(table).sample(sample)
                   .groupBy(key).count().orderBy(F.desc("count")))
    rows = counts.limit(50).collect()
    stats = counts.selectExpr(
        "percentile_approx(count, 0.5) med",
        "percentile_approx(count, 0.99) p99",
        "max(count) mx", "count(*) n").collect()[0]

    med, mx = stats["med"] or 1, stats["mx"]
    heavy = [r for r in rows if r["count"] > med * 4]

    if mx / med < 4:
        shape = "uneven"          # not skew; raise the partition count
    elif len(heavy) <= 2:
        shape = "single_hot"      # salt those keys
    else:
        shape = "heavy_group"     # adaptive resolution across the layer

    return {"shape": shape, "median": med, "max": mx, "ratio": mx / med,
            "heavy_keys": [(r[key], r["count"]) for r in heavy[:20]],
            "distinct_keys": stats["n"]}
```

```python
# Remedy 1 — salt a small, stable hot set. Readers must expand the variants.
HOT = {599686042433355775, 599686042697596927}     # from the profile
SALT_BUCKETS = 32

large = (spark.table("lakehouse.spatial.telemetry")
    .withColumn("join_key",
        F.when(F.col("h3_r5").isin(list(HOT)),
               F.concat_ws("#", F.col("h3_r5"),
                           (F.rand() * SALT_BUCKETS).cast("int")))
         .otherwise(F.col("h3_r5").cast("string"))))

# The reference side must be replicated across the salt buckets for hot keys.
salts = spark.range(SALT_BUCKETS).withColumnRenamed("id", "salt")
small = (spark.table("reference.regions")
    .join(F.broadcast(salts),
          F.col("h3_r5").isin(list(HOT)), how="left")
    .withColumn("join_key",
        F.when(F.col("salt").isNotNull(),
               F.concat_ws("#", F.col("h3_r5"), F.col("salt")))
         .otherwise(F.col("h3_r5").cast("string"))))

result = large.join(small, "join_key").drop("join_key", "salt")
```

## Step-by-step walkthrough

1. **Profile from a sample, not the full table.** A one-percent sample identifies the hot keys reliably and costs a fraction of a full aggregation. The absolute counts are wrong by the sampling factor; the ratios, which are what matter, are not.

2. **Classify by shape before choosing a remedy.** Applying salting to a "generally uneven" profile adds complexity for no gain; applying more partitions to a single dominant key does nothing at all, because the key still lands in one partition.

3. **Salt only the hot keys.** Salting everything multiplies the reference side by the bucket count across the whole join, which is a large cost for a problem confined to two cells. The conditional expression keeps the ordinary keys untouched.

4. **Replicate the reference side for salted keys only.** This is the part that is easy to get wrong: a salted fact row can only match a reference row carrying the same salt, so the reference side must exist in every bucket for those keys. Missing this produces silently incomplete results.

5. **Keep the hot set small and reviewed.** Salting depends on the hot set being stable. Re-derive it from the profile on a schedule, and alert when a new key enters it, because a hot set that has grown to thirty keys is really the middle profile and needs the other remedy.

## Common errors and fixes

| Symptom | Cause | Fix |
|---|---|---|
| Rows missing after salting | Reference side not replicated across buckets | Cross-join the hot reference rows with the salt range |
| Salting made the job slower | Applied to all keys, not just the hot ones | Condition the salt on the hot set |
| Skew persists after adaptive resolution | Density shifted since the mapping was built | Re-derive the resolution mapping; version it |
| One task still dominates | A single feature with an enormous vertex count | This is geometry skew, not key skew — simplify or isolate it |
| Result counts changed | Deduplication missing after replication | Deduplicate on the identifier pair, or use a canonical bucket |

## The other skew: one enormous geometry

<figure class="diagram">
<svg viewBox="0 0 762 222" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Two distinct sources of straggler tasks: many rows in one partition, and one row whose geometry has an enormous vertex count, requiring different remedies">
<rect x="0" y="0" width="762" height="222" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Two different stragglers, two different fixes</text>
<rect x="30" y="58" width="352" height="152" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="206" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">row-count skew</text>
<text x="206" y="118" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">a partition holds 40× the rows</text>
<text x="206" y="146" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">visible in the key profile</text>
<text x="206" y="174" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">fix: salt or split the key</text>
<rect x="398" y="58" width="352" height="152" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="574" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">geometry skew</text>
<text x="574" y="118" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">one row with 400 000 vertices</text>
<text x="574" y="146" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">invisible in the key profile</text>
<text x="574" y="174" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">fix: simplify, or handle separately</text>
</svg>
</figure>

Geometry skew is the case that survives every key-based remedy, because the problem is one row rather than many. A coastline or a national boundary with hundreds of thousands of vertices costs more to evaluate against than a hundred thousand simple polygons, and no repartitioning divides a single row.

Diagnose it by joining the task metrics against a vertex-count column — which is the argument for recording vertex counts at ingest, as described in [geometry validation and repair](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/geometry-validation-and-repair/). The remedy is either to simplify the offending features for join purposes, or to split them into smaller pieces with a shared identifier so the work divides.

## Verification

```python
def assert_balanced(spark, job_id: str, max_ratio: float = 3.0):
    tasks = spark.sparkContext.statusTracker().getStageInfo(job_id).taskDurations
    tasks = sorted(tasks)
    median = tasks[len(tasks) // 2]
    assert tasks[-1] / median <= max_ratio, (
        f"straggler: slowest task {tasks[-1]/median:.1f}× the median")
```

Assert the balance rather than inspecting it, and record the ratio with each run. Skew re-emerges as data grows — a city that was not hot last year becomes hot this year — and a recorded series turns that into a scheduled adjustment instead of an incident.

## Adaptive Resolution, in Practice

For the middle profile — a dozen dense cells rather than one — salting becomes unwieldy and adaptive resolution is the right remedy. The mechanism is a small mapping table consulted at write time.

<figure class="diagram">
<svg viewBox="0 0 762 244" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="An adaptive resolution mapping table assigning a deeper grid level to dense cells, consulted by the write path so each partition lands inside the target size band">
<defs>
<marker id="hs-adapt-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#0e6e7d"/></marker>
</defs>
<rect x="0" y="0" width="762" height="244" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">One small table decides each row&#8217;s partition depth</text>
<rect x="30" y="70" width="200" height="86" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="130" y="100" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">write path</text>
<text x="130" y="124" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">computes the coarse cell</text>
<rect x="290" y="70" width="200" height="86" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="390" y="100" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">depth mapping</text>
<text x="390" y="124" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">coarse cell &#8594; chosen depth</text>
<rect x="550" y="70" width="200" height="86" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="650" y="100" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">partition value</text>
<text x="650" y="124" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">at the chosen depth</text>
<line x1="230" y1="113" x2="290" y2="113" stroke="#0e6e7d" stroke-width="2" marker-end="url(#hs-adapt-arrow)"/>
<line x1="490" y1="113" x2="550" y2="113" stroke="#0e6e7d" stroke-width="2" marker-end="url(#hs-adapt-arrow)"/>
<text x="390" y="200" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">Dense cells go two or three levels deeper; ocean cells stay coarse</text>
<text x="390" y="228" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">Version the mapping — changing it reshuffles history</text>
</svg>
</figure>

Two properties make this work and both are easy to omit. The mapping must be **versioned**, because changing a cell's depth changes where its rows land, and a table written under two versions has a mixed layout that queries must account for. Record the version with each file, and re-derive only during a planned rewrite.

And the reader must be able to **expand a query window into cells at mixed depths**, which requires the containment relation between a cell and its parent to be computable. Hierarchical grids make this trivial; it is one of the concrete reasons to prefer them, as set out in [H3 vs S2 vs geohash for lakehouse partitioning](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/grid-system-selection/h3-vs-s2-vs-geohash-for-lakehouse-partitioning/).

Rebuild the mapping from a sample on a schedule rather than continuously — density shifts over months, not hours, and a mapping that changes weekly produces a table whose layout nobody can reason about. Quarterly, with an alert when a cell's row count leaves its target band, is a workable cadence for most platforms.

## Choosing the Cheapest Remedy That Works

The three remedies differ substantially in cost and in how permanent they are, which is worth weighing before reaching for the most thorough one.

**More partitions** is free and reversible: it is a configuration change with no effect on stored data, and it resolves the "generally uneven" profile completely. Try it first, always, because it costs one run to find out.

**Salting** is cheap and localised. It changes the query on both sides but not the stored layout, so it can be applied to one job without affecting anything else on the platform, and it can be removed as easily. Its weakness is that it depends on the hot set being small and stable, and it adds a permanent complication to the queries that use it.

**Adaptive resolution** changes the stored layout, which makes it the most effective and the least reversible. It fixes the skew for every job that touches the table rather than for one, and it requires a rewrite plus a versioned mapping that somebody must maintain. Reach for it when several jobs share the same skew, which is usually the case once one job has noticed it.

The sequence that wastes the least effort is therefore: raise the partition count, measure; salt the hot keys, measure; and only then consider changing the layout. Teams that start at the third step frequently discover afterwards that the first would have been sufficient, having spent a rewrite to find out.

One caution on measurement: compare like with like. A job re-run after a change will differ in cache warmth, cluster availability and data volume as well as in the change being tested, so a single before-and-after pair is weak evidence. Run each configuration two or three times, compare the medians, and treat a difference under twenty percent as noise. Spatial jobs are variable enough that a smaller apparent improvement is usually not one.
Record the medians rather than the best run, since the best run flatters whichever configuration was tested when the cluster was least busy.
