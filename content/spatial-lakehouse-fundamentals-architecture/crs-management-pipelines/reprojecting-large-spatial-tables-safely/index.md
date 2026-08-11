# Reprojecting Large Spatial Tables Safely

This guide reprojects a multi-terabyte spatial table without a maintenance window, verifying accuracy against control points at every stage and keeping the table queryable throughout — because the naive approach silently degrades coordinates and there is no way to tell afterwards.

## Context and prerequisites

Reprojection is the operation most likely to corrupt a spatial table quietly. It changes every coordinate, it depends on datum grid files that may be absent, and its output is always plausible. This recipe uses PySpark 3.5 with Sedona and Iceberg 1.4; the accuracy background is in [CRS management pipelines](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/crs-management-pipelines/), and the drift detection it reuses in [detecting CRS drift in ingestion pipelines](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/crs-management-pipelines/detecting-crs-drift-in-ingestion-pipelines/).

## The two failure modes that matter

<figure class="diagram">
<svg viewBox="0 0 762 264" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Two reprojection failure modes: a missing datum grid file causing a silent fallback with metre scale error, and derived columns left describing the pre transform coordinates">
<rect x="0" y="0" width="762" height="264" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Both produce a table that looks entirely correct</text>
<rect x="30" y="56" width="352" height="196" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="206" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">missing datum grid</text>
<text x="206" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">the container lacks the grid file</text>
<text x="206" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">PROJ falls back to a coarse</text>
<text x="206" y="162" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">seven-parameter transform</text>
<text x="206" y="190" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">every coordinate moves 0.5 – 3 m</text>
<text x="206" y="220" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">no warning, no error, no way to detect after</text>
<rect x="398" y="56" width="352" height="196" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="574" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">stale derived columns</text>
<text x="574" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">geometry transformed, bbox not</text>
<text x="574" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">boxes describe the old position</text>
<text x="574" y="162" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">grid cells likewise</text>
<text x="574" y="190" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">pruning excludes the right files</text>
<text x="574" y="220" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">queries return too few rows, quietly</text>
</svg>
</figure>

The right-hand failure is the more common and the easier to prevent: every derived column must be recomputed from the transformed geometry, in the same job, as a single unit. The left-hand failure requires the control-point discipline described below, because nothing in the output distinguishes an accurate transform from an approximate one.

## Complete working solution

```python
from pyspark.sql import SparkSession, functions as F
from sedona.spark import SedonaContext

SRC_SRID, DST_SRID = 25832, 4326
CONTROL_TOLERANCE_M = 0.05          # survey markers must land within 5 cm

spark = SedonaContext.create(SparkSession.builder
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.apache.sedona.sql.SedonaSqlExtensions")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate())

# 0. Prove the transform is accurate before touching any data.
control = spark.table("governance.control_points")            # known src and dst coords
check = control.selectExpr(
    "marker_id",
    f"ST_Distance(ST_Transform(ST_GeomFromWKB(src_geom), 'EPSG:{SRC_SRID}', "
    f"'EPSG:{DST_SRID}'), ST_GeomFromWKB(expected_dst_geom)) AS dist_deg")
worst = check.selectExpr("max(dist_deg) * 111320 AS worst_m").collect()[0]["worst_m"]
assert worst < CONTROL_TOLERANCE_M, (
    f"transform inaccurate: worst control point off by {worst:.3f} m — "
    "the datum grid is probably missing from this image")

# 1. Reproject one partition at a time, deriving every dependent column together.
def reproject_partition(day: str) -> None:
    src = spark.table("lakehouse.spatial.telemetry_25832").where(F.col("event_day") == day)
    out = src.selectExpr(
        "asset_id", "event_ts", "event_day",
        f"ST_AsBinary(ST_Transform(ST_GeomFromWKB(geom_wkb), "
        f"'EPSG:{SRC_SRID}', 'EPSG:{DST_SRID}')) AS geom_wkb",
        f"{SRC_SRID} AS source_srid",
    ).selectExpr(
        "*",
        "ST_XMin(ST_GeomFromWKB(geom_wkb)) AS bbox_min_x",
        "ST_YMin(ST_GeomFromWKB(geom_wkb)) AS bbox_min_y",
        "ST_XMax(ST_GeomFromWKB(geom_wkb)) AS bbox_max_x",
        "ST_YMax(ST_GeomFromWKB(geom_wkb)) AS bbox_max_y",
        "ST_H3CellIds(ST_GeomFromWKB(geom_wkb), 5, true)[0] AS h3_r5",
    )
    (out.sortWithinPartitions("bbox_min_x", "bbox_min_y")
        .writeTo("lakehouse.spatial.telemetry_4326").append())
```

## Step-by-step walkthrough

1. **Assert the transform before the data.** The control-point check runs in seconds and is the only thing standing between a correct migration and a three-metre systematic offset across a whole table. Run it in the same container that will run the job, because the failure is environmental rather than logical.

2. **Write to a new table, not in place.** An in-place update of every row is a full rewrite with none of the safety of a copy: no rollback, no dual-read period, no way to compare. A new table costs storage for the overlap and turns a risky migration into a routine one.

3. **Recompute every derived column in the same expression chain.** The bounding boxes and the grid cell come from the transformed geometry, in the same statement, so there is no window in which they can disagree. Deriving them in a later job is the mechanism behind the second failure mode.

4. **Record the source SRID.** After the migration, the provenance of every row is a column rather than a fact somebody remembers, which makes a later question about accuracy answerable.

5. **Sort within partitions on the new coordinates.** The old sort order was in the old coordinate system and means nothing in the new one; re-sorting during the migration is free because the data is being rewritten anyway.

## Common errors and fixes

| Symptom | Cause | Fix |
|---|---|---|
| Control points off by 1–3 m | Datum grid file absent from the image | Install the grid; do not proceed until the assertion passes |
| Query returns fewer rows than expected | Bounding boxes describe pre-transform coordinates | Recompute derived columns from the transformed geometry |
| Some geometries become invalid | Transform collapsed near-duplicate vertices | Validate and repair after the transform, not only before |
| Job runs out of memory on one partition | A dense partition plus geometry inflation | Scope by day and by region; reproject in smaller units |
| Areas change unexpectedly | Comparing areas across coordinate systems | Compute areas in an equal-area projection, not in degrees |

## Verification

<figure class="diagram">
<svg viewBox="0 0 768 230" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Four post migration checks: control point accuracy, row count parity per partition, bounding box coverage of geometry, and extent plausibility in the target coordinate system">
<rect x="0" y="0" width="768" height="230" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Four checks, run per partition as it lands</text>
<rect x="26" y="58" width="172" height="160" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="112" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">accuracy</text>
<text x="112" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">control points</text>
<text x="112" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">within tolerance</text>
<text x="112" y="172" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">catches the grid</text>
<rect x="212" y="58" width="172" height="160" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="298" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">row parity</text>
<text x="298" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">counts per day</text>
<text x="298" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">exactly equal</text>
<text x="298" y="172" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">catches dropped rows</text>
<rect x="398" y="58" width="172" height="160" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="484" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">bbox coverage</text>
<text x="484" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">box contains geometry</text>
<text x="484" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">every row</text>
<text x="484" y="172" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">catches stale derivation</text>
<rect x="584" y="58" width="172" height="160" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="670" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">extent sanity</text>
<text x="670" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">inside target range</text>
<text x="670" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">and expected area</text>
<text x="670" y="172" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#6a3d9a">catches wrong direction</text>
</svg>
</figure>

```sql
-- Run per partition immediately after it is written.
SELECT
  count(*)                                                     AS rows_out,
  count_if(NOT ST_Contains(
      ST_MakeEnvelope(bbox_min_x, bbox_min_y, bbox_max_x, bbox_max_y),
      ST_GeomFromWKB(geom_wkb)))                                AS bbox_violations,
  count_if(abs(bbox_min_x) > 180 OR abs(bbox_min_y) > 90)       AS out_of_range,
  min(bbox_min_x), max(bbox_max_x), min(bbox_min_y), max(bbox_max_y)
FROM lakehouse.spatial.telemetry_4326
WHERE event_day = DATE '2026-03-11';
```

Any non-zero value in the middle two columns fails the partition, and the correct response is to delete and re-run that partition rather than to investigate afterwards — the migration is idempotent by partition, which is the property that makes a partial failure recoverable.

## Cutting over

<figure class="diagram">
<svg viewBox="0 0 768 216" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Four cutover phases: backfill history into the new table, dual write both systems, move readers, then retire the source after a quiet period">
<defs>
<marker id="repro-cut-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#0e6e7d"/></marker>
</defs>
<rect x="0" y="0" width="768" height="216" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Nothing is deleted until the last step</text>
<rect x="24" y="70" width="164" height="80" rx="8" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<text x="106" y="100" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">1. backfill</text>
<text x="106" y="124" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">partition by partition</text>
<rect x="212" y="70" width="164" height="80" rx="8" fill="#ffffff" stroke="#0e6e7d" stroke-width="2"/>
<text x="294" y="100" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">2. dual write</text>
<text x="294" y="124" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">both tables current</text>
<rect x="400" y="70" width="164" height="80" rx="8" fill="#ffffff" stroke="#0e6e7d" stroke-width="2"/>
<text x="482" y="100" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">3. move readers</text>
<text x="482" y="124" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">one consumer at a time</text>
<rect x="588" y="70" width="168" height="80" rx="8" fill="#ffffff" stroke="#6a3d9a" stroke-width="2"/>
<text x="672" y="100" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">4. retire</text>
<text x="672" y="124" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">after a quiet quarter</text>
<line x1="188" y1="110" x2="212" y2="110" stroke="#0e6e7d" stroke-width="2" marker-end="url(#repro-cut-arrow)"/>
<line x1="376" y1="110" x2="400" y2="110" stroke="#0e6e7d" stroke-width="2" marker-end="url(#repro-cut-arrow)"/>
<line x1="564" y1="110" x2="588" y2="110" stroke="#0e6e7d" stroke-width="2" marker-end="url(#repro-cut-arrow)"/>
<text x="390" y="200" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">Reader migration is the long pole; discovery takes longer than the rewrite</text>
</svg>
</figure>

Budget for stage three rather than for stage one. The rewrite is a scheduled job whose duration is known; finding every consumer of the old table is an archaeology exercise, and a query-history sweep by table name is the fastest way to do it. Expect it to turn up two or three consumers nobody remembered, which is exactly why the old table stays live for a quarter after the last known reader moves.

## Choosing the Target System

Before the mechanics, the decision: which coordinate system the table should end up in. Getting this wrong makes the migration a step sideways.

The default answer for a lakehouse table is a **geographic system**, most commonly 4326, and the reason is interoperability rather than accuracy. Every engine, every library and every downstream consumer understands it without configuration, grid identifiers derive from it directly, and a table in it can be joined to any other table in it without a transform. The cost is that distances and areas are not directly computable in its units, which is a real inconvenience and one that is solved per query rather than per table.

A **projected system** is the right choice when the dominant workload is measurement — areas, distances, buffers — over a bounded region, and when the accuracy of those measurements matters more than the convenience of joining. The cost is that the table can only be joined to others in the same projection, that the projection is valid over a limited extent, and that expanding the table's coverage later may exceed that extent.

The awkward middle case is a table whose measurements matter and whose extent is continental. There, storing in a geographic system and projecting per query — into an equal-area projection for area work, into a local one for distance work — is usually better than committing the storage to one projection that is wrong somewhere. It costs a transform per query on the rows that survive filtering, which is a much smaller set than the table.

Whichever is chosen, record the reasoning in the table properties alongside the CRS itself. A future migration will ask why the current system was picked, and a one-line answer prevents the migration from being repeated in the opposite direction two years later.

## Cost and Duration

A reprojection is a full rewrite, so its cost is predictable from the table size and the cluster, and it is worth estimating before proposing the work rather than after starting it.

The transform itself is cheap relative to the I/O: PROJ handles hundreds of thousands of coordinates per second per core, so a table of a billion points transforms in minutes of aggregate CPU. The cost is dominated by reading, decoding, re-encoding and writing, which means the migration runs at roughly the speed of a full-table copy on the same hardware — a useful number because most teams already know it.

Two things inflate that estimate in practice. **Complex geometries** transform vertex by vertex, so a table of detailed boundaries costs proportionally more than its row count suggests. And **re-sorting** during the write adds a shuffle, which is worth paying for because the old sort order is meaningless in the new system, but should be in the estimate rather than a surprise.

The one cost that is easy to forget is **double storage during the overlap**. Both tables exist from the start of the backfill until the source is retired, which on a strict retention budget can be the binding constraint rather than compute. Where it is, migrate partition ranges and expire the corresponding source partitions as each range is verified — slower and it keeps the peak footprint bounded.

A last note on scheduling: run the backfill against historical partitions that are no longer receiving writes, and leave the current partition until the dual-write period has been running long enough to trust. That ordering means the migration never contends with the ingest path, and a failure in the backfill affects data nobody is querying live.
It also means the riskiest part of the migration happens against the least critical data, which is the right way round.
