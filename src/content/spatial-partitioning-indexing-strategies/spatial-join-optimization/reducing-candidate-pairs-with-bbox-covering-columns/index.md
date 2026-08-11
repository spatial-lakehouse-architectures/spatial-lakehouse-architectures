# Reducing Candidate Pairs With Bbox Covering Columns

This guide adds a numeric bounding-box covering to both sides of a spatial join so the expensive geometry predicate runs on a fraction of the pairs, and shows how to verify that the reduction actually happened rather than assuming it.

## Context and prerequisites

The exact predicate in a spatial join costs roughly the same in every engine, because most of them call the same geometry library. What differs — by orders of magnitude — is how many pairs reach it. A numeric covering is the cheapest available way to reduce that number, and it works in any engine that can compare doubles. This recipe uses Trino and Spark SQL; the surrounding decisions are in [spatial join optimization](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/spatial-join-optimization/), and the pushdown mechanics in [predicate pushdown optimization](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/predicate-pushdown-optimization/).

## What a covering does

<figure class="diagram">
<svg viewBox="0 0 714 256" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Bounding box overlap test as a conservative filter: boxes that do not overlap cannot have intersecting geometries, so only overlapping box pairs reach the exact predicate">
<rect x="0" y="0" width="714" height="256" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Boxes that miss cannot contain geometries that meet</text>
<rect x="70" y="70" width="150" height="110" fill="none" stroke="#0e6e7d" stroke-width="2" stroke-dasharray="5 4"/>
<path d="M92 100 L188 84 L204 158 L100 168 Z" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<rect x="250" y="96" width="130" height="100" fill="none" stroke="#2f6e49" stroke-width="2" stroke-dasharray="5 4"/>
<path d="M268 120 L358 110 L364 176 L276 182 Z" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="225" y="216" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#2f6e49">boxes disjoint &#8594; skip, no decode</text>
<text x="225" y="240" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">four double comparisons</text>
<rect x="450" y="70" width="150" height="110" fill="none" stroke="#0e6e7d" stroke-width="2" stroke-dasharray="5 4"/>
<path d="M472 100 L568 84 L584 158 L480 168 Z" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<rect x="540" y="96" width="150" height="100" fill="none" stroke="#9a5a17" stroke-width="2" stroke-dasharray="5 4"/>
<path d="M600 128 L676 118 L682 180 L594 186 Z" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="570" y="216" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#9a5a17">boxes overlap &#8594; run the exact test</text>
<text x="570" y="240" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">may still be a false positive</text>
</svg>
</figure>

The filter is **conservative**: it never rejects a pair whose geometries actually meet, so the result is exact. It does admit false positives — two boxes can overlap while their geometries do not, as the right-hand pair shows — which is why the exact predicate still runs. The value is entirely in how many pairs it eliminates before that point.

The reduction depends on how well each geometry fills its box. Compact features fill their boxes well and the filter is highly selective; long diagonal features — rivers, roads, flight paths — fill them poorly, and a pair of diagonal boxes overlaps far more often than the geometries do.

## Complete working solution

```sql
-- Both tables carry four DOUBLE columns derived at write time.
-- Trino / Spark SQL compatible.
SELECT t.asset_id, r.region_id
FROM   lakehouse.spatial.telemetry t
JOIN   reference.regions r
  ON   t.bbox_min_x <= r.bbox_max_x        -- the covering test:
 AND   t.bbox_max_x >= r.bbox_min_x        -- four comparisons, no decode
 AND   t.bbox_min_y <= r.bbox_max_y
 AND   t.bbox_max_y >= r.bbox_min_y
 AND   ST_Intersects(                       -- the exact test, on survivors only
         ST_GeomFromBinary(t.geom_wkb),
         ST_GeomFromBinary(r.geom_wkb))
WHERE  t.event_day = DATE '2026-03-11';
```

```python
# Deriving the covering at write time. Spark 3.5 + Sedona.
enriched = (raw
  .selectExpr("*",
      "ST_XMin(ST_GeomFromWKB(geom_wkb)) AS bbox_min_x",
      "ST_YMin(ST_GeomFromWKB(geom_wkb)) AS bbox_min_y",
      "ST_XMax(ST_GeomFromWKB(geom_wkb)) AS bbox_max_x",
      "ST_YMax(ST_GeomFromWKB(geom_wkb)) AS bbox_max_y"))

(enriched
   .sortWithinPartitions("bbox_min_x", "bbox_min_y")
   .writeTo("lakehouse.spatial.telemetry").append())
```

The four columns cost 32 bytes per row before compression, and they compress extremely well on a sorted table because neighbouring rows have near-identical bounds — in practice the overhead is a low single-digit percentage of the geometry column they accelerate.

## Step-by-step walkthrough

1. **Write the overlap test as four separate comparisons.** Some engines will recognise a function-based overlap test and some will not; four plain comparisons on plain columns are understood by every optimiser and are individually pushable into the scan.

2. **Put the covering conditions before the exact predicate.** Optimisers reorder conjunctions using selectivity estimates, and their estimate for a geometry function is frequently a fixed default unrelated to the data. Writing the order explicitly removes the dependence on that guess.

3. **Derive the covering from the stored geometry.** If the geometry is transformed or repaired after the covering is computed, the two disagree and the filter starts rejecting pairs it should keep — which produces silently missing results rather than an error.

4. **Sort on the covering columns.** The same columns that make the join filter cheap also make the file and row-group statistics tight, so the covering pays twice: once at the scan and once at the join.

5. **Keep the columns inside the statistics window.** A covering column with no min/max in the manifest still filters at the join, but contributes nothing to file pruning — which is the larger of the two savings.

## Common errors and fixes

| Symptom | Cause | Fix |
|---|---|---|
| No reduction in candidate pairs | Covering written as a function the optimiser cannot push | Use four plain comparisons on plain columns |
| Results missing rows | Covering derived before a transform or repair | Recompute the covering from the geometry actually stored |
| Filter is unselective | Long diagonal geometries fill their boxes poorly | Add a cell-based covering as well; boxes alone are weak here |
| Join slower after adding the covering | Columns outside the statistics window, so no file pruning | Move them earlier in the schema |
| Comparison signs look wrong | Overlap is not containment | Overlap is `a.min <= b.max AND a.max >= b.min` on each axis |

## Verification

<figure class="diagram">
<svg viewBox="0 0 762 222" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Measuring the covering's selectivity by counting pairs surviving the box test against pairs surviving the exact test, with a healthy ratio and an unhealthy one">
<rect x="0" y="0" width="762" height="222" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Selectivity is the number to measure</text>
<rect x="30" y="58" width="352" height="152" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="206" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#2f6e49">healthy</text>
<text x="206" y="118" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">pairs after box test: 12 M</text>
<text x="206" y="142" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">pairs after exact test: 9 M</text>
<text x="206" y="170" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">false-positive rate 25%</text>
<text x="206" y="194" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">the covering is doing its job</text>
<rect x="398" y="58" width="352" height="152" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="574" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#9a5a17">unhealthy</text>
<text x="574" y="118" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">pairs after box test: 900 M</text>
<text x="574" y="142" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">pairs after exact test: 9 M</text>
<text x="574" y="170" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">false-positive rate 99%</text>
<text x="574" y="194" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">boxes are too loose to help</text>
</svg>
</figure>

```sql
-- Two counts, run once, tell you whether the covering earns its storage.
SELECT
  count_if(t.bbox_min_x <= r.bbox_max_x AND t.bbox_max_x >= r.bbox_min_x
       AND t.bbox_min_y <= r.bbox_max_y AND t.bbox_max_y >= r.bbox_min_y) AS box_pairs,
  count_if(ST_Intersects(ST_GeomFromBinary(t.geom_wkb),
                         ST_GeomFromBinary(r.geom_wkb)))                   AS exact_pairs
FROM lakehouse.spatial.telemetry t
CROSS JOIN reference.regions r
WHERE t.event_day = DATE '2026-03-11';
```

A false-positive rate above about ninety percent means the boxes are not discriminating, which almost always indicates elongated diagonal geometries. The remedy is an additional covering that is not a box — a set of grid cells covering each feature — which discriminates on shape rather than only on extent and reduces the pair count for exactly the geometries where boxes fail.

Run the measurement once per join shape rather than continuously; the selectivity is a property of the data's geometry and changes slowly. Re-measure after any change to the layers involved, and record the number alongside the job so a later slowdown can be attributed correctly.

## When a Box Is Not Enough

Bounding boxes fail for a specific and recognisable class of geometry, and knowing the signature saves a lot of fruitless tuning.

<figure class="diagram">
<svg viewBox="0 0 722 264" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Two geometries where the bounding box is a poor proxy: a long diagonal route whose box covers a large empty area, and a sparse multipolygon whose box spans the gaps between its parts">
<rect x="0" y="0" width="722" height="264" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Two shapes that defeat a box covering</text>
<rect x="70" y="70" width="260" height="130" fill="#f2e8da" fill-opacity="0.6" stroke="#9a5a17" stroke-width="2" stroke-dasharray="5 4"/>
<path d="M78 194 L322 78" fill="none" stroke="#0e6e7d" stroke-width="3"/>
<text x="200" y="226" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">a diagonal route</text>
<text x="200" y="248" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">box area &#8776; 50× the corridor it occupies</text>
<rect x="450" y="70" width="260" height="130" fill="#f2e8da" fill-opacity="0.6" stroke="#9a5a17" stroke-width="2" stroke-dasharray="5 4"/>
<circle cx="480" cy="100" r="14" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<circle cx="560" cy="170" r="11" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<circle cx="676" cy="96" r="16" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="580" y="226" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">a scattered multipolygon</text>
<text x="580" y="248" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">box spans the gaps between the parts</text>
</svg>
</figure>

Both shapes have the same problem: the box is a poor description of where the geometry actually is. A route from one corner of a region to another has a box covering the whole region, so it appears to overlap every query window in it. An archipelago's box covers the sea between its islands.

The remedy in both cases is a **cell covering**: precompute the set of grid cells each feature actually touches, store it as an array column, and join on cell overlap rather than on box overlap. A diagonal route touches a narrow chain of cells rather than a rectangular block, and the discrimination improves by roughly the ratio between the box area and the corridor area — which for a long route is a factor of tens.

```sql
-- A cell covering discriminates on shape, not only on extent.
SELECT DISTINCT t.asset_id, r.region_id
FROM   lakehouse.spatial.telemetry t
JOIN   reference.routes r
  ON   arrays_overlap(t.covering_cells, r.covering_cells)
 AND   ST_Intersects(ST_GeomFromBinary(t.geom_wkb),
                     ST_GeomFromBinary(r.geom_wkb));
```

The cost is storage — an array of cell identifiers per feature — and a deduplication step, because a pair sharing several cells matches several times. Both are modest next to the reduction on the shapes where boxes fail, and neither is worth paying on the shapes where boxes work. Deciding per layer, from the measured false-positive rate, is the right granularity.

## Keeping the Covering Correct

A covering is derived data, and derived data that disagrees with its source produces silently wrong results rather than errors.

Three assertions keep it honest. **Coverage**: every geometry must lie inside its own box, checked at write time on every row — this catches the derive-then-transform ordering mistake that is the most common cause of missing results. **Presence**: no null covering columns on a table where they are required, which catches a write path that bypassed the derivation. **Statistics**: min and max present in the file metadata for all four columns, which catches the schema-position mistake that silently disables file pruning.

All three are cheap, and all three belong in the same gate as the geometry validation described in [geometry validation and repair](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/geometry-validation-and-repair/) rather than in a separate check. A batch that satisfies the table's whole contract or is rejected as a unit is far easier to reason about than several partial gates at several stages.

## Cost and Payback

The arithmetic is worth stating because the storage objection comes up every time and the numbers settle it quickly.

Four `DOUBLE` columns are 32 bytes per row uncompressed. On a sorted table they compress to a small fraction of that, because consecutive rows differ only in the low-order bits — measurements on real telemetry tables put the compressed overhead in the region of three to six percent of the geometry column's size.

Against that, the reduction in a typical point-to-region join is between one and three orders of magnitude in the number of exact predicate evaluations. Since the exact predicate is the dominant CPU cost in most spatial joins, and since the covering also enables file-level pruning that the geometry column can never provide, the payback is immediate on any table that is joined more than once.

The case where it does not pay is a table that is only ever written and never joined spatially — a raw landing zone, an archive read only by identifier. There the columns are pure overhead and can reasonably be omitted, provided the decision is recorded so that the first person to attempt a spatial join understands why the table is slow and what to do about it.
