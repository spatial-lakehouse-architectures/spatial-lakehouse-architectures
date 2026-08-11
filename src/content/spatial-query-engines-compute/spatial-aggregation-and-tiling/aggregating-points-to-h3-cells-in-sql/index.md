# Aggregating Points to H3 Cells in SQL

This guide computes counts and statistics per grid cell from a large point table using plain SQL, with the derived-column layout that makes it fast and the rollup that produces every coarser resolution for free.

## Context and prerequisites

Point-to-cell aggregation is the most common spatial query on a lakehouse, and it should not involve geometry at all. This recipe runs on Trino or Spark SQL against a table carrying a materialised cell identifier; the layout it assumes is described in [spatial aggregation and tiling](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/spatial-aggregation-and-tiling/), and the derivation itself in [materializing bbox columns for pushdown](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/predicate-pushdown-optimization/materializing-bbox-columns-for-pushdown/).

## The query that does no spatial work

<figure class="diagram">
<svg viewBox="0 0 768 250" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Execution path of a cell aggregation on a well laid out table: partition pruning, bounding box pruning, an integer group by, with the geometry column never read">
<defs>
<marker id="ahc-path-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#2f6e49"/></marker>
</defs>
<rect x="0" y="0" width="768" height="250" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Four steps, none of which decodes geometry</text>
<rect x="24" y="76" width="164" height="80" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="106" y="106" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">prune partitions</text>
<text x="106" y="130" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">day + cell predicate</text>
<rect x="212" y="76" width="164" height="80" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="294" y="106" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">prune files</text>
<text x="294" y="130" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">bbox statistics</text>
<rect x="400" y="76" width="164" height="80" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="482" y="106" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">read 2 columns</text>
<text x="482" y="130" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">cell id + measure</text>
<rect x="588" y="76" width="168" height="80" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="672" y="106" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">GROUP BY BIGINT</text>
<text x="672" y="130" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">an ordinary aggregation</text>
<line x1="188" y1="116" x2="212" y2="116" stroke="#2f6e49" stroke-width="2" marker-end="url(#ahc-path-arrow)"/>
<line x1="376" y1="116" x2="400" y2="116" stroke="#2f6e49" stroke-width="2" marker-end="url(#ahc-path-arrow)"/>
<line x1="564" y1="116" x2="588" y2="116" stroke="#2f6e49" stroke-width="2" marker-end="url(#ahc-path-arrow)"/>
<text x="390" y="206" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">The geometry column is never opened — columnar storage skips it entirely</text>
<text x="390" y="234" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">Which is why this runs in seconds on data that takes minutes to decode</text>
</svg>
</figure>

## Complete working solution

```sql
-- Trino / Spark SQL. Counts and statistics per cell for one day, one window.
SELECT
    h3_r8                                   AS cell_id,
    count(*)                                AS event_count,
    count(DISTINCT asset_id)                AS distinct_assets,
    sum(duration_s)                         AS total_duration_s,
    sum(duration_s) / count(*)              AS mean_duration_s,
    approx_percentile(speed_kmh, 0.95)      AS p95_speed_kmh
FROM lakehouse.spatial.telemetry
WHERE event_day = DATE '2026-03-11'
  AND bbox_min_x >= 13.0 AND bbox_max_x <= 13.8      -- prunes files
  AND bbox_min_y >= 52.3 AND bbox_max_y <= 52.7
GROUP BY h3_r8;
```

```sql
-- The rollup: every coarser level from the finest, without touching the facts again.
CREATE TABLE summary.telemetry_cells AS
WITH r8 AS (
  SELECT event_day, h3_r8 AS cell_id, 8 AS resolution,
         count(*) AS event_count, sum(duration_s) AS total_duration_s
  FROM lakehouse.spatial.telemetry
  WHERE event_day = DATE '2026-03-11'
  GROUP BY event_day, h3_r8
),
r6 AS (
  SELECT event_day, h3_cell_to_parent(cell_id, 6) AS cell_id, 6 AS resolution,
         sum(event_count) AS event_count, sum(total_duration_s) AS total_duration_s
  FROM r8 GROUP BY event_day, h3_cell_to_parent(cell_id, 6)
),
r4 AS (
  SELECT event_day, h3_cell_to_parent(cell_id, 4) AS cell_id, 4 AS resolution,
         sum(event_count) AS event_count, sum(total_duration_s) AS total_duration_s
  FROM r6 GROUP BY event_day, h3_cell_to_parent(cell_id, 4)
)
SELECT * FROM r8 UNION ALL SELECT * FROM r6 UNION ALL SELECT * FROM r4;
```

## Step-by-step walkthrough

1. **Filter on the bounding-box columns, not on geometry.** The four comparisons prune files before any data is read; an `ST_Intersects` against a window would read everything and filter afterwards.

2. **Group on the integer cell column.** This is what makes the aggregation ordinary. No spatial extension is loaded, no geometry is decoded, and the engine's normal hash aggregation applies.

3. **Store sum and count, not the mean.** The mean is computed at read time from the two, which is what allows a coarser level to be rolled up correctly. Storing the mean makes the rollup impossible without returning to the facts.

4. **Roll up through the parent relation.** Each coarser level aggregates the level above it rather than the raw table, so the expensive pass happens once. The parent function is a bit-manipulation on the identifier and costs nothing.

5. **Keep the resolution as a column.** One summary table holding several resolutions is easier to serve and to refresh than one table per level, and the resolution column makes the query trivial.

## Common errors and fixes

| Symptom | Cause | Fix |
|---|---|---|
| Query takes minutes on a small window | Cell derived in the query rather than stored | Materialise the cell column at write time |
| Coarse-level counts are wrong | Distinct counts rolled up as sums | Store a mergeable sketch, or recompute coarse levels from facts |
| Coarse-level means are wrong | Mean rolled up as an average of averages | Store sum and count; compute the mean at read time |
| No file pruning despite the bbox filter | Bbox columns outside the statistics window | Move them earlier in the schema |
| Cell counts differ from a previous run | Grid library version changed | Pin the library and record the version |

## Distinct counts, done correctly

<figure class="diagram">
<svg viewBox="0 0 762 222" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Distinct counts cannot be summed across cells because an entity may appear in several, so a mergeable sketch is stored instead of a number">
<rect x="0" y="0" width="762" height="222" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Why distinct counts do not roll up</text>
<rect x="30" y="58" width="352" height="152" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="206" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#9a5a17">summing the counts</text>
<text x="206" y="118" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">cell A: 40 distinct vehicles</text>
<text x="206" y="142" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">cell B: 35 distinct vehicles</text>
<text x="206" y="170" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">sum = 75, truth = 52</text>
<text x="206" y="194" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">23 vehicles visited both</text>
<rect x="398" y="58" width="352" height="152" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="574" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#2f6e49">merging sketches</text>
<text x="574" y="118" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">store an HLL sketch per cell</text>
<text x="574" y="142" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">merge, then estimate</text>
<text x="574" y="170" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">result ≈ 52, error ~1%</text>
<text x="574" y="194" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">rolls up at every level</text>
</svg>
</figure>

```sql
-- Store the sketch alongside the count so both roll up.
SELECT event_day, h3_r8 AS cell_id,
       count(*)                              AS event_count,
       approx_set(asset_id)                  AS asset_sketch     -- Trino HLL
FROM lakehouse.spatial.telemetry
WHERE event_day = DATE '2026-03-11'
GROUP BY event_day, h3_r8;

-- Rolling up: merge the sketches, then estimate.
SELECT h3_cell_to_parent(cell_id, 6) AS cell_id,
       sum(event_count)              AS event_count,
       cardinality(merge(asset_sketch)) AS distinct_assets
FROM summary.telemetry_cells_r8
GROUP BY h3_cell_to_parent(cell_id, 6);
```

The approximation is typically within a percent or two, which is entirely adequate for a heatmap and inadequate for a billing figure. Where exactness is required at coarse levels, the honest answer is to recompute those levels from the facts rather than to roll up — the coarse aggregations are cheap precisely because there are few cells, so a second pass is affordable.

## Verification

```sql
-- Rolled-up totals must equal the direct computation, within late-arrival tolerance.
WITH rolled AS (
  SELECT sum(event_count) AS n FROM summary.telemetry_cells
  WHERE resolution = 4 AND event_day = DATE '2026-03-11'
),
direct AS (
  SELECT count(*) AS n FROM lakehouse.spatial.telemetry
  WHERE event_day = DATE '2026-03-11'
)
SELECT rolled.n, direct.n, rolled.n - direct.n AS difference
FROM rolled CROSS JOIN direct;
```

A non-zero difference on a closed day is a defect rather than a rounding artefact: counts sum exactly, so any discrepancy means either a rollup bug or facts that arrived after the summary was computed. Distinguishing the two is a matter of re-running the summary and seeing whether the difference persists.

Run this reconciliation on a schedule for one representative day rather than for every day, and alert on a persistent difference. It costs one query and catches the entire class of silent divergence that makes precomputed summaries untrustworthy.

## Weighted and Time-Bucketed Aggregations

The basic count generalises in two directions that cover most real dashboard requirements.

<figure class="diagram">
<svg viewBox="0 0 762 222" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Two extensions of the cell aggregation: adding a time bucket to the grouping key for temporal analysis, and weighting the measure by a per record value for density surfaces">
<rect x="0" y="0" width="762" height="222" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Two extensions, one grouping key</text>
<rect x="30" y="58" width="352" height="152" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="206" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">space × time</text>
<text x="206" y="118" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">GROUP BY cell, hour</text>
<text x="206" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">rows multiply by the bucket count</text>
<text x="206" y="170" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">choose the coarsest bucket</text>
<text x="206" y="194" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">the consumer actually uses</text>
<rect x="398" y="58" width="352" height="152" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="574" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">weighted density</text>
<text x="574" y="118" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">sum(weight) rather than count(*)</text>
<text x="574" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">rolls up exactly, like a count</text>
<text x="574" y="170" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">divide by cell area for a rate</text>
<text x="574" y="194" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">at read time, not at write time</text>
</svg>
</figure>

The time dimension is the one that needs restraint. Adding an hourly bucket multiplies the summary's row count by twenty-four, and a summary at hourly × resolution 9 over a country is larger than the fact table it summarises. Choose the coarsest bucket the consumer genuinely uses — daily for most dashboards, hourly only where the diurnal pattern is the subject — and add finer buckets as separate, smaller-extent summaries if they are needed.

The weighted case is straightforward and has one trap: dividing by cell area to get a density must happen at read time, because a density does not roll up. Sum the weights, roll those up, and divide by the coarser cell's area at the point of display. Storing the density directly makes every coarser level wrong.

## Cost and Scaling

The aggregation's cost is dominated by rows read rather than by the grouping, which makes it predictable and makes the optimisation obvious.

A `GROUP BY` on an integer over a pruned scan costs roughly what reading those two columns costs. On columnar storage, reading two narrow columns out of a table whose bulk is geometry is a small fraction of the table's bytes — frequently under five percent — so a well-laid-out aggregation reads far less than its row count suggests.

The number that grows uncomfortably is the **cardinality of the grouping key**, because it determines the hash table size. A resolution-11 aggregation over a country produces hundreds of millions of groups, which spills on any single node and shuffles heavily on a cluster. Since a summary at that resolution is almost never displayed, the practical protection is to choose the resolution from the consumer rather than from the data — the same advice this topic gives everywhere, arriving here as a memory constraint rather than as a design preference.

Where a genuinely fine resolution is required over a large extent, the answer is to partition the aggregation by region and run it as several jobs rather than one, which bounds the hash table per job and parallelises cleanly.

## Serving the Result

Once computed, the summary is a small table and the serving question is how a client asks for a region of it.

The natural interface takes a bounding box and a resolution, expands the box into cell identifiers at that resolution, and selects the matching rows. That expansion happens client-side or in a view; either way it turns a spatial request into an `IN` list against an integer column, which is as cheap as a lookup gets.

Two practical points. Cap the number of cells a single request may ask for, because a client that requests a continental extent at a fine resolution will ask for millions of cells and the `IN` list itself becomes the cost. Returning an error that names the limit is far better than attempting it, and it steers the client toward requesting a coarser resolution — which is what they should have asked for at that extent anyway.

And return the cell identifiers rather than geometries. A client that knows the grid system can compute the cell boundaries locally, which removes the geometry from the payload entirely and typically shrinks the response by an order of magnitude. Where the client cannot, offering a geometry-bearing variant as a separate endpoint keeps the fast path fast.

The full serving picture, including the precomputation decision and the freshness signalling, is in [spatial aggregation and tiling](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/spatial-aggregation-and-tiling/).
