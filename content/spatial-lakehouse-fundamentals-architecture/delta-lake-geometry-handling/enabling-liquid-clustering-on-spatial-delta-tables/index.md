# Enabling Liquid Clustering on Spatial Delta Tables

This guide converts a spatial Delta table from scheduled Z-order compaction to liquid clustering, choosing the clustering columns correctly for geometry, and verifying that pruning quality stops oscillating between maintenance runs.

## Context and prerequisites

Classic Z-ordering is applied by an `OPTIMIZE` run and decays until the next one, so a continuously-appended spatial table's query latency follows a sawtooth. Liquid clustering makes the ordering a table property maintained incrementally, flattening that curve. This recipe needs Delta 3.1 or later on a runtime that supports clustering; the ordering theory is in [Z-ordering for geospatial queries](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/z-ordering-for-geospatial-queries/), and the Delta-specific layout context in [Delta Lake geometry handling](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/delta-lake-geometry-handling/).

## What changes, and what does not

<figure class="diagram">
<svg viewBox="0 0 742 288" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Pruning quality over time under scheduled Z-ordering, which sawtooths between maintenance runs, compared with liquid clustering which stays close to its best value">
<rect x="0" y="0" width="742" height="288" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">The same table, the same appends, two maintenance models</text>
<line x1="70" y1="220" x2="730" y2="220" stroke="#33707d" stroke-width="1.5"/>
<line x1="70" y1="56" x2="70" y2="220" stroke="#33707d" stroke-width="1.5"/>
<text x="54" y="64" text-anchor="end" font-family="sans-serif" font-size="11" fill="#33707d">good</text>
<text x="54" y="218" text-anchor="end" font-family="sans-serif" font-size="11" fill="#33707d">poor</text>
<path d="M80 68 L210 200 L212 72 L342 202 L344 70 L474 198 L476 74 L606 204 L608 72 L720 160"
      fill="none" stroke="#9a5a17" stroke-width="2.5"/>
<text x="250" y="248" font-family="sans-serif" font-size="11" font-weight="700" fill="#9a5a17">scheduled Z-order: sawtooth between runs</text>
<path d="M80 68 L200 92 L320 86 L440 96 L560 88 L720 94" fill="none" stroke="#2f6e49" stroke-width="2.5"/>
<text x="470" y="76" font-family="sans-serif" font-size="11" font-weight="700" fill="#2f6e49">liquid clustering: incremental, near-flat</text>
<text x="400" y="272" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#3d5a63">Peak quality is similar; the average and the variance are not</text>
</svg>
</figure>

Two things do not change. The **clustering columns are still numeric** — a binary WKB column carries no spatial ordering, so clustering must be on the derived bounding-box columns or a grid cell identifier, exactly as with Z-order. And the **statistics window still applies**: columns outside `dataSkippingNumIndexedCols` have no min/max, and clustering them accomplishes nothing.

What changes is who maintains the order and when. Under liquid clustering the write path places new data with awareness of the existing layout, and incremental clustering rewrites only the files that need it, so the ordering never degrades far before it is repaired.

## Complete working solution

```sql
-- Delta 3.1+. New table: clustering is declared, not applied per OPTIMIZE.
CREATE TABLE lakehouse.spatial.telemetry (
  asset_id     BIGINT,
  event_ts     TIMESTAMP,
  event_day    DATE,
  h3_r5        BIGINT,
  bbox_min_x   DOUBLE, bbox_min_y DOUBLE,
  bbox_max_x   DOUBLE, bbox_max_y DOUBLE,
  geom_wkb     BINARY
) USING DELTA
CLUSTER BY (bbox_min_x, bbox_min_y)
TBLPROPERTIES (
  'delta.dataSkippingNumIndexedCols' = '8',
  'delta.enableDeletionVectors'      = 'true'
);
```

```python
# Converting an existing Z-ordered table, without a rewrite of history.
spark.sql("""
ALTER TABLE lakehouse.spatial.telemetry_legacy
CLUSTER BY (bbox_min_x, bbox_min_y)
""")

# Incremental clustering: no ZORDER clause, no column list, no full rewrite.
spark.sql("OPTIMIZE lakehouse.spatial.telemetry_legacy")

# Optional: bring historical files under the new layout, scoped and scheduled.
for day in recent_days(30):
    spark.sql(f"""
      OPTIMIZE lakehouse.spatial.telemetry_legacy
      WHERE event_day = DATE '{day}'
    """)
```

Existing data is not reorganised by the `ALTER`; the table becomes a mixture of layouts and queries plan across both. That is deliberate and it is what makes the conversion safe — the improvement arrives as data is naturally rewritten, and a backfill of history is optional and schedulable.

## Step-by-step walkthrough

1. **Cluster on derived columns, never on geometry.** `CLUSTER BY (geom_wkb)` is accepted syntactically and orders rows by byte sequence, which has no spatial meaning whatsoever. Use the bounding-box minima, or a grid cell identifier where one exists.

2. **Keep the clustering columns inside the statistics window.** Setting `dataSkippingNumIndexedCols` to a value that comfortably covers them, and placing them early in the schema, is what makes the ordering usable by the planner. Clustering columns without statistics produce a tidy layout nobody can exploit.

3. **Do not also partition on the same dimension.** Partitioning by `h3_r5` and clustering by the bounding box duplicates the spatial dimension across two mechanisms and produces a directory explosion for no gain. Partition on time, cluster on space.

4. **Run `OPTIMIZE` without a `ZORDER` clause.** Under liquid clustering the columns come from the table, and supplying them per run is both unnecessary and, on some runtimes, an error. The invocation becomes stable across every table.

5. **Backfill history deliberately.** The `ALTER` is instant and affects only new writes; bringing older partitions under the new layout is a scoped rewrite that should be scheduled against partitions no longer receiving writes.

## Common errors and fixes

| Symptom | Cause | Fix |
|---|---|---|
| No improvement after enabling | History still under the old layout | Backfill recent partitions; new writes alone take time to dominate |
| `OPTIMIZE` rewrites far more than expected | First run after conversion | Expected once; subsequent runs are incremental |
| Pruning unchanged despite clustering | Clustering columns outside the statistics window | Raise `dataSkippingNumIndexedCols` or move the columns earlier |
| Some readers now fail | Deletion vectors enabled alongside | Check every reader's supported protocol version before enabling |
| Directory count exploded | Partitioned on a fine grid as well | Remove the spatial partition; clustering replaces it |

## Verification

<figure class="diagram">
<svg viewBox="0 0 764 216" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three checks after enabling liquid clustering: overlap factor measured daily rather than after each optimise, files scanned for a representative query, and the variance of that ratio over a fortnight">
<rect x="0" y="0" width="764" height="216" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Measure the variance, not just the best value</text>
<rect x="26" y="58" width="230" height="146" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="141" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">overlap factor daily</text>
<text x="141" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">from the transaction log</text>
<text x="141" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">expect a flat line, not</text>
<text x="141" y="166" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">a sawtooth</text>
<rect x="274" y="58" width="230" height="146" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="389" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">files scanned</text>
<text x="389" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">a fixed representative query</text>
<text x="389" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">ratio against table total</text>
<text x="389" y="166" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">recorded every run</text>
<rect x="522" y="58" width="230" height="146" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="637" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">variance over 14 days</text>
<text x="637" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">the actual benefit</text>
<text x="637" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">a predictable table is</text>
<text x="637" y="166" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">worth more than a fast one</text>
</svg>
</figure>

The third check is the one that justifies the change to whoever approved it. Liquid clustering rarely produces a better peak than a fresh Z-order run — it produces a *predictable* result, which means capacity planning works, dashboards do not mysteriously slow down on Thursdays, and a benchmark taken on any given day is representative rather than a function of when the last maintenance ran.

```sql
-- Track it daily; the shape of the series is the finding.
SELECT date_trunc('day', timestamp) AS d,
       sum((maxx - minx) * (maxy - miny)) /
       ((max(maxx) - min(minx)) * (max(maxy) - min(miny))) AS overlap_factor
FROM delta_file_stats('lakehouse.spatial.telemetry')
GROUP BY 1 ORDER BY 1;
```

## When to stay with Z-ordering

Liquid clustering is not universally better, and two cases favour the older mechanism.

A table that is **written once and read many times** — a quarterly reference release, a static historical archive — gains nothing from incremental maintenance because there is no drift to correct. One Z-order run at load time produces an optimal layout that never degrades, and the simpler mechanism has broader runtime support.

A platform whose **readers cannot all be upgraded** is the second case. Clustering raises the table's protocol requirements, and a reader that does not support them will either refuse the table or, on older connectors, behave incorrectly. Enumerate every engine that touches the table — including the ad-hoc sessions and the reporting tool nobody remembers configuring — before enabling it, because the failure mode of an unsupported reader is not always a clean error.

## Choosing the Clustering Columns

Two columns is the right answer for almost every spatial table, and the choice between the available pairs matters more than it appears.

<figure class="diagram">
<svg viewBox="0 0 764 246" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three candidate clustering key choices for a spatial Delta table: the bounding box minima, a grid cell identifier, and a grid cell plus time, with the query shape each favours">
<rect x="0" y="0" width="764" height="246" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Three candidates, chosen by query shape</text>
<rect x="26" y="58" width="230" height="176" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="141" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">bbox_min_x, bbox_min_y</text>
<text x="141" y="118" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">the general default</text>
<text x="141" y="146" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">favours arbitrary windows</text>
<text x="141" y="170" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">works with numeric predicates</text>
<text x="141" y="202" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">pick this unless there is a reason not to</text>
<rect x="274" y="58" width="230" height="176" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="389" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">h3_r9 (a fine cell)</text>
<text x="389" y="118" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">when callers filter on cells</text>
<text x="389" y="146" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">one column, high cardinality</text>
<text x="389" y="170" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">no interleaving needed</text>
<text x="389" y="202" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">simplest when the convention exists</text>
<rect x="522" y="58" width="230" height="176" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="637" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">h3_r9, event_ts</text>
<text x="637" y="118" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">when both appear together</text>
<text x="637" y="146" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">only if time is not already</text>
<text x="637" y="170" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">the partition column</text>
<text x="637" y="202" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">redundant on a day-partitioned table</text>
</svg>
</figure>

The right-hand option is the one most often chosen by mistake. On a table already partitioned by day, adding time to the clustering key spends a clustering dimension on a discrimination the partition already provides, and the spatial clustering is correspondingly weaker. Time belongs in the clustering key only when it is not in the partition key.

Between the first two, the deciding question is what callers actually write. A platform where every spatial query goes through a helper that supplies bounding-box predicates should cluster on the bounding box; one where callers filter on cell identifiers should cluster on the cell. Clustering on a column nobody filters is the most common way to do this work and see no benefit at all — the layout is correct and the planner has nothing to match it against.

Where both patterns exist, cluster on the bounding box: numeric range predicates are the more general form, and a cell filter can be translated into one, while the reverse translation is not always available.

## Operating It Afterwards

Once enabled, the maintenance schedule changes shape rather than disappearing, and three habits keep it healthy.

**Run `OPTIMIZE` more often and expect it to do less.** Incremental clustering rewrites only the files that drifted, so a run against an active partition is small and fast. Hourly is reasonable on a streaming table where the old model would have run nightly, and the aggregate cost is usually lower because the work is spread rather than batched.

**Scope it to the active partitions.** Historical partitions that no longer receive writes are already clustered and running against them rewrites nothing at best and wastes I/O at worst. A predicate on the day column keeps every run bounded and keeps it clear of the ingest path.

**Keep watching the overlap factor.** Liquid clustering makes the ordering self-maintaining, not self-guaranteeing: an unusually large burst of writes, a failed maintenance run, or a partition receiving out-of-order backfill can all leave a region of the table poorly clustered. The metric described in [spatial data observability](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/spatial-data-observability/) is what tells you, and it costs nothing to collect.

The conversion is also a good moment to review the table's other layout properties, because most of them were set once and never revisited. Statistics column limits, target file size, deletion vectors, retention window and the partition key itself all interact with clustering, and changing one in isolation is how a table ends up with a set of individually-reasonable settings that do not work together.
Reviewing them together, once, at the moment the clustering model changes, is far cheaper than discovering the interaction from a query that has quietly got slower.

A short review checklist covers it: are the clustering columns inside the statistics window, is the partition key still coarse enough, is the target file size still appropriate for the current write volume, and does the retention window still exceed the longest in-flight job? Four questions, ten minutes, and they close the loop on a change that otherwise tends to be evaluated only by whether the `ALTER` succeeded.
