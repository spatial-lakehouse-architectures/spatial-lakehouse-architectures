# Precomputing Spatial Summaries for Dashboards

This guide turns a slow dashboard query into a small precomputed table with an incremental refresh, a reconciliation check, and an explicit freshness signal that consumers can act on.

## Context and prerequisites

Dashboards are the workload where precomputation pays most: the same handful of queries run hundreds of times an hour against data that changes once a minute. This recipe uses Spark SQL against an Iceberg fact table and PyIceberg for the refresh bookkeeping; the decision framework for what to precompute is in [spatial aggregation and tiling](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/spatial-aggregation-and-tiling/), and the aggregation itself in [aggregating points to H3 cells in SQL](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/spatial-aggregation-and-tiling/aggregating-points-to-h3-cells-in-sql/).

## Identifying what to precompute

<figure class="diagram">
<svg viewBox="0 0 742 248" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Query log analysis ranking candidate summaries by total time consumed, showing three queries accounting for most of the dashboard cost and the long tail contributing little">
<rect x="0" y="0" width="742" height="248" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Total time consumed, by query shape, last 30 days</text>
<line x1="70" y1="210" x2="730" y2="210" stroke="#33707d" stroke-width="1.5"/>
<rect x="90" y="62" width="70" height="148" fill="#9a5a17"/>
<text x="125" y="232" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">events/cell</text>
<rect x="180" y="96" width="70" height="114" fill="#9a5a17"/>
<text x="215" y="232" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">density map</text>
<rect x="270" y="136" width="70" height="74" fill="#0e6e7d"/>
<text x="305" y="232" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">by region</text>
<rect x="360" y="188" width="70" height="22" fill="#2f6e49"/>
<rect x="450" y="194" width="70" height="16" fill="#2f6e49"/>
<rect x="540" y="198" width="70" height="12" fill="#2f6e49"/>
<rect x="630" y="202" width="70" height="8" fill="#2f6e49"/>
<text x="530" y="232" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">the long tail — leave it live</text>
<text x="215" y="50" text-anchor="middle" font-family="sans-serif" font-size="11" font-weight="700" fill="#9a5a17">precompute these</text>
</svg>
</figure>

Rank by **total time consumed** rather than by frequency or by individual duration. A query that runs twice a day for four minutes costs more than one that runs a thousand times for a tenth of a second, and only the product identifies where precomputation pays.

The shape of this distribution is remarkably consistent: two or three query shapes account for most of the dashboard cost, and precomputing those alone captures the benefit. The long tail is cheap to serve live and expensive to maintain as summaries, so leaving it alone is the correct decision rather than an incomplete one.

## Complete working solution

```sql
-- The summary table. One row per cell per day per resolution.
CREATE TABLE summary.events_by_cell (
  event_day       DATE,
  resolution      INT,
  cell_id         BIGINT,
  event_count     BIGINT,
  duration_sum_s  DOUBLE,
  asset_sketch    VARBINARY,          -- mergeable distinct-count sketch
  source_snapshot BIGINT,             -- the fact snapshot this was computed from
  refreshed_at    TIMESTAMP
) USING ICEBERG
PARTITIONED BY (event_day);
```

```python
from datetime import datetime, timezone
from pyiceberg.catalog import load_catalog

def refresh_summary(spark, catalog_name: str, day: str) -> dict:
    """Incremental refresh: recompute only the day that changed."""
    catalog = load_catalog(catalog_name)
    facts = catalog.load_table("spatial.telemetry")
    snapshot = facts.current_snapshot().snapshot_id

    spark.sql(f"""
      DELETE FROM summary.events_by_cell WHERE event_day = DATE '{day}'
    """)
    spark.sql(f"""
      INSERT INTO summary.events_by_cell
      WITH r8 AS (
        SELECT DATE '{day}' AS event_day, 8 AS resolution, h3_r8 AS cell_id,
               count(*) AS event_count, sum(duration_s) AS duration_sum_s,
               hll_sketch_agg(asset_id) AS asset_sketch
        FROM lakehouse.spatial.telemetry
        WHERE event_day = DATE '{day}'
        GROUP BY h3_r8
      ),
      r6 AS (
        SELECT event_day, 6 AS resolution, h3_cell_to_parent(cell_id, 6) AS cell_id,
               sum(event_count), sum(duration_sum_s), hll_union_agg(asset_sketch)
        FROM r8 GROUP BY event_day, h3_cell_to_parent(cell_id, 6)
      )
      SELECT *, {snapshot} AS source_snapshot, current_timestamp() AS refreshed_at
      FROM (SELECT * FROM r8 UNION ALL SELECT * FROM r6)
    """)
    return {"day": day, "source_snapshot": snapshot,
            "refreshed_at": datetime.now(timezone.utc).isoformat()}
```

## Step-by-step walkthrough

1. **Partition the summary by day, as the facts are.** That makes the refresh a partition replacement rather than a merge, which is atomic, idempotent and cheap.

2. **Delete and re-insert the day rather than merging.** A day's summary is small, recomputing it costs one pass over one partition, and the replacement removes any possibility of double-counting on a retry.

3. **Record the source snapshot.** This is the freshness signal. A consumer, or a check, can compare it against the fact table's current snapshot and know exactly how stale the summary is — a comparison of two integers rather than a data diff.

4. **Store the sketch, not the distinct count.** It is what allows the coarser resolution to be rolled up in the same statement rather than requiring a second pass over the facts.

5. **Roll up within the insert.** Every resolution is produced by one job from one scan, so the schedule has a single step and the resolutions cannot drift out of step with each other.

## Common errors and fixes

| Symptom | Cause | Fix |
|---|---|---|
| Summary and facts disagree | Late-arriving facts after the refresh | Re-refresh recent days on a rolling window, not only the current one |
| Refresh occasionally double-counts | Insert without the preceding delete on retry | Delete-then-insert inside one transaction, or overwrite the partition |
| Coarse resolutions are stale | Rolled up in a separate job that failed | Produce every resolution in one statement |
| Distinct counts wrong at coarse levels | Counts summed instead of sketches merged | Store and merge sketches |
| Dashboard shows old numbers with no warning | Freshness never exposed | Return `refreshed_at` alongside the data |

## Handling late arrivals

<figure class="diagram">
<svg viewBox="0 0 712 238" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="A rolling refresh window covering the last several days rather than only the current one, so late arriving facts are eventually incorporated into the summary">
<rect x="0" y="0" width="712" height="238" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Refresh a window, not a day</text>
<rect x="70" y="80" width="90" height="50" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<rect x="160" y="80" width="90" height="50" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<rect x="250" y="80" width="90" height="50" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<rect x="340" y="80" width="90" height="50" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2.5"/>
<rect x="430" y="80" width="90" height="50" fill="#f2e8da" stroke="#9a5a17" stroke-width="1.5"/>
<rect x="520" y="80" width="90" height="50" fill="#f2e8da" stroke="#9a5a17" stroke-width="1.5"/>
<rect x="610" y="80" width="90" height="50" fill="#f2e8da" stroke="#9a5a17" stroke-width="1.5"/>
<text x="115" y="150" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">D-6</text>
<text x="385" y="150" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">D-3</text>
<text x="655" y="150" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">today</text>
<rect x="340" y="72" width="360" height="66" fill="none" stroke="#0e6e7d" stroke-width="2.5" stroke-dasharray="6 4"/>
<text x="520" y="192" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0e6e7d">refresh window: the last four days, every run</text>
<text x="390" y="222" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">Sized from the observed late-arrival distribution, not chosen arbitrarily</text>
</svg>
</figure>

The window width should come from measurement: look at the distribution of the gap between event time and ingest time, and choose a width covering the ninety-ninth percentile. For most telemetry that is two or three days; for sources with intermittent connectivity it can be a week.

The cost of a wider window is linear and small — each day's summary is one pass over one partition — so erring wide is cheap insurance. Erring narrow means late facts are permanently absent from the summary while present in the facts, which is exactly the silent divergence the reconciliation check exists to catch.

## Verification

```sql
-- Reconciliation: the summary must agree with a fresh computation.
WITH stored AS (
  SELECT sum(event_count) AS n FROM summary.events_by_cell
  WHERE event_day = DATE '2026-03-08' AND resolution = 8
),
fresh AS (
  SELECT count(*) AS n FROM lakehouse.spatial.telemetry
  WHERE event_day = DATE '2026-03-08'
)
SELECT stored.n, fresh.n, abs(stored.n - fresh.n) AS diff,
       abs(stored.n - fresh.n) / CAST(fresh.n AS DOUBLE) AS rel_diff
FROM stored CROSS JOIN fresh;
```

Run it on a day outside the refresh window, where no further late arrivals are expected, so any difference is a defect rather than a timing artefact. Alert on a relative difference above a small tolerance, and treat a persistent one as a bug in the refresh rather than as noise.

Expose `refreshed_at` and `source_snapshot` through to the dashboard. A user seeing "as of 14:32" makes their own judgement; a user seeing a number with no timestamp assumes it is current, and will be wrong at exactly the moment the refresh has failed.

## Serving Both Fresh and Historical

The hardest case is a dashboard that must show both a long history and the last few minutes. Precomputation handles the first well and the second badly, and the standard resolution is to serve them from different places and union the results.

<figure class="diagram">
<svg viewBox="0 0 762 230" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="A union view serving historical days from the precomputed summary and the current day live from the fact table, so the dashboard gets both depth and freshness">
<defs>
<marker id="psd-union-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#2f6e49"/></marker>
</defs>
<rect x="0" y="0" width="762" height="230" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Depth from the summary, freshness from the facts</text>
<rect x="30" y="62" width="290" height="70" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="175" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">summary: days before today</text>
<text x="175" y="110" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">millions of rows, milliseconds</text>
<rect x="30" y="148" width="290" height="70" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="175" y="174" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">facts: today only</text>
<text x="175" y="196" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">one partition, aggregated live</text>
<rect x="440" y="104" width="310" height="72" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="595" y="132" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">one view, UNION ALL</text>
<text x="595" y="154" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">the dashboard sees a single table</text>
<line x1="320" y1="96" x2="440" y2="128" stroke="#2f6e49" stroke-width="2" marker-end="url(#psd-union-arrow)"/>
<line x1="320" y1="182" x2="440" y2="152" stroke="#2f6e49" stroke-width="2" marker-end="url(#psd-union-arrow)"/>
</svg>
</figure>

```sql
CREATE VIEW summary.events_by_cell_current AS
SELECT event_day, resolution, cell_id, event_count, duration_sum_s
FROM   summary.events_by_cell
WHERE  event_day < current_date
UNION ALL
SELECT current_date, 8, h3_r8, count(*), sum(duration_s)
FROM   lakehouse.spatial.telemetry
WHERE  event_day = current_date
GROUP BY h3_r8;
```

The live half is affordable because it covers one partition of one day, which is a fraction of a percent of the fact table. The historical half is a small table read directly. The dashboard queries one view and neither knows nor cares about the split.

Two cautions. The boundary between the two halves must be **exclusive on one side and inclusive on the other**, or the current day appears twice — a classic off-by-one that produces doubled numbers for exactly one day. And the live half must carry the **same resolutions** as the summary, or a request for a coarse resolution returns historical data with no current contribution, which looks like a sudden drop to zero.

## When Precomputation Is the Wrong Answer

Three situations where the live query is the better choice, despite the query log suggesting otherwise.

**The query shape changes frequently.** A dashboard under active development, where the grouping and the filters shift weekly, will invalidate its summaries faster than they can be maintained. Wait until the shape stabilises.

**The filters are user-supplied and unbounded.** A summary can precompute aggregations over fixed dimensions; it cannot precompute every possible combination of user-selected filters. Where the dashboard offers arbitrary faceting, the live query against a well-laid-out table is the honest answer, and the effort belongs in the layout.

**The fact table is already small enough.** A table of a few hundred million rows with good partitioning and clustering will answer a cell aggregation in a second or two, which is fast enough for a dashboard. Precomputing it adds a pipeline, a refresh schedule and a reconciliation for a saving nobody perceives.

The test worth applying before building any summary: how long does the live query take against the current layout, with cold caches? If the answer is under about two seconds, fix nothing. If it is minutes, check whether the layout is the problem before assuming the query is — a table without derived cell columns will be slow no matter how much is precomputed downstream of it.
That single measurement settles more precomputation arguments than any amount of discussion, and it costs one query.
Record the number alongside the decision, so the next person asking the same question starts from evidence rather than from the same discussion.
Evidence settles it; opinion does not.
A measured baseline is also what makes a later regression visible, which no amount of reasoning provides.
Take it before building anything, and again after, and the value of the work is documented rather than assumed. Two numbers, one decision, no argument.
Measure, decide, record.
