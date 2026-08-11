# Spatial Data Observability

Most spatial lakehouse failures are silent. A partition grows until it becomes a straggler, a clustering order decays until pruning stops working, a feed stops arriving and the last good data keeps answering queries. None of these raises an error, and all of them are visible in numbers that cost almost nothing to collect. This topic covers what to measure, where the measurements come from, and how to turn them into signals somebody will act on.

## Why Spatial Tables Need Their Own Signals

Generic data observability watches row counts, null rates and freshness. Those are necessary and they miss every failure mode specific to geometry, because a spatial table can have perfect row counts, no nulls and current data while being unusable.

<figure class="diagram">
<svg viewBox="0 0 762 288" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Four spatial failure modes invisible to generic data quality checks: clustering decay, partition skew, extent drift and geometry complexity growth, each with the generic check that misses it">
<rect x="0" y="0" width="762" height="288" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Four failures a row-count check cannot see</text>
<rect x="30" y="56" width="352" height="102" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="206" y="84" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">clustering decay</text>
<text x="206" y="108" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">unsorted appends accumulate</text>
<text x="206" y="132" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">rows correct · queries 20× slower</text>
<rect x="398" y="56" width="352" height="102" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="574" y="84" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">partition skew</text>
<text x="574" y="108" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">one dense cell dominates</text>
<text x="574" y="132" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">totals fine · one task never finishes</text>
<rect x="30" y="174" width="352" height="102" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="206" y="202" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">extent drift</text>
<text x="206" y="226" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">a source changes projection</text>
<text x="206" y="250" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">no nulls · joins return nothing</text>
<rect x="398" y="174" width="352" height="102" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="574" y="202" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">complexity growth</text>
<text x="574" y="226" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">vertex counts creep upward</text>
<text x="574" y="250" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">counts stable · costs double</text>
</svg>
</figure>

Each of these has a cheap numeric signal, and in every case the signal is a **trend** rather than a threshold. Clustering quality does not fail; it degrades. Partition skew does not appear; it grows as a city onboards more sensors. The observability system that catches them is one that records a small number of values regularly and shows their direction, not one that alerts when a value crosses a line — because by the time a line is crossed, the degradation has been running for weeks.

## The Six Numbers Worth Collecting

Everything useful can be derived from six per-table measurements, five of which come from metadata alone and cost milliseconds.

**Overlap factor** — the sum of per-file bounding-box areas divided by the table extent's area — measures clustering quality. Close to one is well clustered; close to the file count means the ordering has decayed or was never applied. It is computed from manifest or transaction-log statistics with no data read.

**Partition skew ratio** — the largest partition's byte size divided by the median — measures whether the layout still fits the data's distribution. Under four is healthy; above ten guarantees a straggler in every distributed job.

**File size distribution** — the median and the tenth percentile of file sizes — catches small-file accumulation before planning latency does. A tenth percentile under 32 MB means compaction is falling behind.

**Snapshot count and manifest bytes** measure metadata growth, which determines planning latency. Both grow monotonically without expiry and are the reason a small table can plan slowly.

**Extent** — the table's overall bounding box, and the per-partition extents — catches coordinate-system drift and expansion into unexpected regions. A table whose extent suddenly includes a second continent has either onboarded a customer or lost its projection.

**Geometry complexity** — the median and maximum vertex counts — is the only one that needs a data read, and only a sampled one. It explains cost growth that no other number accounts for.

```sql
-- Iceberg 1.4+. Five of the six, from metadata only, for one table.
WITH f AS (
  SELECT file_size_in_bytes AS bytes, record_count,
         lower_bounds['bbox_min_x'] AS minx, upper_bounds['bbox_max_x'] AS maxx,
         lower_bounds['bbox_min_y'] AS miny, upper_bounds['bbox_max_y'] AS maxy,
         partition
  FROM lakehouse.spatial.telemetry.files
),
per_partition AS (
  SELECT partition, sum(bytes) AS partition_bytes FROM f GROUP BY partition
)
SELECT
  sum((maxx - minx) * (maxy - miny))
    / ((max(maxx) - min(minx)) * (max(maxy) - min(miny)))          AS overlap_factor,
  max(partition_bytes) / approx_percentile(partition_bytes, 0.5)   AS skew_ratio,
  approx_percentile(bytes, 0.5)                                    AS median_file_bytes,
  approx_percentile(bytes, 0.1)                                    AS p10_file_bytes,
  min(minx) AS extent_min_x, max(maxx) AS extent_max_x,
  min(miny) AS extent_min_y, max(maxy) AS extent_max_y
FROM f CROSS JOIN per_partition;
```

## Turning Numbers Into Signals

Collecting the values is the easy half. Making them actionable requires deciding, per metric, whether a deviation is an alert, a ticket or a note.

<figure class="diagram">
<svg viewBox="0 0 764 244" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three response tiers mapped to the six metrics: page for freshness stalls and extent jumps, ticket for skew and clustering decay, and dashboard only for file size and complexity trends">
<rect x="0" y="0" width="764" height="244" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Match the response to how fast it matters</text>
<rect x="26" y="56" width="230" height="176" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="141" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#9a5a17">page someone</text>
<text x="141" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">no new data past the SLA</text>
<text x="141" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">extent jumps to a new region</text>
<text x="141" y="164" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">coordinates outside CRS range</text>
<text x="141" y="196" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">wrong answers are being served</text>
<rect x="274" y="56" width="230" height="176" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="389" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0e6e7d">raise a ticket</text>
<text x="389" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">skew ratio above 10</text>
<text x="389" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">overlap factor doubled</text>
<text x="389" y="164" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">p10 file size under 32 MB</text>
<text x="389" y="196" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">costs rising, answers correct</text>
<rect x="522" y="56" width="230" height="176" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="637" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#2f6e49">dashboard only</text>
<text x="637" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">snapshot count trend</text>
<text x="637" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">vertex count distribution</text>
<text x="637" y="164" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">storage growth rate</text>
<text x="637" y="196" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">reviewed monthly, not hourly</text>
</svg>
</figure>

The left column is small on purpose. A spatial platform that pages on clustering decay will be ignored within a month, because clustering decay is never urgent and always recoverable. Reserving the page for cases where **incorrect answers are currently being served** keeps the signal meaningful: a stalled feed and a coordinate-system change both mean queries running right now are returning something misleading, and both justify waking somebody.

The middle column is where most of the value sits and where most platforms have nothing. A ticket created automatically when the skew ratio crosses ten, carrying the table name and the offending partition, is a small, actionable piece of work that somebody can pick up in a normal week — and doing so prevents the incident the left column would otherwise eventually page about.

## Where the Measurements Come From

<figure class="diagram">
<svg viewBox="0 0 764 234" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three sources for spatial observability data: table metadata for layout metrics, the ingest job for quality counters, and query history for access patterns and pruning ratios">
<rect x="0" y="0" width="764" height="234" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Three sources, none of which requires a scan</text>
<rect x="26" y="58" width="230" height="164" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="141" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">table metadata</text>
<text x="141" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">manifests, transaction log</text>
<text x="141" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">overlap, skew, file sizes</text>
<text x="141" y="166" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">extent, snapshot count</text>
<text x="141" y="196" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">milliseconds per table</text>
<rect x="274" y="58" width="230" height="164" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="389" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">the ingest job</text>
<text x="389" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">emitted as job metrics</text>
<text x="389" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">validity, repair, quarantine</text>
<text x="389" y="166" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">rows in, rows out, latency</text>
<text x="389" y="196" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">free — it already computed them</text>
<rect x="522" y="58" width="230" height="164" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="637" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">query history</text>
<text x="637" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">engine event listeners</text>
<text x="637" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">files scanned vs total</text>
<text x="637" y="166" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">queried extents, principals</text>
<text x="637" y="196" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#6a3d9a">also the audit trail</text>
</svg>
</figure>

The middle source is the most under-used because the numbers already exist inside the ingest job and are simply thrown away. A pipeline that validates geometry already knows how many rows were clean, snapped, repaired and quarantined; emitting those four counters costs one line and produces the quality trend that no external check can reconstruct.

The right-hand source doubles as the access audit described in [security boundaries for GIS data](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/security-boundaries-for-gis-data/), which is a strong argument for building it once and using it for both purposes. The pruning ratio it yields — files scanned against files available, per query shape — is the single best indicator that a layout has stopped working, because it measures the outcome rather than a proxy for it.

## Making the Signal Legible

A metrics table is only useful if somebody reads it, and the format decides whether they do.

Publish per table, per day, one row: the six numbers plus the ingest counters. That makes the history queryable with ordinary SQL, comparable across tables, and cheap to retain for years. Resist the temptation to store per-file detail; the detail is recoverable from the metadata on demand and storing it turns a small useful table into a large ignored one.

Rank rather than list. A platform with three hundred spatial tables produces three hundred rows a day, and nobody reads three hundred rows. A view that surfaces the ten tables whose overlap factor grew most this month, or whose skew ratio is worst weighted by data volume, converts the same data into a short list somebody will actually work through.

Attach an owner. A metric on an unowned table generates no action regardless of how alarming it is. Where the platform has a table-ownership registry, join it in; where it does not, the observability work will surface the absence quickly, which is itself a useful finding.

Finally, keep the collection job boring. It reads metadata, writes one row per table, and does nothing else. A collector that also tries to fix what it finds becomes a system with its own failure modes, and the first rule of observability infrastructure is that it must be more reliable than the thing it observes.

The individual measurements — partition skew, freshness, and query-extent auditing — are worked through in the guides below, each with a runnable collector and the thresholds that make its output actionable.

## Baselines, and Why Thresholds Come Second

The instinct when building observability is to write thresholds first. For spatial tables that produces a system full of arbitrary numbers, because the healthy value of every metric here depends on the table.

An overlap factor of three is excellent for a table of overlapping coverage footprints and terrible for a table of disjoint administrative boundaries. A skew ratio of six is unavoidable for a dataset partitioned by country and a serious defect for one partitioned by an equal-area grid. A median file size of 40 MB is fine for a table written hourly and a symptom for one written daily.

The workable sequence is therefore to **collect for a month before alerting on anything**. During that month the numbers accumulate, their normal range becomes visible, and the thresholds write themselves — a metric that has sat between 1.2 and 1.6 for four weeks has an obvious alert point, and no amount of prior reasoning would have produced it as reliably.

Store the baseline explicitly rather than inferring it each time. A small table of per-table, per-metric expected ranges, reviewed quarterly, makes the alerting logic trivial and makes a threshold change a reviewable diff rather than a configuration edit nobody sees. It also documents the intent: a range recorded with a one-line reason is a range the next person can evaluate rather than merely inherit.

The one exception, where a threshold is knowable in advance, is any metric measuring a **hard constraint** rather than a preference. Coordinates outside the declared coordinate system's valid range are wrong at any value; a partition holding zero rows after a load that reported success is wrong; a file with no statistics on its bounding-box columns is misconfigured regardless of the table. Those can be asserted from day one, and they are exactly the checks that belong in the paging tier.

## Cost of the Observability Layer Itself

A reasonable objection is that monitoring several hundred tables sounds expensive. The measured cost is small enough to settle the objection quickly.

Five of the six metrics are metadata reads. For a table with ten thousand files, reading and aggregating the manifest statistics takes on the order of a second and transfers a few megabytes. Across three hundred tables that is a few minutes of a single process, once a day, with no cluster involved — the sort of workload a small scheduled job handles comfortably.

The sixth metric, geometry complexity, needs a data read and is the one to sample. Reading a thousand rows from a handful of files per table gives a distribution good enough to spot a trend, at a cost of seconds rather than minutes, and there is no benefit to exactness in a number whose purpose is to show direction.

The query-history collection is effectively free where the engine already records it, and where it does not, an event listener adds a small constant overhead per query. It is worth measuring once rather than assuming: a listener that performs a synchronous write per query will show up in latency, and one that batches will not.

Retention is where costs can quietly grow. One row per table per day is negligible; per-file detail per day is not. Keep the daily table narrow, retain it for years, and reconstruct detail from metadata on demand — the metadata is still there, and reconstructing yesterday's file distribution is a query rather than a restore.

## Related Practice

Observability closes the loop that the rest of this section opens. The layout decisions in [spatial partitioning and indexing strategies](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/) are hypotheses about physical behaviour; the metrics here are how you learn whether they still hold. The maintenance jobs in [lakehouse maintenance automation](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/lakehouse-maintenance-automation/) are the remedies; the metrics decide when to run them and against which partitions. And the ingest counters described above come directly from the validation gate in [geometry validation and repair](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/geometry-validation-and-repair/), which is why building the two together costs less than building either alone.

## A Worked Collector

The whole layer fits in one scheduled job of a few hundred lines. The shape below is deliberately minimal: it reads, it writes one row, and it does nothing clever.

```python
# PyIceberg 0.7+. One row per table per run; no data is scanned.
from datetime import date
from pyiceberg.catalog import load_catalog

def collect(catalog_name: str, identifier: str, run_date: date) -> dict:
    table = load_catalog(catalog_name).load_table(identifier)
    files = list(table.scan().plan_files())

    sizes, boxes, per_partition = [], [], {}
    for task in files:
        f = task.file
        sizes.append(f.file_size_in_bytes)
        lo, hi = f.lower_bounds, f.upper_bounds
        boxes.append((lo.get("bbox_min_x"), lo.get("bbox_min_y"),
                      hi.get("bbox_max_x"), hi.get("bbox_max_y")))
        key = str(f.partition)
        per_partition[key] = per_partition.get(key, 0) + f.file_size_in_bytes

    boxes = [b for b in boxes if None not in b]
    box_area = sum((b[2] - b[0]) * (b[3] - b[1]) for b in boxes)
    extent = (min(b[0] for b in boxes), min(b[1] for b in boxes),
              max(b[2] for b in boxes), max(b[3] for b in boxes))
    extent_area = (extent[2] - extent[0]) * (extent[3] - extent[1])

    part_sizes = sorted(per_partition.values())
    median_part = part_sizes[len(part_sizes) // 2] if part_sizes else 0
    sizes.sort()

    return {
        "run_date": run_date,
        "table": identifier,
        "overlap_factor": box_area / extent_area if extent_area else None,
        "skew_ratio": (max(part_sizes) / median_part) if median_part else None,
        "median_file_bytes": sizes[len(sizes) // 2] if sizes else 0,
        "p10_file_bytes": sizes[len(sizes) // 10] if len(sizes) >= 10 else None,
        "file_count": len(sizes),
        "snapshot_count": len(list(table.history())),
        "extent_min_x": extent[0], "extent_min_y": extent[1],
        "extent_max_x": extent[2], "extent_max_y": extent[3],
    }
```

Run it over the catalog's spatial tables, append the results to a metrics table partitioned by date, and build the ranking views on top. Everything else in this topic — the baselines, the thresholds, the tiered responses — is a query against that one table, which is the property that keeps the whole layer maintainable.

## Anti-Patterns Worth Avoiding

Four arrangements that look like observability and are not, each of which has cost a team a quarter somewhere.

**Sampling data to compute layout metrics.** Overlap factor, skew and file sizes are properties of the metadata, and computing them by scanning rows is thousands of times more expensive for a less accurate answer. Any collector that needs a cluster to run has been built wrong.

**Alerting on absolute values.** "Alert when the table exceeds ten thousand files" is meaningless across a fleet where tables differ by three orders of magnitude in size. Alert on ratios and on deviations from a table's own baseline, or the alerts will be tuned to the largest table and silent for every other one.

**One dashboard per table.** Three hundred dashboards is zero dashboards. Build one ranked view across the fleet and let a table's own history be a drill-down rather than a landing page.

**Monitoring that writes to the tables it monitors.** A collector that appends its findings to the same table it measured perturbs the measurement and, worse, creates a dependency loop in which a broken table cannot report that it is broken. Keep the metrics table separate, on a different schedule, ideally in a different namespace.

The unifying principle is that the observability layer should be simpler than the system it watches. When it acquires its own scaling problem, its own tuning parameters and its own on-call rotation, it has stopped being infrastructure and become another workload — and the first thing that gets switched off in an incident is the thing nobody trusts.

## Getting Started Without Boiling the Ocean

For a platform with no spatial observability today, the order that produces value fastest is narrow and specific.

Start with **freshness on the tables people complain about**. It is the metric with the clearest ownership, the least ambiguity and the most immediate payoff, and it requires nothing but the newest snapshot's timestamp.

Add **pruning ratio for three representative queries** next. It is the single number that predicts cost, it requires no new infrastructure where query history already exists, and its trend is what justifies every maintenance job the platform will later schedule.

Then add the **metadata sweep** across every table, unfiltered, collecting all five layout metrics. This is the step that surfaces the tables nobody was watching, which are consistently the ones that will cause the next incident.

Only then build the alerting, using the month of baseline the sweep has by that point accumulated. Teams that reverse this order — thresholds first, collection second — spend the first month tuning alerts against numbers they have no intuition for, and usually end up switching most of them off. The intuition comes from the data, and it takes a month to acquire.
