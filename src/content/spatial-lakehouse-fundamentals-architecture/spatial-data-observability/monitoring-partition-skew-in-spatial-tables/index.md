# Monitoring Partition Skew in Spatial Tables

This guide builds a metadata-only skew monitor for spatial tables: it computes the size distribution across partitions, identifies the cells that dominate, and produces an actionable list of which partitions to split — without reading a single row of data.

## Context and prerequisites

Spatial data clusters, so a geometrically regular partition scheme produces wildly irregular partitions. The consequence is a straggler task in every distributed job and a query whose latency is set by one dense cell. This recipe uses PyIceberg 0.7+ against an Iceberg table, and the same values are available from Delta's transaction log; the layout background is in [spatial partitioning schemes](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/spatial-partitioning-schemes/), and the wider metric set in [spatial data observability](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/spatial-data-observability/).

## What skew looks like, and what it costs

<figure class="diagram">
<svg viewBox="0 0 742 268" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Task duration profile of a distributed job over a skewed spatial table, showing most tasks completing quickly while three tasks over dense cells run for many times longer and set the job duration">
<rect x="0" y="0" width="742" height="268" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Job duration is set by the slowest task, not the average</text>
<line x1="70" y1="220" x2="730" y2="220" stroke="#33707d" stroke-width="1.5"/>
<line x1="70" y1="56" x2="70" y2="220" stroke="#33707d" stroke-width="1.5"/>
<text x="54" y="62" text-anchor="end" font-family="sans-serif" font-size="11" fill="#33707d">12 m</text>
<text x="54" y="220" text-anchor="end" font-family="sans-serif" font-size="11" fill="#33707d">0</text>
<rect x="86" y="196" width="18" height="24" fill="#2f6e49"/>
<rect x="112" y="200" width="18" height="20" fill="#2f6e49"/>
<rect x="138" y="194" width="18" height="26" fill="#2f6e49"/>
<rect x="164" y="198" width="18" height="22" fill="#2f6e49"/>
<rect x="190" y="192" width="18" height="28" fill="#2f6e49"/>
<rect x="216" y="199" width="18" height="21" fill="#2f6e49"/>
<rect x="242" y="195" width="18" height="25" fill="#2f6e49"/>
<rect x="268" y="197" width="18" height="23" fill="#2f6e49"/>
<rect x="294" y="193" width="18" height="27" fill="#2f6e49"/>
<rect x="320" y="198" width="18" height="22" fill="#2f6e49"/>
<rect x="346" y="196" width="18" height="24" fill="#2f6e49"/>
<rect x="372" y="194" width="18" height="26" fill="#2f6e49"/>
<rect x="398" y="199" width="18" height="21" fill="#2f6e49"/>
<rect x="424" y="195" width="18" height="25" fill="#2f6e49"/>
<rect x="450" y="197" width="18" height="23" fill="#2f6e49"/>
<rect x="476" y="60" width="18" height="160" fill="#9a5a17"/>
<rect x="502" y="96" width="18" height="124" fill="#9a5a17"/>
<rect x="528" y="130" width="18" height="90" fill="#9a5a17"/>
<text x="512" y="252" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">three dense cells</text>
<text x="270" y="252" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">everything else finishes in under a minute</text>
</svg>
</figure>

The cost is not proportional to the skew — it is set entirely by the tail. A job whose median task takes 40 seconds and whose slowest takes 12 minutes runs for 12 minutes, and adding executors changes nothing because the bottleneck is one task that cannot be divided. Every additional worker sits idle waiting for it.

That is why skew is worth monitoring separately from total size: a table can double in volume with no change in job duration, and can stay the same size while its duration triples because one city grew.

## Complete working solution

```python
from statistics import median
from pyiceberg.catalog import load_catalog

SKEW_TICKET_THRESHOLD = 10.0     # raise a ticket above this ratio
SKEW_WARN_THRESHOLD   = 4.0      # note it on the dashboard above this

def partition_profile(catalog_name: str, identifier: str) -> dict:
    """Per-partition bytes and rows, from manifests only. No data is read."""
    table = load_catalog(catalog_name).load_table(identifier)

    by_partition: dict[str, dict] = {}
    for task in table.scan().plan_files():
        f = task.file
        key = str(f.partition)
        entry = by_partition.setdefault(key, {"bytes": 0, "rows": 0, "files": 0})
        entry["bytes"] += f.file_size_in_bytes
        entry["rows"]  += f.record_count
        entry["files"] += 1

    sizes = sorted(e["bytes"] for e in by_partition.values())
    if not sizes:
        return {"table": identifier, "partitions": 0}

    med = median(sizes)
    ratio = (sizes[-1] / med) if med else float("inf")

    offenders = sorted(
        ((k, v) for k, v in by_partition.items() if med and v["bytes"] > med * SKEW_WARN_THRESHOLD),
        key=lambda kv: kv[1]["bytes"], reverse=True)

    return {
        "table": identifier,
        "partitions": len(by_partition),
        "median_bytes": med,
        "max_bytes": sizes[-1],
        "p99_bytes": sizes[int(len(sizes) * 0.99)],
        "skew_ratio": ratio,
        "verdict": ("ticket" if ratio > SKEW_TICKET_THRESHOLD
                    else "watch" if ratio > SKEW_WARN_THRESHOLD else "ok"),
        "offenders": [
            {"partition": k, "bytes": v["bytes"], "rows": v["rows"],
             "files": v["files"], "times_median": v["bytes"] / med}
            for k, v in offenders[:20]
        ],
    }
```

## Step-by-step walkthrough

1. **Aggregate by partition, not by file.** A partition with two hundred small files and one with two large files may hold the same data; the skew that matters is in total bytes per partition, because that is what one task will process.

2. **Use the median, not the mean.** A handful of enormous partitions drags the mean up and hides the skew — comparing the maximum against the mean of a skewed distribution understates the problem by design. The median is stable against exactly the outliers being measured.

3. **Report rows as well as bytes.** They diverge on spatial tables: a partition dense in complex polygons can be large in bytes and modest in rows, and one dense in points the reverse. Which matters depends on the workload, so record both and let the consumer decide.

4. **Return the offenders, not just the ratio.** A ratio tells you there is a problem; a list of the twenty worst partitions with their sizes tells somebody what to do about it. This is the difference between a metric and a work queue.

5. **Cap the offender list.** A pathologically skewed table can have thousands of partitions above the threshold, and a list that long is unusable. The top twenty by size covers the actionable set in practice.

## Common errors and fixes

| Symptom | Cause | Fix |
|---|---|---|
| Ratio is enormous on a small table | One partition, or very few | Ignore skew below a minimum partition count — it is not meaningful |
| Ratio looks fine but jobs still straggle | Skew is inside a partition, not across | Check file sizes within the largest partitions too |
| Offenders change every run | Time-partitioned table, current day incomplete | Exclude the open partition from the comparison |
| Metric misses a known-bad partition | Compared against mean instead of median | Use the median; the mean is pulled up by the outliers |
| Collector is slow on huge tables | Planning every file to read metadata | Scope the scan to recent partitions, or read manifests directly |

## Verification

<figure class="diagram">
<svg viewBox="0 0 762 220" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Skew ratio before and after splitting the three densest cells one resolution level deeper, showing the distribution flattening and the ratio dropping from eighteen to three">
<rect x="0" y="0" width="762" height="220" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">The metric confirms the fix worked</text>
<rect x="30" y="58" width="352" height="150" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="206" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#9a5a17">before</text>
<text x="206" y="118" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">skew_ratio 18.4</text>
<text x="206" y="146" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">median 210 MB · max 3.9 GB</text>
<text x="206" y="174" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">3 partitions above 4× median</text>
<rect x="398" y="58" width="352" height="150" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="574" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#2f6e49">after splitting those three</text>
<text x="574" y="118" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">skew_ratio 3.1</text>
<text x="574" y="146" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">median 195 MB · max 610 MB</text>
<text x="574" y="174" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">0 partitions above 4× median</text>
</svg>
</figure>

Run the profile before and after any layout change and store both. The ratio is the acceptance test for a re-partitioning, and having the before value recorded is what lets you demonstrate that the work achieved something — which matters more than it sounds when the work is a multi-hour rewrite somebody has to approve.

## Turning the output into work

<figure class="diagram">
<svg viewBox="0 0 764 210" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three remedies for a skewed partition, chosen by cause: split dense cells to a finer resolution, salt a small stable hot set, or coarsen the key when the whole distribution is wrong">
<rect x="0" y="0" width="764" height="210" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Three remedies, picked by what the profile shows</text>
<rect x="26" y="58" width="230" height="140" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="141" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">a few dense cells</text>
<text x="141" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">3–20 offenders</text>
<text x="141" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">split them one level deeper</text>
<text x="141" y="166" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">adaptive resolution</text>
<rect x="274" y="58" width="230" height="140" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="389" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">one enormous cell</text>
<text x="389" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">a single stable hot spot</text>
<text x="389" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">salt it with a bucket suffix</text>
<text x="389" y="166" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">readers expand the variants</text>
<rect x="522" y="58" width="230" height="140" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="637" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">most cells too small</text>
<text x="637" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">median well under 128 MB</text>
<text x="637" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">coarsen the key entirely</text>
<text x="637" y="166" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">the resolution is wrong</text>
</svg>
</figure>

The right-hand case is the one the ratio alone will not distinguish, which is why the profile returns the median as well. A table whose median partition is 12 MB and whose largest is 400 MB has a ratio above thirty and does not have a skew problem — it has a resolution problem, and splitting the offenders would make it worse by adding more tiny partitions.

Reading the median first, then the ratio, then the offender list, gives the diagnosis in that order every time, and the three remedies map onto it directly.

## Running It Across a Fleet

A single table's profile is diagnostic; the fleet view is what makes the monitor pay for itself, because it surfaces the tables nobody was watching.

Run the profile for every spatial table in the catalog on a daily schedule, append one row per table to a metrics table, and build a ranking on top. The useful ranking is not by ratio alone — a small table with a ratio of forty matters far less than a large one with a ratio of nine — but by ratio weighted by total bytes, which approximates the amount of compute the skew is wasting.

```sql
-- Daily ranking: which tables are wasting the most compute to skew?
SELECT table_name,
       skew_ratio,
       total_bytes,
       skew_ratio * total_bytes / 1e12 AS waste_score
FROM observability.partition_profile
WHERE run_date = current_date
  AND partitions >= 20            -- ratios below this are not meaningful
  AND skew_ratio > 4
ORDER BY waste_score DESC
LIMIT 10;
```

The partition-count filter matters more than it looks. A table with three partitions always has a high ratio and never has a skew problem worth acting on, and without the filter those tables dominate the ranking and the list is ignored within a fortnight.

Retain the daily rows indefinitely — they are a few hundred bytes each — because the trend is the part that predicts. A table whose ratio has climbed from 3 to 7 over three months will reach 15 by the summer, and scheduling the re-partition now is cheaper than doing it during the incident that eventually follows.

## Detecting Skew That Arrives Suddenly

Gradual skew is the common case and the easy one. The sharp case has a different signature and a different cause.

A ratio that jumps in a single day almost always means a **new data source** was onboarded into an existing table: a customer whose fleet is concentrated in one city, a sensor deployment, a backfill of historical data for one region. The distribution did not drift; a new mode appeared.

The diagnosis is to compare the offender list between two runs rather than the ratio. If yesterday's worst partitions are still the worst and simply got bigger, the growth is organic. If the list has new entries near the top, something new arrived, and the right first question is what — because a re-partition applied to a source that will keep growing at that rate needs a different resolution than one applied to a one-off backfill.

Where the platform records ingest provenance, joining the offender partitions against the source identifiers in those partitions answers the question directly and turns a layout investigation into a two-minute query.

## What This Monitor Deliberately Does Not Do

It does not fix anything, and that is a design decision rather than an omission.

An automated re-partitioner sounds attractive and is a poor idea for spatial tables. The correct remedy depends on which of the three cases the profile shows, the choice of new resolution depends on the query patterns rather than on the data, and the rewrite is expensive enough that it should be scheduled rather than triggered. A monitor that acts would occasionally rewrite a large table at an unhelpful moment for a reason nobody reviewed.

It also does not read data, which bounds its cost and its blast radius. A collector that cannot scan cannot be the cause of an incident, cannot perturb the workload it observes, and can run against every table in the catalog without a capacity conversation — which is precisely what makes running it on everything, rather than on the tables somebody remembered, affordable.
