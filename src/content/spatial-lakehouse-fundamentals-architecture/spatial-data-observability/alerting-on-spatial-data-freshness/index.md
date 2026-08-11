# Alerting on Spatial Data Freshness

This guide builds a freshness monitor for spatial tables that distinguishes a stalled feed from a quiet one, accounts for the geographic and temporal patterns real spatial sources exhibit, and pages only when queries are currently returning misleading answers.

## Context and prerequisites

Freshness is the one spatial metric that justifies waking somebody, because stale data answers queries confidently and wrongly. It is also the metric most often implemented badly, because a naive "no data in the last hour" check fires constantly on sources that are legitimately quiet at night, in winter, or in one region. This recipe reads table metadata through PyIceberg 0.7+ and needs no data scan; the wider metric set is in [spatial data observability](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/spatial-data-observability/).

## Three different questions, often conflated

<figure class="diagram">
<svg viewBox="0 0 764 266" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three distinct freshness questions: when did the table last receive a commit, how recent is the newest event inside it, and is every region still reporting, each answered from a different source">
<rect x="0" y="0" width="764" height="266" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Three questions that all get called &#8220;freshness&#8221;</text>
<rect x="26" y="58" width="230" height="196" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="141" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">write freshness</text>
<text x="141" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">when did a commit last land?</text>
<text x="141" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">source: snapshot timestamp</text>
<text x="141" y="168" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">cost: metadata only</text>
<text x="141" y="198" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">catches: pipeline stopped</text>
<text x="141" y="228" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">misses: pipeline running, empty</text>
<rect x="274" y="58" width="230" height="196" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="389" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">event freshness</text>
<text x="389" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">how recent is the newest row?</text>
<text x="389" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">source: max of the time column</text>
<text x="389" y="168" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">cost: metadata, if tracked</text>
<text x="389" y="198" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">catches: upstream stalled</text>
<text x="389" y="228" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">the one users actually mean</text>
<rect x="522" y="58" width="230" height="196" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="637" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">coverage freshness</text>
<text x="637" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">is every region reporting?</text>
<text x="637" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">source: per-partition maxima</text>
<text x="637" y="168" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">cost: metadata per partition</text>
<text x="637" y="198" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">catches: one region lost</text>
<text x="637" y="228" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">unique to spatial data</text>
</svg>
</figure>

The third question is the one that only exists for spatial data, and it catches the failure that the other two miss entirely: a feed covering forty regions loses one, and the table keeps receiving commits, keeps having recent events, and silently stops knowing anything about one part of the world. Aggregate freshness looks perfect. Every query scoped to that region returns stale results with no indication.

## Complete working solution

```python
from datetime import datetime, timedelta, timezone
from pyiceberg.catalog import load_catalog

def freshness(catalog_name: str, identifier: str,
              time_column: str = "event_ts",
              region_column: str = "region_code") -> dict:
    table = load_catalog(catalog_name).load_table(identifier)
    now = datetime.now(timezone.utc)

    # 1. Write freshness — newest snapshot commit time.
    snapshots = list(table.history())
    last_commit = datetime.fromtimestamp(
        snapshots[-1].timestamp_ms / 1000, tz=timezone.utc) if snapshots else None

    # 2 & 3. Event and coverage freshness — from per-file upper bounds.
    newest_event, per_region = None, {}
    for task in table.scan().plan_files():
        f = task.file
        hi = f.upper_bounds.get(time_column)
        if hi is None:
            continue
        ts = _decode_timestamp(hi)
        newest_event = max(newest_event or ts, ts)
        region = _partition_value(f.partition, region_column)
        if region is not None:
            per_region[region] = max(per_region.get(region, ts), ts)

    stale_regions = {
        r: (now - t).total_seconds() / 3600
        for r, t in per_region.items()
        if (now - t) > timedelta(hours=6)
    }

    return {
        "table": identifier,
        "write_age_minutes": (now - last_commit).total_seconds() / 60 if last_commit else None,
        "event_age_minutes": (now - newest_event).total_seconds() / 60 if newest_event else None,
        "regions_total": len(per_region),
        "regions_stale": len(stale_regions),
        "stale_regions": dict(sorted(stale_regions.items(),
                                     key=lambda kv: -kv[1])[:20]),
    }
```

## Step-by-step walkthrough

1. **Read the snapshot history for write freshness.** It is exact, requires nothing from the schema, and is the only signal available for a table with no time column. It is also the weakest, because a pipeline that runs successfully and writes zero rows produces a fresh commit and no new data.

2. **Read the time column's upper bounds from file statistics.** This gives event freshness without a scan, provided the column is inside the statistics window — which is another reason the column-ordering discipline described elsewhere matters. Where it is not, fall back to a cheap `SELECT max(event_ts)` scoped to the newest partition.

3. **Group by the region dimension.** Any partition column that carries geographic meaning works: a region code, a grid cell, a tenant. The finer it is, the more precisely a coverage gap is located and the more rows the check produces.

4. **Compare against a per-region expectation, not a global one.** The next section covers why a single threshold cannot work across regions with different reporting rhythms.

5. **Cap the stale-region list.** A source that has failed entirely will report every region as stale, and a list of four hundred is noise. The twenty worst by age convey the same information.

## Common errors and fixes

| Symptom | Cause | Fix |
|---|---|---|
| Alerts fire nightly for quiet regions | One global threshold across differing rhythms | Derive a per-region threshold from that region's own history |
| Event freshness needs a full scan | Time column outside the statistics window | Move it earlier in the schema, or raise the statistics limit |
| Write freshness fine, users report stale data | Pipeline running, producing no rows | Alert on event freshness as well, never on write alone |
| A new region alerts immediately | No history to derive a threshold from | Exempt regions younger than the baseline window |
| Freshness jumps backwards | Late-arriving data with older timestamps | Track both maximum event time and commit time; a gap between them is the lateness |

## Deriving per-region thresholds

<figure class="diagram">
<svg viewBox="0 0 732 268" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Inter arrival gap distributions for three regions with different reporting rhythms, showing that a single global threshold either misses failures in busy regions or fires constantly for quiet ones">
<rect x="0" y="0" width="732" height="268" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">One threshold cannot fit three rhythms</text>
<line x1="80" y1="200" x2="720" y2="200" stroke="#33707d" stroke-width="1.5"/>
<text x="400" y="230" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">normal gap between arrivals &#8594;</text>
<rect x="110" y="150" width="70" height="50" fill="#2f6e49"/>
<text x="145" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">urban: 2 min</text>
<rect x="290" y="120" width="70" height="80" fill="#0e6e7d"/>
<text x="325" y="110" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">regional: 45 min</text>
<rect x="520" y="86" width="70" height="114" fill="#9a5a17"/>
<text x="555" y="76" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">remote: 14 h</text>
<line x1="400" y1="60" x2="400" y2="200" stroke="#6a3d9a" stroke-width="2.5" stroke-dasharray="6 4"/>
<text x="408" y="58" font-family="sans-serif" font-size="11" font-weight="700" fill="#6a3d9a">a single global threshold</text>
<text x="240" y="252" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">misses failures here</text>
<text x="580" y="252" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">fires constantly here</text>
</svg>
</figure>

The workable derivation is percentile-based and needs no domain knowledge: for each region, take the distribution of gaps between arrivals over the last thirty days, and set the alert threshold at some multiple of the 99th percentile. A region that normally goes at most twenty minutes without data alerts at an hour; one that normally goes fourteen hours alerts at two days. Both are correct, and neither required anyone to know what the region is.

Recompute the thresholds weekly and store them, rather than deriving them at alert time. Storing them makes the alert logic trivial, makes a threshold change reviewable, and — importantly — prevents an outage from widening its own threshold, which is what happens when a threshold is derived from a window that includes the outage.

## Verification

```python
# Assert the monitor's behaviour on synthetic histories before trusting it.
def test_quiet_region_does_not_alert():
    history = [t for t in hourly_gaps(days=30, mean_gap_h=14)]
    threshold = percentile(history, 0.99) * 2
    assert threshold > timedelta(hours=24), "quiet region threshold too tight"

def test_busy_region_alerts_promptly():
    history = [t for t in hourly_gaps(days=30, mean_gap_h=0.03)]
    threshold = percentile(history, 0.99) * 2
    assert threshold < timedelta(hours=1), "busy region threshold too loose"

def test_coverage_gap_is_detected():
    per_region = {"A": now - timedelta(minutes=5), "B": now - timedelta(hours=30)}
    stale = [r for r, t in per_region.items() if now - t > thresholds[r]]
    assert stale == ["B"]
```

The second test is the one that matters most and is the one a global-threshold implementation fails: a busy region that stops must be caught in minutes, and it will not be if the threshold was set to accommodate the quietest region on the platform.

Wire the resulting alert to a page rather than a ticket, because unlike every other metric in this topic, stale data means queries running right now are returning something misleading — and unlike the others, the remedy is usually upstream and time-sensitive.

## Seasonality and Other Legitimate Silence

Percentile thresholds handle steady rhythms. Several real spatial sources are not steady, and treating their silence as failure produces alerts nobody can act on.

<figure class="diagram">
<svg viewBox="0 0 762 256" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Four patterns of legitimate silence in spatial feeds: diurnal cycles, weekly business rhythms, seasonal shutdowns, and satellite revisit intervals, each with the handling that avoids false alerts">
<rect x="0" y="0" width="762" height="256" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Silence that is not a failure</text>
<rect x="30" y="56" width="352" height="86" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="206" y="82" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">diurnal</text>
<text x="206" y="106" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">vehicle fleets sleep at night</text>
<text x="206" y="128" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">threshold per hour-of-day</text>
<rect x="398" y="56" width="352" height="86" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="574" y="82" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">weekly</text>
<text x="574" y="106" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">surveys run on weekdays</text>
<text x="574" y="128" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">threshold per day-of-week</text>
<rect x="30" y="158" width="352" height="86" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="206" y="184" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">seasonal</text>
<text x="206" y="208" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">agricultural sensors overwinter</text>
<text x="206" y="230" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">suppress within a declared window</text>
<rect x="398" y="158" width="352" height="86" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="574" y="184" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">revisit-driven</text>
<text x="574" y="208" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">satellite passes every N days</text>
<text x="574" y="230" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">alert on missed passes, not on hours</text>
</svg>
</figure>

The bottom-right case deserves a note because it inverts the usual approach. For a satellite-derived layer, elapsed time is the wrong unit entirely: the correct question is whether the expected number of passes have been ingested, and the expected number is computable from the orbit and the area. A region that should have received three acquisitions this week and received one has a problem, even though the newest data is two days old and would pass any time-based check.

The first two cases are handled by conditioning the percentile on the cycle: compute a separate threshold per hour-of-day, or per day-of-week, from the same thirty-day history. That multiplies the number of stored thresholds by twenty-four or by seven, which is still a small table, and it removes the entire class of "it always alerts at 3 a.m." complaints.

The seasonal case is the only one that genuinely needs human input, because thirty days of history cannot predict an annual cycle. A declared suppression window per source, reviewed yearly, is the honest solution — and recording it explicitly is better than the alternative, which is somebody widening the global threshold until the winter alerts stop and leaving it wide.

## What to Put in the Alert

An alert that says "table X is stale" sends the responder to a dashboard. An alert that carries its context sends them to the cause.

Include the **three ages** — write, event and the worst region — because their pattern identifies the failure class immediately. All three old means the pipeline stopped. Write fresh and event old means the pipeline is running and producing nothing. Write and event fresh with one region old means an upstream source was lost.

Include the **affected regions with their ages**, because that is what determines urgency and who to contact. Three adjacent regions going stale together points at a shared upstream; twenty scattered ones points at something in the pipeline.

Include the **last known good time** and the **expected interval**, so the responder can judge severity without querying anything. "Region DE-BY last reported 31 hours ago; expected every 45 minutes" is a complete problem statement.

Finally, include a **link to the query** that produced the numbers. Alerts age badly, and the first thing anyone does is re-run the check to see whether it is still true; making that a click rather than an archaeology exercise saves several minutes at exactly the moment they are most expensive.

Freshness is the one place on a spatial platform where a page is justified, so the rest of the observability layer should stay firmly in the ticket and dashboard tiers described in [spatial data observability](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/spatial-data-observability/). Keeping the paging surface this narrow is what makes it credible when it fires.
