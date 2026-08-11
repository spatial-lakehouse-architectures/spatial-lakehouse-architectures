# Auditing Spatial Query Extents

This guide captures the geographic extent of every query against a spatial table, stores it as a queryable audit trail, and derives the three signals that distinguish ordinary analysis from bulk extraction — a distinction that table-level access logs cannot make.

## Context and prerequisites

Every query in a governed spatial platform is authorised. That is exactly why table-level logging is insufficient: the risk is not an unauthorised query but an authorised principal reassembling, over hundreds of permitted queries, a picture nobody intended to release. The signal lives in the *extent* of each query rather than in its table. This recipe uses a Trino event listener and the equivalent Spark hook; the policy context is in [security boundaries for GIS data](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/security-boundaries-for-gis-data/), and the collection infrastructure it shares with the rest of the metrics is in [spatial data observability](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/spatial-data-observability/).

## What to capture, and from where

<figure class="diagram">
<svg viewBox="0 0 766 256" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Extent extraction from a completed query: the engine event listener supplies principal, tables and pushed filters, from which a bounding box and cell list are recovered and stored with the query metadata">
<defs>
<marker id="aqe-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#0e6e7d"/></marker>
</defs>
<rect x="0" y="0" width="766" height="256" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">From a completed query to one audit row</text>
<rect x="26" y="66" width="200" height="96" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="126" y="94" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">event listener</text>
<text x="126" y="118" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">principal, tables,</text>
<text x="126" y="136" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">pushed filters, rows out</text>
<rect x="288" y="66" width="200" height="96" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="388" y="94" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">extent recovery</text>
<text x="388" y="118" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">bbox from numeric filters</text>
<text x="388" y="136" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">cells from IN lists</text>
<rect x="550" y="66" width="204" height="96" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="652" y="94" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">audit row</text>
<text x="652" y="118" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">who, when, where,</text>
<text x="652" y="136" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">how much came back</text>
<line x1="226" y1="114" x2="288" y2="114" stroke="#0e6e7d" stroke-width="2" marker-end="url(#aqe-arrow)"/>
<line x1="488" y1="114" x2="550" y2="114" stroke="#0e6e7d" stroke-width="2" marker-end="url(#aqe-arrow)"/>
<text x="390" y="212" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">The extent need only be approximate — its purpose is anomaly detection</text>
<text x="390" y="240" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">A bounding box recovered from the pushed filters is entirely sufficient</text>
</svg>
</figure>

Recovering the extent exactly would mean parsing arbitrary SQL, which is neither necessary nor worth the effort. The pushed filters are already structured, already available in the completed-query event, and already contain the numeric bounding-box predicates that any well-formed spatial query on this platform carries. Where a query carries none — a genuine full scan — that absence is itself the most interesting signal in the dataset.

## Complete working solution

```python
# A completed-query handler. Runs off the engine's event stream, not in the query path.
import re, json
from dataclasses import dataclass, asdict

BBOX_RE = re.compile(
    r"bbox_(min|max)_(x|y)\s*(<=|>=|<|>|=)\s*(-?\d+(?:\.\d+)?)", re.I)
CELL_RE = re.compile(r"h3_r\d+\s+IN\s*\(([^)]*)\)", re.I)

@dataclass
class ExtentAudit:
    query_id: str
    principal: str
    started_at: str
    table: str
    min_x: float | None
    min_y: float | None
    max_x: float | None
    max_y: float | None
    cell_count: int
    rows_returned: int
    bytes_scanned: int
    had_spatial_predicate: bool

def extract_extent(event) -> list[ExtentAudit]:
    filters = " AND ".join(event.pushed_filters or []) or event.query_text
    bounds: dict[str, float] = {}
    for side, axis, _op, value in BBOX_RE.findall(filters):
        key = f"{side.lower()}_{axis.lower()}"
        v = float(value)
        # Keep the widest interpretation; this is an approximation by design.
        if key.startswith("min"):
            bounds[key] = min(bounds.get(key, v), v)
        else:
            bounds[key] = max(bounds.get(key, v), v)

    cells = CELL_RE.findall(filters)
    cell_count = sum(len(c.split(",")) for c in cells)

    rows = []
    for table in event.tables_accessed:
        rows.append(ExtentAudit(
            query_id=event.query_id,
            principal=event.principal,
            started_at=event.started_at.isoformat(),
            table=table,
            min_x=bounds.get("min_x"), min_y=bounds.get("min_y"),
            max_x=bounds.get("max_x"), max_y=bounds.get("max_y"),
            cell_count=cell_count,
            rows_returned=event.rows_returned,
            bytes_scanned=event.bytes_scanned,
            had_spatial_predicate=bool(bounds or cells),
        ))
    return rows
```

Append the rows to an audit table partitioned by date. Everything that follows is a query against it.

## Step-by-step walkthrough

1. **Run off the event stream, not in the query path.** A handler that blocks query completion adds latency to every query and becomes an availability risk. Completed-query events are asynchronous by design; keep the handler that way and make it tolerant of its own failures.

2. **Prefer pushed filters to query text.** The pushed filters are the predicates the engine actually applied, already normalised, and immune to formatting. Falling back to the raw text is a reasonable second choice and will occasionally miss a predicate expressed through a view.

3. **Record the absence of a spatial predicate explicitly.** `had_spatial_predicate = False` is the most valuable single column in the table: it identifies both the accidental full scans that cost money and the deliberate ones that warrant a question.

4. **Store rows returned and bytes scanned.** Extent alone does not distinguish a query that examined a city and returned three rows from one that returned three million. Volume is half of the exfiltration signal.

5. **One row per table per query.** A federated query touching four tables produces four rows, which is what makes per-table review possible without re-parsing anything.

## Common errors and fixes

| Symptom | Cause | Fix |
|---|---|---|
| Almost no extents recovered | Callers query through views that hide the predicate | Capture the expanded plan filters rather than the submitted text |
| Extents are wildly wrong | Regex matched a filter on a different table | Scope the extraction per table using the plan's per-scan filters |
| Audit table grows faster than the data | One row per query on a busy platform | Partition by date and expire after the retention period; keep aggregates longer |
| The handler occasionally throws | Malformed or unusual query text | Catch and record a row with nulls; never let the handler drop the event |
| Everything is flagged as suspicious | Thresholds set before a baseline existed | Collect for a month before enabling any detection |

## The three signals

<figure class="diagram">
<svg viewBox="0 0 764 264" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three derived signals from the extent audit: expanding queried area over time, escalating precision from aggregates to individual features, and coverage ratio approaching the full entitled area">
<rect x="0" y="0" width="764" height="264" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Three signals, none conclusive alone</text>
<rect x="26" y="56" width="230" height="196" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="141" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">extent growth</text>
<text x="141" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">area queried per week</text>
<text x="141" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">rising steadily over a month</text>
<text x="141" y="172" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">benign cause: new territory</text>
<text x="141" y="204" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">worth a question, not a block</text>
<rect x="274" y="56" width="230" height="196" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="389" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">precision escalation</text>
<text x="389" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">rows returned per unit area</text>
<text x="389" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">aggregates &#8594; individual rows</text>
<text x="389" y="172" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">benign cause: an investigation</text>
<text x="389" y="204" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">the clearest single indicator</text>
<rect x="522" y="56" width="230" height="196" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="637" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">coverage ratio</text>
<text x="637" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">entitled area actually queried</text>
<text x="637" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">approaching 1.0 in a period</text>
<text x="637" y="172" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">benign cause: a bulk report</text>
<text x="637" y="204" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">strongest for narrow roles</text>
</svg>
</figure>

The coverage ratio is the most discriminating signal for principals whose role implies a small working area. A field engineer entitled to forty sites who queries all forty in a week is behaving normally; one who queries the entire entitled region at feature-level detail is doing something their job does not require, and that is a question worth asking regardless of the answer.

All three produce false positives during legitimate bulk work, which is why they belong in a review queue with a human rather than in an automatic block. An audit system that blocks will be routed around; one that asks will be answered.

## Verification and retention

```sql
-- The weekly review query: principals whose behaviour changed most.
WITH weekly AS (
  SELECT principal,
         date_trunc('week', from_iso8601_timestamp(started_at)) AS wk,
         sum((max_x - min_x) * (max_y - min_y))              AS area_queried,
         sum(rows_returned)                                   AS rows_out,
         count(*) FILTER (WHERE NOT had_spatial_predicate)    AS unbounded_queries
  FROM audit.spatial_query_extents
  WHERE started_at >= date_add('week', -8, current_date)
  GROUP BY 1, 2
)
SELECT principal, wk, area_queried, rows_out, unbounded_queries,
       area_queried / NULLIF(lag(area_queried) OVER (
         PARTITION BY principal ORDER BY wk), 0) AS area_growth
FROM weekly
ORDER BY area_growth DESC NULLS LAST
LIMIT 20;
```

Retain the raw rows for the period the compliance regime requires and the weekly aggregates indefinitely — the aggregates are tiny and they are what a retrospective question actually needs. A request to explain access patterns from eighteen months ago is answerable from a summary table and not from raw logs that were expired at ninety days, which is the arrangement most platforms end up wishing they had chosen.

## Privacy of the Audit Trail Itself

An extent audit is a record of where people looked, which makes it sensitive in its own right — and it is frequently more sensitive than the data it protects.

<figure class="diagram">
<svg viewBox="0 0 764 210" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three controls on the audit table itself: restricted access separate from the data platform, aggregation before any wider sharing, and a retention limit distinct from the underlying data">
<rect x="0" y="0" width="764" height="210" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">The audit table needs its own governance</text>
<rect x="26" y="58" width="230" height="140" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="141" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">separate access</text>
<text x="141" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">not readable by data users</text>
<text x="141" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">a different namespace</text>
<text x="141" y="166" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">and a different role</text>
<rect x="274" y="58" width="230" height="140" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="389" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">aggregate to share</text>
<text x="389" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">weekly totals, not per query</text>
<text x="389" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">nobody outside review</text>
<text x="389" y="166" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">needs the raw rows</text>
<rect x="522" y="58" width="230" height="140" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="637" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">its own retention</text>
<text x="637" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">raw rows expire early</text>
<text x="637" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">aggregates kept longer</text>
<text x="637" y="166" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">set deliberately, not inherited</text>
</svg>
</figure>

A per-query record of which areas a named individual examined, over months, is a behavioural profile. Storing it in the same namespace as the operational metrics, readable by anyone who can read the platform's own tables, converts a security control into a new exposure. Put it in a separate namespace with a distinct role, and grant that role to the small set of people whose job is review.

The aggregation point matters for the same reason. Weekly totals per principal answer every question the review process actually asks and reveal far less than the raw rows. Publish the aggregates to whoever needs to act on them, and keep the raw rows behind the narrower role for the cases where an aggregate raises a question that only detail can answer.

Retention should be set for the audit table on its own terms rather than inherited from the data platform's default. Raw rows have a short useful life — they answer "what happened last week" — while aggregates answer the retrospective questions that arrive months later. Expiring the raw rows early and keeping the summaries is both cheaper and less exposed than the reverse, which is what an inherited policy usually produces.

## Building It Incrementally

The full system described here is worth having and is not where to start. Three stages, each useful on its own, get there without a project.

**Stage one is the unbounded-query counter.** Record only whether each query carried a spatial predicate, per principal per day. It is one boolean, it requires no extent extraction at all, and it immediately identifies both the accidental full scans that dominate the platform's cost and the handful of principals whose queries are never scoped. Most teams find something actionable in the first week.

**Stage two adds the bounding box.** With extents recorded, the area-growth and coverage-ratio signals become computable, and the audit trail starts answering the questions a regulator would ask. This is also the point at which the table becomes sensitive, so it is the right moment to move it behind its own role rather than a later retrofit.

**Stage three adds the derived signals and the review process.** By then a baseline exists, the thresholds write themselves, and the review queue is short enough that somebody will work through it. Attempting stage three first produces a queue full of false positives against thresholds nobody had evidence for, and the usual outcome is that the queue is abandoned.

The same infrastructure serves the cost conversation as well as the security one, which is worth mentioning to whoever is funding it: the unbounded-query counter from stage one typically identifies more wasted compute than any query-tuning exercise, because a single unscoped nightly job against a large table can outweigh every optimisation applied elsewhere. Building it for governance and getting the cost finding for free is a much easier proposal than either alone.
