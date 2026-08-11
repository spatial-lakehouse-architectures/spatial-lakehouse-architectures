# Configuring Trino Resource Groups for Spatial Workloads

This guide configures resource groups so an unbounded spatial query cannot starve the reporting queue, with limits derived from how spatial workloads actually fail rather than from generic defaults.

## Context and prerequisites

Spatial queries fail differently from scalar ones: a missing bounding-box predicate turns a selective query into a full scan, and a join without a broadcast hint turns into a cartesian product. Both consume far more than the author expected, and without isolation both take the cluster down with them. This recipe applies to Trino 400 or later; the governance argument is in [Trino spatial SQL federation](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/trino-spatial-sql-federation/).

## The workload classes worth separating

<figure class="diagram">
<svg viewBox="0 0 768 264" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Four workload classes with their concurrency limits, memory shares and timeouts: scheduled reporting, interactive dashboards, ad hoc analysis and tile or extract generation">
<rect x="0" y="0" width="768" height="264" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Four classes, four failure profiles</text>
<rect x="26" y="56" width="172" height="196" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="112" y="86" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">scheduled</text>
<text x="112" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">reports, extracts</text>
<text x="112" y="142" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">share: 40%</text>
<text x="112" y="164" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">concurrency: 4</text>
<text x="112" y="186" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">timeout: 2 h</text>
<text x="112" y="218" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">must finish</text>
<rect x="212" y="56" width="172" height="196" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="298" y="86" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">interactive</text>
<text x="298" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">dashboards</text>
<text x="298" y="142" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">share: 35%</text>
<text x="298" y="164" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">concurrency: 30</text>
<text x="298" y="186" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">timeout: 60 s</text>
<text x="298" y="218" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">latency matters</text>
<rect x="398" y="56" width="172" height="196" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="484" y="86" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">ad hoc</text>
<text x="484" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">exploration</text>
<text x="484" y="142" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">share: 15%</text>
<text x="484" y="164" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">concurrency: 6</text>
<text x="484" y="186" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">timeout: 15 min</text>
<text x="484" y="218" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">where accidents happen</text>
<rect x="584" y="56" width="172" height="196" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="670" y="86" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">tiles / extracts</text>
<text x="670" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">bulk generation</text>
<text x="670" y="142" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">share: 10%</text>
<text x="670" y="164" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">concurrency: 2</text>
<text x="670" y="186" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">timeout: 4 h</text>
<text x="670" y="218" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#6a3d9a">long, low priority</text>
</svg>
</figure>

The ad-hoc class deserves its small share and short timeout precisely because it is where people learn the data. A query missing its bounding-box predicate is not misconduct — it is the most natural query to write — and the correct response is that the author waits, rather than everybody waiting.

## Complete working solution

```json
{
  "rootGroups": [
    {
      "name": "global",
      "softMemoryLimit": "90%",
      "hardConcurrencyLimit": 60,
      "maxQueued": 200,
      "subGroups": [
        {
          "name": "scheduled",
          "softMemoryLimit": "40%",
          "hardConcurrencyLimit": 4,
          "maxQueued": 40,
          "schedulingPolicy": "fair",
          "schedulingWeight": 40
        },
        {
          "name": "interactive",
          "softMemoryLimit": "35%",
          "hardConcurrencyLimit": 30,
          "maxQueued": 100,
          "schedulingWeight": 35
        },
        {
          "name": "adhoc",
          "softMemoryLimit": "15%",
          "hardConcurrencyLimit": 6,
          "maxQueued": 20,
          "schedulingWeight": 15
        },
        {
          "name": "bulk",
          "softMemoryLimit": "10%",
          "hardConcurrencyLimit": 2,
          "maxQueued": 10,
          "schedulingWeight": 10
        }
      ]
    }
  ],
  "selectors": [
    { "source": "airflow.*",      "group": "global.scheduled"   },
    { "clientTags": ["dashboard"], "group": "global.interactive" },
    { "source": "tile-builder",   "group": "global.bulk"        },
    { "group": "global.adhoc" }
  ]
}
```

```properties
# Session-level limits per group, set through session properties or a policy.
query.max-execution-time=15m
query.max-memory-per-node=8GB
query.max-scan-physical-bytes=2TB
```

## Step-by-step walkthrough

1. **Make the default group the most restricted.** The last selector catches everything unmatched, and it should be the ad-hoc group rather than a permissive one. A new client that nobody configured lands somewhere safe.

2. **Set `maxQueued` deliberately.** A queue absorbs bursts; an unbounded one absorbs a runaway client until the coordinator is holding thousands of pending queries. Small queues for the interactive group, larger for scheduled work that can wait.

3. **Use a scan-bytes limit, not only a time limit.** This is the spatial-specific control: a query without a spatial predicate is identifiable by how much it reads, and killing it at two terabytes scanned is more targeted than killing it after fifteen minutes.

4. **Separate the bulk class.** Tile generation and extract jobs are long, low-priority and predictable. Giving them their own small share prevents them from competing with interactive work while letting them use idle capacity.

5. **Route by source and client tag, not by user.** Users move between workload classes during a day; the application they are using does not. Tagging at the client is more stable and more accurate than mapping identities.

## Common errors and fixes

| Symptom | Cause | Fix |
|---|---|---|
| Dashboards slow whenever someone explores | Ad-hoc share too large, or no separation | Reduce the ad-hoc share; verify the selectors route correctly |
| Scheduled reports miss their window | Weight too low, or queue too long | Raise the scheduled weight; shorten its queue so failures are visible |
| Queries rejected that should run | Concurrency limit below the real demand | Measure the actual concurrency before setting the limit |
| One query still takes the cluster down | Memory limit soft, not enforced per node | Add `query.max-memory-per-node` and a scan-bytes cap |
| Everything lands in the default group | Selectors do not match the client's source string | Log the observed source values before writing selectors |

## The spatial-specific control

<figure class="diagram">
<svg viewBox="0 0 762 222" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="A scan bytes limit stopping an unbounded spatial query early, contrasted with a time limit that lets it consume the cluster for its full duration first">
<rect x="0" y="0" width="762" height="222" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Bytes scanned is the better trigger than elapsed time</text>
<rect x="30" y="58" width="352" height="152" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="206" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#9a5a17">time limit only</text>
<text x="206" y="118" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">the query runs for 15 minutes</text>
<text x="206" y="146" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">saturating I/O the whole time</text>
<text x="206" y="176" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">then fails, having cost everything</text>
<rect x="398" y="58" width="352" height="152" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="574" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#2f6e49">scan-bytes limit</text>
<text x="574" y="118" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">stopped after 2 TB read</text>
<text x="574" y="146" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">often within a minute</text>
<text x="574" y="176" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">and the message names the cause</text>
</svg>
</figure>

The message matters as much as the limit. "Query exceeded the maximum scan of 2 TB" tells the author their query is reading the whole table, which points directly at the missing predicate. "Query exceeded the maximum execution time" tells them nothing actionable and usually results in a request for a longer timeout.

Set the limit from the platform's own distribution: look at the bytes scanned by the ninety-fifth percentile of legitimate queries and set the cap comfortably above it. A cap that fires on normal work will be raised until it means nothing.

## Verification

```sql
-- After deploying, confirm queries land in the intended groups.
SELECT resource_group_id, count(*) AS queries,
       avg(total_bytes) / 1e9 AS avg_gb,
       approx_percentile(elapsed_time_ms, 0.95) / 1000 AS p95_seconds
FROM system.runtime.queries
WHERE created > current_timestamp - interval '1' day
GROUP BY resource_group_id
ORDER BY queries DESC;
```

Two things to look for. A group receiving far more queries than expected means a selector is matching too broadly — usually the default group, catching a client whose source string differs from what the selector assumed. And a group whose average bytes scanned is far above the others contains the unbounded queries, which is exactly the population worth investigating with the extent audit described in [auditing spatial query extents](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/spatial-data-observability/auditing-spatial-query-extents/).

Review the distribution monthly rather than only after an incident. Workload mixes drift as teams adopt the platform, and a configuration that fitted last quarter's usage will gradually stop fitting without anything failing to draw attention to it.

## Rejecting Rather Than Queueing

Resource groups control how much a query may consume. A complementary control decides whether it should start at all, and for spatial workloads it is unusually effective.

<figure class="diagram">
<svg viewBox="0 0 780 218" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="An admission check rejecting a query whose requested extent is implausibly large before it starts, with a message explaining what to change">
<defs>
<marker id="trg-adm-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#0e6e7d"/></marker>
</defs>
<rect x="0" y="0" width="780" height="218" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Reject early, with a message that says what to change</text>
<rect x="26" y="76" width="200" height="80" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="126" y="106" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">query submitted</text>
<text x="126" y="130" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">extent extracted from filters</text>
<rect x="286" y="76" width="200" height="80" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="386" y="106" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">plausible extent?</text>
<text x="386" y="130" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">and a partition predicate?</text>
<rect x="546" y="46" width="208" height="60" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="650" y="80" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">admit</text>
<rect x="546" y="126" width="208" height="80" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="650" y="154" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">reject with guidance</text>
<text x="650" y="178" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">&#8220;add a bbox or cell filter&#8221;</text>
<line x1="226" y1="116" x2="286" y2="116" stroke="#0e6e7d" stroke-width="2" marker-end="url(#trg-adm-arrow)"/>
<line x1="486" y1="100" x2="546" y2="76" stroke="#0e6e7d" stroke-width="2" marker-end="url(#trg-adm-arrow)"/>
<line x1="486" y1="132" x2="546" y2="160" stroke="#9a5a17" stroke-width="2" marker-end="url(#trg-adm-arrow)"/>
</svg>
</figure>

A query against a table partitioned by day and city-scale cells, carrying neither a day predicate nor a spatial one, is almost certainly a mistake. Rejecting it before it starts costs nothing, and the rejection message can name exactly what is missing — which turns an incident into a two-minute correction by the author.

Implement it as a view constraint where the platform routes through views, or as an access-control rule where the connector supports one. Where neither is available, a monitoring rule that kills such queries shortly after they start is a weaker but workable substitute, and it produces the same educational effect.

Be careful to allow the legitimate exceptions. Some queries genuinely need the whole table — a full recompaction check, a global aggregate, a data audit — and those should be submitted with an explicit override tag that routes them to the bulk group. Making the exception explicit is better than widening the rule until it stops protecting anything.

## Reviewing the Configuration Over Time

Resource-group configuration is one of the settings most likely to be written once and never revisited, and one where drift is most consequential.

Three signals indicate it needs attention. **Queue times rising** in a group means its concurrency or share no longer matches demand. **Rejections rising** in the ad-hoc group means either that people are writing worse queries or that the limit is too tight for legitimate work; the extent audit distinguishes the two. And **one group consistently idle** means its share is available to everyone else and the split could be rebalanced.

Review quarterly against the query-history distribution rather than against intuition. The most common finding is that the interactive group needs more concurrency and less memory than originally allocated, because dashboards issue many small queries rather than few large ones — the opposite of the profile most initial configurations assume.

## Federated Queries Need Their Own Limits

A federated spatial query has a cost the resource group does not see: the bytes it pulls from a remote system, and the load it places on a database somebody else operates.

The scan-bytes limit covers the lakehouse side and says nothing about the remote side, because the connector reports what it received rather than what the source scanned to produce it. A federated query that appears modest in Trino's metrics can be running an unindexed full scan on a transactional database, and the first indication is a complaint from that system's owner.

Two controls help. **Per-catalog connection limits** cap how much concurrency Trino can direct at any one source, which turns an unbounded load into a bounded one and is worth agreeing explicitly with the source's owner rather than choosing unilaterally. And **routing federated queries to their own resource group** with a low concurrency limit means that a burst of them queues rather than arriving simultaneously.

Neither substitutes for the query-design discipline covered in [spatial joins across catalogs with Trino](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/trino-spatial-sql-federation/spatial-joins-across-catalogs-with-trino/) — a federated query that pulls a whole remote table is a design problem rather than a scheduling one. But the limits bound the damage while the design is being fixed, which is the useful property of any admission control.
Agree the limits with the source owner before the first scheduled federated job, not after the first complaint.
A limit somebody agreed to is a limit that stays; one imposed unilaterally becomes an argument during an incident.
The conversation is short and it converts a technical control into a shared expectation.
That shift, from control to expectation, is what makes the limit survive the first time it inconveniences somebody.
