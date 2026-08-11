# Spatial Aggregation and Tiling

Most of what a spatial lakehouse serves is not raw geometry — it is summaries: counts per cell, densities per region, tiles for a map. Those outputs are computed from the same tables the rest of this site describes, and the difference between computing them well and badly is the difference between a dashboard that loads in a second and one that times out. This topic covers the aggregation patterns, the tiling pipeline, and how to decide what to precompute.

## Aggregation Is a Layout Question First

The instinct when a spatial aggregation is slow is to optimise the query. Almost always the query is fine and the table is not.

<figure class="diagram">
<svg viewBox="0 0 764 264" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="The same count per cell aggregation over three table layouts: no derived cell column requiring a per row computation, a derived cell column allowing a plain group by, and a precomputed summary table requiring no scan at all">
<rect x="0" y="0" width="764" height="264" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">The same question, three layouts, three costs</text>
<rect x="26" y="56" width="230" height="196" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="141" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">no cell column</text>
<text x="141" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">derive the cell per row,</text>
<text x="141" y="136" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">every query</text>
<text x="141" y="164" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">decode WKB for every row</text>
<text x="141" y="192" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">no pruning possible</text>
<text x="141" y="222" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">minutes, on any engine</text>
<rect x="274" y="56" width="230" height="196" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="389" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">derived cell column</text>
<text x="389" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">GROUP BY an integer</text>
<text x="389" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">no geometry touched at all</text>
<text x="389" y="172" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">prunes on partition and bbox</text>
<text x="389" y="200" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">scales with rows read</text>
<text x="389" y="228" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">seconds</text>
<rect x="522" y="56" width="230" height="196" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="637" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">precomputed summary</text>
<text x="637" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">read a small table</text>
<text x="637" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">no scan of the facts</text>
<text x="637" y="172" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">refreshed on a schedule</text>
<text x="637" y="200" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">staleness is the trade</text>
<text x="637" y="228" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">milliseconds</text>
</svg>
</figure>

The step from the left column to the middle is the one that matters and it is free: a cell identifier derived at write time turns a spatial aggregation into an ordinary integer `GROUP BY` that every engine executes well, with no spatial extension involved. The geometry column is not read at all.

The step from the middle to the right is a design decision with a staleness cost, and it is worth taking only for the specific summaries that are queried repeatedly. Precomputing everything produces a maintenance burden and a set of tables nobody can keep in step; precomputing the three that a dashboard hits every page load is a clear win.

## Multi-Resolution Summaries

A single aggregation resolution serves one zoom level. A map or a dashboard that lets the user zoom needs several, and computing them independently wastes most of the work.

<figure class="diagram">
<svg viewBox="0 0 722 268" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="A hierarchical rollup where fine resolution counts are aggregated upward to coarser levels, so each level is computed from the one below rather than from the raw facts">
<defs>
<marker id="sat-roll-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#2f6e49"/></marker>
</defs>
<rect x="0" y="0" width="722" height="268" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Roll up, do not recompute</text>
<rect x="60" y="66" width="220" height="60" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="170" y="92" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">raw facts</text>
<text x="170" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">10⁹ rows</text>
<rect x="330" y="66" width="180" height="60" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="420" y="92" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">counts at r9</text>
<text x="420" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">10⁷ rows</text>
<rect x="560" y="66" width="150" height="60" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="635" y="92" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">r7</text>
<text x="635" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">10⁵ rows</text>
<rect x="330" y="160" width="180" height="60" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="420" y="186" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">r5</text>
<text x="420" y="206" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">10³ rows</text>
<rect x="560" y="160" width="150" height="60" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="635" y="186" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">r3</text>
<text x="635" y="206" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">10² rows</text>
<line x1="280" y1="96" x2="330" y2="96" stroke="#2f6e49" stroke-width="2" marker-end="url(#sat-roll-arrow)"/>
<line x1="510" y1="96" x2="560" y2="96" stroke="#2f6e49" stroke-width="2" marker-end="url(#sat-roll-arrow)"/>
<line x1="635" y1="126" x2="510" y2="180" stroke="#2f6e49" stroke-width="2" marker-end="url(#sat-roll-arrow)"/>
<line x1="510" y1="190" x2="560" y2="190" stroke="#2f6e49" stroke-width="2" marker-end="url(#sat-roll-arrow)"/>
<text x="390" y="252" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">Only the first arrow touches the facts; every other level is a cheap aggregate of an aggregate</text>
</svg>
</figure>

The rollup works because hierarchical grids make a cell's parent computable, so a coarser count is a sum over the finer counts that share a parent. One expensive pass over the facts produces the finest level, and every coarser level is a small aggregation over the level below.

Two aggregate types complicate this and both have standard answers. **Distinct counts** do not sum, so a rollup of distinct users per cell is wrong; the remedy is to store a sketch — HyperLogLog or similar — which does merge, at the cost of an approximation with a stated error bound. **Averages** do not sum either, but they roll up correctly if the sum and the count are stored separately and the average is computed at read time, which is the right shape regardless.

## When to Precompute

<figure class="diagram">
<svg viewBox="0 0 712 246" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="A decision matrix for precomputation based on query frequency and acceptable staleness, showing precompute for frequent tolerant queries, compute live for rare or freshness critical ones">
<rect x="0" y="0" width="712" height="246" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Two questions decide it</text>
<line x1="120" y1="200" x2="700" y2="200" stroke="#33707d" stroke-width="1.5"/>
<line x1="120" y1="56" x2="120" y2="200" stroke="#33707d" stroke-width="1.5"/>
<text x="410" y="230" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">query frequency &#8594;</text>
<text x="104" y="128" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d" transform="rotate(-90 104 128)">staleness tolerated &#8594;</text>
<rect x="140" y="130" width="260" height="60" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="270" y="156" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">compute live</text>
<text x="270" y="176" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">rare and fresh</text>
<rect x="420" y="130" width="260" height="60" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="550" y="156" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">optimise the live query</text>
<text x="550" y="176" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">frequent but must be current</text>
<rect x="140" y="62" width="260" height="60" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="270" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">do not bother</text>
<text x="270" y="108" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">rare and tolerant</text>
<rect x="420" y="62" width="260" height="60" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2.5"/>
<text x="550" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">precompute</text>
<text x="550" y="108" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">frequent and tolerant</text>
</svg>
</figure>

The lower-right quadrant is the hard one: a summary that is queried constantly and must reflect the last few minutes. Two techniques apply. **Incremental refresh** recomputes only the partitions that changed since the last run, which for a time-partitioned fact table is the current day and makes a minute-level refresh affordable. **Union of precomputed and live** serves the historical part from the summary and the current partition from the facts, which gives full freshness at the cost of a slightly more complex query.

Both are more work than a nightly rebuild, and both are worth it only for the handful of summaries that genuinely sit in that quadrant. Establishing which those are — from query history rather than from opinion — is the first step and frequently reveals that fewer summaries need it than everyone assumed.

## Tiles Are Just Another Summary

Vector tiles look like a rendering concern and are structurally identical to the aggregations above: a fixed grid, a per-cell payload, and a rollup relationship between zoom levels.

Treating them that way simplifies the pipeline considerably. The tile grid is a cell system, the tile content is an aggregate over the features intersecting that cell, and lower zoom levels are rollups of higher ones with simplification applied. A tile pipeline built on the same derived cell columns as the rest of the platform reuses the layout, the pruning and the incremental refresh rather than inventing its own.

The differences are two. Tile payloads are **encoded rather than numeric**, so the rollup is a geometric union plus simplification rather than a sum — which is more expensive but has the same shape. And tiles have a **hard latency requirement** at serve time, which pushes them firmly into the precomputed quadrant for anything beyond a small extent.

The practical arrangement for most platforms is to precompute tiles for the zoom levels and extents that are actually browsed — which query logs reveal to be a small fraction of the theoretical tile space — and to generate the remainder on demand with a cache. The guides below work through the aggregation, the tile generation and the summary refresh in detail.

## Choosing the Aggregation Grid

The grid a summary uses is a separate decision from the grid the table is partitioned on, and conflating them produces summaries at the wrong resolution for their consumers.

The **partition grid** is chosen from data volume: the resolution at which partitions land in the target size band. The **aggregation grid** is chosen from what the consumer displays: the resolution at which a cell is roughly a pixel, or a neighbourhood, or whatever unit the analysis reports in. These two numbers coincide only by accident.

For a dashboard rendering a city at a thousand pixels across, a pixel is around fifty metres and cells much finer than that produce more rows than the display can use. For a report summarising by district, the natural unit is the district rather than any grid at all — and forcing a grid on it produces numbers that do not match the administrative figures anyone will compare them against.

That last case is worth stating plainly: **when the consumer thinks in administrative units, aggregate by administrative unit**. A grid summary that approximates districts will differ from the official district figures by a small amount that is impossible to explain and destroys confidence in the whole platform. Use the grid for analysis whose unit is genuinely arbitrary, and use the real boundaries when the audience has one in mind.

Where both are needed — a heatmap by grid and a table by district — compute both from the same facts rather than deriving one from the other. Deriving district totals from grid cells is the areal-interpolation problem, and its error is exactly the discrepancy that will be noticed.

## Keeping Summaries Honest

A precomputed summary is a copy, and copies drift. Three practices keep the drift visible.

**Record the source snapshot.** Every summary row, or at minimum every summary table, should carry the identifier of the fact-table snapshot it was computed from. That makes staleness a comparison of two numbers rather than an investigation, and it makes a stale summary detectable by a scheduled check rather than by a user.

**Reconcile totals on a schedule.** A daily job that recomputes one summary from the facts and compares it against the stored version costs one query and catches the entire class of silent divergence — a failed refresh, a partial write, a changed derivation. Alert on a difference beyond a tolerance rather than on any difference, since late-arriving facts will produce small legitimate discrepancies.

**Expose the refresh time to consumers.** A dashboard showing a figure without saying when it was computed invites the assumption that it is current. Publishing the summary's own timestamp alongside its numbers costs a column and removes an entire category of misunderstanding.

None of these prevents drift; they make it visible, which is the achievable goal. A summary that is occasionally an hour stale and says so is far more useful than one that is usually current and never says anything.

## Serving the Results

A summary table is only half a serving layer, and the other half determines whether the latency the precomputation bought actually reaches the user.

<figure class="diagram">
<svg viewBox="0 0 764 246" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three serving arrangements for precomputed spatial summaries: querying the lakehouse table directly, an embedded single node engine over an extract, and a key value store keyed by cell identifier">
<rect x="0" y="0" width="764" height="246" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Three serving layers, by latency requirement</text>
<rect x="26" y="58" width="230" height="176" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="141" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">query the table</text>
<text x="141" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">hundreds of milliseconds</text>
<text x="141" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">no extra component</text>
<text x="141" y="168" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">always current with the summary</text>
<text x="141" y="200" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">fine for internal dashboards</text>
<rect x="274" y="58" width="230" height="176" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="389" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">embedded engine</text>
<text x="389" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">tens of milliseconds</text>
<text x="389" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">an extract per service instance</text>
<text x="389" y="168" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">refreshed on the summary&#8217;s schedule</text>
<text x="389" y="200" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">the usual answer for public APIs</text>
<rect x="522" y="58" width="230" height="176" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="637" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">key-value store</text>
<text x="637" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">single-digit milliseconds</text>
<text x="637" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">keyed by cell identifier</text>
<text x="637" y="168" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">no ad-hoc queries at all</text>
<text x="637" y="200" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">for tiles and fixed lookups</text>
</svg>
</figure>

The middle arrangement is under-used and fits a great many cases. A precomputed summary is typically small — millions of rows rather than billions — so an extract of it fits comfortably in an embedded engine alongside the serving process, giving single-digit-millisecond queries with no additional infrastructure and full SQL flexibility. Refreshing the extract on the summary's own schedule keeps them in step.

The right-hand arrangement gives up flexibility entirely in exchange for latency, and it is the right choice for tiles and for lookups whose key is known in advance. Attempting to serve ad-hoc analytical questions from it produces a key space nobody can enumerate.

Whichever is chosen, the serving layer should carry the summary's snapshot identifier and refresh timestamp through to the response. A client that can see how fresh its data is can make its own decisions about caching and about whether to warn the user; one that cannot will assume the data is current, and will be wrong at exactly the moment it matters.

## Incremental Refresh in Practice

A nightly full rebuild is simple and becomes untenable as the fact table grows. Incremental refresh is the standard escape, and for spatial summaries it has a convenient property: the cells affected by a batch of new facts are exactly the cells those facts fall in, which is a small set.

The pattern is to compute the delta rather than the total. Aggregate only the new facts, producing a partial count per cell, then merge that into the summary by adding to the existing value. For sums and counts this is exact; for averages it works if the sum and count are stored separately; for distinct counts it works if a mergeable sketch is stored rather than a number.

Two details make it reliable. The delta must be **computed from a bounded, identified set** of facts — a partition, an offset range, a batch identifier — so that a retry recomputes the same delta rather than double-counting. And the merge must be **idempotent with respect to that identifier**, which in practice means recording which deltas have been applied, exactly as the ingest idempotency pattern does.

Where the facts can be corrected retrospectively — a late arrival, a repaired geometry, a deleted record — incremental refresh alone is insufficient, because a subtraction is required and the original contribution may not be recoverable. The pragmatic arrangement is incremental refresh for the common path plus a periodic full rebuild that reconciles everything, run weekly or monthly. The rebuild is expensive and infrequent; the incremental path is cheap and constant, and together they give both freshness and correctness.

Track the divergence between the two. If a weekly rebuild consistently differs from the incrementally-maintained value by more than a small tolerance, something in the incremental path is wrong, and knowing the magnitude tells you how urgently.

## Common Mistakes

Five patterns account for most disappointing spatial aggregation performance, and all five are layout or design issues rather than query-tuning ones.

**Deriving the cell in the query.** Computing a grid cell from geometry inside a `GROUP BY` forces a decode per row and prevents every form of pruning. Derive it at write time, once.

**Aggregating at a resolution nobody displays.** A summary at resolution 11 feeding a dashboard that renders at resolution 7 produces two thousand times more rows than the display uses, all of which are transferred and discarded.

**Rolling up distinct counts as though they sum.** They do not, and the resulting numbers are silently too high. Store a sketch or accept that coarse levels must be recomputed.

**Precomputing everything.** A summary per plausible question produces dozens of tables, each needing a refresh schedule and a reconciliation, most of which are never queried. Precompute what the query log shows is hot.

**Mixing grid and administrative units.** Approximating a district total from grid cells produces a figure that disagrees with the official one by an unexplainable amount. Aggregate by the unit the audience uses.

Each is easy to avoid at design time and awkward to correct once consumers depend on the output — which is the argument for settling the resolution and the unit before building the pipeline rather than after the first complaint.

## Engine Fit for Aggregation Work

Aggregation is the workload where the engine differences described in this section matter least, which is itself a useful finding.

Once a cell identifier is materialised, a spatial aggregation is an integer `GROUP BY` over a pruned scan, and every engine on this site executes that well. DuckDB will aggregate a few hundred million rows on one machine in seconds; Trino will do the same across a cluster with governance; Sedona will handle volumes beyond either. The choice therefore follows the surrounding requirements — concurrency, governance, existing infrastructure — rather than any property of the aggregation itself.

The exception is the **tile generation** path, which is geometry-heavy rather than numeric: clipping features to tile boundaries, simplifying per zoom level and encoding the payload are all real geometric work. That workload does favour a distributed engine at scale, and it is the one part of this topic where the engine choice has a material effect.

The other exception is **incremental refresh at high frequency**, where the fixed startup cost of a cluster engine dominates a job that processes a small delta. A refresh running every two minutes wants an engine that starts instantly, which points at a single-node process reading a pruned file list — the pattern described in [DuckDB geospatial analytics](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/duckdb-geospatial-analytics/) and [PyIceberg spatial workflows](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/pyiceberg-spatial-workflows/).

For everything else, the layout does the work and the engine is a detail — which is the same conclusion the rest of this section reaches, arrived at from a different direction.

## Readiness Checklist

- [ ] A grid cell identifier is derived at write time and stored as `BIGINT`, not computed per query
- [ ] The aggregation resolution is chosen from what the consumer displays, not from the partition resolution
- [ ] Administrative summaries are aggregated by the real boundaries, never approximated from grid cells
- [ ] Multi-resolution summaries are produced by rollup, with only the finest level touching the facts
- [ ] Averages are stored as sum and count; distinct counts are stored as mergeable sketches
- [ ] Precomputation is applied only to summaries the query log shows are hot
- [ ] Every summary row or table carries the source snapshot identifier and a refresh timestamp
- [ ] A scheduled reconciliation compares one summary against a fresh computation and alerts on divergence
- [ ] Incremental refresh deltas are keyed on an identifier so a retry cannot double-count
- [ ] A periodic full rebuild reconciles what incremental refresh cannot, and its divergence is tracked
- [ ] The serving layer passes the summary's freshness through to the client

The guides below implement the three pieces this list assumes: the aggregation itself, the tile pipeline built on the same foundation, and the refresh mechanics that keep a dashboard summary current without rebuilding it.
Read the aggregation guide first: the tile pipeline and the dashboard refresh both assume the derived-column layout it establishes, and neither is affordable without it.
A platform that has the cell columns in place can build all three of these in a week; one that does not will spend that week discovering why every summary is slow.
The layout is the prerequisite; everything else here is arithmetic.
