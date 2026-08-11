# Spatial Join Optimization

A spatial join is the operation that decides whether a lakehouse feels fast or unusable. It is also the operation where the gap between a naive and a well-designed implementation is largest — routinely two orders of magnitude, and occasionally the difference between minutes and never finishing. This topic covers the anatomy of a spatial join, the three strategies available, and how to choose between them from measurable properties rather than from habit.

## The Anatomy of a Spatial Join

Every spatial join, in every engine, decomposes into the same four stages. Naming them separately is what turns "the join is slow" into a specific diagnosis.

<figure class="diagram">
<svg viewBox="0 0 732 298" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Four stages of a spatial join: file selection from statistics, candidate pair generation, bounding box refinement, and exact geometry evaluation, with the row counts surviving each stage">
<defs>
<marker id="sjo-anat-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#2f6e49"/></marker>
</defs>
<rect x="0" y="0" width="732" height="298" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Four stages, each narrowing the set the next one sees</text>
<rect x="60" y="58" width="660" height="50" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="390" y="79" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">1. file selection — statistics decide what is read at all</text>
<text x="390" y="98" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">10⁹ rows available &#8594; 10⁷ rows read</text>
<rect x="122" y="120" width="536" height="50" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="390" y="141" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">2. candidate pairs — the join strategy decides how many</text>
<text x="390" y="160" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">10⁷ rows &#8594; 10⁷ candidate pairs, or 10¹² if chosen badly</text>
<rect x="184" y="182" width="412" height="50" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="390" y="203" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">3. bbox refinement — cheap numeric comparison</text>
<text x="390" y="222" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">10⁷ pairs &#8594; 10⁶ survive</text>
<rect x="246" y="244" width="288" height="42" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="390" y="270" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">4. exact predicate — GEOS, on 10⁶ pairs</text>
<line x1="390" y1="108" x2="390" y2="120" stroke="#2f6e49" stroke-width="2" marker-end="url(#sjo-anat-arrow)"/>
<line x1="390" y1="170" x2="390" y2="182" stroke="#2f6e49" stroke-width="2" marker-end="url(#sjo-anat-arrow)"/>
<line x1="390" y1="232" x2="390" y2="244" stroke="#2f6e49" stroke-width="2" marker-end="url(#sjo-anat-arrow)"/>
</svg>
</figure>

Stage two is where the catastrophic outcomes live. The other three narrow by predictable factors; stage two either narrows or explodes, depending entirely on the strategy chosen, and a strategy that produces the product of both sides cannot be recovered from by anything downstream. A join that generates a trillion candidate pairs will not finish regardless of how cheap stage three is.

The diagnostic consequence is an ordering. When a join is slow, measure the candidate pair count first. If it is close to the result size, the strategy is right and the remaining cost is real work; if it is orders of magnitude larger, nothing else is worth investigating.

## Three Strategies and Their Preconditions

<figure class="diagram">
<svg viewBox="0 0 764 264" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three spatial join strategies compared on precondition, network cost and failure mode: broadcast index join, spatially partitioned join, and range join on derived cells">
<rect x="0" y="0" width="764" height="264" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Three strategies, three preconditions</text>
<rect x="26" y="56" width="230" height="196" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="141" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">broadcast index</text>
<text x="141" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">needs: one small side</text>
<text x="141" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">network: small side only</text>
<text x="141" y="164" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">candidates: near-optimal</text>
<text x="141" y="192" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">fails when: side outgrows memory</text>
<text x="141" y="222" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">covers most production joins</text>
<rect x="274" y="56" width="230" height="196" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="389" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">spatial partition</text>
<text x="389" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">needs: both sides partitionable</text>
<text x="389" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">network: one shuffle</text>
<text x="389" y="164" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">candidates: good, with duplicates</text>
<text x="389" y="192" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">fails when: the grid is skewed</text>
<text x="389" y="222" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">the large-versus-large answer</text>
<rect x="522" y="56" width="230" height="196" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="637" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">cell equi-join</text>
<text x="637" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">needs: both sides carry cells</text>
<text x="637" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">network: an ordinary shuffle</text>
<text x="637" y="164" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">candidates: depends on resolution</text>
<text x="637" y="192" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">fails when: features span cells</text>
<text x="637" y="222" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">works in any SQL engine</text>
</svg>
</figure>

The third strategy deserves more attention than it usually gets, because it is the only one that requires nothing special from the engine. If both sides carry a grid cell identifier, the spatial join becomes an ordinary equi-join on an integer column plus an exact predicate — which every SQL engine executes well, with no spatial extension, no hint and no special partitioner. The cost is that features spanning several cells must be duplicated across them, and the result must be deduplicated.

That duplication is the whole trade. For point-to-polygon joins where the points sit in one cell each and the polygons are small, it is negligible. For polygon-to-polygon joins over large features it multiplies both sides, and one of the other two strategies is better.

## Choosing Without Guessing

Three measurements pick the strategy, and all three come from metadata or a cheap aggregate.

**The size of the smaller side, serialised.** Not its row count — a thousand detailed boundaries may be hundreds of megabytes while a million points are tens. If it fits comfortably in executor memory with room for the working set, broadcast wins and nothing else needs considering.

**The cell-span distribution of both sides.** How many grid cells does a typical feature touch at the candidate resolution? A distribution concentrated at one makes the cell equi-join attractive; a long tail makes it expensive.

**The skew of the join key.** If the spatial partitioner's cells are as skewed as the underlying data — which they will be unless adaptive resolution is applied — the partitioned join inherits a straggler, and its advantage over broadcast narrows or disappears.

```sql
-- All three, from a sample. Run once before choosing a strategy.
SELECT
  -- 1. serialised size of the small side
  (SELECT sum(length(geom_wkb)) / 1e6 FROM reference.regions)          AS small_side_mb,
  -- 2. cell-span distribution
  approx_percentile(cardinality(cells), 0.5)                            AS median_cells,
  approx_percentile(cardinality(cells), 0.99)                           AS p99_cells,
  -- 3. skew of the join key on the large side
  max(cnt) / approx_percentile(cnt, 0.5)                                AS key_skew
FROM (
  SELECT h3_covering(geom_wkb, 6) AS cells FROM reference.regions
) r
CROSS JOIN (
  SELECT h3_r6, count(*) AS cnt FROM lakehouse.spatial.telemetry
  TABLESAMPLE BERNOULLI (1) GROUP BY h3_r6
) t;
```

## Where the Duplication Problem Bites

<figure class="diagram">
<svg viewBox="0 0 752 248" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="A polygon overlapping four grid cells must be replicated into each for a cell equi-join, and the resulting duplicate matches must be removed after the exact predicate">
<rect x="0" y="0" width="752" height="248" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Replication in, deduplication out</text>
<rect x="90" y="66" width="90" height="70" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="1.5"/>
<rect x="180" y="66" width="90" height="70" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="1.5"/>
<rect x="90" y="136" width="90" height="70" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="1.5"/>
<rect x="180" y="136" width="90" height="70" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="1.5"/>
<path d="M140 100 L232 96 L238 176 L146 182 Z" fill="#f2e8da" fill-opacity="0.8" stroke="#9a5a17" stroke-width="2.5"/>
<text x="180" y="232" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">one polygon &#8594; 4 rows on the join side</text>
<rect x="360" y="80" width="380" height="52" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="550" y="103" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">equi-join on cell id — cheap and parallel</text>
<text x="550" y="122" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">any SQL engine, no spatial partitioner</text>
<rect x="360" y="148" width="380" height="52" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="550" y="171" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">then: exact predicate, then DISTINCT</text>
<text x="550" y="190" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">a point in two cells of one polygon matches twice</text>
</svg>
</figure>

The deduplication step is mandatory and is the part most often forgotten, producing counts that are subtly too high. A point falling inside a polygon that spans four cells will match through whichever cell the point occupies — one match — but a *polygon-to-polygon* join can match through several cells at once, and the pair appears once per shared cell.

Two implementations work. `DISTINCT` on the identifier pair after the exact predicate is simplest and adds a shuffle. Assigning each pair to a single canonical cell — the lowest cell identifier both features share — and filtering to that avoids the shuffle at the cost of a slightly more complex predicate. On large joins the second is materially cheaper.

Resolution choice controls how bad the problem is. A coarser grid means fewer duplicates and more candidate pairs per cell; a finer grid means the reverse. The optimum is usually the resolution at which the median feature touches one or two cells, which for most polygon layers is two or three levels coarser than the resolution used for partitioning.

## What Good Looks Like

A well-tuned spatial join has three properties, all measurable from the query plan and the job metrics.

**Candidate pairs within an order of magnitude of the result size.** This is the single best indicator that the strategy is right. A join returning ten million rows that evaluated fifteen million candidates is doing almost no wasted work; one that evaluated ten billion is doing nothing else.

**Task durations within a factor of three of each other.** Spatial joins are the workload most prone to skew, and a distribution with a long tail means one partition is doing the work of many. The remedy is in the layout rather than in the join.

**Bytes read close to the bytes the answer depends on.** This is stage one working, and it is set by the table's partitioning and statistics rather than by anything in the join itself — which is why the guidance in [spatial partitioning schemes](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/spatial-partitioning-schemes/) and [predicate pushdown optimization](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/predicate-pushdown-optimization/) is a prerequisite for join tuning rather than an alternative to it.

Record all three for the platform's important joins on a schedule. Join performance degrades for the same reasons layouts do — data growth, distribution shift, an added source — and a join that was well-tuned last quarter is not necessarily well-tuned today. The individual techniques are worked through in the guides below.

## The Predicate Matters as Much as the Strategy

Two joins with identical strategies can differ by an order of magnitude because of how the predicate is written, and the differences are not obvious from reading the SQL.

**A function around the indexed column disables everything.** `ST_Intersects(ST_Buffer(a.geom, 100), b.geom)` cannot use any index or statistic on `a.geom`, because the value being tested is not the stored one. Buffer the *other* side, or precompute the buffered geometry as a column, or express the buffer as an expanded bounding box on the numeric columns — all three preserve the optimisation, and the third is usually fastest.

**Distance predicates need translating.** `ST_Distance(a.geom, b.geom) < 500` computes an exact distance for every candidate pair and can be pushed nowhere. Expressed as a bounding-box expansion plus a distance test on the survivors, the same query reads a small fraction of the data and returns identical results. Some engines rewrite this automatically; none should be relied on to.

**Predicate order affects what the optimiser pushes.** A conjunction beginning with a geometry function frequently stops the connector's pushdown at the first untranslatable expression, so the numeric predicates that follow are applied after the scan rather than during it. Writing them first costs nothing and removes the dependency on the optimiser's reordering.

**Equality on a derived key beats containment where it applies.** Joining points to polygons by shared cell identifier and then refining is faster than joining by containment directly, because the first is an equi-join the engine already knows how to execute in parallel. The exactness is preserved by the refinement step; only the mechanism changed.

The common thread is that the engine can only optimise what it can reason about, and geometry functions are opaque to every optimiser at the stages where the large savings are available. Giving it a numeric equivalent to work with — a bounding box, a cell identifier, an expanded envelope — is the whole technique, and it applies identically across every engine on this site.

## Materialising the Join

For joins that run repeatedly against slowly-changing reference data, the fastest join is the one that does not run.

<figure class="diagram">
<svg viewBox="0 0 762 212" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Materialising a point to region assignment as a column on the fact table at ingest, replacing a repeated join with a lookup, valid while the reference geometry is stable">
<defs>
<marker id="sjo-mat-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#0e6e7d"/></marker>
</defs>
<rect x="0" y="0" width="762" height="212" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Assign once at ingest, not on every query</text>
<rect x="30" y="70" width="200" height="80" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="130" y="100" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">at ingest</text>
<text x="130" y="124" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">assign region_id per row</text>
<rect x="290" y="70" width="200" height="80" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="390" y="100" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">stored as a column</text>
<text x="390" y="124" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">with the boundary version</text>
<rect x="550" y="70" width="200" height="80" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="650" y="100" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">queries group by it</text>
<text x="650" y="124" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">no spatial work at all</text>
<line x1="230" y1="110" x2="290" y2="110" stroke="#0e6e7d" stroke-width="2" marker-end="url(#sjo-mat-arrow)"/>
<line x1="490" y1="110" x2="550" y2="110" stroke="#0e6e7d" stroke-width="2" marker-end="url(#sjo-mat-arrow)"/>
<text x="390" y="196" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">The catch: a boundary revision invalidates every assignment made before it</text>
</svg>
</figure>

The catch is the whole design problem. Storing the boundary version alongside the assignment makes the staleness explicit and makes a targeted recomputation possible — when a revision affects one region, only the rows assigned to it under the old version need reassigning, which is a scoped job rather than a full rebuild.

Where boundaries change quarterly and queries run continuously, this trade is overwhelmingly favourable: the assignment is computed once per row instead of once per query per row, and the recomputation is a scheduled job with a known cost. Where boundaries change daily, or where several boundary sets are queried against the same facts, the materialisation becomes its own maintenance burden and the live join is simpler.

## Join Types Beyond Intersection

Most of this topic assumes a containment or intersection join, which is the common case. Three other join shapes appear regularly and each has its own optimisation.

**Nearest-neighbour joins** — "assign each event to the closest facility" — are structurally different because there is no predicate that bounds the search. The standard technique is an expanding search: start with a small radius, keep the events that matched, expand for the remainder, and repeat. Two or three rounds resolve the overwhelming majority, and the cost is bounded by the radius at which the last round runs rather than by the global distance.

**Within-distance joins** are intersection joins in disguise once the distance is converted to an envelope expansion. Expand one side's bounding box by the radius, join on the expanded boxes, and apply the exact distance test to the survivors. The only subtlety is that a radius in metres must be converted to degrees at the correct latitude, and using a single conversion factor across a continental extent introduces an error that grows toward the poles.

**Aggregating joins** — count events per region — can frequently avoid materialising the join at all. Where both sides carry a common cell identifier, aggregating the fact side by cell first and then joining the much smaller aggregate to the regions is dramatically cheaper than joining and then aggregating. The rewrite is mechanical and the saving is proportional to the number of facts per cell.

The unifying observation is that a spatial join is rarely the operation that must happen; it is one implementation of a question. Asking what the query actually needs — an assignment, a count, a nearest match — often reveals a cheaper formulation, and that reformulation beats every tuning technique in this topic combined.

## A Short Diagnostic Sequence

When a spatial join is slow, work through this in order. Each step is cheap and each rules out a class of cause.

- [ ] Bytes read: does the scan match the data the answer depends on, or is it reading everything?
- [ ] Candidate pairs: is the count within an order of magnitude of the result size?
- [ ] Join operator: is it the broadcast or partitioned form, or a generic join with a filter above it?
- [ ] Exchange count: zero for broadcast, one for partitioned; more means an unintended shuffle
- [ ] Task duration spread: within a factor of three, or is one partition dominating?
- [ ] Predicate form: are the numeric predicates present and unwrapped by functions?
- [ ] Small-side size: has the broadcast side outgrown the threshold since this was last checked?
- [ ] Deduplication: is a `DISTINCT` shuffling more data than the join itself?

The first two identify which half of the problem to work on, and in practice one of them is the answer more than three quarters of the time. The remainder are for the cases where the layout is right and the execution is not.

## Engine Differences That Matter

The strategies are universal; their availability and their names are not, and knowing which engine offers what avoids a fruitless search.

**Sedona on Spark** offers all three. It has a broadcast index join with a hint, a spatial partitioner for the large-versus-large case, and it executes the cell equi-join as ordinary SQL. It is also the only one that will silently fall back to a cartesian shuffle when neither of the first two is arranged, which is why plan verification matters more here than elsewhere. The practical guidance is in [Sedona distributed spatial compute](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/sedona-distributed-spatial-compute/).

**Trino** does not have a spatial partitioner, so large-versus-large joins are executed as a shuffle with a filter and are correspondingly expensive. What it does have is excellent handling of the cell equi-join and reliable pushdown of numeric predicates, which makes the derived-key approach the right default there rather than a fallback. See [Trino spatial SQL federation](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/trino-spatial-sql-federation/).

**DuckDB** has an R-tree index that accelerates the candidate generation within a single process, which covers the broadcast case implicitly — the small side is simply in memory, because everything is. There is no distributed strategy because there is no distribution, and the boundary is the node rather than the algorithm. See [DuckDB geospatial analytics](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/duckdb-geospatial-analytics/).

The portable subset across all three is the cell equi-join plus a bounding-box refinement, which is a strong argument for materialising cell identifiers on any table that will be joined spatially. A layout that supports the portable strategy works everywhere and can still use an engine-specific one where it is available; a layout that assumes a particular engine's partitioner constrains the platform to that engine.

## Readiness Checklist

- [ ] Both sides of every scheduled spatial join carry derived bounding-box columns with statistics enabled
- [ ] A grid cell identifier is materialised on any table that participates in a distributed spatial join
- [ ] The join strategy is chosen from measured small-side size, cell-span distribution and key skew, not by habit
- [ ] The physical plan is checked for the intended join operator, not just for a correct result
- [ ] Exchange count matches the strategy: zero for broadcast, one for spatially partitioned
- [ ] Candidate-pair count is recorded and stays within an order of magnitude of the result size
- [ ] Distance and buffer predicates are expressed as envelope expansions plus an exact refinement
- [ ] Numeric predicates precede geometry functions in every conjunction
- [ ] Cell-based joins deduplicate through a canonical cell rather than a table-wide `DISTINCT`
- [ ] Repeated joins against stable reference data are materialised with the reference version recorded
- [ ] Task duration spread is monitored; a factor above three triggers a layout review rather than a cluster resize

Most of these are one-off decisions that then hold for years; the ones that need periodic attention are the small-side size, the key skew and the task spread, because all three drift with the data rather than with the code.

Set a calendar reminder rather than trusting that somebody will notice: the failure mode of all three is gradual, and gradual degradations are precisely the ones that reach an incident before they reach a review.
A quarterly review of the three, against the recorded history, is enough to keep every join on the platform inside its intended strategy.
Everything else on this page is a decision made once, recorded in the table properties, and inherited by whoever writes the next query.
