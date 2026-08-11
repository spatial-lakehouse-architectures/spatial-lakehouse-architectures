# Streaming Spatial Ingestion

A streaming spatial pipeline is where every constraint in this site meets at once: the write must be fast enough to keep up, the geometry must be validated before it lands, the layout must stay queryable, and the table must not accumulate a million tiny files by Friday. This topic covers the shape that satisfies all four, and the specific ways streaming spatial ingest differs from streaming scalar ingest.

## What Makes Spatial Streaming Different

Streaming a table of integers and streaming a table of geometry are not the same problem, and three differences account for most of the difficulty.

<figure class="diagram">
<svg viewBox="0 0 764 280" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three differences between streaming scalar and spatial data: per record CPU cost from validation and derivation, wide records inflating in flight memory, and the layout requirement that makes arrival order harmful">
<rect x="0" y="0" width="764" height="280" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Three differences that change the design</text>
<rect x="26" y="56" width="230" height="212" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="141" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">per-record CPU</text>
<text x="141" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">decode, validate, reproject,</text>
<text x="141" y="136" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">derive bbox and cell</text>
<text x="141" y="164" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">100× a scalar record&#8217;s cost</text>
<text x="141" y="192" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">the loop is CPU-bound,</text>
<text x="141" y="212" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">not I/O-bound</text>
<text x="141" y="244" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">parallelism must be processes</text>
<rect x="274" y="56" width="230" height="212" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="389" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">wide records</text>
<text x="389" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">a polygon is kilobytes,</text>
<text x="389" y="136" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">not tens of bytes</text>
<text x="389" y="164" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">in-flight memory scales</text>
<text x="389" y="192" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">with bytes, not rows</text>
<text x="389" y="212" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">batch sizing must follow</text>
<text x="389" y="244" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">size batches by bytes</text>
<rect x="522" y="56" width="230" height="212" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="637" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">arrival order is wrong</text>
<text x="637" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">events arrive by time,</text>
<text x="637" y="136" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">queries ask by place</text>
<text x="637" y="164" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">every micro-batch spans</text>
<text x="637" y="192" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">the whole extent</text>
<text x="637" y="212" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">so files never prune well</text>
<text x="637" y="244" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">compaction is mandatory</text>
</svg>
</figure>

The third difference is the structural one. A micro-batch of scalar events is a coherent unit — it covers a minute of time, which is exactly how the table is queried. A micro-batch of spatial events covers a minute of time and the entire service area, so its bounding box is the whole extent and it prunes for nobody. No amount of write-side tuning fixes this; it is inherent to the arrival order, and the remedy is asynchronous compaction that re-sorts within the time partition.

That makes maintenance a first-class part of a streaming spatial design rather than an afterthought. A pipeline without a compaction schedule is not a slow pipeline; it is a pipeline whose table becomes unqueryable at a predictable rate.

## The Shape That Works

<figure class="diagram">
<svg viewBox="0 0 768 262" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="A streaming spatial pipeline: source partitions feed a worker pool that validates and derives, a coordinator batches commits, and an asynchronous compactor re-sorts the open partition on its own schedule">
<defs>
<marker id="ssi-shape-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#0e6e7d"/></marker>
</defs>
<rect x="0" y="0" width="768" height="262" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Four components, each with one job</text>
<rect x="24" y="66" width="150" height="76" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="99" y="96" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">source</text>
<text x="99" y="120" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">partitioned stream</text>
<rect x="206" y="66" width="170" height="76" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="291" y="96" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">worker pool</text>
<text x="291" y="120" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">validate, derive, encode</text>
<rect x="408" y="66" width="170" height="76" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="493" y="96" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">commit coordinator</text>
<text x="493" y="120" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">batched, sequential</text>
<rect x="610" y="66" width="146" height="76" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="683" y="96" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">the table</text>
<text x="683" y="120" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">day-partitioned</text>
<line x1="174" y1="104" x2="206" y2="104" stroke="#0e6e7d" stroke-width="2" marker-end="url(#ssi-shape-arrow)"/>
<line x1="376" y1="104" x2="408" y2="104" stroke="#0e6e7d" stroke-width="2" marker-end="url(#ssi-shape-arrow)"/>
<line x1="578" y1="104" x2="610" y2="104" stroke="#0e6e7d" stroke-width="2" marker-end="url(#ssi-shape-arrow)"/>
<rect x="408" y="180" width="348" height="70" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="582" y="208" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">compactor — separate schedule, separate budget</text>
<text x="582" y="230" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">bin-pack hourly, sort once when the partition closes</text>
<line x1="683" y1="142" x2="683" y2="180" stroke="#2f6e49" stroke-width="2" marker-end="url(#ssi-shape-arrow)"/>
</svg>
</figure>

The commit coordinator is the component teams most often omit, and its absence produces the most confusing symptoms. Without it, every worker commits independently, conflicts appear under load, retries amplify the contention, and the pipeline's throughput collapses at exactly the moment volume rises. With it, workers produce data files in parallel and one process publishes them in batches, so conflicts are structurally impossible and snapshot count stays proportional to commit batches rather than to worker count.

The compactor's separation matters equally. Running compaction inside the ingest loop couples a variable-cost maintenance operation to a latency-sensitive path; running it on its own schedule, against partitions the writer is not currently targeting, keeps both predictable.

## Latency, Throughput and File Size Are One Decision

The three parameters everyone tunes independently are in fact a single trade-off, and seeing it as one avoids a lot of circular tuning.

<figure class="diagram">
<svg viewBox="0 0 732 250" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="The relationship between commit interval, resulting file size and end to end latency, showing that a short interval gives low latency and small files while a long one gives the reverse">
<rect x="0" y="0" width="732" height="250" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">One knob, three consequences</text>
<line x1="80" y1="200" x2="720" y2="200" stroke="#33707d" stroke-width="1.5"/>
<text x="400" y="234" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">commit interval &#8594;</text>
<text x="130" y="222" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">10 s</text>
<text x="400" y="222" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">5 min</text>
<text x="670" y="222" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">1 h</text>
<path d="M110 186 L680 60" fill="none" stroke="#9a5a17" stroke-width="2.5"/>
<text x="560" y="82" font-family="sans-serif" font-size="11" font-weight="700" fill="#9a5a17">file size &#8593;</text>
<path d="M110 60 L680 186" fill="none" stroke="#0e6e7d" stroke-width="2.5"/>
<text x="150" y="56" font-family="sans-serif" font-size="11" font-weight="700" fill="#0e6e7d">freshness &#8593;</text>
<rect x="330" y="52" width="140" height="148" fill="#e6f0ea" fill-opacity="0.5" stroke="#2f6e49" stroke-width="2"/>
<text x="400" y="128" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">workable band</text>
<text x="400" y="150" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">1–5 minutes</text>
</svg>
</figure>

Most spatial streaming workloads land in the one-to-five-minute band, and the reason is that the consumers rarely need better. A dashboard refreshing every thirty seconds against data committed every minute is already at the limit of what anyone notices, and the file sizes at that interval — combined with hourly bin-packing — stay manageable.

Below about thirty seconds the file-count arithmetic turns hostile: a ten-second interval produces 8,640 commits a day per writer, and the compactor spends more effort merging than the ingest spent writing. Above about fifteen minutes, freshness starts to be a product decision rather than an engineering one, and it should be made as such.

The parameter worth deriving rather than choosing is the **batch byte size**, since the record width varies enormously between point telemetry and polygon updates. Measuring the serialised size of a representative batch and targeting a fixed byte budget produces a stable memory profile across sources that a row-count target never will.

## Backpressure and What to Drop

A streaming spatial pipeline will eventually receive more than it can process, and deciding in advance what happens then is the difference between graceful degradation and an outage.

The instinct is to scale out, and it is often right — the transformation stage parallelises perfectly across processes, so throughput scales with cores until the commit path or the source partitioning becomes the limit. Both of those limits are worth knowing in advance rather than discovering during an incident.

When scaling is not fast enough, the options are to buffer, to shed or to degrade. **Buffering** is correct for short bursts and requires the source to retain data — which a log-based stream does and a push-based feed does not. **Shedding** means dropping records, and for spatial telemetry it is more defensible than it sounds: dropping every second position report from a densely-sampled track loses very little information, whereas dropping a boundary update loses a fact. Deciding which streams are sheddable, in advance, is the useful preparation.

**Degrading** is the underused option: continue ingesting but skip the expensive optional work. Geometry validation can be deferred to a later pass, cell derivation can fall back to a coarser resolution, and simplification can be skipped. Each reduces per-record cost substantially and each is recoverable later, provided the records are marked so a backfill knows what to redo.

Whichever policy is chosen, instrument the lag and alert on its trend rather than its value. A lag of two minutes that has been growing for an hour is a more urgent signal than a lag of ten minutes that is stable, and only the trend distinguishes them.

## Exactly-Once, and What It Costs

Every streaming pipeline eventually has the duplicate-records conversation, and for spatial data the answer is usually cheaper than the general case.

The general problem is that a worker can write data files, fail before committing, and be restarted — leaving orphaned files and, if the source position advanced, a gap. Table formats make the first half harmless: uncommitted files are invisible and are reclaimed by orphan cleanup. The second half is the real work, and it is solved by tying the source position to the commit.

The pattern is to **store the source offsets in the commit itself**, as a table property or a metadata column, and to resume from what the table says rather than from what the consumer group says. A restart then replays from the last committed position, and any records written but not committed are simply rewritten. The table is the single source of truth about what has been ingested, which is the property that makes the whole thing reasonable.

Where records may legitimately be replayed — a source that redelivers on restart — deduplication becomes necessary, and spatial data offers a convenient key: the combination of a source record identifier and an event timestamp is usually unique and is cheap to compare. Deduplicating within a commit batch is nearly free; deduplicating against history requires either a merge or a downstream distinct, and the merge is expensive enough on a large table that most pipelines choose to tolerate rare duplicates and remove them in a scheduled pass instead.

The cost worth stating explicitly is that exactly-once semantics constrain the commit to be the transaction boundary, which rules out the "commit per worker" arrangement and requires the coordinator. Teams that want both maximum write parallelism and exactly-once are asking for two things that trade against each other, and the coordinator is the standard resolution.

## Handling Late and Out-of-Order Data

Spatial telemetry arrives late routinely — a vehicle regains connectivity and uploads an hour of buffered positions — and the table's partitioning determines how expensive that is.

<figure class="diagram">
<svg viewBox="0 0 764 222" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Late arriving records landing in a closed partition, forcing either a rewrite of that partition or an append that leaves it unsorted, with a third option of a separate late arrivals table">
<rect x="0" y="0" width="764" height="222" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Three ways to absorb a late batch</text>
<rect x="26" y="58" width="230" height="152" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="141" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">rewrite the partition</text>
<text x="141" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">keeps sort order perfect</text>
<text x="141" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">costs a full partition rewrite</text>
<text x="141" y="170" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">for a handful of rows</text>
<text x="141" y="196" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">rarely worth it</text>
<rect x="274" y="58" width="230" height="152" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="389" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">append and re-sort later</text>
<text x="389" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">cheap now, tidy later</text>
<text x="389" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">partition briefly unsorted</text>
<text x="389" y="170" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">next compaction fixes it</text>
<text x="389" y="196" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">the usual answer</text>
<rect x="522" y="58" width="230" height="152" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="637" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">a late-arrivals table</text>
<text x="637" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">union at read time</text>
<text x="637" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">main table untouched</text>
<text x="637" y="170" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">merged on a schedule</text>
<text x="637" y="196" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">for extreme lateness</text>
</svg>
</figure>

The middle option is right for almost every case, and it depends on the compaction schedule already existing — which is another reason to treat compaction as part of the design rather than as an optimisation. A partition that receives a late batch is briefly less well sorted and is repaired by the next scheduled run.

Set a **lateness horizon** explicitly: records older than it go to the late-arrivals table or are rejected, and records within it are appended normally. Without a horizon, a corrupted timestamp can cause an append to a partition from three years ago, which is both surprising and expensive. A horizon of a few days covers genuine connectivity gaps and excludes the pathological cases.

## Validation in the Hot Path

The validation contract described in [geometry validation and repair](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/geometry-validation-and-repair/) applies to streaming exactly as it does to batch, and the objection — that it costs throughput — deserves a measured answer rather than an assumption.

For point telemetry, which is the overwhelming majority of streaming spatial volume, validation is nearly free. A point is valid by construction, so the check reduces to a null test and a coordinate-range assertion, both of which are single comparisons. There is no throughput argument against validating every point.

For polygon streams — boundary updates, coverage footprints, geofence definitions — validation is genuinely more expensive and the volumes are correspondingly lower. A stream of a few hundred polygon updates a minute can afford full validation with room to spare; the arithmetic only becomes uncomfortable at volumes that polygon streams rarely reach.

The case that needs care is a mixed stream at high volume with occasional very complex geometries. There, a vertex-count threshold works well: validate everything below it, and route the small number above it to a separate slower path that validates thoroughly without blocking the main flow. The threshold is a throughput protection rather than a correctness compromise, because the complex geometries are still validated — just not synchronously.

What should never be skipped, at any volume, are the **coordinate-range and extent assertions**. They are two comparisons per record, they catch the coordinate-system drift that silently breaks every downstream join, and they are the difference between noticing an upstream change within a minute and noticing it in a monthly report.

## Operating the Pipeline

Four signals tell you whether a streaming spatial pipeline is healthy, and none of them is throughput.

**Consumer lag trend** is the primary signal: whether the pipeline is keeping up. Its direction matters more than its value, and a lag that has grown steadily for an hour warrants attention before it becomes a lag that has grown for a day.

**Files per partition** is the file-count canary. It should oscillate within a band as writes accumulate and compaction reduces them; a monotonic climb means compaction is falling behind, and the query latency degradation follows about a week later.

**Commit conflict rate** should be near zero with a coordinator in place. Any sustained non-zero rate means something else is writing to the table — often a backfill, sometimes a second instance of the pipeline that a deployment left running.

**Quality counters** — clean, snapped, repaired, quarantined — are the upstream signal. A step change in the ratio means a source changed, and catching that from the counters is far cheaper than catching it from a downstream consumer's complaint.

All four are cheap, all four are trends rather than thresholds, and together they cover the failure modes that actually occur. The wider practice is in [spatial data observability](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/spatial-data-observability/); the guides below work through the ingest, the batch sizing and the commit semantics in code.

## Choosing the Runtime

Three runtimes are commonly used for streaming spatial ingest, and the choice follows from volume and from what else the platform already runs rather than from any spatial consideration.

**A plain Python consumer** — a loop reading from the source, a process pool for transformation, and a table-format writer — is sufficient for a surprising range of volumes and is by far the simplest to operate. It has no cluster, no checkpoint directory, no framework semantics to learn, and its failure modes are ordinary Python failure modes. For volumes up to tens of thousands of records a second it is a legitimate production choice, and it is the right place to start.

**Structured streaming on Spark** brings checkpointing, exactly-once semantics and scaling for free, at the cost of a cluster and of framework semantics that interact with the table format in ways worth understanding before relying on them. It is the right answer when the same platform already runs Spark for batch transforms, because the operational surface is shared.

**A dedicated stream processor** writing to the table through a connector is the right answer when the pipeline does substantial stateful work before the write — windowed aggregation, session reconstruction, enrichment against a changing reference set. For pure ingest it adds a component without adding capability.

The spatial-specific consideration cuts across all three: whichever runtime is used, the transformation stage must have real parallelism, because it is CPU-bound rather than I/O-bound. A framework that provides concurrency through an event loop rather than through processes will not help, and a plain Python consumer with a process pool will outperform an async one by the number of cores available.

Measure the per-record transformation cost early. It is the number that determines how much parallelism is needed, it is easy to obtain from a single batch, and it is the input to every capacity decision that follows.

## A Readiness Checklist

- [ ] Transformation runs in a process pool, not on an event loop — the work is CPU-bound
- [ ] Batches are sized by serialised bytes, not by row count
- [ ] Commits are batched through a single coordinator; workers produce files, one process publishes
- [ ] Source offsets are recorded in the commit, so a restart resumes from what the table says
- [ ] Coordinate-range and extent assertions run on every record, at any volume
- [ ] A vertex-count threshold routes pathological geometries to a slower path rather than blocking the stream
- [ ] Compaction is scheduled separately, with its own budget, scoped to partitions the writer is not targeting
- [ ] A sort rewrite runs once per partition when it closes, and never again
- [ ] A lateness horizon is declared; records beyond it go to a late-arrivals table rather than an old partition
- [ ] Backpressure policy is decided in advance: which streams may be shed, what may be degraded
- [ ] Consumer lag, files per partition, commit conflicts and quality counters are emitted as trends

The list is short because the failure modes are few and repetitive. Nearly every troubled streaming spatial pipeline is missing the coordinator, the compaction schedule, or both — and the symptoms of those two omissions account for the majority of the incidents this topic exists to prevent.

## Where This Sits

Streaming ingest is where the rest of this site's guidance is exercised hardest, because every constraint applies simultaneously and none can be deferred. The validation contract from [geometry validation and repair](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/geometry-validation-and-repair/) must run in the hot path. The layout requirements from [spatial partitioning schemes](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/spatial-partitioning-schemes/) determine what the writer may partition on. The maintenance schedule from [lakehouse maintenance automation](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/lakehouse-maintenance-automation/) is not optional here in the way it can be for batch tables. And the metrics from [spatial data observability](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/spatial-data-observability/) are what make the whole arrangement legible.

A team that has built a well-behaved streaming spatial pipeline has, by necessity, made every decision the rest of this site describes — which is why it is a reasonable last thing to build rather than a first.
The guides below implement each piece: the consumer itself, the batch sizing that keeps it stable, and the commit semantics that make it safe to retry.
Read them in that order; each assumes the decisions the previous one settled, and together they cover a production pipeline end to end. Skipping ahead to the third produces a correct pipeline that nobody can operate.
The first two are prerequisites, not preliminaries.
