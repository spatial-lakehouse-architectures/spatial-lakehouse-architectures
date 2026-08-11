# Async Execution Patterns

Asynchronous execution is a foundational requirement for spatial data lakehouse architectures. Geospatial workloads—spanning high-frequency IoT telemetry, multi-petabyte satellite mosaics, and LiDAR point clouds—introduce severe I/O bottlenecks and compute skew that synchronous batch pipelines cannot absorb. By decoupling compute orchestration from storage mutations, platform teams achieve higher ingestion throughput while preserving strict ACID guarantees. This execution model integrates directly into the broader [Python Ecosystem & Integration Workflows](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/), where task schedulers, distributed executors, and format-specific APIs converge to handle spatial transformations without blocking the main pipeline thread or stalling downstream query engines.

<figure class="diagram">
<svg viewBox="0 0 752 201" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Async geometry ingestion pipeline: submit, queue, worker pool, gather and commit">
<defs>
<marker id="async-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#1f6f8b"/></marker>
</defs>
<rect x="0" y="0" width="752" height="201" fill="#f7fbfc"/>
<text x="380" y="30" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Async geometry ingestion pipeline</text>
<rect x="20" y="75" width="150" height="70" rx="8" fill="#ffffff" stroke="#1f6f8b" stroke-width="2"/>
<text x="95" y="105" text-anchor="middle" font-family="sans-serif" font-size="14" font-weight="600" fill="#0d3b45">Async Submit</text>
<text x="95" y="125" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">tile / scene tasks</text>
<rect x="210" y="75" width="150" height="70" rx="8" fill="#ffffff" stroke="#1f6f8b" stroke-width="2"/>
<text x="285" y="105" text-anchor="middle" font-family="sans-serif" font-size="14" font-weight="600" fill="#0d3b45">Task Queue</text>
<text x="285" y="125" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">backpressure</text>
<rect x="400" y="75" width="150" height="70" rx="8" fill="#ffffff" stroke="#1f6f8b" stroke-width="2"/>
<text x="475" y="105" text-anchor="middle" font-family="sans-serif" font-size="14" font-weight="600" fill="#0d3b45">Worker Pool</text>
<text x="475" y="125" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">WKB decode</text>
<rect x="590" y="75" width="150" height="70" rx="8" fill="#ffffff" stroke="#1f6f8b" stroke-width="2"/>
<text x="665" y="105" text-anchor="middle" font-family="sans-serif" font-size="14" font-weight="600" fill="#0d3b45">Gather + Commit</text>
<text x="665" y="125" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">ACID write</text>
<line x1="170" y1="110" x2="210" y2="110" stroke="#1f6f8b" stroke-width="2" marker-end="url(#async-arrow)"/>
<line x1="360" y1="110" x2="400" y2="110" stroke="#1f6f8b" stroke-width="2" marker-end="url(#async-arrow)"/>
<line x1="550" y1="110" x2="590" y2="110" stroke="#1f6f8b" stroke-width="2" marker-end="url(#async-arrow)"/>
<text x="380" y="185" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">Non-blocking dispatch keeps storage mutations off the main pipeline thread</text>
</svg>
</figure>

## Spatial Partitioning & Async Compaction

Spatial partitioning strategies (H3, S2, Quadkey, or Z-order curves) dictate how background maintenance jobs are scheduled, prioritized, and isolated. When geometry-heavy tables exceed 100M rows per partition, synchronous compaction routinely triggers OOM errors on executor nodes due to WKB/GeoJSON deserialization overhead and unbounded memory allocation during spatial joins. The production-ready mitigation is to dispatch compaction asynchronously, monitor manifest sizes, and trigger rewrites only when storage thresholds are breached.

For Apache Iceberg, async compaction relies on the `rewrite_data_files` stored procedure. Configure the following parameters to align with spatial data characteristics:
- `target-file-size-bytes`: `134217728` (128MB)
- `max-concurrent-file-group-rewrites`: 3–5 to prevent object storage API throttling
- `partial-progress.enabled`: `true` (allows checkpointing during long rewrites)

```sql
-- Async Iceberg compaction for spatially-sorted table
CALL catalog.system.rewrite_data_files(
  table => 'spatial_catalog.iot_telemetry',
  strategy => 'sort',
  options => map(
    'sort-order', 'min_x ASC, min_y ASC',
    'max-concurrent-file-group-rewrites', '4',
    'partial-progress.enabled', 'true',
    'target-file-size-bytes', '134217728'
  )
);
```

Delta Lake approaches the same problem through the `OPTIMIZE` command. Because Delta's optimistic concurrency control may require explicit retry logic when multiple async writers target the same partition, implement exponential backoff with jitter. Reference implementations for geometry-aware async dispatch are documented in [Delta-rs Geometry Processing](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/delta-rs-geometry-processing/), which covers Rust-backed parallel execution and safe concurrent writes.

```python
import asyncio
import random
from deltalake import DeltaTable, write_deltalake

async def async_optimize_with_backoff(table_path: str, zorder_cols: list[str]):
    """
    Runs Delta OPTIMIZE with Z-ORDER via delta-rs Python API.
    delta-rs optimize().execute_z_order_by() is synchronous; run in thread pool.
    """
    max_retries = 5
    loop = asyncio.get_running_loop()

    for attempt in range(max_retries):
        try:
            dt = DeltaTable(table_path)
            # Run the blocking optimize call in a thread pool executor
            await loop.run_in_executor(
                None,
                lambda: dt.optimize.z_order(zorder_cols)
            )
            print(f"OPTIMIZE z_order succeeded for {table_path}")
            return
        except Exception as e:
            delay = (2 ** attempt) + random.uniform(0, 1)
            print(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay:.2f}s")
            await asyncio.sleep(delay)

    raise RuntimeError(f"Max retries exceeded for async OPTIMIZE on {table_path}")

# Example usage
asyncio.run(async_optimize_with_backoff(
    "s3://lakehouse/telemetry",
    ["min_x", "min_y", "max_x", "max_y"]
))
```

## Metadata-Driven Indexing & Predicate Pushdown

Spatial indexing in lakehouse formats is inherently metadata-driven. Iceberg stores partition specs, sort orders, and statistics in the metadata layer, enabling async index materialization via background `rewrite_data_files` operations. When integrating vectorized geometry operations, teams route index builds through [PyIceberg Spatial Workflows](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/pyiceberg-spatial-workflows/), leveraging `asyncio` to parallelize metadata refresh and spatial predicate caching across partition boundaries.

Critical configuration for async metadata updates:
- `write.metadata.compression-codec`: `zstd`
- `write.metadata.previous-versions-max`: `20` (increase for active tables with many snapshots)
- `history.expire.max-snapshot-age-ms`: `2592000000` (30 days)

```python
import asyncio
from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table

async def refresh_partition_metadata(table: Table, partition_ids: list[str]):
    """
    Triggers async metadata refresh for a list of partition IDs.
    PyIceberg catalog I/O is synchronous; use run_in_executor to avoid blocking.
    """
    loop = asyncio.get_running_loop()
    semaphore = asyncio.Semaphore(5)  # Max 5 concurrent catalog calls

    async def refresh_one(part_id: str):
        async with semaphore:
            await loop.run_in_executor(
                None,
                lambda: table.refresh()  # Re-read metadata from catalog
            )
            print(f"Refreshed metadata for partition {part_id}")

    await asyncio.gather(*[refresh_one(pid) for pid in partition_ids])

catalog = load_catalog("default")
tbl = catalog.load_table("spatial_catalog.satellite_tiles")
asyncio.run(refresh_partition_metadata(tbl, ["res7_cluster_01", "res7_cluster_02"]))
```

## Orchestration & CI/CD Validation

Async execution patterns require deterministic validation before production deployment. CI pipelines must verify partition alignment, retention policies, and compaction thresholds. The following GitHub Actions workflow validates async maintenance configurations against a staging lakehouse:

```yaml
name: Validate Async Spatial Maintenance
on:
  push:
    paths: ['lakehouse-configs/compaction-policies/**']

jobs:
  validate-config:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Setup Python & Lakehouse CLI
        run: |
          pip install pyiceberg delta-spark pyarrow
      - name: Run Async Config Validator
        run: |
          python -c "
          import json, sys
          with open('lakehouse-configs/compaction-policies/spatial.json') as f:
              cfg = json.load(f)
          assert cfg['crs'] == 'EPSG:4326', 'Invalid CRS'
          assert cfg['retention_days'] >= 14, 'Retention too short for async vacuum'
          assert cfg['max_concurrent_compactions'] <= 5, 'Throttle risk: reduce concurrent compactions'
          print('Async spatial config validated')
          "
```

## Troubleshooting & Operational Guardrails

| Symptom | Root Cause | Remediation |
|---------|------------|-------------|
| `java.lang.OutOfMemoryError: Java heap space` during compaction | Unbounded WKB deserialization in synchronous executors | Switch to async dispatch; set `spark.sql.shuffle.partitions` to match vCPU count; enable `spark.memory.fraction=0.7` |
| Metadata lock contention on concurrent writes | Multiple async `OPTIMIZE`/`rewrite` jobs targeting identical partitions | Cap concurrent jobs to 3 per partition key; use partition-level locking or job queuing |
| Query latency spikes after async vacuum | Aggressive snapshot pruning breaking time-travel compatibility | Align `history.expire.max-snapshot-age-ms` with SLA (minimum 30 days); verify `gc.enabled=false` during peak query windows |
| Spatial predicate pushdown failure | Missing sort order or stale statistics post-compaction | Re-run `OPTIMIZE` (Delta) or `rewrite_data_files` (Iceberg) with explicit sort order; verify bbox column statistics via `DESCRIBE DETAIL` or manifest inspection |

Monitor async job health using manifest size metrics and object storage request rates. When `write.metadata.previous-versions-max` approaches capacity, trigger async metadata cleanup before it impacts catalog resolution latency. Maintain explicit retention windows (e.g., 30 days for snapshots, 14 days for transaction logs) and enforce CRS consistency (`EPSG:4326`) across all async workers to prevent coordinate transformation drift during background rewrites.

For authoritative reference on compaction semantics and maintenance procedures, consult the official [Apache Iceberg Maintenance Documentation](https://iceberg.apache.org/docs/latest/maintenance/) and [Delta Lake OPTIMIZE Utilities](https://docs.delta.io/latest/delta-utility.html#optimize-a-delta-table). Python async primitives used in spatial dispatch follow the standard [asyncio Task Management](https://docs.python.org/3/library/asyncio.html) model.

## What Concurrency Actually Buys in a Spatial Pipeline

Async is frequently reached for as a general speed-up and delivers nothing, because the bottleneck in most spatial Python code is CPU inside the interpreter rather than waiting on a socket. Knowing which parts of the pipeline are I/O-bound is the whole decision.

<figure class="diagram">
<svg viewBox="0 0 762 268" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Stages of a spatial write pipeline classified as I/O bound or CPU bound: reading source files and uploading to object storage are I/O bound and benefit from async, while geometry validation, reprojection and WKB encoding are CPU bound and need processes">
<rect x="0" y="0" width="762" height="268" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Only the shaded stages get faster with async</text>
<rect x="30" y="58" width="352" height="98" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="206" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">I/O bound — async helps</text>
<text x="206" y="110" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">listing source objects · GET requests</text>
<text x="206" y="130" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">PUT of written parts · catalog commits</text>
<rect x="398" y="58" width="352" height="98" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="574" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">CPU bound — async does nothing</text>
<text x="574" y="110" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">ST_IsValid · reprojection · WKB encode</text>
<text x="574" y="130" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">cell derivation · Parquet encode</text>
<rect x="130" y="180" width="520" height="76" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="390" y="208" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">the working pattern: process pool for the right, async for the left</text>
<text x="390" y="232" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">CPU work in workers, results streamed back to an async writer with bounded concurrency</text>
</svg>
</figure>

The measurement that settles it takes minutes: profile one batch and record the wall-clock split between waiting on network and executing Python. In most ingestion pipelines the split is around 30/70 in favour of CPU, which means an async rewrite that leaves the geometry work on the event loop can improve total runtime by at most a third — and frequently makes it worse, because the event loop is blocked by the CPU work and the concurrency never materialises.

The pattern that does work separates the two explicitly. Geometry validation, reprojection and encoding run in a process pool where they get real parallelism; the results come back as encoded buffers; and an async writer with a bounded semaphore issues the uploads and the commit. Each half is doing the kind of work it is good at, and the event loop is never blocked by anything that takes longer than a few microseconds.

## Bounding Concurrency Deliberately

Unbounded concurrency is the most common defect in async spatial code, and its symptom is memory exhaustion rather than an error message about concurrency.

<figure class="diagram">
<svg viewBox="0 0 732 244" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Memory profile of an unbounded async writer where every serialised batch is held in flight simultaneously, against a semaphore bounded writer where memory stays flat at the concurrency limit">
<rect x="0" y="0" width="732" height="244" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Memory in flight, with and without a semaphore</text>
<line x1="80" y1="196" x2="720" y2="196" stroke="#33707d" stroke-width="1.5"/>
<line x1="80" y1="56" x2="80" y2="196" stroke="#33707d" stroke-width="1.5"/>
<text x="64" y="62" text-anchor="end" font-family="sans-serif" font-size="11" fill="#33707d">OOM</text>
<path d="M90 192 L280 60" fill="none" stroke="#9a5a17" stroke-width="2.5"/>
<text x="200" y="86" font-family="sans-serif" font-size="11" font-weight="700" fill="#9a5a17">unbounded: grows with batch count</text>
<path d="M90 192 L200 150 L720 150" fill="none" stroke="#2f6e49" stroke-width="2.5"/>
<text x="440" y="140" font-family="sans-serif" font-size="11" font-weight="700" fill="#2f6e49">bounded: flat at limit × batch size</text>
<text x="400" y="228" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">batches submitted &#8594;</text>
</svg>
</figure>

Wide geometries make this far worse than it is for scalar data. A batch of ten thousand administrative polygons can serialise to tens of megabytes, so a hundred in-flight batches is gigabytes of buffers held simultaneously — and the code that produced them looks entirely reasonable, because `asyncio.gather` over a list comprehension is the idiomatic way to write it.

The fix is a semaphore sized from the actual batch footprint rather than from a habit. Measure the serialised size of a representative batch, decide how much memory the process may hold in flight, and divide. For most spatial writers the answer lands between eight and thirty-two, which is far below the number an unbounded gather would create and far above one. Then assert it: a test that submits a thousand batches and checks peak resident memory stays under a bound catches a regression that no functional test will.

## Commit Contention and Retry Strategy

The second failure mode of concurrent writing is not memory but conflict. Table formats resolve concurrent commits optimistically: two writers that both append succeed, two writers whose changes overlap produce a conflict, and the loser retries. Under concurrency the retry behaviour becomes the dominant factor in throughput.

Three parameters govern it. The **commit granularity** decides how often a conflict is possible — a writer that commits every batch conflicts far more than one that accumulates and commits every thirty seconds. The **retry policy** decides how expensive a conflict is; a naive immediate retry from several writers produces a thundering herd that makes the conflict window worse. And the **scope of each write** decides whether conflicts are even possible: writers that append to disjoint partitions rarely conflict at all, because the metadata they touch does not overlap.

The practical guidance follows directly. Batch commits rather than committing per unit of work; a spatial write that commits once per five thousand features instead of once per feature reduces both the conflict rate and the metadata growth by three orders of magnitude. Use exponential backoff with jitter rather than a fixed delay. And where possible, shard the writers by partition so that concurrency is between disjoint regions of the table rather than within one.

Where conflicts persist despite all three, the remaining lever is to serialise the commit step while keeping the expensive work parallel. A pool of workers produces written data files concurrently, and a single coordinator performs the commits sequentially. Commits are cheap — they write metadata, not data — so the serialisation costs almost nothing, and it removes conflict entirely. This is the arrangement most mature pipelines converge on, and it is worth reaching for before spending a week tuning backoff parameters.

Instrument the conflict rate as a metric regardless of which approach is chosen. A rate that climbs over weeks is usually a symptom of something else — more writers, larger batches, a maintenance job that has started overlapping the ingest window — and it is easier to diagnose from a trend than from an incident.

## Cancellation, Timeouts and Partial Writes

Async code makes cancellation possible, which means a spatial pipeline has to decide what a cancelled write leaves behind.

The good news is that table formats make this tractable: data files written but never committed are simply orphans, invisible to every reader, and reclaimed by the orphan-file cleanup described in the maintenance guides. A cancelled job therefore leaves no incorrect state, only wasted storage. That property is worth relying on deliberately — it means a timeout can be aggressive without risking correctness.

The bad news is that orphans accumulate faster than teams expect when cancellation is routine. A pipeline that times out a few times a day and writes hundreds of megabytes before each timeout will build up terabytes over a year, invisibly, because nothing lists them. Run the orphan cleanup on a schedule and alert on the volume it reclaims; a sudden rise is a signal that something upstream is failing repeatedly and being retried silently.

Timeouts themselves need to be set per operation rather than globally. An object-storage `PUT` that has not completed in thirty seconds is almost certainly stuck and retrying is correct; a catalog commit that has not completed in thirty seconds may simply be contended and retrying makes the contention worse. Distinguish the two, and give the commit a longer budget with backoff rather than a short one with immediate retry.

Finally, make cancellation propagate to the process pool. A cancelled async task that leaves CPU workers grinding on geometry for another ten minutes has not been cancelled in any useful sense, and the pattern is common enough to be worth an explicit test: cancel a job mid-batch and assert that the worker processes exit within a bounded time.

## A Reference Shape for a Concurrent Spatial Writer

Putting the pieces together gives a shape that has proved durable across a range of pipelines, and it is worth stating as a whole because the individual pieces are easy to adopt in isolation and get wrong together.

Source discovery runs first and is naturally async: listing objects, reading manifests, resolving what has changed since the last run. It produces a work list of source units, each small enough to process independently.

Transformation runs in a process pool, one unit per task. Each worker validates geometry, reprojects to the canonical system, encodes WKB, derives the bounding-box columns and the partition value, and returns an encoded Arrow batch rather than Python objects — returning objects across the process boundary costs a pickle round trip that frequently exceeds the work itself.

Writing runs on the event loop with a semaphore, taking batches as workers finish them and issuing the object-storage writes concurrently. Committing runs on a single coordinator, batching the results of many writes into one commit so that conflicts are rare and metadata stays small.

Observability wraps all of it: counts in and out at each stage, in-flight bytes, conflict count, and per-stage wall-clock. The last of these is what makes the next optimisation obvious rather than speculative, and it is the piece most often left out — a pipeline that reports only total runtime gives no information about which of its four stages to improve.

The shape is deliberately boring. Its value is that each stage fails in one recognisable way — discovery finds nothing, transformation raises on bad geometry, writes exhaust memory, commits conflict — and each has an established fix. Pipelines that mix the stages produce failures that could be any of the four.

## When Not to Introduce Concurrency

Concurrency has a fixed cost in comprehensibility, and there are pipelines where paying it is a mistake.

A job that runs once nightly and finishes in eleven minutes does not need to finish in six. The saving is invisible to everyone, and the cost — a harder failure mode, a stack trace that spans an event loop and a process pool, a new class of memory bug — is paid by whoever debugs it at three in the morning. Sequential code that finishes inside its window is finished code.

A job whose runtime is dominated by a single large file gains nothing either, because there is nothing to overlap. Concurrency helps when there are many independent units; it does nothing for one unit that is slow. The fix there is either to split the unit or to make the work itself faster, usually by moving it out of Python.

And a job that is failing intermittently should be made reliable before it is made fast. Concurrency multiplies the number of ways a flaky dependency can express itself, and debugging an intermittent failure across concurrent tasks is materially harder than debugging it in a loop.

The order that works: make it correct, make it reliable, measure where the time goes, and only then introduce concurrency into the stage the measurement pointed at. Most pipelines that reach for async first end up with the same runtime and a worse debugging experience.

For the concrete implementation of a bounded, retrying async writer against a catalog, see [async catalog writes with PyIceberg and asyncio](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/async-execution-patterns/async-catalog-writes-with-pyiceberg-and-asyncio/), which works through the semaphore sizing and the commit coordination in code.
Read it alongside this page rather than instead of it: the shape here is what the code there implements.
