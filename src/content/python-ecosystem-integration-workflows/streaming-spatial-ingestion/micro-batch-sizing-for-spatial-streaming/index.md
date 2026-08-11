# Micro-Batch Sizing for Spatial Streaming

This guide derives the commit interval and batch size for a spatial streaming pipeline from three measurable inputs, rather than copying a default that was tuned for scalar data.

## Context and prerequisites

Batch sizing decides freshness, file size, memory footprint and how much work the compactor inherits — all at once. For spatial data the usual defaults mislead, because record widths vary by two orders of magnitude between point telemetry and polygon updates. This recipe applies to any streaming runtime; the examples use a plain Python consumer and PySpark structured streaming. The surrounding design is in [streaming spatial ingestion](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/streaming-spatial-ingestion/).

## Size by bytes, never by rows

<figure class="diagram">
<svg viewBox="0 0 632 282" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="The same row count producing wildly different batch sizes for point telemetry, mixed records and polygon updates, showing why a row based batch size gives an unpredictable memory footprint">
<rect x="0" y="0" width="632" height="282" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">100 000 rows, three very different batches</text>
<rect x="60" y="70" width="42" height="46" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="230" y="99" font-family="sans-serif" font-size="12" fill="#0d3b45">point telemetry — 3 MB</text>
<rect x="60" y="132" width="150" height="46" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="230" y="161" font-family="sans-serif" font-size="12" fill="#0d3b45">mixed features — 34 MB</text>
<rect x="60" y="194" width="560" height="46" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="230" y="223" font-family="sans-serif" font-size="12" fill="#0d3b45">boundary updates — 780 MB</text>
<text x="390" y="266" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">A row-count batch size is a memory footprint nobody chose</text>
</svg>
</figure>

The consequence is practical rather than theoretical. A pipeline configured for 100,000 rows per batch runs comfortably on point data and exhausts memory the first time a boundary refresh arrives on the same topic — and the failure occurs in production, at whatever hour the refresh is scheduled.

Targeting bytes fixes the memory profile regardless of what arrives. Measure the serialised size of a representative sample, divide the memory budget by it, and the resulting row count becomes a derived value that adapts as the mix changes.

## Complete working solution

```python
import pyarrow as pa

MEMORY_BUDGET_MB = 512          # per worker, for in-flight buffers
TARGET_FILE_MB   = 256          # committed file size we want
MAX_LATENCY_S    = 120          # freshness commitment to consumers

def measure_record_bytes(sample_batch: pa.Table) -> float:
    """Serialised bytes per record, from a real sample."""
    return sample_batch.nbytes / max(sample_batch.num_rows, 1)

def derive_sizing(sample_batch: pa.Table, records_per_second: float) -> dict:
    per_record = measure_record_bytes(sample_batch)

    rows_by_memory = int((MEMORY_BUDGET_MB * 1e6) / per_record)
    rows_by_file   = int((TARGET_FILE_MB   * 1e6) / per_record)
    rows_by_time   = int(records_per_second * MAX_LATENCY_S)

    rows = min(rows_by_memory, rows_by_file, rows_by_time)
    interval = rows / records_per_second if records_per_second else MAX_LATENCY_S

    return {
        "bytes_per_record": per_record,
        "rows_per_batch": rows,
        "commit_interval_s": min(interval, MAX_LATENCY_S),
        "expected_file_mb": rows * per_record / 1e6,
        "binding_constraint": (
            "memory" if rows == rows_by_memory else
            "file_size" if rows == rows_by_file else "latency"),
    }
```

```python
# Applying it in a plain consumer: trigger on whichever bound arrives first.
buffered_bytes, last_commit = 0, time.monotonic()
for batch in stream:
    buffer.append(batch)
    buffered_bytes += batch.nbytes
    if (buffered_bytes >= TARGET_FILE_MB * 1e6
            or time.monotonic() - last_commit >= MAX_LATENCY_S):
        commit(buffer)
        buffer, buffered_bytes = [], 0
        last_commit = time.monotonic()
```

```python
# Structured streaming: the equivalent knobs.
(stream.writeStream
   .trigger(processingTime="2 minutes")            # the latency bound
   .option("maxBytesPerTrigger", 256 * 1024 * 1024) # the size bound
   .option("fanout-enabled", "false")               # one file per partition per batch
   .toTable("lakehouse.spatial.telemetry"))
```

## Step-by-step walkthrough

1. **Measure from a real sample.** `Table.nbytes` on a representative batch gives the serialised size directly. Estimating from row counts and assumed widths is where the two-order-of-magnitude errors come from.

2. **Compute three candidate sizes and take the minimum.** Memory bounds what a worker can hold, file size bounds what the compactor inherits, and latency bounds how stale the data may be. Whichever is smallest is the binding constraint, and knowing which one it is tells you what to change if the result is unsatisfactory.

3. **Report the binding constraint.** A pipeline bound by latency has memory to spare and could batch more if freshness were relaxed; one bound by memory needs more workers or smaller records. These are different conversations and the derivation distinguishes them for free.

4. **Trigger on both bounds.** Size alone stalls during quiet periods; time alone produces enormous batches during bursts. Both, with whichever arrives first, handles the full range of arrival rates.

5. **Re-derive when the record mix changes.** A new source with wider records changes the bytes per record and therefore every downstream number. Recomputing from a fresh sample takes seconds and should be part of onboarding any new producer.

## Common errors and fixes

| Symptom | Cause | Fix |
|---|---|---|
| Memory exhaustion when polygons arrive | Batch sized in rows | Size by bytes; re-derive on mix change |
| Thousands of small files overnight | Latency bound far below what consumers need | Relax the freshness target; two minutes suits most dashboards |
| Batches never fill during quiet hours | No time-based trigger | Add the latency bound as a second trigger |
| Files far larger than the target | Several partitions written per batch, or fanout enabled | Check the writer's file-per-partition behaviour |
| Latency far worse than configured | Processing time exceeds the trigger interval | The pipeline is not keeping up; scale the transformation stage |

## The file-size consequence

<figure class="diagram">
<svg viewBox="0 0 732 264" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="How the commit interval determines the number of files per partition per day and therefore the compaction workload, with a short interval producing thousands of files and a longer one producing hundreds">
<rect x="0" y="0" width="732" height="264" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Files per partition per day, by commit interval</text>
<line x1="80" y1="200" x2="720" y2="200" stroke="#33707d" stroke-width="1.5"/>
<rect x="110" y="62" width="60" height="138" fill="#9a5a17"/>
<text x="140" y="222" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">10 s</text>
<text x="140" y="52" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">8 640</text>
<rect x="250" y="140" width="60" height="60" fill="#0e6e7d"/>
<text x="280" y="222" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">1 min</text>
<text x="280" y="130" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">1 440</text>
<rect x="390" y="176" width="60" height="24" fill="#2f6e49"/>
<text x="420" y="222" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">5 min</text>
<text x="420" y="166" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">288</text>
<rect x="530" y="190" width="60" height="10" fill="#2f6e49"/>
<text x="560" y="222" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">15 min</text>
<text x="560" y="180" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">96</text>
<text x="390" y="248" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">Every one of these files must be compacted; the compactor&#8217;s cost scales with the count</text>
</svg>
</figure>

The compactor's workload is what makes the short intervals expensive, and it is a cost that appears on a different budget line from the ingest — which is why it is frequently ignored when the interval is chosen. A ten-second interval on a table with fifty partitions produces over four hundred thousand files a day for the compactor to merge, and the merging costs more compute than the ingest did.

Choosing the interval with the compaction cost in view usually moves it upward. The freshness a consumer actually needs is rarely below a minute, and the difference between one minute and ten seconds is a sixfold reduction in the maintenance workload for a difference nobody perceives.

## Verification

```python
def verify_sizing(table, expected_file_mb: float, tolerance: float = 2.0):
    sizes = [t.file.file_size_in_bytes / 1e6 for t in table.scan().plan_files()]
    sizes.sort()
    median = sizes[len(sizes) // 2]
    assert median > expected_file_mb / tolerance, (
        f"files far smaller than intended: median {median:.0f} MB "
        f"vs expected {expected_file_mb:.0f} MB")
    assert median < expected_file_mb * tolerance, (
        f"files far larger than intended: median {median:.0f} MB")
```

Run it against the newest partition, before compaction has run, so it measures what the writer produced rather than what the compactor repaired. A median far below the target means the batch is not filling — usually the latency bound binding earlier than expected — and a median far above means several partitions are being written per batch, which is a fanout question rather than a sizing one.

Record the derived sizing alongside the pipeline configuration, with the measured bytes per record and the date. The next person to wonder why the interval is two minutes will find an answer rather than a convention.

## Partition Fanout, the Hidden Multiplier

The batch size determines how many rows are committed at once; the partition fanout determines how many files that becomes, and the two multiply.

<figure class="diagram">
<svg viewBox="0 0 705 208" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="One micro batch spread across many partitions produces one small file per partition, whereas a batch confined to few partitions produces fewer larger files">
<rect x="0" y="0" width="705" height="208" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">The same batch, two partition schemes</text>
<text x="196" y="62" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#9a5a17">day + fine cell</text>
<rect x="70" y="80" width="18" height="18" fill="#f2e8da" stroke="#9a5a17" stroke-width="1"/>
<rect x="94" y="80" width="18" height="18" fill="#f2e8da" stroke="#9a5a17" stroke-width="1"/>
<rect x="118" y="80" width="18" height="18" fill="#f2e8da" stroke="#9a5a17" stroke-width="1"/>
<rect x="142" y="80" width="18" height="18" fill="#f2e8da" stroke="#9a5a17" stroke-width="1"/>
<rect x="166" y="80" width="18" height="18" fill="#f2e8da" stroke="#9a5a17" stroke-width="1"/>
<rect x="190" y="80" width="18" height="18" fill="#f2e8da" stroke="#9a5a17" stroke-width="1"/>
<rect x="214" y="80" width="18" height="18" fill="#f2e8da" stroke="#9a5a17" stroke-width="1"/>
<rect x="238" y="80" width="18" height="18" fill="#f2e8da" stroke="#9a5a17" stroke-width="1"/>
<rect x="70" y="104" width="18" height="18" fill="#f2e8da" stroke="#9a5a17" stroke-width="1"/>
<rect x="94" y="104" width="18" height="18" fill="#f2e8da" stroke="#9a5a17" stroke-width="1"/>
<rect x="118" y="104" width="18" height="18" fill="#f2e8da" stroke="#9a5a17" stroke-width="1"/>
<rect x="142" y="104" width="18" height="18" fill="#f2e8da" stroke="#9a5a17" stroke-width="1"/>
<rect x="166" y="104" width="18" height="18" fill="#f2e8da" stroke="#9a5a17" stroke-width="1"/>
<rect x="190" y="104" width="18" height="18" fill="#f2e8da" stroke="#9a5a17" stroke-width="1"/>
<rect x="214" y="104" width="18" height="18" fill="#f2e8da" stroke="#9a5a17" stroke-width="1"/>
<rect x="238" y="104" width="18" height="18" fill="#f2e8da" stroke="#9a5a17" stroke-width="1"/>
<text x="163" y="170" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">16 files of 2 MB each</text>
<text x="163" y="192" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">the compactor inherits all of them</text>
<text x="584" y="62" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#2f6e49">day only</text>
<rect x="500" y="80" width="168" height="42" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="584" y="170" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">1 file of 32 MB</text>
<text x="584" y="192" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">spatial layout comes from clustering</text>
</svg>
</figure>

This is the strongest practical argument against partitioning a streaming spatial table on a fine grid cell. Each micro-batch spans the whole service area, so it writes into every cell partition, and a batch that would have been one healthy file becomes hundreds of fragments. The file count then scales with the product of the commit rate and the partition count, which is how tables reach millions of files in a month.

Partition on time alone, and obtain the spatial layout from the sort order or clustering instead. The spatial pruning is nearly as good — file-level statistics on the bounding-box columns do the work — and the file count stays proportional to the commit rate alone.

Where a spatial partition dimension is genuinely required, keep it very coarse: a handful of regions rather than thousands of cells, chosen so that a typical micro-batch touches one or two of them rather than all.

## Adapting Under Load

A fixed batch size is right for a steady stream and wrong for one with a diurnal cycle or occasional bursts, which describes most spatial telemetry.

The simplest adaptation is to keep the byte target fixed and let the interval float, which the dual-trigger arrangement already does: during busy periods the size bound fires first and commits are frequent; during quiet periods the time bound fires and commits are small. Memory stays bounded, freshness stays within the commitment, and nothing needs tuning.

The case that needs more is a sustained burst that outpaces the transformation stage. Here the buffer fills faster than it drains, and the correct response is not a larger batch — that only postpones the problem — but backpressure: stop consuming until the buffer drains. A consumer that keeps reading while its buffer grows converts a throughput problem into an out-of-memory kill, which loses the buffered data as well.

Implement it as a simple check before each poll: if buffered bytes exceed a high-water mark, skip the poll and wait. Kafka and similar sources retain data, so pausing costs lag rather than loss, and lag is a signal somebody can act on while a restart loop is not.

Where bursts are frequent and predictable — an hourly upload window, an end-of-shift flush — the more durable answer is to size the transformation pool for the peak rather than the mean. Spatial transformation is CPU-bound and parallelises cleanly, so the pool size is the one parameter that directly converts hardware into headroom.
Every other knob adjusts how the work is scheduled; only the pool size changes how much work can be done.
Size the pool for the peak, and the batch parameters stop needing attention.
That is the whole of the capacity conversation for a spatial stream.
