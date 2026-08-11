# Writing Kafka Geospatial Streams to Iceberg

This guide is a complete, runnable consumer that reads geospatial events from Kafka, validates and enriches them, and commits them to an Iceberg table with batched commits and offsets recorded in the table itself.

## Context and prerequisites

The recipe uses `confluent-kafka` and PyIceberg 0.7+ with a REST catalog, and runs as a plain Python process — no cluster. It implements the shape described in [streaming spatial ingestion](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/streaming-spatial-ingestion/): parallel transformation, sequential batched commits, and offsets stored with the data. The validation it applies is the gate from [geometry validation and repair](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/geometry-validation-and-repair/).

## The flow

<figure class="diagram">
<svg viewBox="0 0 768 250" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Kafka partitions consumed into an in-process buffer, transformed in a worker pool, accumulated into a commit batch, and published as one Iceberg commit carrying the source offsets">
<defs>
<marker id="kis-flow-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#2f6e49"/></marker>
</defs>
<rect x="0" y="0" width="768" height="250" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Offsets travel with the data, not beside it</text>
<rect x="24" y="70" width="140" height="80" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="94" y="100" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">Kafka</text>
<text x="94" y="124" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">N partitions</text>
<rect x="196" y="70" width="150" height="80" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="271" y="100" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">worker pool</text>
<text x="271" y="124" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">validate + derive</text>
<rect x="378" y="70" width="150" height="80" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="453" y="100" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">commit batch</text>
<text x="453" y="124" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">bytes or time trigger</text>
<rect x="560" y="70" width="196" height="80" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="658" y="100" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">one Iceberg commit</text>
<text x="658" y="124" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">data + offsets together</text>
<line x1="164" y1="110" x2="196" y2="110" stroke="#2f6e49" stroke-width="2" marker-end="url(#kis-flow-arrow)"/>
<line x1="346" y1="110" x2="378" y2="110" stroke="#2f6e49" stroke-width="2" marker-end="url(#kis-flow-arrow)"/>
<line x1="528" y1="110" x2="560" y2="110" stroke="#2f6e49" stroke-width="2" marker-end="url(#kis-flow-arrow)"/>
<text x="390" y="206" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">On restart, resume from the offsets the table reports — not from the consumer group</text>
<text x="390" y="234" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">That single choice is what makes the pipeline restartable without gaps or duplicates</text>
</svg>
</figure>

## Complete working solution

```python
import json, time
from concurrent.futures import ProcessPoolExecutor
from confluent_kafka import Consumer, TopicPartition
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from shapely import from_wkb, to_wkb, is_valid, make_valid, set_precision, bounds

TOPIC          = "vehicle.positions"
TABLE          = "spatial.telemetry"
COMMIT_BYTES   = 128 * 1024 * 1024      # publish when the batch reaches this
COMMIT_SECONDS = 120                     # …or when this much time has passed
SCHEMA = pa.schema([
    ("asset_id", pa.int64()), ("event_ts", pa.timestamp("us", tz="UTC")),
    ("h3_r5", pa.int64()),
    ("bbox_min_x", pa.float64()), ("bbox_min_y", pa.float64()),
    ("bbox_max_x", pa.float64()), ("bbox_max_y", pa.float64()),
    ("geometry", pa.binary()),
])

def transform(raw_messages: list[bytes]) -> list[dict]:
    """Runs in a worker process. CPU-bound: decode, validate, derive."""
    out = []
    for payload in raw_messages:
        rec = json.loads(payload)
        geom = from_wkb(bytes.fromhex(rec["geom_hex"]))
        geom = set_precision(geom, 1e-9)
        if not is_valid(geom):
            geom = make_valid(geom)
        minx, miny, maxx, maxy = bounds(geom)
        if abs(minx) > 180 or abs(miny) > 90:
            continue                       # coordinate-range assertion
        out.append({
            "asset_id": rec["asset_id"],
            "event_ts": rec["ts"],
            "h3_r5": rec["h3_r5"],
            "bbox_min_x": minx, "bbox_min_y": miny,
            "bbox_max_x": maxx, "bbox_max_y": maxy,
            "geometry": to_wkb(geom),
        })
    return out

def resume_offsets(table) -> dict[int, int]:
    """The table is the source of truth about what has been ingested."""
    raw = table.properties.get("kafka.offsets")
    return {int(k): int(v) for k, v in json.loads(raw).items()} if raw else {}

def run():
    catalog = load_catalog("prod")
    table = catalog.load_table(TABLE)

    consumer = Consumer({
        "bootstrap.servers": "kafka:9092",
        "group.id": "spatial-ingest",
        "enable.auto.commit": False,          # the table owns the offsets
        "auto.offset.reset": "earliest",
    })
    stored = resume_offsets(table)
    parts = [TopicPartition(TOPIC, p, o + 1) for p, o in stored.items()]
    consumer.assign(parts) if parts else consumer.subscribe([TOPIC])

    pool = ProcessPoolExecutor(max_workers=8)
    buffer, pending_offsets, buffered_bytes = [], {}, 0
    last_commit = time.monotonic()

    while True:
        msgs = consumer.consume(num_messages=5000, timeout=1.0)
        if msgs:
            payloads = [m.value() for m in msgs if not m.error()]
            for m in msgs:
                if not m.error():
                    pending_offsets[m.partition()] = m.offset()
            rows = pool.submit(transform, payloads).result()
            buffer.extend(rows)
            buffered_bytes += sum(len(r["geometry"]) for r in rows)

        due = (buffered_bytes >= COMMIT_BYTES
               or time.monotonic() - last_commit >= COMMIT_SECONDS)
        if due and buffer:
            batch = pa.Table.from_pylist(buffer, schema=SCHEMA)
            table = catalog.load_table(TABLE)        # refresh before committing
            table.append(batch)
            with table.transaction() as tx:
                tx.set_properties({"kafka.offsets": json.dumps(pending_offsets)})
            buffer, buffered_bytes = [], 0
            last_commit = time.monotonic()
```

## Step-by-step walkthrough

1. **Disable auto-commit and own the offsets.** The consumer group's offsets and the table's contents can diverge; making the table authoritative removes the divergence entirely, at the cost of one table property.

2. **Transform in worker processes.** Geometry decode, validation and derivation are CPU-bound Python, so threads give nothing. A pool sized to the available cores is where the throughput comes from.

3. **Trigger commits on bytes or time, whichever comes first.** Bytes bound the memory and produce well-sized files; the time bound guarantees freshness when volume is low, so a quiet period does not leave data uncommitted for hours.

4. **Refresh the table before appending.** Another writer — a backfill, a maintenance job — may have committed since the last load, and appending from a stale table object risks a conflict that a refresh avoids.

5. **Write offsets in the same transaction as the data where the catalog allows it.** Where it does not, write them immediately after and accept a small window in which a crash causes a replay of one batch — which the deduplication described below makes harmless.

## Common errors and fixes

| Symptom | Cause | Fix |
|---|---|---|
| Duplicates after a restart | Offsets committed before the data | Always commit data first, offsets second |
| Gaps after a restart | Consumer group offsets used instead of the table's | Resume from the table property |
| Memory grows until the process is killed | Buffer bounded by rows, not bytes | Accumulate serialised bytes and trigger on them |
| Thousands of tiny files by morning | Commit interval far too short | Raise it; one to five minutes suits most workloads |
| Throughput does not improve with more workers | Transformation on threads, not processes | Use a process pool; the work is CPU-bound |
| Commit conflicts under load | Several instances writing concurrently | One writer per table, or route through a coordinator |

## Verification

<figure class="diagram">
<svg viewBox="0 0 764 210" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three restart tests for the consumer: clean restart resumes without gaps, crash between data and offsets replays one batch, and a duplicate instance is prevented from writing concurrently">
<rect x="0" y="0" width="764" height="210" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Three restart scenarios worth testing deliberately</text>
<rect x="26" y="58" width="230" height="140" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="141" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">clean restart</text>
<text x="141" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">stop, start</text>
<text x="141" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">expect: no gap,</text>
<text x="141" y="166" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">no duplicate</text>
<rect x="274" y="58" width="230" height="140" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="389" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">crash mid-commit</text>
<text x="389" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">kill between the two writes</text>
<text x="389" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">expect: one batch replayed,</text>
<text x="389" y="166" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">never a gap</text>
<rect x="522" y="58" width="230" height="140" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="637" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">duplicate instance</text>
<text x="637" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">two consumers, one table</text>
<text x="637" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">expect: conflicts, or a</text>
<text x="637" y="166" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">lock that prevents it</text>
</svg>
</figure>

```python
def test_no_gap_after_restart(table, topic_end_offsets):
    stored = resume_offsets(table)
    for partition, end in topic_end_offsets.items():
        assert partition in stored, f"partition {partition} never committed"
        assert stored[partition] <= end
    # And the row count must equal the number of accepted messages.
    assert table.scan().count() == expected_accepted_count()
```

The middle scenario is the one worth exercising deliberately, because its correct outcome — a replayed batch — is only acceptable if duplicates are tolerable or removed. Decide which, test it, and record the decision; a pipeline that is "exactly once" because nobody has tested a crash is a pipeline whose semantics are unknown.

Run a scheduled duplicate check as a backstop: a count of rows sharing an asset identifier and timestamp, per day, alerting on a non-zero result. It costs one aggregate and it turns an assumption into a measurement.

## Scaling Beyond One Process

A single consumer process handles a great deal, and when it stops doing so the scaling path is well-defined rather than a rewrite.

<figure class="diagram">
<svg viewBox="0 0 768 264" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Scaling a Kafka spatial consumer: first add worker processes within one consumer, then shard consumers by partition with a single commit coordinator, and only then move to a cluster runtime">
<defs>
<marker id="kis-scale-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#0e6e7d"/></marker>
</defs>
<rect x="0" y="0" width="768" height="264" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Three steps, in this order</text>
<rect x="24" y="70" width="212" height="112" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="130" y="100" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">1. more workers</text>
<text x="130" y="126" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">same process, bigger pool</text>
<text x="130" y="152" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">free; scales to core count</text>
<rect x="284" y="70" width="212" height="112" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="390" y="100" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">2. shard consumers</text>
<text x="390" y="126" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">one per partition group</text>
<text x="390" y="152" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">needs a commit coordinator</text>
<rect x="544" y="70" width="212" height="112" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="650" y="100" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">3. cluster runtime</text>
<text x="650" y="126" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">structured streaming</text>
<text x="650" y="152" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">when sharding is not enough</text>
<line x1="236" y1="126" x2="284" y2="126" stroke="#0e6e7d" stroke-width="2" marker-end="url(#kis-scale-arrow)"/>
<line x1="496" y1="126" x2="544" y2="126" stroke="#0e6e7d" stroke-width="2" marker-end="url(#kis-scale-arrow)"/>
<text x="390" y="222" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">Step two is where the offset design pays for itself</text>
<text x="390" y="248" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">Per-partition offsets shard naturally; a single opaque checkpoint does not</text>
</svg>
</figure>

Step two requires one addition: a commit coordinator, because several consumers appending independently will conflict. The consumers write data files and hand the file lists plus their offsets to the coordinator, which publishes one commit covering all of them. The offsets map already being per-partition is what makes merging them trivial.

Step three is a genuine change of runtime and should be reached only when sharding has been exhausted. In practice a sharded set of plain consumers handles very large volumes, and the operational simplicity of a process with no cluster is worth preserving for as long as the throughput allows.

## Schema Handling on the Wire

One detail specific to spatial payloads is worth settling early: how geometry arrives in the message.

Hex-encoded WKB in JSON, as used above, is the most interoperable and the least efficient — it inflates the geometry by a factor of two and costs a hex decode per record. It is a reasonable default for moderate volumes and for streams whose producers are diverse.

Binary WKB in a schema-registered format — Avro or Protobuf with a bytes field — removes both costs and is the right choice at volume. The producer and consumer share a registered schema, the geometry travels as raw bytes, and the decode is a memory view rather than a parse.

What should be avoided is GeoJSON in the message body. It is three to six times larger than WKB, it parses slowly, and its `properties` object is an open map that will acquire fields nobody declared. Where producers emit it, converting at the edge — a small transformation service between the producer and the topic the consumer reads — costs little and keeps the ingest path fast.

Whichever encoding is used, carry the coordinate reference system in the message or in the topic's schema rather than assuming it. A producer that changes its projection is the most common upstream failure in spatial streaming, and a declared SRID field turns it into a rejected batch rather than a silent corruption.

## Operating It

Four things to have in place before this consumer runs unattended.

**A lag metric with a trend.** Consumer lag alone is ambiguous — a lag of five minutes may be a burst being absorbed or a pipeline falling behind. The derivative distinguishes them, and it is the only signal worth alerting on.

**A dead-letter path.** Records that fail validation must go somewhere queryable, with the reason and the source offset attached. Dropping them silently means an upstream change is invisible; failing the batch means one bad record halts the stream. A dead-letter topic or table is the third option and the right one.

**A single-writer guarantee.** Two instances of this consumer against one table will conflict, and the conflicts will look like intermittent failures rather than like a deployment problem. Enforce it with a lock, a scheduler constraint, or a partition assignment that makes overlap impossible.

**A compaction schedule that is already running.** This consumer produces well-formed but unsorted files by design, at whatever rate the commit interval implies. Without the compactor described in [lakehouse maintenance automation](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/lakehouse-maintenance-automation/), the table degrades on a predictable schedule and the degradation will be blamed on the consumer.

None of the four is difficult, and all four are easier to arrange before the first production run than after the first incident.
Arranging them in advance takes an afternoon and removes the four most common ways this pipeline fails in its first month.
None of them is subtle; all of them are silent until they are not.
