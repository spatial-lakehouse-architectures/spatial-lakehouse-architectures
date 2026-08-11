# Idempotent Spatial Ingestion With Exactly-Once Commits

This guide makes a spatial ingest pipeline safe to retry: a batch that is processed twice produces the same table state as one processed once, and a crash at any point leaves neither gaps nor duplicates.

## Context and prerequisites

Every ingestion pipeline is retried eventually — by an orchestrator, by a restart, by an operator re-running a failed day. Whether that is harmless depends on decisions made in the write path. This recipe uses PyIceberg 0.7+ and applies equally to Delta; the streaming context is in [streaming spatial ingestion](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/streaming-spatial-ingestion/), and the concurrency model in [async execution patterns](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/async-execution-patterns/).

## What the table format gives you, and what it does not

<figure class="diagram">
<svg viewBox="0 0 762 268" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Guarantees provided by the table format — atomic commits, invisible uncommitted files and snapshot isolation — against the guarantees the pipeline must add: idempotent replay and source position tracking">
<rect x="0" y="0" width="762" height="268" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Half the problem is already solved; the other half is yours</text>
<rect x="30" y="56" width="352" height="200" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="206" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#2f6e49">the format guarantees</text>
<text x="206" y="118" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">a commit is atomic — all or nothing</text>
<text x="206" y="146" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">uncommitted files are invisible</text>
<text x="206" y="174" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">readers see a consistent snapshot</text>
<text x="206" y="202" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">a failed write leaves only orphans</text>
<text x="206" y="232" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">so a crash never corrupts the table</text>
<rect x="398" y="56" width="352" height="200" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="574" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#9a5a17">you must guarantee</text>
<text x="574" y="118" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">a replay produces the same state</text>
<text x="574" y="146" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">the source position is recoverable</text>
<text x="574" y="174" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">derived values are deterministic</text>
<text x="574" y="202" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">orphans are eventually reclaimed</text>
<text x="574" y="232" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">so a retry is safe rather than merely survivable</text>
</svg>
</figure>

The right-hand column contains one item specific to spatial data: **derived values must be deterministic**. A pipeline that assigns a grid cell using one library version and reprocesses with another produces different partition values for the same input, which turns an idempotent replay into a duplicate with a different key. Pinning the derivation and recording its version, as described throughout this section, is a prerequisite for idempotency rather than a separate concern.

## Complete working solution

```python
import json
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo

BATCH_PROPERTY = "ingest.completed_batches"

def already_ingested(table, batch_id: str) -> bool:
    done = json.loads(table.properties.get(BATCH_PROPERTY, "[]"))
    return batch_id in done

def ingest_batch(catalog_name: str, identifier: str, batch_id: str,
                 arrow_table, partition_value) -> str:
    """Idempotent by construction: a replayed batch_id is a no-op."""
    catalog = load_catalog(catalog_name)
    table = catalog.load_table(identifier)

    if already_ingested(table, batch_id):
        return "skipped"

    with table.transaction() as tx:
        # Overwrite rather than append: replacing this batch's partition slice
        # makes a partial previous attempt irrelevant.
        tx.overwrite(arrow_table,
                     overwrite_filter=EqualTo("ingest_batch_id", batch_id))
        done = json.loads(table.properties.get(BATCH_PROPERTY, "[]"))
        done.append(batch_id)
        tx.set_properties({BATCH_PROPERTY: json.dumps(done[-500:])})

    return "committed"
```

```python
# The batch identifier must be derived from the input, not generated.
import hashlib

def batch_id_for(source: str, partition: str, offsets: tuple[int, int]) -> str:
    key = f"{source}:{partition}:{offsets[0]}-{offsets[1]}"
    return hashlib.sha256(key.encode()).hexdigest()[:32]
```

## Step-by-step walkthrough

1. **Derive the batch identifier from the input.** A generated identifier — a timestamp, a UUID — is different on every attempt, so a replay looks like new work. Hashing the source, the partition and the offset range makes the identifier a function of what is being ingested, which is what makes the replay recognisable.

2. **Carry the identifier as a column.** Storing `ingest_batch_id` on every row is what allows the overwrite filter to replace exactly the rows a previous attempt wrote. Without it, a partial attempt's rows cannot be identified and must be tolerated as duplicates.

3. **Overwrite rather than append.** An append after a partial failure adds the successfully-written rows a second time. An overwrite scoped to the batch identifier replaces whatever the previous attempt managed to write, which makes the operation idempotent regardless of where it failed.

4. **Record completion in the same transaction.** The property update and the data write must commit together, or a crash between them re-runs a batch that was already applied — which the overwrite makes harmless, but which wastes the work.

5. **Bound the completed-batch list.** Keeping the last few hundred identifiers is sufficient to catch realistic replays and keeps the property small. An unbounded list grows into the metadata and slows every commit.

## Common errors and fixes

| Symptom | Cause | Fix |
|---|---|---|
| Duplicates after a retry | Append used instead of a scoped overwrite | Overwrite on the batch identifier |
| Retry does nothing but data is missing | Completion recorded before the data committed | Record completion inside the same transaction |
| Batch identifiers never match on replay | Identifier generated rather than derived | Hash the source, partition and offsets |
| Partition values differ between attempts | Grid library version changed | Pin the derivation and record its version |
| Commit property grows unboundedly | Full history of batch identifiers retained | Keep a bounded window |

## Where the guarantee ends

<figure class="diagram">
<svg viewBox="0 0 764 222" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three boundaries of the idempotency guarantee: non deterministic transforms, side effects outside the table, and downstream consumers that have already read a partial state">
<rect x="0" y="0" width="764" height="222" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Three things a scoped overwrite does not fix</text>
<rect x="26" y="58" width="230" height="152" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="141" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">non-deterministic transforms</text>
<text x="141" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">a timestamp, a random salt</text>
<text x="141" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">the replay writes different rows</text>
<text x="141" y="172" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">make the transform pure</text>
<rect x="274" y="58" width="230" height="152" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="389" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">side effects</text>
<text x="389" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">a webhook, an email, a queue</text>
<text x="389" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">fired twice on a replay</text>
<text x="389" y="172" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">move them after the commit</text>
<rect x="522" y="58" width="230" height="152" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="637" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">downstream reads</text>
<text x="637" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">a consumer read the old state</text>
<text x="637" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">and acted on it</text>
<text x="637" y="172" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#6a3d9a">consumers need their own idempotency</text>
</svg>
</figure>

The middle case is the one that catches teams who have done everything else correctly. A pipeline that commits and then publishes a notification will publish twice on a replay, because the notification is outside the transaction. Moving side effects strictly after the commit, and making them idempotent in their own right, is the only reliable arrangement — and the alternative, attempting to include them in the transaction, is not available.

## Verification

```python
def test_replay_is_a_noop(catalog, identifier, batch, batch_id):
    first  = ingest_batch(catalog, identifier, batch_id, batch, "2026-03-11")
    count1 = load_catalog(catalog).load_table(identifier).scan().count()

    second = ingest_batch(catalog, identifier, batch_id, batch, "2026-03-11")
    count2 = load_catalog(catalog).load_table(identifier).scan().count()

    assert first == "committed" and second == "skipped"
    assert count1 == count2, "replay changed the row count"

def test_partial_failure_then_retry(catalog, identifier, batch, batch_id):
    write_files_without_committing(batch)          # simulate a crash mid-write
    result = ingest_batch(catalog, identifier, batch_id, batch, "2026-03-11")
    assert result == "committed"
    assert no_duplicate_rows(identifier, batch_id)
```

The second test is the one worth running against a real table rather than a mock, because it exercises the interaction between orphaned files and the scoped overwrite — which is the part of the design most likely to be subtly wrong and the part that only manifests during an incident.

Schedule the orphan cleanup that reclaims the files those failed attempts leave behind, with a retention margin longer than the longest possible in-flight write. Without it, an idempotent pipeline that retries regularly accumulates storage indefinitely, invisibly, because nothing lists files no snapshot references.

## Choosing the Batch Granularity

The batch identifier defines the unit of idempotency, and choosing it too coarse or too fine both cost something.

<figure class="diagram">
<svg viewBox="0 0 764 222" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three granularities for the idempotency unit: per source file, per offset range and per partition day, with the retry cost and bookkeeping overhead of each">
<rect x="0" y="0" width="764" height="222" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Granularity trades retry cost against bookkeeping</text>
<rect x="26" y="58" width="230" height="152" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="141" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">per source file</text>
<text x="141" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">natural for batch ingest</text>
<text x="141" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">retry cost: one file</text>
<text x="141" y="172" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">identifiers: many, but bounded</text>
<rect x="274" y="58" width="230" height="152" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="389" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">per offset range</text>
<text x="389" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">natural for streaming</text>
<text x="389" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">retry cost: one commit batch</text>
<text x="389" y="172" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">the usual choice</text>
<rect x="522" y="58" width="230" height="152" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="637" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">per partition day</text>
<text x="637" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">coarse and simple</text>
<text x="637" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">retry cost: a whole day</text>
<text x="637" y="172" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">only for daily batch loads</text>
</svg>
</figure>

The right-hand option is attractive because it needs no batch identifier at all — the partition itself is the unit, and an overwrite of the partition is inherently idempotent. It is the correct choice for a daily batch load where reprocessing a day is cheap and expected. It is the wrong choice for streaming, where reprocessing a day to recover from a two-minute failure is absurd.

The middle option is the streaming default and works well, provided the offset range is genuinely reproducible. Where the source does not provide stable offsets — some queue systems do not — the fallback is a content hash of the batch, which is more expensive to compute and equally deterministic.

Whichever granularity is used, keep the identifier as a column on the data rather than only in the table properties. The property tells you a batch was completed; the column tells you which rows it wrote, and only the second supports the scoped overwrite that makes the retry safe.

## Interaction With Compaction

One subtlety deserves stating because it surprises people: compaction rewrites files, and a compacted table no longer has the file boundaries the original batches wrote.

This is harmless for the design above, because the scoped overwrite filters on a **column value** rather than on file identity. A batch's rows remain identifiable after any number of rewrites, so a replay months later still replaces exactly the right rows.

It would not be harmless for a design that tracked which files a batch produced and deleted them on replay — an approach that seems simpler and breaks the first time the compactor runs. That is the main argument for the column: it survives every physical reorganisation the table will undergo.

The one cost is that the batch identifier column persists in the table indefinitely, adding a small string per row. Where that matters, it can be dropped from partitions older than the replay window, since a batch from last year will never be replayed — which is a scoped rewrite that the compactor can perform as part of its ordinary work.

## A Practical Summary

Idempotent spatial ingestion reduces to five decisions, and a pipeline that has made all five is safe to retry from any point.

Derive the batch identifier from the input rather than generating it, so a replay is recognisable. Carry it as a column, so the rows a previous attempt wrote can be replaced rather than merely tolerated. Overwrite scoped to that identifier rather than appending, so a partial attempt is irrelevant. Record completion inside the same transaction as the data, so the two cannot diverge. And keep every derivation deterministic — pinned libraries, recorded versions, no timestamps or randomness in the transform — so the replay produces the same rows rather than equivalent ones.

The spatial-specific item is the last, and it is the one most often overlooked because it does not look like a durability concern. A grid cell derived from a library that changed between attempts, or a validation repair that behaves differently under a new GEOS version, produces a replay that writes *different* data under the same batch identifier — which is a subtler failure than a duplicate and considerably harder to notice.

Test the replay path deliberately, on a real table, including a crash between the data write and the completion record. It is a ten-minute exercise, it is the only way to know the guarantee holds, and it is invariably more informative than reasoning about it.
