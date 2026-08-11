# Removing Orphan Files From Spatial Tables

This guide reclaims the storage left behind by failed writes and superseded compactions, with a safety margin derived from the platform's own longest-running job rather than from a default that will eventually delete a live file.

## Context and prerequisites

Orphan files are objects in a table's storage location that no snapshot references: the output of a write that failed before committing, or of a compaction that was superseded. They are invisible to every query and to snapshot expiry, and on a spatial table with large geometry payloads they accumulate quickly. This recipe uses PyIceberg 0.7+ and Spark's Iceberg procedures; the scheduling context is in [lakehouse maintenance automation](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/lakehouse-maintenance-automation/).

## Why orphans accumulate faster on spatial tables

<figure class="diagram">
<svg viewBox="0 0 764 264" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three sources of orphan files on a spatial table: failed writes that had already produced large geometry files, superseded compaction output, and cancelled long running rewrites">
<rect x="0" y="0" width="764" height="264" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Three sources, all amplified by geometry size</text>
<rect x="26" y="56" width="230" height="196" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="141" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">failed writes</text>
<text x="141" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">files written, commit never made</text>
<text x="141" y="146" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">a spatial batch is large,</text>
<text x="141" y="168" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">so each failure leaves more</text>
<text x="141" y="200" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">a retry loop leaves a lot</text>
<text x="141" y="228" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">the most common source</text>
<rect x="274" y="56" width="230" height="196" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="389" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">superseded compaction</text>
<text x="389" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">a rewrite lost a commit race</text>
<text x="389" y="146" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">its output is complete</text>
<text x="389" y="168" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">and referenced by nothing</text>
<text x="389" y="200" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">a whole partition&#8217;s worth</text>
<text x="389" y="228" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">large, and easy to miss</text>
<rect x="522" y="56" width="230" height="196" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="637" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">cancelled rewrites</text>
<text x="637" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">a timeout on a long job</text>
<text x="637" y="146" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">reprojections and sort</text>
<text x="637" y="168" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">rewrites run for hours</text>
<text x="637" y="200" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">and produce a lot before failing</text>
<text x="637" y="228" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">rare but very large</text>
</svg>
</figure>

The amplification is straightforward: a spatial batch carries geometry, so the same number of failed writes leaves an order of magnitude more bytes than an equivalent scalar pipeline. A table whose ingest retries a few times a day can accumulate terabytes over a year without any query noticing, because nothing lists files no snapshot references.

## Complete working solution

```python
from datetime import datetime, timedelta, timezone

SAFETY_HOURS = 72          # must exceed the longest possible in-flight write

def orphan_report(spark, identifier: str) -> dict:
    """Dry run first, always. Reports what would be deleted."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=SAFETY_HOURS)
    rows = spark.sql(f"""
        CALL lakehouse.system.remove_orphan_files(
            table       => '{identifier}',
            older_than  => TIMESTAMP '{cutoff.strftime('%Y-%m-%d %H:%M:%S')}',
            dry_run     => true)
    """).collect()
    paths = [r["orphan_file_location"] for r in rows]
    return {"count": len(paths), "sample": paths[:20]}

def remove_orphans(spark, identifier: str, expected_max: int = 5000) -> int:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=SAFETY_HOURS)
    report = orphan_report(spark, identifier)
    if report["count"] > expected_max:
        raise RuntimeError(
            f"{report['count']} orphans — far above the expected maximum. "
            "Investigate before deleting; this may indicate a misconfigured "
            "storage location rather than genuine orphans.")
    spark.sql(f"""
        CALL lakehouse.system.remove_orphan_files(
            table       => '{identifier}',
            older_than  => TIMESTAMP '{cutoff.strftime('%Y-%m-%d %H:%M:%S')}')
    """)
    return report["count"]
```

## Step-by-step walkthrough

1. **Derive the safety margin from the longest job, not from a default.** The margin must exceed the duration of any write that could currently be in flight. On a spatial platform the longest is usually a reprojection or a full sort rewrite, and those run for hours. Seventy-two hours is a defensible default; anything under twenty-four needs a specific justification.

2. **Always dry-run first.** The procedure reports what it would delete without deleting it, and the report is the only chance to notice that something is wrong before the deletion is irreversible.

3. **Guard on the count.** An unexpectedly large orphan list is the signature of a misconfigured storage location — the procedure comparing the table's files against a directory that also contains another table's data, for instance. Deleting in that situation destroys live data belonging to something else.

4. **Never disable the retention check.** Where the procedure offers a flag to ignore the minimum age, it exists for tests. Using it in production is the single most destructive maintenance operation available on a lakehouse.

5. **Run it after snapshot expiry, not before.** Expiry releases the files that compaction superseded; running orphan removal first reclaims nothing from that source and the run is largely wasted.

## Common errors and fixes

| Symptom | Cause | Fix |
|---|---|---|
| Nothing is reclaimed | Run before snapshot expiry | Order the maintenance: compact, expire, then remove orphans |
| A live file was deleted | Margin shorter than an in-flight write | Raise the margin above the longest job; restore from a backup |
| Orphan count is enormous | Storage location shared with another table | Give each table its own prefix; investigate before deleting |
| The procedure is very slow | Listing millions of objects | Scope by partition prefix where the procedure supports it |
| Storage does not fall after removal | Object versioning or soft delete enabled | Check the bucket's lifecycle configuration too |

## What the margin protects

<figure class="diagram">
<svg viewBox="0 0 732 260" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="A long running rewrite writing files over several hours before committing, with a short safety margin cutting into that window and deleting files the pending commit will reference">
<rect x="0" y="0" width="732" height="260" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">The margin must cover the whole write, not the average one</text>
<line x1="70" y1="120" x2="720" y2="120" stroke="#cfe3e7" stroke-width="6" stroke-linecap="round"/>
<rect x="150" y="98" width="330" height="44" rx="6" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="315" y="126" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">a 6-hour sort rewrite: writing files</text>
<circle cx="480" cy="120" r="10" fill="#2f6e49"/>
<text x="480" y="88" text-anchor="middle" font-family="sans-serif" font-size="11" font-weight="700" fill="#2f6e49">commit</text>
<rect x="540" y="168" width="180" height="44" rx="6" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="630" y="196" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">a 2-hour margin</text>
<line x1="540" y1="168" x2="540" y2="120" stroke="#9a5a17" stroke-width="2" stroke-dasharray="5 4"/>
<text x="315" y="176" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">files written here are older than a 2-hour margin</text>
<text x="315" y="198" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">and would be deleted before the commit lands</text>
<text x="390" y="244" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">A margin below the longest write is a scheduled data-loss event</text>
</svg>
</figure>

The failure this produces is not subtle and is not recoverable without a restore: the rewrite commits successfully, referencing files that were deleted minutes earlier, and every subsequent read of that partition fails with a missing-file error. The table is broken and the only fix is to roll back to a prior snapshot.

Because the consequence is so severe and the saving from a shorter margin so small, the correct bias is strongly toward a generous margin. Storage held for an extra two days costs a rounding error; a broken table costs an incident.

## Verification

```python
def verify_removal(spark, identifier: str, before_bytes: int) -> dict:
    after = spark.sql(f"""
        SELECT sum(file_size_in_bytes) AS b FROM {identifier}.files
    """).collect()[0]["b"]
    storage = measure_storage_prefix(identifier)      # from the object store
    return {
        "referenced_bytes": after,
        "storage_bytes": storage,
        "unreferenced_bytes": storage - after,
        "reclaimed_bytes": before_bytes - storage,
    }
```

The useful ongoing metric is `unreferenced_bytes` — the gap between what the table references and what the storage prefix holds. A healthy table's gap is small and stable; a growing gap means orphans are accumulating faster than they are removed, which points at a retry loop somewhere upstream.

Track it per table alongside the other observability metrics, and treat a rising trend as a signal about the write path rather than about the cleanup schedule. Increasing the cleanup frequency treats the symptom; finding the job that fails and retries three times a night treats the cause.

## Scheduling It Safely

Orphan removal is the one maintenance operation that can destroy data, so its schedule deserves more care than the others.

<figure class="diagram">
<svg viewBox="0 0 762 242" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="A safe schedule for orphan removal: run weekly rather than nightly, always after snapshot expiry, always with a dry run first, and never concurrently with a long rewrite">
<rect x="0" y="0" width="762" height="242" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Four scheduling rules</text>
<rect x="30" y="58" width="352" height="80" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="206" y="86" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">weekly, not nightly</text>
<text x="206" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">the storage saving does not need daily attention</text>
<rect x="398" y="58" width="352" height="80" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="574" y="86" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">after expiry, always</text>
<text x="574" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">or the superseded files stay referenced</text>
<rect x="30" y="150" width="352" height="80" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="206" y="178" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">dry run, then delete</text>
<text x="206" y="204" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">with a guard on an unexpected count</text>
<rect x="398" y="150" width="352" height="80" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="574" y="178" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">never during a rewrite</text>
<text x="574" y="204" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">check for running jobs before starting</text>
</svg>
</figure>

The last rule deserves implementation rather than intention. A scheduler that starts orphan removal at 02:00 regardless of what else is running will eventually overlap with a long backfill, and the safety margin is the only thing preventing a problem. Checking for running maintenance jobs before starting — a query against the job history, or a shared lock — removes the dependence on the margin being generous enough.

Weekly is sufficient because the storage recovered does not accumulate fast enough to justify daily attention, and each run is a chance for something to go wrong. The exception is a table whose ingest is known to fail and retry frequently; there the orphan rate is high, and the right fix is upstream rather than a more aggressive cleanup.

## The Bucket-Level Complement

Orphan removal handles files inside the table's location that no snapshot references. Two other categories of waste live outside its reach and need a bucket lifecycle rule instead.

**Staging and temporary prefixes.** Many write paths stage output before moving or registering it, and an aborted write leaves partial objects there. The table's cleanup never looks at that prefix, so nothing removes them. A lifecycle rule expiring objects older than a few days is the whole solution.

**Multipart upload fragments.** An interrupted upload leaves parts that are billed and are invisible to ordinary object listings. On a spatial platform writing large geometry files, these can be substantial. Every major object store offers a lifecycle rule to abort incomplete multipart uploads after a set period; enabling it is a one-line configuration that many platforms have never done.

Both are worth checking once, because both accumulate silently and neither is visible in any table-level metric. A storage audit comparing the bucket's total size against the sum of every table's referenced bytes will reveal the gap, and the two rules above usually explain most of it.

## A Worked Example of the Gap

The arithmetic is worth doing once on a real platform, because the result is usually larger than anyone expects and it makes the case for the schedule without argument.

Take a streaming spatial table ingesting at a modest rate, whose write fails and retries twice a day on average — a rate nobody would consider alarming. Each failed attempt has typically written most of a commit batch before failing, so each leaves on the order of a hundred megabytes of geometry files behind. That is 200 MB a day, 6 GB a month, and 70 GB a year, from one table, with no visible symptom at any point.

Add the compaction races. A sort rewrite that loses a commit conflict discards its entire output, which for a day's partition can be several gigabytes. On a busy table this happens perhaps weekly, adding another 100 GB or so annually.

Across a platform of a hundred spatial tables, the total reaches the tens of terabytes — a cost that appears on the storage invoice with no line item explaining it, and that grows monotonically because nothing removes it. A weekly cleanup with a generous safety margin recovers all of it and takes minutes to run.

Measure the gap on one table before scheduling anything. Comparing the storage prefix's size against the sum of the table's referenced file sizes takes two queries, and the number it produces is usually the whole business case.
It is also the fastest way to discover that the ingest retries far more often than anyone believed.
