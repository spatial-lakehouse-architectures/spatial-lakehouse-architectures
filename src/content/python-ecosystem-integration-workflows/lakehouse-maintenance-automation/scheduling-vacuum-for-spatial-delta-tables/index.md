# Scheduling VACUUM for Spatial Delta Tables

This guide provides a complete delta-rs recipe that schedules `OPTIMIZE` followed by `VACUUM` with a safe retention window on a spatial Delta table, guarding time-travel SLAs so background cleanup never deletes files an in-flight reader or rollback still needs.

## Context and prerequisites

Delta Lake accumulates two kinds of cruft on spatial workloads: many small data files from streaming geometry writes, and tombstoned files left behind by every `OPTIMIZE`, `MERGE`, or overwrite. `OPTIMIZE` with Z-order on the bounding-box columns fixes the first by compacting and spatially clustering; `VACUUM` fixes the second by physically deleting files no longer referenced by any retained version. The danger is that `VACUUM` is irreversible and time-travel-breaking — delete too aggressively and you corrupt a running scan or lose a rollback point. This is the Delta counterpart to [compacting spatial Iceberg tables with rewrite_data_files](/python-ecosystem-integration-workflows/lakehouse-maintenance-automation/compacting-spatial-iceberg-tables-with-rewrite-data-files/), and part of the broader [lakehouse maintenance automation](/python-ecosystem-integration-workflows/lakehouse-maintenance-automation/) discipline. It uses delta-rs (the Python `deltalake` package, no JVM) 0.18+, building on the write patterns in [delta-rs geometry processing](/python-ecosystem-integration-workflows/delta-rs-geometry-processing/).

## Complete working solution

This script computes a retention window from an explicit time-travel SLA, runs Z-order compaction on the bbox columns, then vacuums with that retention — refusing to proceed if the requested window is unsafe.

```python
from datetime import timedelta
from deltalake import DeltaTable

# --- policy inputs -------------------------------------------------
TIME_TRAVEL_SLA_HOURS = 168          # consumers may time-travel back 7 days
LONGEST_QUERY_HOURS = 6              # longest-running downstream scan
ROLLBACK_MARGIN_HOURS = 24          # safety buffer for manual rollback
BBOX_ZORDER = ["bbox_minx", "bbox_miny", "bbox_maxx", "bbox_maxy"]

def safe_retention_hours() -> int:
    """Retention must exceed every consumer that could still need old files."""
    return max(TIME_TRAVEL_SLA_HOURS,
               LONGEST_QUERY_HOURS + ROLLBACK_MARGIN_HOURS)

def optimize_and_vacuum(table_uri: str,
                        storage_options: dict | None = None) -> None:
    dt = DeltaTable(table_uri, storage_options=storage_options)

    # 1. compact + spatially cluster with Z-order on the bounding box
    metrics = dt.optimize.z_order(
        BBOX_ZORDER,
        target_size=134_217_728,          # 128 MB output files
        max_concurrent_tasks=4,
    )
    print(f"optimize: {metrics['numFilesAdded']} added, "
          f"{metrics['numFilesRemoved']} removed")

    # 2. re-load so VACUUM sees the post-optimize log state
    dt = DeltaTable(table_uri, storage_options=storage_options)

    retention_h = safe_retention_hours()
    # dry-run first: list what WOULD be deleted, delete nothing
    to_delete = dt.vacuum(
        retention_hours=retention_h,
        dry_run=True,
        enforce_retention_duration=True,
    )
    print(f"vacuum dry-run: {len(to_delete)} files eligible "
          f"at {retention_h}h retention")

    # 3. real vacuum — retention floor enforced, no override
    deleted = dt.vacuum(
        retention_hours=retention_h,
        dry_run=False,
        enforce_retention_duration=True,
    )
    print(f"vacuum: deleted {len(deleted)} tombstoned files")

if __name__ == "__main__":
    optimize_and_vacuum(
        "s3://lakehouse/gis/parcels",
        storage_options={"AWS_REGION": "us-east-1"},
    )
```

## Step-by-step walkthrough

1. **`safe_retention_hours`** encodes the core safety rule as code: the vacuum window is the maximum of the time-travel SLA and the longest query plus a rollback margin. Any file younger than this may still be referenced by a running scan or needed for a rollback, so it must survive. Here that resolves to 168 hours (the SLA dominates).

2. **`dt.optimize.z_order(BBOX_ZORDER, ...)`** runs delta-rs's native Rust OPTIMIZE, compacting small files into ~128 MB outputs and interleaving them by the four bounding-box columns. Z-order clustering on the bbox is what makes subsequent file-skipping effective for spatial range queries — the same idea developed in [Z-ordering for geospatial queries](/spatial-partitioning-indexing-strategies/z-ordering-for-geospatial-queries/). `max_concurrent_tasks=4` bounds object-store parallelism to avoid throttling.

3. **Re-loading the `DeltaTable`** after optimize is important: OPTIMIZE writes a new commit that tombstones the pre-compaction files, and VACUUM must read that fresh log to know which files are now unreferenced.

4. **The dry-run vacuum** (`dry_run=True`) returns the list of files that *would* be deleted without touching storage. Logging this count in the schedule gives an auditable record and a chance to alert if the number is anomalously large.

5. **`enforce_retention_duration=True`** keeps delta-rs's built-in safety floor active. delta-rs refuses retention below the table's `deletedFileRetentionDuration` (default 168 hours) unless you explicitly disable this — which you should not do in automation.

6. **The real vacuum** physically deletes only tombstoned files older than the retention window and returns the deleted paths. Because retention respects the SLA, no time-travel target inside the window loses its files.

Scheduling this on a cadence — nightly via Airflow, or `on: schedule` in GitHub Actions — follows the same off-thread dispatch principle as the other [async execution patterns](/python-ecosystem-integration-workflows/async-execution-patterns/): the vacuum call is blocking, so run it in a worker, not on the scheduler heartbeat.

### Why the retention floor is not negotiable

The single most common way teams corrupt a Delta table is by lowering VACUUM retention to reclaim storage faster. The mechanism is subtle: Delta's transaction log records a version as a set of file references, and time travel to that version works only while those physical files still exist. VACUUM deletes any data file that is no longer referenced by the *current* version and is older than the retention window — but a file unreferenced by the current version may still be referenced by an older version a consumer is allowed to time-travel to. If retention is shorter than the time-travel SLA, VACUUM deletes exactly those files, and the next `load_as_version` against that target fails with a missing-file error that no amount of retrying will fix. The file is gone.

There is a second, faster failure: a long-running reader. Suppose a downstream Sedona job started a scan against version N, and while it runs, an `OPTIMIZE` produces version N+1 that tombstones some of N's files. If VACUUM then runs with a retention window shorter than the scan's duration, it deletes files the scan is still reading, and the scan dies mid-flight with a file-not-found error. This is why `safe_retention_hours` takes the maximum of the SLA and the longest query plus a margin — both consumers must be protected, and the stricter of the two wins. In practice the 168-hour default exists precisely because it comfortably covers both for most workloads; treat any pressure to lower it as a request to trade a storage-cost saving for a correctness risk, and push back accordingly.

If storage cost genuinely demands shorter retention, the correct lever is not `enforce_retention_duration=False` — it is lowering the *write frequency of tombstones* by compacting less often, or shortening the declared time-travel SLA with consumer sign-off so the floor can be lowered honestly. Never disable the guard in an unattended scheduled job.

### Wiring it into a schedule

The blocking optimize-and-vacuum entrypoint drops into an Airflow `PythonOperator` or a GitHub Actions cron step unchanged. Two scheduling rules matter: cap concurrency so two maintenance runs never overlap on the same table (an overlapping VACUUM can race an OPTIMIZE's tombstone accounting), and stagger OPTIMIZE and VACUUM cadences — compact hourly if writes are heavy, but vacuum only daily, because tombstones accumulate slowly and every vacuum is an irreversible sweep you want to run deliberately, not continuously.

```python
# GitHub Actions cron entrypoint: python -m maintenance.delta_vacuum
import os
if __name__ == "__main__":
    optimize_and_vacuum(
        os.environ["DELTA_TABLE_URI"],
        storage_options={"AWS_REGION": os.environ["AWS_REGION"]},
    )
```

## Common errors and fixes

| Error | Cause | Fix |
|---|---|---|
| `InvalidVacuumRetentionPeriod` / retention-too-short error | Requested `retention_hours` below the table's 168h safety floor | Raise retention above the SLA; never set `enforce_retention_duration=False` in scheduled jobs |
| Time-travel query fails with missing-file error after vacuum | Retention shorter than a consumer's time-travel SLA | Recompute `safe_retention_hours` from the true SLA; increase `deletedFileRetentionDuration` on the table |
| `OPTIMIZE` adds files but VACUUM deletes nothing | VACUUM run against a stale `DeltaTable` handle from before optimize | Re-instantiate `DeltaTable` after optimize so the new commit is visible |
| Vacuum deletes far more than expected | An overwrite or large MERGE tombstoned many files just outside retention | Confirm via dry-run before real run; treat a spike as an alert, not an auto-proceed |

## Verification

Confirm compaction reduced the active file set and vacuum reclaimed tombstones. Inspect the table's file and history state directly from delta-rs:

```python
from deltalake import DeltaTable

dt = DeltaTable("s3://lakehouse/gis/parcels",
                storage_options={"AWS_REGION": "us-east-1"})
print("active data files:", len(dt.files()))
print("current version:", dt.version())

# recent operations: OPTIMIZE then VACUUM END should be the latest entries
for entry in dt.history(5):
    print(entry.get("operation"), entry.get("operationParameters", {}))
```

Verify the time-travel SLA still holds by loading a version at the retention boundary and confirming it reads without error:

```python
# load the table as of 7 days ago; must still resolve after vacuum
dt_old = DeltaTable("s3://lakehouse/gis/parcels",
                    storage_options={"AWS_REGION": "us-east-1"})
dt_old.load_as_version(dt_old.version())   # or load_with_datetime(...)
print("time-travel target still readable:", len(dt_old.files()), "files")
```

<figure class="diagram">
<svg viewBox="0 0 760 250" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Delta OPTIMIZE then VACUUM with a retention window protecting recent versions">
<defs>
<marker id="arw-delta-vac" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#0e6e7d"/></marker>
</defs>
<rect x="0" y="0" width="760" height="250" fill="#f7fbfc"/>
<text x="380" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">OPTIMIZE then VACUUM under a retention floor</text>
<rect x="30" y="70" width="150" height="60" rx="8" fill="#ffffff" stroke="#9a5a17" stroke-width="2"/>
<text x="105" y="95" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="600" fill="#0d3b45">small bbox files</text>
<text x="105" y="113" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">+ tombstones</text>
<rect x="235" y="70" width="160" height="60" rx="8" fill="#ffffff" stroke="#6a3d9a" stroke-width="2"/>
<text x="315" y="95" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="600" fill="#0d3b45">OPTIMIZE z_order</text>
<text x="315" y="113" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">128 MB bbox-clustered</text>
<rect x="450" y="70" width="160" height="60" rx="8" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<text x="530" y="95" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="600" fill="#0d3b45">VACUUM</text>
<text x="530" y="113" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">delete &gt; 168h old</text>
<line x1="180" y1="100" x2="235" y2="100" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-delta-vac)"/>
<line x1="395" y1="100" x2="450" y2="100" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-delta-vac)"/>
<rect x="120" y="165" width="520" height="52" rx="6" fill="#ffffff" stroke="#cfe3e7" stroke-width="1.5"/>
<rect x="470" y="171" width="164" height="40" rx="4" fill="#f7fbfc" stroke="#2f6e49" stroke-width="1.5"/>
<text x="552" y="188" text-anchor="middle" font-family="sans-serif" font-size="11" font-weight="600" fill="#0d3b45">retention window</text>
<text x="552" y="203" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">kept: time-travel safe</text>
<text x="290" y="196" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">older tombstones → deleted</text>
<text x="380" y="240" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#3d5a63">retention = max(SLA, longest query + rollback margin)</text>
</svg>
</figure>

Before writing new geometry into the table, gate it with the checks in [building a schema validation pipeline for geospatial tables](/python-ecosystem-integration-workflows/lakehouse-maintenance-automation/schema-validation-pipeline-for-geospatial-tables/), and watch for coordinate-system regressions with [detecting CRS drift in ingestion pipelines](/spatial-lakehouse-fundamentals-architecture/crs-management-pipelines/detecting-crs-drift-in-ingestion-pipelines/). The authoritative reference for these operations is the [Delta Lake utility commands documentation](https://docs.delta.io/latest/delta-utility.html#remove-files-no-longer-referenced-by-a-delta-table) and the [delta-rs usage guide](https://delta-io.github.io/delta-rs/usage/optimize/small-file-compaction-with-optimize/).
