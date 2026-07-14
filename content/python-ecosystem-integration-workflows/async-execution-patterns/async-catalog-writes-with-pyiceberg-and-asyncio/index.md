# Async Catalog Writes with PyIceberg and asyncio

This recipe writes many spatial partitions into an Apache Iceberg table concurrently from a single Python process, using `asyncio` to overlap Parquet serialization and object-store uploads while funneling the actual catalog commit through a serialized, retry-guarded path that respects Iceberg snapshot isolation.

## Context and prerequisites

PyIceberg's `Table.append` is synchronous and, more importantly, a commit is a compare-and-swap against the catalog's current metadata pointer: two commits racing on the same table will make one of them fail with a `CommitFailedException`. The trick for high throughput is to parallelize the expensive, side-effect-free work — building Arrow tables and writing data files — while keeping the metadata commits either serialized or individually retried. This page belongs to the [async execution patterns](/python-ecosystem-integration-workflows/async-execution-patterns/) topic area and builds directly on [PyIceberg spatial workflows](/python-ecosystem-integration-workflows/pyiceberg-spatial-workflows/). You need PyIceberg 0.7+ (`pip install "pyiceberg[s3fs,pyarrow]"`), a configured catalog (REST or SQL), and Python 3.11+ for `asyncio.TaskGroup`. Geometry rides as WKB in a `binary` column, the encoding covered in [reading shapefiles into PyIceberg dataframes efficiently](/python-ecosystem-integration-workflows/pyiceberg-spatial-workflows/reading-shapefiles-into-pyiceberg-dataframes-efficiently/).

## Complete working solution

The pattern: a bounded `Semaphore` caps concurrent Arrow builds, `loop.run_in_executor` moves the blocking PyIceberg calls off the event loop, and a single `asyncio.Lock` serializes commits so snapshots apply one at a time with a retry on conflict.

```python
# async_iceberg_writes.py
# PyIceberg 0.7+, Python 3.11+, Iceberg format-version 2
import asyncio
import random
from datetime import date

import pyarrow as pa
from shapely import Point
from shapely import to_wkb
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import CommitFailedException

CATALOG = load_catalog("prod")  # resolved from ~/.pyiceberg.yaml
TABLE_ID = "gis.sensor_readings"
MAX_INFLIGHT = 4          # concurrent Arrow builds / uploads
MAX_COMMIT_RETRIES = 5

# One Arrow schema shared by every partition; geometry stored as WKB bytes.
ARROW_SCHEMA = pa.schema([
    pa.field("reading_id", pa.int64(), nullable=False),
    pa.field("utm_zone", pa.string(), nullable=False),
    pa.field("obs_date", pa.date32(), nullable=False),
    pa.field("geom_wkb", pa.binary(), nullable=False),  # EPSG:4326 WKB
])

_commit_lock = asyncio.Lock()


def build_partition(utm_zone: str, n_rows: int) -> pa.Table:
    """CPU-bound: synthesize one UTM-zone partition as an Arrow table."""
    lon0 = -126.0 + 6.0 * (hash(utm_zone) % 10)
    pts = [to_wkb(Point(lon0 + random.random(), 30 + random.random()))
           for _ in range(n_rows)]
    return pa.table({
        "reading_id": pa.array(range(n_rows), pa.int64()),
        "utm_zone": pa.array([utm_zone] * n_rows, pa.string()),
        "obs_date": pa.array([date(2026, 7, 14)] * n_rows, pa.date32()),
        "geom_wkb": pa.array(pts, pa.binary()),
    }, schema=ARROW_SCHEMA)


async def append_partition(sem: asyncio.Semaphore, utm_zone: str, n_rows: int) -> str:
    """Build off-thread, then commit under a lock with retry."""
    loop = asyncio.get_running_loop()
    async with sem:
        # Build the Arrow table without blocking the event loop.
        tbl = await loop.run_in_executor(None, build_partition, utm_zone, n_rows)

    # Serialize commits so snapshots apply one at a time.
    for attempt in range(MAX_COMMIT_RETRIES):
        async with _commit_lock:
            try:
                ice = CATALOG.load_table(TABLE_ID)   # refresh metadata each try
                await loop.run_in_executor(None, ice.append, tbl)
                return f"{utm_zone}: committed {tbl.num_rows} rows"
            except CommitFailedException:
                if attempt == MAX_COMMIT_RETRIES - 1:
                    raise
        # Backoff outside the lock so peers can proceed.
        await asyncio.sleep((2 ** attempt) * 0.1 + random.random() * 0.1)
    raise RuntimeError(f"{utm_zone}: exhausted commit retries")


async def main() -> None:
    zones = ["32U", "33U", "10S", "11S", "18T", "19T"]
    sem = asyncio.Semaphore(MAX_INFLIGHT)
    async with asyncio.TaskGroup() as tg:
        tasks = [tg.create_task(append_partition(sem, z, 5000)) for z in zones]
    for t in tasks:
        print(t.result())


if __name__ == "__main__":
    asyncio.run(main())
```

## Step-by-step walkthrough

1. **Separate build from commit.** `build_partition` is pure CPU (Shapely + Arrow) with no catalog contact, so it parallelizes cleanly. Only the commit — the compare-and-swap on table metadata — needs coordination.

2. **`run_in_executor` for blocking calls.** PyIceberg and Shapely are synchronous and release nothing to the event loop. Wrapping `build_partition` and `ice.append` in `loop.run_in_executor(None, ...)` hands them to the default `ThreadPoolExecutor`, so uploads for one zone overlap with Arrow construction for another instead of blocking `main`.

3. **`Semaphore(MAX_INFLIGHT)` caps memory.** Each in-flight partition holds a full Arrow table in RAM. The semaphore bounds how many build simultaneously; four is a sane default that keeps peak memory to roughly `4 × partition_size` and matches typical executor thread counts.

4. **`asyncio.Lock` serializes commits.** Iceberg commits are not commutative on the metadata pointer. Holding `_commit_lock` around `load_table` + `append` guarantees each commit sees the snapshot produced by the previous one, so no two tasks race the same base metadata. This trades a little commit throughput for zero avoidable conflicts.

5. **Refresh inside the retry.** `CATALOG.load_table(TABLE_ID)` runs *inside* the loop, not once outside it. On a conflict — say another process committed between your load and append — the next attempt reloads the current metadata and re-applies onto the new snapshot, which is what snapshot isolation requires.

6. **Exponential backoff with jitter, outside the lock.** The `await asyncio.sleep` sits after releasing `_commit_lock` so a retrying task does not starve peers. Jitter (`random.random() * 0.1`) desynchronizes retries when a REST catalog briefly rejects several at once.

7. **`TaskGroup` for structured concurrency.** Python 3.11's `asyncio.TaskGroup` awaits every task and, if one raises after retries exhaust, cancels the rest and propagates — you never silently lose a failed partition. If you can tolerate independent partial success, swap it for `asyncio.gather(*tasks, return_exceptions=True)`.

If your catalog and object store are latency-bound rather than CPU-bound, raise `MAX_INFLIGHT` and grow the executor with `loop.set_default_executor(ThreadPoolExecutor(max_workers=16))`. The commit lock is intentionally *not* widened — serialized commits are the correctness anchor.

## Common errors and fixes

| Error | Cause | Fix |
|---|---|---|
| `CommitFailedException` on every attempt | Another writer holds the table; retries reload but keep losing the race | Increase `MAX_COMMIT_RETRIES`, add jitter, or route all writes through one process |
| `RuntimeError: There is no current event loop` | Calling `asyncio.get_running_loop()` outside a coroutine | Only call it inside `async def`; use `asyncio.run(main())` as the entry point |
| Memory blows up with many zones | Semaphore too high or executor unbounded | Lower `MAX_INFLIGHT`; each permit pins one Arrow table in RAM |
| Commits appear serialized but slow | Every task contends on `_commit_lock` sequentially | Expected: commits are serial by design; parallelism is in the build/upload phase |

## Verification

Confirm that every partition produced its own snapshot and that the committed row count matches what you sent. Iceberg records one snapshot per successful `append`.

```python
# verify_snapshots.py
from pyiceberg.catalog import load_catalog

tbl = load_catalog("prod").load_table("gis.sensor_readings")

snaps = list(tbl.snapshots())
print("snapshots:", len(snaps))          # expect >= number of partitions appended
for s in snaps[-6:]:
    print(s.snapshot_id, s.summary.get("added-records"),
          s.summary.get("operation"))     # each: 5000, 'append'

# Total rows and distinct partitions committed.
df = tbl.scan().to_arrow()
print("total rows:", df.num_rows)                       # 6 zones * 5000 = 30000
print("distinct zones:", len(set(df.column("utm_zone").to_pylist())))  # 6
```

Seeing one `append` snapshot per zone with `added-records = 5000` each, and a total of 30000 rows across six distinct `utm_zone` values, proves the concurrent writes all landed under proper isolation with no lost updates. For the schema-side of this pipeline — turning GeoDataFrames into the Arrow tables you commit here — see [mapping GeoPandas DataFrames to Arrow schemas](/python-ecosystem-integration-workflows/dataframe-mapping-strategies/mapping-geopandas-dataframes-to-arrow-schemas/).

<figure class="diagram">
<svg viewBox="0 0 760 260" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Concurrent Arrow builds funneling through a semaphore and a serialized commit lock into an Iceberg table">
<title>Async build, serialized commit</title>
<desc>Multiple UTM-zone partitions build Arrow tables in parallel under a semaphore, then pass one at a time through a commit lock that appends each as its own Iceberg snapshot with retry on conflict.</desc>
<defs>
<marker id="arw-pyice-async" markerWidth="9" markerHeight="9" refX="7" refY="4" orient="auto"><path d="M0 0 L9 4 L0 8 z" fill="#0e6e7d"/></marker>
</defs>
<rect x="0" y="0" width="760" height="260" fill="#f7fbfc"/>
<text x="90" y="30" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">build (executor)</text>
<rect x="20" y="45" width="140" height="34" rx="5" fill="#ffffff" stroke="#2f6e49"/><text x="90" y="67" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">zone 32U -> Arrow</text>
<rect x="20" y="90" width="140" height="34" rx="5" fill="#ffffff" stroke="#2f6e49"/><text x="90" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">zone 33U -> Arrow</text>
<rect x="20" y="135" width="140" height="34" rx="5" fill="#ffffff" stroke="#2f6e49"/><text x="90" y="157" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">zone 10S -> Arrow</text>
<rect x="20" y="180" width="140" height="34" rx="5" fill="#ffffff" stroke="#2f6e49"/><text x="90" y="202" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">zone 11S -> Arrow</text>
<rect x="215" y="90" width="120" height="80" rx="6" fill="#ffffff" stroke="#9a5a17"/>
<text x="275" y="122" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">Semaphore</text>
<text x="275" y="142" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">max 4 in-flight</text>
<rect x="390" y="90" width="130" height="80" rx="6" fill="#ffffff" stroke="#6a3d9a"/>
<text x="455" y="118" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">commit Lock</text>
<text x="455" y="138" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#6a3d9a">append 1-at-a-time</text>
<text x="455" y="156" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#6a3d9a">retry on conflict</text>
<rect x="575" y="90" width="160" height="80" rx="6" fill="#ffffff" stroke="#0e6e7d"/>
<text x="655" y="118" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">Iceberg table</text>
<text x="655" y="138" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">one snapshot</text>
<text x="655" y="156" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">per partition</text>
<line x1="160" y1="62" x2="213" y2="110" stroke="#0e6e7d" stroke-width="1.5" marker-end="url(#arw-pyice-async)"/>
<line x1="160" y1="107" x2="213" y2="120" stroke="#0e6e7d" stroke-width="1.5" marker-end="url(#arw-pyice-async)"/>
<line x1="160" y1="152" x2="213" y2="140" stroke="#0e6e7d" stroke-width="1.5" marker-end="url(#arw-pyice-async)"/>
<line x1="160" y1="197" x2="213" y2="150" stroke="#0e6e7d" stroke-width="1.5" marker-end="url(#arw-pyice-async)"/>
<line x1="335" y1="130" x2="388" y2="130" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-pyice-async)"/>
<line x1="520" y1="130" x2="573" y2="130" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-pyice-async)"/>
</svg>
</figure>

The essential discipline is that concurrency lives in the build-and-upload stage while the commit stage stays serial and idempotent under retry — a shape that generalizes to any open-table-format writer. For the broader concurrency toolbox this draws on, return to [async execution patterns](/python-ecosystem-integration-workflows/async-execution-patterns/), and consult the [PyIceberg API documentation](https://py.iceberg.apache.org/api/) for commit and snapshot semantics.
