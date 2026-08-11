# Using delta-rs to write spatial parquet files

Unbounded memory consumption and transaction contention are the dominant failure modes when ingesting vector geometries into Delta Lake. The root cause is a structural mismatch between in-memory spatial representations, Parquet row group boundaries, and Delta's optimistic concurrency control. Resolving this requires a deterministic write pipeline that isolates geometry serialization, enforces strict partition pruning, and leverages the async Rust execution engine. This guide details the exact configuration, failure resolution, and parameter tuning required for production-grade spatial ingestion.

## Serialization Contract & Schema Enforcement

Spatial columns stored as raw WKB bytes lack native Delta type hints. Relying on automatic inference defaults to generic `binary` without spatial metadata, which breaks downstream spatial indexing and forces full table scans. Pre-serialize geometries to Well-Known Binary (WKB) and attach [GeoParquet](https://github.com/opengeospatial/geoparquet)-compliant metadata before invoking the write engine. This aligns with the [Delta-rs Geometry Processing](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/delta-rs-geometry-processing/) validation pipeline, ensuring CRS consistency and bounding-box constraints are enforced prior to heap allocation. Stripping GeoJSON overhead reduces write-phase memory pressure by 40–60%.

Explicit schema enforcement prevents drift during schema evolution. Always construct a `pyarrow.Schema` object with `pa.binary()` for geometry columns and pass it directly to the writer. Omitting this step triggers naive binary inference, causing row group fragmentation and amplifying compaction overhead during `OPTIMIZE` cycles.

## Async Execution & Chunked Write Pipeline

The synchronous `write_deltalake` API blocks the Python GIL and serializes commit attempts, which causes transaction retries under concurrent workloads. Production pipelines must stream spatial data through chunked iterators that respect a 256MB–512MB per-partition threshold. Chunking must occur before the Rust write engine is invoked, as `delta-rs` does not perform automatic spill-to-disk during serialization.

```python
import asyncio
import pyarrow as pa
from deltalake import write_deltalake
from concurrent.futures import ThreadPoolExecutor

# Explicit schema to prevent drift and enforce WKB binary layout
SPATIAL_SCHEMA = pa.schema([
    ("geometry",  pa.binary()),             # WKB bytes, little-endian
    ("h3_res8",   pa.string()),             # H3 partition key
    ("event_ts",  pa.timestamp("us")),
    ("bbox_min_x", pa.float64()),
    ("bbox_min_y", pa.float64()),
    ("bbox_max_x", pa.float64()),
    ("bbox_max_y", pa.float64()),
])

async def stream_write_spatial(table_uri: str, chunk_iterator, max_workers: int = 4):
    """
    Writes spatial chunks to Delta Lake using the Rust engine.
    Each chunk is offloaded to a thread pool to avoid blocking the event loop.
    """
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        tasks = []
        for chunk in chunk_iterator:
            # Enforce schema at Arrow conversion boundary
            arrow_tbl = pa.Table.from_pandas(chunk, schema=SPATIAL_SCHEMA)

            # Offload to delta-rs Rust runtime in thread pool
            task = loop.run_in_executor(
                pool,
                lambda tbl=arrow_tbl: write_deltalake(
                    table_or_uri=table_uri,
                    data=tbl,
                    mode="append",
                    partition_by=["h3_res8"],
                    schema_mode="merge"
                )
            )
            tasks.append(task)

        # Await all chunks; propagate first exception
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for res in results:
            if isinstance(res, Exception):
                raise res
```

This pattern integrates cleanly into broader [Python Ecosystem & Integration Workflows](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/) by decoupling DataFrame materialization from the write boundary. Aligning DataFrame partitions with Delta's target file size prevents write amplification and ensures each Parquet file contains a single, contiguous spatial extent.

## Failure Modes & Deterministic Resolution

| Failure Mode | Root Cause | Resolution |
|--------------|------------|------------|
| **OOM during row group materialization** | Unbounded geometry payloads exceed heap limits during Parquet serialization | Pre-chunk at 256MB and strip GeoJSON padding before converting to WKB |
| **Schema drift on append** | Implicit type inference promotes `binary` to `string` or alters nullable flags | Pass explicit `pa.schema`, set `schema_mode="merge"`, and validate column order before write invocation |
| **Snapshot conflict / commit retry** | GIL blocking causes overlapping `COMMIT` operations on `_delta_log` | Limit concurrent writers to 4 and implement exponential backoff on `DeltaError`; commits run through the Rust engine outside the GIL |
| **Vacuum latency spike** | Fragmented row groups from misaligned partition boundaries increase small-file count | Partition on high-cardinality spatial keys (H3 res7–8 or temporal windows), run `dt.optimize.compact()` post-ingestion |

## Production Parameter Matrix

| Parameter | Recommended Value | Rationale |
|-----------|-------------------|-----------|
| `partition_by` | `["h3_res8"]` or `["date"]` | High-cardinality spatial/temporal keys enable predicate pushdown and partition pruning |
| `schema_mode` | `"merge"` | Allows safe column addition without breaking existing readers |
| `max_workers` (ThreadPoolExecutor) | `4` | Balances throughput against Delta transaction log lock contention |

Post-write, compact fragmented files:
```python
from deltalake import DeltaTable

dt = DeltaTable(table_uri)
# Bin-pack into 512MB target files
dt.optimize.compact(target_size=512 * 1024 * 1024)
```

Monitor `_delta_log` commit latency; sustained values >2s indicate partition skew or insufficient backoff configuration. Enforce deterministic serialization contracts at the ingestion boundary to eliminate schema drift and guarantee reproducible spatial lakehouse performance.

## The Schema Is the Whole Design

Everything that determines whether the resulting table is fast lives in the schema declaration, and it is worth laying out explicitly before writing any code.

<figure class="diagram">
<svg viewBox="0 0 732 272" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Column order in a spatial Delta schema showing identifier, partition column and bounding box columns placed inside the statistics window, with the geometry payload and wide attributes after it">
<rect x="0" y="0" width="732" height="272" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Column order decides whether skipping works</text>
<rect x="60" y="58" width="660" height="106" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2.5"/>
<text x="390" y="84" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">inside the statistics window (first 32 columns by default)</text>
<rect x="80" y="98" width="140" height="46" rx="6" fill="#ffffff" stroke="#2f6e49" stroke-width="1.5"/>
<text x="150" y="126" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">feature_id</text>
<rect x="232" y="98" width="140" height="46" rx="6" fill="#ffffff" stroke="#2f6e49" stroke-width="1.5"/>
<text x="302" y="126" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">event_day</text>
<rect x="384" y="98" width="140" height="46" rx="6" fill="#ffffff" stroke="#2f6e49" stroke-width="1.5"/>
<text x="454" y="126" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">h3_r5</text>
<rect x="536" y="98" width="164" height="46" rx="6" fill="#d7e8de" stroke="#2f6e49" stroke-width="2"/>
<text x="618" y="120" text-anchor="middle" font-family="sans-serif" font-size="11" font-weight="700" fill="#0d3b45">bbox_min_x …</text>
<text x="618" y="136" text-anchor="middle" font-family="sans-serif" font-size="11" font-weight="700" fill="#0d3b45">bbox_max_y</text>
<rect x="60" y="184" width="660" height="76" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="390" y="210" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">outside it — statistics here would be useless anyway</text>
<text x="390" y="236" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">geometry (BINARY, wide) · free-text attributes · rarely-filtered columns</text>
</svg>
</figure>

Putting the geometry column last is deliberate on two counts. Statistics on a WKB column are meaningless — the minimum and maximum of a byte array carry no spatial information — and collecting them wastes space in the log while inflating the statistics payload on every commit. Placing wide columns beyond the window also leaves room for the columns that genuinely benefit.

The four bounding-box columns should be adjacent and immediately after the partition column. Adjacency is not required by anything, but it makes the schema self-documenting and makes the "are they inside the window" check a matter of counting to eight rather than auditing a fifty-column list.

## Getting the Write Options Right

<figure class="diagram">
<svg viewBox="0 0 762 256" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Four write options that matter for a spatial Delta table: compression codec, target file size, partition columns and schema mode, with the recommended value for each">
<rect x="0" y="0" width="762" height="256" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Four options worth setting explicitly</text>
<rect x="30" y="56" width="352" height="86" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="206" y="82" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">compression: zstd</text>
<text x="206" y="106" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">WKB compresses well; zstd beats snappy</text>
<text x="206" y="126" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">by 15–30% at similar decode speed</text>
<rect x="398" y="56" width="352" height="86" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="574" y="82" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">target file size: 128–512 MB</text>
<text x="574" y="106" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">smaller inflates request counts</text>
<text x="574" y="126" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">larger delays the first byte</text>
<rect x="30" y="158" width="352" height="86" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="206" y="184" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">partition_by: coarse only</text>
<text x="206" y="208" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">day, and a coarse cell at most</text>
<text x="206" y="228" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">never a fine grid resolution</text>
<rect x="398" y="158" width="352" height="86" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="574" y="184" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">schema_mode: explicit</text>
<text x="574" y="208" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">merge only when evolution is intended</text>
<text x="574" y="228" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">otherwise a typo becomes a column</text>
</svg>
</figure>

The schema-mode default deserves particular attention on spatial tables. Automatic merging is convenient during development and dangerous in production: a batch with a mistyped column name silently adds a column rather than failing, and the mistake is only visible later as a mostly-null field nobody can account for. Enable merging deliberately, for the write that is meant to evolve the schema, and leave it off otherwise.

## Verifying the Result Immediately

<figure class="diagram">
<svg viewBox="0 0 752 204" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Post-write verification reading the Delta transaction log to confirm min and max values are recorded for each bounding box column and that file sizes fall in the intended band">
<rect x="0" y="0" width="752" height="204" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Read the log back before trusting the table</text>
<rect x="40" y="60" width="320" height="132" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="200" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">what to look for in each add action</text>
<text x="200" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">minValues.bbox_min_x present</text>
<text x="200" y="138" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">maxValues.bbox_max_y present</text>
<text x="200" y="160" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">numRecords &gt; 0</text>
<text x="200" y="180" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">size in the intended band</text>
<rect x="420" y="60" width="320" height="132" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="580" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">what their absence means</text>
<text x="580" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">bbox columns outside the window</text>
<text x="580" y="138" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">or statistics disabled entirely</text>
<text x="580" y="160" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">the table works and prunes nothing</text>
<text x="580" y="180" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">no error will ever be raised</text>
</svg>
</figure>

This check costs one JSON read and takes a second. Wire it into the write job as a post-condition rather than running it manually, because the property it guards is the one that silently disappears when a schema grows — and a schema that grows is the normal course of events rather than an exception.

## Common Mistakes

A short list of what goes wrong in practice, all of which the checks above catch:

- **Geometry stored as WKT.** Convenient during debugging and roughly twice the size on disk, with a slower decode on every read. Convert to WKB before writing and keep WKT for log lines only.
- **Bounding boxes derived before reprojection.** The values describe the geometry's old position, so skipping excludes the files a query needs. Derive after every transform, and assert coverage.
- **Mixed dimensionality.** A source containing both 2D and 3D geometries produces a column that behaves inconsistently across engines. Force to two dimensions at ingest unless elevation is genuinely required, and record the decision in table properties.
- **Partitioning on a fine grid resolution.** Delta writes each partition as a directory, and a fine cell produces a directory explosion that no compaction repairs. Partition on day and a coarse cell; cluster on the fine one.
- **Relying on schema inference per batch.** A batch whose optional column is entirely null infers a different schema and fails the write. Declare the schema once and cast every batch to it.
- **Committing per row group.** Each commit adds a log entry, and a job that commits thousands of times leaves a table whose reads spend longer replaying the log than reading data. Accumulate and commit once per unit of work.

None of these produces an error at write time except the last two, which is precisely why the post-write verification matters more here than it would for a scalar table.

The habit that prevents all six is to treat the write function as a contract rather than as a script: it declares its schema, it derives what it needs, it asserts its post-conditions, and it fails rather than producing a table that merely resembles the intended one. Everything else in this guide follows from that stance, and pipelines that adopt it tend to need very little maintenance afterwards.

For the wider context — when this path is the right one at all, and what to do when the data outgrows it — see [delta-rs geometry processing](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/delta-rs-geometry-processing/).

That page also covers the maintenance operations available from the same Python process, so a table created with this recipe can be compacted and vacuumed without introducing a second toolchain.

Keeping the whole lifecycle in one language and one process is much of the appeal, and it holds until the largest single operation stops fitting on the node — at which point the same table, unchanged, can be handed to a cluster.
Nothing about the data or its layout changes in that transition, which is exactly the property that makes starting small a low-risk decision rather than a bet.
The write path stays the same in either regime, which is what keeps the eventual change cheap.
The executor changes; the contract does not.
Verify that once on a copy and the eventual migration is a scheduling decision.
