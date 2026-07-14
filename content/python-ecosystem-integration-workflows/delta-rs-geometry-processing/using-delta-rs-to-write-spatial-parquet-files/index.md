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
