# Async Execution Patterns

Asynchronous execution is a foundational requirement for spatial data lakehouse architectures. Geospatial workloads—spanning high-frequency IoT telemetry, multi-petabyte satellite mosaics, and LiDAR point clouds—introduce severe I/O bottlenecks and compute skew that synchronous batch pipelines cannot absorb. By decoupling compute orchestration from storage mutations, platform teams achieve higher ingestion throughput while preserving strict ACID guarantees. This execution model integrates directly into the broader [Python Ecosystem & Integration Workflows](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/), where task schedulers, distributed executors, and format-specific APIs converge to handle spatial transformations without blocking the main pipeline thread or stalling downstream query engines.

<figure class="diagram">
<svg viewBox="0 0 760 210" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Async geometry ingestion pipeline: submit, queue, worker pool, gather and commit">
<defs>
<marker id="async-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#1f6f8b"/></marker>
</defs>
<rect x="0" y="0" width="760" height="210" fill="#f7fbfc"/>
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
