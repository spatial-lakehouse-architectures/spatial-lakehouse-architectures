# Async Execution Patterns

Asynchronous execution is a foundational requirement for spatial data lakehouse architectures. Geospatial workloads—spanning high-frequency IoT telemetry, multi-petabyte satellite mosaics, and LiDAR point clouds—introduce severe I/O bottlenecks and compute skew that synchronous batch pipelines cannot absorb. By decoupling compute orchestration from storage mutations, platform teams achieve higher ingestion throughput while preserving strict ACID guarantees. This execution model integrates directly into the broader [Python Ecosystem & Integration Workflows](/python-ecosystem-integration-workflows/), where task schedulers, distributed executors, and format-specific APIs converge to handle spatial transformations without blocking the main pipeline thread or stalling downstream query engines.

## Spatial Partitioning & Async Compaction

Spatial partitioning strategies (H3, S2, Quadkey, or Z-order curves) dictate how background maintenance jobs are scheduled, prioritized, and isolated. When geometry-heavy tables exceed 100M rows per partition, synchronous compaction routinely triggers OOM errors on executor nodes due to WKB/GeoJSON deserialization overhead and unbounded memory allocation during spatial joins. The production-ready mitigation is to dispatch compaction asynchronously, monitor manifest sizes, and trigger rewrites only when storage thresholds are breached.

For Apache Iceberg, async compaction relies on the `rewrite_data_files` procedure. Configure the following parameters to align with spatial data characteristics:
- `write.target-data-file-size-bytes`: 134217728 (128 MB)
- `compaction.worker-threads`: Match available vCPU cores (e.g., 8)
- `compaction.max-concurrent-jobs`: 3–5 to prevent object storage API throttling

```sql
-- Async Iceberg compaction for EPSG:4326 partitioned table
CALL catalog.system.rewrite_data_files(
  table => 'spatial_catalog.iot_telemetry',
  strategy => 'sort',
  options => map(
    'sort-order', 'h3_res7, timestamp',
    'max-concurrent-file-group-rewrites', '4',
    'partial-progress.enabled', 'true'
  )
);
```

Delta Lake approaches the same problem through the `OPTIMIZE` command with `ZORDER BY (geometry_column)`. Because Delta's optimistic concurrency control may require explicit retry logic when multiple async writers target the same partition, implement exponential backoff with jitter. Reference implementations for geometry-aware async dispatch are documented in [Delta-rs Geometry Processing](/python-ecosystem-integration-workflows/delta-rs-geometry-processing/), which covers Rust-backed parallel execution and safe concurrent writes.

```python
import asyncio
import time
import random
from delta import configure_spark_with_delta_pip, DeltaTable

spark = configure_spark_with_delta_pip().getOrCreate()

async def async_optimize_with_backoff(table_path: str, zorder_cols: list[str]):
    max_retries = 5
    for attempt in range(max_retries):
        try:
            dt = DeltaTable.forPath(spark, table_path)
            dt.optimize().executeZOrderBy(*zorder_cols)
            print(f"✅ OPTIMIZE succeeded for {table_path}")
            break
        except Exception as e:
            delay = (2 ** attempt) + random.uniform(0, 1)
            print(f"⚠️  Attempt {attempt+1} failed: {e}. Retrying in {delay:.2f}s")
            await asyncio.sleep(delay)
    else:
        raise RuntimeError("Max retries exceeded for async OPTIMIZE")

# Run with explicit spatial bounds: lat [-90, 90], lon [-180, 180]
asyncio.run(async_optimize_with_backoff("s3://lakehouse/telemetry", ["geometry"]))
```

## Metadata-Driven Indexing & Predicate Pushdown

Spatial indexing in lakehouse formats is inherently metadata-driven. Iceberg stores partition specs, sort orders, and statistics in the metadata layer, enabling async index materialization via background `add-files` operations. When integrating vectorized geometry operations, teams route index builds through [PyIceberg Spatial Workflows](/python-ecosystem-integration-workflows/pyiceberg-spatial-workflows/), leveraging `asyncio` to parallelize R-tree construction and spatial predicate caching across partition boundaries.

Critical configuration for async metadata updates:
- `write.metadata.compression-codec`: `zstd`
- `write.metadata.previous-versions-max`: 1000
- `snapshot-retention-ms`: 2592000000 (30 days)
- `gc.enabled`: `true` (with vacuum threshold aligned to retention)

```python
import asyncio
import aiohttp
from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table

async def build_async_rtree_index(table: Table, partition_bounds: dict):
    """
    partition_bounds example:
    {"h3_res7": ["872830828ffffff", "872830829ffffff"], "retention_days": 30}
    """
    async with aiohttp.ClientSession() as session:
        tasks = []
        for part_id, bounds in partition_bounds.items():
            tasks.append(_materialize_partition_index(session, table, part_id, bounds))
        await asyncio.gather(*tasks)

async def _materialize_partition_index(session, table, part_id, bounds):
    # Simulate async predicate cache build + manifest rewrite
    manifest = table.current_snapshot().manifests()
    await asyncio.sleep(0.1)  # I/O bound network call to object storage
    print(f"Indexed partition {part_id} with bounds {bounds}")

# Execute against a catalog configured for EPSG:4326 geometries
catalog = load_catalog("default")
tbl = catalog.load_table("spatial_catalog.satellite_tiles")
asyncio.run(build_async_rtree_index(tbl, {"res7_cluster_01": ["872830828ffffff"], "retention_days": 30}))
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
          pip install pyiceberg[sql] delta-spark
      - name: Run Async Config Validator
        run: |
          python -c "
          import json, sys
          with open('lakehouse-configs/compaction-policies/spatial.json') as f:
            cfg = json.load(f)
          assert cfg['crs'] == 'EPSG:4326', 'Invalid CRS'
          assert cfg['retention_days'] >= 14, 'Retention too short for async vacuum'
          assert cfg['max_concurrent_compactions'] <= 5, 'Throttle risk'
          print('✅ Async spatial config validated')
          "
```

## Troubleshooting & Operational Guardrails

| Symptom | Root Cause | Remediation |
|---------|------------|-------------|
| `java.lang.OutOfMemoryError: Java heap space` during compaction | Unbounded WKB deserialization in synchronous executors | Switch to async dispatch, set `spark.sql.shuffle.partitions` to match vCPU count, enable `spark.memory.fraction=0.7` |
| Metadata lock contention on concurrent writes | Multiple async `OPTIMIZE`/`rewrite` jobs targeting identical partitions | Implement partition-level locking via Redis/ZooKeeper, cap concurrent jobs to 3 per partition key |
| Query latency spikes after async vacuum | Aggressive snapshot pruning breaking time-travel compatibility | Align `snapshot-retention-ms` with SLA (min 30 days), verify `gc.enabled=false` during peak query windows |
| Spatial predicate pushdown failure | Missing sort order or stale statistics post-compaction | Run `ANALYZE TABLE ... COMPUTE STATISTICS` async, enforce `sort-order` on geometry partition columns |

Monitor async job health using manifest size metrics and object storage request rates. When `write.metadata.previous-versions-max` approaches capacity, trigger async metadata cleanup before it impacts catalog resolution latency. Maintain explicit retention windows (e.g., 30 days for snapshots, 14 days for transaction logs) and enforce CRS consistency (`EPSG:4326` or `EPSG:3857`) across all async workers to prevent coordinate transformation drift during background rewrites.

For authoritative reference on compaction semantics and maintenance procedures, consult the official [Apache Iceberg Maintenance Documentation](https://iceberg.apache.org/docs/latest/maintenance/) and [Delta Lake OPTIMIZE Utilities](https://docs.delta.io/latest/delta-utility.html#optimize-a-delta-table). Python async primitives used in spatial dispatch follow the standard [asyncio Task Management](https://docs.python.org/3/library/asyncio.html) model.