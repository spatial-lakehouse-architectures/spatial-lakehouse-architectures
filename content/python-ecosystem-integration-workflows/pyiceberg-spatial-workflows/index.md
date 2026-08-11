# Optimizing PyIceberg Spatial Workflows: Partitioning, Maintenance, and CI/CD in the Lakehouse

Spatial data lakehouse architectures demand rigorous partitioning, indexing, and maintenance strategies to handle high-cardinality geometries, streaming telemetry, and evolving coordinate reference systems. When building production-grade pipelines, the choice between Apache Iceberg and Delta Lake dictates your compaction cadence, metadata overhead, and query planner behavior. This guide operationalizes PyIceberg spatial workflows, anchoring them within the broader [Python Ecosystem & Integration Workflows](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/) to ensure consistent schema evolution, catalog integration, and compute orchestration. We focus on actionable configurations for spatial partitioning, index tuning, automated maintenance, and CI/CD validation.

## Spatial Partitioning & Predicate Pushdown

Iceberg supports hidden partitioning and transform functions, but spatial workloads rarely benefit from naive date or region buckets. High-cardinality WKB/WKT strings trigger manifest explosion and degrade predicate pushdown. Instead, implement hierarchical grid-based partitioning using H3 or Geohash encodings. Materialize a deterministic `grid_id` during ingestion and apply `BucketTransform` via `pyiceberg.partitioning.PartitionField`.

When migrating from directory-based engines, recognize that Iceberg relies on manifest-level statistics (`lower_bounds`, `upper_bounds`) rather than directory pruning. Debug misaligned partitions by inspecting `table.metadata.partition_spec()` and verifying that manifest statistics correctly reflect spatial bounds via the explicit `bbox_*` columns. For ingestion pipelines that consume legacy GIS formats, consult [Reading shapefiles into PyIceberg DataFrames efficiently](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/pyiceberg-spatial-workflows/reading-shapefiles-into-pyiceberg-dataframes-efficiently/) to ensure geometry normalization before partition assignment.

```python
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import BucketTransform
from pyiceberg.schema import Schema
from pyiceberg.types import (
    LongType, StructType, NestedField, BinaryType, DoubleType
)

catalog = load_catalog("default")
schema = Schema(
    NestedField(1, "event_id",      LongType(),   required=True),
    NestedField(2, "grid_id",       LongType(),   required=True),
    NestedField(3, "geometry_wkb",  BinaryType(), required=True),  # WKB, not string
    NestedField(4, "min_x",         DoubleType(), required=True),
    NestedField(5, "max_x",         DoubleType(), required=True),
    NestedField(6, "min_y",         DoubleType(), required=True),
    NestedField(7, "max_y",         DoubleType(), required=True),
    identifier_field_ids=[1]
)

# Partition by H3 resolution 6 bucket (4096 buckets)
partition_spec = PartitionSpec(
    PartitionField(
        source_id=2,
        field_id=1000,
        transform=BucketTransform(4096),
        name="grid_id_bucket"
    )
)

table = catalog.create_table(
    identifier="spatial_raw.telemetry_events",
    schema=schema,
    partition_spec=partition_spec,
    properties={
        "write.parquet.compression-codec": "zstd",
        "write.parquet.compression-level": "3",
        "write.metadata.previous-versions-max": "10",
        "write.metadata.delete-after-commit.enabled": "true"
    }
)
```

If queries scan excessive files despite partition pruning, run `table.scan().plan_files()` to audit manifest coverage. Adjust bucket counts or check whether `grid_id` values cover the expected spatial density.

## Indexing & Query Optimization Trade-offs

Iceberg does not ship with native R-tree or spatial indexes. Query performance hinges on Z-ordering coordinate bounds (`min_x`, `max_x`, `min_y`, `max_y`) and enforcing strict sort orders before compaction. Configure table properties with `write.sort-order` to sort by bounding columns, then trigger `rewrite_data_files`. Target file sizes between 128MB and 256MB to align with Spark/Trino block sizes and minimize small-file overhead.

Delta Lake users often leverage [Delta-rs Geometry Processing](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/delta-rs-geometry-processing/) for Rust-accelerated spatial predicates, but PyIceberg relies on PyArrow compute and catalog-level statistics. The trade-off is explicit: Iceberg provides superior snapshot isolation, time-travel, and concurrent write safety, but you must manually maintain sort order and monitor `rewrite_data_files` execution.

```python
import time
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import GreaterThanOrEqual

catalog = load_catalog("default")
table = catalog.load_table("spatial_raw.telemetry_events")

# Update sort order in table properties
with table.update_properties() as upd:
    upd["write.sort-order"] = "min_x ASC, max_x ASC, min_y ASC, max_y ASC"

# Execute compaction via PyIceberg maintenance API
# Note: PyIceberg's rewrite_data_files is available in PyIceberg 0.6+
table.rewrite_data_files(
    strategy="sort",
    sort_order=table.sort_order(),
    target_file_size_bytes=200 * 1024 * 1024  # 200MB
)
```

If spatial joins degrade after compaction, verify that PyIceberg preserved the sort order by checking `table.sort_order()` and ensuring no unsorted appends bypassed the compaction queue.

## Automated Maintenance & Lifecycle Management

Production spatial tables accumulate metadata bloat and orphaned Parquet files without disciplined lifecycle policies. Implement automated maintenance using PyIceberg's built-in maintenance routines, scheduled via Airflow, Dagster, or cloud-native schedulers.

```python
import time
from pyiceberg.catalog import load_catalog

catalog = load_catalog("default")

def run_maintenance(table_name: str, retention_days: int = 30):
    table = catalog.load_table(table_name)
    cutoff_ms = int(time.time() * 1000) - (retention_days * 86400 * 1000)

    # Expire snapshots older than retention window
    table.expire_snapshots(older_than_timestamp_ms=cutoff_ms)

    # Remove orphan files (files not referenced by any snapshot)
    table.remove_orphan_files(older_than_timestamp_ms=cutoff_ms)

    # Compact small files
    table.rewrite_data_files(
        strategy="binpack",
        target_file_size_bytes=150 * 1024 * 1024
    )
    print(f"Maintenance complete for {table_name}")
```

Set explicit retention parameters aligned with compliance requirements. For telemetry workloads, a 30-day snapshot retention with 7-day orphan cleanup balances time-travel debugging capabilities with metadata storage costs.

## CI/CD Validation & Pipeline Hardening

Spatial pipelines fail silently when schema drift or partition misalignment occurs. Embed validation gates in your CI/CD pipeline to enforce table contracts before deployment.

```yaml
# .github/workflows/spatial-validation.yml
name: PyIceberg Spatial Validation
on: [push, pull_request]
jobs:
  validate-spatial-schema:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - name: Install dependencies
        run: pip install pyiceberg pyarrow shapely pytest
      - name: Run spatial contract tests
        run: |
          python -m pytest tests/test_spatial_contracts.py -v
          python scripts/validate_partition_bounds.py \
            --table spatial_raw.telemetry_events \
            --crs EPSG:4326
```

The validation script should assert that:
1. All geometry columns are typed as `BINARY` (WKB), not `STRING`
2. Partition bounds align with expected H3/Geohash resolutions
3. CRS metadata (`EPSG:4326` or `EPSG:3857`) is explicitly stored in table properties
4. Sort order matches the declared `write.sort-order`

Fail fast on schema drift. Use `pyiceberg.schema.Schema` comparison to detect incompatible type promotions before they corrupt downstream spatial joins.

## Troubleshooting & Operational Runbook

| Symptom | Root Cause | Diagnostic Command | Remediation |
|---------|------------|-------------------|-------------|
| Excessive file scans despite partition filter | Manifest statistics misaligned with spatial bounds | `table.scan().plan_files()` | Rebuild partition spec; verify `grid_id` materialization logic |
| Sort order drift after concurrent writes | Unsorted appends bypassing compaction queue | `table.sort_order()` | Enforce `write.sort-order` at catalog level; schedule hourly `binpack` |
| High metadata overhead (>5GB) | Snapshot retention too long or too many versions | `table.history()` | Reduce `write.metadata.previous-versions-max`; run `expire_snapshots()` |
| CRS mismatch in spatial joins | Implicit projection during ingestion | `table.properties().get('crs')` | Standardize to `EPSG:4326` at ingestion; reject non-conforming records |

For authoritative reference on spatial coordinate systems and metadata standards, consult the [EPSG Registry](https://epsg.io/) for CRS validation and review the [OGC GeoPackage Specification](https://www.ogc.org/standards/geopackage) for geometry encoding best practices. Always validate spatial predicates against the official [PyIceberg Documentation](https://py.iceberg.apache.org/) to ensure API compatibility across minor releases.

Production spatial workflows require disciplined partitioning, explicit sort enforcement, and automated maintenance. By treating spatial metadata as first-class infrastructure and embedding validation gates into CI/CD pipelines, teams can achieve predictable query latency, controlled storage growth, and resilient schema evolution in the lakehouse.

## PyIceberg as a Metadata Tool First

The most valuable thing PyIceberg does for a spatial platform is not moving data — it is answering questions about tables cheaply, without an engine.

<figure class="diagram">
<svg viewBox="0 0 762 280" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Four classes of question PyIceberg answers from metadata alone: table layout and properties, per file bounding box statistics, snapshot history and sizes, and scan planning that produces a pruned file list for another engine to read">
<rect x="0" y="0" width="762" height="280" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Questions answerable without reading a single row</text>
<rect x="30" y="56" width="352" height="98" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="206" y="84" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">layout and contract</text>
<text x="206" y="108" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">partition spec, sort order, properties</text>
<text x="206" y="130" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">&#8220;is this table configured for spatial pruning?&#8221;</text>
<rect x="398" y="56" width="352" height="98" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="574" y="84" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">per-file statistics</text>
<text x="574" y="108" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">bbox bounds, row counts, file sizes</text>
<text x="574" y="130" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">&#8220;how well is this table clustered?&#8221;</text>
<rect x="30" y="170" width="352" height="98" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="206" y="198" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">snapshot history</text>
<text x="206" y="222" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">commit times, sizes, parent chain</text>
<text x="206" y="244" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">&#8220;when did this table change, and by how much?&#8221;</text>
<rect x="398" y="170" width="352" height="98" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="574" y="198" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">scan planning</text>
<text x="574" y="222" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">a pruned file list for a predicate</text>
<text x="574" y="244" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">&#8220;which files would this query read?&#8221;</text>
</svg>
</figure>

The bottom-right box is the one that unlocks the most useful pattern in the Python ecosystem: PyIceberg plans the scan and hands the resulting file list to something else — Arrow, DuckDB, Polars — for the actual compute. The catalog integration, the snapshot isolation and the partition pruning all come from PyIceberg; the execution comes from an engine that is very fast at reading Parquet. Neither has to be good at the other's job.

The top-right box is what makes the clustering metrics elsewhere on this site cheap to compute. Per-file bounds for the bounding-box columns are already in the manifests, so an overlap-factor calculation is a metadata read of a few hundred milliseconds rather than a scan. Running it nightly across every spatial table in a catalog is entirely practical.

## Writing With PyIceberg: What to Watch

Writes work well within a clear envelope, and the boundaries of that envelope are worth stating.

<figure class="diagram">
<svg viewBox="0 0 762 222" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Envelope for PyIceberg writes: comfortable for moderate append volumes with declared schemas, requiring care for large rewrites, wide geometry batches and heavy concurrent commits">
<rect x="0" y="0" width="762" height="222" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Where PyIceberg writes are comfortable</text>
<rect x="30" y="58" width="352" height="152" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="206" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#2f6e49">comfortable</text>
<text x="206" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">appends up to a few tens of GB</text>
<text x="206" y="138" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">declared Arrow schemas</text>
<text x="206" y="162" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">partition values derived up front</text>
<text x="206" y="186" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">one writer per partition</text>
<rect x="398" y="58" width="352" height="152" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="574" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#9a5a17">needs care</text>
<text x="574" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">full-table rewrites</text>
<text x="574" y="138" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">very wide geometry batches</text>
<text x="574" y="162" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">many concurrent committers</text>
<text x="574" y="186" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">schema evolution mid-stream</text>
</svg>
</figure>

The wide-geometry case is the one specific to spatial work. PyIceberg buffers what it writes, and a batch of large polygons occupies far more memory than its row count suggests — a hundred thousand complex boundaries can be several gigabytes in Arrow. Size batches by serialised bytes rather than by rows, and the writes stay predictable.

## Scan Planning as a Building Block

The scan-planning capability deserves its own treatment, because it is the piece that makes PyIceberg compose with everything else.

A scan takes a row filter and returns the set of data files that could contain matching rows, having applied partition pruning and file-statistics pruning along the way. That file list is an ordinary list of object paths, which means any reader can consume it: Arrow's dataset API, DuckDB's `read_parquet`, Polars, or a bespoke loop. The table format's guarantees — snapshot isolation, correct partition semantics, up-to-date statistics — are preserved because the planning came from the format, while the execution comes from whatever is fastest for the job.

For spatial work this is particularly valuable because the numeric bounding-box predicate is exactly the kind of filter the planner handles well. A scan filtered on the four bbox columns plus a partition value returns a small file list in milliseconds, and handing that list to DuckDB gives full spatial SQL over precisely the right data with no cluster involved.

```python
# PyIceberg 0.7+ plans; DuckDB executes. Neither does the other's job.
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThanOrEqual
import duckdb

tbl = load_catalog("prod").load_table("spatial.telemetry")
scan = tbl.scan(row_filter=And(
    GreaterThanOrEqual("bbox_max_x", 13.0), LessThanOrEqual("bbox_min_x", 13.8),
    GreaterThanOrEqual("bbox_max_y", 52.3), LessThanOrEqual("bbox_min_y", 52.7),
))
files = [t.file.file_path for t in scan.plan_files()]

con = duckdb.connect()
con.execute("INSTALL spatial; LOAD spatial;")
con.execute("SELECT count(*) FROM read_parquet($files) WHERE ST_Intersects("
            "ST_GeomFromWKB(geometry), ST_MakeEnvelope(13.0, 52.3, 13.8, 52.7))",
            {"files": files}).fetchone()
```

Two cautions. The file list is a **point-in-time** view: a concurrent commit does not invalidate it, because the files remain until expiry, but it does mean the result reflects the snapshot planned against rather than the newest one. That is usually the desired behaviour and should be stated explicitly rather than assumed. And delete files must be applied for merge-on-read tables — planning returns them alongside data files, and a reader that ignores them will return deleted rows.

## Catalog Choice and Its Consequences

PyIceberg speaks to several catalog implementations, and the choice affects a spatial workflow in ways that are easy to overlook until a migration.

A **REST catalog** is the most portable option and the one that keeps the Python client thinnest: authentication, table resolution and commit coordination all happen server-side, so the client needs no cloud credentials beyond object-storage access. For platforms where several engines share tables, this is usually the right default.

A **Hive metastore** works and carries the constraints of its age: coarse authorisation, a Thrift dependency, and awkwardness around concurrent commits at scale. It remains common because it already exists in many organisations, and PyIceberg's support for it is adequate for reading and moderate writing.

A **filesystem-backed catalog** is the simplest to stand up and the easiest to misuse. It coordinates commits through atomic object operations, which works well on some storage backends and not on others, and it offers no authorisation layer at all. It is excellent for development and for single-writer pipelines, and it should not be the production catalog for a table with several writers.

Whichever is chosen, keep the catalog configuration in one place and load it by name rather than constructing it in each script. PyIceberg reads a configuration file for exactly this reason, and the alternative — connection parameters inline in a dozen jobs — is the pattern that makes a catalog migration painful. It also keeps credentials out of the code, which matters more for the spatial platform than for most, because the data frequently carries location sensitivity of the kind covered in [security boundaries for GIS data](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/security-boundaries-for-gis-data/).

## Putting It Together

A mature PyIceberg-based spatial workflow tends to look like three cooperating pieces rather than one program.

The **writer** appends batches under a declared schema with derived spatial columns, sized by serialised bytes, committing in batches to keep metadata small. It is the only piece that needs write credentials, and it is the piece that enforces the table's contract.

The **reader** plans scans and hands file lists to a fast execution engine, so that analysis code never needs to know about partition columns, snapshots or delete files. It is where the helper functions live, and it is what makes the fast query path the default one for everybody else.

The **operator** runs on a schedule and touches no data at all: it audits table properties, computes clustering metrics from manifests, expires snapshots, reports on partition skew, and fails loudly when a table drifts from the contract. It is the cheapest of the three to build and the one most often missing.

Together they keep the platform's spatial tables in a state where the other guides on this site apply: partitioned sensibly, clustered usefully, described accurately, and cheap to query. Individually, each is a modest amount of code — a few hundred lines — and the discipline is in keeping them separate so that each has one job and one failure mode.

## Failure Modes Specific to PyIceberg

Four failures recur often enough to be worth naming, and each has a short fix.

**Schema mismatch on append.** A batch whose inferred Arrow schema differs from the table's — usually a nullable-versus-required difference or an all-null column inferring as null type — is rejected at commit. Declare the schema explicitly and cast every batch to it before writing; the error then surfaces at conversion, in the job that produced the batch, rather than at the end of a long write.

**Commit conflicts under concurrency.** Two writers appending to the same table can conflict on metadata even when their data does not overlap. Batch commits, back off with jitter, and where the conflict rate stays high, funnel commits through a single coordinator while keeping the data production parallel.

**Missing statistics on derived columns.** The default metrics mode truncates, which is useless for doubles. Set the bounding-box columns to full metrics explicitly at table creation, and audit for it — a table without them plans perfectly and reads everything.

**Stale catalog credentials in long-running jobs.** A job that runs for hours may outlive its token, and the failure appears at the commit rather than at the start, after all the work is done. Refresh credentials on a timer rather than acquiring them once, and make the commit step retry on an authentication failure as well as on a conflict.

None of these is subtle once seen, and all of them cost a wasted run the first time. Encoding the fixes into the shared writer module described above means each is paid for once across the whole platform.

## Where This Fits

PyIceberg is the piece that makes the rest of the Python ecosystem usable against a governed table, and its best use is as glue rather than as an engine. It knows what the table is, which files matter for a predicate, and how to commit a change safely; the fast work belongs to Arrow, DuckDB or Spark depending on scale. Workflows that respect that division stay simple, because each component is doing what it is good at. Workflows that try to make PyIceberg the compute layer, or that bypass it to read files directly, give up either performance or correctness — and the second is the more expensive of the two, because a stale file list produces answers that look right.

See [async execution patterns](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/async-execution-patterns/) for the concurrency model that surrounds these writes, and [lakehouse maintenance automation](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/lakehouse-maintenance-automation/) for the scheduled operator work.

## A Small Operational Dashboard

<figure class="diagram">
<svg viewBox="0 0 768 210" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Four metrics a PyIceberg operator job can publish from metadata alone: contract compliance, clustering overlap factor, snapshot count and partition skew">
<rect x="0" y="0" width="768" height="210" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Four numbers, all from metadata, per table per night</text>
<rect x="26" y="58" width="172" height="140" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="112" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">contract</text>
<text x="112" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">bbox columns present</text>
<text x="112" y="138" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">metrics full</text>
<text x="112" y="160" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">CRS declared</text>
<rect x="212" y="58" width="172" height="140" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="298" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">clustering</text>
<text x="298" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">overlap factor</text>
<text x="298" y="138" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">per partition</text>
<text x="298" y="160" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">trend over weeks</text>
<rect x="398" y="58" width="172" height="140" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="484" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">metadata size</text>
<text x="484" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">snapshot count</text>
<text x="484" y="138" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">manifest bytes</text>
<text x="484" y="160" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">planning latency</text>
<rect x="584" y="58" width="172" height="140" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="670" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">skew</text>
<text x="670" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">max ÷ median</text>
<text x="670" y="138" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">partition bytes</text>
<text x="670" y="160" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">and row counts</text>
</svg>
</figure>

All four are cheap enough to compute for every spatial table in a catalog in a few minutes, and together they answer the question that otherwise requires an incident to raise: which tables are drifting away from the layout they were designed with. Publish them as a small table rather than as an alert, review them monthly, and act on trends rather than on single readings.
