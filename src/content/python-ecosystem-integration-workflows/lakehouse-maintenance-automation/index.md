# Automating Lakehouse Maintenance for Spatial Tables

Spatial lakehouse tables degrade faster than their non-spatial counterparts. Streaming ingestion of GPS pings, tiled raster footprints, and vector features produces thousands of small Parquet files per hour, each carrying heavy WKB payloads and wide bounding-box statistics; every micro-batch mints a new snapshot; and every schema drift in an upstream shapefile or GeoParquet export threatens the geometry column's type contract. Left alone, an Iceberg or Delta table serving spatial queries will accumulate file counts that blow up planning time, snapshot histories that bloat metadata, and silent CRS or geometry-type corruption that only surfaces when a downstream `ST_Intersects` returns garbage. This guide shows data engineers and platform teams how to automate the three maintenance disciplines that keep spatial tables healthy — compaction, snapshot expiration and vacuum, and pre-write schema validation — driven entirely from Python and scheduled through Airflow or GitHub Actions. It sits inside the broader [Python Ecosystem & Integration Workflows](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/) section, and leans on the same PyIceberg, delta-rs, and PySpark tooling used across it.

## When to use this

Automated maintenance is not free — it consumes compute, holds table locks, and can break time-travel consumers if tuned carelessly. Decide where each discipline belongs before you schedule anything.

| Table symptom | Maintenance action | Trigger it when |
|---|---|---|
| Thousands of files < 32 MB per partition; slow query planning | Compaction (`rewrite_data_files` / `OPTIMIZE`) with spatial sort | Avg file size in a partition drops below ~64 MB or file count per partition exceeds ~200 |
| Metadata directory growing; slow catalog resolution; storage cost creep | Snapshot expiration + `VACUUM` / `remove_orphan_files` | Snapshot count exceeds retention SLA or orphan data files detected |
| Upstream schema drift; geometry column re-typed; CRS metadata missing | Pre-write schema validation gate | Every ingestion, before any commit reaches the table |

The rule of thumb: **validation runs on every write, compaction runs on a cadence proportional to write frequency, and expiration runs least often but with the strictest safety checks** because it is the only irreversible operation of the three. A hot IoT telemetry table might validate on every micro-batch, compact hourly, and expire snapshots nightly; a monthly-refreshed parcel boundary table might validate per load, compact after each load, and expire weekly.

<figure class="diagram">
<svg viewBox="0 0 760 300" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Scheduler dispatching compaction, vacuum and validation jobs against a spatial table with monitoring feedback">
<defs>
<marker id="arw-maint-flow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#0e6e7d"/></marker>
<marker id="arw-maint-mon" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#9a5a17"/></marker>
</defs>
<rect x="0" y="0" width="760" height="300" fill="#f7fbfc"/>
<text x="380" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Automated spatial-table maintenance loop</text>
<rect x="20" y="90" width="150" height="120" rx="8" fill="#ffffff" stroke="#6a3d9a" stroke-width="2"/>
<text x="95" y="120" text-anchor="middle" font-family="sans-serif" font-size="14" font-weight="600" fill="#0d3b45">Scheduler</text>
<text x="95" y="142" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">Airflow DAG /</text>
<text x="95" y="158" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">GitHub Actions</text>
<text x="95" y="180" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">cron cadence</text>
<rect x="250" y="55" width="180" height="52" rx="8" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<text x="340" y="78" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="600" fill="#0d3b45">Validation gate</text>
<text x="340" y="96" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">geometry / CRS / bbox</text>
<rect x="250" y="124" width="180" height="52" rx="8" fill="#ffffff" stroke="#0e6e7d" stroke-width="2"/>
<text x="340" y="147" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="600" fill="#0d3b45">Compaction</text>
<text x="340" y="165" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">rewrite + spatial sort</text>
<rect x="250" y="193" width="180" height="52" rx="8" fill="#ffffff" stroke="#9a5a17" stroke-width="2"/>
<text x="340" y="216" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="600" fill="#0d3b45">Expire + VACUUM</text>
<text x="340" y="234" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">safe retention</text>
<rect x="510" y="105" width="150" height="90" rx="8" fill="#ffffff" stroke="#0e6e7d" stroke-width="2"/>
<text x="585" y="140" text-anchor="middle" font-family="sans-serif" font-size="14" font-weight="600" fill="#0d3b45">Spatial table</text>
<text x="585" y="160" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">Iceberg / Delta</text>
<text x="585" y="178" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">+ snapshots</text>
<rect x="510" y="228" width="230" height="46" rx="8" fill="#ffffff" stroke="#9a5a17" stroke-width="2"/>
<text x="625" y="250" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="600" fill="#0d3b45">Monitoring</text>
<text x="625" y="267" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">file counts, snapshot age, metrics</text>
<line x1="170" y1="120" x2="250" y2="82" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-maint-flow)"/>
<line x1="170" y1="150" x2="250" y2="150" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-maint-flow)"/>
<line x1="170" y1="180" x2="250" y2="215" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-maint-flow)"/>
<line x1="430" y1="82" x2="510" y2="130" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-maint-flow)"/>
<line x1="430" y1="150" x2="510" y2="150" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-maint-flow)"/>
<line x1="430" y1="219" x2="510" y2="170" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-maint-flow)"/>
<line x1="585" y1="195" x2="600" y2="228" stroke="#9a5a17" stroke-width="2" marker-end="url(#arw-maint-mon)"/>
<polyline points="512,256 190,256 172,206" fill="none" stroke="#9a5a17" stroke-width="2" stroke-dasharray="5 4" marker-end="url(#arw-maint-mon)"/>
<text x="330" y="292" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#3d5a63">Monitoring feeds thresholds back to the scheduler so jobs run only when needed</text>
</svg>
</figure>

## Prerequisites and environment setup

The automation stack combines three libraries. PyIceberg drives table inspection and catalog operations; PySpark (with the Iceberg runtime) executes the `rewrite_data_files` and `expire_snapshots` procedures that PyIceberg does not yet run itself; and delta-rs handles Delta maintenance without a JVM. Pin versions explicitly — spatial procedures and the delta-rs optimize API have both changed shape across recent releases.

```bash
# Python 3.11 recommended
pip install "pyiceberg[s3fs,glue]==0.7.1" \
            "deltalake==0.18.2" \
            "pyspark==3.5.1" \
            "shapely==2.0.4" \
            "pyproj==3.6.1" \
            "pyarrow==16.1.0"

# Iceberg Spark runtime (1.9.x line) — matched to Spark 3.5
export ICEBERG_VERSION=1.9.0
export SPARK_JARS="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:${ICEBERG_VERSION}"
```

A minimal Spark session wired to a REST-backed Iceberg catalog, reused by the compaction and expiration jobs below:

```python
from pyspark.sql import SparkSession

def build_spark(app: str = "spatial-maintenance") -> SparkSession:
    return (
        SparkSession.builder.appName(app)
        .config("spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.0")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "rest")
        .config("spark.sql.catalog.lake.uri", "http://catalog:8181")
        .config("spark.sql.catalog.lake.warehouse", "s3://lakehouse/wh")
        # keep shuffle partitions aligned to executor cores for compaction
        .config("spark.sql.shuffle.partitions", "64")
        .getOrCreate()
    )
```

For the Delta path you need no JVM at all: delta-rs runs the OPTIMIZE and VACUUM operations natively in Rust, which is why the [Delta-rs Geometry Processing](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/delta-rs-geometry-processing/) workflows are the natural home for lightweight, containerized maintenance jobs.

## Step-by-step implementation

The three disciplines share a skeleton: inspect current state, decide whether the threshold is breached, execute the operation, then verify. Below is the orchestration spine; each discipline then gets its own deep-dive guide.

### Step 1 — Gate every write with schema validation

Validation must run **before** the commit, not after, because a bad geometry column type or a missing CRS is far cheaper to reject than to unwind from table history. The gate inspects the Arrow schema of the batch about to be written and asserts the geometry contract.

```python
import pyarrow as pa

REQUIRED_BBOX = ("bbox_minx", "bbox_miny", "bbox_maxx", "bbox_maxy")

def validate_batch(table: pa.Table, geom_col: str = "geometry") -> None:
    """Raise before write if the spatial contract is violated."""
    schema = table.schema
    # 1. geometry column exists and is binary (WKB)
    field = schema.field(geom_col)
    if not pa.types.is_binary(field.type) and not pa.types.is_large_binary(field.type):
        raise ValueError(f"{geom_col} must be WKB binary, got {field.type}")
    # 2. CRS metadata present on the field
    meta = field.metadata or {}
    if b"crs" not in meta:
        raise ValueError(f"{geom_col} missing CRS metadata")
    # 3. required bbox columns present
    missing = [c for c in REQUIRED_BBOX if c not in schema.names]
    if missing:
        raise ValueError(f"missing bbox columns: {missing}")
    # 4. no null geometries
    if table.column(geom_col).null_count > 0:
        raise ValueError("null geometry values are not allowed")
```

The full CI-integrated version — with CRS normalization, GeoParquet metadata checks, and a failing exit code — is covered in [building a schema validation pipeline for geospatial tables](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/lakehouse-maintenance-automation/schema-validation-pipeline-for-geospatial-tables/). Wiring it upstream of ingestion complements the drift detection described in [detecting CRS drift in ingestion pipelines](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/crs-management-pipelines/detecting-crs-drift-in-ingestion-pipelines/).

### Step 2 — Decide whether compaction is warranted

Do not compact on a blind schedule; compact when file fragmentation actually justifies the rewrite cost. PyIceberg exposes the current data files — the same table-inspection surface used across [PyIceberg spatial workflows](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/pyiceberg-spatial-workflows/) — so you can compute per-partition file counts and average size cheaply before committing any Spark cluster time.

```python
from pyiceberg.catalog import load_catalog

def needs_compaction(table_id: str, min_avg_mb: float = 64.0,
                     max_files: int = 200) -> bool:
    catalog = load_catalog("lake")
    tbl = catalog.load_table(table_id)
    files = list(tbl.scan().plan_files())
    if not files:
        return False
    total_bytes = sum(f.file.file_size_in_bytes for f in files)
    avg_mb = (total_bytes / len(files)) / (1024 * 1024)
    return avg_mb < min_avg_mb or len(files) > max_files
```

When the gate returns `True`, run the sort-based rewrite. The critical spatial detail is that the sort order must be on the bounding-box columns so that co-located geometries land in the same file — this is what makes later [predicate pushdown](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/z-ordering-for-geospatial-queries/) prune files effectively. The complete recipe, including partial-progress and target-file-size tuning, is in [compacting spatial Iceberg tables with rewrite_data_files](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/lakehouse-maintenance-automation/compacting-spatial-iceberg-tables-with-rewrite-data-files/).

```python
def compact_iceberg(spark, table_fqn: str) -> None:
    spark.sql(f"""
        CALL lake.system.rewrite_data_files(
          table => '{table_fqn}',
          strategy => 'sort',
          sort_order => 'bbox_minx ASC NULLS LAST, bbox_miny ASC NULLS LAST',
          options => map(
            'target-file-size-bytes', '134217728',
            'max-concurrent-file-group-rewrites', '4',
            'partial-progress.enabled', 'true',
            'partial-progress.max-commits', '10'
          )
        )
    """).show(truncate=False)
```

### Step 3 — Expire snapshots and vacuum with safe retention

Expiration is irreversible: once a snapshot is gone, its exclusive data files are deletable and time travel to that point is lost. The automation must therefore respect a retention floor that is the maximum of your longest-running query, your time-travel SLA, and your rollback window. For Iceberg, `expire_snapshots` prunes metadata and `remove_orphan_files` sweeps unreferenced data files.

```python
def expire_iceberg(spark, table_fqn: str, retain_days: int = 7) -> None:
    older_than = f"TIMESTAMP '{_days_ago(retain_days)}'"
    spark.sql(f"""
        CALL lake.system.expire_snapshots(
          table => '{table_fqn}',
          older_than => {older_than},
          retain_last => 5
        )
    """).show()
    # orphan sweep uses a conservative 3-day floor to avoid racing writers
    spark.sql(f"""
        CALL lake.system.remove_orphan_files(
          table => '{table_fqn}',
          older_than => TIMESTAMP '{_days_ago(3)}'
        )
    """).show()

def _days_ago(n: int) -> str:
    from datetime import datetime, timedelta, timezone
    return (datetime.now(timezone.utc) - timedelta(days=n)).strftime("%Y-%m-%d %H:%M:%S")
```

The Delta equivalent pairs `OPTIMIZE` (with Z-order on bbox columns) and `VACUUM`, and delta-rs enforces a 7-day retention floor by default that you should override only with eyes open. That full recipe, including how to guard time-travel SLAs, is in [scheduling VACUUM for spatial Delta tables](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/lakehouse-maintenance-automation/scheduling-vacuum-for-spatial-delta-tables/).

### Step 4 — Schedule the whole loop

Airflow expresses the dependency chain cleanly: validate is upstream of ingestion, compaction depends on ingestion, and expiration runs on its own slower cadence. Long-running Spark procedures should be dispatched off the scheduler thread — the same non-blocking principle developed in [async execution patterns](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/async-execution-patterns/).

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

with DAG(
    dag_id="spatial_table_maintenance",
    schedule="0 * * * *",          # hourly compaction cadence
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,             # never overlap maintenance runs
) as dag:

    def _compact():
        spark = build_spark()
        if needs_compaction("lake.gis.telemetry"):
            compact_iceberg(spark, "lake.gis.telemetry")

    def _expire():
        # gated to run only at 02:00 via branching or a separate DAG in practice
        if datetime.utcnow().hour == 2:
            expire_iceberg(build_spark(), "lake.gis.telemetry", retain_days=7)

    compact = PythonOperator(task_id="compact", python_callable=_compact)
    expire = PythonOperator(task_id="expire", python_callable=_expire)
    compact >> expire
```

For teams without Airflow, a scheduled GitHub Actions workflow (`on: schedule: - cron`) running the same Python entrypoints against a warehouse credential in secrets is a lightweight substitute, and it doubles as the place your validation gate already lives if you validate GeoParquet in CI — see [validating GeoParquet metadata in CI](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/geoparquet-encoding-standards/validating-geoparquet-metadata-in-ci/).

### Step 5 — Close the loop with monitoring

The dashed feedback arrow in the diagram is the difference between a maintenance system and a set of blind cron jobs. Every run should emit metrics that the scheduler's threshold gates read on the next cycle, so compaction fires only when fragmentation is real and expiration fires only when snapshot count actually exceeds the SLA. The three signals worth persisting are file-count-per-partition, average-file-size-per-partition, and oldest-retained-snapshot-age; all three come straight from Iceberg metadata tables at metadata-only cost.

```python
import time

def emit_table_metrics(spark, table_fqn: str) -> dict:
    """Collect maintenance signals for a monitoring backend."""
    files = spark.sql(f"""
        SELECT count(*) AS n,
               avg(file_size_in_bytes) / 1048576 AS avg_mb
        FROM lake.{table_fqn}.files
    """).collect()[0]
    snaps = spark.sql(f"""
        SELECT count(*) AS n,
               (unix_millis(current_timestamp()) - unix_millis(min(committed_at)))
                 / 86400000 AS oldest_days
        FROM lake.{table_fqn}.snapshots
    """).collect()[0]
    metrics = {
        "table": table_fqn,
        "ts": int(time.time()),
        "file_count": int(files["n"]),
        "avg_file_mb": round(float(files["avg_mb"] or 0), 1),
        "snapshot_count": int(snaps["n"]),
        "oldest_snapshot_days": round(float(snaps["oldest_days"] or 0), 1),
    }
    # push to StatsD / Prometheus pushgateway / a metrics table here
    print(metrics)
    return metrics
```

Alert when `avg_file_mb` trends downward run-over-run despite compaction (a sign the sort order is wrong or writes are outrunning maintenance), when `snapshot_count` grows unbounded (expiration is failing or not scheduled), or when `oldest_snapshot_days` drops below the time-travel SLA (expiration is too aggressive). These three alerts catch the overwhelming majority of maintenance regressions before a consumer notices. Delta tables expose the same signals through `DeltaTable.get_add_actions()` and `history()`, so a single monitoring schema covers both formats.

The one structural difference between the formats worth internalizing: Iceberg separates compaction (`rewrite_data_files`), snapshot expiration (`expire_snapshots`), and orphan cleanup (`remove_orphan_files`) into three explicit procedures you schedule independently, whereas Delta folds file production into `OPTIMIZE` and physical deletion into `VACUUM`, with the transaction log's `deletedFileRetentionDuration` acting as the safety floor. On Iceberg you therefore have finer control but more moving parts to schedule; on Delta you have fewer knobs but must respect the retention floor rather than an explicit per-run `older_than`. Neither is strictly better — pick the one whose operational surface your team can reason about, a trade-off examined more fully in the fundamentals section.

## Verification and testing

Every maintenance run must prove it improved the table without corrupting it. Three checks cover the common failure surface.

**File count and size after compaction.** Iceberg's `files` metadata table gives an exact post-run picture; a healthy compacted partition shows a small number of ~128 MB files.

```sql
-- Spark SQL against the Iceberg files metadata table
SELECT partition, count(*) AS file_count,
       cast(avg(file_size_in_bytes) / 1048576 AS int) AS avg_mb
FROM lake.gis.telemetry.files
GROUP BY partition
ORDER BY file_count DESC;
```

**Snapshot retention after expiration.** Confirm you kept exactly what the SLA requires and no more.

```sql
SELECT count(*) AS snapshots,
       min(committed_at) AS oldest
FROM lake.gis.telemetry.snapshots;
```

**Bounding-box integrity after any rewrite.** A rewrite must never alter data — only its layout. Compare aggregate bbox extents before and after; they must be identical.

```sql
SELECT min(bbox_minx) AS xmin, min(bbox_miny) AS ymin,
       max(bbox_maxx) AS xmax, max(bbox_maxy) AS ymax,
       count(*) AS n
FROM lake.gis.telemetry;
```

Row count and extent drift after a rewrite is a red flag that a filter or sort expression silently dropped rows — treat any change as a failed run and roll back to the pre-maintenance snapshot.

## Performance and tuning

Compaction throughput is dominated by shuffle and object-store round-trips, not by geometry decoding, provided you keep WKB in binary form rather than re-parsing to Shapely mid-rewrite. Concrete knobs and their ranges:

- **`target-file-size-bytes`**: `134217728` (128 MB) is the sweet spot for spatial scans. Smaller files (< 64 MB) inflate planning; larger files (> 256 MB) hurt selective bbox pruning because each file spans a wider spatial extent.
- **`max-concurrent-file-group-rewrites`**: 3–5. Above 5, S3/GCS request throttling (`503 SlowDown`) becomes the bottleneck and total wall-clock time gets *worse*.
- **`partial-progress.enabled`** + **`partial-progress.max-commits`** (10): essential for tables over ~1 TB so a mid-rewrite failure checkpoints instead of discarding hours of work.
- **`spark.sql.shuffle.partitions`**: match total executor cores; a 4-executor × 4-core cluster wants 16–64. Too high fragments the sort output back into small files.
- **VACUUM retention**: never below your longest query duration plus a safety margin. A 168-hour (7-day) floor is the delta-rs default; dropping to 1 hour to reclaim storage will corrupt any reader mid-scan.

Expected ratios: a partition of 500 files averaging 12 MB compacts to roughly 45 files of ~128 MB, cutting query planning time by 5–10× and shrinking the manifest list proportionally. Snapshot expiration on a table with 30 days of hourly snapshots (~720 snapshots) down to a 7-day retention typically reclaims 60–80% of metadata volume.

## Common errors and fixes

| Symptom | Root cause | Fix |
|---|---|---|
| `rewrite_data_files` runs but file count barely drops | Sort strategy chosen but no sort order given, or files already at target size | Pass an explicit `sort_order` on bbox columns; lower `min-input-files` in options to force grouping of near-target files |
| `503 SlowDown` / throttling errors during compaction | Too many concurrent file-group rewrites hammering object storage | Reduce `max-concurrent-file-group-rewrites` to 3; enable `partial-progress` so partial commits survive |
| Time-travel query fails with `snapshot ... not found` after nightly job | Expiration retention shorter than a consumer's time-travel SLA | Raise `retain_days` / `retain_last` above the SLA; align `history.expire.max-snapshot-age-ms` with it |
| `VACUUM` refuses to run below 168 hours | delta-rs safety floor preventing deletion of files an in-flight reader needs | Keep retention ≥ longest query; only override with `enforce_retention_duration=False` after confirming no active readers |
| Ingestion commits a re-typed geometry column and breaks downstream `ST_*` | No pre-write validation gate | Run the schema validation gate before every write; fail the pipeline on geometry-type or CRS violation |
| Row count changes after a rewrite | A filter/where expression leaked into the rewrite options | Remove any `where` from the rewrite call; rewrites must be layout-only — verify with the bbox integrity query |

Compaction, expiration, and validation are not independent chores — they compose into a single automated loop where validation protects the write, compaction protects query performance, and expiration protects storage cost, all under a scheduler that fires each only when monitoring says it is needed. Follow the three deep-dive guides for the complete, copy-paste implementations: [compacting spatial Iceberg tables with rewrite_data_files](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/lakehouse-maintenance-automation/compacting-spatial-iceberg-tables-with-rewrite-data-files/), [scheduling VACUUM for spatial Delta tables](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/lakehouse-maintenance-automation/scheduling-vacuum-for-spatial-delta-tables/), and [building a schema validation pipeline for geospatial tables](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/lakehouse-maintenance-automation/schema-validation-pipeline-for-geospatial-tables/). For authoritative reference, consult the [Apache Iceberg maintenance documentation](https://iceberg.apache.org/docs/latest/maintenance/), the Iceberg [Spark procedures reference](https://iceberg.apache.org/docs/latest/spark-procedures/), and the [Delta Lake utility commands](https://docs.delta.io/latest/delta-utility.html).
