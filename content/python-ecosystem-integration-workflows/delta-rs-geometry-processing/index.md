# Delta-rs Geometry Processing

Spatial data lakehouse architectures increasingly rely on Rust-backed table formats to handle high-throughput geometry workloads at cloud scale. Within the broader [Python Ecosystem & Integration Workflows](/python-ecosystem-integration-workflows/), `delta-rs` has emerged as a critical runtime for bridging GIS backends with object storage. Unlike legacy shapefile or GeoPackage pipelines, `delta-rs` operates directly on Parquet with ACID transactional guarantees, but geometry columns introduce unique serialization, partitioning, and compaction challenges. This guide targets platform engineers and GIS backend developers implementing `delta-rs` in production, focusing on operational configuration, debugging patterns, and format-specific trade-offs.

## Partitioning Strategies for Spatial Data

Geometry data inherently defies standard range or hash partitioning. Effective spatial partitioning requires mapping 2D/3D coordinates to discrete bucket keys without introducing severe data skew. `delta-rs` supports partition evolution, but spatial workloads benefit from hierarchical grid systems (H3, S2, or QuadKey) applied as string partition columns. When configuring `partition_by` in `delta-rs`, avoid partitioning directly on WKB/WKT columns; instead, compute a spatial index key upstream during ingestion.

For workloads requiring frequent bounding-box predicates, Z-ordering on coordinate bounds (`min_x`, `max_x`, `min_y`, `max_y`) significantly reduces scan overhead. The following Python pipeline demonstrates H3 index generation at resolution 7, explicit CRS tagging, and write configuration using `deltalake` (delta-rs Python bindings) with h3-py 4.x:

```python
import pyarrow as pa
import pandas as pd
import shapely.wkb
import h3  # h3-py 4.x: h3.latlng_to_cell()
from deltalake import write_deltalake

# Assume df is a GeoDataFrame with 'geometry' as Shapely objects in EPSG:4326
def compute_spatial_partitions(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["crs"] = "EPSG:4326"

    # Serialize geometry to WKB bytes
    df["geometry"] = df["geometry"].apply(
        lambda g: shapely.wkb.dumps(g, include_srid=False) if g is not None else None
    )

    # Extract bounding box for data skipping
    bounds = df["geometry"].apply(
        lambda b: shapely.wkb.loads(b).bounds if b else (None, None, None, None)
    )
    df["min_x"] = [b[0] for b in bounds]
    df["min_y"] = [b[1] for b in bounds]
    df["max_x"] = [b[2] for b in bounds]
    df["max_y"] = [b[3] for b in bounds]

    # Compute H3 index from centroid (h3-py 4.x API)
    def to_h3(wkb_bytes):
        if not wkb_bytes:
            return None
        c = shapely.wkb.loads(wkb_bytes).centroid
        return h3.latlng_to_cell(c.y, c.x, 7)

    df["h3_res7"] = df["geometry"].apply(to_h3)
    return df

df_partitioned = compute_spatial_partitions(df)
schema = pa.schema([
    ("id",      pa.int64()),
    ("geometry", pa.binary()),   # WKB bytes
    ("h3_res7", pa.string()),
    ("min_x",   pa.float64()), ("max_x", pa.float64()),
    ("min_y",   pa.float64()), ("max_y", pa.float64()),
    ("crs",     pa.string())
])

arrow_table = pa.Table.from_pandas(df_partitioned, schema=schema)
write_deltalake(
    "s3://spatial-lakehouse/raw/parcels",
    arrow_table,
    partition_by=["h3_res7"],
    mode="append"
)
```

Debugging partition skew involves inspecting `_delta_log` JSON commit files and monitoring file size distribution via `DeltaTable.get_add_actions()`. If you observe >10x variance in partition file counts, re-evaluate grid resolution or implement dynamic partition pruning. In CI/CD pipelines, enforce partition validation by asserting that spatial keys align with expected geographic extents before committing writes. For deeper schema alignment patterns, review [DataFrame Mapping Strategies](/python-ecosystem-integration-workflows/dataframe-mapping-strategies/) when designing ingestion contracts.

## Spatial Indexing & Data Skipping Trade-offs

`delta-rs` relies on Parquet column statistics and data skipping rather than explicit spatial indexes like PostGIS or GeoMesa. This creates a fundamental architectural divergence from [PyIceberg Spatial Workflows](/python-ecosystem-integration-workflows/pyiceberg-spatial-workflows/), where Iceberg's hidden partitioning and manifest-level metadata can be tuned for spatial predicate pushdown without altering table schemas. In `delta-rs`, you must explicitly materialize spatial bounds as separate columns to enable data skipping.

Configure table properties to force the query engine to index your geometry-derived bounds during compaction:

```sql
CREATE OR REPLACE TABLE parcels_spatial (
    id      BIGINT,
    geometry BINARY,
    min_x   DOUBLE, max_x DOUBLE,
    min_y   DOUBLE, max_y DOUBLE,
    h3_res7 STRING
)
USING DELTA
LOCATION 's3://spatial-lakehouse/curated/parcels'
TBLPROPERTIES (
    'delta.dataSkippingNumIndexedCols' = '6',
    'delta.columnMapping.mode' = 'name',
    'delta.checkpointInterval' = '10',
    'delta.enableDeletionVectors' = 'true'
);
```

Debugging missed data skipping: enable `RUST_LOG=delta_kernel=debug` when running `delta-rs` directly, and verify that `min_x`/`max_x` statistics are populated in the Parquet metadata footer. For complex polygons, compute convex hull bounds or centroid coordinates during ingestion to avoid bounding-box inflation. Always validate CRS consistency at the ingestion layer; mixing EPSG:3857 and EPSG:4326 in the same partition will silently corrupt spatial predicates. Reference the official [EPSG Geodetic Parameter Dataset](https://epsg.org/) for authoritative coordinate reference system definitions.

## Maintenance, Compaction, and Retention

Geometry columns introduce significant storage overhead. WKB serialization typically inflates row sizes by 30–50% compared to native coordinate arrays, making aggressive compaction and retention policies mandatory. `delta-rs` provides bin-packing compaction and `VACUUM` (garbage collection) operations that must be scheduled via orchestration layers (Airflow, Dagster, or Kubernetes CronJobs).

```python
from deltalake import DeltaTable

dt = DeltaTable("s3://spatial-lakehouse/curated/parcels")

# Bin-pack small files into 1GB targets, preserving partition boundaries
dt.optimize.compact(target_size=1024 * 1024 * 1024)

# Remove untracked files older than 30 days (720 hours)
# Default retention is 7 days; extend for spatial audit compliance
dt.vacuum(retention_hours=720, dry_run=False, enforce_retention_duration=True)
```

Set explicit retention parameters in table properties to prevent transaction log bloat:
- `delta.logRetentionDuration = interval 30 days`
- `delta.deletedFileRetentionDuration = interval 7 days`
- `delta.enableExpiredLogCleanup = true`

When writing spatial Parquet files, ensure the Rust writer is configured to handle large binary columns efficiently. Refer to [Using delta-rs to write spatial parquet files](/python-ecosystem-integration-workflows/delta-rs-geometry-processing/using-delta-rs-to-write-spatial-parquet-files/) for serialization benchmarks and memory tuning guidance.

## CI/CD Validation & Schema Enforcement

Production spatial tables fail silently when CRS drift or invalid geometries bypass ingestion gates. Implement pre-commit validation using `pyproj` and `shapely` to enforce topological integrity before `delta-rs` commits:

```yaml
# .github/workflows/spatial-validation.yml
name: Spatial Schema Validation
on: [pull_request]
jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Validate CRS & Geometry
        run: |
          pip install shapely pyproj pyarrow deltalake
          python scripts/validate_spatial_schema.py
```

```python
# scripts/validate_spatial_schema.py
import pyarrow.parquet as pq
import shapely.wkb
from pyproj import CRS

def validate_table(path: str):
    pf = pq.ParquetFile(path)
    schema = pf.schema_arrow
    assert "crs" in schema.names, "Missing CRS column"
    assert schema.field("geometry").type == pq.lib.binary(), "geometry must be BINARY"

    # Sample first row group for topology check
    batch = pf.read_row_group(0)
    geoms = [
        shapely.wkb.loads(b.as_py())
        for b in batch.column("geometry")
        if b.as_py() is not None
    ]
    invalid = [i for i, g in enumerate(geoms) if not g.is_valid]
    assert len(invalid) == 0, f"Invalid geometries at indices: {invalid}"

    crs_val = CRS.from_user_input(batch.column("crs")[0].as_py())
    assert crs_val.to_epsg() == 4326, "CRS mismatch: expected EPSG:4326"
    print("Spatial schema validation passed")

if __name__ == "__main__":
    validate_table("tests/fixtures/sample_parcels.parquet")
```

## Production Troubleshooting Paths

| Symptom | Root Cause | Diagnostic Command / Fix |
|---------|------------|--------------------------|
| Full table scans on `ST_Intersects` | Bounds columns missing from data skipping index | Verify `delta.dataSkippingNumIndexedCols` covers bound columns. Re-run `OPTIMIZE` to rebuild stats. |
| `DeltaError: Transaction log too large` | Checkpoint interval too low or log cleanup disabled | Set `delta.checkpointInterval = 10`. Enable `delta.enableExpiredLogCleanup`. Run `VACUUM`. |
| Partition skew (>10x file count variance) | H3 resolution mismatch with data density | Downgrade H3 res (e.g., 7 → 6) for sparse regions. Implement dynamic partition pruning in query layer. |
| WKB deserialization failures | Mixed endianness or invalid GeoParquet encoding | Enforce `geometry` as `pa.binary()` with little-endian WKB (`shapely.wkb.dumps(geom, little_endian=True)`). Validate against [GeoParquet Specification](https://github.com/opengeospatial/geoparquet). |
| Query timeout on spatial joins | Missing Z-ordering on coordinate bounds | Apply `ZORDER BY min_x, max_x, min_y, max_y` during `OPTIMIZE`. Ensure predicate pushdown is enabled in query engine. |

For persistent transaction conflicts, inspect the `_delta_log` directory for concurrent commit collisions. Use `delta-rs` conflict resolution policies (`MergeSchema` or retry with backoff) to serialize geometry updates safely. Consult the official [Delta Lake Transaction Protocol](https://docs.delta.io/latest/delta-protocol.html) for isolation level guarantees and conflict resolution semantics.
