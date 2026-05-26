# DataFrame Mapping Strategies

Mapping spatial DataFrames to open table formats requires deliberate schema alignment, coordinate reference system (CRS) normalization, and storage layout optimization. Within modern spatial data lakehouse architectures, the choice between Apache Iceberg and Delta Lake dictates how geometry types, bounding boxes, and spatial indexes are materialized on disk. Establishing a consistent mapping strategy begins with understanding how Python-based transformation pipelines interact with underlying storage engines. The broader [Python Ecosystem & Integration Workflows](/python-ecosystem-integration-workflows/) establishes the hierarchy of tooling—from GeoPandas and Shapely to distributed compute frameworks—that must be orchestrated before data reaches the lakehouse layer.

## Geometry Serialization & Schema Contracts

DataFrame mapping in spatial contexts extends beyond simple column type coercion. Geometry columns must be serialized into a format compatible with the target table specification while preserving topological validity and minimizing storage bloat. When targeting Iceberg, WKB (Well-Known Binary) or native GeoParquet layouts are preferred, leveraging Iceberg’s support for nested types and schema evolution as defined in the [Apache Iceberg specification](https://iceberg.apache.org/spec/). Delta Lake relies on Parquet’s native geometry handling through community extensions or explicit string/WKB encoding until native spatial types mature.

In both cases, mapping pipelines should enforce CRS standardization at ingestion. Global analytical workloads typically standardize to `EPSG:4326` for coordinate storage, while web-mapping or raster-alignment pipelines project to `EPSG:3857` prior to write. Implement pre-write validation hooks that run `ST_IsValid` and `ST_IsSimple` checks; silently dropping invalid geometries during DataFrame mapping creates audit gaps and breaks spatial index assumptions. Adherence to the [OGC Simple Features specification](https://www.ogc.org/standards/sfa) ensures that serialized geometries maintain ring orientation and closure rules required by downstream query engines.

```python
import geopandas as gpd
import pyarrow as pa
from shapely.validation import make_valid

def map_and_validate_spatial_df(gdf: gpd.GeoDataFrame, target_crs: str = "EPSG:4326") -> pa.Table:
    # 1. CRS normalization
    if gdf.crs is None or gdf.crs.to_string() != target_crs:
        gdf = gdf.to_crs(target_crs)
    
    # 2. Geometry validation & repair
    invalid_mask = ~gdf.geometry.is_valid
    if invalid_mask.any():
        gdf.loc[invalid_mask, "geometry"] = gdf.loc[invalid_mask, "geometry"].apply(make_valid)
        
    # 3. WKB serialization for Parquet/Iceberg/Delta compatibility
    gdf["geometry_wkb"] = gdf.geometry.apply(lambda geom: geom.wkb)
    gdf = gdf.drop(columns=["geometry"])
    
    return pa.Table.from_pandas(gdf)
```

## Partitioning & Spatial Index Materialization

Spatial partitioning strategies directly impact query performance and compaction overhead. Range partitioning on raw latitude/longitude coordinates consistently leads to severe data skew in urban corridors or coastal boundaries. Instead, implement space-filling curve partitioning—Z-order or Hilbert curves—applied to projected coordinates. Iceberg handles this through hidden partitioning and sort-order metadata, allowing the query engine to prune files without exposing partition columns to downstream consumers. Delta Lake requires explicit partition columns or relies on Delta’s built-in Z-Ordering (`OPTIMIZE ... ZORDER BY`).

For high-throughput spatial joins, precompute spatial indexes (R-tree or quadtree) as auxiliary tables or materialized views rather than embedding them directly in the base DataFrame. Reference implementations for schema evolution, partition spec updates, and predicate pushdown validation are detailed in [PyIceberg Spatial Workflows](/python-ecosystem-integration-workflows/pyiceberg-spatial-workflows/). Always verify that spatial bounding boxes align with partition boundaries to maximize file pruning.

**Recommended Partition Parameters:**
- **Hilbert/Z-Order Level:** `12` to `14` (balances granularity with metadata overhead)
- **Partition Column:** `hilbert_idx` (computed from `x, y` after projection to `EPSG:3857`)
- **Target File Size:** `128MB - 512MB` per Parquet file
- **Snapshot Retention:** `30 days` (Iceberg) / `7 days` (Delta) with concurrent compaction

## Production Pipeline Implementation

Mapping pipelines must be deterministic, idempotent, and validated in CI before deployment. Below is a reference architecture combining Python transformation, SQL DDL, and CI schema enforcement.

**SQL DDL (Iceberg Example)**
```sql
CREATE TABLE spatial_analytics.asset_footprints (
    asset_id BIGINT,
    region_code VARCHAR(10),
    geometry_wkb BINARY,
    hilbert_idx BIGINT
)
PARTITIONED BY (hilbert_idx)
TBLPROPERTIES (
    'write.target-file-size-bytes'='268435456',
    'write.metadata.delete-after-commit.enabled'='true',
    'history.expire.max-snapshot-age-ms'='2592000000' -- 30 days
);
```

**CI/CD Schema Validation (GitHub Actions)**
```yaml
name: Validate Spatial Mapping Schema
on: [pull_request]
jobs:
  schema-check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Run PyArrow Schema Validation
        run: |
          python -c "
          import pandas as pd
          import pyarrow as pa
          schema = pa.schema([
              pa.field('asset_id', pa.int64()),
              pa.field('region_code', pa.string()),
              pa.field('geometry_wkb', pa.binary()),
              pa.field('hilbert_idx', pa.int64())
          ])
          df = pd.read_parquet('tests/fixtures/asset_footprints.parquet')
          assert schema.equals(pa.Schema.from_pandas(df)), 'Schema drift detected'
          print('Schema validation passed')
          "
```

When implementing Delta Lake mappings, leverage Rust-backed processing for lower memory overhead during WKB serialization and partition computation. Production patterns for high-throughput geometry ingestion and partition-aware writes are documented in [Delta-rs Geometry Processing](/python-ecosystem-integration-workflows/delta-rs-geometry-processing/). Ensure your pipeline enforces `OPTIMIZE` and `VACUUM` schedules aligned with your retention policy to prevent metadata bloat and orphaned file accumulation.

## Operational Troubleshooting Paths

| Symptom | Root Cause | Resolution Path |
|---------|------------|-----------------|
| **Query returns empty spatial join results** | CRS mismatch between joined tables or invalid bounding box alignment | Verify both tables use identical `EPSG` codes. Run `ST_Extent()` on source tables to confirm coordinate ranges overlap. Re-project and recompute `hilbert_idx`. |
| **High write latency / OOM during mapping** | Unoptimized WKB serialization or excessive geometry complexity | Apply `ST_SimplifyPreserveTopology` with tolerance `0.0001` degrees before serialization. Batch writes to `10k-50k` rows per partition. |
| **Partition skew (>80% data in 2-3 partitions)** | Raw lat/lon partitioning or low Hilbert level | Switch to `hilbert_idx` computed on `EPSG:3857`. Increase curve level to `13`. Run `REWRITE DATA` (Delta) or `rewrite_data_files` (Iceberg) to rebalance. |
| **Metadata directory grows unbounded** | Missing snapshot expiration or aggressive commit frequency | Set `history.expire.max-snapshot-age-ms` to `2592000000` (30 days). Schedule `expire_snapshots()` post-compaction. Monitor catalog metadata size via cloud storage metrics. |
| **Predicate pushdown fails on geometry columns** | Query engine cannot parse WKB for spatial pruning | Store bounding box coordinates (`min_x, min_y, max_x, max_y`) as explicit `DOUBLE` columns alongside `geometry_wkb`. Filter on bounding boxes first, then apply `ST_Intersects` post-read. |

Production spatial mapping requires strict governance around serialization contracts, partition topology, and retention policies. By standardizing on WKB/GeoParquet, enforcing CRS normalization at ingestion, and decoupling spatial indexes from base tables, engineering teams can maintain sub-second query latency while scaling to petabyte-level geospatial workloads.