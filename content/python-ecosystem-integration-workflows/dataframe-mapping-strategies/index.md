# DataFrame Mapping Strategies

Mapping spatial DataFrames to open table formats requires deliberate schema alignment, coordinate reference system (CRS) normalization, and storage layout optimization. Within modern spatial data lakehouse architectures, the choice between Apache Iceberg and Delta Lake dictates how geometry types, bounding boxes, and spatial indexes are materialized on disk. Establishing a consistent mapping strategy begins with understanding how Python-based transformation pipelines interact with underlying storage engines. The broader [Python Ecosystem & Integration Workflows](/python-ecosystem-integration-workflows/) establishes the hierarchy of tooling—from GeoPandas and Shapely to distributed compute frameworks—that must be orchestrated before data reaches the lakehouse layer.

<figure class="diagram">
<svg viewBox="0 0 760 210" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="DataFrame mapping pipeline: GeoDataFrame, CRS normalize, schema map, Arrow table, table format write">
<defs>
<marker id="map-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#7a4ea0"/></marker>
</defs>
<rect x="0" y="0" width="760" height="210" fill="#faf8fc"/>
<text x="380" y="30" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#3a1f50">Spatial DataFrame mapping pipeline</text>
<rect x="14" y="75" width="138" height="70" rx="8" fill="#ffffff" stroke="#7a4ea0" stroke-width="2"/>
<text x="83" y="105" text-anchor="middle" font-family="sans-serif" font-size="14" font-weight="600" fill="#3a1f50">GeoDataFrame</text>
<text x="83" y="125" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#6b4d80">Shapely geoms</text>
<rect x="196" y="75" width="138" height="70" rx="8" fill="#ffffff" stroke="#7a4ea0" stroke-width="2"/>
<text x="265" y="105" text-anchor="middle" font-family="sans-serif" font-size="14" font-weight="600" fill="#3a1f50">CRS Normalize</text>
<text x="265" y="125" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#6b4d80">EPSG:4326</text>
<rect x="378" y="75" width="138" height="70" rx="8" fill="#ffffff" stroke="#7a4ea0" stroke-width="2"/>
<text x="447" y="105" text-anchor="middle" font-family="sans-serif" font-size="14" font-weight="600" fill="#3a1f50">Schema Map</text>
<text x="447" y="125" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#6b4d80">WKB to BINARY</text>
<rect x="560" y="75" width="186" height="70" rx="8" fill="#ffffff" stroke="#7a4ea0" stroke-width="2"/>
<text x="653" y="105" text-anchor="middle" font-family="sans-serif" font-size="14" font-weight="600" fill="#3a1f50">Iceberg / Delta Write</text>
<text x="653" y="125" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#6b4d80">Arrow table</text>
<line x1="152" y1="110" x2="196" y2="110" stroke="#7a4ea0" stroke-width="2" marker-end="url(#map-arrow)"/>
<line x1="334" y1="110" x2="378" y2="110" stroke="#7a4ea0" stroke-width="2" marker-end="url(#map-arrow)"/>
<line x1="516" y1="110" x2="560" y2="110" stroke="#7a4ea0" stroke-width="2" marker-end="url(#map-arrow)"/>
<text x="380" y="185" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#5d4870">Each stage enforces topological validity before geometry reaches the lakehouse layer</text>
</svg>
</figure>

## Geometry Serialization & Schema Contracts

DataFrame mapping in spatial contexts extends beyond simple column type coercion. Geometry columns must be serialized into a format compatible with the target table specification while preserving topological validity and minimizing storage bloat. When targeting Iceberg, WKB (Well-Known Binary) is stored in `BINARY` columns, leveraging Iceberg's support for nested types and schema evolution as defined in the [Apache Iceberg specification](https://iceberg.apache.org/spec/). Delta Lake uses the same Parquet `BINARY` encoding for WKB.

In both cases, mapping pipelines should enforce CRS standardization at ingestion. Global analytical workloads typically standardize to `EPSG:4326` for coordinate storage, while web-mapping or raster-alignment pipelines project to `EPSG:3857` prior to write. Implement pre-write validation hooks that run `is_valid` and `is_simple` checks; silently dropping invalid geometries during DataFrame mapping creates audit gaps and breaks spatial index assumptions. Adherence to the [OGC Simple Features specification](https://www.ogc.org/standards/sfa) ensures that serialized geometries maintain ring orientation and closure rules required by downstream query engines.

```python
import geopandas as gpd
import pyarrow as pa
from shapely.validation import make_valid

def map_and_validate_spatial_df(gdf: gpd.GeoDataFrame, target_crs: str = "EPSG:4326") -> pa.Table:
    # 1. CRS normalization
    if gdf.crs is None or gdf.crs.to_epsg() != int(target_crs.split(":")[1]):
        gdf = gdf.to_crs(target_crs)

    # 2. Geometry validation & repair
    invalid_mask = ~gdf.geometry.is_valid
    if invalid_mask.any():
        gdf = gdf.copy()
        gdf.loc[invalid_mask, "geometry"] = gdf.loc[invalid_mask, "geometry"].apply(make_valid)

    # 3. WKB serialization for Parquet/Iceberg/Delta compatibility
    import shapely.wkb
    gdf = gdf.copy()
    gdf["geometry_wkb"] = gdf.geometry.apply(
        lambda g: shapely.wkb.dumps(g, include_srid=False)
    )
    gdf = gdf.drop(columns=["geometry"])

    return pa.Table.from_pandas(gdf)
```

## Partitioning & Spatial Index Materialization

Spatial partitioning strategies directly impact query performance and compaction overhead. Range partitioning on raw latitude/longitude coordinates consistently leads to severe data skew in urban corridors or coastal boundaries. Instead, implement space-filling curve partitioning—Z-order or Hilbert curves—applied to projected coordinates. Iceberg handles this through hidden partitioning and sort-order metadata, allowing the query engine to prune files without exposing partition columns to downstream consumers. Delta Lake requires explicit partition columns or relies on Delta's built-in Z-ordering (`OPTIMIZE ... ZORDER BY`).

For high-throughput spatial joins, precompute spatial indexes (R-tree or quadtree) as auxiliary tables or materialized views rather than embedding them directly in the base DataFrame. Reference implementations for schema evolution, partition spec updates, and predicate pushdown validation are detailed in [PyIceberg Spatial Workflows](/python-ecosystem-integration-workflows/pyiceberg-spatial-workflows/). Always verify that spatial bounding boxes align with partition boundaries to maximize file pruning.

**Recommended Partition Parameters:**
- **Spatial Key:** H3 resolution 7 string column bucketed via `bucket(128, h3_res7)` in Iceberg, or used directly as a partition column in Delta
- **Partition Column (in Delta):** `h3_res7` or coarser parent hex (to avoid small-files)
- **Target File Size:** 128MB–512MB per Parquet file
- **Snapshot Retention:** 30 days (Iceberg) / 7 days (Delta) with concurrent compaction

## Production Pipeline Implementation

Mapping pipelines must be deterministic, idempotent, and validated in CI before deployment.

**SQL DDL (Iceberg Example)**
```sql
CREATE TABLE spatial_analytics.asset_footprints (
    asset_id     BIGINT,
    region_code  VARCHAR(10),
    geometry_wkb BINARY,
    bbox_min_x   DOUBLE,
    bbox_min_y   DOUBLE,
    bbox_max_x   DOUBLE,
    bbox_max_y   DOUBLE
)
USING iceberg
PARTITIONED BY (bucket(128, asset_id))
TBLPROPERTIES (
    'write.target-file-size-bytes'='268435456',
    'write.sort-order'='bbox_min_x ASC, bbox_min_y ASC',
    'write.metadata.delete-after-commit.enabled'='true',
    'history.expire.max-snapshot-age-ms'='2592000000'  -- 30 days
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
          pip install pyarrow shapely
          python -c "
          import pyarrow as pa
          import pyarrow.parquet as pq

          expected_schema = pa.schema([
              pa.field('asset_id',     pa.int64()),
              pa.field('region_code',  pa.string()),
              pa.field('geometry_wkb', pa.binary()),
              pa.field('bbox_min_x',   pa.float64()),
              pa.field('bbox_min_y',   pa.float64()),
              pa.field('bbox_max_x',   pa.float64()),
              pa.field('bbox_max_y',   pa.float64()),
          ])

          actual = pq.read_schema('tests/fixtures/asset_footprints.parquet')
          assert actual.equals(expected_schema), \
              f'Schema drift detected.\nExpected: {expected_schema}\nActual: {actual}'
          print('Schema validation passed')
          "
```

When implementing Delta Lake mappings, leverage Rust-backed processing for lower memory overhead during WKB serialization and partition computation. Production patterns for high-throughput geometry ingestion and partition-aware writes are documented in [Delta-rs Geometry Processing](/python-ecosystem-integration-workflows/delta-rs-geometry-processing/). Ensure your pipeline enforces `OPTIMIZE` and `VACUUM` schedules aligned with your retention policy to prevent metadata bloat and orphaned file accumulation.

## Operational Troubleshooting Paths

| Symptom | Root Cause | Resolution Path |
|---------|------------|-----------------|
| **Query returns empty spatial join results** | CRS mismatch between joined tables or invalid bounding box alignment | Verify both tables use identical EPSG codes. Run `ST_Extent()` on source tables to confirm coordinate ranges overlap. Re-project and recompute bbox columns. |
| **High write latency / OOM during mapping** | Unoptimized WKB serialization or excessive geometry complexity | Apply `ST_SimplifyPreserveTopology` with tolerance `0.0001` degrees before serialization. Batch writes to 10k–50k rows per chunk. |
| **Partition skew (>80% data in 2–3 partitions)** | Raw lat/lon partitioning or low-resolution H3 | Switch to `h3_res7` bucketed partition. Run `rewrite_data_files` (Iceberg) or `OPTIMIZE` (Delta) to rebalance. |
| **Metadata directory grows unbounded** | Missing snapshot expiration or aggressive commit frequency | Set `history.expire.max-snapshot-age-ms` to `2592000000` (30 days). Schedule `expire_snapshots()` post-compaction. |
| **Predicate pushdown fails on geometry columns** | Query engine cannot parse WKB for spatial pruning | Store bounding box coordinates as explicit `DOUBLE` columns alongside `geometry_wkb`. Filter on bounding boxes first, then apply `ST_Intersects` post-read. |

Production spatial mapping requires strict governance around serialization contracts, partition topology, and retention policies. By standardizing on WKB, enforcing CRS normalization at ingestion, and decoupling spatial indexes from base tables, engineering teams can maintain sub-second query latency while scaling to petabyte-level geospatial workloads.
