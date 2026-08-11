# DataFrame Mapping Strategies

Mapping spatial DataFrames to open table formats requires deliberate schema alignment, coordinate reference system (CRS) normalization, and storage layout optimization. Within modern spatial data lakehouse architectures, the choice between Apache Iceberg and Delta Lake dictates how geometry types, bounding boxes, and spatial indexes are materialized on disk. Establishing a consistent mapping strategy begins with understanding how Python-based transformation pipelines interact with underlying storage engines. The broader [Python Ecosystem & Integration Workflows](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/) establishes the hierarchy of tooling—from GeoPandas and Shapely to distributed compute frameworks—that must be orchestrated before data reaches the lakehouse layer.

<figure class="diagram">
<svg viewBox="0 0 772 222" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="DataFrame mapping pipeline: GeoDataFrame, CRS normalize, schema map, Arrow table, table format write">
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

For high-throughput spatial joins, precompute spatial indexes (R-tree or quadtree) as auxiliary tables or materialized views rather than embedding them directly in the base DataFrame. Reference implementations for schema evolution, partition spec updates, and predicate pushdown validation are detailed in [PyIceberg Spatial Workflows](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/pyiceberg-spatial-workflows/). Always verify that spatial bounding boxes align with partition boundaries to maximize file pruning.

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

When implementing Delta Lake mappings, leverage Rust-backed processing for lower memory overhead during WKB serialization and partition computation. Production patterns for high-throughput geometry ingestion and partition-aware writes are documented in [Delta-rs Geometry Processing](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/delta-rs-geometry-processing/). Ensure your pipeline enforces `OPTIMIZE` and `VACUUM` schedules aligned with your retention policy to prevent metadata bloat and orphaned file accumulation.

## Operational Troubleshooting Paths

| Symptom | Root Cause | Resolution Path |
|---------|------------|-----------------|
| **Query returns empty spatial join results** | CRS mismatch between joined tables or invalid bounding box alignment | Verify both tables use identical EPSG codes. Run `ST_Extent()` on source tables to confirm coordinate ranges overlap. Re-project and recompute bbox columns. |
| **High write latency / OOM during mapping** | Unoptimized WKB serialization or excessive geometry complexity | Apply `ST_SimplifyPreserveTopology` with tolerance `0.0001` degrees before serialization. Batch writes to 10k–50k rows per chunk. |
| **Partition skew (>80% data in 2–3 partitions)** | Raw lat/lon partitioning or low-resolution H3 | Switch to `h3_res7` bucketed partition. Run `rewrite_data_files` (Iceberg) or `OPTIMIZE` (Delta) to rebalance. |
| **Metadata directory grows unbounded** | Missing snapshot expiration or aggressive commit frequency | Set `history.expire.max-snapshot-age-ms` to `2592000000` (30 days). Schedule `expire_snapshots()` post-compaction. |
| **Predicate pushdown fails on geometry columns** | Query engine cannot parse WKB for spatial pruning | Store bounding box coordinates as explicit `DOUBLE` columns alongside `geometry_wkb`. Filter on bounding boxes first, then apply `ST_Intersects` post-read. |

Production spatial mapping requires strict governance around serialization contracts, partition topology, and retention policies. By standardizing on WKB, enforcing CRS normalization at ingestion, and decoupling spatial indexes from base tables, engineering teams can maintain sub-second query latency while scaling to petabyte-level geospatial workloads.

## The Shape of the Mapping Problem

Moving a GeoDataFrame into a lakehouse table is a schema translation, and the translation has three independent axes that are easy to conflate: the geometry encoding, the attribute types, and the metadata that describes both.

<figure class="diagram">
<svg viewBox="0 0 764 268" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three independent axes of the mapping from a GeoDataFrame to a lakehouse table: geometry encoding, attribute type mapping including pandas nullable types, and the metadata layer carrying CRS and geometry types">
<rect x="0" y="0" width="764" height="268" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Three translations happening at once</text>
<rect x="26" y="56" width="230" height="200" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="141" y="84" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">geometry</text>
<text x="141" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">GeoSeries &#8594; binary</text>
<text x="141" y="138" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">choose WKB</text>
<text x="141" y="158" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">force 2D explicitly</text>
<text x="141" y="178" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">normalise empties to null</text>
<text x="141" y="204" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">derive bbox columns</text>
<text x="141" y="230" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">the part everyone remembers</text>
<rect x="274" y="56" width="230" height="200" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="389" y="84" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">attributes</text>
<text x="389" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">pandas dtypes &#8594; Arrow</text>
<text x="389" y="138" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">object columns are a trap</text>
<text x="389" y="158" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">int64 with nulls becomes float</text>
<text x="389" y="178" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">categoricals need a decision</text>
<text x="389" y="204" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">timestamps need a timezone</text>
<text x="389" y="230" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">the part that silently drifts</text>
<rect x="522" y="56" width="230" height="200" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="637" y="84" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">metadata</text>
<text x="637" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">frame state &#8594; file + table</text>
<text x="637" y="138" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">CRS as PROJJSON</text>
<text x="637" y="158" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">geometry types present</text>
<text x="637" y="178" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">edge interpretation</text>
<text x="637" y="204" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">primary column name</text>
<text x="637" y="230" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">the part that gets omitted</text>
</svg>
</figure>

The middle column causes more production incidents than the geometry does, precisely because it looks routine. An integer column containing nulls arrives in pandas as `float64`, which round-trips to a Parquet `DOUBLE`, which means an identifier column silently becomes a floating-point value and loses precision above 2⁵³. Declaring the Arrow schema explicitly rather than inferring it from the frame prevents the entire category, and it is the single highest-value habit in this area.

Object-dtype columns are the other recurring trap. A column of mixed types infers to Arrow as whatever the first non-null value suggests, and fails on the row where the type changes — usually far into a long write. Coerce every column to a declared type before conversion and let the coercion fail loudly at row zero.

## Writing the Schema Down Instead of Inferring It

<figure class="diagram">
<svg viewBox="0 0 766 256" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Comparison of inferred and declared schemas across four properties: stability between batches, behaviour when a column is all null, error timing, and reviewability in version control">
<rect x="0" y="0" width="766" height="256" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Inferred versus declared</text>
<rect x="26" y="52" width="220" height="34" rx="6" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="1.5"/>
<text x="136" y="75" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">property</text>
<rect x="254" y="52" width="240" height="34" rx="6" fill="#f2e8da" stroke="#9a5a17" stroke-width="1.5"/>
<text x="374" y="75" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">inferred</text>
<rect x="502" y="52" width="252" height="34" rx="6" fill="#e6f0ea" stroke="#2f6e49" stroke-width="1.5"/>
<text x="628" y="75" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">declared</text>
<text x="36" y="114" font-family="sans-serif" font-size="12" fill="#0d3b45">stability across batches</text>
<text x="264" y="114" font-family="sans-serif" font-size="12" fill="#9a5a17">varies with content</text>
<text x="512" y="114" font-family="sans-serif" font-size="12" fill="#2f6e49">identical every time</text>
<line x1="26" y1="128" x2="754" y2="128" stroke="#cfe3e7" stroke-width="1.5"/>
<text x="36" y="156" font-family="sans-serif" font-size="12" fill="#0d3b45">an all-null column</text>
<text x="264" y="156" font-family="sans-serif" font-size="12" fill="#9a5a17">becomes null type</text>
<text x="512" y="156" font-family="sans-serif" font-size="12" fill="#2f6e49">keeps its intended type</text>
<line x1="26" y1="170" x2="754" y2="170" stroke="#cfe3e7" stroke-width="1.5"/>
<text x="36" y="198" font-family="sans-serif" font-size="12" fill="#0d3b45">when errors surface</text>
<text x="264" y="198" font-family="sans-serif" font-size="12" fill="#9a5a17">mid-write, on row N</text>
<text x="512" y="198" font-family="sans-serif" font-size="12" fill="#2f6e49">immediately, on conversion</text>
<line x1="26" y1="212" x2="754" y2="212" stroke="#cfe3e7" stroke-width="1.5"/>
<text x="36" y="240" font-family="sans-serif" font-size="12" fill="#0d3b45">reviewability</text>
<text x="264" y="240" font-family="sans-serif" font-size="12" fill="#9a5a17">invisible in a diff</text>
<text x="512" y="240" font-family="sans-serif" font-size="12" fill="#2f6e49">a file in the repository</text>
</svg>
</figure>

The all-null row is the one that catches teams during backfills. A batch in which an optional attribute happens to be null for every row infers to Arrow's null type, which is not compatible with the table's declared type, and the write fails with a schema mismatch that appears to come from nowhere — the code did not change, the table did not change, only the content of one batch did. A declared schema removes the dependency on content entirely.

## Chunking Large Frames Without Losing the Contract

A GeoDataFrame that does not fit in memory has to be processed in chunks, and chunking introduces two problems that a single-pass conversion does not have.

The first is **schema consistency across chunks**. Every chunk must produce an identical Arrow schema, or the write fails partway through with a mismatch. This is another argument for a declared schema, but it also means the declaration must live outside the chunk loop and be passed in, rather than being derived from the first chunk and reused — a pattern that works until the first chunk happens to be unrepresentative.

The second is **statistics quality**. Writing many small chunks produces many small row groups, each with its own statistics, and a reader benefits from that only if the chunks are spatially coherent. Chunking an arbitrarily-ordered frame by row position produces row groups whose bounding boxes each cover the full extent, which is the within-file failure described elsewhere on this site. Sorting the frame by a spatial key before chunking costs one sort and makes every subsequent read cheaper.

```python
# Sort once, then chunk — each row group gets a compact extent.
import pyarrow as pa, pyarrow.parquet as pq

gdf = gdf.sort_values("h3_r7")                       # spatial coherence per chunk
schema = pa.schema([                                  # declared once, outside the loop
    ("feature_id", pa.int64()),
    ("h3_r7", pa.int64()),
    ("bbox_min_x", pa.float64()), ("bbox_min_y", pa.float64()),
    ("bbox_max_x", pa.float64()), ("bbox_max_y", pa.float64()),
    ("geometry", pa.binary()),
])

with pq.ParquetWriter("out.parquet", schema, compression="zstd") as writer:
    for start in range(0, len(gdf), 200_000):
        chunk = gdf.iloc[start:start + 200_000]
        writer.write_table(to_arrow(chunk, schema))   # to_arrow enforces the declared types
```

Chunk size trades memory against row-group count. Two hundred thousand rows of point data is a few tens of megabytes and produces row groups in a healthy size band; the same count of complex polygons may be several hundred megabytes, so size from the serialised footprint rather than from the row count. Measuring one representative chunk and dividing gives the number in a minute.

## Round-Tripping as the Acceptance Test

The mapping is correct when a frame written and read back is equivalent to the original, and "equivalent" needs defining precisely enough to test.

Geometry equivalence should be geometric rather than byte-level, at an explicit tolerance, because a valid round trip may normalise ring orientation or the starting vertex. Attribute equivalence should be type-aware: an integer column that returns as a float has failed even though every value compares equal. Metadata equivalence means the CRS, the geometry types and the primary column name all survive — which is the property most likely to be silently lost, since nothing downstream complains immediately.

Run the test on a fixture containing the awkward cases rather than on a sample of production data: a null geometry, an empty geometry, a multipolygon with a hole, an all-null attribute column, an integer column containing nulls, a timestamp at a DST boundary, and a string containing a non-ASCII character. Seven rows exercise more of the mapping than seven million typical ones, and the fixture stays fast enough to run on every commit.

## Reading Back Into a Frame

The reverse direction gets less attention and has its own set of choices, all of which affect how much memory the read costs.

Reading a whole table into a GeoDataFrame is convenient and rarely the right default, because it materialises every geometry as a Python-level object. For a table of ten million features that is several gigabytes of objects on top of the buffers, and most of the work that follows — filtering, aggregating, joining — would run faster on Arrow directly.

The pattern that scales is to **push filters into the reader and delay the geometry materialisation**. Read with a bounding-box predicate so entire row groups never leave storage, keep the result as an Arrow table, do the attribute work there, and convert to shapely geometries only for the rows that actually need geometric operations. On a typical analysis this reduces the number of geometry objects constructed by two or three orders of magnitude.

Where the whole frame genuinely is needed, read it in chunks and process incrementally rather than concatenating. Concatenating GeoDataFrames is where the CRS reset described earlier most often happens, and it doubles peak memory at the moment of concatenation, which is the moment the process is already at its largest.

One convenience worth building once: a small helper that takes a table name and a bounding box, resolves the partition cells, applies the numeric predicate, and returns an Arrow table with the geometry column still encoded. Every analysis in the codebase then starts from the same efficient path, and the knowledge about partition columns and predicate shape lives in one function rather than in every notebook.

## Keeping the Mapping in One Place

The recurring theme across every section of this page is that the mapping should exist once, as code, rather than being re-expressed in each pipeline that touches the table.

Concretely that means a small module per table family, exposing three functions: one that converts a GeoDataFrame to an Arrow table under the declared schema, one that converts back, and one that returns the schema itself so tests and writers share a single definition. Every pipeline imports it. Nothing constructs a schema inline.

The benefit is not tidiness but detectability. When the mapping lives in one module, a change to it is a diff that someone reviews, its tests run on every commit, and a library upgrade that alters behaviour fails visibly. When it lives in six notebooks, the same change happens five times correctly and once not, and the sixth produces a table whose integer identifiers are floats — which is exactly the kind of defect that survives for a year because every individual query returns something.

Version the module alongside the contract version described in the schema-evolution guidance, and record that version as a table property on write. The combination means a row can be traced back to the exact code that produced it, which is what makes a geometry discrepancy an investigation of minutes rather than of weeks.

## A Note on Performance Expectations

It is worth setting expectations about where the time goes in a mapping step, because the intuition is usually wrong.

Serialising geometry to WKB is fast — it is a memory copy with a small header per feature — and it rarely dominates. What dominates, on frames of any size, is Python-level iteration: any code that loops over rows to build a list, apply a function elementwise, or construct dictionaries will be an order of magnitude slower than the vectorised path, regardless of how simple the operation inside the loop looks.

The practical consequence is that a mapping written with vectorised operations throughout will convert millions of features per minute on a single core, while the same logic written with `.apply()` or an explicit loop will take an hour. When a conversion feels slow, the first thing to look for is not the geometry handling but the row-wise code that crept in around it — usually to handle a special case that could have been handled with a mask.

Bounding-box derivation is the one place where the vectorised path is not obvious, and it is worth doing properly: the array-level bounds function returns all four values for the whole column at once, and using it instead of iterating geometries typically accounts for more of the total speed-up than every other optimisation combined.

The mechanics of building the Arrow schema itself, including the metadata that makes the output valid GeoParquet, are worked through in [mapping GeoPandas dataframes to Arrow schemas](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/dataframe-mapping-strategies/mapping-geopandas-dataframes-to-arrow-schemas/).
That guide also covers the metadata block that turns an ordinary Parquet file into a self-describing spatial one.
Between them, the two pages cover the full round trip: frame in, table out, frame back, with nothing lost in either direction.
Keep both under the same tests, and a change to one cannot silently break the other.
That is the whole argument for centralising it: one definition, one set of tests, one place a change is reviewed.
