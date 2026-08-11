# Python Ecosystem & Integration Workflows for Spatial Lakehouses

The Python ecosystem functions as the primary integration and orchestration fabric for modern spatial data lakehouses. While table formats like Apache Iceberg and Delta Lake enforce transactional guarantees, schema evolution, and catalog metadata, Python bridges the gap between raw geospatial ingestion, analytical transformation, and downstream consumption. These integration workflows establish the architectural contracts, compute boundaries, and operational guardrails required to run production-grade spatial workflows without compromising query performance, data integrity, or infrastructure efficiency.

<figure class="diagram">
<svg viewBox="0 0 757 218" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="The Python write contract for spatial tables: in-memory GeoPandas geometry passes through Apache Arrow as the zero-copy interchange layer to PyIceberg and delta-rs writers, into the open table format, with a CI/CD validation gate guarding commits">
<defs>
<marker id="py-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#0e6e7d"/></marker>
</defs>
<rect x="0" y="0" width="757" height="218" fill="#f7fbfc"/>
<text x="380" y="26" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">The Python write contract for spatial tables</text>
<rect x="15" y="50" width="165" height="80" rx="8" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<text x="97" y="82" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">GeoPandas</text>
<text x="97" y="104" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">GeoDataFrame</text>
<rect x="210" y="50" width="165" height="80" rx="8" fill="#ffffff" stroke="#0e6e7d" stroke-width="3"/>
<text x="292" y="82" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">Apache Arrow</text>
<text x="292" y="104" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">zero-copy interchange</text>
<rect x="405" y="50" width="165" height="80" rx="8" fill="#ffffff" stroke="#9a5a17" stroke-width="2"/>
<text x="487" y="82" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">PyIceberg / delta-rs</text>
<text x="487" y="104" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">table writers</text>
<rect x="600" y="50" width="145" height="80" rx="8" fill="#ffffff" stroke="#6a3d9a" stroke-width="2"/>
<text x="672" y="82" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">Open table</text>
<text x="672" y="104" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">Iceberg / Delta</text>
<line x1="180" y1="90" x2="210" y2="90" stroke="#0e6e7d" stroke-width="2" marker-end="url(#py-arrow)"/>
<line x1="375" y1="90" x2="405" y2="90" stroke="#0e6e7d" stroke-width="2" marker-end="url(#py-arrow)"/>
<line x1="570" y1="90" x2="600" y2="90" stroke="#0e6e7d" stroke-width="2" marker-end="url(#py-arrow)"/>
<rect x="475" y="160" width="220" height="46" rx="8" fill="#ffffff" stroke="#0e6e7d" stroke-width="2" stroke-dasharray="6 4"/>
<text x="585" y="182" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">CI/CD validation gate</text>
<text x="585" y="199" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">schema · CRS · bbox checks</text>
<line x1="585" y1="160" x2="585" y2="133" stroke="#0e6e7d" stroke-width="2" marker-end="url(#py-arrow)"/>
<text x="245" y="150" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">Arrow is the interchange contract between Python geometry and the writers</text>
</svg>
</figure>

## Core Architecture & Type Contracts

At the foundation of any spatial lakehouse is a strict contract between the object storage layer and the distributed compute engine. Python libraries must translate between native spatial representations (WKB, GeoJSON, PostGIS `GEOMETRY`) and the columnar Parquet-backed formats expected by Iceberg or Delta. Misaligned type mappings are the primary cause of silent data corruption during ingestion, particularly when CRS metadata or topology constraints are stripped during serialization. Engineers must enforce explicit Arrow schema declarations and leverage vectorized conversion routines to maintain throughput across distributed workers. The [DataFrame Mapping Strategies](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/dataframe-mapping-strategies/) reference details how to align pandas, Polars, and GeoPandas structures with lakehouse schemas while preserving spatial attributes and avoiding implicit type promotion.

Production pipelines should standardize on the [GeoParquet specification](https://geoparquet.org/) to ensure interoperability across engines. Below is a production-ready schema definition that enforces WKB storage, explicit CRS metadata, and prevents implicit casting during ingestion:

```python
import pyarrow as pa
import pyarrow.parquet as pq

# Explicit spatial schema contract
spatial_schema = pa.schema([
    pa.field("parcel_id", pa.string(), nullable=False),
    pa.field("geometry", pa.binary(), nullable=False),  # WKB encoded
    pa.field("crs", pa.string(), nullable=False),
    pa.field("updated_at", pa.timestamp("us", tz="UTC"))
])

# Attach GeoParquet metadata to the schema
geo_metadata = b'{"columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Polygon", "MultiPolygon"]}}}'
spatial_schema = spatial_schema.with_metadata({"geo": geo_metadata})

def validate_and_cast(df: pa.Table) -> pa.Table:
    return df.cast(spatial_schema, safe=True)
```

When interacting directly with Iceberg catalogs, Python must respect snapshot isolation and partition pruning semantics to prevent read-write conflicts. The [PyIceberg Spatial Workflows](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/pyiceberg-spatial-workflows/) guide outlines the exact API patterns for transactional reads, metadata compaction, and partition alignment required to maintain consistency across concurrent geospatial queries. Delta Lake implementations require similar discipline: explicit `OPTIMIZE` and `ZORDER BY` operations on spatial bounding boxes or centroid columns should be scheduled during low-traffic windows to maintain query performance without saturating storage I/O.

## The Type Contract Across Library Boundaries

A spatial Python pipeline is a chain of type conversions, and every link in that chain can lose information silently. The geometry that leaves Shapely is not the geometry that arrives in a Parquet file unless each handover is explicit about encoding, coordinate reference system and null handling. Debugging a pipeline is usually the work of finding which link dropped what.

<figure class="diagram">
<svg viewBox="0 0 768 314" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Conversion chain from Shapely geometry through GeoPandas, PyArrow and the table format, annotated with what each handover can lose: CRS, Z coordinates, geometry metadata and null semantics">
<defs>
<marker id="py-chain-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#9a5a17"/></marker>
</defs>
<rect x="0" y="0" width="768" height="314" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">What each handover can silently drop</text>
<rect x="24" y="62" width="150" height="72" rx="8" fill="#ffffff" stroke="#9a5a17" stroke-width="2"/>
<text x="99" y="90" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">Shapely</text>
<text x="99" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">geometry object</text>
<rect x="216" y="62" width="150" height="72" rx="8" fill="#ffffff" stroke="#9a5a17" stroke-width="2"/>
<text x="291" y="90" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">GeoPandas</text>
<text x="291" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">GeoSeries + crs</text>
<rect x="408" y="62" width="150" height="72" rx="8" fill="#ffffff" stroke="#0e6e7d" stroke-width="2"/>
<text x="483" y="90" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">PyArrow</text>
<text x="483" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">binary column</text>
<rect x="600" y="62" width="156" height="72" rx="8" fill="#ffffff" stroke="#6a3d9a" stroke-width="2"/>
<text x="678" y="90" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">Iceberg / Delta</text>
<text x="678" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">committed file</text>
<line x1="174" y1="98" x2="216" y2="98" stroke="#9a5a17" stroke-width="2" marker-end="url(#py-chain-arrow)"/>
<line x1="366" y1="98" x2="408" y2="98" stroke="#9a5a17" stroke-width="2" marker-end="url(#py-chain-arrow)"/>
<line x1="558" y1="98" x2="600" y2="98" stroke="#9a5a17" stroke-width="2" marker-end="url(#py-chain-arrow)"/>
<rect x="196" y="168" width="190" height="96" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="291" y="192" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">loses: nothing yet</text>
<text x="291" y="214" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">but CRS now lives on the</text>
<text x="291" y="230" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">column, not the geometry —</text>
<text x="291" y="246" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">a concat can reset it</text>
<rect x="404" y="168" width="182" height="96" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="495" y="192" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">loses: CRS, geometry type</text>
<text x="495" y="214" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">WKB is opaque bytes;</text>
<text x="495" y="230" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">re-attach it as schema</text>
<text x="495" y="246" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">metadata explicitly</text>
<rect x="600" y="168" width="156" height="96" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="678" y="192" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">loses: bbox stats</text>
<text x="678" y="214" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">if the derived columns</text>
<text x="678" y="230" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">were never computed,</text>
<text x="678" y="246" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">skipping is impossible</text>
<line x1="291" y1="134" x2="291" y2="168" stroke="#9a5a17" stroke-width="2" marker-end="url(#py-chain-arrow)"/>
<line x1="483" y1="134" x2="490" y2="168" stroke="#0e6e7d" stroke-width="2" marker-end="url(#py-chain-arrow)"/>
<line x1="678" y1="134" x2="678" y2="168" stroke="#6a3d9a" stroke-width="2" marker-end="url(#py-chain-arrow)"/>
<text x="390" y="298" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">Assert the contract at each boundary; none of these losses raises an exception</text>
</svg>
</figure>

The Shapely-to-GeoPandas step is the safest, but it introduces a subtle relocation: the coordinate reference system stops being a property of the data and becomes a property of the column. Operations that rebuild the column — `pd.concat` of frames with mismatched CRS, a `groupby().apply()` that returns plain Series, a merge that reorders — can produce a GeoDataFrame whose `.crs` is `None`. Nothing raises; the frame simply forgets where on Earth it is. Assert `gdf.crs is not None and gdf.crs.to_epsg() == 4326` after any structural operation, not just at load.

The GeoPandas-to-Arrow step is where most information is lost by design. Serialising to WKB produces an opaque `binary` column: no CRS, no declared geometry type, no dimensionality. That is the correct wire format, but it means the metadata must travel separately, as GeoParquet schema metadata or as explicit table properties. The detail of doing this properly is in [mapping GeoPandas dataframes to Arrow schemas](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/dataframe-mapping-strategies/mapping-geopandas-dataframes-to-arrow-schemas/); the principle is that a reader must be able to reconstruct the full spatial meaning from the file alone, with no reference to the code that wrote it.

Dimensionality deserves a specific warning. Shapely happily holds Z coordinates, WKB encodes them, and many downstream consumers ignore them — but `ST_Intersects` in a 2D engine against 3D WKB behaves inconsistently across implementations, and a mixed 2D/3D column will produce results that differ by engine. Decide on dimensionality per table, enforce it at ingest with an explicit `force_2d` step, and record the decision in the table properties.

Null handling is the last trap. A missing geometry can be represented as a SQL `NULL`, as an empty `GEOMETRYCOLLECTION`, or as a zero-length byte string, and the three behave differently in every predicate. Normalise to `NULL` at the boundary and reject empty geometries at validation, so that `WHERE geometry IS NOT NULL` means what a reader expects.


## Spatial Processing & Compute Boundaries

Python's strength lies in its mature geospatial stack, but raw Python iteration over geometries will bottleneck any production pipeline. Compute boundaries must be explicitly defined: heavy spatial joins, tiling, topology validation, and raster operations should be delegated to vectorized C/Rust backends or distributed engines, while Python handles orchestration, metadata enrichment, and business logic routing. For Delta-backed tables, the [Delta-rs Geometry Processing](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/delta-rs-geometry-processing/) documentation specifies how to leverage the underlying Rust engine for zero-copy geometry serialization, predicate pushdown, and native spatial indexing without crossing the Python GIL.

```python
# Offloading spatial predicate evaluation to Delta-rs via DuckDB
import duckdb
from deltalake import DeltaTable

dt = DeltaTable("s3://lakehouse/parcels/")
table_uri = dt.table_uri

# Push spatial filter down to the DuckDB/Rust engine before Python materialization
query = """
LOAD spatial;
SELECT parcel_id, ST_AsBinary(ST_GeomFromWKB(geometry)) AS wkb
FROM delta_scan(?)
WHERE ST_Contains(
    ST_GeomFromText('POLYGON((-122.5 37.7, -122.3 37.7, -122.3 37.9, -122.5 37.9, -122.5 37.7))'),
    ST_GeomFromWKB(geometry)
)
"""
result = duckdb.execute(query, [table_uri]).arrow()
```

**Performance Trade-off:** WKB storage increases raw byte footprint by approximately 15% compared to native PostGIS types, but eliminates engine-specific geometry serialization overhead during distributed reads. Pairing WKB with partition pruning on `ST_Centroid` or `ST_Envelope` bounding boxes yields optimal scan reduction. Avoid `ST_DWithin` or `ST_Intersects` on unpartitioned tables; the resulting full-table scan will saturate network I/O regardless of compute scaling.

## Execution Models & Pipeline Orchestration

Geospatial lakehouse workloads exhibit distinct I/O and CPU characteristics. Catalog metadata resolution, manifest listing, and cloud object storage handshakes are highly I/O bound and benefit from concurrent execution. Conversely, spatial transformations, raster resampling, and topology validation are CPU bound and require process-level parallelism. Mixing these execution models without explicit boundaries leads to thread starvation and memory fragmentation.

For metadata-heavy operations like catalog synchronization or manifest compaction, leverage [Async Execution Patterns](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/async-execution-patterns/) to overlap network latency with local compute. The following pattern demonstrates concurrent Iceberg snapshot resolution using `asyncio`:

```python
import asyncio
from pyiceberg.catalog import load_catalog

async def fetch_snapshot_metadata(catalog_name: str, table_names: list[str]):
    catalog = load_catalog(catalog_name)
    semaphore = asyncio.Semaphore(10)  # Limit concurrent catalog requests

    async def resolve_snapshot(table: str):
        async with semaphore:
            # PyIceberg catalog operations are synchronous; run in executor
            loop = asyncio.get_event_loop()
            tbl = await loop.run_in_executor(None, catalog.load_table, table)
            snap = tbl.current_snapshot()
            return snap.summary if snap else None

    tasks = [resolve_snapshot(t) for t in table_names]
    return await asyncio.gather(*tasks)
```

For CPU-bound spatial transformations, structure workloads around chunked Arrow tables or Polars lazy execution graphs. Avoid monolithic `apply()` operations. Instead, partition workloads by spatial index (e.g., H3 or S2 cells), process chunks in isolated worker pools, and materialize results incrementally to prevent OOM conditions on large joins.

## Choosing a Python Path: PyIceberg, delta-rs, Spark or DuckDB

Four Python routes into a spatial lakehouse coexist, and they are not competitors so much as different points on a scale/complexity curve. Choosing badly is rarely fatal but is consistently expensive: a Spark cluster spun up to write 200 MB, or a single-process PyIceberg job asked to rewrite a terabyte.

<figure class="diagram">
<svg viewBox="0 0 738 304" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Positioning of four Python routes by data volume and operational weight: DuckDB and delta-rs for single-node work, PyIceberg for catalog-driven metadata operations, and Spark with Sedona for distributed rewrites">
<rect x="0" y="0" width="738" height="304" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Where each Python route earns its keep</text>
<line x1="80" y1="238" x2="726" y2="238" stroke="#33707d" stroke-width="1.5"/>
<line x1="80" y1="56" x2="80" y2="238" stroke="#33707d" stroke-width="1.5"/>
<text x="403" y="266" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">data volume per job &#8594;</text>
<text x="64" y="150" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d" transform="rotate(-90 64 150)">operational weight &#8594;</text>
<circle cx="176" cy="206" r="34" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="176" y="203" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">DuckDB</text>
<text x="176" y="220" text-anchor="middle" font-family="sans-serif" font-size="10" fill="#33707d">&lt; 100 GB</text>
<circle cx="330" cy="168" r="38" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="330" y="165" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">delta-rs</text>
<text x="330" y="182" text-anchor="middle" font-family="sans-serif" font-size="10" fill="#33707d">&lt; 500 GB</text>
<circle cx="492" cy="140" r="40" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="492" y="137" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">PyIceberg</text>
<text x="492" y="154" text-anchor="middle" font-family="sans-serif" font-size="10" fill="#33707d">metadata-first</text>
<circle cx="654" cy="92" r="44" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="654" y="89" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">Spark + Sedona</text>
<text x="654" y="106" text-anchor="middle" font-family="sans-serif" font-size="10" fill="#33707d">unbounded</text>
<text x="390" y="288" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">The right route is the lightest one that still finishes inside the batch window</text>
</svg>
</figure>

**DuckDB** is the default for anything that fits comfortably on one machine, which in practice means far more than teams expect: 50–100 GB of GeoParquet is routine on a laptop-class instance. It reads Parquet and Iceberg directly, its spatial extension covers the common predicates, and it has no cluster, no driver and no submit step. Use it for exploration, for validation jobs in CI, and for serving aggregations. Its limitation is write-side: it is not the tool for committing snapshots to a governed catalog.

**delta-rs** is the Rust-backed Python writer for Delta tables, and it occupies the sweet spot for single-node production writes. It commits real Delta transactions without a JVM, handles partitioning and schema evolution, and is fast enough that the Python overhead is in the geometry serialisation rather than the write. The practical ceiling is memory: it materialises what it writes. See [using delta-rs to write spatial Parquet files](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/delta-rs-geometry-processing/using-delta-rs-to-write-spatial-parquet-files/).

**PyIceberg** is best understood as a catalog client that can also move data. Its strongest use is metadata work — inspecting snapshots, expiring them, planning scans, reading partition statistics, orchestrating maintenance — where it is dramatically lighter than any engine. It writes well at moderate volumes, and its scan planning can hand a pruned file list to DuckDB or Arrow for the actual compute, which is frequently the best of both. The patterns are in [PyIceberg spatial workflows](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/pyiceberg-spatial-workflows/).

**Spark with Sedona** is the answer when the job genuinely does not fit on one machine, or when the operation is a distributed shuffle — a many-to-many spatial join, a full-table sort rewrite, a reprojection of billions of rows. It carries real operational weight: cluster provisioning, dependency resolution for GeoTools, JVM tuning. Pay that cost when the alternative is not finishing, and not before.

The decision rule that holds up in practice: start at the lightest route that can express the job, measure whether it finishes inside the window with headroom, and move one step right only when it does not. Teams that start at Spark by default end up maintaining a cluster to do work a single process would finish in ninety seconds.


## Memory & Storage Trade-offs

Geospatial data introduces asymmetric memory pressure. Vector features scale linearly with coordinate density, while raster datasets scale quadratically with resolution and band count. Loading multi-gigabyte satellite imagery or LiDAR point clouds into pandas or GeoPandas will immediately exhaust worker memory. Production systems must implement streaming ingestion, memory-mapped file access, and tile-based processing.

When integrating rasters into a lakehouse, convert raw TIFFs to Cloud-Optimized GeoTIFF (COG) or Zarr before ingestion. This enables HTTP range requests and predicate pushdown at the tile level, reducing network transfer by 80–95% compared to monolithic raster downloads.

**Storage Layout Trade-offs:**
- **Parquet + WKB:** Optimal for vector analytics, fast predicate pushdown, compatible with Iceberg/Delta. Higher storage cost than PostGIS but eliminates engine lock-in.
- **Zarr/COG:** Optimal for raster analytics and ML training. Enables chunk-level parallelism but lacks ACID transactionality without external catalog wrappers.
- **Hybrid Approach:** Store vector metadata in Iceberg/Delta with pointers to COG/Zarr assets. Use spatial joins to filter raster tiles before materialization.

## Production Guardrails & CI/CD Integration

Operational reliability in spatial lakehouses requires automated validation at every pipeline stage. Schema drift, invalid CRS assumptions, and malformed WKB payloads must be caught before data reaches production tables. Integrate spatial validation into CI/CD workflows:

```yaml
# .github/workflows/spatial-schema-validation.yml
name: Spatial Schema Validation
on: [pull_request]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install pyarrow shapely
      - name: Run Spatial Schema Checks
        run: |
          python -c "
          import pyarrow as pa
          import pyarrow.parquet as pq
          from shapely import wkb

          table = pq.read_table('tests/fixtures/sample_parcels.parquet')
          assert 'geometry' in table.schema.names, 'Missing geometry column'
          assert table.schema.field('geometry').type == pa.binary(), 'geometry must be BINARY (WKB)'

          # Validate first 100 geometries
          for wkb_bytes in table.column('geometry').to_pylist()[:100]:
              if wkb_bytes is not None:
                  geom = wkb.loads(wkb_bytes)
                  assert geom.is_valid, f'Invalid geometry: {geom.wkt[:80]}'

          print('Spatial schema validation passed.')
          "
```

Schedule `OPTIMIZE` and `ZORDER BY` operations via cron or Airflow DAGs during maintenance windows. Monitor snapshot growth using catalog metadata APIs and enforce compaction thresholds when small file counts exceed 10,000 per partition.

By enforcing strict type contracts, delegating heavy compute to vectorized backends, and aligning execution models with workload characteristics, Python becomes a resilient orchestration layer for spatial lakehouses. The [Apache Iceberg Python documentation](https://py.iceberg.apache.org/) and [Delta Lake official docs](https://docs.delta.io/latest/index.html) provide additional reference implementations for catalog integration and transaction management.

## Reproducibility: Pinning the Geospatial Stack

The Python geospatial stack is a stack of C and Rust libraries wearing a Python interface, and the versions that matter are usually the ones not listed in `requirements.txt`. GEOS, PROJ and GDAL are the actual engines behind Shapely, pyproj and Fiona, and their behaviour changes between releases in ways that alter results rather than raising errors.

Three specific hazards recur. **GEOS version changes predicate edge cases**: whether two polygons that share exactly one boundary point intersect has been answered consistently for years, but validity repair, buffer end-caps and overlay output vertex ordering have all changed across GEOS 3.8 to 3.12. A pipeline that computes a geometry hash for reconciliation will see every hash change after an unpinned upgrade. **PROJ ships its own datum grid files**, and a transformation between two CRSs can silently fall back to a less accurate path when a grid is missing from the container — producing coordinates that differ by metres, which is invisible in a map and fatal in a cadastral join. **GDAL's driver behaviour** for Shapefile field truncation and encoding differs across versions, so an ingest that worked in 3.6 can produce differently-named columns in 3.8.

The mitigation is unglamorous and effective: pin the binary layer, not just the Python layer. Build ingestion images from a lock file that captures the GEOS, PROJ and GDAL versions, record those versions as table properties on every write, and assert them at job start. When a reconciliation job reports drift, the first question — "did the geometry library change?" — is then answerable in seconds rather than days.

```python
# Assert the binary stack at job start; fail fast rather than write inconsistent geometry.
import shapely, pyproj, pyarrow

EXPECTED = {"geos": "3.12.1", "proj": "9.3.1", "arrow": "15.0.2"}
actual = {
    "geos": shapely.geos_version_string.split("-")[0].strip(),
    "proj": pyproj.__proj_version__,
    "arrow": pyarrow.__version__,
}
drift = {k: (EXPECTED[k], v) for k, v in actual.items() if v != EXPECTED[k]}
if drift:
    raise RuntimeError(f"geospatial binary stack drifted: {drift}")
```

Record the same dictionary as table metadata on each commit. Six months later, when a downstream consumer reports that a boundary moved, the snapshot itself will tell you which library wrote it — and whether the move is a data change or a library change. That distinction is otherwise close to unrecoverable, and it is the reason the [schema validation pipeline for geospatial tables](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/lakehouse-maintenance-automation/schema-validation-pipeline-for-geospatial-tables/) belongs in every project from the first commit.

## Failure Modes and Operational Gotchas

- **GIL-bound geometry work masquerading as parallelism.** Threading a Shapely-heavy loop yields almost nothing; the work is in Python-level object churn. Use process pools, or push the operation into Arrow compute or DuckDB where the loop runs in native code.
- **Materialising the whole table to filter it.** `pd.read_parquet(...)` followed by a spatial predicate reads everything. Push the bounding-box predicate into the reader's filter argument so entire row groups never leave storage.
- **Silent CRS reset after `concat`.** Combining GeoDataFrames whose CRS differ produces a frame with `crs=None` in some versions and the first frame's CRS in others. Normalise before combining, and assert after.
- **Shapely 1.x idioms on Shapely 2.x.** Iterating geometries elementwise still works but is an order of magnitude slower than the vectorised interface; the migration is mostly mechanical and worth doing before optimising anything else.
- **Unbounded memory in async writers.** Firing hundreds of concurrent uploads without a semaphore will exhaust memory on a wide-geometry table, because each in-flight batch holds its serialised buffer. Bound concurrency explicitly, as described in [async catalog writes with PyIceberg and asyncio](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/async-execution-patterns/async-catalog-writes-with-pyiceberg-and-asyncio/).
- **Fork-unsafe native libraries in multiprocessing.** GDAL and some GEOS builds do not survive `fork` cleanly; use the `spawn` start method for worker pools that touch them.
- **Timezone-naive timestamps in a partitioned table.** Unrelated to geometry, but it lands in every spatial telemetry pipeline: naive timestamps partition by whatever the worker's locale was.

## Testing Spatial Transformations

Spatial code fails in ways that ordinary unit tests do not catch, because the output is usually *plausible*. A reprojection with the wrong datum still returns coordinates in the right country. A buffer with a degree-valued distance still returns a polygon. Tests therefore have to assert on properties, not just on absence of exceptions.

**Round-trip tests** are the cheapest high-value check. Serialise a geometry to WKB, write it, read it back, and compare the result to the original with `equals_exact` at a tolerance appropriate to the storage precision — not with `==`, which compares object identity of coordinate sequences in some versions. A round-trip test catches precision truncation, dimensionality loss and byte-order mistakes in one assertion, and it is the test that would have caught most of the migration incidents described earlier on this page.

**Invariant tests** assert things that must hold regardless of the data. Every geometry is valid after the validation step. Every coordinate lies inside the declared CRS bounds. Every derived bounding box contains its geometry. Row count is preserved across a reprojection. None of these needs a fixture file; they run against whatever sample the pipeline is given, which means they keep working when the fixtures go stale.

**Golden-geometry tests** pin behaviour that depends on the binary stack. Keep a handful of deliberately awkward geometries — a bowtie polygon, a ring with a repeated vertex, a multipolygon with a hole touching its shell, a line crossing the antimeridian — with their expected post-processing WKB hashes. When a library upgrade changes one, the diff is explicit rather than discovered in production. Store them as hex WKB in the repository so the test has no dependency on a GIS file format.

```python
# pytest: properties that must hold for any input batch, not just the fixtures.
import pytest
from shapely import from_wkb, box

def test_bbox_columns_contain_their_geometry(batch):
    for row in batch.to_pylist():
        geom = from_wkb(row["geometry"])
        envelope = box(row["bbox_min_x"], row["bbox_min_y"],
                       row["bbox_max_x"], row["bbox_max_y"])
        assert envelope.covers(geom), f"bbox does not cover geometry {row['id']}"

def test_coordinates_are_geographic(batch):
    for row in batch.to_pylist():
        assert -180.0 <= row["bbox_min_x"] <= 180.0, "x outside geographic range — projected CRS?"
        assert -90.0 <= row["bbox_min_y"] <= 90.0, "y outside geographic range — projected CRS?"

@pytest.mark.parametrize("wkb_hex,expected_valid", [
    ("0103000000010000000500000000000000000000000000000000000000"
     "000000000000f03f000000000000f03f0000000000000000000000000000f03f"
     "000000000000f03f00000000000000000000000000000000000000000000 0000".replace(" ", ""), True),
])
def test_known_geometries_survive_the_pipeline(wkb_hex, expected_valid):
    assert from_wkb(bytes.fromhex(wkb_hex)).is_valid is expected_valid
```

**Differential tests** are the strongest tool available and are underused. Run the same spatial predicate through two independent implementations — Shapely locally and DuckDB's spatial extension, or PostGIS and Sedona — over the same sample, and assert the results agree. Where they disagree, one of them is wrong, and finding out which is exactly the investigation worth having *before* the pipeline is authoritative. This is also how to build confidence during the migration phases described in the [fundamentals section](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/): the diff between old and new systems is the test suite.

Finally, keep the sample small and the runtime under a minute. A spatial test suite that takes twenty minutes will be skipped under deadline pressure, and a skipped test catches nothing. Ten thousand rows chosen to include the pathological cases beats ten million chosen at random.

Treat the suite as documentation of the contract this section describes: a reader who wants to know what the pipeline guarantees should be able to read the property tests and get a complete, current answer, rather than a paragraph that was true when it was written.

One organisational note closes the loop. The reason these practices survive contact with a real team is that they are enforced by machinery rather than by memory: the binary-stack assertion runs at job start, the property tests run on every pull request, the reconciliation job runs nightly and pages someone. A convention that lives only in a runbook decays at the first deadline. A convention that fails a build survives staff turnover, library upgrades and the quarter when everyone is busy — which is precisely the quarter in which a silent coordinate reference system change would otherwise reach production and stay there.
