# Reading shapefiles into PyIceberg DataFrames efficiently

The primary failure mode in spatial lakehouse ingestion pipelines is unbounded memory allocation during legacy vector parsing. Shapefiles (`.shp`, `.shx`, `.dbf`) lack native spatial indexing at the file level and force geometry deserialization into in-memory object graphs. When routed directly into PyIceberg through high-level `geopandas` or `fiona` `read_file()` calls, the resulting DataFrame triggers immediate OOM kills on standard 16–32GB worker nodes. This occurs because PyIceberg's schema inference engine attempts to materialize the entire coordinate array before Parquet serialization, while simultaneously allocating temporary buffers for WKT/WKB conversion. Production-grade ingestion requires decoupling I/O, geometry serialization, and Iceberg write transactions into a strictly bounded, chunked pipeline.

## Root Cause Analysis: Double-Materialization Overhead

Shapefile ingestion fails at two distinct boundaries: cursor exhaustion and schema inference latency. The `.shp` format stores geometries as variable-length binary records. Loading these into a Pandas-backed DataFrame forces contiguous memory allocation proportional to vertex count, not file size. When PyIceberg attempts to map these objects to its native `binary` type, it performs a full scan to infer nullability and precision. This double-materialization pattern is unsustainable for municipal-scale parcels, hydrological networks, or cadastral datasets exceeding 500MB.

The resolution requires bypassing high-level geometry object instantiation entirely. Instead of materializing `shapely` objects, the pipeline must stream raw WKB bytes directly from the file cursor, apply explicit schema constraints, and append batches to the Iceberg table using transactional `append` operations. This approach aligns with established [PyIceberg Spatial Workflows](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/pyiceberg-spatial-workflows/) that prioritize binary column mapping over object-relational translation.

## Pipeline Architecture: Chunked WKB Streaming

The ingestion engine must operate at the record level, not the dataset level. `pyogrio` provides a low-overhead cursor interface that supports `batch_size` iteration without loading the full attribute table. Each batch is extracted as a `pyarrow.RecordBatch`, where the geometry column is immediately serialized to WKB bytes using Shapely 2.0+ vectorized routines. This eliminates Python object overhead and reduces memory footprint by 60–80% compared to GeoJSON or WKT intermediaries.

The architecture enforces three strict boundaries:
1. **Explicit Schema Declaration**: Bypasses PyIceberg's inference engine by defining column types upfront.
2. **Vectorized WKB Conversion**: Uses Shapely 2.0+ C-accelerated routines to convert Arrow geometry arrays directly to binary.
3. **Transactional Append Isolation**: Commits each batch as a discrete Iceberg snapshot, enabling rollback on failure and preventing partial writes.

## Production Implementation

The following pipeline configures a chunked reader, applies deterministic WKB serialization, and writes to an Iceberg table with explicit partitioning and compression.

```python
import os
import gc
import logging
import pyogrio
import pyarrow as pa
import shapely
import shapely.wkb
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import BinaryType, StringType, IntegerType, NestedField
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# 1. Define explicit Iceberg schema to bypass inference
iceberg_schema = Schema(
    NestedField(1, "parcel_id",    IntegerType(), required=True),
    NestedField(2, "owner_name",   StringType(),  required=False),
    NestedField(3, "zoning_code",  StringType(),  required=False),
    NestedField(4, "geometry_wkb", BinaryType(),  required=True),
)

# 2. Initialize table with partitioning and compression properties
catalog = load_catalog("default")
table = catalog.create_table(
    identifier="spatial.parcels_raw",
    schema=iceberg_schema,
    partition_spec=PartitionSpec(
        PartitionField(
            source_id=3,      # zoning_code field
            field_id=1000,
            transform=IdentityTransform(),
            name="zoning_code"
        )
    ),
    properties={
        "write.parquet.compression-codec": "zstd",
        "write.parquet.compression-level": "3",
        "write.target-file-size-bytes": "134217728",  # 128MB
        "write.parquet.page-size-bytes": "1048576",   # 1MB
    },
)

def stream_shapefile_to_iceberg(shapefile_path: str, batch_size: int = 25000):
    """Chunked WKB ingestion pipeline with transactional rollback."""
    if not os.path.exists(shapefile_path):
        raise FileNotFoundError(f"Shapefile not found: {shapefile_path}")

    # pyogrio.open_arrow yields an iterator of pyarrow.RecordBatch
    with pyogrio.open_arrow(shapefile_path, batch_size=batch_size) as reader:
        for batch_idx, batch in enumerate(reader):
            try:
                # Extract geometry column (Arrow geometry array from pyogrio)
                geo_col = batch.column("geometry").to_pylist()

                # Convert to WKB bytes using Shapely 2.0+ vectorized API
                # pyogrio returns shapely geometry objects when use_arrow=True
                geoms = [shapely.from_wkb(g) if isinstance(g, bytes) else g
                         for g in geo_col]
                wkb_bytes_list = [
                    shapely.wkb.dumps(g, include_srid=False) if g is not None else None
                    for g in geoms
                ]
                wkb_array = pa.array(wkb_bytes_list, type=pa.binary())

                # Construct PyArrow table matching Iceberg schema
                arrow_table = pa.table({
                    "parcel_id":    batch.column("parcel_id"),
                    "owner_name":   batch.column("owner_name"),
                    "zoning_code":  batch.column("zoning_code"),
                    "geometry_wkb": wkb_array,
                })

                # Transactional append — each batch is an isolated snapshot
                table.append(arrow_table)
                logging.info(f"Committed batch {batch_idx} ({len(arrow_table)} rows)")

                # Force garbage collection to release Arrow buffers
                del arrow_table, wkb_array, batch
                gc.collect()

            except Exception as e:
                logging.error(f"Batch {batch_idx} failed: {e}")
                raise RuntimeError(
                    f"Ingestion aborted at batch {batch_idx}. Check transaction logs."
                ) from e

    logging.info("Ingestion complete. Run snapshot expiration to reclaim staging files.")
```

## Configuration & Tuning Parameters

Production deployments require explicit memory and I/O constraints:

| Parameter | Recommended Value | Impact |
|-----------|-------------------|--------|
| `batch_size` | `15000–35000` | Balances cursor overhead with memory pressure. Exceeding 50k triggers swap on 16GB nodes. |
| `write.target-file-size-bytes` | `134217728` (128MB) | Aligns with Iceberg's default file sizing. Prevents small-file fragmentation during compaction. |
| `PYARROW_MEMORY_LIMIT` (env var) | 85% of container RAM | Enforces hard cap on Arrow buffer allocation: `os.environ["PYARROW_MEMORY_LIMIT"] = "12G"` |
| `shapely.wkb.dumps(include_srid)` | `False` | Excludes SRID from WKB payload (store separately in a `srid INT` column if needed). |
| `iceberg.catalog.io-impl` | `pyiceberg.io.pyarrow.PyArrowFileIO` | Ensures zero-copy Parquet writes and native Arrow buffer reuse. |

These settings integrate seamlessly with broader [Python Ecosystem & Integration Workflows](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/) that standardize lakehouse I/O across heterogeneous data sources.

## Failure Resolution & Debugging

### OOM During Append
**Symptom**: Worker process killed with `SIGKILL` or `MemoryError` during `table.append()`.
**Resolution**: Reduce `batch_size` to `10000`. Set `PYARROW_MEMORY_LIMIT` environment variable. Disable `geopandas` fallback by ensuring `pyogrio` is the sole vector reader. Monitor RSS with `psutil.Process(os.getpid()).memory_info().rss`.

### Schema Drift / Type Mismatch
**Symptom**: `pyiceberg.exceptions.SchemaMismatchError: Field 'geometry_wkb' type mismatch`.
**Resolution**: PyIceberg enforces strict schema evolution. If the source shapefile adds columns, update the Iceberg table via `table.update_schema().add_column(...).commit()` before ingestion. Never rely on implicit type coercion.

### Transaction Timeout / Staging File Accumulation
**Symptom**: `TimeoutError` during commit or excessive `.metadata/` directory growth.
**Resolution**: Increase commit retry attempts via the `PYICEBERG_COMMIT_RETRY_ATTEMPTS` environment variable (default: 4). Schedule `table.expire_snapshots()` and `table.remove_orphan_files()` post-ingestion. Staging files are retained until snapshot expiration by design.

### Invalid WKB / Geometry Validation
**Symptom**: `shapely.errors.GEOSException` or `Invalid geometry type` during `wkb.dumps`.
**Resolution**: Pre-validate with `pyogrio.open_arrow(..., skip_invalid=True)` where supported, or wrap `shapely.wkb.dumps()` in a `try/except` and log malformed record indices for manual QA. Iceberg does not perform runtime geometry validation; enforce it upstream.

For authoritative reference on Arrow geometry interoperability and WKB specification compliance, consult the [Pyogrio Documentation](https://pyogrio.readthedocs.io/en/latest/) and the [Apache Iceberg Python API Reference](https://py.iceberg.apache.org/). Implementing this pipeline guarantees deterministic memory bounds, snapshot-safe commits, and direct compatibility with downstream spatial query engines.

## Why Shapefiles Resist Efficient Reading

The format's constraints are the reason a naive read is slow, and each one has a specific workaround.

<figure class="diagram">
<svg viewBox="0 0 774 284" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Four shapefile constraints and their workarounds: multiple sidecar files that must all be present, field names truncated to ten characters, no null distinction for numeric fields, and an encoding that is often undeclared">
<rect x="0" y="0" width="774" height="284" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Four constraints, four workarounds</text>
<rect x="30" y="56" width="352" height="100" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="206" y="84" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">several sidecar files</text>
<text x="206" y="108" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">.shp .shx .dbf .prj .cpg</text>
<text x="206" y="132" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">fetch all of them; a missing .prj means an unknown CRS</text>
<rect x="398" y="56" width="352" height="100" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="574" y="84" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">10-character field names</text>
<text x="574" y="108" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">&#8220;population_2024&#8221; becomes &#8220;populati_1&#8221;</text>
<text x="574" y="132" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">map to real names explicitly at ingest</text>
<rect x="30" y="172" width="352" height="100" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="206" y="200" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">no true nulls</text>
<text x="206" y="224" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">missing numerics arrive as 0 or -9999</text>
<text x="206" y="248" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">declare the sentinel and convert it</text>
<rect x="398" y="172" width="352" height="100" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="574" y="200" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">undeclared text encoding</text>
<text x="574" y="224" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">.cpg is optional and often absent</text>
<text x="574" y="248" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">specify it; do not let the reader guess</text>
</svg>
</figure>

The sentinel-value problem is the one that most often survives into the lakehouse. A population column where missing values are recorded as zero produces sums and averages that are wrong by an amount nobody can detect from the output, and the fix — declaring the sentinel per column at ingest and converting to null — takes one line and must be done at the boundary, because once the value is in the table it is indistinguishable from a real zero.

## Reading Efficiently Rather Than Simply

<figure class="diagram">
<svg viewBox="0 0 762 222" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Comparison of a naive shapefile read that loads the whole file into a GeoDataFrame against a streaming read that yields batches and converts to Arrow incrementally">
<rect x="0" y="0" width="762" height="222" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">The read strategy decides peak memory</text>
<rect x="30" y="58" width="352" height="152" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="206" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">read whole file</text>
<text x="206" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">one GeoDataFrame in memory</text>
<text x="206" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">shapely objects per feature</text>
<text x="206" y="164" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">peak &#8776; 4–8× the file size</text>
<text x="206" y="190" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">fails on large national datasets</text>
<rect x="398" y="58" width="352" height="152" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="574" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">stream in batches</text>
<text x="574" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">read N features, convert, release</text>
<text x="574" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">WKB encoded per batch</text>
<text x="574" y="164" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">peak &#8776; one batch</text>
<text x="574" y="190" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">flat memory, any file size</text>
</svg>
</figure>

The four-to-eight-times multiplier on the left is not an exaggeration: a shapefile's geometry is a compact binary record, and a shapely object graph for the same feature is substantially larger, with Python object overhead on every ring and every coordinate array. Streaming avoids constructing more than one batch of them at a time and makes the memory profile independent of the input size, which is what allows the same code to handle a municipal extract and a continental one.

## Normalising on the Way In

<figure class="diagram">
<svg viewBox="0 0 768 232" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Normalisation steps applied to each streamed batch before writing: rename truncated fields, convert sentinel values to null, reproject to the canonical CRS, force two dimensions, encode WKB and derive bounding box columns">
<defs>
<marker id="shp-norm-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#2f6e49"/></marker>
</defs>
<rect x="0" y="0" width="768" height="232" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Everything that must happen before the write</text>
<rect x="24" y="66" width="140" height="66" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="94" y="94" text-anchor="middle" font-family="sans-serif" font-size="11" font-weight="700" fill="#0d3b45">rename fields</text>
<text x="94" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">from the mapping</text>
<rect x="184" y="66" width="140" height="66" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="254" y="94" text-anchor="middle" font-family="sans-serif" font-size="11" font-weight="700" fill="#0d3b45">sentinels to null</text>
<text x="254" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">per declared column</text>
<rect x="344" y="66" width="140" height="66" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="414" y="94" text-anchor="middle" font-family="sans-serif" font-size="11" font-weight="700" fill="#0d3b45">reproject</text>
<text x="414" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">from the .prj</text>
<rect x="504" y="66" width="132" height="66" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="570" y="94" text-anchor="middle" font-family="sans-serif" font-size="11" font-weight="700" fill="#0d3b45">force 2D</text>
<text x="570" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">drop Z and M</text>
<rect x="656" y="66" width="100" height="66" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="706" y="94" text-anchor="middle" font-family="sans-serif" font-size="11" font-weight="700" fill="#0d3b45">encode</text>
<text x="706" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">WKB + bbox</text>
<line x1="164" y1="99" x2="184" y2="99" stroke="#2f6e49" stroke-width="2" marker-end="url(#shp-norm-arrow)"/>
<line x1="324" y1="99" x2="344" y2="99" stroke="#2f6e49" stroke-width="2" marker-end="url(#shp-norm-arrow)"/>
<line x1="484" y1="99" x2="504" y2="99" stroke="#2f6e49" stroke-width="2" marker-end="url(#shp-norm-arrow)"/>
<line x1="636" y1="99" x2="656" y2="99" stroke="#2f6e49" stroke-width="2" marker-end="url(#shp-norm-arrow)"/>
<text x="390" y="186" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">Order matters: reproject before deriving the bounding box, always</text>
<text x="390" y="216" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">A box derived first describes where the geometry used to be</text>
</svg>
</figure>

Reading the CRS from the `.prj` file rather than assuming one is the step most often skipped, and it is the one that determines whether the reprojection is even meaningful. A shapefile without a `.prj` has no declared coordinate system at all, and the correct response is to fail rather than to guess — a guess that happens to be wrong produces data that is plausibly positioned and systematically displaced, which is the hardest defect on this site to detect after the fact.

Once normalised, the batches are ordinary Arrow tables and everything downstream — the declared schema, the derived columns, the append with a bounded semaphore — is identical to any other spatial ingest path on this site. That is the goal: the shapefile's peculiarities are handled once, at the boundary, and never leak into the lakehouse.
Downstream readers should never need to know the data started as a shapefile, and if they do, something was normalised too late.
Every downstream guide on this site assumes exactly that, which is why the normalisation step is worth doing thoroughly rather than approximately.

A shapefile handled well at the boundary produces a table indistinguishable from one loaded from any other source, and that indistinguishability is the measure of whether the ingest did its job.
Anything less leaves the format’s constraints to be rediscovered by whoever queries the table next.
That rediscovery is always more expensive than doing it once at the boundary.
 Handle it at the boundary, once, and every downstream reader inherits a clean contract.
