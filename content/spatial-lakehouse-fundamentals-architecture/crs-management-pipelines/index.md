# CRS Management Pipelines for Spatial Lakehouses

A spatial lakehouse is only trustworthy if every geometry it stores speaks the same coordinate language. When one upstream feed arrives in a web-mercator projection, another in a national grid, and a third in lat/lon, a naive append silently mixes incompatible coordinates into a single table — and the corruption stays invisible until a spatial join returns points in the middle of the ocean. A coordinate reference system (CRS) management pipeline closes that gap by making the CRS an enforced contract on ingestion rather than an assumption at query time. This guide, part of [Spatial Lakehouse Fundamentals & Architecture](/spatial-lakehouse-fundamentals-architecture/), shows how to declare an authoritative CRS, transform every incoming batch to it on write, persist that CRS in table and file metadata, and detect drift the moment an upstream source changes projection.

The convention this site adopts — and the one the GeoParquet ecosystem defaults to — is storage in EPSG:4326 (WGS 84 longitude/latitude). That is a deliberate storage decision, not a claim that EPSG:4326 is the best analysis CRS. It is the interoperability lingua franca: DuckDB spatial, Sedona, GeoPandas, and every OGC-compliant reader agree on its axis order and units, so a table stored in EPSG:4326 can be read by any engine without a per-engine reprojection shim. Analytical reprojection (to an equal-area or local UTM CRS for distance and area math) happens downstream, close to the query, where the correct projection depends on the question being asked.

## When to enforce a canonical CRS

Not every table needs a full reprojection pipeline, but the moment more than one producer writes to a spatial dataset, CRS enforcement stops being optional. Use the criteria below to decide how much machinery a given feed warrants.

| Situation | Enforce canonical CRS on write? | Notes |
|---|---|---|
| Single trusted producer, CRS contractually fixed | Assert only (fail on mismatch) | Cheaper than transforming; catches accidental drift without reprojection cost. |
| Multiple producers, heterogeneous CRS | Transform on write to EPSG:4326 | The default lakehouse pattern; normalizes everything at the boundary. |
| Raw archival zone (bronze) that must preserve source CRS | Store source CRS, transform in silver layer | Keep provenance; normalize when promoting to the query-serving tier. |
| High-precision survey/cadastral data in a local projected CRS | Store native CRS, document exception | Reprojection to EPSG:4326 loses sub-metre precision; keep native and reproject per query. |
| Streaming point telemetry (GPS) | Assert EPSG:4326, reject out-of-range | GPS is already lon/lat; a range check is usually enough. |

The recurring decision is assert-versus-transform. Asserting is a validation gate: read the declared CRS, compare it to the expected authority, and fail the batch if they disagree. Transforming is heavier — it rewrites every coordinate through a [pyproj](https://pyproj4.github.io/pyproj/stable/) transformer — but it is the only correct choice when you genuinely receive mixed projections. A robust pipeline does both: transform when the source CRS is known and different, assert when it is supposed to already match, and quarantine when the CRS is absent or unrecognized.

## Architecture of the CRS enforcement path

The pipeline sits at the ingestion boundary, between the raw landing zone and the query-serving table. Every batch passes through four stages before a single row is committed.

<figure class="diagram">
<svg viewBox="0 0 760 250" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="CRS management pipeline data flow from ingestion through assert or transform, validation, and write to the lakehouse table">
<title>CRS enforcement data path on ingestion</title>
<desc>Incoming batches enter a CRS assert-or-transform stage, pass a validation gate that checks coordinate ranges and metadata, then write to the GeoParquet-backed table in EPSG:4326; failures are quarantined.</desc>
<defs>
<marker id="arw-crsmgmt" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
<path d="M0 0 L10 5 L0 10 z" fill="#0e6e7d"/>
</marker>
</defs>
<rect x="0" y="0" width="760" height="250" fill="#f7fbfc"/>
<rect x="18" y="90" width="120" height="64" rx="6" fill="#ffffff" stroke="#cfe3e7" stroke-width="1.5"/>
<text x="78" y="116" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="bold" fill="#0d3b45">Ingest</text>
<text x="78" y="134" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">mixed CRS</text>
<rect x="188" y="70" width="140" height="104" rx="6" fill="#ffffff" stroke="#0e6e7d" stroke-width="1.5"/>
<text x="258" y="98" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="bold" fill="#0d3b45">Assert /</text>
<text x="258" y="116" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="bold" fill="#0d3b45">Transform</text>
<text x="258" y="138" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">pyproj → 4326</text>
<text x="258" y="154" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">read src CRS</text>
<rect x="378" y="70" width="140" height="104" rx="6" fill="#ffffff" stroke="#2f6e49" stroke-width="1.5"/>
<text x="448" y="98" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="bold" fill="#0d3b45">Validate</text>
<text x="448" y="120" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">range check</text>
<text x="448" y="136" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">CRS == 4326</text>
<text x="448" y="152" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">drift heuristic</text>
<rect x="568" y="90" width="130" height="64" rx="6" fill="#ffffff" stroke="#6a3d9a" stroke-width="1.5"/>
<text x="633" y="112" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="bold" fill="#0d3b45">Write</text>
<text x="633" y="132" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">GeoParquet</text>
<text x="633" y="147" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">CRS in meta</text>
<rect x="378" y="196" width="140" height="42" rx="6" fill="#ffffff" stroke="#9a5a17" stroke-width="1.5"/>
<text x="448" y="222" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="bold" fill="#9a5a17">Quarantine</text>
<line x1="138" y1="122" x2="182" y2="122" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-crsmgmt)"/>
<line x1="328" y1="122" x2="372" y2="122" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-crsmgmt)"/>
<line x1="518" y1="122" x2="562" y2="122" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-crsmgmt)"/>
<line x1="448" y1="174" x2="448" y2="192" stroke="#9a5a17" stroke-width="2" marker-end="url(#arw-crsmgmt)"/>
</svg>
</figure>

The transform stage reads the declared source CRS (from GeoParquet metadata, a sidecar `.prj`, or a producer-supplied EPSG code) and, if it differs from the storage authority, reprojects. The validate stage is a second, independent check that never trusts the declared CRS blindly — it verifies coordinates fall inside the valid EPSG:4326 envelope and runs the range heuristics detailed in [Detecting CRS Drift in Ingestion Pipelines](/spatial-lakehouse-fundamentals-architecture/crs-management-pipelines/detecting-crs-drift-in-ingestion-pipelines/). Only after both stages pass does the batch write to the [GeoParquet-encoded](/spatial-lakehouse-fundamentals-architecture/geoparquet-encoding-standards/) table with the CRS stamped into column metadata.

## Prerequisites and environment setup

The pipeline is built on [pyproj](https://pyproj4.github.io/pyproj/stable/) for coordinate transformation and [GeoPandas](https://geopandas.org/) for geometry handling and GeoParquet I/O. Pin the versions below; pyproj bundles a specific PROJ data release, and mismatched PROJ grids produce subtly different transformation results.

```bash
python -m pip install \
  geopandas==1.0.1 \
  pyproj==3.7.1 \
  shapely==2.1.1 \
  pyarrow==17.0.0
```

```python
import geopandas as gpd
import pyproj
from pyproj import CRS, Transformer

# The single authoritative storage CRS for the whole lakehouse.
STORAGE_EPSG = 4326  # WGS 84 lon/lat — numeric literal in code, EPSG:4326 in prose.
STORAGE_CRS = CRS.from_epsg(STORAGE_EPSG)

print("pyproj:", pyproj.__version__, "PROJ:", pyproj.proj_version_str)
print("Storage CRS:", STORAGE_CRS.name, "axis order:",
      [ax.abbrev for ax in STORAGE_CRS.axis_info])
```

Define `STORAGE_EPSG` in exactly one module and import it everywhere. A hardcoded `4326` scattered across ingestion jobs is how drift creeps in when someone later "temporarily" changes one copy. Treat it as configuration, not a literal.

## Step-by-step implementation

### 1. Resolve the source CRS deterministically

Never guess a source CRS. Resolve it from an explicit signal, in priority order: producer-supplied EPSG code, GeoParquet/GeoDataFrame `.crs`, then a sidecar `.prj`. If none exists, the batch is undeclared and must be quarantined — not assumed to be EPSG:4326.

```python
from pyproj import CRS

def resolve_source_crs(gdf, declared_epsg=None):
    """Return a CRS object or raise if the source CRS is undeclared."""
    if declared_epsg is not None:
        return CRS.from_epsg(int(declared_epsg))
    if gdf.crs is not None:
        return CRS.from_user_input(gdf.crs)
    raise ValueError("Undeclared source CRS: refuse to assume EPSG:4326")
```

### 2. Assert or transform to the storage CRS

With the source CRS known, decide whether to pass through or reproject. `always_xy=True` forces longitude/latitude (easting/northing) axis order, sidestepping the notorious EPSG:4326 axis-order ambiguity that flips coordinates between libraries.

```python
from pyproj import Transformer

def enforce_storage_crs(gdf, source_crs, storage_crs=STORAGE_CRS):
    """Reproject to the storage CRS if needed; assert if already matching."""
    if source_crs.equals(storage_crs):
        # Already canonical — assert, do not transform.
        return gdf.set_crs(storage_crs, allow_override=True)

    # GeoPandas delegates to pyproj; always_xy keeps lon/lat order.
    reprojected = gdf.to_crs(storage_crs)
    return reprojected
```

For non-geometry coordinate arrays (for example, raw telemetry columns), build the transformer explicitly rather than round-tripping through GeoPandas:

```python
transformer = Transformer.from_crs(
    CRS.from_epsg(3857),   # web mercator source
    STORAGE_CRS,           # 4326 target
    always_xy=True,
)
lon, lat = transformer.transform(x_meters, y_meters)
```

### 3. Validate independently of the declared CRS

The declared CRS can lie — a producer may relabel a projected file as EPSG:4326 without reprojecting. Validate the actual coordinate ranges against the EPSG:4326 envelope so mislabeled data is caught before it lands.

```python
def validate_wgs84_envelope(gdf):
    """Coordinates outside the EPSG:4326 envelope indicate a mislabeled CRS."""
    minx, miny, maxx, maxy = gdf.total_bounds
    assert -180.0 <= minx and maxx <= 180.0, f"X out of range: {minx}..{maxx}"
    assert -90.0 <= miny and maxy <= 90.0,  f"Y out of range: {miny}..{maxy}"
    # A dataset spanning only a few thousandths of a degree over a wide
    # projected extent is a classic sign of un-reprojected mercator metres.
    return gdf
```

### 4. Persist the CRS in table and file metadata

Writing coordinates without recording their CRS is the original sin. GeoParquet stores the CRS as PROJJSON in the file's geo metadata; GeoPandas writes it automatically when the GeoDataFrame carries a CRS. Verify it is present rather than trusting the writer.

```python
def write_with_crs(gdf, path):
    """Write GeoParquet with an explicit, verifiable CRS stamp."""
    assert gdf.crs is not None, "Refusing to write geometry without a CRS"
    assert gdf.crs.to_epsg() == STORAGE_EPSG, "Not in storage CRS"
    gdf.to_parquet(path, geometry_encoding="WKB", write_covering_bbox=True)
```

For the lakehouse table itself, carry the CRS as an explicit `STRING` column (value `EPSG:4326`) alongside the geometry, in addition to the file metadata. Table-level column metadata is not uniformly propagated by every engine, so a materialized `crs` column gives query engines and CI checks a reliable handle. This mirrors the CRS-as-column pattern used in [Open Table Format Versioning](/spatial-lakehouse-fundamentals-architecture/open-table-format-versioning/) to survive schema evolution.

### 5. Compose the pipeline

The full ingest function chains the stages and routes failures to quarantine instead of aborting the whole run.

```python
def ingest_batch(gdf, declared_epsg=None, out_path="s3://lake/silver/geo.parquet"):
    try:
        src = resolve_source_crs(gdf, declared_epsg)
        gdf = enforce_storage_crs(gdf, src)
        gdf = validate_wgs84_envelope(gdf)
        gdf["crs"] = "EPSG:4326"
        write_with_crs(gdf, out_path)
        return {"status": "ok", "rows": len(gdf), "source_crs": src.to_epsg()}
    except (ValueError, AssertionError) as exc:
        # Quarantine, alert, and keep the pipeline running.
        return {"status": "quarantined", "reason": str(exc)}
```

## Verification and testing

After ingestion, prove the table is canonical from two angles: file metadata and coordinate statistics. Read the GeoParquet geo metadata directly to confirm the stored CRS.

```python
import json, pyarrow.parquet as pq

meta = pq.read_schema("s3://lake/silver/geo.parquet").metadata
geo = json.loads(meta[b"geo"])
col = geo["primary_column"]
crs = geo["columns"][col]["crs"]
print("Stored CRS id:", crs["id"] if crs else "MISSING")
assert crs and crs["id"]["code"] == 4326
```

Then confirm the coordinate envelope with an engine query. In DuckDB (spatial extension ≥ 1.0), compute the bounds and reject anything outside EPSG:4326:

```sql
-- DuckDB spatial: bounds must sit inside the WGS84 envelope
SELECT
  min(ST_XMin(geom)) AS min_x, max(ST_XMax(geom)) AS max_x,
  min(ST_YMin(geom)) AS min_y, max(ST_YMax(geom)) AS max_y
FROM read_parquet('s3://lake/silver/geo.parquet')
HAVING min_x >= -180 AND max_x <= 180
   AND min_y >= -90  AND max_y <= 90;
```

An empty result from that `HAVING` clause is a failing test: some coordinate escaped the envelope. Add both checks to CI so a bad batch fails the build, following the metadata-validation discipline in [Validating GeoParquet metadata in CI](/spatial-lakehouse-fundamentals-architecture/geoparquet-encoding-standards/validating-geoparquet-metadata-in-ci/).

## Performance and tuning

Reprojection is CPU-bound and, done row-by-row, dominates ingest time. The tuning levers, in order of impact:

- **Vectorize transforms.** `Transformer.transform` accepts NumPy arrays and processes them in a single PROJ call. Transforming 10 million points as one array runs roughly 50–100x faster than a Python loop over individual points. GeoPandas `to_crs` already does this internally — never fall back to per-row `apply`.
- **Skip the no-op path.** When `source_crs.equals(storage_crs)`, do not call `to_crs`; asserting is effectively free while a self-transform still walks every coordinate. On feeds that are already EPSG:4326, this alone removes the reprojection cost entirely.
- **Cache transformers.** Building a `Transformer` triggers a PROJ pipeline lookup (a few milliseconds). If you ingest thousands of small batches from the same source CRS, construct the transformer once and reuse it; `pyproj` also maintains an internal `TransformerGroup` cache you should not defeat by recreating CRS objects per batch.
- **Right-size batches.** Reprojection parallelizes well across partitions. Target 128 MB output files (the same target used across the lakehouse) so downstream compaction — covered in [Compacting spatial Iceberg tables with rewrite_data_files](/python-ecosystem-integration-workflows/lakehouse-maintenance-automation/compacting-spatial-iceberg-tables-with-rewrite-data-files/) — has little to do.
- **Choose the transformation, not just the CRS pair.** For datum shifts (for example NAD83 ↔ WGS 84), multiple pipelines with different accuracy exist. Pin the pipeline explicitly when sub-metre accuracy matters; the default "best available" pipeline can change between PROJ data releases and silently shift results.

As a rough baseline, a modern core reprojects on the order of 5–15 million coordinates per second through a cached, vectorized transformer, so a 50-million-point batch reprojects in single-digit seconds — trivial next to the object-store write. If reprojection shows up as a bottleneck, the cause is almost always a Python loop or a per-batch transformer rebuild, not PROJ itself.

## Common errors and fixes

| Symptom | Root cause | Fix |
|---|---|---|
| Points land in the Gulf of Guinea (0, 0) or off by continents | Axis-order swap — library returned lat/lon where lon/lat was expected | Always pass `always_xy=True` to `Transformer.from_crs`; verify axis order with `CRS.axis_info`. |
| Coordinates in the millions instead of ±180 | File labeled EPSG:4326 but coordinates are still web-mercator metres | Add the envelope range check in stage 3; reproject from the real source CRS (often 3857). |
| `CRSError: Invalid projection` on ingest | Source declared an unknown or vendor-specific CRS string | Resolve via `CRS.from_user_input`; if unresolvable, quarantine and require a valid EPSG code from the producer. |
| Different results after a pyproj upgrade | New PROJ data release changed the default datum-shift pipeline | Pin the transformation pipeline explicitly; pin `pyproj` and record `pyproj.proj_version_str` with the data. |
| Query engine reports no CRS on the table | CRS written to file metadata but not materialized as a column | Add the `crs` STRING column (value `EPSG:4326`) per stage 4; do not rely on column metadata propagation alone. |

Once the enforcement path is solid, the highest-leverage addition is continuous drift detection so a source that silently re-projects gets caught on the next batch rather than after a quarter of corrupt joins — that is the focus of [Detecting CRS Drift in Ingestion Pipelines](/spatial-lakehouse-fundamentals-architecture/crs-management-pipelines/detecting-crs-drift-in-ingestion-pipelines/). Pair CRS enforcement with the schema-evolution controls in [Managing spatial schema evolution in open table formats](/spatial-lakehouse-fundamentals-architecture/open-table-format-versioning/managing-spatial-schema-evolution-in-open-table-formats/) and the encoding rules in [GeoParquet encoding standards](/spatial-lakehouse-fundamentals-architecture/geoparquet-encoding-standards/) to keep the entire ingestion boundary — CRS, schema, and encoding — under one enforced contract.
