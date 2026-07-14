# Building a Schema Validation Pipeline for Geospatial Tables

This guide builds a complete Python and CI recipe that validates a geospatial table's schema before any write — checking the geometry column type, required CRS metadata, mandatory bounding-box columns, and non-null geometries — and fails the pipeline on any violation so corrupt spatial data never reaches the lakehouse.

## Context and prerequisites

The cheapest place to catch a broken geometry column is before it is written, not after it has corrupted a snapshot. Upstream shapefile exports, GeoParquet re-encodings, and DataFrame transforms all silently re-type the geometry column, drop CRS metadata, or emit null geometries; once committed, unwinding them means a rollback and re-ingest. A pre-write validation gate turns those failures into a red CI check instead. This is the write-protection discipline of [lakehouse maintenance automation](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/lakehouse-maintenance-automation/), and it complements the post-ingest [detecting CRS drift in ingestion pipelines](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/crs-management-pipelines/detecting-crs-drift-in-ingestion-pipelines/) and the artifact-level [validating GeoParquet metadata in CI](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/geoparquet-encoding-standards/validating-geoparquet-metadata-in-ci/). It uses PyArrow 16, Shapely 2, and pyproj 3.6, with geometry stored as WKB binary and CRS declared as EPSG:4326.

## Complete working solution

The validator inspects a PyArrow table's schema and data, raising a typed exception on the first violation. A thin CLI wrapper reads a Parquet batch and exits non-zero on failure, so it drops straight into a CI step.

```python
# validate_geo_schema.py
from __future__ import annotations
import sys
import pyarrow as pa
import pyarrow.parquet as pq
from pyproj import CRS

REQUIRED_BBOX = ("bbox_minx", "bbox_miny", "bbox_maxx", "bbox_maxy")
EXPECTED_EPSG = 4326

class SchemaViolation(Exception):
    """Raised when a geospatial table fails a pre-write contract check."""

def _check_geometry_type(schema: pa.Schema, geom_col: str) -> None:
    if geom_col not in schema.names:
        raise SchemaViolation(f"missing geometry column '{geom_col}'")
    t = schema.field(geom_col).type
    if not (pa.types.is_binary(t) or pa.types.is_large_binary(t)):
        raise SchemaViolation(
            f"'{geom_col}' must be WKB binary, got {t}")

def _check_crs(schema: pa.Schema, geom_col: str) -> None:
    meta = schema.field(geom_col).metadata or {}
    raw = meta.get(b"crs")
    if raw is None:
        raise SchemaViolation(f"'{geom_col}' has no CRS metadata")
    # accept any authority string pyproj can parse; assert the EPSG code
    epsg = CRS.from_user_input(raw.decode()).to_epsg()
    if epsg != EXPECTED_EPSG:
        raise SchemaViolation(
            f"CRS EPSG:{epsg} != required EPSG:{EXPECTED_EPSG}")

def _check_bbox_columns(schema: pa.Schema) -> None:
    missing = [c for c in REQUIRED_BBOX if c not in schema.names]
    if missing:
        raise SchemaViolation(f"missing bbox columns: {missing}")
    for c in REQUIRED_BBOX:
        if not pa.types.is_floating(schema.field(c).type):
            raise SchemaViolation(f"bbox column '{c}' must be float")

def _check_non_null_geometry(table: pa.Table, geom_col: str) -> None:
    nulls = table.column(geom_col).null_count
    if nulls > 0:
        raise SchemaViolation(f"{nulls} null geometry values found")

def validate(table: pa.Table, geom_col: str = "geometry") -> None:
    """Run the full contract. Raises SchemaViolation on the first failure."""
    schema = table.schema
    _check_geometry_type(schema, geom_col)
    _check_crs(schema, geom_col)
    _check_bbox_columns(schema)
    _check_non_null_geometry(table, geom_col)

def main(path: str) -> int:
    table = pq.read_table(path)
    try:
        validate(table)
    except SchemaViolation as exc:
        print(f"SCHEMA VALIDATION FAILED: {exc}", file=sys.stderr)
        return 1
    print(f"OK: {path} passed the geospatial schema contract "
          f"({table.num_rows} rows)")
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
```

The GitHub Actions workflow runs the validator against staged data before the write job is allowed to proceed:

```yaml
name: Validate Geospatial Schema
on:
  push:
    paths: ['data/staged/**.parquet']

jobs:
  validate-schema:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - name: Install deps
        run: pip install "pyarrow==16.1.0" "pyproj==3.6.1" "shapely==2.0.4"
      - name: Validate staged geospatial batch
        run: |
          set -e
          for f in data/staged/*.parquet; do
            python validate_geo_schema.py "$f"
          done
```

## Step-by-step walkthrough

1. **`SchemaViolation`** is a dedicated exception so callers (and the CLI) can distinguish a contract failure from an unrelated crash and translate it into a non-zero exit rather than a stack-trace-and-succeed.

2. **`_check_geometry_type`** asserts the column exists and is `binary` or `large_binary`. WKB is a byte string; if an upstream step re-typed it to `string` or a struct, spatial UDFs downstream will silently misread it, so this is the highest-value check.

3. **`_check_crs`** reads the field's Arrow metadata, requires a `crs` key, and parses it with `pyproj.CRS.from_user_input` — which accepts WKT, PROJJSON, or `EPSG:4326` strings — then asserts the numeric EPSG code equals 4326. Comparing the parsed EPSG code rather than the raw string tolerates equivalent CRS encodings while still rejecting the wrong system.

4. **`_check_bbox_columns`** enforces the presence and float type of the four `bbox_*` columns that file-level [predicate pushdown](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/z-ordering-for-geospatial-queries/) depends on. A table missing these will still write, but every spatial range query will full-scan.

5. **`_check_non_null_geometry`** uses Arrow's `null_count`, a metadata-only operation, to reject batches containing null geometries before they poison joins.

6. **`main`** reads the Parquet batch, runs the contract, and returns an exit code. In CI a non-zero exit fails the job and blocks the downstream write — the whole point of a pre-write gate. This mirrors the fail-fast principle used throughout the [Python Ecosystem & Integration Workflows](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/) section.

### Schema-level versus value-level checks

The four checks above split into two categories, and the distinction governs how much data each must touch. **Schema-level checks** — geometry type, CRS metadata, bbox column presence and type — read only the Arrow schema object, which Parquet stores in the file footer. They are effectively free: reading a 10 GB Parquet file's schema costs a single footer read, not a scan. **Value-level checks** — null geometry counts — must touch column data. The null check here is still cheap because Arrow tracks null counts in per-column metadata (`null_count` is O(1), not a scan), but if you extend the gate to catch invalid geometries (self-intersecting polygons, non-closed rings) you cross into genuine per-row work with Shapely, which is orders of magnitude more expensive. Keep the fast schema checks first so the gate short-circuits on the common failures before paying for any value-level pass.

If you do add a geometry-validity pass, run it on a sample rather than the full batch for large loads, and make it explicit that it is a sample so a caller does not mistake a passing sample for a fully validated batch:

```python
import shapely
from shapely import from_wkb

def sample_geometry_validity(table: pa.Table, geom_col: str = "geometry",
                             sample: int = 1000) -> None:
    """Check a sample of geometries are valid (closed rings, no self-intersection)."""
    n = min(sample, table.num_rows)
    wkb_values = table.column(geom_col).slice(0, n).to_pylist()
    for i, raw in enumerate(wkb_values):
        geom = from_wkb(raw)
        if not geom.is_valid:
            reason = shapely.is_valid_reason(geom)
            raise SchemaViolation(f"invalid geometry at row {i}: {reason}")
```

### Where the gate belongs in the pipeline

Place the validator at the boundary the data crosses from "external and untrusted" to "written into the lakehouse" — typically the last step before a PyIceberg or delta-rs write call, and mirrored as a CI check on staged artifacts so violations surface on the pull request rather than at runtime. Running it in both places is deliberate: the CI check gives fast feedback to whoever changed the pipeline, and the runtime call is the last line of defence for data that arrives without going through CI at all (a manual backfill, an ad-hoc load). The gate is intentionally narrow — it asserts the contract the lakehouse depends on and nothing more — so it stays fast enough to run on every single write without becoming a bottleneck.

## Common errors and fixes

| Error | Cause | Fix |
|---|---|---|
| `'geometry' must be WKB binary, got string` | Upstream export serialized geometry as WKT text | Re-encode to WKB (`shapely.to_wkb`) before the write; fix the export step |
| `'geometry' has no CRS metadata` | GeoPandas-to-Arrow conversion dropped field metadata | Attach CRS to the Arrow field metadata explicitly when building the table |
| `CRS EPSG:3857 != required EPSG:4326` | Data in Web Mercator, contract expects WGS84 | Reproject with pyproj before staging, or widen the contract if 3857 is intended |
| `N null geometry values found` | Failed geocode or empty feature left null rows | Filter nulls upstream, or route them to a quarantine table instead of the main write |

## Verification

Run the validator against a deliberately broken batch to confirm it fails, then against a good batch to confirm it passes. This is the minimal test that the gate actually gates:

```python
import pyarrow as pa
from shapely import Point, to_wkb
from validate_geo_schema import validate, SchemaViolation

def build(good: bool) -> pa.Table:
    geom = to_wkb(Point(11.5, 48.1))
    field_meta = {b"crs": b"EPSG:4326"} if good else {}
    geom_field = pa.field("geometry", pa.binary(), metadata=field_meta)
    schema = pa.schema([
        geom_field,
        pa.field("bbox_minx", pa.float64()), pa.field("bbox_miny", pa.float64()),
        pa.field("bbox_maxx", pa.float64()), pa.field("bbox_maxy", pa.float64()),
    ])
    return pa.table({
        "geometry": [geom],
        "bbox_minx": [11.5], "bbox_miny": [48.1],
        "bbox_maxx": [11.5], "bbox_maxy": [48.1],
    }, schema=schema)

# good batch passes silently
validate(build(good=True))
print("good batch: passed")

# bad batch (no CRS metadata) must raise
try:
    validate(build(good=False))
    raise AssertionError("expected SchemaViolation")
except SchemaViolation as exc:
    print(f"bad batch correctly rejected: {exc}")
```

<figure class="diagram">
<svg viewBox="0 0 760 250" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Staged batch passing through four validation checks into the table or rejected to CI failure">
<defs>
<marker id="arw-geo-val" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#0e6e7d"/></marker>
<marker id="arw-geo-val-fail" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#9a5a17"/></marker>
</defs>
<rect x="0" y="0" width="760" height="250" fill="#f7fbfc"/>
<text x="380" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Pre-write geospatial schema gate</text>
<rect x="20" y="95" width="130" height="60" rx="8" fill="#ffffff" stroke="#6a3d9a" stroke-width="2"/>
<text x="85" y="122" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="600" fill="#0d3b45">staged batch</text>
<text x="85" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">Parquet / Arrow</text>
<rect x="205" y="70" width="230" height="110" rx="8" fill="#ffffff" stroke="#0e6e7d" stroke-width="2"/>
<text x="320" y="92" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="600" fill="#0d3b45">validation gate</text>
<text x="320" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">1. geometry is WKB binary</text>
<text x="320" y="132" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">2. CRS metadata = EPSG:4326</text>
<text x="320" y="150" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">3. bbox columns present</text>
<text x="320" y="168" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">4. no null geometry</text>
<rect x="500" y="60" width="230" height="52" rx="8" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<text x="615" y="83" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="600" fill="#0d3b45">pass → write to table</text>
<text x="615" y="101" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">commit reaches lakehouse</text>
<rect x="500" y="140" width="230" height="52" rx="8" fill="#ffffff" stroke="#9a5a17" stroke-width="2"/>
<text x="615" y="163" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="600" fill="#0d3b45">fail → exit 1</text>
<text x="615" y="181" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">CI blocks the write</text>
<line x1="150" y1="125" x2="205" y2="125" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-geo-val)"/>
<line x1="435" y1="105" x2="500" y2="86" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-geo-val)"/>
<line x1="435" y1="150" x2="500" y2="166" stroke="#9a5a17" stroke-width="2" marker-end="url(#arw-geo-val-fail)"/>
<text x="380" y="228" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#3d5a63">First violation short-circuits: corrupt spatial data never reaches a snapshot</text>
</svg>
</figure>

Once a batch passes the gate and is written, the ongoing maintenance in [compacting spatial Iceberg tables with rewrite_data_files](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/lakehouse-maintenance-automation/compacting-spatial-iceberg-tables-with-rewrite-data-files/) and [scheduling VACUUM for spatial Delta tables](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/lakehouse-maintenance-automation/scheduling-vacuum-for-spatial-delta-tables/) keeps it healthy. The authoritative references for the underlying contracts are the [PyArrow schema and metadata API](https://arrow.apache.org/docs/python/generated/pyarrow.Schema.html) and the [OGC GeoParquet specification](https://geoparquet.org/releases/v1.0.0/) that standardizes the geometry column and CRS metadata this gate enforces.
