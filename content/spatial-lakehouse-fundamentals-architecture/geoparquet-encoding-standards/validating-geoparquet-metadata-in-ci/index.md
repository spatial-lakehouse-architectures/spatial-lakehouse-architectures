# Validating GeoParquet Metadata in CI

This guide gives you a complete Python plus GitHub Actions recipe that inspects a GeoParquet file's `geo` metadata — version, `primary_column`, CRS presence, and the bbox `covering` — and fails the build before the file is promoted, so encoding drift never reaches a production table.

## Context and prerequisites

A GeoParquet file is only interoperable if its footer `geo` block is well-formed: the wrong version, a missing `primary_column`, a dropped CRS, or an absent `covering` bbox each silently breaks a downstream reader or defeats file skipping. Rather than discover that at query time, assert the contract in continuous integration. This page builds on the field definitions in the [GeoParquet encoding standards](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/geoparquet-encoding-standards/) topic area and the layout trade-offs in [GeoParquet vs WKB column storage](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/geoparquet-encoding-standards/geoparquet-vs-wkb-column-storage-trade-offs/). You need pyarrow 17+ and pytest; no cloud credentials are required because validation runs on the file's footer alone. The CRS assertions here complement the runtime enforcement in [detecting CRS drift in ingestion pipelines](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/crs-management-pipelines/detecting-crs-drift-in-ingestion-pipelines/).

## Complete working solution

Two files: a reusable validator module and a pytest wrapper. The validator reads only the Parquet footer, so it is fast and safe to run on large files. It returns a list of human-readable failures; an empty list means the file conforms.

```python
# geoparquet_gate.py
import json
import sys
import pyarrow.parquet as pq

REQUIRED_VERSION_PREFIX = "1.1"
EXPECTED_EPSG = 4326  # numeric EPSG literal for this project's canonical CRS


def read_geo_metadata(path):
    """Return the parsed 'geo' block from a Parquet file footer, or None."""
    kv = pq.ParquetFile(path).metadata.metadata or {}
    if b"geo" not in kv:
        return None
    return json.loads(kv[b"geo"].decode("utf-8"))


def validate_geoparquet(path):
    """Return a list of validation failures; empty list means the file is valid."""
    failures = []
    geo = read_geo_metadata(path)
    if geo is None:
        return [f"{path}: no 'geo' metadata block in footer"]

    version = geo.get("version", "")
    if not version.startswith(REQUIRED_VERSION_PREFIX):
        failures.append(f"{path}: version {version!r} is not {REQUIRED_VERSION_PREFIX}.x")

    primary = geo.get("primary_column")
    if not primary:
        failures.append(f"{path}: missing 'primary_column'")
        return failures  # nothing else is checkable without it

    col = geo.get("columns", {}).get(primary)
    if col is None:
        failures.append(f"{path}: primary_column {primary!r} absent from 'columns'")
        return failures

    crs = col.get("crs")
    if not crs:
        failures.append(f"{path}: CRS missing for column {primary!r}")
    else:
        code = crs.get("id", {}).get("code")
        if code != EXPECTED_EPSG:
            failures.append(f"{path}: CRS EPSG code {code} != {EXPECTED_EPSG}")

    covering = col.get("covering")
    if not covering or "bbox" not in covering:
        failures.append(f"{path}: no bbox 'covering' for column {primary!r}")

    if col.get("encoding") not in {"WKB", "point", "linestring", "polygon",
                                   "multipoint", "multilinestring", "multipolygon"}:
        failures.append(f"{path}: unexpected encoding {col.get('encoding')!r}")

    return failures


def main(paths):
    all_failures = []
    for p in paths:
        all_failures.extend(validate_geoparquet(p))
    if all_failures:
        print("GeoParquet validation FAILED:")
        for f in all_failures:
            print(f"  - {f}")
        return 1
    print(f"GeoParquet validation passed for {len(paths)} file(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
```

```yaml
# .github/workflows/geoparquet-gate.yml
name: GeoParquet metadata gate
on:
  pull_request:
    paths:
      - "data/**.parquet"
      - "geoparquet_gate.py"
      - "tests/test_geoparquet_gate.py"
jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - name: Install dependencies
        run: python -m pip install "pyarrow>=17.0.0" "pytest>=8.0" "geopandas>=1.0.1" "shapely>=2.0.4"
      - name: Validate GeoParquet metadata
        run: pytest tests/test_geoparquet_gate.py -v
```

## Step-by-step walkthrough

1. **Footer-only read.** `pq.ParquetFile(path).metadata.metadata` returns the file's key-value metadata without scanning any row groups, so validation cost is independent of file size.
2. **Presence gate.** A missing `b"geo"` key means the file is not GeoParquet at all — the first and most common drift, caught immediately.
3. **Version gate.** `version.startswith("1.1")` rejects files written against an older spec whose `covering` semantics differ. Tighten or loosen the prefix to match your promotion policy.
4. **Primary column gate.** Everything else keys off `primary_column`; if it is absent or points at a column not in `columns`, the function returns early with a precise message rather than raising.
5. **CRS gate.** It asserts both that a `crs` object exists and that its EPSG `code` equals the numeric literal `4326`. Dropping the code check but keeping the presence check is a valid looser policy for multi-CRS datasets.
6. **Covering gate.** A missing `covering.bbox` means engines cannot use per-row bounds for file skipping — a performance regression that this catches before merge.
7. **Exit code.** `main` returns `1` on any failure so the CI step fails; the pytest wrapper below turns each file into an assertion for readable reports.

```python
# tests/test_geoparquet_gate.py
import glob
import pytest
from geoparquet_gate import validate_geoparquet

PARQUET_FILES = sorted(glob.glob("data/**/*.parquet", recursive=True))


@pytest.mark.parametrize("path", PARQUET_FILES or [pytest.param(None, marks=pytest.mark.skip)])
def test_geoparquet_metadata_is_valid(path):
    failures = validate_geoparquet(path)
    assert not failures, "\n".join(failures)
```

## Common errors and fixes

| Error | Cause | Fix |
|---|---|---|
| `no 'geo' metadata block in footer` | File written with a plain Parquet writer, not GeoParquet | Rewrite via GeoPandas `to_parquet(schema_version="1.1.0")` |
| `CRS EPSG code None != 4326` | GeoDataFrame written without a CRS set | Set `crs=4326` before writing; reject undefined-CRS inputs upstream |
| `no bbox 'covering'` | `write_covering_bbox=True` was omitted at write time | Re-export with the covering flag enabled |
| `version '1.0.0' is not 1.1.x` | File produced by an older GeoParquet writer | Upgrade the writer or relax `REQUIRED_VERSION_PREFIX` deliberately |
| Job passes but skips every file | `data/**.parquet` glob matched nothing | Confirm the path filter and that artifacts are checked out |

## Verification

Prove the gate before trusting it: build one conforming file and one broken file, then confirm the validator disagrees about them.

```python
import geopandas
import pyarrow as pa
import pyarrow.parquet as pq
from shapely.geometry import Point
from geoparquet_gate import validate_geoparquet

good = geopandas.GeoDataFrame(
    {"id": [1]}, geometry=[Point(-122.4, 37.8)], crs=4326)
good.to_parquet("good.parquet", schema_version="1.1.0", write_covering_bbox=True)

pq.write_table(pa.table({"id": [1]}), "bad.parquet")  # no geo metadata at all

assert validate_geoparquet("good.parquet") == []
assert validate_geoparquet("bad.parquet")  # non-empty -> fails the build
print("gate distinguishes conforming from non-conforming files")
```

## CI gate flow

<figure class="diagram">
<svg viewBox="0 0 760 210" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Continuous integration flow validating GeoParquet metadata: pull request triggers a footer read, four metadata assertions gate the result into promote or block">
<title>GeoParquet metadata CI gate</title>
<desc>A pull request triggers a footer-only metadata read, which feeds four assertions — version, primary_column, CRS present, bbox covering — and a passing result promotes the file while any failure blocks the merge.</desc>
<defs>
<marker id="arw-ci" markerWidth="9" markerHeight="9" refX="7" refY="4.5" orient="auto"><path d="M0,0 L9,4.5 L0,9 z" fill="#0e6e7d"/></marker>
</defs>
<rect x="0" y="0" width="760" height="210" fill="#f7fbfc"/>
<rect x="16" y="78" width="120" height="54" rx="6" fill="#ffffff" stroke="#0e6e7d"/>
<text x="76" y="102" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">Pull request</text>
<text x="76" y="120" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">.parquet changed</text>
<rect x="170" y="78" width="130" height="54" rx="6" fill="#ffffff" stroke="#6a3d9a"/>
<text x="235" y="100" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">Read footer</text>
<text x="235" y="118" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#6a3d9a">"geo" block only</text>
<rect x="334" y="24" width="150" height="30" rx="4" fill="#ffffff" stroke="#2f6e49"/>
<text x="409" y="44" text-anchor="middle" font-family="sans-serif" font-size="11.5" fill="#0d3b45">version == 1.1.x</text>
<rect x="334" y="62" width="150" height="30" rx="4" fill="#ffffff" stroke="#2f6e49"/>
<text x="409" y="82" text-anchor="middle" font-family="sans-serif" font-size="11.5" fill="#0d3b45">primary_column set</text>
<rect x="334" y="100" width="150" height="30" rx="4" fill="#ffffff" stroke="#2f6e49"/>
<text x="409" y="120" text-anchor="middle" font-family="sans-serif" font-size="11.5" fill="#0d3b45">CRS = EPSG 4326</text>
<rect x="334" y="138" width="150" height="30" rx="4" fill="#ffffff" stroke="#2f6e49"/>
<text x="409" y="158" text-anchor="middle" font-family="sans-serif" font-size="11.5" fill="#0d3b45">bbox covering set</text>
<rect x="524" y="48" width="120" height="44" rx="6" fill="#ffffff" stroke="#2f6e49"/>
<text x="584" y="75" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#2f6e49">Promote</text>
<rect x="524" y="120" width="120" height="44" rx="6" fill="#ffffff" stroke="#9a5a17"/>
<text x="584" y="147" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#9a5a17">Block merge</text>
<line x1="136" y1="105" x2="168" y2="105" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-ci)"/>
<line x1="300" y1="95" x2="332" y2="70" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-ci)"/>
<line x1="300" y1="105" x2="332" y2="115" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-ci)"/>
<line x1="484" y1="70" x2="522" y2="70" stroke="#2f6e49" stroke-width="2" marker-end="url(#arw-ci)"/>
<line x1="484" y1="150" x2="522" y2="145" stroke="#9a5a17" stroke-width="2" marker-end="url(#arw-ci)"/>
<text x="504" y="112" text-anchor="middle" font-family="sans-serif" font-size="10.5" fill="#33707d">all pass / any fail</text>
</svg>
</figure>

Wiring this gate into promotion closes the loop opened by the [GeoParquet encoding standards](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/geoparquet-encoding-standards/) topic area within the [Spatial Lakehouse Fundamentals & Architecture](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/) section, and it pairs naturally with the schema checks around [Iceberg spatial type support](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/iceberg-spatial-type-support/). Keep the assertions aligned with the field names in the [GeoParquet specification](https://geoparquet.org/releases/v1.1.0/) and the CRS model in the [OGC Simple Features Access](https://www.ogc.org/standard/sfa/) standard so the gate stays correct as the spec evolves.
