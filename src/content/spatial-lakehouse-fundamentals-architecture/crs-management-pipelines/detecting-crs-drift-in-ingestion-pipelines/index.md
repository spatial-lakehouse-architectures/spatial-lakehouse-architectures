# Detecting CRS Drift in Ingestion Pipelines

This guide gives you a complete Python recipe that inspects each incoming batch, compares its declared coordinate reference system against what the coordinates actually look like, and fails or flags the pipeline the instant an upstream source silently changes projection.

## Context and prerequisites

CRS drift is the failure where a producer re-projects its output without telling you — a shapefile export switches from lon/lat to a national grid, or a partner's API starts emitting web-mercator metres under an unchanged `EPSG:4326` label. The declared CRS still reads as before, so an assert-only gate passes, but the coordinates are now wrong by kilometres. This page is the detection half of the broader [CRS management pipeline](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/crs-management-pipelines/); it assumes you already normalize storage to the EPSG:4326 convention and want a guard that trusts coordinate evidence over declared metadata. You need `geopandas`, `pyproj`, `shapely`, and `numpy` installed (see the parent guide for pinned versions).

The core idea: a declared CRS is a claim, and coordinate ranges are evidence. Detection cross-examines the claim. Longitude/latitude data lives inside a ±180 / ±90 envelope; projected metres routinely reach into the hundreds of thousands or millions. When the declared CRS says one thing and the numeric magnitude says another, that contradiction is drift.

## Complete working solution

```python
import numpy as np
import geopandas as gpd
from pyproj import CRS
from dataclasses import dataclass, field

STORAGE_EPSG = 4326  # canonical lakehouse storage CRS (EPSG:4326 in prose)

# Coordinate envelope that geographic (degree-based) data must fit inside.
WGS84_BOUNDS = (-180.0, -90.0, 180.0, 90.0)


@dataclass
class DriftReport:
    ok: bool
    reasons: list = field(default_factory=list)
    declared_epsg: int = None
    observed_bounds: tuple = None

    def raise_if_drift(self):
        if not self.ok:
            raise ValueError("CRS drift detected: " + "; ".join(self.reasons))


def _looks_geographic(bounds):
    """Do the observed bounds fit inside the degree envelope?"""
    minx, miny, maxx, maxy = bounds
    return (WGS84_BOUNDS[0] <= minx and maxx <= WGS84_BOUNDS[2]
            and WGS84_BOUNDS[1] <= miny and maxy <= WGS84_BOUNDS[3])


def _degenerate_extent(bounds, min_span=1e-6):
    """A near-zero degree span usually means un-reprojected metres
    squeezed into a tiny fraction of a degree, or a single-point batch."""
    minx, miny, maxx, maxy = bounds
    return (maxx - minx) < min_span and (maxy - miny) < min_span


def detect_crs_drift(gdf, expected_epsg=STORAGE_EPSG, sample=200_000):
    """Cross-examine a batch's declared CRS against its coordinate evidence.

    Returns a DriftReport. Fails when the declared CRS is missing/unexpected,
    or when coordinate magnitudes contradict a geographic declaration.
    """
    reasons = []

    # 1. Declared-CRS check (the claim).
    declared = CRS.from_user_input(gdf.crs) if gdf.crs is not None else None
    declared_epsg = declared.to_epsg() if declared is not None else None
    if declared is None:
        reasons.append("no CRS declared on incoming batch")
    elif declared_epsg != expected_epsg:
        reasons.append(f"declared EPSG {declared_epsg} != expected {expected_epsg}")

    # 2. Evidence check (the coordinates). Sample for speed on large batches.
    geom = gdf.geometry
    if len(geom) > sample:
        geom = geom.sample(sample, random_state=0)
    bounds = tuple(geom.total_bounds)  # (minx, miny, maxx, maxy)

    if declared_epsg == expected_epsg:
        # Declared geographic — evidence must agree.
        if not _looks_geographic(bounds):
            reasons.append(
                f"declared EPSG:{expected_epsg} but bounds {bounds} exceed the "
                "degree envelope — coordinates look projected (drift)")
        elif _degenerate_extent(bounds) and len(gdf) > 1:
            reasons.append(
                f"declared geographic but extent {bounds} is degenerate — "
                "possible un-reprojected projected coordinates")

    return DriftReport(
        ok=(len(reasons) == 0),
        reasons=reasons,
        declared_epsg=declared_epsg,
        observed_bounds=bounds,
    )
```

```python
# --- Run it against a batch on the ingestion path ---
def guard_batch(path, declared_epsg=None):
    gdf = gpd.read_parquet(path)
    if declared_epsg is not None and gdf.crs is None:
        gdf = gdf.set_crs(declared_epsg, allow_override=True)

    report = detect_crs_drift(gdf, expected_epsg=STORAGE_EPSG)
    if not report.ok:
        # Hard-fail the pipeline; alert with structured context.
        print({"event": "crs_drift", "path": path,
               "declared_epsg": report.declared_epsg,
               "bounds": report.observed_bounds,
               "reasons": report.reasons})
        report.raise_if_drift()
    return report


if __name__ == "__main__":
    guard_batch("s3://lake/incoming/partner_points.parquet", declared_epsg=4326)
```

## Step-by-step walkthrough

1. **Two independent signals.** `detect_crs_drift` never trusts a single source. Step 1 reads the *declared* CRS from the GeoDataFrame; step 2 measures the *observed* coordinate bounds. Drift is a disagreement between them, so both must be gathered before any verdict.
2. **Declared-CRS check.** A missing CRS is immediate drift — an undeclared batch cannot be assumed canonical. A declared EPSG that is not the expected code is also flagged; this catches the honest case where a producer switched projection *and* correctly relabeled, which still breaks a table that expects EPSG:4326.
3. **Sampling for scale.** `total_bounds` over tens of millions of geometries is wasteful when a 200k random sample bounds the extent just as reliably. `random_state=0` keeps the check deterministic so the same batch always yields the same verdict — essential for reproducible CI runs.
4. **The envelope test.** `_looks_geographic` asks whether the coordinates physically fit inside ±180 / ±90. Web-mercator metres (EPSG:3857) reach ±20 037 508, so a mislabeled mercator batch blows past the envelope immediately and is caught even though its label reads EPSG:4326.
5. **The degenerate-extent heuristic.** The subtler failure is projected coordinates that happen to be small — or a local grid whose values do fall under 180. `_degenerate_extent` flags a multi-row batch whose entire spatial span collapses to under a micro-degree, which real geographic data covering an actual area never does. It is a heuristic, so it is scoped to `len(gdf) > 1` to avoid false-positives on single-point batches.
6. **Structured failure.** `guard_batch` emits a machine-readable drift event (for your log pipeline or alerting) *before* raising, so the alert carries the declared EPSG, the observed bounds, and the human-readable reasons. `raise_if_drift` then stops the pipeline so no drifted batch reaches the table.

## Common errors and fixes

| Error | Cause | Fix |
|---|---|---|
| Drift flagged on a legitimately tiny extent (single city block) | Degenerate-extent threshold too coarse for high-zoom data | Lower `min_span` or gate the heuristic on a minimum row count / known feed. |
| No drift caught though coordinates are wrong | A projected CRS whose values still fall inside ±180 (some local grids) | Add a positive assertion: require `gdf.crs` to `equals` the expected CRS, not just fit the envelope. |
| `CRSError` when reading `gdf.crs` | Batch carries an invalid or vendor CRS WKT | Wrap `CRS.from_user_input` in try/except and treat failures as drift. |
| Check passes locally, fails in CI | Different `pyproj`/PROJ data version resolves CRS codes differently | Pin `pyproj` and record `pyproj.proj_version_str` alongside the batch. |

## Verification

Prove the detector both catches drift and clears clean batches by feeding it a deliberately mislabeled frame:

```python
from shapely.geometry import Point
import geopandas as gpd

# A "drifted" batch: web-mercator metres wearing an EPSG:4326 label.
drifted = gpd.GeoDataFrame(
    geometry=[Point(-8237642.0, 4970241.0), Point(-8230000.0, 4975000.0)],
    crs=4326,  # the LIE — these are metres, not degrees
)
assert detect_crs_drift(drifted).ok is False

# A clean geographic batch.
clean = gpd.GeoDataFrame(
    geometry=[Point(-73.98, 40.75), Point(-73.95, 40.78)],
    crs=4326,
)
assert detect_crs_drift(clean).ok is True
print("drift detector verified")
```

Wire `guard_batch` into the ingestion stage of your [CRS management pipeline](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/crs-management-pipelines/) so every batch is cross-examined before write, and record the observed bounds per batch so a slow drift trend is visible over time. Pair it with the metadata assertions in [Validating GeoParquet metadata in CI](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/geoparquet-encoding-standards/validating-geoparquet-metadata-in-ci/) and the [GeoParquet encoding standards](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/geoparquet-encoding-standards/) so both the declared CRS metadata and the coordinate evidence are checked on the same boundary.

<figure class="diagram">
<svg viewBox="0 0 760 240" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Drift detection logic comparing declared CRS against observed coordinate bounds to reach a pass or fail verdict">
<title>CRS drift detection decision flow</title>
<desc>A batch's declared CRS and its observed coordinate bounds are checked independently; agreement passes to write, disagreement is flagged as drift and quarantined.</desc>
<defs>
<marker id="arw-crsdrift" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
<path d="M0 0 L10 5 L0 10 z" fill="#0e6e7d"/>
</marker>
</defs>
<rect x="0" y="0" width="760" height="240" fill="#f7fbfc"/>
<rect x="20" y="94" width="130" height="52" rx="6" fill="#ffffff" stroke="#cfe3e7" stroke-width="1.5"/>
<text x="85" y="116" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="bold" fill="#0d3b45">Incoming batch</text>
<text x="85" y="134" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">declared + geom</text>
<rect x="200" y="34" width="180" height="60" rx="6" fill="#ffffff" stroke="#0e6e7d" stroke-width="1.5"/>
<text x="290" y="58" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="bold" fill="#0d3b45">Claim: declared CRS</text>
<text x="290" y="78" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">== EPSG:4326 ?</text>
<rect x="200" y="146" width="180" height="60" rx="6" fill="#ffffff" stroke="#2f6e49" stroke-width="1.5"/>
<text x="290" y="170" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="bold" fill="#0d3b45">Evidence: bounds</text>
<text x="290" y="190" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">inside ±180 / ±90 ?</text>
<rect x="430" y="90" width="150" height="60" rx="6" fill="#ffffff" stroke="#9a5a17" stroke-width="1.5"/>
<text x="505" y="114" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="bold" fill="#0d3b45">Agree?</text>
<text x="505" y="134" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">claim vs evidence</text>
<rect x="628" y="46" width="120" height="52" rx="6" fill="#ffffff" stroke="#6a3d9a" stroke-width="1.5"/>
<text x="688" y="70" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="bold" fill="#2f6e49">Pass → write</text>
<text x="688" y="88" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">canonical</text>
<rect x="628" y="142" width="120" height="52" rx="6" fill="#ffffff" stroke="#9a5a17" stroke-width="1.5"/>
<text x="688" y="166" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="bold" fill="#9a5a17">Drift → fail</text>
<text x="688" y="184" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">quarantine</text>
<line x1="150" y1="112" x2="196" y2="74" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-crsdrift)"/>
<line x1="150" y1="128" x2="196" y2="168" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-crsdrift)"/>
<line x1="380" y1="64" x2="426" y2="102" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-crsdrift)"/>
<line x1="380" y1="176" x2="426" y2="138" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-crsdrift)"/>
<line x1="580" y1="108" x2="624" y2="80" stroke="#2f6e49" stroke-width="2" marker-end="url(#arw-crsdrift)"/>
<line x1="580" y1="132" x2="624" y2="160" stroke="#9a5a17" stroke-width="2" marker-end="url(#arw-crsdrift)"/>
</svg>
</figure>
