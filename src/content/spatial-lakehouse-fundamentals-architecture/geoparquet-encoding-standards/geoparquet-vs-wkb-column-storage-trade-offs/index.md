# GeoParquet vs WKB Column Storage: Trade-offs

This guide settles a concrete storage decision: whether to persist geometry as a GeoParquet-native column carrying file-level `geo` metadata and a bbox covering, or as a plain WKB `BINARY` column whose spatial semantics live entirely in your pipeline and catalog.

## Context and prerequisites

Both approaches ultimately serialize geometry as Well-Known Binary — the difference is the metadata contract wrapped around those bytes. GeoParquet publishes CRS, encoding, and bounding-box covering into the Parquet footer so any compliant tool self-configures; the raw-column approach ships only the bytes and expects every reader to know the CRS and encoding out of band. This page is a companion to the [GeoParquet encoding standards](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/geoparquet-encoding-standards/) topic area and assumes you have GeoPandas 1.0+, pyarrow 17+, and Shapely 2.0+ installed. The choice interacts directly with how your [Iceberg spatial tables](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/iceberg-spatial-type-support/) expose statistics for [predicate pushdown](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/predicate-pushdown-optimization/).

## Complete working solution

The script below writes the identical geometries two ways from one GeoDataFrame — once as GeoParquet 1.1 with a covering bbox, once as a raw WKB `BINARY` column plus hand-rolled bbox columns — then reports the metadata, schema, and file size of each so the trade-offs are measurable rather than asserted.

```python
import json
import geopandas
import pyarrow as pa
import pyarrow.parquet as pq
from shapely import to_wkb
from shapely.geometry import Point, Polygon

# --- Shared source data, explicit CRS via numeric EPSG literal ---------------
gdf = geopandas.GeoDataFrame(
    {"feature_id": ["a", "b", "c"], "kind": ["pt", "pt", "poly"]},
    geometry=[
        Point(-122.4194, 37.7749),
        Point(-122.2712, 37.8044),
        Polygon([(-122.52, 37.70), (-122.35, 37.70),
                 (-122.35, 37.83), (-122.52, 37.83)]),
    ],
    crs=4326,
)

# --- Option A: GeoParquet-native geometry column ----------------------------
gdf.to_parquet(
    "geo_native.parquet",
    engine="pyarrow",
    schema_version="1.1.0",
    write_covering_bbox=True,
    geometry_encoding="WKB",
    compression="zstd",
)

# --- Option B: plain WKB BINARY column + explicit bbox DOUBLE columns --------
bounds = gdf.geometry.bounds  # minx, miny, maxx, maxy per row
plain = pa.table({
    "feature_id": pa.array(gdf["feature_id"]),
    "kind": pa.array(gdf["kind"]),
    "geometry_wkb": pa.array([to_wkb(g) for g in gdf.geometry], type=pa.binary()),
    "bbox_min_x": pa.array(bounds["minx"], type=pa.float64()),
    "bbox_min_y": pa.array(bounds["miny"], type=pa.float64()),
    "bbox_max_x": pa.array(bounds["maxx"], type=pa.float64()),
    "bbox_max_y": pa.array(bounds["maxy"], type=pa.float64()),
})
pq.write_table(plain, "wkb_plain.parquet", compression="zstd")


def describe(path):
    pf = pq.ParquetFile(path)
    kv = pf.metadata.metadata or {}
    has_geo = b"geo" in kv
    geo = json.loads(kv[b"geo"].decode()) if has_geo else None
    return {
        "path": path,
        "has_geo_metadata": has_geo,
        "geoparquet_version": geo["version"] if geo else None,
        "crs_in_file": bool(geo and geo["columns"][geo["primary_column"]].get("crs")),
        "columns": [f.name for f in pf.schema_arrow],
        "size_bytes": pf.metadata.serialized_size,
    }


for p in ("geo_native.parquet", "wkb_plain.parquet"):
    print(json.dumps(describe(p), indent=2, default=str))
```

## Step-by-step walkthrough

1. **One source, two writers.** A single `GeoDataFrame` with `crs=4326` feeds both paths, so any difference is purely the storage contract, not the data.
2. **Option A** calls `to_parquet(schema_version="1.1.0", write_covering_bbox=True)`. GeoPandas serializes each geometry to WKB, writes the `geo` footer block (version, `primary_column`, encoding, PROJJSON CRS), and materializes a `bbox` struct column plus a `covering.bbox` pointer.
3. **Option B** serializes geometry with Shapely's `to_wkb` into a `pa.binary()` column and computes four `DOUBLE` bbox columns by hand from `GeoDataFrame.bounds`. There is no `geo` metadata — the CRS `4326` exists only in your head and your catalog.
4. **`describe()`** reads each file's footer key-value metadata. Only Option A returns `has_geo_metadata=True` and a non-null `crs_in_file`; Option B exposes flat `bbox_min_x`-style columns instead of a nested `covering` struct.
5. **Pushdown mechanics differ.** Option A's `bbox` struct is read by GeoParquet-aware engines (DuckDB spatial ≥ 1.0, recent GDAL) automatically; Option B's flat columns are pushed down by any engine — including plain Spark SQL or Trino — because they are ordinary numeric columns with Parquet min/max statistics.

The decision table below summarizes where each wins.

| Dimension | GeoParquet-native column | Raw WKB `BINARY` column |
|---|---|---|
| Interoperability | High — CRS + encoding self-described in-file | Low — readers need external CRS knowledge |
| CRS storage | PROJJSON inside `geo` metadata | Not in file; catalog/convention only |
| Bbox statistics | `covering` struct, read by GeoParquet-aware engines | Flat `DOUBLE` columns, read by every engine |
| Engine support breadth | Growing (DuckDB, GDAL, Sedona, GeoPandas) | Universal — it is just binary + doubles |
| Predicate pushdown | Automatic for GeoParquet 1.1 readers | Automatic anywhere via numeric min/max |
| Table-format friendliness | File KV metadata may be dropped by Iceberg/Delta | Fully first-class columns in any catalog |
| Failure mode | Non-1.1 reader ignores `covering` | Reader silently assumes wrong CRS |

The pragmatic answer for lakehouse tables is often *both*: write GeoParquet metadata for interoperability and keep explicit bbox `DOUBLE` columns so engines that never parse the `geo` block still prune files. Pure raw-WKB makes sense when the catalog is authoritative and readers are homogeneous; pure GeoParquet makes sense when files leave your platform.

## What Each Choice Costs a Future Reader

The trade-off is easiest to judge from the perspective of somebody who finds the files in three years with no access to the pipeline that produced them.

<figure class="diagram">
<svg viewBox="0 0 758 238" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Comparison of a self-describing GeoParquet file against a plain WKB binary column, from the perspective of a future reader who must determine the encoding, coordinate reference system and geometry types">
<rect x="0" y="0" width="758" height="238" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">What a reader can learn from the file alone</text>
<rect x="34" y="58" width="330" height="168" rx="8" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<text x="199" y="84" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#2f6e49">GeoParquet column</text>
<text x="199" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">encoding: stated</text>
<text x="199" y="134" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">CRS: stated as PROJJSON</text>
<text x="199" y="156" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">geometry types: stated</text>
<text x="199" y="178" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">edges: stated</text>
<text x="199" y="206" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">cost: writer must support the spec</text>
<rect x="416" y="58" width="330" height="168" rx="8" fill="#ffffff" stroke="#9a5a17" stroke-width="2"/>
<text x="581" y="84" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#9a5a17">plain WKB BINARY</text>
<text x="581" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">encoding: inferred from bytes</text>
<text x="581" y="134" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">CRS: unknown</text>
<text x="581" y="156" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">geometry types: unknown</text>
<text x="581" y="178" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">edges: assumed planar</text>
<text x="581" y="206" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">cost: the contract lives outside the file</text>
</svg>
</figure>

The two "unknown" rows are the ones that turn into incidents. Encoding is recoverable — WKB has a recognisable byte-order flag and a small set of valid type codes, so a determined reader can work it out — but the coordinate reference system is not recoverable from the coordinates alone in any general way. Values between -180 and 180 are consistent with 4326, with 4258, with 4277 and with a dozen other systems whose datums differ by metres. Guessing produces data that is plausibly wrong.

That asymmetry is why the recommendation lands where it does: use the plain binary column when the table is fully governed by a catalog that carries the CRS as a property and every consumer reaches the data through that catalog, and use GeoParquet metadata whenever files might be read directly, copied between buckets, handed to a partner, or archived. The second case is more common than teams expect, because it includes every incident investigation.

## Measuring the Real Storage Difference

The size argument between the two options is frequently overstated, and it is easy to settle empirically on your own data rather than accepting a general claim.

<figure class="diagram">
<svg viewBox="0 0 748 228" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Storage breakdown of a spatial Parquet file showing that the geometry payload dominates while the geo metadata block is a fixed few kilobytes regardless of row count">
<rect x="0" y="0" width="748" height="228" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Where the bytes go in a 512 MB spatial file</text>
<rect x="70" y="70" width="470" height="52" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="1.5"/>
<text x="305" y="102" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">WKB geometry payload — ~78%</text>
<rect x="540" y="70" width="118" height="52" fill="#e6f0ea" stroke="#2f6e49" stroke-width="1.5"/>
<text x="599" y="102" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">attributes</text>
<rect x="658" y="70" width="56" height="52" fill="#f2e8da" stroke="#9a5a17" stroke-width="1.5"/>
<text x="686" y="102" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">bbox</text>
<rect x="714" y="70" width="12" height="52" fill="#faf8fc" stroke="#6a3d9a" stroke-width="1.5"/>
<text x="720" y="152" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#6a3d9a">geo</text>
<text x="720" y="170" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#6a3d9a">~4 KB</text>
<text x="390" y="212" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">The metadata block does not scale with row count — its cost is a rounding error</text>
</svg>
</figure>

The `geo` metadata is a few kilobytes per file no matter how many rows the file holds, so at any realistic file size it is invisible. The four bounding-box columns are a genuine cost — thirty-two bytes per row before compression, though they compress extremely well because neighbouring rows in a sorted file have near-identical bounds — and they are worth it in every case where the table is queried by location, which is the entire premise of storing it here.

## Common errors and fixes

| Error | Cause | Fix |
|---|---|---|
| `KeyError: b'geo'` when inspecting Option B | Raw WKB files carry no `geo` block by design | Guard with `b"geo" in metadata` before decoding |
| GeoParquet file loses its CRS after an Iceberg write | Table format did not surface file KV metadata | Persist CRS in table properties; add explicit bbox columns |
| Spark ignores the `covering` bbox | Engine reads GeoParquet 1.0 metadata only | Query the flat bbox columns, or upgrade the reader |
| `to_wkb` raises on a null geometry | WKB has no encoding for null | Filter nulls or map them to `None` before the array build |

## Verification

Confirm both files return the same geometries and that only the GeoParquet file self-reports its CRS.

```python
import geopandas

a = geopandas.read_parquet("geo_native.parquet")
assert a.crs.to_epsg() == 4326                      # CRS recovered from metadata
assert a.geometry.geom_type.tolist() == ["Point", "Point", "Polygon"]

b = geopandas.read_parquet("wkb_plain.parquet", columns=["feature_id"]) \
    if False else None  # raw file needs manual WKB decode + CRS assignment
print("geo_native self-describes CRS:", a.crs is not None)
```

## Storage layout compared

<figure class="diagram">
<svg viewBox="0 0 752 240" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Side by side comparison of a GeoParquet-native geometry column with geo metadata versus a plain WKB binary column with hand-rolled bbox columns">
<title>GeoParquet-native vs raw WKB layout</title>
<desc>Left: a file with a WKB column, a bbox struct, and a geo footer holding CRS and covering. Right: a file with a WKB binary column and four flat bbox double columns but no geo footer, requiring external CRS knowledge.</desc>
<defs>
<marker id="arw-trade" markerWidth="9" markerHeight="9" refX="7" refY="4.5" orient="auto"><path d="M0,0 L9,4.5 L0,9 z" fill="#6a3d9a"/></marker>
</defs>
<rect x="0" y="0" width="752" height="240" fill="#f7fbfc"/>
<rect x="20" y="24" width="340" height="204" rx="6" fill="#ffffff" stroke="#cfe3e7"/>
<text x="190" y="46" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="bold" fill="#0d3b45">Option A — GeoParquet-native</text>
<rect x="38" y="58" width="304" height="34" rx="3" fill="#ffffff" stroke="#0e6e7d"/>
<text x="190" y="80" text-anchor="middle" font-family="sans-serif" font-size="11.5" fill="#0d3b45">geometry: WKB BINARY</text>
<rect x="38" y="98" width="304" height="34" rx="3" fill="#ffffff" stroke="#2f6e49"/>
<text x="190" y="120" text-anchor="middle" font-family="sans-serif" font-size="11.5" fill="#0d3b45">bbox: struct{xmin,ymin,xmax,ymax}</text>
<rect x="38" y="138" width="304" height="72" rx="3" fill="#f7fbfc" stroke="#6a3d9a"/>
<text x="190" y="158" text-anchor="middle" font-family="sans-serif" font-size="11.5" font-weight="bold" fill="#6a3d9a">footer "geo"</text>
<text x="190" y="176" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">crs: PROJJSON (EPSG 4326)</text>
<text x="190" y="194" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">covering.bbox -&gt; bbox</text>
<rect x="400" y="24" width="340" height="204" rx="6" fill="#ffffff" stroke="#cfe3e7"/>
<text x="570" y="46" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="bold" fill="#0d3b45">Option B — raw WKB column</text>
<rect x="418" y="58" width="304" height="34" rx="3" fill="#ffffff" stroke="#0e6e7d"/>
<text x="570" y="80" text-anchor="middle" font-family="sans-serif" font-size="11.5" fill="#0d3b45">geometry_wkb: BINARY</text>
<rect x="418" y="98" width="304" height="52" rx="3" fill="#ffffff" stroke="#9a5a17"/>
<text x="570" y="118" text-anchor="middle" font-family="sans-serif" font-size="11.5" fill="#0d3b45">bbox_min_x, bbox_min_y (DOUBLE)</text>
<text x="570" y="138" text-anchor="middle" font-family="sans-serif" font-size="11.5" fill="#0d3b45">bbox_max_x, bbox_max_y (DOUBLE)</text>
<rect x="418" y="156" width="304" height="54" rx="3" fill="#f7fbfc" stroke="#cfe3e7" stroke-dasharray="5 3"/>
<text x="570" y="178" text-anchor="middle" font-family="sans-serif" font-size="11.5" font-weight="bold" fill="#33707d">no "geo" footer</text>
<text x="570" y="196" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">CRS known only externally</text>
<line x1="360" y1="174" x2="416" y2="174" stroke="#6a3d9a" stroke-width="2" marker-end="url(#arw-trade)"/>
</svg>
</figure>

For the full specification of the metadata Option A writes, return to [GeoParquet encoding standards](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/geoparquet-encoding-standards/); to keep either layout honest in a pipeline, wire up [validating GeoParquet metadata in CI](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/geoparquet-encoding-standards/validating-geoparquet-metadata-in-ci/). The authoritative field definitions live in the [GeoParquet specification](https://geoparquet.org/releases/v1.1.0/) and the [OGC Simple Features Access](https://www.ogc.org/standard/sfa/) standard.

## Choosing Once, Per Table

Resist the temptation to decide per pipeline. A platform where some spatial tables are self-describing and others are not forces every consumer to handle both cases, which in practice means every consumer handles the simpler case and quietly mishandles the other. Pick one convention, apply it to every spatial table, and make the exception require a written justification. The convention that ages best for most teams is self-describing files everywhere, on the grounds that the marginal cost is a few kilobytes per file and the marginal benefit is that no future reader has to guess. Where a governed catalog genuinely is the only access path — a closed platform, a single engine, strong controls on direct object access — the plain binary column is defensible and slightly cheaper; just write down which regime the table is in.

Whichever regime applies, the derived bounding-box columns are not part of the trade-off and should be present either way. They are what makes location a first-class predicate for the query planner, they are independent of how the geometry itself is encoded, and their cost is small enough to be uninteresting next to the scan they eliminate.

Record the choice as a table property so a reader never has to infer it from the bytes, and revisit it only when the access pattern changes rather than when a new pipeline is written.
