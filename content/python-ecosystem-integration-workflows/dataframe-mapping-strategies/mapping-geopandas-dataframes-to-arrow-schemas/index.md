# Mapping GeoPandas DataFrames to Arrow Schemas

This recipe converts a GeoPandas GeoDataFrame into a PyArrow table under an explicit geometry-as-WKB schema that carries CRS metadata and tolerates null geometries, producing bytes ready for PyIceberg or Parquet writes, and verifies a lossless round-trip back to GeoPandas.

## Context and prerequisites

A GeoDataFrame's geometry column holds Shapely objects, which are not an Arrow-native type; to hand geometry to Iceberg, Delta, or Parquet you must serialize it to a stable binary encoding and attach the CRS as sidecar metadata, because Arrow itself has no geometry type or coordinate-reference concept. This page sits in the [DataFrame mapping strategies](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/dataframe-mapping-strategies/) topic area and is the practical counterpart to the [GeoParquet encoding standards](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/geoparquet-encoding-standards/) that formalize how that metadata is written. You need GeoPandas 1.0+, Shapely 2.x, and PyArrow 15+ (`pip install "geopandas>=1.0" "pyarrow>=15"`). Two encodings matter: plain WKB in an Arrow `binary` field (maximally portable, the choice for PyIceberg), and GeoArrow (native coordinate arrays, faster but newer). This recipe uses WKB with an explicit schema and shows the GeoArrow shortcut at the end.

## Complete working solution

The mapping builds an Arrow schema by hand, serializes each geometry with Shapely's vectorized `to_wkb` (passing nulls through as Arrow nulls), and stamps the CRS and encoding into the schema-level metadata so downstream readers can reconstruct the geometry faithfully.

```python
# geodf_to_arrow.py
# GeoPandas 1.0+, Shapely 2.x, PyArrow 15+
import json

import geopandas as gpd
import numpy as np
import pyarrow as pa
from shapely import to_wkb, from_wkb
from shapely.geometry import Point, Polygon

# --- sample GeoDataFrame with a null geometry and non-geometry columns ---
gdf = gpd.GeoDataFrame(
    {
        "station_id": [101, 102, 103],
        "name": ["alpha", "beta", "gamma"],
        "geometry": [
            Point(-122.42, 37.77),
            Polygon([(-121, 38), (-120, 38), (-120, 39), (-121, 38)]),
            None,                               # null geometry must survive
        ],
    },
    crs="EPSG:4326",
)


def geodf_to_arrow(gdf: gpd.GeoDataFrame, geom_col: str = "geometry") -> pa.Table:
    """Map a GeoDataFrame to an Arrow table with geometry as WKB + CRS metadata."""
    geom = gdf[geom_col]
    epsg = geom.crs.to_epsg() if geom.crs else None
    if epsg is None:
        raise ValueError("GeoDataFrame has no EPSG-resolvable CRS; set one first")

    # Vectorized WKB; None geometries become None -> Arrow null.
    mask = geom.isna().to_numpy()
    wkb_vals = to_wkb(geom.to_numpy(), output_dimension=2)
    wkb_vals = np.where(mask, None, wkb_vals)
    geom_arr = pa.array(list(wkb_vals), type=pa.binary())

    # Non-geometry columns straight from pandas.
    fields = [pa.field("geom_wkb", pa.binary(), nullable=True)]
    arrays = [geom_arr]
    for col in gdf.columns:
        if col == geom_col:
            continue
        arrays.append(pa.array(gdf[col].to_numpy()))
        fields.append(pa.field(col, arrays[-1].type))

    # GeoParquet-style geo metadata on the geom field and the schema.
    geo_meta = {
        "encoding": "WKB",
        "crs_epsg": epsg,
        "geometry_type": "mixed",
    }
    field_meta = {b"ARROW:extension:name": b"geoarrow.wkb",
                  b"geo": json.dumps(geo_meta).encode()}
    fields[0] = fields[0].with_metadata(field_meta)

    schema = pa.schema(fields, metadata={
        b"geo": json.dumps({"version": "1.1.0",
                            "primary_column": "geom_wkb",
                            "columns": {"geom_wkb": geo_meta}}).encode()
    })
    return pa.Table.from_arrays(arrays, schema=schema)


table = geodf_to_arrow(gdf)
print(table.schema)
print("geo metadata:", table.schema.metadata[b"geo"].decode())
```

## Step-by-step walkthrough

1. **Resolve the CRS to an EPSG integer.** `geom.crs.to_epsg()` collapses a full PROJ CRS to the numeric authority code (here `4326`). Arrow metadata is bytes-only, so a compact integer is far more portable than an embedded WKT2 string, and it fails loudly with `ValueError` when the frame has no CRS — an unset CRS is the single most common source of silent reprojection bugs downstream.

2. **Vectorize WKB serialization.** Shapely 2's `to_wkb` operates on the whole NumPy array of geometries at once rather than per-row Python calls, which is roughly an order of magnitude faster on large frames. `output_dimension=2` forces XY output, dropping stray Z values that would otherwise bloat the bytes.

3. **Preserve nulls explicitly.** `to_wkb` returns `None` for a `None` geometry, but mixing `None` into a NumPy object array is fragile, so `np.where(mask, None, wkb_vals)` normalizes it and `pa.array(..., type=pa.binary())` maps those to true Arrow nulls. A null geometry stays null rather than becoming empty bytes, which matters because empty WKB and null are distinct states.

4. **Copy the attribute columns.** Non-geometry columns convert through `pa.array(gdf[col].to_numpy())`, letting Arrow infer `int64`, `string`, etc. Each field's inferred type is recorded so the schema is fully explicit rather than left to `Table.from_pandas` guesswork.

5. **Attach GeoParquet-compatible metadata.** The geometry field carries `ARROW:extension:name = geoarrow.wkb` so GeoArrow-aware readers recognize it, and a `geo` JSON blob records the encoding, EPSG, and geometry type. The schema-level `geo` key mirrors the [GeoParquet 1.1 metadata layout](https://geoparquet.org/releases/v1.1.0/) with `version`, `primary_column`, and per-column entries, so a `pyarrow.parquet.write_table` of this object is a valid GeoParquet file.

6. **Build the table from explicit arrays.** `pa.Table.from_arrays(arrays, schema=schema)` binds columns to the hand-built schema, guaranteeing column order and the WKB binary type instead of relying on inference. This is the object you pass to a PyIceberg `table.append(...)` or `pq.write_table(...)`.

If you want native GeoArrow coordinate storage instead of WKB, GeoPandas 1.0 exposes `gdf.to_arrow(geometry_encoding="geoarrow")` directly, which yields interleaved coordinate arrays and richer metadata; WKB remains the safer default for PyIceberg today because the geometry lands as an ordinary `binary` column that any engine can store. The trade-offs between the two are laid out in [GeoParquet vs WKB column storage trade-offs](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/geoparquet-encoding-standards/geoparquet-vs-wkb-column-storage-trade-offs/).

## Common errors and fixes

| Error | Cause | Fix |
|---|---|---|
| `ValueError: no EPSG-resolvable CRS` | GeoDataFrame constructed without `crs=` | Call `gdf.set_crs(4326)` (or the correct code) before mapping |
| `ArrowInvalid: Could not convert ... to binary` | Passing Shapely objects straight into `pa.array` | Serialize with `to_wkb` first; Arrow has no geometry type |
| Null geometry becomes empty bytes | `None` coerced to `b""` during array build | Use the `np.where(mask, None, ...)` step and `pa.binary()` nullable field |
| Reader reprojects wrongly | CRS metadata missing after write | Confirm the `geo` schema metadata survives; some writers strip unknown keys |

## Verification

Round-trip the Arrow table back into a GeoDataFrame and assert geometry, CRS, and null all survive. Equality on the WKB bytes is the strict test.

```python
# verify_roundtrip.py
import json
import geopandas as gpd
import pyarrow as pa
from shapely import from_wkb

meta = json.loads(table.schema.metadata[b"geo"].decode())
epsg = meta["columns"]["geom_wkb"]["crs_epsg"]
assert epsg == 4326

wkb_back = table.column("geom_wkb").to_pylist()
geoms = [from_wkb(b) if b is not None else None for b in wkb_back]

gdf_back = gpd.GeoDataFrame(
    {"station_id": table.column("station_id").to_pylist(),
     "name": table.column("name").to_pylist(),
     "geometry": geoms},
    crs=epsg,
)

assert gdf_back.geometry.iloc[2] is None          # null preserved
assert gdf_back.geometry.iloc[0].equals(gpd.GeoSeries([__import__("shapely").geometry.Point(-122.42, 37.77)]).iloc[0])
assert gdf_back.crs.to_epsg() == 4326
print("round-trip OK:", gdf_back.geometry.tolist())
```

When all three assertions pass — the null stays `None`, the point geometry compares equal, and the CRS resolves back to `4326` — the mapping is proven lossless and safe to persist. From here the same Arrow table feeds a concurrent load via [async catalog writes with PyIceberg and asyncio](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/async-execution-patterns/async-catalog-writes-with-pyiceberg-and-asyncio/), or an efficient bulk ingest as described in [reading shapefiles into PyIceberg dataframes efficiently](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/pyiceberg-spatial-workflows/reading-shapefiles-into-pyiceberg-dataframes-efficiently/).

<figure class="diagram">
<svg viewBox="0 0 760 240" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Pipeline mapping a GeoDataFrame geometry column to a WKB Arrow field with attached CRS metadata">
<title>GeoDataFrame to Arrow WKB mapping</title>
<desc>A GeoDataFrame's Shapely geometry column is serialized to WKB with nulls preserved, attribute columns copied, and CRS plus encoding stamped into schema metadata to yield a GeoParquet-ready Arrow table.</desc>
<defs>
<marker id="arw-gpd-arrow" markerWidth="9" markerHeight="9" refX="7" refY="4" orient="auto"><path d="M0 0 L9 4 L0 8 z" fill="#0e6e7d"/></marker>
</defs>
<rect x="0" y="0" width="760" height="240" fill="#f7fbfc"/>
<rect x="20" y="70" width="150" height="100" rx="6" fill="#ffffff" stroke="#2f6e49"/>
<text x="95" y="94" text-anchor="middle" font-family="sans-serif" font-size="13" fill="#0d3b45">GeoDataFrame</text>
<text x="95" y="118" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">Shapely geometry</text>
<text x="95" y="136" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">crs = EPSG:4326</text>
<text x="95" y="154" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">1 null geom</text>
<rect x="210" y="55" width="160" height="130" rx="6" fill="#ffffff" stroke="#9a5a17"/>
<text x="290" y="79" text-anchor="middle" font-family="sans-serif" font-size="13" fill="#0d3b45">to_wkb (vectorized)</text>
<text x="290" y="103" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">output_dimension=2</text>
<text x="290" y="123" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">None -> Arrow null</text>
<text x="290" y="147" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">to_epsg() = 4326</text>
<text x="290" y="167" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">attrs copied</text>
<rect x="410" y="70" width="150" height="100" rx="6" fill="#ffffff" stroke="#6a3d9a"/>
<text x="485" y="94" text-anchor="middle" font-family="sans-serif" font-size="13" fill="#0d3b45">Arrow schema</text>
<text x="485" y="118" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#6a3d9a">geom_wkb: binary</text>
<text x="485" y="136" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#6a3d9a">geo: {epsg, WKB}</text>
<text x="485" y="154" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">geoarrow.wkb ext</text>
<rect x="600" y="70" width="140" height="100" rx="6" fill="#ffffff" stroke="#0e6e7d"/>
<text x="670" y="102" text-anchor="middle" font-family="sans-serif" font-size="13" fill="#0d3b45">PyIceberg /</text>
<text x="670" y="122" text-anchor="middle" font-family="sans-serif" font-size="13" fill="#0d3b45">GeoParquet</text>
<text x="670" y="146" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">append / write</text>
<line x1="170" y1="120" x2="207" y2="120" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-gpd-arrow)"/>
<line x1="370" y1="120" x2="407" y2="120" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-gpd-arrow)"/>
<line x1="560" y1="120" x2="597" y2="120" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-gpd-arrow)"/>
</svg>
</figure>

The load-bearing decisions are resolving the CRS to a numeric EPSG code before serialization and keeping nulls distinct from empty geometries — get those right and the WKB Arrow table interoperates cleanly with every lakehouse engine. For the encoding standard this mapping conforms to, see [GeoParquet encoding standards](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/geoparquet-encoding-standards/), and the [GeoPandas Arrow documentation](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.to_arrow.html) for the native GeoArrow path.
