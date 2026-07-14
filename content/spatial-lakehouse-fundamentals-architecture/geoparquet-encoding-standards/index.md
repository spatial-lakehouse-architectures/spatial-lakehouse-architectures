# GeoParquet Encoding Standards for Lakehouse Tables

GeoParquet is the convention that turns an ordinary Parquet file into a self-describing spatial dataset by attaching a `"geo"` key to the file's key-value metadata. That single metadata block — not a sidecar `.prj`, not an out-of-band catalog entry — is what lets DuckDB, GDAL, GeoPandas, Sedona, and Trino agree on which column holds geometry, how it is serialized, and what coordinate reference system it lives in. This guide covers the GeoParquet 1.1 specification as it applies to open-table lakehouse storage: the structure of the `geo` metadata, WKB column encoding, the `primary_column`/`columns`/`bbox`/`covering` fields, PROJJSON CRS storage, the native Parquet `GEOMETRY` logical type, and how engines actually read all of it at scan time. It is written for data engineers and platform architects who are standardizing how geometry lands in an Iceberg or Delta table and want the on-disk contract to be unambiguous across every reader.

## When to use file-level GeoParquet metadata

The core decision is whether to lean on GeoParquet's file-level `geo` metadata or to treat geometry as a plain WKB `BINARY` column with the spatial contract enforced entirely in your pipeline. The metadata approach buys interoperability and self-description; the raw-column approach buys engine-agnostic simplicity at the cost of every reader needing out-of-band knowledge of the CRS and encoding. The table below frames the choice; the dedicated [GeoParquet vs WKB column storage trade-offs](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/geoparquet-encoding-standards/geoparquet-vs-wkb-column-storage-trade-offs/) guide runs the full comparison with runnable code for both.

| Requirement | GeoParquet file metadata | Raw WKB `BINARY` column |
|---|---|---|
| Cross-tool interoperability (GDAL, DuckDB, QGIS) | Native — CRS + encoding travel with the file | Manual — every reader needs external CRS knowledge |
| CRS captured in the file | Yes, as PROJJSON in `geo.columns.<name>.crs` | No — stored elsewhere or lost |
| File-skipping bbox statistics | Yes, via the `covering` bbox struct | Only if you hand-roll bbox columns |
| Works with engines that ignore `geo` | Degrades to a binary column | Always |
| Round-trips through Iceberg/Delta metadata | Partial — table formats may drop file KV metadata | Full — it is just a column |

Reach for file-level metadata when the same physical files are read by heterogeneous tools or handed to external consumers. Reach for raw WKB columns, plus explicit bounding-box columns, when the files live behind an [Iceberg spatial table](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/iceberg-spatial-type-support/) whose catalog is the single source of truth and whose manifest statistics already drive [predicate pushdown](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/predicate-pushdown-optimization/).

## Architecture: what lives inside a GeoParquet file

<figure class="diagram">
<svg viewBox="0 0 760 300" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Layout of a GeoParquet file showing row groups, per-column bbox covering, and the file-level geo metadata block read by engines">
<title>GeoParquet 1.1 file layout</title>
<desc>A Parquet file with two row groups, each holding a WKB geometry column and a bbox covering struct, and a file footer whose key-value metadata carries the geo block with version, primary_column, columns, CRS as PROJJSON, and covering. Engines read the footer first.</desc>
<defs>
<marker id="arw-geopq" markerWidth="9" markerHeight="9" refX="7" refY="4.5" orient="auto"><path d="M0,0 L9,4.5 L0,9 z" fill="#0e6e7d"/></marker>
</defs>
<rect x="0" y="0" width="760" height="300" fill="#f7fbfc"/>
<rect x="20" y="30" width="330" height="240" rx="6" fill="#ffffff" stroke="#cfe3e7"/>
<text x="185" y="52" text-anchor="middle" font-family="sans-serif" font-size="14" font-weight="bold" fill="#0d3b45">Parquet data (.parquet)</text>
<rect x="38" y="66" width="294" height="86" rx="4" fill="#f7fbfc" stroke="#cfe3e7"/>
<text x="185" y="84" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="bold" fill="#0e6e7d">Row group 0</text>
<rect x="50" y="94" width="130" height="46" rx="3" fill="#ffffff" stroke="#0e6e7d"/>
<text x="115" y="113" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">geometry</text>
<text x="115" y="130" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">WKB BINARY</text>
<rect x="192" y="94" width="128" height="46" rx="3" fill="#ffffff" stroke="#2f6e49"/>
<text x="256" y="113" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">bbox struct</text>
<text x="256" y="130" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">xmin..ymax</text>
<rect x="38" y="160" width="294" height="86" rx="4" fill="#f7fbfc" stroke="#cfe3e7"/>
<text x="185" y="178" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="bold" fill="#0e6e7d">Row group 1</text>
<rect x="50" y="188" width="130" height="46" rx="3" fill="#ffffff" stroke="#0e6e7d"/>
<text x="115" y="207" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">geometry</text>
<text x="115" y="224" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">WKB BINARY</text>
<rect x="192" y="188" width="128" height="46" rx="3" fill="#ffffff" stroke="#2f6e49"/>
<text x="256" y="207" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">bbox struct</text>
<text x="256" y="224" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">xmin..ymax</text>
<rect x="410" y="30" width="330" height="240" rx="6" fill="#ffffff" stroke="#cfe3e7"/>
<text x="575" y="52" text-anchor="middle" font-family="sans-serif" font-size="14" font-weight="bold" fill="#0d3b45">Footer key-value: "geo"</text>
<rect x="428" y="66" width="294" height="30" rx="3" fill="#f7fbfc" stroke="#cfe3e7"/>
<text x="440" y="86" font-family="sans-serif" font-size="11.5" fill="#0d3b45">version: "1.1.0"</text>
<rect x="428" y="102" width="294" height="30" rx="3" fill="#f7fbfc" stroke="#cfe3e7"/>
<text x="440" y="122" font-family="sans-serif" font-size="11.5" fill="#0d3b45">primary_column: "geometry"</text>
<rect x="428" y="138" width="294" height="46" rx="3" fill="#f7fbfc" stroke="#6a3d9a"/>
<text x="440" y="157" font-family="sans-serif" font-size="11.5" fill="#0d3b45">columns.geometry.encoding: WKB</text>
<text x="440" y="176" font-family="sans-serif" font-size="11.5" fill="#6a3d9a">columns.geometry.crs: PROJJSON</text>
<rect x="428" y="190" width="294" height="30" rx="3" fill="#f7fbfc" stroke="#9a5a17"/>
<text x="440" y="210" font-family="sans-serif" font-size="11.5" fill="#9a5a17">columns.geometry.covering.bbox</text>
<rect x="428" y="226" width="294" height="30" rx="3" fill="#f7fbfc" stroke="#cfe3e7"/>
<text x="440" y="246" font-family="sans-serif" font-size="11.5" fill="#0d3b45">columns.geometry.geometry_types[]</text>
<line x1="350" y1="118" x2="408" y2="118" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-geopq)"/>
<text x="379" y="110" text-anchor="middle" font-family="sans-serif" font-size="10.5" fill="#33707d">read first</text>
</svg>
</figure>

A GeoParquet file is a normal Parquet file plus a JSON document stored under the key `geo` in the file footer's key-value metadata. Every compliant reader opens the footer, parses that JSON, and only then knows how to interpret the geometry column. The document has a `version`, a `primary_column` naming the default geometry column, and a `columns` map. Each entry in `columns` describes one geometry column: its `encoding` (`WKB` in 1.0, or `point`/`linestring`/other native GeoArrow encodings added in 1.1), its `geometry_types` array, its `crs` as a PROJJSON object, and — the headline 1.1 feature — an optional `covering` field that points at a per-row bounding-box struct column engines can use for file and row-group skipping.

## Prerequisites and environment setup

The reference stack for authoring and inspecting GeoParquet is GeoPandas on top of pyarrow and Shapely. GeoPandas 1.0 writes GeoParquet 1.1 including the `covering` bbox by default when you pass `write_covering_bbox=True`; pyarrow gives you raw access to the footer metadata for verification.

```bash
python -m pip install \
  "geopandas>=1.0.1" \
  "pyarrow>=17.0.0" \
  "shapely>=2.0.4" \
  "pyproj>=3.6.1"
```

```python
import geopandas
import pyarrow
import pyproj

# Confirm the toolchain versions that support GeoParquet 1.1 + covering bbox.
print("geopandas", geopandas.__version__)
print("pyarrow", pyarrow.__version__)
print("pyproj", pyproj.__version__)
assert tuple(int(p) for p in geopandas.__version__.split(".")[:2]) >= (1, 0)
```

## Step-by-step implementation

### Step 1 — Build a GeoDataFrame with an explicit CRS

Never let the CRS default. Set it explicitly with the numeric EPSG code so the written PROJJSON is deterministic. Silent CRS assumptions are the root cause of most downstream join failures; the [CRS management pipelines](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/crs-management-pipelines/) section covers enforcing this at ingest, and [detecting CRS drift in ingestion pipelines](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/crs-management-pipelines/detecting-crs-drift-in-ingestion-pipelines/) covers catching it after the fact.

```python
import geopandas
from shapely.geometry import Point, Polygon

gdf = geopandas.GeoDataFrame(
    {
        "feature_id": ["sensor-001", "sensor-002", "zone-a"],
        "kind": ["point", "point", "polygon"],
    },
    geometry=[
        Point(-122.4194, 37.7749),
        Point(-122.2712, 37.8044),
        Polygon([(-122.52, 37.70), (-122.35, 37.70),
                 (-122.35, 37.83), (-122.52, 37.83)]),
    ],
    crs=4326,  # numeric EPSG literal -> written as PROJJSON in the geo metadata
)
print(gdf.crs.to_epsg())  # 4326
```

### Step 2 — Write GeoParquet 1.1 with a bbox covering

The `write_covering_bbox=True` flag materializes a `bbox` struct column (`xmin`, `ymin`, `xmax`, `ymax`) per row and records a `covering.bbox` entry in the `geo` metadata pointing at it. Combined with row-group statistics, this is what lets a reader skip row groups whose bounds miss the query window.

```python
gdf.to_parquet(
    "sensors.parquet",
    engine="pyarrow",
    schema_version="1.1.0",       # emit GeoParquet 1.1 metadata
    write_covering_bbox=True,     # add per-row bbox struct + covering entry
    geometry_encoding="WKB",      # WKB is the maximally interoperable encoding
    compression="zstd",
)
```

### Step 3 — Inspect the `geo` footer metadata

Read the footer key-value metadata straight from pyarrow to confirm exactly what was written. This is the same block a CI gate should assert on, as detailed in [validating GeoParquet metadata in CI](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/geoparquet-encoding-standards/validating-geoparquet-metadata-in-ci/).

```python
import json
import pyarrow.parquet as pq

pf = pq.ParquetFile("sensors.parquet")
kv = pf.metadata.metadata  # dict of bytes -> bytes
geo = json.loads(kv[b"geo"].decode("utf-8"))

print("version:", geo["version"])
print("primary_column:", geo["primary_column"])
col = geo["columns"][geo["primary_column"]]
print("encoding:", col["encoding"])
print("geometry_types:", col["geometry_types"])
print("covering:", col.get("covering"))
print("crs id:", col["crs"]["id"])  # {'authority': 'EPSG', 'code': 4326}
```

### Step 4 — Understand the native Parquet GEOMETRY logical type

GeoParquet's `geo` block is a convention layered on top of a `BINARY` column. Separately, the Parquet format itself is gaining a native `GEOMETRY` (and `GEOGRAPHY`) logical type that annotates the column in the Parquet schema, carrying the CRS and edge interpolation as part of the type rather than as file metadata. The two are complementary: a writer can emit a column whose Parquet logical type is `GEOMETRY` and still publish a `geo` block for readers that key off it. When your engine and file writer both support the native logical type, prefer it — statistics become first-class and CRS travels in the schema. Until the ecosystem catches up, the `geo` metadata plus WKB remains the portable baseline, which is why GeoPandas still defaults to WKB.

## Verification and testing

Verify three things: the geometry survived a round-trip, the covering bbox is queryable, and the CRS is intact. DuckDB (spatial extension ≥ 1.0) reads GeoParquet natively and exposes the bbox for pushdown.

```python
import geopandas

# Round-trip: geometry and CRS preserved.
back = geopandas.read_parquet("sensors.parquet")
assert back.crs.to_epsg() == 4326
assert back.geometry.geom_type.tolist() == ["Point", "Point", "Polygon"]
print("total_bounds:", back.total_bounds)  # [xmin ymin xmax ymax]
```

```sql
-- DuckDB (spatial >= 1.0): confirm the covering bbox drives file skipping.
INSTALL spatial; LOAD spatial;
SELECT count(*)
FROM read_parquet('sensors.parquet')
WHERE bbox.xmin < -122.30 AND bbox.xmax > -122.55
  AND bbox.ymin <  37.85 AND bbox.ymax >  37.68;
```

A correct write yields a `bbox` struct column visible in the Parquet schema, `covering.bbox` populated in the `geo` metadata, and a `crs.id` of `{"authority": "EPSG", "code": 4326}`. If any is missing, treat the file as non-conforming.

## Performance and tuning

The covering bbox only helps if geometries are spatially clustered within row groups; a randomly ordered file has row-group bounds that all overlap the query window, defeating skipping. Sort by a space-filling key before writing so nearby features share row groups — the same clustering discipline that [predicate pushdown optimization](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/predicate-pushdown-optimization/) depends on.

- **Row-group size**: keep the Parquet row group at 64–128 MB (`row_group_size` measured in rows for your average geometry width). Smaller groups give finer skipping granularity but inflate footer metadata; larger groups coarsen the bbox and read amplification climbs.
- **Compression**: `zstd` at level 3 typically beats `snappy` by 20–35% on WKB polygon columns with negligible decode cost. WKB compresses poorly compared to coordinate-delta encodings, so this matters.
- **Encoding choice**: WKB maximizes interoperability; GeoParquet 1.1 native GeoArrow encodings (`point`, etc.) can be 2–4x smaller and faster to decode for homogeneous point datasets but are read by fewer engines.
- **Covering overhead**: the bbox struct adds four `DOUBLE` columns — roughly 32 bytes/row before compression. On point-heavy tables that can be 10–15% of file size, so drop it only if no reader does spatial range filtering.

## Common errors and fixes

| Symptom | Root cause | Fix |
|---|---|---|
| Reader sees geometry as opaque `BINARY`, no spatial ops | `geo` metadata absent or `primary_column` unset | Rewrite with GeoPandas/`to_parquet(schema_version="1.1.0")`; assert `b"geo"` in footer KV |
| `crs.id` missing or CRS reads as null | GeoDataFrame written without a CRS set | Set `crs=4326` before write; never rely on the default |
| No row-group skipping despite a bbox filter | `write_covering_bbox` omitted or data unsorted | Write with `write_covering_bbox=True` and sort by a geohash/Hilbert key first |
| Trino/Sedona ignores the `covering` bbox | Engine reads 1.0 metadata only | Keep explicit `DOUBLE` bbox columns as a fallback; verify engine GeoParquet 1.1 support |
| Table-format read drops the `geo` block | Iceberg/Delta may not surface file KV metadata to the engine | Store CRS/encoding in table properties too; treat catalog as source of truth |

Standardizing GeoParquet encoding is the file-level half of a spatial lakehouse contract; the catalog-level half lives in [Iceberg spatial type support](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/iceberg-spatial-type-support/) and the broader [Spatial Lakehouse Fundamentals & Architecture](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/) section. Anchor every implementation to the canonical [GeoParquet specification](https://geoparquet.org/) and the [OGC Simple Features](https://www.ogc.org/standard/sfa/) coordinate model so the metadata you write stays portable across the entire tool ecosystem.
