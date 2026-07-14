# Selecting a Discrete Global Grid for Lakehouse Partitioning

Choosing the discrete global grid that becomes your partition key is the single highest-leverage decision in a spatial lakehouse layout: it fixes cell shape, neighbor semantics, partition cardinality, and how cleanly the key maps onto Iceberg and Delta partition transforms. This topic area compares the three grids that dominate production data engineering — H3 (hexagonal), S2 (quadrilateral cells ordered along a Hilbert curve), and geohash (base-32 rectangles) — and gives you a defensible way to pick one and size its resolution. It sits inside the broader [Spatial Partitioning & Indexing Strategies](/spatial-partitioning-indexing-strategies/) section and assumes you have already ruled out naive range or hash partitioning on raw coordinates because of skew.

## When to use a discrete global grid

A discrete global grid (DGG) earns its place when your access pattern is "give me everything near here" and your storage engine prunes on an equality or set predicate over a partition column. If your queries are pure bounding-box range scans, an in-file space-filling-curve layout via [Z-ordering for geospatial queries](/spatial-partitioning-indexing-strategies/z-ordering-for-geospatial-queries/) may serve you better than a grid partition. Use the table below to decide.

| Signal in your workload | Favors | Why |
|---|---|---|
| KNN, hexagon aggregation, "within N rings" | **H3** | Uniform 6-neighbor adjacency, equal-ish cell area |
| Spherical accuracy, cell-range covering, Google-stack interop | **S2** | True spherical cells, `S2CellUnion` range covers |
| Human-readable prefixes, minimal dependencies, cheap ingest | **Geohash** | Base-32 string, prefix = coarser cell, pure-Python encode |
| Equality lookups on a single partition column | any DGG | Grid id is a clean partition/bucket key |
| Only axis-aligned bbox range scans | none — use Z-order | Grid adds cardinality without pruning benefit |

The rest of this guide treats the grid id as a first-class partition column. For a wider comparison of grid schemes against space-filling curves and tree indexes, see [spatial partitioning schemes](/spatial-partitioning-indexing-strategies/spatial-partitioning-schemes/).

## How the three grids differ

The three systems disagree on four properties that matter for partitioning: cell shape, area distortion across latitude, the parent/child hierarchy, and neighbor traversal.

**H3** tiles the sphere with hexagons (plus 12 pentagons at icosahedron vertices). Every hexagon has exactly six equidistant neighbors, which makes ring queries and flow modeling clean. Resolutions run 0–15; each finer resolution divides a cell into 7 children (aperture-7), so the hierarchy is *not* strictly containing — a child hexagon can straddle a parent boundary. Cell ids are 64-bit integers, conventionally handled as 15-character hex strings such as `872830828ffffff`.

**S2** projects the sphere onto the six faces of a cube and recursively subdivides each face into four quadrilateral children, ordering the cells along a Hilbert space-filling curve. Levels run 0–30. Because subdivision is aperture-4 and strictly containing, an S2 cell id encodes its full ancestry: truncating the 64-bit id gives you the parent, and a contiguous id range covers a contiguous region. That range property is what makes S2 attractive for covering an arbitrary polygon with a compact set of cell ranges.

**Geohash** interleaves latitude and longitude bits and base-32 encodes them into strings where each added character refines the cell and every prefix is a valid coarser cell. Cells are lat/lon rectangles in EPSG:4326, so they distort badly toward the poles and are non-square at most latitudes. Geohash has no native hexagonal neighborhood; adjacency requires computing the eight bordering rectangles, and neighbors across a base-32 "seam" share no common prefix — the classic geohash edge problem.

<figure class="diagram">
<svg viewBox="0 0 760 300" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three discrete global grids covering the same map extent, each feeding a partition column in a lakehouse table">
<title>H3, S2 and geohash over the same area, mapped to a partition column</title>
<desc>Left: the same rectangular map extent tiled three ways — hexagons for H3, Hilbert-ordered quadrilaterals for S2, and axis-aligned base-32 rectangles for geohash. Right: each grid encodes a point into a cell id that becomes the table partition key.</desc>
<rect x="0" y="0" width="760" height="300" fill="#f7fbfc"/>
<text x="380" y="24" text-anchor="middle" font-family="sans-serif" font-size="15" fill="#0d3b45" font-weight="bold">One extent, three grids, one partition column</text>
<!-- H3 panel -->
<rect x="20" y="44" width="150" height="150" fill="#ffffff" stroke="#cfe3e7"/>
<text x="95" y="62" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0e6e7d" font-weight="bold">H3 hexagons</text>
<polygon points="60,88 78,88 87,103 78,118 60,118 51,103" fill="#e4f0f2" stroke="#0e6e7d"/>
<polygon points="96,88 114,88 123,103 114,118 96,118 87,103" fill="#e4f0f2" stroke="#0e6e7d"/>
<polygon points="78,118 96,118 105,133 96,148 78,148 69,133" fill="#cfe3e7" stroke="#0e6e7d"/>
<polygon points="114,118 132,118 141,133 132,148 114,148 105,133" fill="#e4f0f2" stroke="#0e6e7d"/>
<polygon points="60,148 78,148 87,163 78,178 60,178 51,163" fill="#e4f0f2" stroke="#0e6e7d"/>
<text x="95" y="137" text-anchor="middle" font-family="sans-serif" font-size="10" fill="#0d3b45">6 neighbors</text>
<!-- S2 panel -->
<rect x="190" y="44" width="150" height="150" fill="#ffffff" stroke="#cfe3e7"/>
<text x="265" y="62" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#2f6e49" font-weight="bold">S2 Hilbert cells</text>
<rect x="215" y="90" width="30" height="30" fill="#e6f0ea" stroke="#2f6e49"/>
<rect x="245" y="90" width="30" height="30" fill="#e6f0ea" stroke="#2f6e49"/>
<rect x="215" y="120" width="30" height="30" fill="#d7e8de" stroke="#2f6e49"/>
<rect x="245" y="120" width="30" height="30" fill="#e6f0ea" stroke="#2f6e49"/>
<rect x="275" y="90" width="30" height="60" fill="#e6f0ea" stroke="#2f6e49"/>
<path d="M230,105 L230,135 L260,135 L260,105 L290,120" fill="none" stroke="#9a5a17" stroke-width="1.5"/>
<text x="265" y="168" text-anchor="middle" font-family="sans-serif" font-size="10" fill="#0d3b45">contiguous id range</text>
<!-- Geohash panel -->
<rect x="360" y="44" width="150" height="150" fill="#ffffff" stroke="#cfe3e7"/>
<text x="435" y="62" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#9a5a17" font-weight="bold">Geohash rects</text>
<rect x="385" y="90" width="34" height="24" fill="#f2e8da" stroke="#9a5a17"/>
<rect x="419" y="90" width="34" height="24" fill="#f2e8da" stroke="#9a5a17"/>
<rect x="453" y="90" width="34" height="24" fill="#f2e8da" stroke="#9a5a17"/>
<rect x="385" y="114" width="34" height="24" fill="#eaddc8" stroke="#9a5a17"/>
<rect x="419" y="114" width="34" height="24" fill="#f2e8da" stroke="#9a5a17"/>
<rect x="453" y="114" width="34" height="24" fill="#f2e8da" stroke="#9a5a17"/>
<rect x="385" y="138" width="34" height="24" fill="#f2e8da" stroke="#9a5a17"/>
<text x="435" y="164" text-anchor="middle" font-family="sans-serif" font-size="10" fill="#0d3b45">prefix = parent</text>
<!-- mapping to partition column -->
<line x1="510" y1="119" x2="560" y2="119" stroke="#33707d" stroke-width="2" marker-end="url(#arw-grid-select)"/>
<rect x="560" y="80" width="180" height="120" fill="#ffffff" stroke="#0e6e7d" stroke-width="1.5"/>
<text x="650" y="100" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45" font-weight="bold">Iceberg / Delta table</text>
<text x="650" y="122" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">PARTITIONED BY</text>
<text x="650" y="142" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">bucket(N, grid_id)</text>
<text x="650" y="162" text-anchor="middle" font-family="sans-serif" font-size="10" fill="#0d3b45">grid_id = cell key</text>
<text x="650" y="182" text-anchor="middle" font-family="sans-serif" font-size="10" fill="#0d3b45">pruned on equality</text>
<defs>
<marker id="arw-grid-select" markerWidth="10" markerHeight="10" refX="8" refY="3" orient="auto"><path d="M0,0 L8,3 L0,6 Z" fill="#33707d"/></marker>
</defs>
</svg>
</figure>

## Prerequisites and environment setup

Install the three encoders plus a Spark/Iceberg or Delta runtime. The `h3` package is h3-py 4.x (module-level functions), `s2sphere` is a pure-Python S2 implementation, and `python-geohash` provides `geohash.encode`.

```bash
python -m pip install "h3>=4.1,<5" "s2sphere>=0.2.5" "python-geohash>=0.8.5" \
    "pyspark==3.5.1" "delta-spark==3.2.0"
```

For Iceberg, use the Spark 3.5 runtime with the Iceberg 1.9.0 jars and format-version 2 tables. The grid encoders run on the driver during UDF registration but execute per-row on executors, so every worker image must contain them.

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("grid-system-selection")
    .config("spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.0")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lake.type", "hadoop")
    .config("spark.sql.catalog.lake.warehouse", "s3://lakehouse/warehouse")
    .getOrCreate()
)
```

## Step-by-step implementation

### 1. Encode the point to all three cell keys

Register one UDF per grid so you can compare cardinality on real data before committing. All three take EPSG:4326 lat/lon and return a string key.

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import h3
import s2sphere
import geohash  # python-geohash

@udf(StringType())
def h3_key(lat: float, lon: float, res: int = 7) -> str:
    # h3-py 4.x: module-level latlng_to_cell returns a 15-char hex id
    return h3.latlng_to_cell(lat, lon, res)

@udf(StringType())
def s2_key(lat: float, lon: float, level: int = 13) -> str:
    ll = s2sphere.LatLng.from_degrees(lat, lon)
    cell = s2sphere.CellId.from_lat_lng(ll).parent(level)
    return cell.to_token()  # compact hex token, e.g. '89c25a'

@udf(StringType())
def geohash_key(lat: float, lon: float, precision: int = 7) -> str:
    return geohash.encode(lat, lon, precision=precision)
```

The resolution defaults above are deliberately chosen to yield roughly comparable cell footprints: H3 res 7 (~5 km² average), S2 level 13 (~1.3 km² average), geohash precision 7 (~150 m × 150 m). Resolution sizing, not the grid choice alone, controls partition cardinality — see [choosing an H3 resolution for point data](/spatial-partitioning-indexing-strategies/grid-system-selection/choosing-h3-resolution-for-point-data/) for the sizing method.

### 2. Materialize the partition column

Add the chosen grid id as a real column so the engine can prune on it. Do this once at ingest; never compute the key inside a query predicate, because a UDF in the `WHERE` clause defeats partition pruning.

```python
from pyspark.sql.functions import col

raw = (
    spark.read.schema("device_id STRING, lat DOUBLE, lon DOUBLE, ts TIMESTAMP")
    .json("s3://raw-telemetry/2024-10/")
    .filter((col("lat").between(-90, 90)) & (col("lon").between(-180, 180)))
)

enriched = raw.withColumn("h3_res7", h3_key(col("lat"), col("lon")))
```

### 3. Map the key onto a partition transform

Both formats accept the string key directly. Wrapping it in a `bucket` transform caps directory/manifest count when cell cardinality is high — the recommended default for a global dataset.

```sql
-- Apache Iceberg (Spark SQL), format-version 2, Iceberg 1.9.0
CREATE TABLE lake.geo.events (
    device_id STRING,
    lat DOUBLE,
    lon DOUBLE,
    ts TIMESTAMP,
    h3_res7 STRING
) USING iceberg
PARTITIONED BY (bucket(256, h3_res7), days(ts))
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);
```

For Delta Lake, `partitionBy("h3_res7")` writes physical directories, so prefer a coarser resolution there; Iceberg's `bucket` transform is the safer high-cardinality choice. The concrete Delta directory-based flow is covered in [implementing H3 hexagon partitioning in Delta Lake](/spatial-partitioning-indexing-strategies/spatial-partitioning-schemes/implementing-h3-hexagon-partitioning-in-delta-lake/).

## Verification and testing

Confirm the key prunes partitions and check its cardinality before you scale ingest. Cardinality that climbs past ~50k live partitions per day is the leading cause of manifest bloat.

```python
enriched.createOrReplaceTempView("enriched")
spark.sql("""
    SELECT COUNT(DISTINCT h3_res7) AS cells,
           COUNT(*)                AS rows,
           ROUND(COUNT(*) / COUNT(DISTINCT h3_res7), 1) AS rows_per_cell
    FROM enriched
""").show()
```

Then validate that an equality predicate on the key produces `PartitionFilters` rather than a full scan:

```sql
EXPLAIN
SELECT * FROM lake.geo.events
WHERE h3_res7 = '872830828ffffff'
  AND ts >= DATE '2024-10-01';
```

A healthy plan lists the grid column under partition filters and shows a bucket/partition count far below the table total. If it does not, the predicate is being applied post-scan — see [predicate pushdown optimization](/spatial-partitioning-indexing-strategies/predicate-pushdown-optimization/) for the pushdown checklist.

## Performance and tuning

- **Resolution is the dominant knob.** Each H3 level multiplies cell count by ~7, each S2 level by 4, each geohash character by 32. Moving H3 res 7 → 8 roughly septuples partition cardinality. Size for 100 MB–1 GB of data per cell after compaction.
- **Bucket count.** With `bucket(N, grid_id)`, set `N` so that `rows_per_bucket × avg_row_bytes` lands in the 128 MB–1 GB target. For a 500 GB daily table, `N` between 256 and 1024 is typical.
- **Neighbor queries.** H3 `grid_disk(cell, k)` expands to a k-ring in one call; geohash needs `geohash.neighbors()` iterated per level and must handle base-32 seams; S2 uses `S2CellUnion` range covers. If your workload is ring-heavy, H3's constant 6-neighbor topology cuts query fan-out.
- **Hot partitions.** Dense urban cells hold orders of magnitude more rows than rural ones. The `bucket` transform spreads a hot cell across files by hashing; without it, a single H3 cell over a city center becomes a multi-gigabyte partition that stalls compaction. Pair coarse grid partitions with intra-file [Z-ordering](/spatial-partitioning-indexing-strategies/z-ordering-for-geospatial-queries/) on `lon, lat` to keep row groups locally clustered.
- **Interop cost.** Geohash strings are self-describing and need no library to decode a bounding box, which lowers cross-team friction; H3 and S2 ids are opaque without their libraries.

## Common errors and fixes

| Symptom | Root cause | Fix |
|---|---|---|
| Millions of tiny files, slow `LIST` | Grid resolution too fine for Delta directory partitioning | Coarsen resolution or move to Iceberg `bucket(N, grid_id)` |
| Query scans whole table despite grid filter | Grid id computed inside `WHERE` via UDF, or column not materialized | Persist the key as a column at ingest; filter on the literal id |
| One partition 100× larger than the rest | Hot urban cell, no hashing | Wrap key in `bucket()`; add Z-order on coordinates within the cell |
| Neighbor cells miss data at boundaries | Geohash base-32 seam or H3 pentagon distortion | Use `h3.grid_disk`; for geohash expand with `geohash.neighbors` on all 8 |
| S2/H3 ids differ between ingest and query | Mismatched library version or resolution/level constant | Pin `h3>=4.1`, `s2sphere>=0.2.5`; centralize the resolution constant |

Once you have chosen a grid, the two guides in this topic area go deeper: a full [head-to-head H3 vs S2 vs geohash comparison](/spatial-partitioning-indexing-strategies/grid-system-selection/h3-vs-s2-vs-geohash-for-lakehouse-partitioning/) with runnable cardinality benchmarks, and a data-driven method for [choosing an H3 resolution for point data](/spatial-partitioning-indexing-strategies/grid-system-selection/choosing-h3-resolution-for-point-data/). Authoritative references: the [H3 documentation](https://h3geo.org/docs/) and the [S2 Geometry library](https://s2geometry.io/).
