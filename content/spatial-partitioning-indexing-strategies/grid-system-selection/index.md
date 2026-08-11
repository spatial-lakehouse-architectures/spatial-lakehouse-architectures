# Selecting a Discrete Global Grid for Lakehouse Partitioning

Choosing the discrete global grid that becomes your partition key is the single highest-leverage decision in a spatial lakehouse layout: it fixes cell shape, neighbor semantics, partition cardinality, and how cleanly the key maps onto Iceberg and Delta partition transforms. This topic area compares the three grids that dominate production data engineering — H3 (hexagonal), S2 (quadrilateral cells ordered along a Hilbert curve), and geohash (base-32 rectangles) — and gives you a defensible way to pick one and size its resolution. It sits inside the broader [Spatial Partitioning & Indexing Strategies](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/) section and assumes you have already ruled out naive range or hash partitioning on raw coordinates because of skew.

## When to use a discrete global grid

A discrete global grid (DGG) earns its place when your access pattern is "give me everything near here" and your storage engine prunes on an equality or set predicate over a partition column. If your queries are pure bounding-box range scans, an in-file space-filling-curve layout via [Z-ordering for geospatial queries](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/z-ordering-for-geospatial-queries/) may serve you better than a grid partition. Use the table below to decide.

| Signal in your workload | Favors | Why |
|---|---|---|
| KNN, hexagon aggregation, "within N rings" | **H3** | Uniform 6-neighbor adjacency, equal-ish cell area |
| Spherical accuracy, cell-range covering, Google-stack interop | **S2** | True spherical cells, `S2CellUnion` range covers |
| Human-readable prefixes, minimal dependencies, cheap ingest | **Geohash** | Base-32 string, prefix = coarser cell, pure-Python encode |
| Equality lookups on a single partition column | any DGG | Grid id is a clean partition/bucket key |
| Only axis-aligned bbox range scans | none — use Z-order | Grid adds cardinality without pruning benefit |

The rest of this guide treats the grid id as a first-class partition column. For a wider comparison of grid schemes against space-filling curves and tree indexes, see [spatial partitioning schemes](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/spatial-partitioning-schemes/).

## How the three grids differ

The three systems disagree on four properties that matter for partitioning: cell shape, area distortion across latitude, the parent/child hierarchy, and neighbor traversal.

**H3** tiles the sphere with hexagons (plus 12 pentagons at icosahedron vertices). Every hexagon has exactly six equidistant neighbors, which makes ring queries and flow modeling clean. Resolutions run 0–15; each finer resolution divides a cell into 7 children (aperture-7), so the hierarchy is *not* strictly containing — a child hexagon can straddle a parent boundary. Cell ids are 64-bit integers, conventionally handled as 15-character hex strings such as `872830828ffffff`.

**S2** projects the sphere onto the six faces of a cube and recursively subdivides each face into four quadrilateral children, ordering the cells along a Hilbert space-filling curve. Levels run 0–30. Because subdivision is aperture-4 and strictly containing, an S2 cell id encodes its full ancestry: truncating the 64-bit id gives you the parent, and a contiguous id range covers a contiguous region. That range property is what makes S2 attractive for covering an arbitrary polygon with a compact set of cell ranges.

**Geohash** interleaves latitude and longitude bits and base-32 encodes them into strings where each added character refines the cell and every prefix is a valid coarser cell. Cells are lat/lon rectangles in EPSG:4326, so they distort badly toward the poles and are non-square at most latitudes. Geohash has no native hexagonal neighborhood; adjacency requires computing the eight bordering rectangles, and neighbors across a base-32 "seam" share no common prefix — the classic geohash edge problem.

<figure class="diagram">
<svg viewBox="0 0 752 212" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three discrete global grids covering the same map extent, each feeding a partition column in a lakehouse table">
<title>H3, S2 and geohash over the same area, mapped to a partition column</title>
<desc>Left: the same rectangular map extent tiled three ways — hexagons for H3, Hilbert-ordered quadrilaterals for S2, and axis-aligned base-32 rectangles for geohash. Right: each grid encodes a point into a cell id that becomes the table partition key.</desc>
<rect x="0" y="0" width="752" height="212" fill="#f7fbfc"/>
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
<text x="435" y="182" text-anchor="middle" font-family="sans-serif" font-size="10" fill="#0d3b45">prefix = parent</text>
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

The resolution defaults above are deliberately chosen to yield roughly comparable cell footprints: H3 res 7 (~5 km² average), S2 level 13 (~1.3 km² average), geohash precision 7 (~150 m × 150 m). Resolution sizing, not the grid choice alone, controls partition cardinality — see [choosing an H3 resolution for point data](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/grid-system-selection/choosing-h3-resolution-for-point-data/) for the sizing method.

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

For Delta Lake, `partitionBy("h3_res7")` writes physical directories, so prefer a coarser resolution there; Iceberg's `bucket` transform is the safer high-cardinality choice. The concrete Delta directory-based flow is covered in [implementing H3 hexagon partitioning in Delta Lake](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/spatial-partitioning-schemes/implementing-h3-hexagon-partitioning-in-delta-lake/).

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

A healthy plan lists the grid column under partition filters and shows a bucket/partition count far below the table total. If it does not, the predicate is being applied post-scan — see [predicate pushdown optimization](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/predicate-pushdown-optimization/) for the pushdown checklist.

## Performance and tuning

- **Resolution is the dominant knob.** Each H3 level multiplies cell count by ~7, each S2 level by 4, each geohash character by 32. Moving H3 res 7 → 8 roughly septuples partition cardinality. Size for 100 MB–1 GB of data per cell after compaction.
- **Bucket count.** With `bucket(N, grid_id)`, set `N` so that `rows_per_bucket × avg_row_bytes` lands in the 128 MB–1 GB target. For a 500 GB daily table, `N` between 256 and 1024 is typical.
- **Neighbor queries.** H3 `grid_disk(cell, k)` expands to a k-ring in one call; geohash needs `geohash.neighbors()` iterated per level and must handle base-32 seams; S2 uses `S2CellUnion` range covers. If your workload is ring-heavy, H3's constant 6-neighbor topology cuts query fan-out.
- **Hot partitions.** Dense urban cells hold orders of magnitude more rows than rural ones. The `bucket` transform spreads a hot cell across files by hashing; without it, a single H3 cell over a city center becomes a multi-gigabyte partition that stalls compaction. Pair coarse grid partitions with intra-file [Z-ordering](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/z-ordering-for-geospatial-queries/) on `lon, lat` to keep row groups locally clustered.
- **Interop cost.** Geohash strings are self-describing and need no library to decode a bounding box, which lowers cross-team friction; H3 and S2 ids are opaque without their libraries.

## Common errors and fixes

| Symptom | Root cause | Fix |
|---|---|---|
| Millions of tiny files, slow `LIST` | Grid resolution too fine for Delta directory partitioning | Coarsen resolution or move to Iceberg `bucket(N, grid_id)` |
| Query scans whole table despite grid filter | Grid id computed inside `WHERE` via UDF, or column not materialized | Persist the key as a column at ingest; filter on the literal id |
| One partition 100× larger than the rest | Hot urban cell, no hashing | Wrap key in `bucket()`; add Z-order on coordinates within the cell |
| Neighbor cells miss data at boundaries | Geohash base-32 seam or H3 pentagon distortion | Use `h3.grid_disk`; for geohash expand with `geohash.neighbors` on all 8 |
| S2/H3 ids differ between ingest and query | Mismatched library version or resolution/level constant | Pin `h3>=4.1`, `s2sphere>=0.2.5`; centralize the resolution constant |

Once you have chosen a grid, the two guides in this topic area go deeper: a full [head-to-head H3 vs S2 vs geohash comparison](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/grid-system-selection/h3-vs-s2-vs-geohash-for-lakehouse-partitioning/) with runnable cardinality benchmarks, and a data-driven method for [choosing an H3 resolution for point data](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/grid-system-selection/choosing-h3-resolution-for-point-data/). Authoritative references: the [H3 documentation](https://h3geo.org/docs/) and the [S2 Geometry library](https://s2geometry.io/).

## What "Discrete Global Grid" Actually Guarantees

The three systems compared on this page differ in geometry, but they share a set of properties that is worth stating explicitly, because the properties — not the shapes — are what make a grid useful as a partition key.

<figure class="diagram">
<svg viewBox="0 0 762 270" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Four properties a discrete global grid provides for lakehouse partitioning: total coverage with no gaps, a stable integer identifier per cell, a hierarchy where a child identifier implies its parent, and a computable neighbourhood for window expansion">
<rect x="0" y="0" width="762" height="270" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">The four properties that make a grid usable as a key</text>
<rect x="30" y="56" width="352" height="94" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="206" y="82" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">total coverage</text>
<text x="206" y="106" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">every point on Earth is in exactly one cell</text>
<text x="206" y="128" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">no gaps, no overlaps, no null partition</text>
<rect x="398" y="56" width="352" height="94" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="574" y="82" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">stable identifier</text>
<text x="574" y="106" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">a 64-bit integer, deterministic forever</text>
<text x="574" y="128" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">the same point always yields the same cell</text>
<rect x="30" y="164" width="352" height="94" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="206" y="190" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">hierarchy</text>
<text x="206" y="214" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">a fine cell&#8217;s parent is computable</text>
<text x="206" y="236" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">enables mixed-resolution partitioning</text>
<rect x="398" y="164" width="352" height="94" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="574" y="190" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">computable neighbourhood</text>
<text x="574" y="214" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">a window expands to a cell list without a scan</text>
<text x="574" y="236" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">this is what makes pruning possible</text>
</svg>
</figure>

The fourth property is the one that does the work in a lakehouse. Partition pruning requires the planner to convert "this bounding box" into "these partition values" before reading anything, and that conversion is only possible when the grid can enumerate the cells covering an arbitrary region cheaply. A grid without that property can still label rows, but it cannot accelerate a query, which makes it decorative.

The third property decides whether the scheme can adapt. A hierarchy where a child's identifier determines its parent allows a table to hold cells at several resolutions simultaneously and still answer a query correctly — coarse cells in sparse regions, fine cells in dense ones. Without it, mixed-resolution layouts require an explicit lookup on every read, which is workable but noticeably more machinery.

The first property matters for a reason that only shows up in production: it guarantees there is no null partition. Data that falls outside a bespoke grid has to go somewhere, and that somewhere becomes an unbounded partition holding everything the scheme failed to anticipate — the antimeridian crossings, the coordinates at exactly ±90°, the rows whose geometry is empty. A global grid removes the category.

## Cell Identifiers as Data, Not as Labels

Treating the cell identifier as an ordinary column with ordinary requirements avoids most of the operational problems that follow a grid choice.

<figure class="diagram">
<svg viewBox="0 0 766 230" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Requirements for storing a grid cell identifier: integer typed rather than a hex string, resolution recorded separately, derivation version recorded, and statistics enabled so the planner can prune on it">
<rect x="0" y="0" width="766" height="230" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Four requirements for the cell column</text>
<rect x="26" y="58" width="356" height="72" rx="8" fill="#ffffff" stroke="#0e6e7d" stroke-width="2"/>
<text x="204" y="84" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">BIGINT, never a hex string</text>
<text x="204" y="108" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">strings sort lexically and defeat range predicates</text>
<rect x="398" y="58" width="356" height="72" rx="8" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<text x="576" y="84" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">resolution in the column name</text>
<text x="576" y="108" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">h3_r5, not cell_id — the level must be visible</text>
<rect x="26" y="146" width="356" height="72" rx="8" fill="#ffffff" stroke="#9a5a17" stroke-width="2"/>
<text x="204" y="172" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">derivation version recorded</text>
<text x="204" y="196" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">so a library upgrade is detectable, not silent</text>
<rect x="398" y="146" width="356" height="72" rx="8" fill="#ffffff" stroke="#6a3d9a" stroke-width="2"/>
<text x="576" y="172" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">statistics enabled</text>
<text x="576" y="196" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">a column with no min/max prunes nothing</text>
</svg>
</figure>

The naming requirement earns its place through a specific failure. A column called `cell_id` is ambiguous the moment a second resolution is introduced, and tables frequently acquire a second resolution — a coarse one for partitioning and a fine one for aggregation. Encoding the level in the name makes a mismatched join fail at parse time rather than return an empty result.

The derivation version matters because grid libraries do change, rarely but consequentially. A cell assignment that shifts at a boundary between library versions puts a small number of rows in a different partition than the same coordinates would produce today, and the effect is a query that misses a handful of rows near cell edges. Recording the version alongside the identifier makes that detectable with a group-by rather than by comparison against a re-derivation of the entire table.

## Migrating Between Grids

Changing grid system after a table is populated is uncommon but not rare, and it happens for two reasons: a library licensing or support change, or the discovery that the chosen system handles the workload's geometry badly.

The migration is mechanically simple and operationally awkward. Deriving the new cell identifier is a per-row computation from coordinates that are already present, so it parallelises perfectly and costs one pass over the data. The awkwardness is that the new column cannot become the partition key without a rewrite, and every consumer that references the old column has to move.

The additive pattern applies here as it does to schema evolution generally. Add the new cell column, backfill it, keep both populated on the write path, migrate readers individually, and drop the old column when nothing reads it. Where the format supports partition evolution, the partition key can switch at a point in time with old data staying under the old specification, which makes the rewrite optional and schedulable rather than blocking.

One consideration is specific to grids. Because cell identifiers are opaque integers, a consumer using the wrong column produces no error and no obviously wrong output — just an empty or partial result. Give the new column a clearly different name, and consider making the old column's values invalid under the new derivation so a mismatch fails rather than returning nothing. Silence is the failure mode to design against.

Before committing to a migration, verify that the problem is genuinely the grid. Most complaints attributed to grid choice turn out to be resolution choice, skew, or a missing predicate on the partition column — all of which are cheaper to fix and none of which improves by changing systems. Reserve the migration for cases where a property of the grid itself, such as its behaviour at the poles or its cell-shape distortion at high latitudes, is demonstrably the cause.

## Resolution Is a Bigger Decision Than System

Teams spend more time choosing between grid systems than choosing a resolution, and the effect sizes point the other way.

Switching between the three systems at the same nominal cell size changes query performance by a modest amount — the differences are in cell shape, identifier structure and library ergonomics rather than in how much data a query reads. Changing resolution by one level changes the number of cells covering a fixed area by a factor of roughly five to seven, which changes partition count, average partition size and the number of partitions a typical query touches by the same factor. That is the decision that determines whether the table performs.

The right procedure is to fix the resolution from the query extent first, then choose the system on secondary criteria: which library is already in the stack, whether the workload needs uniform cell shapes for aggregation, whether identifiers need to be human-readable, whether polar coverage matters. Making the choice in that order takes an afternoon; making it in the reverse order produces a well-argued system choice at a resolution that does not fit the workload.

The detailed procedure for fixing the resolution is in [choosing H3 resolution for point data](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/grid-system-selection/choosing-h3-resolution-for-point-data/), and the system-level comparison, once the resolution is settled, is in [H3 vs S2 vs geohash for lakehouse partitioning](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/grid-system-selection/h3-vs-s2-vs-geohash-for-lakehouse-partitioning/).

Whichever system a table settles on, record the choice, the resolution and the library version in the table's own properties so that the next person to touch it inherits the reasoning rather than reverse-engineering it from the data.
