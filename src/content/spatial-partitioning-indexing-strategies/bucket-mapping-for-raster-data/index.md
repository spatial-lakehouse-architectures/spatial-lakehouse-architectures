# Bucket Mapping for Raster Data

Raster datasets—satellite imagery, digital elevation models (DEMs), LiDAR derivatives, and climate reanalysis grids—introduce distinct storage and query challenges in modern data lakehouses. Unlike vector geometries, rasters are inherently grid-aligned, frequently exceed multi-terabyte scales, and exhibit highly localized, bounding-box-driven access patterns. Bucket mapping translates continuous spatial coordinates into discrete, query-optimized partition directories, enabling efficient predicate pushdown, metadata pruning, and predictable I/O behavior. This technique operates as a specialized implementation layer within broader [Spatial Partitioning & Indexing Strategies](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/), where physical storage layout must align tightly with downstream analytical and GIS processing workloads.

## Deterministic Coordinate Transformation & CRS Alignment

Effective bucket mapping begins with deterministic coordinate transformation. Raw geographic coordinates (WGS84, EPSG:4326) introduce severe distortion and uneven bucket sizes at higher latitudes, making them unsuitable for direct partitioning. Production pipelines must project raster extents into a metric-aligned coordinate reference system (CRS) before computing bucket identifiers.

For continental-scale ingestion, Universal Transverse Mercator (UTM) zones establish a natural, meter-based grid. The ingestion pipeline derives the UTM zone identifier, truncates easting/northing values to a fixed tile size, and hashes them into partition columns. See [Mapping UTM zones to Iceberg partition columns](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/bucket-mapping-for-raster-data/mapping-utm-zones-to-iceberg-partition-columns/) for detailed schema evolution patterns.

**Explicit Production Parameters:**
- **Target CRS:** `EPSG:32633` (UTM Zone 33N)
- **Tile Size:** `2000m × 2000m` (aligns with typical 1024×1024 GeoTIFF block boundaries)
- **Bucket Formula:** `bucket_id = CONCAT(FLOOR(easting / 2000), '_', FLOOR(northing / 2000))`
- **Partition Bounds:** `easting ∈ [100000, 900000]`, `northing ∈ [1100000, 9200000]`

This deterministic mapping ensures that adjacent spatial tiles map to predictable directory paths, preventing coordinate drift from propagating into consumer queries.

## Partition Hierarchy & Directory Layout

Architecting the partition hierarchy requires balancing directory depth against query selectivity. [Spatial Partitioning Schemes](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/spatial-partitioning-schemes/) outlines the trade-offs between coarse administrative boundaries, hierarchical quadtrees, and flat spatial hashing. For raster workloads, a two-tier partition strategy consistently delivers optimal query planner performance:

1. **Coarse Partition:** `utm_zone`, `acquisition_year`, `sensor_type`
2. **Bucket Partition:** `spatial_bucket` (string-encoded grid cell)

This structure prevents metadata explosion while maintaining high pruning efficiency for regional or temporal queries. Target file sizes should align with cloud storage block limits (typically 128MB–512MB per Parquet/GeoParquet file) to avoid excessive `LIST` API calls during manifest reads. Over-partitioning below 64MB/file triggers metadata bloat, while under-partitioning above 2GB/file degrades parallel read throughput.

## Production Implementation Patterns

### PySpark Ingestion Pipeline

The following snippet demonstrates coordinate projection, bucket derivation, and Iceberg table writes with explicit partition specs. It assumes `easting_utm33n` and `northing_utm33n` columns were computed upstream (e.g., via `pyproj` or Sedona `ST_Transform`):

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, floor, concat_ws
import pyspark.sql.types as T

spark = SparkSession.builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
    .getOrCreate()

# Load raw raster catalog (GeoTIFF paths + bounding boxes pre-projected to EPSG:32633)
raw_df = spark.read.parquet("s3://raw-catalog/landsat_metadata/")

# Compute 2000m bucket IDs from UTM 33N coordinates
bucket_df = raw_df.withColumn(
    "spatial_bucket",
    concat_ws(
        "_",
        floor(col("easting_utm33n") / 2000).cast(T.IntegerType()),
        floor(col("northing_utm33n") / 2000).cast(T.IntegerType())
    )
)

# Write with Iceberg partitioning
bucket_df.writeTo("lakehouse.raster.landsat_bucketed") \
    .partitionedBy("utm_zone", "acquisition_year", "spatial_bucket") \
    .append()
```

### SQL DDL & Query Pruning

Define the table schema and verify partition pruning via `EXPLAIN`:

```sql
CREATE TABLE lakehouse.raster.landsat_bucketed (
    raster_path      STRING,
    sensor_type      STRING,
    acquisition_date DATE,
    utm_zone         INT,
    easting          DOUBLE,
    northing         DOUBLE,
    spatial_bucket   STRING
) USING iceberg
PARTITIONED BY (utm_zone, year(acquisition_date), spatial_bucket);

-- Query engine will prune partitions matching the bucket range
EXPLAIN SELECT COUNT(*) FROM lakehouse.raster.landsat_bucketed
WHERE spatial_bucket BETWEEN '450_5200' AND '455_5205'
  AND acquisition_date >= '2023-01-01';
```

### CI/CD Validation Step

Validate partition structure before merging pipeline changes:

```yaml
# .github/workflows/validate-partitions.yml
name: Validate Raster Partition Schema
on: [pull_request]
jobs:
  check-partitions:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Run partition validator
        run: |
          python scripts/validate_bucket_schema.py \
            --catalog-path s3://lakehouse/raster/landsat/ \
            --expected-tile-size 2000 \
            --crs EPSG:32633 \
            --max-partitions-per-year 15000
```

## Query Execution & Pruning Mechanics

Bucket mapping enables the query planner to translate spatial predicates directly into directory scans. When a bounding box intersects multiple tiles, the engine computes the overlapping `spatial_bucket` range, reads only the relevant manifest entries, and skips non-matching partitions entirely. This reduces I/O by 60–85% compared to unpartitioned lakehouse scans.

For multi-dimensional workloads combining spatial, temporal, and spectral filters, bucket mapping pairs effectively with [Z-Ordering for Geospatial Queries](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/z-ordering-for-geospatial-queries/). While bucket mapping handles coarse directory pruning, Z-ordering optimizes file-level data layout within those directories, minimizing the number of Parquet row groups scanned per query.

## Operational Guardrails & Troubleshooting

### Common Failure Modes
| Symptom | Root Cause | Remediation |
|---------|------------|-------------|
| Query planner scans 100% of partitions | CRS mismatch between ingestion and query layer | Standardize all pipelines to a single EPSG code; validate with `ST_Transform` checks |
| Metadata bloat (>500k partitions) | Tile size too small or over-partitioning | Increase `tile_size` to 4000m+; consolidate historical data using `OPTIMIZE` / `rewrite_data_files` |
| Severe partition skew | Coastal/urban rasters span multiple UTM zones | Implement zone-edge buffering; route edge tiles to a dedicated `utm_zone=99` fallback partition |
| `FileNotFoundException` on read | Stale manifest after external deletion | Run `CALL lakehouse.system.expire_snapshots(...)` and refresh catalog metadata |

### Retention & Lifecycle Policies

Raster archives require strict lifecycle management to control storage costs. Implement time-based retention aligned with data utility:
- **Operational Satellite Imagery:** `retention_days = 1095` (3-year rolling)
- **Climate Reanalysis Grids:** `retention_days = 3650` (10-year archival)
- **LiDAR Point Clouds:** `retention_days = 7300` (20-year compliance)

Automate cleanup using scheduled Iceberg maintenance:
```sql
CALL lakehouse.system.expire_snapshots(
    table => 'lakehouse.raster.landsat_bucketed',
    older_than => TIMESTAMPADD(YEAR, -3, CURRENT_TIMESTAMP),
    retain_last => 5
);
```

### Debugging Workflow
1. **Verify Bucket Alignment:** Cross-check `spatial_bucket` values against known tile boundaries using the [GDAL Raster Data Model](https://gdal.org/user/raster_data_model.html) reference.
2. **Inspect Manifests:** Query `lakehouse.raster.landsat_bucketed.partitions` to confirm partition distribution matches expected spatial density.
3. **Profile Query Plans:** Run `EXPLAIN FORMATTED` to verify `PartitionFilters` predicates are applied before `FileScan`.
4. **Validate CRS Consistency:** Ensure all upstream producers reference the official [EPSG Geodetic Parameter Dataset](https://epsg.org/) to prevent silent coordinate drift.

Bucket mapping transforms unstructured raster sprawl into deterministic, query-ready storage layouts. By enforcing strict CRS alignment, calibrated tile sizes, and automated lifecycle policies, platform teams can deliver sub-second spatial pruning at petabyte scale while maintaining full compatibility with downstream GIS and analytical engines.

## Why Raster Does Not Fit a Row-Oriented Table

Open table formats model a table as rows with columns. A raster is a regular grid of values with an affine transform mapping grid indices to coordinates. Forcing one into the other is possible in three ways, and the choice determines everything downstream.

<figure class="diagram">
<svg viewBox="0 0 764 272" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three ways to represent raster data in a lakehouse: one row per pixel, one row per tile holding an encoded block, and one row per scene holding only metadata and a pointer to an external cloud optimised file">
<rect x="0" y="0" width="764" height="272" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Three representations, three very different tables</text>
<rect x="26" y="56" width="230" height="204" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="141" y="84" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">row per pixel</text>
<text x="141" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">x, y, band, value</text>
<text x="141" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">fully queryable in SQL</text>
<text x="141" y="162" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">arbitrary spatial joins</text>
<text x="141" y="192" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">10⁹ rows per modest scene</text>
<text x="141" y="214" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">storage explodes</text>
<text x="141" y="240" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">use for: small analytical extracts</text>
<rect x="274" y="56" width="230" height="204" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="389" y="84" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">row per tile</text>
<text x="389" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">tile id, bbox, encoded block</text>
<text x="389" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">tractable row counts</text>
<text x="389" y="162" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">spatial pruning works</text>
<text x="389" y="192" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">values need decoding in a UDF</text>
<text x="389" y="214" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">tile size is a hard commitment</text>
<text x="389" y="240" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">use for: analysis at tile granularity</text>
<rect x="522" y="56" width="230" height="204" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="637" y="84" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">row per scene</text>
<text x="637" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">footprint, time, band, URI</text>
<text x="637" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">tiny table, fast catalogue</text>
<text x="637" y="162" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">pixels stay in their native format</text>
<text x="637" y="192" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">no pixel-level SQL</text>
<text x="637" y="214" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">two systems to operate</text>
<text x="637" y="240" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">use for: discovery over archives</text>
</svg>
</figure>

The middle option is the one this section is mostly about, because it is the only one that keeps pixel values inside the lakehouse at a scale that stays operable. The arithmetic is straightforward: a 10,980-pixel-square scene at 10-metre resolution holds around 120 million pixels per band, so a row-per-pixel table for a year of global coverage reaches a scale no engine will enjoy. The same scene split into 512-pixel tiles is about 460 rows per band — a number that partitions, prunes and joins comfortably.

The right-hand option is not a failure to commit; it is frequently the correct architecture. Keeping pixels in Cloud-Optimised GeoTIFF or Zarr, and keeping footprints, acquisition times, cloud cover and quality flags in a lakehouse table, gives fast discovery over a huge archive with a table that stays small. Analysis then reads only the assets the discovery query selected. The cost is that the pixel access path is not SQL, which matters only if pixel-level SQL is actually required.

Most mature platforms run the right-hand and middle patterns together: a catalogue table over the full archive, and tiled tables for the specific products that analysts query repeatedly. That combination keeps the expensive representation scoped to the data that earns it.

## Aligning Raster Tiles With Vector Partitions

The reason to think about raster bucketing at all, rather than treating it as a separate storage problem, is that raster and vector data are usually joined — extracting values at points, summarising a band within polygons, masking one by the other. The join is efficient only when the two layouts agree.

<figure class="diagram">
<svg viewBox="0 0 724 268" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="A vector partition grid overlaid on raster tiles in two configurations: misaligned, where each vector cell touches four raster tiles, and aligned, where the raster tile boundaries fall on vector cell boundaries">
<rect x="0" y="0" width="724" height="268" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Alignment decides how many tiles a cell-scoped join reads</text>
<text x="196" y="62" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#9a5a17">misaligned</text>
<rect x="96" y="78" width="200" height="150" fill="#ffffff" stroke="#9a5a17" stroke-width="1.5"/>
<line x1="96" y1="128" x2="296" y2="128" stroke="#9a5a17" stroke-width="1.5"/>
<line x1="96" y1="178" x2="296" y2="178" stroke="#9a5a17" stroke-width="1.5"/>
<line x1="163" y1="78" x2="163" y2="228" stroke="#9a5a17" stroke-width="1.5"/>
<line x1="230" y1="78" x2="230" y2="228" stroke="#9a5a17" stroke-width="1.5"/>
<rect x="140" y="105" width="80" height="66" fill="#f2e8da" fill-opacity="0.75" stroke="#0e6e7d" stroke-width="2.5"/>
<text x="196" y="252" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">one vector cell &#8594; 4 raster tiles read</text>
<text x="584" y="62" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#2f6e49">aligned</text>
<rect x="484" y="78" width="200" height="150" fill="#ffffff" stroke="#2f6e49" stroke-width="1.5"/>
<line x1="484" y1="128" x2="684" y2="128" stroke="#2f6e49" stroke-width="1.5"/>
<line x1="484" y1="178" x2="684" y2="178" stroke="#2f6e49" stroke-width="1.5"/>
<line x1="551" y1="78" x2="551" y2="228" stroke="#2f6e49" stroke-width="1.5"/>
<line x1="618" y1="78" x2="618" y2="228" stroke="#2f6e49" stroke-width="1.5"/>
<rect x="551" y="128" width="67" height="50" fill="#e6f0ea" fill-opacity="0.75" stroke="#0e6e7d" stroke-width="2.5"/>
<text x="584" y="252" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">one vector cell &#8594; 1 raster tile read</text>
</svg>
</figure>

The four-to-one difference shown here is the best case for misalignment; with an offset in both axes and a tile smaller than the cell it is worse. On a join over millions of cells, that factor multiplies directly into bytes read and into shuffle volume, and it is entirely avoidable by choosing the tile origin and size so that tile boundaries fall on partition-cell boundaries.

Achieving alignment usually means accepting a tile size that is not a round number of pixels, or a partition resolution that is not the one a purely vector analysis would choose. That trade is worth making when raster-vector joins are a routine workload, and not worth making when they are occasional — in which case a reprojection-and-resample step at join time is cheaper than distorting both layouts permanently.

One caution: alignment holds only within a single projection. A raster tiled in a projected coordinate system and a vector table partitioned by a geographic grid cannot align, because the cell boundaries are curves in the raster's space. Where both matter, pick the coordinate system for the join, and store a partition key derived in that system on both sides.

## Choosing a Tile Size

Tile size is the raster equivalent of partition resolution, and the same arithmetic decides it: the tile should be large enough that the per-row overhead is negligible and small enough that a typical query does not read much it does not need.

<figure class="diagram">
<svg viewBox="0 0 732 246" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Trade-off curve for raster tile size showing that small tiles inflate row counts and metadata while large tiles force queries to read pixels outside the requested area, with a workable band in the middle">
<rect x="0" y="0" width="732" height="246" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Tile size: two costs pulling in opposite directions</text>
<line x1="80" y1="196" x2="720" y2="196" stroke="#33707d" stroke-width="1.5"/>
<text x="400" y="230" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">tile edge in pixels &#8594;</text>
<text x="120" y="216" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">64</text>
<text x="290" y="216" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">256</text>
<text x="450" y="216" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">512</text>
<text x="620" y="216" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">2048</text>
<path d="M100 70 C 180 140, 240 172, 340 184" fill="none" stroke="#9a5a17" stroke-width="2.5"/>
<text x="150" y="60" font-family="sans-serif" font-size="11" font-weight="700" fill="#9a5a17">row count + metadata cost</text>
<path d="M340 184 C 460 176, 560 130, 700 66" fill="none" stroke="#6a3d9a" stroke-width="2.5"/>
<text x="560" y="60" font-family="sans-serif" font-size="11" font-weight="700" fill="#6a3d9a">wasted pixels per query</text>
<rect x="286" y="66" width="150" height="126" fill="#e6f0ea" fill-opacity="0.55" stroke="#2f6e49" stroke-width="2"/>
<text x="361" y="124" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">workable band</text>
<text x="361" y="146" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">256–512 px</text>
</svg>
</figure>

The 256-to-512 pixel band is where most production systems land, and the reason is not arbitrary. At 512 pixels square with a 16-bit band, a tile holds about 512 KB uncompressed — large enough that the row overhead and the per-tile metadata are irrelevant, small enough that a point query decodes half a megabyte rather than fifty. It also matches the internal tiling of most Cloud-Optimised GeoTIFF products, so a conversion is a copy rather than a resample.

Two workload characteristics push the choice within the band. **Point sampling** — extracting a value at scattered locations — favours smaller tiles, because every sample decodes a whole tile to read one pixel. **Window statistics** over polygons favour larger tiles, because the decode cost amortises across many pixels that are all needed. Where both workloads matter and the archive is large enough to justify it, storing two tilings is defensible; where it is not, size for the more frequent one.

The decision is effectively permanent, because re-tiling is a full rewrite of the pixel data rather than a metadata change. That asymmetry with vector partitioning is worth stating plainly: a vector partition key can often evolve, a raster tiling cannot, so the measurement effort belongs up front.

## Metadata Every Tile Row Needs

A tile row is only useful if the reader can place it, decode it and trust it without consulting anything else.

The **spatial placement** needs both the tile's bounding box in the table's declared coordinate system and the affine transform mapping pixel indices to coordinates. The bounding box drives pruning; the transform is what lets a consumer compute which pixel corresponds to a given location without re-deriving it from the tiling scheme.

The **decode contract** needs the data type, the band count, the compression codec and the nodata value. Nodata in particular has to be explicit, because the alternative is that every consumer invents its own convention and aggregate statistics silently include fill values. A mean computed over a tile whose nodata is 0 and whose consumer assumed -9999 is wrong by an amount nobody will notice.

The **provenance** needs the source scene identifier, the acquisition timestamp and the processing version. These are what make a result reproducible and what make a targeted reprocessing possible — when a provider reissues a scene, the rows to replace are exactly those carrying its identifier, and finding them should be a predicate rather than a search.

Finally, a **quality summary** per tile pays for itself immediately: the fraction of valid pixels, and the minimum and maximum of each band. The valid fraction lets a query skip tiles that are entirely cloud or entirely outside the swath before decoding anything, and the per-band range serves the same pruning role for value-based filters that the bounding box serves for spatial ones.

## Operational Notes for Tiled Raster Tables

Tiled raster tables behave differently from vector tables in maintenance, and three habits keep them healthy.

**Compaction has a lower ceiling.** A tile row already holds a large binary payload, so target file sizes are reached with far fewer rows than a vector table needs. Compacting to 128 MB files means roughly 250 tiles per file at half a megabyte each, which is a perfectly reasonable target — but a compaction configured by row count rather than by byte size will produce multi-gigabyte files and long tail-latency reads.

**Sorting matters less, tiling matters more.** Because the tile identifier already encodes position, and because tiles are large, within-file ordering has much less effect than it does for point data. Effort that would go into sort tuning on a vector table is better spent on tile alignment and on the per-tile quality summary.

**Reprocessing is routine.** Providers reissue scenes, and a raster table that cannot cleanly replace one scene's tiles will accumulate duplicates or require full rewrites. Partitioning by scene identifier — or at minimum keeping it as a column with statistics — makes the replacement a scoped delete-and-append rather than a table-wide operation.

Together these keep the table's maintenance cost proportional to the data that changes rather than to the archive's total size, which is the only property that lets a raster table grow for years without the operations budget growing with it.

## A Short Decision Summary

Store pixels as tiles inside the lakehouse when analysts run repeated value extractions and the product set is bounded. Keep pixels in their native cloud-optimised format with a lakehouse catalogue over the footprints when the archive is large, heterogeneous and mostly searched rather than computed over. Size tiles in the 256-to-512 pixel band unless a measurement says otherwise, align tile boundaries to the vector partition grid when raster-vector joins are routine, and give every tile row a bounding box, an affine transform, an explicit nodata value, a provenance identifier and a valid-pixel fraction. Those five fields are what make the table self-describing enough that a query planner can prune it and a future reader can use it, which is the same standard the vector tables on this site are held to.

The recurring theme is that raster in a lakehouse succeeds when the table describes the pixels rather than trying to be the pixels. Every field above exists so that a planner can decide what to read and a consumer can decode what it gets, and the tables that disappoint are almost always the ones that stored the bytes and left the description to the pipeline that wrote them.
 Design the description first, and the storage decision usually follows from it.

A last practical suggestion: keep a single worked example of the full path — one scene, tiled, catalogued, joined to a vector layer and sampled at points — in the repository as an executable notebook. Raster layouts have enough moving parts that a written specification is regularly interpreted two ways, and a runnable example settles the interpretation in seconds.
 A notebook that runs is worth more than a page that describes.
 Keep it current, and keep it small.
