# Spatial Partitioning & Indexing Strategies for Lakehouse Architectures

## Core Architecture & Lakehouse Fundamentals

Modern spatial data platforms have transitioned from proprietary GIS storage (PostGIS, GeoTIFF mosaics, Shapefiles) to open table formats like Apache Iceberg and Delta Lake. This architectural shift redefines how spatial metadata, physical file layout, and query execution interact. In a lakehouse, spatial performance is governed by three planes: object storage (S3/ADLS/GCS), the table format's metadata catalog, and the compute engine's spatial extensions.

<figure class="diagram">
<svg viewBox="0 0 757 222" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="A spatial query flowing through partition pruning and bbox file-skipping: query window, catalog manifest filter prunes partitions, file-level bbox min max stats skip files, and only surviving Parquet files are scanned">
<defs>
<marker id="part-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#0e6e7d"/></marker>
</defs>
<rect x="0" y="0" width="757" height="222" fill="#f7fbfc"/>
<text x="380" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Partition prune, then bbox file-skip</text>
<rect x="15" y="58" width="165" height="86" rx="8" fill="#ffffff" stroke="#0e6e7d" stroke-width="2"/>
<text x="97" y="90" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">Query window</text>
<text x="97" y="112" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">ST_Intersects(bbox)</text>
<rect x="210" y="58" width="165" height="86" rx="8" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<text x="292" y="90" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">Manifest filter</text>
<text x="292" y="112" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">prune partitions</text>
<rect x="405" y="58" width="165" height="86" rx="8" fill="#ffffff" stroke="#9a5a17" stroke-width="2"/>
<text x="487" y="90" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">Bbox min/max</text>
<text x="487" y="112" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">skip files</text>
<rect x="600" y="58" width="145" height="86" rx="8" fill="#ffffff" stroke="#6a3d9a" stroke-width="2"/>
<text x="672" y="90" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">Scan Parquet</text>
<text x="672" y="112" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">surviving files</text>
<line x1="180" y1="101" x2="210" y2="101" stroke="#0e6e7d" stroke-width="2" marker-end="url(#part-arrow)"/>
<line x1="375" y1="101" x2="405" y2="101" stroke="#0e6e7d" stroke-width="2" marker-end="url(#part-arrow)"/>
<line x1="570" y1="101" x2="600" y2="101" stroke="#0e6e7d" stroke-width="2" marker-end="url(#part-arrow)"/>
<text x="380" y="185" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="600" fill="#0d3b45">1,000 files &#8594; 40 after partition prune &#8594; 6 after bbox skip</text>
<text x="380" y="206" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">Every stage discards non-overlapping data before any geometry is deserialized</text>
</svg>
</figure>

Unlike traditional RDBMS spatial indexes that maintain in-memory or on-disk tree structures (GiST, R-tree), lakehouse architectures rely on partition metadata, column-level statistics, and file-level clustering to achieve spatial selectivity. The catalog tracks min/max bounding boxes and CRS metadata per data file. Query engines leverage this metadata to prune scans before deserializing geometries. This model eliminates index bloat and enables concurrent reads/writes, but it demands deliberate partitioning strategies, explicit clustering configurations, and strict operational boundaries to prevent performance degradation.

## Choosing a Partition Key: A Decision Path

Partition-key selection is the highest-leverage decision on this page, and it is made once per table with consequences that persist for years. The choice is driven by the *query* shape, not by the data shape — a table whose rows are points scattered worldwide but whose queries are always scoped to one city should be partitioned by city-scale cells, not by anything intrinsic to the points.

<figure class="diagram">
<svg viewBox="0 0 774 360" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Decision tree for choosing a spatial partition key: start from the dominant query filter, branching to administrative partitioning, hierarchical grid partitioning, time-plus-grid compound keys, or no spatial partitioning with sort order alone">
<defs>
<marker id="part-dt-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#2f6e49"/></marker>
</defs>
<rect x="0" y="0" width="774" height="360" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Which partition key does the workload actually need?</text>
<rect x="255" y="48" width="270" height="52" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="390" y="70" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">What filters the dominant query?</text>
<text x="390" y="89" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">measure it; do not assume it</text>
<rect x="18" y="150" width="168" height="96" rx="8" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<text x="102" y="174" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">A named region</text>
<text x="102" y="196" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">&#8220;all assets in Bavaria&#8221;</text>
<text x="102" y="218" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">partition by the</text>
<text x="102" y="234" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">administrative code</text>
<rect x="204" y="150" width="168" height="96" rx="8" fill="#ffffff" stroke="#0e6e7d" stroke-width="2"/>
<text x="288" y="174" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">An arbitrary window</text>
<text x="288" y="196" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">a map viewport, a buffer</text>
<text x="288" y="218" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">partition by a grid cell</text>
<text x="288" y="234" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">at query resolution</text>
<rect x="390" y="150" width="168" height="96" rx="8" fill="#ffffff" stroke="#0e6e7d" stroke-width="2"/>
<text x="474" y="174" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">Time, then place</text>
<text x="474" y="196" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">&#8220;yesterday, near here&#8221;</text>
<text x="474" y="218" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">compound key: day</text>
<text x="474" y="234" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">first, coarse cell second</text>
<rect x="576" y="150" width="186" height="96" rx="8" fill="#ffffff" stroke="#9a5a17" stroke-width="2"/>
<text x="669" y="174" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">Nothing spatial</text>
<text x="669" y="196" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">full-extent aggregations</text>
<text x="669" y="218" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">do not partition; use</text>
<text x="669" y="234" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">sort order + bbox stats</text>
<line x1="330" y1="100" x2="140" y2="150" stroke="#2f6e49" stroke-width="2" marker-end="url(#part-dt-arrow)"/>
<line x1="366" y1="100" x2="300" y2="150" stroke="#2f6e49" stroke-width="2" marker-end="url(#part-dt-arrow)"/>
<line x1="414" y1="100" x2="462" y2="150" stroke="#2f6e49" stroke-width="2" marker-end="url(#part-dt-arrow)"/>
<line x1="450" y1="100" x2="630" y2="150" stroke="#2f6e49" stroke-width="2" marker-end="url(#part-dt-arrow)"/>
<rect x="120" y="286" width="540" height="62" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="390" y="310" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">Then check the arithmetic in every branch</text>
<text x="390" y="331" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">target 128 MB–1 GB per partition file, and fewer than ~10,000 partitions per table</text>
</svg>
</figure>

The four branches carry different failure modes. **Administrative partitioning** is the most readable and the most skewed: a country-code partition puts 40% of the rows in one value for most global datasets, and that partition becomes the straggler in every job. It is nonetheless the right answer when queries genuinely arrive as region names, because the pruning is exact and requires no spatial reasoning from the planner at all.

**Grid partitioning** trades readability for uniformity. A hierarchical cell identifier at the resolution of the typical query window gives predictable partition sizes and lets the planner turn a bounding-box filter into an `IN` list of cell identifiers. The resolution choice is covered in detail in [choosing H3 resolution for point data](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/grid-system-selection/choosing-h3-resolution-for-point-data/); the important discipline is to select it from the *query* extent rather than from the data density, then verify the resulting partition count.

**Compound keys** are correct whenever a time filter is present in nearly every query, which is the case for all telemetry. Put time first: it is the more selective predicate, it aligns with the retention and compaction lifecycle, and it means the spatial component only ever discriminates within a day's data. Inverting the order — cell first, day second — produces the same logical result and a much worse physical layout, because every daily maintenance job then touches every partition.

**No spatial partition at all** is an underrated answer. For tables under a few hundred gigabytes whose queries scan wide extents, partitioning adds metadata overhead without adding pruning. A single flat table sorted by a space-filling curve, with reliable per-file bounding-box statistics, will prune adequately and avoids every small-file pathology.

## Sizing Partitions: The Arithmetic

Partition design fails in the arithmetic far more often than in the concept. Two numbers govern it, and both can be computed before a single row is written.

The first is **bytes per partition**. Take the compressed table size, divide by the number of distinct partition values, and check the result lands between 128 MB and 1 GB. A 4 TB table split across H3 resolution-7 cells over a populated continent yields hundreds of thousands of partitions averaging well under 20 MB — a guaranteed small-file problem, before any consideration of skew. The same table at resolution 4 yields a few thousand partitions in the right size band.

The second is **partition count against planning cost**. Query planning must enumerate and filter partition metadata, and that cost is roughly linear in partition count. Below about 10,000 partitions, planning is negligible. Between 10,000 and 100,000 it becomes visible on short queries. Above that, the planner dominates the runtime for anything selective, which is precisely the query type the partitioning existed to accelerate.

There is a third number worth computing even though it is harder to estimate: **rows per file after sorting**. Data skipping works at the row-group level inside a file, typically 128 MB of rows at a time. A file holding 8 million points spread randomly over a continent has row groups whose bounding boxes each cover the continent, so skipping achieves nothing within the file. The same file sorted by a space-filling curve gives each row group a compact box, and a selective query reads two row groups instead of sixty. Sorting is what makes file-level statistics useful; without it, per-file bounding boxes degenerate towards the full extent.

A practical calibration loop: write one day of representative data with a candidate key, measure the file-size distribution and the partition count, run three representative queries with `EXPLAIN ANALYZE`, and record the bytes actually scanned against the bytes the query logically needed. A ratio under 5× is healthy. A ratio above 50× means the layout is not doing the work, and no amount of engine tuning will compensate.


## Partitioning Boundary Design

Partitioning in a spatial lakehouse dictates the physical directory hierarchy and directly controls I/O scope. Traditional temporal or business-key partitions rarely align with geographic predicates. When evaluating [Spatial Partitioning Schemes](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/spatial-partitioning-schemes/), architects must balance partition granularity against the small-files problem. Over-partitioning by high-resolution grid cells (e.g., H3 resolution 10 or S2 level 15) creates millions of directories, overwhelming catalog APIs and inflating manifest overhead. Under-partitioning forces full-table scans, negating spatial filtering benefits.

Production configurations typically adopt hierarchical spatial partitioning combined with a secondary temporal key. This limits directory fan-out while preserving spatial locality. Always cap active partitions per table to fewer than 10,000 to avoid catalog latency spikes and ensure efficient manifest generation.

**Iceberg Partition Specification:**
```sql
-- Apache Iceberg: Hierarchical spatial + temporal partitioning
CREATE TABLE geospatial.traffic_events (
  event_id BIGINT,
  geom     BINARY,   -- WKB
  h3_res6  STRING,   -- computed upstream
  event_ts TIMESTAMP,
  payload  STRING
)
USING iceberg
PARTITIONED BY (h3_res6, days(event_ts))
LOCATION 's3://lakehouse-prod/traffic/';
```

**Trade-off:** Coarse partitions reduce metadata overhead but increase scan volume per query. Fine partitions improve pruning but increase write amplification and catalog API calls. Use `bucket()` or `truncate()` transforms on encoded spatial keys to enforce deterministic boundaries and prevent hot partitions during bulk ingestion.

## Multi-Dimensional File Clustering

Partitioning handles coarse locality, but arbitrary spatial predicates (`ST_Intersects`, `ST_DWithin`) require fine-grained file clustering. Spatial coordinates are inherently two-dimensional, while object storage and table formats operate on one-dimensional byte streams. Mapping techniques like Z-ordering or Hilbert curves preserve spatial locality during writes. Implementing [Z-Ordering for Geospatial Queries](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/z-ordering-for-geospatial-queries/) ensures that geometries with similar bounding boxes are co-located in the same Parquet files.

**Delta Lake Z-Order Optimization:**
```sql
-- Delta Lake: Optimize with Z-Ordering on spatial bounding coordinates
OPTIMIZE geospatial.land_parcels
ZORDER BY (min_lon, min_lat, max_lon, max_lat);
```

**Trade-off:** Z-ordering increases write amplification and requires periodic `OPTIMIZE`/`VACUUM` cycles. It is most effective when read patterns heavily filter on coordinate ranges or bounding boxes. Avoid Z-ordering on high-cardinality string columns, unbounded geometries, or tables with high-frequency micro-batch writes, as the compaction cost will outweigh query latency gains.

## Metadata-Driven Predicate Pushdown

Lakehouse performance hinges on metadata pruning. Engines like Spark, Trino, and DuckDB extract min/max bounding boxes from file footers. When a query executes, the planner evaluates these statistics before deserializing geometries. Properly configured [Predicate Pushdown Optimization](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/predicate-pushdown-optimization/) eliminates unnecessary I/O by skipping files whose bounding boxes do not intersect the query window.

**Implementation Requirements:**
- Ensure spatial columns are written with a consistent CRS (e.g., EPSG:4326) to prevent coordinate transformation overhead during filtering.
- Materialize explicit bounding box columns (`bbox_min_x`, `bbox_min_y`, `bbox_max_x`, `bbox_max_y`) as `DOUBLE` so the engine can track min/max statistics in manifests.
- Align geometry serialization with the [OGC Simple Features Access](https://www.ogc.org/standards/sfa) specification to guarantee consistent predicate evaluation across heterogeneous compute engines.

**Trade-off:** Metadata pruning is only as accurate as the statistics collected. Sparse or highly skewed spatial distributions can lead to false positives where files are scanned unnecessarily. Supplement pruning with file-level clustering to tighten bounding box accuracy.

## Raster & Vector Hybrid Layouts

Spatial workloads rarely consist solely of vector geometries. Raster data (satellite imagery, DEMs, LiDAR) requires chunked storage aligned with spatial boundaries. [Bucket Mapping for Raster Data](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/bucket-mapping-for-raster-data/) enables efficient tile retrieval by mapping geographic extents to deterministic object storage keys. Production pipelines typically split large GeoTIFFs into 256×256 or 512×512 pixel tiles, store them with spatial metadata, and register them in the lakehouse catalog.

**PySpark Raster Chunking Pattern:**
```python
import rasterio
from pyspark.sql import SparkSession

def chunk_raster_to_catalog(raster_uri: str, chunk_size: int = 512,
                              output_table: str = "geospatial.raster_tiles"):
    """
    Opens a raster, extracts spatial metadata, and registers tile records.
    Actual tile pixel data should be stored as COG or Zarr externally;
    this function writes a metadata catalog entry per tile.
    """
    with rasterio.open(raster_uri) as src:
        transform = src.transform
        crs = src.crs.to_epsg()
        width, height = src.width, src.height

        tiles = []
        for row_off in range(0, height, chunk_size):
            for col_off in range(0, width, chunk_size):
                # Compute tile bounding box in source CRS
                min_x, min_y = rasterio.transform.xy(transform, row_off + chunk_size, col_off,
                                                      offset="ul")
                max_x, max_y = rasterio.transform.xy(transform, row_off, col_off + chunk_size,
                                                      offset="ul")
                tiles.append({
                    "raster_uri": raster_uri,
                    "tile_row": row_off // chunk_size,
                    "tile_col": col_off // chunk_size,
                    "bbox_min_x": float(min_x), "bbox_max_x": float(max_x),
                    "bbox_min_y": float(min_y), "bbox_max_y": float(max_y),
                    "epsg": crs
                })
        return tiles

# In production, convert tile list to a Spark DataFrame and write to Iceberg/Delta
```

**Trade-off:** Raster chunking increases object count but enables parallelized reads and spatial filtering at the tile level. Use Cloud-Optimized GeoTIFF (COG) standards for direct HTTP range requests when full lakehouse ingestion is unnecessary. For analytical workloads requiring vector-raster joins, store tile bounding boxes as vector metadata to enable predicate pushdown before fetching binary payloads.

## Index Maintenance & Synchronization

Unlike traditional RDBMS indexes that update synchronously, lakehouse spatial layouts require asynchronous maintenance. Compaction, Z-ordering, and manifest rewriting must run outside peak query windows.

**CI/CD Maintenance Pipeline (GitHub Actions):**
```yaml
name: Lakehouse Spatial Maintenance
on:
  schedule:
    - cron: '0 2 * * *' # Daily at 2 AM UTC
  workflow_dispatch:
jobs:
  optimize-spatial-tables:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Run Iceberg Compaction & Rewrite
        run: |
          spark-submit \
            --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.0 \
            --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
            --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
            --conf spark.sql.catalog.lakehouse.catalog-impl=org.apache.iceberg.rest.RESTCatalog \
            scripts/optimize_spatial_tables.py
```

**Trade-off:** Asynchronous maintenance introduces eventual consistency for spatial layouts. Queries immediately after bulk writes may experience degraded pruning until compaction completes. Implement read/write isolation via snapshot isolation or branch-based workflows to prevent query planners from reading partially optimized manifests.

## Query Planner Integration

The final performance layer depends on how compute engines interpret spatial metadata. Modern planners integrate with table catalogs to generate optimized execution trees. Ensure your engine version supports spatial predicate pushdown (e.g., Spark 3.4+, Trino 420+, DuckDB 1.0+).

Reference the official [Apache Iceberg Partitioning Documentation](https://iceberg.apache.org/docs/latest/partitioning/) to align your table specs with engine-specific optimizer capabilities. Misaligned partition transforms (e.g., using `bucket()` on unencoded geometries) will bypass the planner's pruning logic and force full scans.

## Skew, Hot Cells and the Long Tail of Density

Spatial data is never uniformly distributed, and every partitioning scheme inherits that non-uniformity. Population, traffic, sensors and buildings all cluster; a partition key that is geometrically regular produces partitions that are wildly irregular in size. This is the single most common reason a well-designed scheme underperforms in production.

<figure class="diagram">
<svg viewBox="0 0 752 300" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Bar chart contrasting partition row counts before and after splitting hot cells: three dense urban cells dominate the raw distribution, while subdividing them to a finer resolution flattens the profile across partitions">
<rect x="0" y="0" width="752" height="300" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Row counts per partition, before and after splitting hot cells</text>
<line x1="70" y1="230" x2="740" y2="230" stroke="#33707d" stroke-width="1.5"/>
<line x1="70" y1="60" x2="70" y2="230" stroke="#33707d" stroke-width="1.5"/>
<text x="52" y="66" text-anchor="end" font-family="sans-serif" font-size="11" fill="#33707d">40M</text>
<text x="52" y="150" text-anchor="end" font-family="sans-serif" font-size="11" fill="#33707d">20M</text>
<text x="52" y="234" text-anchor="end" font-family="sans-serif" font-size="11" fill="#33707d">0</text>
<rect x="92" y="66" width="30" height="164" fill="#9a5a17"/>
<rect x="146" y="104" width="30" height="126" fill="#9a5a17"/>
<rect x="200" y="146" width="30" height="84" fill="#9a5a17"/>
<rect x="254" y="200" width="30" height="30" fill="#9a5a17"/>
<rect x="308" y="209" width="30" height="21" fill="#9a5a17"/>
<rect x="362" y="215" width="30" height="15" fill="#9a5a17"/>
<text x="242" y="256" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#9a5a17">Uniform resolution: 3 cells hold 78% of rows</text>
<rect x="440" y="186" width="24" height="44" fill="#2f6e49"/>
<rect x="474" y="180" width="24" height="50" fill="#2f6e49"/>
<rect x="508" y="190" width="24" height="40" fill="#2f6e49"/>
<rect x="542" y="184" width="24" height="46" fill="#2f6e49"/>
<rect x="576" y="192" width="24" height="38" fill="#2f6e49"/>
<rect x="610" y="182" width="24" height="48" fill="#2f6e49"/>
<rect x="644" y="188" width="24" height="42" fill="#2f6e49"/>
<rect x="678" y="194" width="24" height="36" fill="#2f6e49"/>
<text x="571" y="256" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#2f6e49">Adaptive: dense cells split one level deeper</text>
<text x="390" y="284" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">Same data, same total bytes — only the assignment of rows to partitions changed</text>
</svg>
</figure>

The mitigation is **adaptive resolution**: instead of one global grid level, assign each cell the resolution at which its row count falls inside the target band. Build a small lookup table from a sample — cell identifier at a coarse level, chosen depth — persist it alongside the table, and have the write path consult it when deriving the partition column. Dense metropolitan cells subdivide two or three levels deeper than ocean cells, and the resulting partitions are within a factor of two of each other in size instead of a factor of a thousand.

Two implementation details make or break this. First, the lookup must be **versioned and stable**: changing a cell's depth changes where rows land, so a rebuild reshuffles history. Version the mapping, record which version wrote each file, and only re-derive during a planned rewrite. Second, the reader must be able to **expand a query window into the right cell identifiers at mixed depths**, which means the containment relation has to be computable — hierarchical grids where a child's identifier encodes its parent make this trivial, and flat grids make it painful. This is one of the concrete reasons to prefer a hierarchical system, as compared in [H3 vs S2 vs geohash for lakehouse partitioning](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/grid-system-selection/h3-vs-s2-vs-geohash-for-lakehouse-partitioning/).

A cheaper approximation, when adaptive resolution is too much machinery, is **salted splitting of known hot cells**: keep one global resolution, but for the handful of cells above the size threshold, append a bucket suffix derived from a hash of the row identifier. Queries must then expand a hot cell into its salted variants, which is a small planner-side inconvenience in exchange for removing the straggler. It works precisely because the hot set is small and stable — a dozen city cells, not a moving target.

Whichever route is taken, instrument it. Emit partition row counts and byte sizes as a table-level metric after each write, alert on any partition exceeding four times the median, and review quarterly. Density shifts: a new sensor deployment or a customer onboarding can turn a healthy cell into a straggler in a week, and the only cheap moment to notice is before the query latency graph does.

## Failure Modes and Operational Gotchas

- **Partition columns that are not in the predicate.** A table partitioned by `h3_r5` prunes nothing if the query filters on latitude and longitude alone. The derived column must be computed on the *query* side too, either by the user, a view, or a UDF the planner can constant-fold. Ship a view that does this so callers cannot forget.
- **Grid identifiers stored as strings.** Cell identifiers are 64-bit integers; storing them as hexadecimal strings inflates the column, defeats numeric range predicates, and makes min/max statistics useless because lexicographic order is not numeric order. Store `BIGINT`.
- **Sorting inside partitions that is never refreshed.** A sort order applied at table creation degrades as appends arrive out of order. Data skipping quietly declines over weeks. Re-sort as part of scheduled compaction, not once at setup.
- **Reprojection after partitioning.** Deriving a cell identifier from projected coordinates and then querying with geographic ones — or vice versa — produces silently empty results. Derive partition keys from a single, declared CRS and assert it in CI.
- **Antimeridian-crossing extents.** A query window that wraps ±180° expands into two disjoint ranges, and a naive implementation produces a window covering the whole planet. Split the window before expanding it into cells.
- **Partition evolution treated as free.** Changing a partition spec does not rewrite existing data; the table becomes a mixture of layouts, and queries must plan against both. This is a supported and useful capability, but it needs a follow-up rewrite job and an explicit end date for the old layout.
- **Too many small partitions from a compound key.** Day × cell multiplies cardinalities. A year of data over 2,000 cells is 730,000 partitions. Choose the coarser cell level for compound keys than you would for a spatial-only key.


## Verifying That Pruning Actually Happens

A partitioning scheme is a hypothesis about physical layout, and every hypothesis needs a measurement. The failure that costs the most is the one where the design is correct on paper, an implementation detail silently disables it, and nobody notices for a quarter because the queries still return the right answers — just slowly and expensively.

Three measurements settle it. The first is **files scanned against files in the table**. Every engine exposes this: Spark reports `numFiles` and `filesPruned` in the scan node, Trino shows input rows and split counts per operator, DuckDB's `EXPLAIN ANALYZE` reports rows scanned per Parquet reader. Run a query whose answer lives in one partition and confirm the file count is single-digit. If it equals the table's file count, pruning is off, and the cause is almost always that the predicate does not mention the partition column.

The second is **bytes read against bytes returned**. This catches the subtler failure where partition pruning works but within-file skipping does not, because the files are unsorted or the statistics are missing. A selective query that reads 400 GB to return 200 MB is skipping nothing inside its files. Check the Parquet footers directly — statistics are absent for any column beyond the writer's statistics limit, and absent statistics are indistinguishable from unhelpful ones at the query layer.

The third is **planning time against execution time**. When planning exceeds a second on a selective query, the partition count is too high, and the fix is a coarser key rather than a faster engine. This measurement is the one that catches over-partitioning before it becomes an incident, because over-partitioned tables look excellent on the first two metrics — they prune beautifully, they just take longer to decide what to prune than to read it.

Automate all three. A nightly job that runs a fixed set of representative queries and records these ratios into a small metrics table gives a trend line, and the trend is what matters: pruning ratios degrade gradually as unsorted appends accumulate, and a graph makes the moment to re-run compaction obvious. The mechanics of scheduling that work are covered in [lakehouse maintenance automation](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/lakehouse-maintenance-automation/), and the engine-specific interpretation of the plan output in [how predicate pushdown reduces GIS query latency](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/predicate-pushdown-optimization/how-predicate-pushdown-reduces-gis-query-latency/).

One caution on benchmarking: warm object-storage caches and warm engine metadata caches will flatter any layout. Measure with cold caches, or at least measure both, and compare against the same query on the same day rather than against a number recorded before the last schema change.


### A Minimal Pruning Regression Test

Encode the expectation as an assertion rather than a dashboard. The check below runs against any Iceberg or Delta table from a Spark session, fails the build when a known-selective query stops pruning, and takes seconds.

```python
# Spark 3.5 + Iceberg 1.4. Fails if a city-scoped query touches more than 2% of files.
plan = spark.sql("""
    SELECT count(*) FROM lakehouse.spatial.telemetry
    WHERE h3_r5 IN (599686042433355775, 599686042697596927)
      AND event_day = DATE '2024-03-11'
""").queryExecution.executedPlan.toString()

scanned = int(plan.split("numFiles=")[1].split(",")[0].strip(") "))
total = spark.sql(
    "SELECT count(*) AS n FROM lakehouse.spatial.telemetry.files"
).collect()[0]["n"]

assert scanned / total < 0.02, f"pruning regressed: {scanned}/{total} files scanned"
```

Wire it into the same pipeline that validates schemas so a partition-spec change, a lost sort order or a dropped statistics column fails a pull request rather than a customer query.


## Operational Summary

Spatial performance in a lakehouse is engineered, not inherited. Success requires:
1. **Hierarchical partitioning** capped at fewer than 10,000 active partitions.
2. **Coordinate-aware clustering** (Z-order/Hilbert) applied selectively to read-heavy tables.
3. **Explicit bounding box columns** typed as `DOUBLE` so manifests capture accurate min/max statistics.
4. **Asynchronous maintenance** pipelines that decouple writes from optimization.
5. **Engine-aware planner integration** to ensure spatial predicates translate to file-level filters.

By treating spatial layout as a first-class infrastructure concern, teams can achieve sub-second query latency at petabyte scale without sacrificing open-format interoperability or operational reliability.

Read the layout decisions on this page as a single system rather than a menu. The partition key decides which files a query opens, the sort order decides how much of each file it reads, the derived bounding-box columns decide whether it can make that decision without decoding geometry, and the maintenance schedule decides whether all three keep working next quarter. Any one of them implemented alone gives a fraction of the benefit and often none at all, which is why layouts that look correct in a design document so frequently disappoint in production: the missing piece is rarely the clever one.
