# Spatial Partitioning Schemes

Geospatial workloads in modern data lakehouses demand partitioning strategies that respect topological adjacency rather than lexical ordering. Traditional range or hash partitioning on coordinate columns produces severe data skew, triggers the small-files problem, and degrades join performance. Within the broader [Spatial Partitioning & Indexing Strategies](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/) framework, spatial partitioning schemes translate geographic primitives into deterministic storage layouts, enabling directory pruning, metadata efficiency, and predictable query latency. This guide details production-grade configuration, format-specific trade-offs, and operational maintenance for deploying spatial partitioning across Delta Lake and Apache Iceberg.

## Architectural Foundations

Spatial partitioning maps geometries to discrete storage units using hierarchical grids or space-filling curves. The choice of scheme directly impacts I/O patterns, transaction log overhead, and compaction frequency.

| Scheme | Topology | Best Use Case | Lakehouse Fit |
|--------|----------|---------------|---------------|
| **H3 / S2 Grids** | Hexagonal / Quadtree | Global telemetry, IoT trajectories, regional aggregations | Delta (directory-based), Iceberg (partition transforms) |
| **Z-Order / Hilbert** | Space-filling curve | Bounding-box scans, KNN, multi-dimensional clustering | In-file clustering (requires compaction) |
| **QuadTree / R-Tree** | Hierarchical bounding boxes | High-precision cadastral parcel joins | External index files (not native to Parquet-backed formats) |

Delta Lake materializes partitions as physical directories. Over-partitioning at fine grid resolutions (e.g., H3 resolution 10+) generates millions of directories, inflating `LIST` operations and fragmenting the `_delta_log`. Apache Iceberg abstracts partitioning via transform functions, storing partition metadata in manifests rather than directories, which scales better to high cardinality but requires careful spec versioning.

## Operational Configuration & Parameters

Production deployments must enforce explicit spatial contracts before ingestion begins.

### Coordinate Reference System (CRS) & Bounds
Always standardize to **EPSG:4326 (WGS84)** at ingestion. Projected CRS values (e.g., EPSG:3857) introduce distortion that breaks spatial partition boundaries unless all downstream queries also use the projected CRS. Define explicit global or regional bounds to prevent null-partition drift:
- **Global:** `[-180.0, -90.0, 180.0, 90.0]`
- **Regional (CONUS):** `[-125.0, 24.5, -66.9, 49.3]`

### Partition Resolution & File Sizing
Target **128MB–1GB** uncompressed Parquet files post-compaction. For H3, resolution `7` (~5 km² cells) typically yields optimal cardinality for continental-scale datasets. Higher resolutions (8–10) should only be used when query predicates consistently filter to sub-kilometer extents.

### Retention Policies
Enforce time-based retention alongside spatial partitions to prevent metadata bloat:
```sql
ALTER TABLE geospatial.events SET TBLPROPERTIES (
  'delta.logRetentionDuration' = 'interval 30 days',
  'delta.deletedFileRetentionDuration' = 'interval 7 days'
);
```

## Production Implementation Patterns

### PySpark: H3 Partitioning in Delta Lake

The following pipeline ingests WGS84 trajectories, computes H3 cells, and writes partitioned Delta tables with explicit schema enforcement. It uses the `h3` Python library (h3-py), which in version 4.x exposes functions directly at the module level.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType
import h3  # h3-py 4.x: use h3.latlng_to_cell()

spark = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

schema = StructType([
    StructField("device_id", StringType(), False),
    StructField("lat", DoubleType(), False),
    StructField("lon", DoubleType(), False),
    StructField("ts", TimestampType(), False)
])

# Register H3 UDF — h3-py 4.x API
@udf(StringType())
def h3_from_latlng(lat: float, lon: float) -> str:
    return h3.latlng_to_cell(lat, lon, 7)

# Ingest raw telemetry
df = spark.read.schema(schema).json("s3://raw-telemetry/2024-10/")

# Compute H3 partition key (resolution 7) and enforce CRS bounds
df_spatial = df.filter(
    (col("lat") >= -90.0) & (col("lat") <= 90.0) &
    (col("lon") >= -180.0) & (col("lon") <= 180.0)
).withColumn("h3_res7", h3_from_latlng(col("lat"), col("lon")))

df_spatial.write \
    .format("delta") \
    .partitionBy("h3_res7") \
    .option("mergeSchema", "false") \
    .mode("append") \
    .save("s3://lakehouse/geospatial/events")
```

For a complete breakdown of directory pruning behavior and transaction log optimization, review [Implementing H3 hexagon partitioning in Delta Lake](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/spatial-partitioning-schemes/implementing-h3-hexagon-partitioning-in-delta-lake/).

### Spark SQL: Apache Iceberg Partition Transforms

Iceberg handles spatial partitioning via metadata transforms, avoiding directory proliferation. Use `bucket` transforms alongside spatial UDFs.

```sql
CREATE TABLE iceberg.geospatial.events (
    device_id STRING,
    lat       DOUBLE,
    lon       DOUBLE,
    ts        TIMESTAMP,
    h3_res7   STRING
) USING iceberg
PARTITIONED BY (
    bucket(128, h3_res7),
    days(ts)
)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.metadata.previous-versions-max' = '10'
);

-- H3 function must be registered in the session (e.g., via Apache Sedona or a custom UDF)
INSERT INTO iceberg.geospatial.events
SELECT
    device_id, lat, lon, ts,
    h3_latlng_to_cell(lat, lon, 7) AS h3_res7
FROM raw.telemetry_stream
WHERE ts >= '2024-01-01';
```

## Clustering & Query Optimization

Partitioning alone cannot resolve intra-file spatial locality. Pair coarse partitions with in-file clustering to minimize row-group scans. Applying [Z-Ordering for Geospatial Queries](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/z-ordering-for-geospatial-queries/) interleaves longitude and latitude bits, ensuring adjacent coordinates share Parquet row groups. This requires scheduled `OPTIMIZE` or `rewrite_data_files` jobs to maintain clustering efficiency as data ages.

For Iceberg, enforce sort order during writes:
```sql
-- Re-cluster with an explicit sort via stored procedure
CALL spark_catalog.system.rewrite_data_files(
  table => 'iceberg.geospatial.events',
  strategy => 'sort',
  sort_order => 'h3_res7 ASC, lon ASC, lat ASC'
);
```

## Maintenance, Compaction & Troubleshooting

### Manifest Bloat & Checkpoint Fragmentation
Monitor partition cardinality weekly. If `h3_res7` cardinality exceeds 50,000 unique values per day, consider:
1. Upgrading to Iceberg v2 partition specs with `bucket()` to cap cardinality
2. Implementing dynamic partition pruning via query predicates
3. Running daily compaction to merge small files (`delta.autoOptimize.optimizeWrite = true`)

### Predicate Pushdown Debugging
Spatial predicates fail to prune when column statistics lack envelope bounds. Ensure your query engine leverages [Predicate Pushdown Optimization](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/predicate-pushdown-optimization/) by exposing min/max coordinates in Parquet metadata. In Spark, validate pushdown with:

```sql
EXPLAIN EXTENDED
SELECT * FROM geospatial.events
WHERE h3_res7 = '872830828ffffff'
  AND lon BETWEEN -120 AND -115
  AND lat BETWEEN 35 AND 40;
```

If the plan shows `AlwaysTrue` or `Filter` without `PartitionFilters`, verify:
- `spark.sql.parquet.filterPushdown=true`
- H3 column is typed as `STRING` (not `BINARY`)
- Null-handling is explicit in spatial UDFs
- GeoParquet metadata aligns with [OGC GeoParquet Specification](https://geoparquet.org/)

### CI/CD Validation Pipeline
Enforce spatial contracts before merging pipeline changes:

```yaml
name: spatial-partition-validation
on: [pull_request]
jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Run Partition Cardinality Check
        run: |
          pip install pyspark delta-spark
          python -c "
          from pyspark.sql import SparkSession
          spark = SparkSession.builder.getOrCreate()
          result = spark.sql('''
            SELECT
              COUNT(DISTINCT h3_res7) AS unique_cells,
              COUNT(*)                AS total_rows
            FROM geospatial.events
            WHERE ts >= current_date() - INTERVAL 1 DAY
          ''').collect()[0]
          assert result['unique_cells'] <= 50000, \
              f'Partition cardinality too high: {result[\"unique_cells\"]}'
          print(f'Cardinality check passed: {result[\"unique_cells\"]} unique cells')
          "
```

## Operational Checklist
- [ ] Standardize all inputs to EPSG:4326 before partition computation
- [ ] Cap grid resolution to prevent directory/manifest explosion
- [ ] Schedule weekly compaction and Z-order rewrites
- [ ] Validate spatial UDF determinism and null handling
- [ ] Monitor `spark.sql.files.maxPartitionBytes` and adjust to 128MB–1GB targets
- [ ] Enforce schema contracts via CI/CD cardinality thresholds

Adhering to these patterns ensures spatial partitioning scales predictably across petabyte-scale lakehouses, minimizing I/O overhead while maintaining strict query latency SLAs.

## Comparing the Four Schemes Side by Side

Every spatial partitioning scheme in production use is one of four shapes, and the differences between them are easiest to see when they are put against the same criteria rather than described in isolation.

<figure class="diagram">
<svg viewBox="0 0 766 312" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Four partitioning schemes compared on uniformity of partition size, quality of locality, planner friendliness and readability: administrative regions, fixed degree grid, hierarchical cell grid, and a compound time plus cell key">
<rect x="0" y="0" width="766" height="312" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Four schemes, four criteria</text>
<rect x="26" y="52" width="180" height="34" rx="6" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="1.5"/>
<text x="116" y="75" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">scheme</text>
<rect x="212" y="52" width="130" height="34" rx="6" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="1.5"/>
<text x="277" y="75" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">uniformity</text>
<rect x="348" y="52" width="130" height="34" rx="6" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="1.5"/>
<text x="413" y="75" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">locality</text>
<rect x="484" y="52" width="130" height="34" rx="6" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="1.5"/>
<text x="549" y="75" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">pushdown</text>
<rect x="620" y="52" width="134" height="34" rx="6" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="1.5"/>
<text x="687" y="75" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">readability</text>
<text x="36" y="118" font-family="sans-serif" font-size="12" fill="#0d3b45">administrative code</text>
<text x="277" y="118" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#9a5a17">poor</text>
<text x="413" y="118" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#2f6e49">good</text>
<text x="549" y="118" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#2f6e49">exact</text>
<text x="687" y="118" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#2f6e49">excellent</text>
<line x1="26" y1="132" x2="754" y2="132" stroke="#cfe3e7" stroke-width="1.5"/>
<text x="36" y="162" font-family="sans-serif" font-size="12" fill="#0d3b45">fixed degree grid</text>
<text x="277" y="162" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#9a5a17">poor at latitude</text>
<text x="413" y="162" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#2f6e49">good</text>
<text x="549" y="162" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#2f6e49">simple ranges</text>
<text x="687" y="162" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#2f6e49">good</text>
<line x1="26" y1="176" x2="754" y2="176" stroke="#cfe3e7" stroke-width="1.5"/>
<text x="36" y="206" font-family="sans-serif" font-size="12" fill="#0d3b45">hierarchical cells</text>
<text x="277" y="206" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#2f6e49">good</text>
<text x="413" y="206" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#2f6e49">very good</text>
<text x="549" y="206" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#2f6e49">IN list of cells</text>
<text x="687" y="206" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#9a5a17">opaque ids</text>
<line x1="26" y1="220" x2="754" y2="220" stroke="#cfe3e7" stroke-width="1.5"/>
<text x="36" y="250" font-family="sans-serif" font-size="12" fill="#0d3b45">day + coarse cell</text>
<text x="277" y="250" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#2f6e49">very good</text>
<text x="413" y="250" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0e6e7d">good within a day</text>
<text x="549" y="250" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#2f6e49">two-level pruning</text>
<text x="687" y="250" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0e6e7d">mixed</text>
<text x="390" y="296" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">No row wins outright — the workload decides which column matters most</text>
</svg>
</figure>

Uniformity is the criterion teams underweight and later regret. An administrative scheme is beautifully readable and catastrophically skewed: for most national datasets, one or two regions hold a third of the rows, and every job that touches the table waits on the task processing them. A fixed degree grid looks uniform on a map projection and is not uniform on the ground, because a one-degree cell at 60° latitude covers half the area of one at the equator — which produces a partition-size gradient that follows population density in exactly the wrong direction for European and North American datasets.

Readability matters more than it appears in a table like this, because it determines whether anybody writes the right query. A partition key of `DE-BY` is self-explanatory; a key of `599686042433355775` is not, and a caller who cannot construct it will filter on latitude and longitude instead and read the whole table. Whenever the key is opaque, ship a view or a helper function alongside the table so that the fast query is also the easy one.

The last row — a compound key of day and a coarse cell — is the shape most production telemetry tables converge on, and its "good within a day" locality is worth reading carefully. Spatial locality only applies inside a day's partition, so a query spanning a year and a small area reads 365 partitions. That is usually correct behaviour, because the alternative orderings make the far more common recent-data query worse.

## Deriving the Partition Column Once, Correctly

Whatever scheme is chosen, the partition value has to be computed at write time, and the way that computation is implemented decides whether the table stays consistent over years of pipeline changes.

<figure class="diagram">
<svg viewBox="0 0 764 234" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Two implementations of partition key derivation: scattered inline computation in each pipeline which drifts, versus a single shared function used by every writer and by the query helper view">
<defs>
<marker id="part-derive-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#2f6e49"/></marker>
</defs>
<rect x="0" y="0" width="764" height="234" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">One derivation, many callers</text>
<rect x="30" y="60" width="150" height="46" rx="6" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<text x="105" y="88" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">batch ingest</text>
<rect x="30" y="118" width="150" height="46" rx="6" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<text x="105" y="146" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">streaming ingest</text>
<rect x="30" y="176" width="150" height="46" rx="6" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<text x="105" y="204" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">backfill job</text>
<rect x="278" y="112" width="216" height="60" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2.5"/>
<text x="386" y="138" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">cell_for(lon, lat, res)</text>
<text x="386" y="158" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">one versioned implementation</text>
<line x1="180" y1="83" x2="278" y2="128" stroke="#2f6e49" stroke-width="2" marker-end="url(#part-derive-arrow)"/>
<line x1="180" y1="141" x2="278" y2="141" stroke="#2f6e49" stroke-width="2" marker-end="url(#part-derive-arrow)"/>
<line x1="180" y1="199" x2="278" y2="156" stroke="#2f6e49" stroke-width="2" marker-end="url(#part-derive-arrow)"/>
<rect x="592" y="60" width="160" height="46" rx="6" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="672" y="88" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">the table</text>
<rect x="592" y="118" width="160" height="46" rx="6" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="672" y="146" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">query helper view</text>
<rect x="592" y="176" width="160" height="46" rx="6" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="672" y="204" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">validation tests</text>
<line x1="494" y1="128" x2="592" y2="83" stroke="#0e6e7d" stroke-width="2" marker-end="url(#part-derive-arrow)"/>
<line x1="494" y1="141" x2="592" y2="141" stroke="#0e6e7d" stroke-width="2" marker-end="url(#part-derive-arrow)"/>
<line x1="494" y1="156" x2="592" y2="199" stroke="#0e6e7d" stroke-width="2" marker-end="url(#part-derive-arrow)"/>
</svg>
</figure>

The failure this prevents is subtle and common: two pipelines write the same table using the same nominal scheme but different library versions, different rounding at cell boundaries, or one that reprojects first and one that does not. The data looks fine. Queries return plausible results. And a small percentage of rows sit in the wrong partition, so a query scoped to a cell silently misses them — which is exactly the kind of defect that is discovered by a customer rather than by a test.

Publishing the derivation as a single versioned function, used by every writer and by the helper view that callers query through, makes the property enforceable. Add a test that asserts a fixed set of coordinates maps to a fixed set of cell identifiers, and the function becomes safe to upgrade: a library change that alters any of those mappings fails the test rather than quietly resharding the table.

## Sizing, Skew and the Rewrite Budget

A scheme that is correct can still be wrong at a given scale, and the arithmetic that decides it is short enough to do before writing any data.

<figure class="diagram">
<svg viewBox="0 0 764 210" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three sizing checks for a candidate partition scheme: average bytes per partition against the target band, total partition count against the planning budget, and the ratio of the largest partition to the median as a skew measure">
<rect x="0" y="0" width="764" height="210" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Three numbers to compute before writing anything</text>
<rect x="26" y="58" width="230" height="140" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="141" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">bytes / partition</text>
<text x="141" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">table size ÷ partition count</text>
<text x="141" y="140" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#2f6e49">target 128 MB – 1 GB</text>
<text x="141" y="170" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">below 32 MB means</text>
<text x="141" y="186" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">a small-file problem</text>
<rect x="274" y="58" width="230" height="140" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="389" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">partition count</text>
<text x="389" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">distinct values, all dimensions</text>
<text x="389" y="140" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#2f6e49">keep under ~10,000</text>
<text x="389" y="170" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">planning cost grows</text>
<text x="389" y="186" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">linearly beyond that</text>
<rect x="522" y="58" width="230" height="140" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="637" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">skew ratio</text>
<text x="637" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">largest ÷ median partition</text>
<text x="637" y="140" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#2f6e49">aim below 4×</text>
<text x="637" y="170" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">above 10× produces</text>
<text x="637" y="186" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">a permanent straggler</text>
</svg>
</figure>

All three are computable from a one-percent sample, which makes candidate evaluation cheap: derive the key on the sample, group, and read the numbers off. A scheme that fails any of the three at sample scale will fail worse at full scale, and no engine configuration will rescue it.

The fourth number, which cannot be sampled, is the **rewrite budget**: how long a full re-partition of the table would take, and whether the platform can afford to run it. That number determines how reversible the decision is. A 2 TB table rewrites in under an hour on modest compute, so getting the scheme slightly wrong is a nuisance. A 900 TB table does not rewrite at all in practice, so the initial choice is close to permanent and deserves proportionally more analysis — and it is an argument for choosing a format whose partition specification can evolve without rewriting history.

## Evolving a Scheme on a Table That Is Already Large

The partitioning decision that survives contact with production is rarely the first one, and the interesting engineering is in changing it without a maintenance window.

The first thing to establish is whether the format allows a mixed layout. Where the partition specification is recorded per file rather than per table, a change is a metadata operation: new writes use the new specification, old files keep the old one, and the planner evaluates both. Queries continue to work throughout, with slightly worse pruning on the historical portion until it is rewritten. That is a genuinely incremental migration, and it can be spread over weeks with a rewrite job that processes one month of history per night.

Where the layout is fixed at table creation, the migration is a copy: a new table with the new scheme, a backfill, a period of dual writing, a cutover of readers, and a drop. Each step is straightforward and the whole sequence takes longer than anyone estimates, mostly because the reader cutover requires finding every consumer. Budget for the discovery rather than for the copy — a query-history sweep by table name is the fastest way to enumerate them, and it will always turn up two or three nobody remembered.

Either way, **write the target layout down as a specification before starting**, including the exact derivation, the resolution, the expected partition count and the expected skew ratio at the current data volume. Migrations that begin without those numbers tend to be judged by whether they completed rather than by whether they improved anything, and it is entirely possible to spend a fortnight moving a table into a scheme that is worse.

Two practical cautions apply to the transition period itself. Compaction jobs must be aware that files exist under both layouts, because a naive rewrite that reads a mixed set and writes under one specification will silently reshard data the migration plan expected to leave alone. And monitoring thresholds calibrated on the old layout will fire spuriously against the new one — partition counts change by an order of magnitude, so alerts tied to absolute numbers need updating as part of the change rather than afterwards.

## Documenting the Scheme Where Callers Will See It

The last step of implementing a partitioning scheme is telling people about it, and the place to do that is the table, not a wiki page.

A table comment that names the partition columns, the derivation and the resolution turns the fastest query into the obvious one. Something as short as "partitioned by event_day then h3_r5 (resolution 5); filter both for pruning; use v_telemetry_geo to derive cells from a bounding box" appears in every catalogue browser, every `DESCRIBE` output and every schema panel in a BI tool. It reaches a caller at the moment they are writing the query, which no amount of documentation elsewhere achieves.

The complementary artefact is a helper view that hides the derivation entirely, so that a caller who wants "everything within this bounding box yesterday" writes exactly that and the view supplies the cell list. Where the engine supports it, a table-valued function is better still, because it can accept the bounding box as a parameter rather than requiring the caller to know the internal column names.

Finally, publish the numbers. A short, regularly refreshed table showing partition count, median partition size, skew ratio and the pruning ratio observed on the standard query set makes the health of the layout visible to whoever is deciding whether to add another dimension to the key. Schemes degrade quietly as data grows and distributions shift, and a visible metric is the only thing that reliably prompts the review before the latency graph does.

## A Short Checklist Before Committing

Run through these before the first production write, because every one of them is cheap now and expensive after a petabyte has landed.

- [ ] The partition key appears in the predicate of every representative query, either directly or through a helper view
- [ ] Average partition size lands between 128 MB and 1 GB on a realistic volume projection, not on today's volume
- [ ] Total partition count stays under ten thousand across the whole retention window, including the time dimension
- [ ] Skew ratio between the largest and median partition is under four on a one-percent sample
- [ ] The derivation function is versioned, shared by every writer, and covered by a fixed-coordinate test
- [ ] Cell identifiers are stored as integers, never as strings
- [ ] A rewrite of the full table has a measured duration and fits inside an acceptable maintenance budget
- [ ] The table comment names the partition columns and the derivation so callers can find them
- [ ] Partition metrics — count, size distribution, skew — are emitted after each write and reviewed on a schedule

The list is short because the failures are repetitive. Almost every disappointing spatial table in production fails at least two of these, and usually the same two: the key is absent from the query predicate, and nobody computed the partition count before choosing the resolution.
