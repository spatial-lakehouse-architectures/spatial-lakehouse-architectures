# Predicate Pushdown Optimization

Predicate pushdown is the foundational execution strategy that shifts filtering logic from distributed compute engines down to the object storage metadata layer. In spatial data lakehouses built on Apache Iceberg or Delta Lake, this mechanism determines whether a geospatial workload scans terabytes of raw Parquet files or reads only the relevant spatial partitions. As a core execution mechanism within the broader [Spatial Partitioning & Indexing Strategies](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/) framework, pushdown optimization directly dictates query latency, compute cost, and concurrency limits for GIS analytics. Without it, spatial predicates degrade into full-table scans, negating the economic and performance advantages of the lakehouse paradigm.

## Core Mechanics and Spatial Metadata Translation

Spatial predicates (`ST_Intersects`, `ST_Contains`, `ST_DWithin`) introduce execution complexity because geometry types are typically serialized as binary WKB or custom spatial encodings. Traditional relational pushdown relies on scalar column statistics, but spatial engines must translate geometric bounds into file-level metadata filters. When a query specifies a geographic bounding box, the execution planner evaluates min/max coordinate statistics stored in table manifests. If a file's spatial envelope does not overlap the query window, the file is skipped entirely before deserialization.

This early pruning requires strict coordinate reference system (CRS) alignment. Engines evaluate pushdown against the stored CRS (typically `EPSG:4326` for lat/lon). Mismatched CRS values between query predicates and table metadata force full deserialization and runtime reprojection, bypassing pushdown entirely. The planner also handles topology-aware predicates by approximating complex geometries with their minimum bounding rectangles (MBRs) during the initial filter pass, deferring exact geometric computation to the post-filter stage. For authoritative specifications on spatial predicate evaluation and WKB serialization, consult the [OGC Simple Features Access Standard](https://www.ogc.org/publications/standard/sfa/).

## Format-Specific Execution: Iceberg vs. Delta Lake

The effectiveness of spatial pushdown depends heavily on the underlying table format's metadata architecture and data skipping capabilities.

**Apache Iceberg** leverages a hierarchical manifest system where each manifest file stores per-file statistics, including custom spatial bounds when configured via auxiliary metadata columns. Iceberg's hidden partitioning allows engineers to partition by upstream-computed spatial keys (e.g., geohash strings or H3 cell IDs) without exposing those columns in the query schema. The engine evaluates predicates directly against these manifests, enabling aggressive file skipping. Configuration requires `spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions` and careful handling of null geometries. Trade-off: Iceberg's manifest pruning is highly efficient but requires proactive metadata maintenance; stale manifests lead to degraded skip rates.

**Delta Lake** relies on the `_delta_log` and built-in data skipping indexes that automatically track min/max/null counts for all columns. Spatial pushdown in Delta is most effective when combined with multi-dimensional clustering. While Delta does not natively support hidden spatial partition transforms, it compensates through `ZORDER BY` on derived spatial columns (e.g., `bbox_min_x`, `bbox_min_y`). This approach co-locates spatially adjacent records within Parquet row groups, allowing the engine to skip entire blocks during predicate evaluation. For implementation details on Delta's skipping architecture, refer to [Delta Lake Data Skipping Documentation](https://docs.delta.io/latest/delta-batch.html#data-skipping).

When designing partition layouts, engineers must balance granularity with metadata overhead. Over-partitioning creates excessive manifest files, increasing planner latency and diminishing pushdown returns. A comprehensive breakdown of optimal partition boundaries and clustering dimensions is available in the [Spatial Partitioning Schemes](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/spatial-partitioning-schemes/) guide. Additionally, applying [Z-Ordering for Geospatial Queries](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/z-ordering-for-geospatial-queries/) alongside pushdown ensures that spatial predicates hit contiguous Parquet blocks rather than scattered files.

## Production Implementation: Configuration & Validation

The following examples demonstrate production-ready configuration, query execution, and CI validation for spatial predicate pushdown.

### PySpark Configuration & Table Creation
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

spark = SparkSession.builder \
    .appName("SpatialPushdownConfig") \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.parquet.filterPushdown", "true") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Explicit spatial parameters
CRS = "EPSG:4326"
RETENTION_DAYS = 90

# Create Iceberg table with bounding box columns for predicate pushdown
spark.sql("""
    CREATE TABLE IF NOT EXISTS analytics.spatial_events (
        event_id  STRING,
        timestamp TIMESTAMP,
        geometry  BINARY,
        x_min     DOUBLE,
        y_min     DOUBLE,
        x_max     DOUBLE,
        y_max     DOUBLE
    ) USING iceberg
    PARTITIONED BY (bucket(16, event_id))
    LOCATION 's3://lakehouse-prod/analytics/spatial_events'
""")

# Write with explicit spatial bounds extraction
df = spark.read.parquet("s3://ingest-raw/events/")
df.withColumn("x_min", expr("ST_XMin(ST_GeomFromWKB(geometry))")) \
  .withColumn("y_min", expr("ST_YMin(ST_GeomFromWKB(geometry))")) \
  .withColumn("x_max", expr("ST_XMax(ST_GeomFromWKB(geometry))")) \
  .withColumn("y_max", expr("ST_YMax(ST_GeomFromWKB(geometry))")) \
  .writeTo("analytics.spatial_events") \
  .append()
```

### SQL Query with Pushdown Verification
```sql
-- Verify pushdown via EXPLAIN before execution.
-- The bbox filter pushes down to manifest; ST_Intersects runs post-filter.
EXPLAIN FORMATTED
SELECT event_id, timestamp, geometry
FROM analytics.spatial_events
WHERE x_min >= -122.5 AND x_max <= -122.0
  AND y_min >= 37.5  AND y_max <= 38.0
  AND ST_Intersects(
        ST_GeomFromWKB(geometry),
        ST_GeomFromText('POLYGON((-122.5 37.5, -122.0 37.5, -122.0 38.0, -122.5 38.0, -122.5 37.5))', 4326)
      );
```

### CI/CD Validation Pipeline (GitHub Actions)
```yaml
name: Validate Spatial Pushdown
on:
  push:
    branches: [main]
jobs:
  validate-pushdown:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Run Pushdown Validation
        run: |
          pip install pyspark pyiceberg
          python -c "
          from pyspark.sql import SparkSession
          spark = SparkSession.builder \
              .config('spark.sql.extensions',
                      'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions') \
              .getOrCreate()
          plan = spark.sql(
              'EXPLAIN FORMATTED SELECT * FROM analytics.spatial_events '
              'WHERE x_min >= -122.5 AND x_max <= -122.0'
          ).collect()[0][0]
          assert 'PushedFilters' in plan, 'Predicate pushdown failed'
          print('Pushdown validation passed.')
          "
```

## Troubleshooting & Operational Guardrails

Even with correct configuration, spatial pushdown can silently degrade. Use the following diagnostic paths to maintain production reliability:

1. **Stale Manifest Statistics:** Iceberg and Delta rely on accurate min/max bounds. If `rewrite_data_files` or `OPTIMIZE` jobs run infrequently, file-level stats drift from actual geometry distributions. Schedule metadata compaction daily and monitor `num_files_skipped` metrics in query execution logs.
2. **Null Geometry Handling:** Pushdown engines treat `NULL` bounds conservatively. If a table contains unprocessed or malformed geometries, the planner may bypass skipping to avoid false negatives. Implement strict ingestion validation and route invalid records to a quarantine table before spatial column extraction.
3. **CRS Mismatch & Unit Drift:** Queries using metric predicates (`ST_DWithin` in meters) against lat/lon tables (`EPSG:4326`) bypass pushdown because the engine cannot safely convert units at the metadata layer. Standardize all stored bounds to a single CRS and perform unit conversions post-filter.
4. **Over-Partitioning & Small Files:** Partitioning by high-cardinality spatial hashes (e.g., Geohash at level 12+) creates thousands of tiny Parquet files. The planner spends more time reading manifests than skipping data. Enforce a minimum file size of 128MB and use `VACUUM` / `expire_snapshots` to prevent metadata bloat.
5. **Query Plan Inspection:** Always run `EXPLAIN FORMATTED` before deploying spatial workloads. Look for `PushedFilters` containing `x_min`, `y_min`, or bounding box predicates. If the plan shows `Filter` at the `Project` or `Scan` level without storage-layer predicates, pushdown is disabled or unsupported for that predicate type.

For detailed metrics on how early pruning impacts end-to-end GIS query execution, review [How predicate pushdown reduces GIS query latency](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/predicate-pushdown-optimization/how-predicate-pushdown-reduces-gis-query-latency/).

## The Four Places a Predicate Can Be Evaluated

"Pushdown" describes a direction, not a destination. A spatial predicate can be resolved at four distinct layers, and the difference in cost between the outermost and innermost is several orders of magnitude.

<figure class="diagram">
<svg viewBox="0 0 722 320" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Four evaluation layers for a spatial predicate, from partition pruning in the catalog through file statistics and row group statistics to full geometry evaluation in the engine, with the data volume touched at each layer">
<defs>
<marker id="pp-layer-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#2f6e49"/></marker>
</defs>
<rect x="0" y="0" width="722" height="320" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Where the filter runs decides what it costs</text>
<rect x="70" y="56" width="640" height="54" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="390" y="78" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">1. partition pruning — in the catalog, no storage touched</text>
<text x="390" y="98" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">cost: microseconds · eliminates: 90–99% of files</text>
<rect x="118" y="122" width="544" height="54" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="390" y="144" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">2. file statistics — manifest or transaction log</text>
<text x="390" y="164" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">cost: milliseconds · eliminates: most of what survived</text>
<rect x="166" y="188" width="448" height="54" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="390" y="210" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">3. row-group statistics — inside each opened file</text>
<text x="390" y="230" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">cost: one footer read per file · needs the file to be sorted</text>
<rect x="214" y="254" width="352" height="54" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="390" y="276" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">4. exact geometry predicate — in the engine</text>
<text x="390" y="296" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">cost: per surviving row · the only layer that is exact</text>
<line x1="390" y1="110" x2="390" y2="122" stroke="#2f6e49" stroke-width="2" marker-end="url(#pp-layer-arrow)"/>
<line x1="390" y1="176" x2="390" y2="188" stroke="#2f6e49" stroke-width="2" marker-end="url(#pp-layer-arrow)"/>
<line x1="390" y1="242" x2="390" y2="254" stroke="#2f6e49" stroke-width="2" marker-end="url(#pp-layer-arrow)"/>
</svg>
</figure>

Layers one through three are all **approximate and cheap**: they can only prove that data cannot match, never that it does. Layer four is exact and expensive. A well-tuned spatial query is one where the first three layers have reduced the candidate set to something small enough that the fourth layer's cost is irrelevant, and every optimisation on this page is ultimately about feeding the first three layers information they can use.

The critical property is that each layer needs a **numeric** predicate. None of them can evaluate a geometry function. `ST_Intersects(geom, :window)` is opaque to all three cheap layers, so a query expressing only that predicate skips straight to layer four and reads the entire table. Adding `AND bbox_min_x <= :maxx AND bbox_max_x >= :minx AND bbox_min_y <= :maxy AND bbox_max_y >= :miny` gives every cheap layer something to work with, and the geometry test then runs on the survivors. The two predicates together are logically redundant and operationally essential.

## Writing the Redundant Predicate Without Burdening Callers

Requiring every caller to write four extra comparisons is a policy that fails within a month, because someone will forget, and their query will be correct and slow rather than incorrect and loud.

<figure class="diagram">
<svg viewBox="0 0 764 210" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three ways to supply the numeric bounding box predicate on the caller's behalf: a view that accepts a window, a table valued function, and a client library that rewrites the query before submission">
<rect x="0" y="0" width="764" height="210" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Three ways to make the fast query the easy one</text>
<rect x="26" y="58" width="230" height="140" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="141" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">a view</text>
<text x="141" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">derives bbox columns from</text>
<text x="141" y="128" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">the geometry it exposes</text>
<text x="141" y="158" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">works everywhere</text>
<text x="141" y="178" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">caller still writes the filter</text>
<rect x="274" y="58" width="230" height="140" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="389" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">a table function</text>
<text x="389" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">takes the window as</text>
<text x="389" y="128" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">four parameters</text>
<text x="389" y="158" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">nothing to forget</text>
<text x="389" y="178" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">engine support varies</text>
<rect x="522" y="58" width="230" height="140" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="637" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">a client helper</text>
<text x="637" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">builds the SQL from a</text>
<text x="637" y="128" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">geometry argument</text>
<text x="637" y="158" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">best ergonomics</text>
<text x="637" y="178" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">only helps its own users</text>
</svg>
</figure>

The table-valued function is the strongest option where the engine supports it, because the window is a parameter rather than a convention and there is no way to call it without supplying one. Where it is unavailable, a client helper covers the majority of traffic and a view covers the rest, and the combination is usually enough. What does not work is documentation alone: the failure mode is silent, so nothing corrects a caller who ignores it.

Whichever route is chosen, add a **plan assertion** to the pipeline that runs representative queries and fails when the file count scanned exceeds a threshold. That converts "somebody wrote a slow query" from an invisible cost into a build failure, and it is the only mechanism on this page that keeps working when the person who set up the conventions has moved on.

## When Pushdown Silently Stops Working

Pushdown is fragile in a specific way: several unrelated changes disable it without changing results, so the symptom is always "the same query got slower" with no obvious cause.

<figure class="diagram">
<svg viewBox="0 0 764 254" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Six changes that silently disable spatial predicate pushdown: a function wrapped around the indexed column, a cast introduced by type mismatch, statistics dropped beyond the writer column limit, an OR that spans columns, a subquery the optimiser cannot fold, and unsorted appends since the last compaction">
<rect x="0" y="0" width="764" height="254" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Six ways pushdown disappears without an error</text>
<rect x="26" y="56" width="230" height="86" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="141" y="82" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">function on the column</text>
<text x="141" y="106" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">round(bbox_min_x) &lt; 10</text>
<text x="141" y="126" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">opaque to statistics</text>
<rect x="274" y="56" width="230" height="86" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="389" y="82" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">implicit cast</text>
<text x="389" y="106" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">DOUBLE column vs DECIMAL literal</text>
<text x="389" y="126" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">cast wraps the column</text>
<rect x="522" y="56" width="230" height="86" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="637" y="82" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">statistics not collected</text>
<text x="637" y="106" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">column beyond the writer limit</text>
<text x="637" y="126" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">nothing to compare against</text>
<rect x="26" y="156" width="230" height="86" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="141" y="182" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">OR across columns</text>
<text x="141" y="206" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">bbox_min_x &lt; 10 OR tag = &#8216;x&#8217;</text>
<text x="141" y="226" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#6a3d9a">neither side can prune alone</text>
<rect x="274" y="156" width="230" height="86" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="389" y="182" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">unfoldable subquery</text>
<text x="389" y="206" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">bounds from another table</text>
<text x="389" y="226" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">not known at planning time</text>
<rect x="522" y="156" width="230" height="86" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="637" y="182" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">unsorted appends</text>
<text x="637" y="206" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">since the last compaction</text>
<text x="637" y="226" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">statistics exist but are loose</text>
</svg>
</figure>

The implicit-cast case deserves emphasis because it is invisible in the SQL text. A `DOUBLE` column compared against a literal the engine parses as `DECIMAL` produces a plan where the column is wrapped in a cast, and a wrapped column cannot be matched against statistics. Writing the literal with an explicit type — or as a value that parses to a double unambiguously — restores pruning and changes nothing else about the query.

The unfoldable subquery case is the one with the least satisfying fix. When the query window comes from another table, the planner does not know it at planning time and cannot prune. Where that pattern is common, resolve the bounds in the client and inline them as literals, or use a dynamic-filtering-capable engine and confirm from the plan that the dynamic filter is actually being applied rather than merely being available.

## Reading the Plan in Each Engine

Confirming pushdown means reading a query plan, and every engine reports it differently. Knowing where to look turns a twenty-minute investigation into a thirty-second one.

**Spark** shows it in two places. `PushedFilters` on the scan node lists the predicates the data source accepted — a predicate absent from that list is being applied above the scan and prunes nothing. The `numFiles` and `filesPruned` metrics on the executed plan give the actual reduction. Read both: filters can be pushed and still prune nothing when statistics are missing.

**Trino** reports it through `EXPLAIN ANALYZE`, where the `ScanFilterProject` operator shows input rows and the split count. The number to compare is input rows against the table's total rows; a selective query reading everything means the connector received no usable predicate. Trino also exposes per-query statistics through its web interface, which is faster to read than the text plan during an incident.

**DuckDB** prints per-operator row counts from `EXPLAIN ANALYZE`, and the Parquet reader reports the number of row groups scanned against the number available. That ratio is the clearest single indicator available in any engine, because it isolates within-file skipping from file-level pruning.

The habit worth building is to record these numbers for a fixed query set on a schedule rather than to check them reactively. Pruning degrades gradually — as unsorted data accumulates, as a schema change moves a column past the statistics limit, as a partition spec evolves — and a time series makes the degradation visible while it is still cheap to fix. A single reading taken during an incident tells you the current state and nothing about when it changed, which is usually the more useful fact.

## The Predicate Rewrite That Costs Nothing

There is one transformation that improves nearly every spatial query and requires no changes to the table: expressing the spatial filter as a conjunction ordered from cheapest to most expensive.

The bounding-box comparisons come first, because they can be pushed to every layer. Any partition-column predicate comes before them if the value is known. The exact geometry test comes last. Optimisers generally reorder conjunctions themselves, but they reorder based on estimated selectivity, and their estimates for geometry functions are frequently wrong — often defaulting to a fixed guess that bears no relation to the actual selectivity. Writing the order explicitly costs nothing and removes the dependence on that estimate.

A second, related rewrite: replace `ST_Distance(geom, point) < r` with a bounding-box pre-filter plus the distance test. The distance function alone cannot be pushed anywhere, and it also computes an exact distance for every row when only a comparison is needed. Expanding the point by `r` into a box, filtering on the numeric bounds, and then applying the distance test to the survivors gives identical results and typically reads a small fraction of the data.

Both rewrites are mechanical enough to be applied by a helper library or a view, which is where they belong — a transformation that has to be remembered by every author is a transformation that will be applied inconsistently.

## A Regression Suite for Pruning

Everything on this page is a property that can silently stop holding, which makes it a natural fit for automated assertion rather than for periodic review.

Build the suite from three or four queries that represent the real access patterns — a small window, a large window, a window plus a time filter, a join against a reference table — and record for each the number of files scanned, the bytes read and the wall-clock. Assert on the ratios rather than on absolute values, because absolute numbers drift as the table grows and a threshold expressed in gigabytes will need constant revision, while "reads under two percent of files" stays meaningful for years.

Run it against a snapshot rather than against the live table, so a concurrent write cannot make the result flap. Pinning a snapshot identifier also makes the results comparable across runs, which is what turns the suite from a pass/fail gate into a trend that shows degradation before it crosses the threshold.

Where the suite fails, the diagnosis order is always the same: check that the predicate reached the scan, check that statistics exist for the columns it references, check that the files are still sorted, and check that the partition specification has not changed underneath. Four checks, in that order, resolve the overwhelming majority of pushdown regressions — and having them written down as a runbook matters more than it sounds, because pushdown failures surface as generic slowness and the instinct is to reach for engine tuning first.

Finally, keep the suite in the same repository as the pipeline that writes the table. A pruning regression is almost always caused by a change to the writer — a new column, a reordered schema, a changed partition spec, a dropped sort order — so the test that catches it belongs where that change is reviewed.

None of this is exotic engineering. It is the ordinary discipline of writing down what the system is supposed to do, measuring whether it still does, and making the measurement fail loudly. Spatial workloads simply make the discipline pay for itself faster, because the gap between a query that prunes and one that does not is measured in orders of magnitude rather than percentages.
 The measurement is the whole discipline.
