# How to Run ST_Intersects in DuckDB on GeoParquet

This guide is a complete, runnable recipe for loading DuckDB's spatial extension, reading a GeoParquet file straight from disk or S3, and executing an `ST_Intersects` spatial join accelerated by an R-tree index, with a verification step that proves the index was used.

## Context and prerequisites

`ST_Intersects` returns true when two geometries share at least one point, and it is the workhorse predicate for point-in-polygon and polygon-overlap joins. Run this recipe on DuckDB 1.0 or later (validated on 1.1.x) with the `spatial` extension; for `s3://` inputs you also need `httpfs`. This page sits under [DuckDB geospatial analytics on lakehouse tables](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/duckdb-geospatial-analytics/), which covers the broader engine-selection picture — here we focus on getting one join correct and fast. The GeoParquet inputs are assumed to follow the standard WKB column encoding described in [GeoParquet encoding standards](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/geoparquet-encoding-standards/).

## Complete working solution

```python
import duckdb

con = duckdb.connect()

# 1. Load spatial (geometry types + ST_* functions) and httpfs (for s3:// paths)
con.execute("INSTALL spatial; LOAD spatial;")
con.execute("INSTALL httpfs; LOAD httpfs;")

# If reading from S3, create a secret (skip for local files)
con.execute("""
CREATE OR REPLACE SECRET s3_src (
    TYPE S3, PROVIDER credential_chain, REGION 'us-east-1'
);
""")

# 2. Load points GeoParquet, decoding the WKB geometry column into GEOMETRY
con.execute("""
CREATE TABLE sensors AS
SELECT
    sensor_id,
    ST_GeomFromWKB(geometry) AS geom
FROM read_parquet('s3://lakehouse/sensors/*.parquet');
""")

# 3. Load polygons GeoParquet
con.execute("""
CREATE TABLE districts AS
SELECT
    district_id,
    name,
    ST_GeomFromWKB(geometry) AS geom
FROM read_parquet('s3://lakehouse/districts/*.parquet');
""")

# 4. Build an R-tree on the larger (points) table so the join can prune by bbox
con.execute("CREATE INDEX sensors_rtree ON sensors USING RTREE (geom);")

# 5. The ST_Intersects spatial join: assign each sensor to its district
result = con.execute("""
SELECT
    d.name           AS district,
    count(*)         AS sensor_count
FROM sensors s
JOIN districts d
  ON ST_Intersects(s.geom, d.geom)
GROUP BY d.name
ORDER BY sensor_count DESC;
""").fetchall()

for district, n in result:
    print(f"{district:20s} {n}")

con.close()
```

For a single-file local run, replace the `s3://` globs with a filesystem path such as `'/data/sensors.parquet'` and drop the secret and `httpfs` lines — everything else is identical.

## Step-by-step walkthrough

1. **Load extensions.** `LOAD spatial` registers the `GEOMETRY` type and every `ST_*` function, including `ST_Intersects` and `ST_GeomFromWKB`. `LOAD httpfs` is only needed for object storage; local files work with `spatial` alone. Extensions are cached after the first `INSTALL`, so repeated runs skip the download.

2. **Decode WKB into GEOMETRY.** GeoParquet stores geometry as Well-Known Binary in a Parquet `BLOB` column. `ST_GeomFromWKB(geometry)` reinterprets those bytes as DuckDB's native geometry type. Skipping this step leaves you with an opaque blob and `ST_Intersects` will raise a binder error.

3. **Materialize the polygon side.** The districts table is small, so loading it into a base table (rather than a view) lets the optimizer see its cardinality and treat it as the build side of the join.

4. **Create the R-tree.** `USING RTREE (geom)` builds a bounding-box index. During the join, DuckDB first checks R-tree bounding boxes for overlap — a cheap integer comparison — and only calls the expensive GEOS `ST_Intersects` on candidate pairs that pass. Indexing the larger table gives the biggest win because most of its rows are eliminated at the bbox stage.

5. **Run the join.** `ON ST_Intersects(s.geom, d.geom)` is the join condition. Because the predicate is a plain two-argument `ST_Intersects` over the indexed column, the optimizer can extract a bounding-box comparison and drive it through the R-tree. Wrapping either geometry in a transform here would defeat the index.

## Common errors and fixes

| Error | Cause | Fix |
|---|---|---|
| `Binder Error: No function matches ST_Intersects(BLOB, BLOB)` | Geometry column was never decoded from WKB | Wrap each side in `ST_GeomFromWKB(...)` when loading |
| Join is correct but slow; plan shows `SEQ_SCAN` + nested loop | No R-tree, or predicate wraps the indexed geom in a function | Create the R-tree and keep the predicate as `ST_Intersects(indexed_geom, other)` |
| `IO Error ... 403` reading `s3://` | Credentials or region not resolved | Create an S3 secret with `PROVIDER credential_chain` and the correct `REGION` |
| Counts look too high (points matched to several districts) | District polygons overlap at shared borders | Expected for `ST_Intersects` on touching boundaries; use `ST_Contains` or `ST_Within` for strict interior assignment |

## Verification

Confirm the R-tree is actually driving the join, not a full scan, with `EXPLAIN ANALYZE`.

```python
plan = con.execute("""
EXPLAIN ANALYZE
SELECT count(*)
FROM sensors s JOIN districts d
  ON ST_Intersects(s.geom, d.geom);
""").fetchall()
for row in plan:
    print(row[1])
```

Look for an `RTREE_INDEX_SCAN` operator on the sensors side and a low `Rows Scanned` count relative to the table size. As an independent correctness check, verify that every sensor total is preserved across the join grouping:

```python
total_joined = con.execute("""
SELECT sum(sensor_count) FROM (
    SELECT count(*) AS sensor_count
    FROM sensors s JOIN districts d ON ST_Intersects(s.geom, d.geom)
    GROUP BY d.name
);
""").fetchone()[0]
print("sensor-district pairs:", total_joined)
```

<figure class="diagram">
<svg viewBox="0 0 742 221" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Two-stage ST_Intersects join: R-tree bounding-box filter narrows candidates before exact GEOS refinement">
<defs>
<marker id="arw-stint" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#0e6e7d"/></marker>
</defs>
<rect x="0" y="0" width="742" height="221" fill="#f7fbfc"/>
<text x="380" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">ST_Intersects: bbox filter then exact refine</text>
<rect x="30" y="70" width="170" height="90" rx="8" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<text x="115" y="100" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="600" fill="#0d3b45">GeoParquet</text>
<text x="115" y="120" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">WKB geometry</text>
<text x="115" y="138" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">all rows</text>
<rect x="270" y="70" width="180" height="90" rx="8" fill="#ffffff" stroke="#6a3d9a" stroke-width="2"/>
<text x="360" y="100" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#6a3d9a">R-tree filter</text>
<text x="360" y="120" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">bbox overlap</text>
<text x="360" y="138" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">cheap, prunes most</text>
<rect x="520" y="70" width="210" height="90" rx="8" fill="#ffffff" stroke="#0e6e7d" stroke-width="2.5"/>
<text x="625" y="100" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">GEOS ST_Intersects</text>
<text x="625" y="120" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">exact predicate</text>
<text x="625" y="138" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">candidates only</text>
<line x1="200" y1="115" x2="270" y2="115" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-stint)"/>
<line x1="450" y1="115" x2="520" y2="115" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-stint)"/>
<text x="380" y="205" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">The R-tree eliminates non-overlapping pairs before the costly geometry test runs</text>
</svg>
</figure>

The two-stage filter-then-refine pattern shown above is why the R-tree matters so much: the exact GEOS predicate only ever runs on the handful of candidate pairs whose bounding boxes already overlap. To push this further — skipping whole row groups before decode by filtering on a numeric `bbox` covering column — see [predicate pushdown optimization](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/predicate-pushdown-optimization/), and to run the same style of join against an Iceberg table instead of loose GeoParquet, see [querying Iceberg tables with the DuckDB spatial extension](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/duckdb-geospatial-analytics/querying-iceberg-tables-with-duckdb-spatial-extension/). The canonical function reference is the [DuckDB spatial functions documentation](https://duckdb.org/docs/stable/extensions/spatial/functions).

## Making the Join Faster Still

The R-tree gets the join working; three further steps get it fast, and each is independent of the others.

<figure class="diagram">
<svg viewBox="0 0 764 244" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three additional optimisations for a DuckDB spatial join: filter row groups on numeric bounding box columns before decoding, project only the needed columns, and materialise the small side as a table so the optimiser sees its cardinality">
<rect x="0" y="0" width="764" height="244" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Three cheap steps beyond the index</text>
<rect x="26" y="56" width="230" height="176" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="141" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">1. bbox pre-filter</text>
<text x="141" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">numeric columns in the file</text>
<text x="141" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">skips whole row groups</text>
<text x="141" y="162" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">before any WKB is decoded</text>
<text x="141" y="196" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">largest effect of the three</text>
<rect x="274" y="56" width="230" height="176" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="389" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">2. project narrowly</text>
<text x="389" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">name the columns you need</text>
<text x="389" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">SELECT * reads every</text>
<text x="389" y="162" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">attribute column too</text>
<text x="389" y="196" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">free, and often forgotten</text>
<rect x="522" y="56" width="230" height="176" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="637" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">3. materialise the small side</text>
<text x="637" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">CREATE TABLE, not a view</text>
<text x="637" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">the optimiser then knows</text>
<text x="637" y="162" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">its true cardinality</text>
<text x="637" y="196" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">changes the build side</text>
</svg>
</figure>

The bounding-box pre-filter is worth applying even with the R-tree in place, because the two operate at different stages: the numeric predicate eliminates row groups during the Parquet read, before any geometry exists in memory, while the R-tree eliminates candidate pairs after the rows have been materialised. Doing both means the index is only ever asked about rows that survived the file-level prune.

## Verifying Correctness, Not Just Speed

<figure class="diagram">
<svg viewBox="0 0 764 210" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three correctness checks for a spatial join: boundary cases where geometries touch, points falling in multiple polygons, and rows with null or empty geometry">
<rect x="0" y="0" width="764" height="210" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Three cases the happy path never exercises</text>
<rect x="26" y="58" width="230" height="140" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="141" y="86" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">touching boundaries</text>
<text x="141" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">a point exactly on an edge</text>
<text x="141" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">ST_Intersects says yes</text>
<text x="141" y="162" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">ST_Contains says no</text>
<rect x="274" y="58" width="230" height="140" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="389" y="86" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">multiple matches</text>
<text x="389" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">overlapping polygons</text>
<text x="389" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">counts exceed the input</text>
<text x="389" y="162" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">decide: expected or a bug?</text>
<rect x="522" y="58" width="230" height="140" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="637" y="86" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">null and empty</text>
<text x="637" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">missing geometry rows</text>
<text x="637" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">silently dropped by the join</text>
<text x="637" y="162" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">count them separately</text>
</svg>
</figure>

The right-hand case is the one that produces quiet undercounts. Rows with null geometry never satisfy any spatial predicate, so an inner join drops them without comment and a total that should have been complete is short by however many there were. Counting them before the join, and deciding explicitly whether they belong in the output, turns a silent loss into a stated one.

## Scaling the Same Recipe

The recipe above holds up well past the scale most people expect, and the two adjustments that extend it further are both about avoiding materialisation.

The first is to **skip the intermediate tables** when the source is already well laid out. `CREATE TABLE ... AS SELECT` reads and stores everything before the join begins; querying `read_parquet` directly in the join lets DuckDB push the bounding-box predicate into the Parquet reader and never materialise the rows it will discard. The intermediate table is worth creating only for the small side, where the optimiser benefits from knowing the cardinality.

The second is to **stream the output** rather than collecting it. `fetchall` builds the entire result in Python memory, which is fine for an aggregate and wasteful for a join that returns millions of rows. Writing the result straight to Parquet with `COPY ... TO` keeps peak memory flat and produces an output that the rest of the pipeline can read without a further conversion.

Beyond that, the boundary is the one described in the section overview: when the join's working set exceeds the node, the recipe stops applying and the workload belongs on a cluster. Everything up to that point — and it is a lot of data — runs comfortably in a single process with no infrastructure at all, which is the reason this pattern is worth having in the toolkit even on platforms whose default answer is distributed compute.

For reading a governed table rather than loose files, and for the predicate split that keeps the table planner involved, see [querying Iceberg tables with the DuckDB spatial extension](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/duckdb-geospatial-analytics/querying-iceberg-tables-with-duckdb-spatial-extension/).
That guide picks up exactly where this one leaves off, with the same two-stage predicate structure applied across the table boundary.
The layout guidance that makes the bounding-box columns available in the first place is in [spatial partitioning and indexing strategies](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/).
Without those columns, the R-tree is doing all the work alone and the file-level prune never happens.

The two mechanisms compose, and a table that supports both will answer this query in a fraction of the time either alone would achieve.
Check both in the plan before concluding the query is as fast as it can be.
A plan that shows one but not the other has headroom left in it.
 Both mechanisms are cheap to add and neither is enabled by default.
