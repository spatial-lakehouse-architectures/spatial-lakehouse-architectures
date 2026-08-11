# Querying Iceberg Tables with the DuckDB Spatial Extension

This guide shows how to read an Apache Iceberg table from DuckDB using the `iceberg` extension, decode its WKB geometry column with the `spatial` extension, and run a spatial query end to end — including catalog and snapshot metadata configuration for both metadata-file and REST-catalog access.

## Context and prerequisites

Apache Iceberg (1.4+; these examples target a table written by Spark 3.5 with the 1.9.0 runtime) stores geometry as WKB in a binary column, since core Iceberg has no dedicated geometry type. DuckDB reads Iceberg through the `iceberg` extension, which parses the table's metadata to find the current snapshot's data files, and decodes geometry through the `spatial` extension. Use DuckDB 1.0 or later. This page belongs to [DuckDB geospatial analytics on lakehouse tables](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/duckdb-geospatial-analytics/); if your data is loose GeoParquet rather than an Iceberg table, the companion recipe [how to run ST_Intersects in DuckDB on GeoParquet](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/duckdb-geospatial-analytics/how-to-run-st-intersects-in-duckdb-on-geoparquet/) applies instead.

## Complete working solution

```python
import duckdb

con = duckdb.connect()

# 1. Load the three extensions: iceberg (table format), spatial (geometry), httpfs (S3)
con.execute("INSTALL iceberg; LOAD iceberg;")
con.execute("INSTALL spatial; LOAD spatial;")
con.execute("INSTALL httpfs; LOAD httpfs;")

# 2. S3 credentials for the warehouse bucket
con.execute("""
CREATE OR REPLACE SECRET s3_warehouse (
    TYPE S3, PROVIDER credential_chain, REGION 'us-east-1'
);
""")

# 3a. Direct metadata-file access: point iceberg_scan at the current metadata JSON.
#     Iceberg stores geometry as WKB in a BLOB column named "geom_wkb" here.
con.execute("""
CREATE VIEW parcels AS
SELECT
    parcel_id,
    zoning,
    ST_GeomFromWKB(geom_wkb) AS geom
FROM iceberg_scan(
    's3://warehouse/gis/parcels/metadata/00042-abc.metadata.json',
    allow_moved_paths = true
);
""")

# 4. Spatial query: total parcel area per zoning class within a bounding box.
#    ST_MakeEnvelope(xmin, ymin, xmax, ymax) builds the query window (EPSG:4326 coords).
result = con.execute("""
SELECT
    zoning,
    round(sum(ST_Area(geom)), 2) AS total_area,
    count(*)                     AS n
FROM parcels
WHERE ST_Intersects(
        geom,
        ST_MakeEnvelope(-122.42, 37.77, -122.40, 37.79)
      )
GROUP BY zoning
ORDER BY total_area DESC;
""").fetchall()

for zoning, area, n in result:
    print(f"{zoning:12s} area={area:12.2f}  parcels={n}")

con.close()
```

If you run a REST catalog (Nessie, Polaris, AWS Glue via the Iceberg REST spec), attach it instead of pointing at a metadata file, then query by table name:

```python
con.execute("""
ATTACH 's3://warehouse/gis' AS gis_cat (
    TYPE ICEBERG,
    ENDPOINT 'https://catalog.example.com/api/catalog'
);
""")
con.execute("""
SELECT count(*) FROM gis_cat.parcels
WHERE ST_Contains(
    ST_MakeEnvelope(-122.5, 37.7, -122.3, 37.8),
    ST_GeomFromWKB(geom_wkb)
);
""")
```

## Step-by-step walkthrough

1. **Load all three extensions.** `iceberg` reads the table format, `spatial` supplies `ST_GeomFromWKB`/`ST_Area`/`ST_Intersects`, and `httpfs` fetches bytes from S3. All three must be loaded in the same connection; missing `iceberg` yields a `Catalog Error: Table Function "iceberg_scan" does not exist`.

2. **Resolve the snapshot.** `iceberg_scan` with a metadata JSON path reads that file's `current-snapshot-id`, walks the manifest list, and produces the set of live data files. To query an older snapshot for time-travel, pass `snapshot_from_id` or `snapshot_from_timestamp` as options — useful for reproducing a past spatial aggregate.

3. **`allow_moved_paths = true`.** Iceberg metadata records absolute data-file paths. If the table was copied or the warehouse prefix changed, this option lets DuckDB rebase those paths against the metadata location instead of failing on a stale absolute path.

4. **Decode WKB.** The geometry lives as raw WKB in a `BLOB` column. `ST_GeomFromWKB(geom_wkb)` turns it into a DuckDB `GEOMETRY`. Do this once in the view so downstream queries operate on real geometries.

5. **Run the spatial predicate.** `ST_MakeEnvelope` builds an axis-aligned rectangle from `xmin, ymin, xmax, ymax`, and `ST_Intersects` filters parcels overlapping that window. `ST_Area` then aggregates by zoning class. Because the geometry is decoded from the scanned column, the predicate runs per row after Iceberg has already pruned data files by partition.

## Common errors and fixes

| Error | Cause | Fix |
|---|---|---|
| `Catalog Error: Table Function "iceberg_scan" does not exist` | `iceberg` extension not loaded | `INSTALL iceberg; LOAD iceberg;` before scanning |
| `IO Error: No files found that match the pattern` | Stale absolute data-file paths in metadata after a move | Add `allow_moved_paths = true` to `iceberg_scan` |
| `Invalid Error: Could not read metadata` | Pointed at a manifest or data file, not the `*.metadata.json` | Pass the current metadata JSON path (or attach a REST catalog) |
| `Binder Error: No function matches ST_Area(BLOB)` | Passed the raw WKB column to a spatial function | Wrap it in `ST_GeomFromWKB(geom_wkb)` first |
| Empty result over a known-populated area | Query envelope in a different CRS than stored geometry | Build the envelope in the table's CRS (EPSG:4326 here) or reproject with `ST_Transform` |

## Verification

Confirm you are reading the expected snapshot and that geometry decoded correctly. Iceberg metadata functions expose the snapshot history, and a validity check catches WKB corruption.

```python
# Which snapshot did we resolve, and how many data files?
meta = con.execute("""
SELECT count(*) AS files
FROM iceberg_metadata('s3://warehouse/gis/parcels/metadata/00042-abc.metadata.json');
""").fetchone()
print("data files in snapshot:", meta[0])

# Every decoded geometry should be valid
invalid = con.execute("""
SELECT count(*) FROM parcels WHERE NOT ST_IsValid(geom);
""").fetchone()[0]
assert invalid == 0, f"{invalid} invalid geometries decoded from WKB"
print("all geometries valid")
```

<figure class="diagram">
<svg viewBox="0 0 752 244" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="DuckDB resolving an Iceberg snapshot through metadata and manifests, then decoding WKB geometry for a spatial query">
<defs>
<marker id="arw-ice-duck" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#9a5a17"/></marker>
</defs>
<rect x="0" y="0" width="752" height="244" fill="#f7fbfc"/>
<text x="380" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Iceberg read path in DuckDB</text>
<rect x="25" y="75" width="160" height="70" rx="8" fill="#ffffff" stroke="#9a5a17" stroke-width="2"/>
<text x="105" y="103" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="600" fill="#0d3b45">metadata.json</text>
<text x="105" y="122" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">current snapshot</text>
<rect x="215" y="75" width="160" height="70" rx="8" fill="#ffffff" stroke="#9a5a17" stroke-width="2"/>
<text x="295" y="103" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="600" fill="#0d3b45">manifest list</text>
<text x="295" y="122" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">data-file set</text>
<rect x="405" y="75" width="160" height="70" rx="8" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<text x="485" y="103" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="600" fill="#0d3b45">Parquet data</text>
<text x="485" y="122" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">WKB BLOB col</text>
<rect x="595" y="75" width="145" height="70" rx="8" fill="#ffffff" stroke="#0e6e7d" stroke-width="2.5"/>
<text x="667" y="103" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">ST_GeomFromWKB</text>
<text x="667" y="122" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">GEOMETRY</text>
<line x1="185" y1="110" x2="215" y2="110" stroke="#9a5a17" stroke-width="2" marker-end="url(#arw-ice-duck)"/>
<line x1="375" y1="110" x2="405" y2="110" stroke="#9a5a17" stroke-width="2" marker-end="url(#arw-ice-duck)"/>
<line x1="565" y1="110" x2="595" y2="110" stroke="#9a5a17" stroke-width="2" marker-end="url(#arw-ice-duck)"/>
<rect x="405" y="180" width="335" height="50" rx="8" fill="#ffffff" stroke="#6a3d9a" stroke-width="2"/>
<text x="572" y="202" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="600" fill="#6a3d9a">ST_Intersects(geom, ST_MakeEnvelope(...))</text>
<text x="572" y="220" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">spatial predicate after decode</text>
<line x1="667" y1="145" x2="620" y2="180" stroke="#6a3d9a" stroke-width="2" marker-end="url(#arw-ice-duck)"/>
<text x="205" y="210" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">Partition pruning happens</text>
<text x="205" y="228" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">at the manifest stage</text>
</svg>
</figure>

The read path above shows why Iceberg complements DuckDB's spatial engine: partition pruning happens at the manifest stage before any geometry is decoded, so the WKB-to-`GEOMETRY` conversion and the `ST_Intersects` refinement only run on files the snapshot metadata already selected. To reduce the scan even further with bbox statistics, see [predicate pushdown optimization](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/predicate-pushdown-optimization/); for how the geometry bytes are laid out in the first place, see [GeoParquet encoding standards](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/geoparquet-encoding-standards/), and for how Iceberg and Delta compare for spatial storage, see [Iceberg vs Delta Lake for spatial data](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/open-table-format-versioning/iceberg-vs-delta-lake-for-spatial-data/). The authoritative references are the [DuckDB Iceberg extension documentation](https://duckdb.org/docs/stable/extensions/iceberg) and the [Apache Iceberg table specification](https://iceberg.apache.org/spec/).

## Why Reading the Table Beats Reading the Files

Pointing DuckDB at a table's Parquet files with a glob is one line and works immediately, which is exactly why it is worth being explicit about what it gives up.

<figure class="diagram">
<svg viewBox="0 0 762 284" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Four guarantees lost when reading Iceberg data files directly with a glob: snapshot consistency, delete file application, partition pruning by the planner, and schema evolution mapping by field id">
<rect x="0" y="0" width="762" height="284" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">What a glob quietly gives up</text>
<rect x="30" y="56" width="352" height="100" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="206" y="82" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">snapshot consistency</text>
<text x="206" y="106" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">a compaction mid-query changes the file set</text>
<text x="206" y="130" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">results can double-count or miss rows</text>
<rect x="398" y="56" width="352" height="100" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="574" y="82" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">delete files</text>
<text x="574" y="106" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">merge-on-read deletes are separate objects</text>
<text x="574" y="130" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">a glob returns rows that were deleted</text>
<rect x="30" y="172" width="352" height="100" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="206" y="198" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">partition pruning</text>
<text x="206" y="222" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">the planner never runs</text>
<text x="206" y="246" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">every file is opened and read</text>
<rect x="398" y="172" width="352" height="100" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="574" y="198" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">schema evolution</text>
<text x="574" y="222" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">columns map by field id, not by name</text>
<text x="574" y="246" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">a renamed column reads as null</text>
</svg>
</figure>

The bottom-right case is the one that surprises people who know the other three. Iceberg tracks columns by identifier rather than by name, so a renamed column is the same column to the table and a different one to a reader working from file schemas alone. The glob returns nulls for it, silently, and a downstream aggregate quietly becomes zero.

Where the extension is available, use it. Where it is not, plan the scan with a client that understands the table and hand DuckDB the resulting file list — that keeps three of the four guarantees, and the fourth can be recovered by applying the delete files explicitly.

## Pushing Predicates Through the Boundary

<figure class="diagram">
<svg viewBox="0 0 758 212" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Predicate placement when DuckDB reads an Iceberg table: partition and bounding box predicates belong in the scan planning step, the exact geometry test belongs in DuckDB">
<defs>
<marker id="dbi-pred-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#2f6e49"/></marker>
</defs>
<rect x="0" y="0" width="758" height="212" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Split the predicate across the boundary</text>
<rect x="34" y="66" width="290" height="96" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="179" y="94" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">to the table planner</text>
<text x="179" y="120" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">partition value, bbox ranges, time</text>
<text x="179" y="142" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">decides which files exist</text>
<rect x="456" y="66" width="290" height="96" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="601" y="94" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">to DuckDB</text>
<text x="601" y="120" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">ST_Intersects, ST_Contains, distance</text>
<text x="601" y="142" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">decides which rows survive</text>
<line x1="324" y1="114" x2="456" y2="114" stroke="#2f6e49" stroke-width="2" marker-end="url(#dbi-pred-arrow)"/>
<text x="390" y="196" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">Both halves are needed; either alone reads far more than necessary</text>
</svg>
</figure>

Getting this split wrong in the common direction — sending only the geometry predicate — means the planner has nothing to prune with and DuckDB receives the whole table. Getting it wrong in the other direction, sending only the bounding-box filter, returns rows whose boxes overlap but whose geometries do not. The two halves are complementary rather than alternative, which is the same filter-then-refine structure that appears everywhere else in this section.

## Operational Notes

Three practical points close this out, all of which come up on the first production deployment.

**Snapshot pinning.** Where a job runs for a while and issues several queries, pin the snapshot once and reuse it rather than resolving the current one each time. Otherwise a compaction between two queries in the same job can produce results that disagree with each other, which is a genuinely confusing bug to diagnose because both queries are individually correct.

**Credential lifetime.** Object-storage credentials configured on a connection do not refresh, and a long-running analysis session will fail partway through when they expire. Refresh explicitly on a timer, or scope sessions short enough that expiry is not reachable.

**Result size.** A spatial join against a lakehouse table can return more rows than expected — overlapping polygons multiply matches, and a bounding-box pre-filter without an exact test returns false positives. Write results to Parquet rather than materialising them in the client, and count before collecting.

None of these is specific to geometry, and all of them show up faster on spatial workloads because the data volumes and the join fan-out are larger than the scalar equivalents. Settling them once in a shared helper means every subsequent analysis inherits the correct behaviour.
A helper is a few dozen lines and removes an entire category of first-week surprises.
Write it once, import it everywhere, and the correctness properties come for free.
A shared helper is also the natural place to record which snapshot a result came from, which turns a reproducibility question into a lookup.

Six months later, when somebody asks why two reports disagree, the recorded snapshot identifier answers it immediately.
Without it, the reconciliation is guesswork against a table that has since changed.
Record it with the result, not alongside it.
 A result without its snapshot is an observation without a timestamp.
 Treat the two as a single artefact and reproducibility follows for free.
