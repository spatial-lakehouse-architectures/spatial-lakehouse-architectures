# Trino Spatial SQL and Cross-Catalog Federation

Trino is a massively parallel processing (MPP) SQL engine that shines when a spatial query must span data that lives in more than one system at once. A single `SELECT` can intersect points stored in an Apache Iceberg catalog against administrative polygons kept in PostgreSQL and legacy parcel tables registered in a Hive metastore, all without copying anything into a staging area. This topic area covers the Trino geospatial function surface (`ST_Intersects`, `ST_Contains`, `ST_Distance`), the `Geometry` and Bing-tile types, spatial-partitioning-aware distributed joins, and the federation mechanics that let one coordinator plan a query across three connectors. It sits inside the broader [Spatial Query Engines & Compute Optimization](/spatial-query-engines-compute/) section, alongside single-node engines such as [DuckDB geospatial analytics](/spatial-query-engines-compute/duckdb-geospatial-analytics/) and cluster-scale frameworks like [Apache Sedona](/spatial-query-engines-compute/sedona-distributed-spatial-compute/).

## When to use this

Trino is the right tool when the spatial workload is interactive SQL over data that is already governed by a lakehouse catalog and, critically, when the reference geometries you join against are not in the same store as the fact data. Reach for a single-node engine when everything fits on one machine, and reach for a Spark-based framework when you need to write results back through a heavy transformation DAG.

| Signal | Trino | DuckDB | Sedona (Spark) |
|---|---|---|---|
| Join spans Iceberg + Hive + PostgreSQL | Yes, native federation | No, one file/DB at a time | Partial, via per-source readers |
| Interactive latency on 10–500 GB | Strong (seconds) | Strong on one node | Weaker (job startup cost) |
| Petabyte spatial joins with custom UDFs | Adequate | No | Best |
| No cluster to operate | No | Yes | No |
| ST_ function library | Rich, ANSI-flavored | Rich (GEOS) | Richest (JTS) |

The federation angle is the deciding factor. If the query is `iceberg.telemetry.pings ⋈ postgresql.ref.zones`, Trino plans it as one distributed job; the alternatives force an export-and-reload.

<figure class="diagram">
<svg viewBox="0 0 760 300" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Trino coordinator federating a spatial join across Iceberg, Hive and PostgreSQL catalogs">
<defs>
<marker id="arw-trino-fed" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#0e6e7d"/></marker>
</defs>
<rect x="0" y="0" width="760" height="300" fill="#f7fbfc"/>
<text x="380" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Cross-catalog spatial join on one Trino coordinator</text>
<rect x="280" y="50" width="200" height="60" rx="8" fill="#ffffff" stroke="#0e6e7d" stroke-width="2"/>
<text x="380" y="78" text-anchor="middle" font-family="sans-serif" font-size="14" font-weight="600" fill="#0d3b45">Coordinator</text>
<text x="380" y="97" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">plans ST_Intersects join</text>
<rect x="60" y="150" width="180" height="70" rx="8" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<text x="150" y="178" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="600" fill="#0d3b45">iceberg catalog</text>
<text x="150" y="197" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">pings (fact, huge)</text>
<rect x="290" y="150" width="180" height="70" rx="8" fill="#ffffff" stroke="#9a5a17" stroke-width="2"/>
<text x="380" y="178" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="600" fill="#0d3b45">hive catalog</text>
<text x="380" y="197" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">parcels (legacy)</text>
<rect x="520" y="150" width="180" height="70" rx="8" fill="#ffffff" stroke="#6a3d9a" stroke-width="2"/>
<text x="610" y="178" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="600" fill="#0d3b45">postgresql catalog</text>
<text x="610" y="197" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">zones (reference)</text>
<line x1="330" y1="110" x2="180" y2="150" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-trino-fed)"/>
<line x1="380" y1="110" x2="380" y2="150" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-trino-fed)"/>
<line x1="430" y1="110" x2="590" y2="150" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-trino-fed)"/>
<text x="380" y="258" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">Partition pushdown prunes Iceberg files; small reference sides are broadcast to workers</text>
<text x="380" y="280" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">One distributed plan, no export-and-reload between systems</text>
</svg>
</figure>

## Prerequisites and environment setup

Spatial support in Trino is built into the engine core (the `ST_` functions ship with the server), but stable Iceberg spatial predicate pushdown and the improved distributed-join planner landed across the 4xx line, so pin **Trino 420 or newer**. The functions operate on the `Geometry` type produced by `ST_GeometryFromText` / `ST_GeomFromBinary`, and on `SphericalGeography` for great-circle work. Configure the three connectors as separate catalogs; each catalog is a properties file in `etc/catalog/`.

```properties
# etc/catalog/iceberg.properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=https://catalog.internal:8181
fs.native-s3.enabled=true
s3.region=us-east-1
# push spatial partition predicates down into manifest planning
iceberg.dynamic-filtering.wait-timeout=5s
```

```properties
# etc/catalog/postgresql.properties
connector.name=postgresql
connection-url=jdbc:postgresql://pg.internal:5432/reference
connection-user=trino_ro
connection-password=${ENV:PG_PW}
# PostGIS geometry arrives as WKB; cast on read
```

```properties
# etc/catalog/hive.properties
connector.name=hive
hive.metastore.uri=thrift://hms.internal:9083
fs.native-s3.enabled=true
```

Verify the engine sees the geospatial catalog with `SHOW FUNCTIONS LIKE 'ST\_%';`. If nothing returns, you are on a build without the geospatial plugin — use an official Trino distribution rather than a stripped container image.

## Step-by-step implementation

### 1. Materialize geometry columns as the Geometry type

Iceberg stores geometry as WKB in a `varbinary` column (see [Iceberg spatial type support](/spatial-lakehouse-fundamentals-architecture/iceberg-spatial-type-support/) for the encoding contract). Trino spatial functions do not operate on raw `varbinary`; you must reconstruct a `Geometry` with `ST_GeomFromBinary`. Wrap this in a view so downstream queries never touch the raw bytes.

```sql
-- Trino DDL: a view that exposes decoded geometry over Iceberg WKB
CREATE OR REPLACE VIEW iceberg.telemetry.pings_geo AS
SELECT
  device_id,
  event_ts,
  ST_GeomFromBinary(geom_wkb) AS geom,   -- varbinary WKB -> Geometry
  grid_h3                                 -- pre-computed partition key
FROM iceberg.telemetry.pings;
```

### 2. Add a spatial partition key both sides can share

A distributed spatial join is only broadcast-free when both inputs carry a colocatable key. The durable pattern is a coarse grid cell computed at write time and stored as a partition column. Trino exposes Bing-tile helpers (`bing_tile`, `bing_tile_at`, `bing_tiles_around`) that map a lon/lat and zoom to a tile you can join on. Precompute the tile on the reference side once:

```sql
-- Trino: attach a zoom-12 Bing tile to each reference zone's centroid
CREATE TABLE postgresql.reference.zone_tiles AS
SELECT
  zone_id,
  ST_AsBinary(geom) AS geom_wkb,
  bing_tile_coordinates(
    bing_tile_at(ST_Y(ST_Centroid(geom)), ST_X(ST_Centroid(geom)), 12)
  ) AS tile
FROM postgresql.reference.zones;
```

### 3. Run the cross-catalog spatial join

The join predicate combines the cheap equi-join on the tile (which drives partitioning and pruning) with the exact `ST_Intersects` test. The tile equality lets Trino colocate rows; the geometry predicate removes false positives from tile-boundary overlap. This end-to-end recipe is expanded in [Spatial joins across catalogs with Trino](/spatial-query-engines-compute/trino-spatial-sql-federation/spatial-joins-across-catalogs-with-trino/).

```sql
SET SESSION join_distribution_type = 'PARTITIONED';

SELECT p.device_id, z.zone_id, p.event_ts
FROM iceberg.telemetry.pings_geo AS p
JOIN postgresql.reference.zone_tiles AS z
  ON p.grid_h3 = z.tile                          -- colocation / prune key
 AND ST_Intersects(p.geom, ST_GeomFromBinary(z.geom_wkb))  -- exact test
WHERE p.event_ts >= TIMESTAMP '2026-07-01 00:00:00';
```

Because `grid_h3` and `tile` are both partition-aligned, the planner distributes each side by the same key and avoids broadcasting the (potentially large) reference set. The `event_ts` filter is pushed into Iceberg manifest planning, so only relevant data files are scanned — the same mechanism described under [predicate pushdown optimization](/spatial-partitioning-indexing-strategies/predicate-pushdown-optimization/).

## Verification and testing

Never trust that pushdown and partitioned distribution happened — read the plan. `EXPLAIN ANALYZE` reports per-operator row counts and the actual distribution type chosen.

```sql
EXPLAIN ANALYZE
SELECT count(*)
FROM iceberg.telemetry.pings_geo p
JOIN postgresql.reference.zone_tiles z
  ON p.grid_h3 = z.tile AND ST_Intersects(p.geom, ST_GeomFromBinary(z.geom_wkb));
```

Confirm three things in the output: the Iceberg scan shows `dynamicFilters` applied and an `input rows` count far below the table total (proof of file pruning); the join node reads `Distribution: PARTITIONED` rather than `REPLICATED`; and the `ScanFilterProject` over PostgreSQL shows the tile projection pushed down. A row-count sanity check against a brute-force `ST_Contains` on a small bounding box should match exactly.

## Performance and tuning

The dominant cost in a federated spatial join is data movement, so the goal is to move as few bytes across the exchange as possible. Concrete knobs:

- `join_distribution_type`: force `PARTITIONED` for two large spatial sides; leave `AUTOMATIC` only when one side is provably tiny (< ~10 MB after filtering), where a broadcast wins.
- `spatial_partitioning`: build a partitioning table with `CALL system.create_spatial_partitioning` and pass its name to `ST_Intersects`-style joins via the `spatial_partitioning('name')` argument. This gives the optimizer a KDB-tree so it can partition by geometry rather than only by a precomputed grid key, cutting cross-worker shuffle by 40–70% on skewed data.
- `node-scheduler.max-splits-per-node`: raise from the default 256 toward 512 when scanning wide Iceberg tables so workers stay saturated.
- `query.max-memory-per-node`: geometry objects are heap-heavy; budget at least 4–8 GB per worker for joins over 100M+ geometries and enable spill (`spill-enabled=true`) as a safety valve.

A well-partitioned join over an Iceberg fact table that is [Z-ordered for spatial locality](/spatial-partitioning-indexing-strategies/z-ordering-for-geospatial-queries/optimizing-spatial-joins-with-iceberg-z-ordering/) typically scans 5–15% of files and completes in single-digit seconds at the 100 GB scale; the same query without partition alignment degrades to a full-table broadcast and can be 20–50x slower.

## Common errors and fixes

| Symptom | Root cause | Fix |
|---|---|---|
| `Function st_intersects not registered` | Running a distribution without the geospatial plugin, or passing `varbinary` instead of `Geometry` | Use an official Trino 420+ build; wrap raw WKB in `ST_GeomFromBinary()` before the predicate |
| Join falls back to `REPLICATED` and OOMs | Optimizer misjudged reference-side size; no shared partition key | Set `join_distribution_type='PARTITIONED'`; add a colocatable tile/grid key to both sides |
| Iceberg scan reads the whole table | Spatial predicate is not a partition predicate, so no manifest pruning | Filter on the partition column (grid/tile) in addition to `ST_` test; verify `dynamicFilters` in `EXPLAIN ANALYZE` |
| PostGIS join returns zero rows | PostGIS `geometry` came back as WKB but SRID/axis order differs | Standardize on EPSG:4326 lon/lat; re-encode reference with `ST_AsBinary` and decode with `ST_GeomFromBinary` |
| `Query exceeded per-node memory limit` | Millions of large geometries held in the join hash | Enable `spill-enabled`, raise `query.max-memory-per-node`, coarsen the grid so partitions are smaller |

For the authoritative function reference and connector behavior, consult the [Trino geospatial functions documentation](https://trino.io/docs/current/functions/geospatial.html) and the [Trino Iceberg connector documentation](https://trino.io/docs/current/connector/iceberg.html). When you are ready to benchmark this engine against DuckDB and Sedona on identical GeoParquet inputs, see [benchmarking spatial query engines on GeoParquet](/spatial-query-engines-compute/engine-benchmarking-selection/benchmarking-spatial-query-engines-on-geoparquet/).
