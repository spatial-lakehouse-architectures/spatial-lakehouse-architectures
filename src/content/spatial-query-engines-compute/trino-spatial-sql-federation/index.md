# Trino Spatial SQL and Cross-Catalog Federation

Trino is a massively parallel processing (MPP) SQL engine that shines when a spatial query must span data that lives in more than one system at once. A single `SELECT` can intersect points stored in an Apache Iceberg catalog against administrative polygons kept in PostgreSQL and legacy parcel tables registered in a Hive metastore, all without copying anything into a staging area. This topic area covers the Trino geospatial function surface (`ST_Intersects`, `ST_Contains`, `ST_Distance`), the `Geometry` and Bing-tile types, spatial-partitioning-aware distributed joins, and the federation mechanics that let one coordinator plan a query across three connectors. It sits inside the broader [Spatial Query Engines & Compute Optimization](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/) section, alongside single-node engines such as [DuckDB geospatial analytics](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/duckdb-geospatial-analytics/) and cluster-scale frameworks like [Apache Sedona](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/sedona-distributed-spatial-compute/).

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
<svg viewBox="0 0 712 296" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Trino coordinator federating a spatial join across Iceberg, Hive and PostgreSQL catalogs">
<defs>
<marker id="arw-trino-fed" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#0e6e7d"/></marker>
</defs>
<rect x="0" y="0" width="712" height="296" fill="#f7fbfc"/>
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

Iceberg stores geometry as WKB in a `varbinary` column (see [Iceberg spatial type support](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/iceberg-spatial-type-support/) for the encoding contract). Trino spatial functions do not operate on raw `varbinary`; you must reconstruct a `Geometry` with `ST_GeomFromBinary`. Wrap this in a view so downstream queries never touch the raw bytes.

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

The join predicate combines the cheap equi-join on the tile (which drives partitioning and pruning) with the exact `ST_Intersects` test. The tile equality lets Trino colocate rows; the geometry predicate removes false positives from tile-boundary overlap. This end-to-end recipe is expanded in [Spatial joins across catalogs with Trino](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/trino-spatial-sql-federation/spatial-joins-across-catalogs-with-trino/).

```sql
SET SESSION join_distribution_type = 'PARTITIONED';

SELECT p.device_id, z.zone_id, p.event_ts
FROM iceberg.telemetry.pings_geo AS p
JOIN postgresql.reference.zone_tiles AS z
  ON p.grid_h3 = z.tile                          -- colocation / prune key
 AND ST_Intersects(p.geom, ST_GeomFromBinary(z.geom_wkb))  -- exact test
WHERE p.event_ts >= TIMESTAMP '2026-07-01 00:00:00';
```

Because `grid_h3` and `tile` are both partition-aligned, the planner distributes each side by the same key and avoids broadcasting the (potentially large) reference set. The `event_ts` filter is pushed into Iceberg manifest planning, so only relevant data files are scanned — the same mechanism described under [predicate pushdown optimization](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/predicate-pushdown-optimization/).

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

A well-partitioned join over an Iceberg fact table that is [Z-ordered for spatial locality](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/z-ordering-for-geospatial-queries/optimizing-spatial-joins-with-iceberg-z-ordering/) typically scans 5–15% of files and completes in single-digit seconds at the 100 GB scale; the same query without partition alignment degrades to a full-table broadcast and can be 20–50x slower.

## Common errors and fixes

| Symptom | Root cause | Fix |
|---|---|---|
| `Function st_intersects not registered` | Running a distribution without the geospatial plugin, or passing `varbinary` instead of `Geometry` | Use an official Trino 420+ build; wrap raw WKB in `ST_GeomFromBinary()` before the predicate |
| Join falls back to `REPLICATED` and OOMs | Optimizer misjudged reference-side size; no shared partition key | Set `join_distribution_type='PARTITIONED'`; add a colocatable tile/grid key to both sides |
| Iceberg scan reads the whole table | Spatial predicate is not a partition predicate, so no manifest pruning | Filter on the partition column (grid/tile) in addition to `ST_` test; verify `dynamicFilters` in `EXPLAIN ANALYZE` |
| PostGIS join returns zero rows | PostGIS `geometry` came back as WKB but SRID/axis order differs | Standardize on EPSG:4326 lon/lat; re-encode reference with `ST_AsBinary` and decode with `ST_GeomFromBinary` |
| `Query exceeded per-node memory limit` | Millions of large geometries held in the join hash | Enable `spill-enabled`, raise `query.max-memory-per-node`, coarsen the grid so partitions are smaller |

For the authoritative function reference and connector behavior, consult the [Trino geospatial functions documentation](https://trino.io/docs/current/functions/geospatial.html) and the [Trino Iceberg connector documentation](https://trino.io/docs/current/connector/iceberg.html). When you are ready to benchmark this engine against DuckDB and Sedona on identical GeoParquet inputs, see [benchmarking spatial query engines on GeoParquet](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/engine-benchmarking-selection/benchmarking-spatial-query-engines-on-geoparquet/).

## What Federation Costs, and Where

Trino's ability to join across catalogs is genuinely useful and is also the place where spatial queries most often become unexpectedly expensive. The reason is structural: a connector can push filters down only as far as the source system understands them.

<figure class="diagram">
<svg viewBox="0 0 762 286" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="A federated spatial join showing the lakehouse side where partition and bbox predicates push down, and a relational source where the spatial predicate cannot be pushed so the whole table is pulled across the network">
<defs>
<marker id="tf-fed-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#0e6e7d"/></marker>
</defs>
<rect x="0" y="0" width="762" height="286" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Pushdown stops at the connector&#8217;s capability</text>
<rect x="30" y="60" width="300" height="120" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="180" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">Iceberg catalog</text>
<text x="180" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">partition + bbox predicates push down</text>
<text x="180" y="138" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">splits pruned before reading</text>
<text x="180" y="160" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">a few GB cross the wire</text>
<rect x="450" y="60" width="300" height="120" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="600" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">relational source</text>
<text x="600" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">no spatial pushdown</text>
<text x="600" y="138" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">only simple column filters</text>
<text x="600" y="160" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">the whole table crosses the wire</text>
<rect x="240" y="212" width="300" height="62" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="390" y="238" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">the join happens in Trino</text>
<text x="390" y="258" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">paying for whatever each side sent</text>
<line x1="180" y1="180" x2="330" y2="212" stroke="#0e6e7d" stroke-width="2" marker-end="url(#tf-fed-arrow)"/>
<line x1="600" y1="180" x2="450" y2="212" stroke="#9a5a17" stroke-width="2" marker-end="url(#tf-fed-arrow)"/>
</svg>
</figure>

The asymmetry is the whole issue. The lakehouse side benefits from every optimisation described across this site; the remote side frequently has none of them, so a query that reads five gigabytes locally may pull two hundred from the other source. The plan looks reasonable and the total is dominated by a transfer nobody intended.

Three mitigations work. **Filter the remote side on something it understands** — a region code, a customer identifier, a date — even when the spatial predicate would be sufficient logically, because a non-spatial filter is one the connector can push. **Materialise stable reference data locally**, since a boundary table that changes quarterly does not need to be federated at all; a scheduled copy into the lakehouse removes the transfer permanently. And **check the plan for the transfer volume** before promoting a federated query to a schedule, because the difference between a good and a bad federated query is invisible in the SQL.

## Making Spatial SQL Portable Across Catalogs

Function coverage differs between Trino's own spatial functions and those of the systems it federates to, and a query written against one will not always mean the same thing against another.

The safest discipline is to **perform all spatial evaluation in Trino** rather than pushing geometry functions to a remote source, even when the remote source supports them. This keeps semantics consistent — one implementation of `ST_Intersects`, one interpretation of boundary-touching cases, one behaviour for empty geometries — at the cost of transferring geometry. Where the transfer is affordable, the consistency is worth more than the saving, because a federated query whose answer depends on which side evaluated the predicate is a query nobody can reason about.

Where the transfer is not affordable, the alternative is to reduce the geometry to something both sides agree on: a grid cell identifier, or a bounding box in four numeric columns. Both are computable on either side with identical results, both push down as ordinary predicates, and both narrow the candidate set enough that the exact test can then run in Trino on a small remainder. This is the same filter-then-refine pattern that appears throughout this section, applied across a network boundary rather than across a storage one.

## Resource Groups as the Governance Layer

The reason Trino ends up as the shared spatial SQL surface in most organisations is not raw speed. It is that it can be governed, and spatial workloads need governing more than most.

<figure class="diagram">
<svg viewBox="0 0 764 230" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Resource groups isolating three workload classes: scheduled reporting with a guaranteed share, interactive analysis with a moderate cap, and ad hoc exploration with a small cap and a hard query timeout">
<rect x="0" y="0" width="764" height="230" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">One runaway spatial join must not starve the rest</text>
<rect x="26" y="58" width="230" height="160" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="141" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">scheduled</text>
<text x="141" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">reports, extracts, exports</text>
<text x="141" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">guaranteed share</text>
<text x="141" y="166" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">generous timeout</text>
<text x="141" y="194" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">must finish, predictably</text>
<rect x="274" y="58" width="230" height="160" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="389" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">interactive</text>
<text x="389" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">dashboards, known queries</text>
<text x="389" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">moderate concurrency cap</text>
<text x="389" y="166" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">short timeout</text>
<text x="389" y="194" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">latency matters most</text>
<rect x="522" y="58" width="230" height="160" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="637" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">ad hoc</text>
<text x="637" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">exploration, one-off analysis</text>
<text x="637" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">small cap, hard timeout</text>
<text x="637" y="166" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">rejected above a size limit</text>
<text x="637" y="194" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">where the accidents happen</text>
</svg>
</figure>

The right-hand group is where an unbounded spatial predicate lands — a join written without a bounding-box filter, a window covering a continent, a self-join over a large table. Those queries are not malicious and are usually written by someone learning the data. Capping their share means the lesson costs them a slow query rather than costing everyone else their afternoon.

A spatial-specific refinement worth adding: reject rather than queue when the requested extent is implausible. A query whose bounding box covers the planet against a table partitioned by city-scale cells is almost certainly a mistake, and failing it immediately with a message explaining why is kinder and cheaper than running it. Implementing this as a view constraint or a policy costs little and prevents the single most common accidental full-table scan.

The concrete cross-catalog join, including the predicate ordering that keeps pushdown alive, is worked through in [spatial joins across catalogs with Trino](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/trino-spatial-sql-federation/spatial-joins-across-catalogs-with-trino/). Read it after the governance section above, because a federated query that is fast and ungoverned is a different problem rather than a solved one.

## Operating the Connector Layer

Beyond query design, a federated spatial deployment has a small set of operational concerns that are worth settling deliberately.

**Connector concurrency limits.** A Trino cluster can issue far more concurrent requests to a source system than that system expects, and a spatial join that pulls from a transactional database can degrade it for its primary users. Cap the per-catalog connection count to something the source owner has agreed to, and treat that as a contract rather than a tuning parameter.

**Statistics on the remote side.** Trino's cost-based optimiser uses table statistics to choose join order, and a remote catalog reporting no statistics will frequently produce a plan that builds the hash table on the wrong side. Where the connector supports it, ensure statistics are collected; where it does not, be prepared to hint the join order explicitly for the queries that matter.

**Schema drift across catalogs.** A column renamed in a source system breaks the federated view silently at the next query rather than at the moment of change. A scheduled check that resolves every federated view and reports failures gives a day's warning instead of an incident.

**Credential and network paths.** Federated queries mean the coordinator and workers need reachability and credentials for every source, which is a larger surface than a single-catalog deployment. Enumerate it explicitly, and review it when a source is added — a spatial platform that quietly acquires read access to six systems has a governance question to answer as well as a networking one.

None of these is specific to geospatial data, and all of them bite harder here because spatial joins move more bytes than scalar ones and because the reference datasets involved are frequently owned by another team. Settling them early makes federation the useful capability it should be rather than the thing that gets blamed after an outage.

For the layout work that makes the lakehouse side of a federated query cheap in the first place, see [spatial partitioning and indexing strategies](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/) — a federated join across a well-laid-out table and a filtered remote source is a very different proposition from one across two unoptimised ones.

The same is true of the governance layer: resource groups protect a well-designed platform from an occasional mistake, and they cannot rescue one where every query is a full scan.
Layout first, then governance, then federation — in that order, each layer works.

For the engine-selection question that decides whether Trino is the right surface at all — against a single-node engine for bounded work, or a cluster for distributed transforms — see [engine benchmarking and selection](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/engine-benchmarking-selection/).

That page also sets out the benchmark design that makes such a comparison predictive rather than merely numeric, which matters more for federated workloads than for local ones because the transfer cost dominates and is easy to leave out of a measurement.

Include the transfer in the measurement, or the comparison will favour whichever engine happened to be closest to the data on the day.

Measured properly, federation is a capability with a known price rather than an unbounded one, which is the state it needs to be in before anyone schedules a query against it.

Until then, treat every federated spatial query as a draft: correct, useful, and not yet safe to schedule against a source somebody else operates.
 Promote it once the transfer volume is known and bounded, and not before.
 Scheduling an unbounded federated query is how another team learns about your platform. They will not be pleased, and they will be right.
