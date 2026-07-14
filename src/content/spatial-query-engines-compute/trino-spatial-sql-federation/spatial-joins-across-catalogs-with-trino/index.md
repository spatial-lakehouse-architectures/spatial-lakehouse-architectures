# Spatial Joins Across Catalogs with Trino

This guide gives you a single runnable Trino SQL recipe that spatially joins a large fact table in an Iceberg catalog against reference geometries in a separate PostgreSQL catalog, using `ST_Intersects` and a shared spatial partition key so the distributed join stays broadcast-free.

## Context and prerequisites

This recipe is the concrete, copy-paste companion to the [Trino spatial SQL and cross-catalog federation](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/trino-spatial-sql-federation/) topic area, and it lives within the wider [Spatial Query Engines & Compute Optimization](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/) section. You need Trino 420 or newer with the geospatial functions available (`SHOW FUNCTIONS LIKE 'ST\_%';` should list them), an `iceberg` catalog holding a `pings` table whose geometry is stored as WKB in a `varbinary` column, and a `postgresql` catalog exposing a PostGIS `zones` table. Both sides must agree on EPSG:4326 lon/lat. The key idea is that a naive `ST_Intersects` join with no colocation key forces Trino to broadcast one side to every worker; by attaching the same coarse Bing-tile key to both inputs, the planner partitions instead of broadcasting.

## Complete working solution

```sql
-- Trino 420+  |  Broadcast-free federated spatial join:
-- Iceberg fact table (pings) x PostgreSQL reference table (zones)

-- (1) Materialize a tile-keyed reference layer once. Each zone gets the
--     zoom-12 Bing tile of its centroid, plus its geometry as WKB.
CREATE TABLE postgresql.reference.zone_tiles AS
SELECT
  zone_id,
  ST_AsBinary(geom)                                        AS geom_wkb,
  bing_tile_coordinates(
    bing_tile_at(ST_Y(ST_Centroid(geom)),
                 ST_X(ST_Centroid(geom)), 12)
  )                                                         AS tile
FROM postgresql.reference.zones;

-- (2) Force a partitioned (hash) join and run the spatial join.
SET SESSION join_distribution_type = 'PARTITIONED';

SELECT
    p.device_id,
    z.zone_id,
    p.event_ts
FROM iceberg.telemetry.pings AS p
JOIN postgresql.reference.zone_tiles AS z
       -- cheap equi-join on the shared tile drives colocation + pruning
    ON p.tile_z12 = z.tile
       -- exact geometry test removes tile-boundary false positives
   AND ST_Intersects(
         ST_GeomFromBinary(p.geom_wkb),
         ST_GeomFromBinary(z.geom_wkb)
       )
       -- partition predicate on the Iceberg side prunes data files
WHERE p.event_ts >= TIMESTAMP '2026-07-01 00:00:00'
  AND p.event_ts <  TIMESTAMP '2026-07-08 00:00:00';
```

If your `pings` table does not already carry a `tile_z12` partition column, add it at write time (or in a one-off `CREATE TABLE ... AS SELECT`) so the equi-join key exists on the fact side too:

```sql
-- One-time: add the same zoom-12 tile key to the fact table
CREATE TABLE iceberg.telemetry.pings_tiled
WITH (partitioning = ARRAY['tile_z12'])
AS
SELECT
  device_id,
  event_ts,
  geom_wkb,
  bing_tile_coordinates(
    bing_tile_at(ST_Y(ST_GeomFromBinary(geom_wkb)),
                 ST_X(ST_GeomFromBinary(geom_wkb)), 12)
  ) AS tile_z12
FROM iceberg.telemetry.pings;
```

## Step-by-step walkthrough

1. **Build the tile-keyed reference layer (step 1).** `ST_Centroid` collapses each zone polygon to a point, `bing_tile_at(lat, lon, 12)` maps that point to a zoom-12 tile (~9.8 km at the equator), and `bing_tile_coordinates` turns the tile into a `(x, y, zoom)` row you can equi-join on. Storing `ST_AsBinary(geom)` keeps the exact polygon available for the precise test. Zoom 12 is a deliberate trade-off: coarse enough that a zone maps to few tiles, fine enough that each tile bucket holds a manageable slice of the fact table.

2. **Force partitioned distribution (step 2, first line).** `SET SESSION join_distribution_type = 'PARTITIONED'` tells the optimizer to hash-partition both inputs by the join key rather than replicate one side. Without this, Trino's cost model may broadcast the reference table to every worker; on a large reference set that is exactly the memory blowup you are trying to avoid.

3. **Equi-join on the tile first.** `p.tile_z12 = z.tile` is the load-bearing predicate. It is a plain equality, so Trino hash-partitions on it, colocating fact and reference rows that share a tile onto the same worker. This is what makes the join broadcast-free.

4. **Refine with `ST_Intersects`.** Two features can share a tile without actually overlapping, so `ST_Intersects` on the decoded geometries removes those false positives. `ST_GeomFromBinary` reconstructs a `Geometry` from the WKB bytes on each side; the spatial functions cannot operate on raw `varbinary`.

5. **Push a partition predicate into Iceberg.** The `event_ts` range filter is pushed into Iceberg manifest planning so only the relevant data files are opened. Combined with the tile key this is the same [predicate pushdown](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/predicate-pushdown-optimization/) that keeps the scan to a small fraction of the table.

## Common errors and fixes

| Error | Cause | Fix |
|---|---|---|
| Join spills or OOMs on the reference side | Distribution defaulted to `REPLICATED` (broadcast) | `SET SESSION join_distribution_type = 'PARTITIONED'` and ensure both sides carry `tile_z12`/`tile` |
| `Unexpected type varbinary` from `ST_Intersects` | Passed raw WKB column instead of a `Geometry` | Wrap both arguments in `ST_GeomFromBinary()` |
| Result set is empty | SRID / axis order mismatch between PostGIS and Iceberg | Standardize on EPSG:4326 lon/lat before encoding both sides with `ST_AsBinary` |
| Iceberg scan reads every file | No partition predicate; only the spatial test filters | Keep the `event_ts` (or tile) range filter in `WHERE`; check `EXPLAIN ANALYZE` for `dynamicFilters` |

## Verification

Confirm the join partitioned rather than broadcast, and that Iceberg pruned files, by reading the plan and comparing against a brute-force count on a small area:

```sql
-- 1) Prove PARTITIONED distribution + Iceberg pruning
EXPLAIN ANALYZE
SELECT count(*)
FROM iceberg.telemetry.pings_tiled p
JOIN postgresql.reference.zone_tiles z
  ON p.tile_z12 = z.tile
 AND ST_Intersects(ST_GeomFromBinary(p.geom_wkb),
                   ST_GeomFromBinary(z.geom_wkb));
-- look for  Distribution: PARTITIONED  and an Iceberg scan
-- 'input rows' far below the table total.

-- 2) Correctness spot-check against a single known zone
SELECT count(*)
FROM iceberg.telemetry.pings_tiled p
JOIN postgresql.reference.zones z ON z.zone_id = 'Z-0042'
WHERE ST_Contains(z.geom, ST_GeomFromBinary(p.geom_wkb));
```

The tiled join, restricted to zone `Z-0042`, must return the same count as the brute-force `ST_Contains`.

<figure class="diagram">
<svg viewBox="0 0 760 250" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Tile equi-join colocates Iceberg pings and PostgreSQL zones before the exact ST_Intersects test">
<defs>
<marker id="arw-trino-xcat" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#0e6e7d"/></marker>
</defs>
<rect x="0" y="0" width="760" height="250" fill="#f7fbfc"/>
<text x="380" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Broadcast-free spatial join via a shared tile key</text>
<rect x="30" y="60" width="200" height="60" rx="8" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<text x="130" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="600" fill="#0d3b45">iceberg pings</text>
<text x="130" y="105" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">tile_z12 key</text>
<rect x="30" y="140" width="200" height="60" rx="8" fill="#ffffff" stroke="#6a3d9a" stroke-width="2"/>
<text x="130" y="166" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="600" fill="#0d3b45">postgresql zones</text>
<text x="130" y="185" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">tile key</text>
<rect x="300" y="100" width="180" height="60" rx="8" fill="#ffffff" stroke="#0e6e7d" stroke-width="2"/>
<text x="390" y="126" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="600" fill="#0d3b45">hash partition</text>
<text x="390" y="145" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">on tile = tile</text>
<rect x="550" y="100" width="180" height="60" rx="8" fill="#ffffff" stroke="#9a5a17" stroke-width="2"/>
<text x="640" y="126" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="600" fill="#0d3b45">ST_Intersects</text>
<text x="640" y="145" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">exact refine</text>
<line x1="230" y1="90" x2="300" y2="120" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-trino-xcat)"/>
<line x1="230" y1="170" x2="300" y2="140" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-trino-xcat)"/>
<line x1="480" y1="130" x2="550" y2="130" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-trino-xcat)"/>
<text x="380" y="230" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">Shared tile key colocates rows per worker, so neither side is broadcast</text>
</svg>
</figure>

For deeper federation and tuning context, return to the [Trino federation topic area](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/trino-spatial-sql-federation/); to compare this approach against distributed Spark joins see [broadcast spatial joins with Apache Sedona](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/sedona-distributed-spatial-compute/broadcast-spatial-joins-with-apache-sedona/), and to pre-optimize the Iceberg side see [optimizing spatial joins with Iceberg Z-ordering](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/z-ordering-for-geospatial-queries/optimizing-spatial-joins-with-iceberg-z-ordering/). The canonical function semantics live in the [Trino geospatial functions documentation](https://trino.io/docs/current/functions/geospatial.html).
