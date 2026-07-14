# Row-Level Security for Spatial Data in Trino

This guide delivers a runnable recipe that enforces row-level security on a spatial Iceberg table in Trino by combining a system access control provider with a tenant-scoped geometry predicate, so a filtered query can only ever return geometries inside a caller's authorized region.

## Context and prerequisites

Trino has no `CREATE ROW ACCESS POLICY` DDL the way some warehouses do; row filtering is delivered through the access control SPI, either the built-in file-based system access control or an Open Policy Agent (OPA) plugin, both of which can attach a per-user `WHERE` expression to a table at parse time. This page sits under [Security Boundaries for GIS Data](/spatial-lakehouse-fundamentals-architecture/security-boundaries-for-gis-data/) and assumes you already understand why the tenant predicate must resolve before spatial compute, covered in [implementing row-level security for geospatial datasets](/spatial-lakehouse-fundamentals-architecture/security-boundaries-for-gis-data/implementing-row-level-security-for-geospatial-datasets/). You need Trino 440 or newer with the `iceberg` connector, an Iceberg 1.4+ catalog (REST or Hive), and the geometry functions from Trino's `geospatial` library (`ST_Contains`, `ST_GeomFromBinary`). The table stores geometry as WKB in a `VARBINARY` column, the portable encoding described in [Iceberg spatial type support](/spatial-lakehouse-fundamentals-architecture/iceberg-spatial-type-support/).

## Complete working solution

First, create and populate the spatial Iceberg table, plus a lookup table that maps each principal to the polygon they are allowed to see. Run this in the Trino CLI against catalog `iceberg`, schema `gis`.

```sql
-- Trino SQL (Iceberg connector, Trino 440+; Iceberg format-version 2)
CREATE SCHEMA IF NOT EXISTS iceberg.gis
  WITH (location = 's3://lakehouse/gis');

CREATE TABLE iceberg.gis.spatial_assets (
    asset_id   BIGINT,
    tenant_id  VARCHAR NOT NULL,
    geom_wkb   VARBINARY,      -- geometry serialized as WKB, EPSG:4326
    label      VARCHAR
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['tenant_id'],
    format_version = 2
);

-- Tenant boundary polygons, one authorized region per principal.
CREATE TABLE iceberg.gis.tenant_regions (
    principal   VARCHAR NOT NULL,
    tenant_id   VARCHAR NOT NULL,
    region_wkt  VARCHAR NOT NULL   -- WKT polygon in EPSG:4326
);

INSERT INTO iceberg.gis.tenant_regions VALUES
  ('analyst_west', 'acme_west',
   'POLYGON ((-123 37, -121 37, -121 39, -123 39, -123 37))'),
  ('analyst_east', 'acme_east',
   'POLYGON ((-74 40, -72 40, -72 42, -74 42, -74 40))');

-- Two rows inside the west polygon, one inside the east polygon.
-- ST_Point(x, y) builds the point; ST_AsBinary emits standard WKB bytes.
INSERT INTO iceberg.gis.spatial_assets
SELECT asset_id, tenant_id, ST_AsBinary(pt), label
FROM (
  VALUES
    (1, 'acme_west', ST_Point(-122.4, 37.8), 'sf_pump'),
    (2, 'acme_west', ST_Point(-121.9, 38.5), 'delta_valve'),
    (3, 'acme_east', ST_Point(-73.9,  40.7), 'nyc_meter')
) AS t(asset_id, tenant_id, pt, label);
```

The security itself lives outside SQL, in a file-based system access control config. Point Trino at it with `etc/access-control.properties`:

```properties
# etc/access-control.properties
access-control.name=file
security.config-file=etc/rules.json
```

```json
{
  "catalogs": [
    { "user": ".*", "catalog": "iceberg", "allow": "read-only" }
  ],
  "tables": [
    {
      "user": "analyst_.*",
      "catalog": "iceberg",
      "schema": "gis",
      "table": "spatial_assets",
      "privileges": ["SELECT"],
      "filter": "tenant_id = (SELECT tenant_id FROM iceberg.gis.tenant_regions WHERE principal = CURRENT_USER) AND ST_Contains(ST_GeomFromText((SELECT region_wkt FROM iceberg.gis.tenant_regions WHERE principal = CURRENT_USER)), ST_GeomFromBinary(geom_wkb))",
      "filter_environment": { "user": "system_rls" }
    },
    {
      "user": "admin",
      "catalog": "iceberg",
      "schema": "gis",
      "table": "spatial_assets",
      "privileges": ["SELECT", "INSERT", "DELETE", "OWNERSHIP"]
    }
  ]
}
```

Every `SELECT` issued by an `analyst_*` principal is silently rewritten to append that `filter` expression as an additional conjunct, and the rewrite happens during analysis, before the Iceberg connector plans file pruning.

## Step-by-step walkthrough

1. **Partition on `tenant_id`.** The `partitioning = ARRAY['tenant_id']` clause means the coarse tenant predicate injected by the row filter becomes a partition filter. Trino prunes entire data files for other tenants during split generation, so the expensive `ST_Contains` call never touches their rows. This is the storage-level half of the boundary; the geometry predicate is the fine-grained half.

2. **Store geometry as WKB.** `geom_wkb` is `VARBINARY` holding standard Well-Known Binary. `ST_GeomFromBinary(geom_wkb)` reconstructs a Trino geometry at query time. Keeping the raw bytes (rather than a serialized Trino type) keeps the table portable to Spark, DuckDB, and PyIceberg readers.

3. **Model the authorization region as data.** `tenant_regions` maps a `CURRENT_USER` principal to both a `tenant_id` and a WKT boundary polygon. Driving policy from a table rather than hardcoding polygons in JSON means you re-provision access with an `INSERT`, not a Trino restart.

4. **Compose the filter.** The `filter` string has two conjuncts. The first, `tenant_id = (SELECT ...)`, is a cheap equality that Trino pushes into `PartitionFilters`. The second wraps the row's geometry in `ST_Contains(region_polygon, point)` so only geometries physically inside the caller's polygon survive. Because both are ANDed, a row must belong to the tenant *and* fall within the region.

5. **Pin the `filter_environment`.** The subqueries inside the filter read `tenant_regions`. Setting `filter_environment.user` to a fixed identity (`system_rls`) evaluates those lookups as a trusted principal, so the analyst does not need direct grants on the lookup table and cannot shadow it.

6. **Separate the admin rule.** The `admin` entry grants full DML with no filter, which is what your ingestion job uses. Order matters: file-based rules match top-down, so keep the narrow `analyst_.*` rule above any broad allow.

If you prefer externalized policy, swap `access-control.name=file` for `access-control.name=opa` and return the same expression from a Rego `rowFilters` rule; the OPA batch authorizer contract is documented in the [Trino OPA access control docs](https://trino.io/docs/current/security/opa-access-control.html).

## Common errors and fixes

| Error | Cause | Fix |
|---|---|---|
| `Column 'tenant_id' cannot be resolved` in filter | Filter expression references a column not projected by the base scan | Filters see all base table columns even if the user's query omits them; check the column name and that the rule targets the right table |
| Analyst sees rows outside their polygon | `ST_Contains` argument order reversed (`point, region`) | `ST_Contains(container, contained)` — the polygon is the first argument, the geometry is second |
| `Access Denied: Cannot select from table tenant_regions` | Subquery evaluated as the analyst, who lacks grants | Set `filter_environment.user` to a principal that can read the lookup table |
| Query scans all tenant partitions | Only the `ST_Contains` conjunct pushed down; equality lost | Confirm `tenant_id` is in the partition spec and appears in `EXPLAIN` under `PartitionFilters` |

## Verification

Confirm two things: that the filter prunes partitions before spatial evaluation, and that cross-region geometries are invisible. Impersonate `analyst_west` (via `--user analyst_west` in the CLI) and inspect the plan and the rows.

```sql
-- As analyst_west
EXPLAIN (TYPE DISTRIBUTED)
SELECT asset_id, label FROM iceberg.gis.spatial_assets;
-- Expect a dynamic/partition filter on tenant_id = 'acme_west' at the
-- TableScan, with the ST_Contains predicate applied as a filter above it.

SELECT asset_id, label,
       ST_AsText(ST_GeomFromBinary(geom_wkb)) AS geom
FROM iceberg.gis.spatial_assets
ORDER BY asset_id;
-- Returns only asset_id 1 and 2 (inside the west polygon).
-- asset_id 3 (acme_east / NYC) is absent even without an explicit WHERE.

-- Negative check: try to reach another tenant explicitly.
SELECT count(*) FROM iceberg.gis.spatial_assets
WHERE tenant_id = 'acme_east';
-- Returns 0: the injected filter ANDs with the user predicate,
-- so the east tenant remains unreachable.
```

The `count(*)` returning `0` for an explicit cross-tenant probe is the decisive test: the row filter is not a view the user can bypass, it is appended to every access path, including aggregates. For cross-catalog spatial work where the same principle applies to federated sources, see [spatial joins across catalogs with Trino](/spatial-query-engines-compute/trino-spatial-sql-federation/spatial-joins-across-catalogs-with-trino/).

<figure class="diagram">
<svg viewBox="0 0 760 250" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Trino query flow showing row filter injection before Iceberg partition pruning and spatial evaluation">
<title>Row filter injection in the Trino analyzer</title>
<desc>An analyst query enters the Trino analyzer, the file-based access control appends a tenant equality and an ST_Contains predicate, then the Iceberg connector prunes tenant partitions before geometry evaluation returns only authorized rows.</desc>
<defs>
<marker id="arw-trino-rls" markerWidth="9" markerHeight="9" refX="7" refY="4" orient="auto"><path d="M0 0 L9 4 L0 8 z" fill="#0e6e7d"/></marker>
</defs>
<rect x="0" y="0" width="760" height="250" fill="#f7fbfc"/>
<rect x="20" y="95" width="120" height="60" rx="6" fill="#ffffff" stroke="#cfe3e7"/>
<text x="80" y="120" text-anchor="middle" font-family="sans-serif" font-size="13" fill="#0d3b45">analyst_west</text>
<text x="80" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">SELECT * FROM assets</text>
<rect x="185" y="70" width="170" height="110" rx="6" fill="#ffffff" stroke="#6a3d9a"/>
<text x="270" y="92" text-anchor="middle" font-family="sans-serif" font-size="13" fill="#0d3b45">Analyzer + access control</text>
<text x="270" y="118" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#6a3d9a">inject: tenant_id = 'acme_west'</text>
<text x="270" y="138" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#6a3d9a">AND ST_Contains(region, geom)</text>
<text x="270" y="162" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">filter_environment=system_rls</text>
<rect x="400" y="70" width="150" height="110" rx="6" fill="#ffffff" stroke="#2f6e49"/>
<text x="475" y="92" text-anchor="middle" font-family="sans-serif" font-size="13" fill="#0d3b45">Iceberg connector</text>
<text x="475" y="118" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">prune tenant partitions</text>
<text x="475" y="138" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">then eval ST_Contains</text>
<text x="475" y="162" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">skip acme_east files</text>
<rect x="600" y="95" width="140" height="60" rx="6" fill="#ffffff" stroke="#9a5a17"/>
<text x="670" y="120" text-anchor="middle" font-family="sans-serif" font-size="13" fill="#0d3b45">rows 1, 2</text>
<text x="670" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">inside west polygon</text>
<line x1="140" y1="125" x2="182" y2="125" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-trino-rls)"/>
<line x1="355" y1="125" x2="397" y2="125" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-trino-rls)"/>
<line x1="550" y1="125" x2="597" y2="125" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-trino-rls)"/>
</svg>
</figure>

Because the filter is bound in the access control layer rather than a view, it survives `JOIN`, `UNION`, and aggregate rewrites, which is the property that distinguishes durable row-level security from a maskable convenience view. For the Databricks equivalent using declarative row filter functions, compare [row-level security in Databricks Unity Catalog for GIS data](/spatial-lakehouse-fundamentals-architecture/security-boundaries-for-gis-data/row-level-security-in-databricks-unity-catalog-for-gis/), and consult the [Trino file-based access control reference](https://trino.io/docs/current/security/file-system-access-control.html) for the full rule grammar.
