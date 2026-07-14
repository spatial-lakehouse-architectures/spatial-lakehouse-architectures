# Row-Level Security in Databricks Unity Catalog for GIS Data

This recipe implements row-level security on a spatial Delta table in Databricks Unity Catalog by attaching a SQL `ROW FILTER` function that restricts rows to a caller's tenant and bounding region, plus a `COLUMN MASK` that coarsens geometry precision for unprivileged principals, and verifies both from an ordinary session.

## Context and prerequisites

Unity Catalog exposes declarative row filters and column masks as first-class SQL objects: a filter is a UDF returning `BOOLEAN` that Databricks evaluates per row before results leave the engine, and a mask is a UDF that transforms a single column's value in flight. Unlike a secured view, these bind to the base table with `ALTER TABLE`, so they cannot be sidestepped by querying the table directly. This page extends [Security Boundaries for GIS Data](/spatial-lakehouse-fundamentals-architecture/security-boundaries-for-gis-data/) and pairs with the engine-agnostic treatment in [implementing row-level security for geospatial datasets](/spatial-lakehouse-fundamentals-architecture/security-boundaries-for-gis-data/implementing-row-level-security-for-geospatial-datasets/). You need a Unity Catalog metastore, a Databricks Runtime with the built-in `ST_*` SQL spatial functions (DBR 17.1+ / SQL warehouses with geospatial functions enabled), and permission to create functions in the target schema. Geometry is stored in a Delta table using the native `GEOMETRY` type with SRID 4326.

## Complete working solution

Create the spatial Delta table, a group-to-region mapping table, then the row filter and column mask functions, and finally bind them with `ALTER TABLE`.

```sql
-- Databricks SQL (Unity Catalog, DBR 17.1+); geometry as native GEOMETRY, EPSG:4326
CREATE CATALOG IF NOT EXISTS gis_prod;
CREATE SCHEMA IF NOT EXISTS gis_prod.assets;

CREATE TABLE gis_prod.assets.spatial_assets (
    asset_id  BIGINT,
    tenant_id STRING NOT NULL,
    geom      GEOMETRY(4326),
    label     STRING
)
USING DELTA
PARTITIONED BY (tenant_id)
TBLPROPERTIES (delta.enableDeletionVectors = true);

-- Which group may see which tenant, and the bounding polygon it is scoped to.
CREATE TABLE gis_prod.assets.tenant_grants (
    group_name STRING NOT NULL,
    tenant_id  STRING NOT NULL,
    region_wkt STRING NOT NULL   -- WKT polygon, EPSG:4326
);

INSERT INTO gis_prod.assets.tenant_grants VALUES
  ('west_analysts', 'acme_west',
   'POLYGON((-123 37, -121 37, -121 39, -123 39, -123 37))'),
  ('east_analysts', 'acme_east',
   'POLYGON((-74 40, -72 40, -72 42, -74 42, -74 40))');

INSERT INTO gis_prod.assets.spatial_assets VALUES
  (1, 'acme_west', ST_GeomFromText('POINT(-122.4 37.8)', 4326), 'sf_pump'),
  (2, 'acme_west', ST_GeomFromText('POINT(-121.9 38.5)', 4326), 'delta_valve'),
  (3, 'acme_east', ST_GeomFromText('POINT(-73.9 40.7)', 4326), 'nyc_meter');
```

```sql
-- Row filter: TRUE only when the caller belongs to a group whose grant
-- both matches the row's tenant and contains the row's geometry.
CREATE OR REPLACE FUNCTION gis_prod.assets.rls_tenant_region(row_tenant STRING, row_geom GEOMETRY)
RETURN
  is_account_group_member('gis_admins')
  OR EXISTS (
    SELECT 1
    FROM gis_prod.assets.tenant_grants g
    WHERE is_account_group_member(g.group_name)
      AND g.tenant_id = row_tenant
      AND ST_Contains(ST_GeomFromText(g.region_wkt, 4326), row_geom)
  );

-- Column mask: full geometry for admins, snapped-to-grid geometry otherwise.
CREATE OR REPLACE FUNCTION gis_prod.assets.mask_geom(g GEOMETRY)
RETURN CASE
  WHEN is_account_group_member('gis_admins') THEN g
  ELSE ST_GeomFromText(
         concat('POINT(', cast(round(ST_X(g), 1) AS STRING), ' ',
                          cast(round(ST_Y(g), 1) AS STRING), ')'), 4326)
END;

-- Bind both policies to the base table.
ALTER TABLE gis_prod.assets.spatial_assets
  SET ROW FILTER gis_prod.assets.rls_tenant_region ON (tenant_id, geom);

ALTER TABLE gis_prod.assets.spatial_assets
  ALTER COLUMN geom SET MASK gis_prod.assets.mask_geom;
```

After the two `ALTER TABLE` statements, every reader of `spatial_assets` — dashboards, notebooks, JDBC clients — transparently receives filtered rows with masked coordinates, no view required.

## Step-by-step walkthrough

1. **Native `GEOMETRY(4326)` column.** Storing geometry with an explicit SRID lets Databricks apply spatial functions without per-query parsing and lets the optimizer keep spatial statistics. Partitioning by `tenant_id` gives Delta a coarse pruning key that the row filter's tenant equality can exploit.

2. **Policy driven by `tenant_grants`.** Rather than hardcoding group logic, the mapping table joins a Unity Catalog account group to a tenant and a bounding polygon. Adding a customer is an `INSERT`; the filter picks it up on the next query with no redeploy.

3. **`is_account_group_member` for identity.** Unity Catalog resolves the caller's group membership server-side. The filter returns `TRUE` when an admin, or when the caller is in a granted group whose row matches on tenant *and* whose polygon `ST_Contains` the row geometry. The two spatial and tenant conditions are ANDed, so a west analyst cannot see east rows even if a future grant row misconfigures the tenant.

4. **Filter signature matches `ON (...)`.** The function parameters `(row_tenant, row_geom)` are bound positionally to `ON (tenant_id, geom)`. Databricks passes those two columns into the filter for every candidate row; the function must return `BOOLEAN`.

5. **Column mask coarsens precision.** Even authorized-to-see-the-row analysts should not always get survey-grade coordinates. `mask_geom` returns the full geometry for admins and otherwise snaps to one decimal degree (~11 km), a defense against coordinate-precision leakage. Masks operate independently of the row filter and stack with it.

6. **`ALTER TABLE ... SET ROW FILTER / SET MASK`.** This is the binding step. Because it modifies the table object, not a view, there is no unfiltered path to the data for principals who only hold `SELECT`. Dropping a policy is `ALTER TABLE ... DROP ROW FILTER`. The full grammar lives in the [Unity Catalog row filters and column masks documentation](https://docs.databricks.com/aws/en/tables/row-and-column-filters).

## Common errors and fixes

| Error | Cause | Fix |
|---|---|---|
| `The row filter function must return BOOLEAN` | Function body returns a non-boolean (e.g. a `GEOMETRY`) | Ensure the top-level expression evaluates to TRUE/FALSE; wrap spatial checks in `EXISTS`/`ST_Contains` |
| `Number of arguments does not match` on `ALTER TABLE` | `ON (...)` column list length differs from the function's parameters | Align the `ON` columns with the function signature order and arity |
| All rows disappear for every user | No matching `tenant_grants` row, or caller not in any listed group | Verify `is_account_group_member` group names exactly match account groups (case-sensitive) |
| Mask throws on NULL geometry | `ST_X`/`ST_Y` called on a NULL `geom` | Add `WHEN g IS NULL THEN NULL` as the first `CASE` branch |

## Verification

Validate as a non-admin member of `west_analysts`. The filter should hide the east tenant and the mask should blunt coordinate precision, all without any `WHERE` clause.

```sql
-- Run as a user in group west_analysts (not gis_admins)
SELECT asset_id, label, ST_AsText(geom) AS geom_txt
FROM gis_prod.assets.spatial_assets
ORDER BY asset_id;
-- Expected: only asset_id 1 and 2. Coordinates rounded, e.g. POINT(-122.4 37.8).
-- asset_id 3 (acme_east) never appears.

-- Negative probe: explicit cross-tenant request still returns nothing.
SELECT count(*) AS leaked
FROM gis_prod.assets.spatial_assets
WHERE tenant_id = 'acme_east';
-- leaked = 0

-- Confirm the policies are bound.
DESCRIBE EXTENDED gis_prod.assets.spatial_assets;
-- Look for a "Row Filter" line naming rls_tenant_region and a masked geom column.
```

A `leaked` count of `0` on the explicit east probe is the proof that the row filter is enforced at the table, not layered as a bypassable view. Re-running the same query as a `gis_admins` member returns all three rows with full-precision geometry, confirming the policy branches on identity.

<figure class="diagram">
<svg viewBox="0 0 760 250" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Unity Catalog applying a row filter then a column mask to a spatial Delta table before returning results">
<title>Row filter and column mask on a spatial Delta table</title>
<desc>A non-admin query hits the Delta table, Unity Catalog evaluates the row filter to drop other tenants and out-of-region rows, applies the geometry column mask to coarsen coordinates, then returns filtered masked rows.</desc>
<defs>
<marker id="arw-uc-rls" markerWidth="9" markerHeight="9" refX="7" refY="4" orient="auto"><path d="M0 0 L9 4 L0 8 z" fill="#0e6e7d"/></marker>
</defs>
<rect x="0" y="0" width="760" height="250" fill="#f7fbfc"/>
<rect x="18" y="95" width="120" height="60" rx="6" fill="#ffffff" stroke="#cfe3e7"/>
<text x="78" y="120" text-anchor="middle" font-family="sans-serif" font-size="13" fill="#0d3b45">west_analyst</text>
<text x="78" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">SELECT geom ...</text>
<rect x="180" y="80" width="150" height="90" rx="6" fill="#ffffff" stroke="#2f6e49"/>
<text x="255" y="102" text-anchor="middle" font-family="sans-serif" font-size="13" fill="#0d3b45">ROW FILTER</text>
<text x="255" y="126" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">tenant match AND</text>
<text x="255" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">ST_Contains(region)</text>
<rect x="370" y="80" width="150" height="90" rx="6" fill="#ffffff" stroke="#6a3d9a"/>
<text x="445" y="102" text-anchor="middle" font-family="sans-serif" font-size="13" fill="#0d3b45">COLUMN MASK</text>
<text x="445" y="126" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#6a3d9a">round coords</text>
<text x="445" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#6a3d9a">~11 km grid</text>
<rect x="560" y="80" width="180" height="90" rx="6" fill="#ffffff" stroke="#9a5a17"/>
<text x="650" y="102" text-anchor="middle" font-family="sans-serif" font-size="13" fill="#0d3b45">result set</text>
<text x="650" y="126" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">rows 1, 2 only</text>
<text x="650" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">coarse geometry</text>
<line x1="138" y1="125" x2="177" y2="125" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-uc-rls)"/>
<line x1="330" y1="125" x2="367" y2="125" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-uc-rls)"/>
<line x1="520" y1="125" x2="557" y2="125" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-uc-rls)"/>
</svg>
</figure>

The Databricks model is declarative where Trino's is provider-driven; for the open-source counterpart that injects the same predicate through the access control SPI, see [row-level security for spatial data in Trino](/spatial-lakehouse-fundamentals-architecture/security-boundaries-for-gis-data/row-level-security-for-spatial-data-in-trino/), and consult the [Databricks geospatial functions reference](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-functions-builtin) for the full `ST_*` catalog.
