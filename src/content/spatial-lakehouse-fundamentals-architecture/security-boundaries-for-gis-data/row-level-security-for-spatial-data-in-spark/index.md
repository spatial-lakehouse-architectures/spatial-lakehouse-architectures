# Row-Level Security for Spatial Data in Spark

This guide enforces spatial entitlements in Spark SQL without giving up partition pruning, using a broadcast entitlement table and a two-stage predicate that keeps the exact geometry test off the critical path.

## Context and prerequisites

Spark has no built-in row-filter mechanism equivalent to a catalog-managed policy, so spatial entitlement is enforced through a secured view plus a session that cannot reach the base table. This recipe runs on Spark 3.5 with Iceberg 1.4 and Sedona for the geometry functions; the policy design it implements is set out in [security boundaries for GIS data](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/security-boundaries-for-gis-data/), and the equivalent catalog-managed approaches in [row-level security for spatial data in Trino](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/security-boundaries-for-gis-data/row-level-security-for-spatial-data-in-trino/) and [row-level security in Databricks Unity Catalog for GIS](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/security-boundaries-for-gis-data/row-level-security-in-databricks-unity-catalog-for-gis/).

## Where the enforcement boundary sits

<figure class="diagram">
<svg viewBox="0 0 764 212" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Spark enforcement layering: storage permissions deny direct object access, the session reaches only the secured view, and the view applies the entitlement join before any geometry evaluation">
<defs>
<marker id="rlsspark-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#0e6e7d"/></marker>
</defs>
<rect x="0" y="0" width="764" height="212" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Three layers, because a view alone is not a boundary</text>
<rect x="26" y="60" width="230" height="140" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="141" y="90" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">storage permissions</text>
<text x="141" y="118" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">the job role, not the user,</text>
<text x="141" y="138" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">can read the bucket prefix</text>
<text x="141" y="168" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">without this, nothing else matters</text>
<rect x="274" y="60" width="230" height="140" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="389" y="90" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">catalog grants</text>
<text x="389" y="118" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">base table not selectable</text>
<text x="389" y="138" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">only the secured view is</text>
<text x="389" y="168" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">the view must be the only path</text>
<rect x="522" y="60" width="230" height="140" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="637" y="90" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">the view's predicate</text>
<text x="637" y="118" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">entitlement cells first</text>
<text x="637" y="138" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">geometry test at the border</text>
<text x="637" y="168" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">correctness and speed together</text>
</svg>
</figure>

The middle layer is where Spark deployments most often fail. A secured view is only a control if the base table is unreachable, and in a shared session a user who knows the table name can simply read it. Where the catalog cannot express that grant, the enforcement has to move up into the job submission layer — a submitted job runs with a fixed view reference and users do not get an interactive session against the catalog at all.

## Complete working solution

```python
from pyspark.sql import SparkSession
from sedona.spark import SedonaContext

spark = SedonaContext.create(
    SparkSession.builder
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.apache.sedona.sql.SedonaSqlExtensions")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.autoBroadcastJoinThreshold", 64 * 1024 * 1024)
    .getOrCreate())

PRINCIPAL = spark.conf.get("spark.app.principal")     # set at submit time

# The entitlement decomposition: interior cells need no geometry test.
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW my_cells AS
SELECT cell_id, is_partial
FROM governance.entitlement_cells
WHERE principal = '{PRINCIPAL}'
""").cache()

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW my_area AS
SELECT ST_Union_Aggr(area_geom) AS area
FROM governance.entitlements
WHERE principal = '{PRINCIPAL}'
""")

secured = spark.sql("""
SELECT /*+ BROADCAST(c) */
       t.asset_id, t.event_ts, t.geom_wkb, t.h3_r5
FROM   lakehouse.spatial.telemetry t
JOIN   my_cells c ON t.h3_r5 = c.cell_id
WHERE  c.is_partial = false
   OR  ST_Contains((SELECT area FROM my_area),
                   ST_GeomFromWKB(t.geom_wkb))
""")
secured.createOrReplaceTempView("secure_telemetry")
```

Every downstream query reads `secure_telemetry`. The join on `h3_r5` is what preserves pruning: it is an equality against the partition column, so Spark restricts the scan to the entitled partitions before any geometry is decoded.

## Step-by-step walkthrough

1. **Take the principal from the submission, not from the query.** A principal read from a session variable a user can set is not a security control. Bind it at submit time, from the authenticated identity, and treat any job that does not supply it as a failure.

2. **Broadcast the entitlement cells.** The cell table is small — thousands of rows for a large entitlement — and broadcasting it turns the join into a local lookup with no shuffle. The hint is important because Spark's size estimate for a cached temp view is frequently pessimistic.

3. **Split on `is_partial`.** Cells wholly inside the entitled area need no geometry evaluation; membership proves containment. On a typical entitlement, over ninety percent of matching rows take this path and never touch GEOS.

4. **Evaluate the exact predicate only at the border.** The subquery resolving the union of entitlement geometries is evaluated once and broadcast; the containment test then runs on the small minority of rows in partial cells.

5. **Cache the cell view.** It is read by every query in the session, and recomputing it per query adds a catalog round trip to each one.

## Common errors and fixes

| Symptom | Cause | Fix |
|---|---|---|
| Full scan despite the join | `h3_r5` is not the partition column, or is cast | Confirm the partition spec; avoid casts in the join key |
| A principal with no entitlements sees everything | Empty cell view became a no-op join | Test the empty case explicitly; an inner join must return zero rows |
| Query slows down over months | Entitlement cell table has grown past broadcast threshold | Raise the threshold, or coarsen the decomposition resolution |
| Users can still read the base table | Catalog grant missing | Revoke select on the base table; a view is not a boundary on its own |
| Results differ between two users' sessions | Entitlement decomposition stale for one | Rebuild the decomposition on entitlement change, and version it |

## Verification

<figure class="diagram">
<svg viewBox="0 0 762 234" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Four security tests for the Spark secured view: an empty entitlement returns zero rows, an out of area point is excluded, a join to an unsecured table cannot reintroduce rows, and the base table is not selectable">
<rect x="0" y="0" width="762" height="234" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Four tests, each catching a different bypass</text>
<rect x="30" y="58" width="352" height="76" rx="8" fill="#ffffff" stroke="#0e6e7d" stroke-width="2"/>
<text x="206" y="84" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">empty entitlement</text>
<text x="206" y="108" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">must return exactly zero rows</text>
<rect x="398" y="58" width="352" height="76" rx="8" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<text x="574" y="84" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">point just outside the border</text>
<text x="574" y="108" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">must be excluded, not rounded in</text>
<rect x="30" y="146" width="352" height="76" rx="8" fill="#ffffff" stroke="#9a5a17" stroke-width="2"/>
<text x="206" y="172" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">join to an unsecured table</text>
<text x="206" y="196" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">must not reintroduce filtered rows</text>
<rect x="398" y="146" width="352" height="76" rx="8" fill="#ffffff" stroke="#6a3d9a" stroke-width="2"/>
<text x="574" y="172" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">base table access</text>
<text x="574" y="196" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">must fail for a data user</text>
</svg>
</figure>

```python
def test_empty_entitlement_returns_nothing(spark):
    spark.sql("CREATE OR REPLACE TEMP VIEW my_cells AS "
              "SELECT CAST(NULL AS BIGINT) cell_id, false is_partial WHERE false")
    n = spark.sql("SELECT count(*) c FROM secure_telemetry").collect()[0]["c"]
    assert n == 0, "empty entitlement leaked rows"

def test_pruning_survives_the_policy(spark):
    plan = spark.sql("SELECT count(*) FROM secure_telemetry "
                     "WHERE event_day = DATE '2026-03-11'"
                     ).queryExecution.executedPlan.toString()
    assert "PushedFilters" in plan and "h3_r5" in plan, "entitlement broke pushdown"
```

The second test is the one that keeps the control usable. A policy that is correct and turns every query into a full scan will be worked around by whoever owns the compute budget, and the workaround is always worse than the policy.

## Keeping the entitlement current

The decomposition is derived data with a dependency on both the entitlement geometries and the grid resolution. Rebuild it when either changes, version it so a query can state which version it used, and swap versions by pointer rather than updating rows in place — an atomic swap makes a revocation take effect between queries rather than during one, and leaves an auditable record of what each principal could see at any past moment.

Where entitlements change frequently, cache the resolved cell list per principal with a short expiry rather than reading it on every query. The trade is a bounded delay on revocation against a catalog read per query, and a two-minute expiry is usually an acceptable position on that trade — but it should be a stated position rather than an accident of implementation.

## Why Spark Is the Awkward Case

Comparing the three engines side by side makes clear why this recipe carries more machinery than its Trino and Databricks equivalents.

<figure class="diagram">
<svg viewBox="0 0 764 246" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Comparison of where the enforcement decision lives in Trino, Databricks Unity Catalog and Spark, showing that only Spark requires the application to establish the boundary itself">
<rect x="0" y="0" width="764" height="246" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Who owns the boundary</text>
<rect x="26" y="58" width="230" height="176" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="141" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">Trino</text>
<text x="141" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">system access control plugin</text>
<text x="141" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">the engine injects the filter</text>
<text x="141" y="168" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">into every plan</text>
<text x="141" y="200" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">unbypassable by SQL</text>
<rect x="274" y="58" width="230" height="176" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="389" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">Unity Catalog</text>
<text x="389" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">ROW FILTER on the table</text>
<text x="389" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">attached to the object,</text>
<text x="389" y="168" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">not to a view</text>
<text x="389" y="200" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">travels with the table</text>
<rect x="522" y="58" width="230" height="176" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="637" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">Spark</text>
<text x="637" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">a view plus a catalog grant</text>
<text x="637" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">the application must ensure</text>
<text x="637" y="168" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">the view is the only path</text>
<text x="637" y="200" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">bypassable if the grant is missing</text>
</svg>
</figure>

The consequence is that a Spark deployment must decide deliberately how users reach data. Two arrangements work. In the first, users never get an interactive session: they submit parameterised jobs, the platform binds the principal, and the job template references the secured view. In the second, interactive sessions exist but the catalog grants make the base tables unselectable and only views are readable, which requires the catalog to support object-level grants and requires somebody to audit them.

What does not work is a secured view in a workspace where the base table is also readable. That arrangement is common because it is what you get by default, it looks correct in every demonstration, and it provides no security at all against anyone who reads the view definition.

Where the platform already runs Trino or Unity Catalog for its governed surface, the pragmatic answer is often to keep Spark for scheduled transforms that run with a service identity and no per-user entitlement at all, and to route every user-facing spatial query through the engine that can enforce policy natively. That division is simpler to reason about than replicating the policy in two places, and it removes the risk of the two definitions drifting apart.

## Performance Notes

Three measurements are worth taking after the policy is in place, because a spatial entitlement can be correct and expensive in ways that only show up under load.

**Broadcast size of the cell view.** Print it once per session. A decomposition that has grown past the broadcast threshold silently becomes a shuffle join, and the query that took four seconds starts taking three minutes with no change to the SQL. Where growth is expected, coarsen the decomposition resolution rather than raising the threshold indefinitely — a larger broadcast costs memory on every executor.

**Fraction of rows taking the exact-predicate path.** Add a temporary count grouped by `is_partial` and confirm the border fraction is small. A decomposition at too coarse a resolution puts most cells in the partial category, which means the geometry test runs on most rows and the whole optimisation has evaporated. Under ten percent is a healthy figure.

**Files scanned against files available.** This is the pruning check, and it is the one that determines whether the policy is affordable at all. Run it for a representative query with and without the secured view; the file counts should be similar, because the entitlement join narrows the scan rather than widening it. A secured query that reads more files than the unsecured equivalent means the join is being applied after the scan rather than as a partition predicate.

All three are cheap to check and each catches a distinct regression. Recording them at deployment gives a baseline, and re-checking after any change to the entitlement structure catches the case where a policy change quietly made every query on the platform slower. A policy nobody can afford is a policy that will be removed.
