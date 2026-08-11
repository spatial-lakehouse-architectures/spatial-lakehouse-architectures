# Implementing Row-Level Security for Geospatial Datasets: Preventing Spatial Index Bypass in Lakehouse Query Planners

In production spatial lakehouse deployments, deterministic access control is a non-negotiable infrastructure requirement. The primary failure mode occurs when distributed query planners prioritize spatial index pruning over row-level security (RLS) predicates. Engines like Apache Spark, Trino, and Presto routinely push bounding-box or spatial-join predicates to the storage layer before resolving tenant or role-based access controls. This execution order triggers premature file scans, exposes spatial extents to unauthorized principals, and circumvents security boundaries during early-return optimizations. The engineering mandate is unambiguous: RLS evaluation must resolve at the metadata scan phase, strictly preceding spatial predicate execution.

## Metadata-First Access Control Architecture

To enforce this execution sequence, access control must bind directly to the table format's metadata layer. As established in [Spatial Lakehouse Fundamentals & Architecture](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/), geometry columns stored as raw WKB or WKT require full deserialization by the query planner to extract spatial bounds, which forces a file scan before any security filter can apply. This architectural gap enables spatial index bypass.

Modern table formats mitigate this by exposing structured metadata and manifest-level partition pruning. The access control pipeline must treat tenant identifiers as first-class partition or metadata columns, enabling the storage layer to prune files before spatial compute begins. Refer to [Security Boundaries for GIS Data](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/security-boundaries-for-gis-data/) for the threat model governing this execution order and the compliance implications of premature spatial evaluation.

## Production Configuration Blueprint

The core configuration objective is to materialize RLS predicates into the file-level metadata scan. This requires three coordinated steps: partition alignment, predicate pushdown control, and access policy enforcement.

### 1. Table DDL with RLS-Aligned Partitioning

Bind the `tenant_id` or `access_tier` directly to partition specs. Do not rely on post-scan `WHERE` clauses for security filtering.

```sql
-- Apache Iceberg
CREATE TABLE analytics.spatial_assets (
  asset_id   BIGINT,
  tenant_id  STRING  NOT NULL,
  geom       BINARY,           -- WKB
  created_at TIMESTAMP
) USING iceberg
PARTITIONED BY (tenant_id, days(created_at))
TBLPROPERTIES (
  'write.parquet.bloom-filter-enabled.column.tenant_id' = 'true',
  'write.metadata.delete-after-commit.enabled' = 'true'
);

-- Delta Lake
CREATE TABLE analytics.spatial_assets_delta (
  asset_id   BIGINT,
  tenant_id  STRING  NOT NULL,
  geom       BINARY,
  created_at TIMESTAMP
) USING delta
PARTITIONED BY (tenant_id)
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true'
);
```

### 2. Query Engine Optimization Parameters

Disable aggressive spatial predicate pushdown until the RLS filter is resolved. In Spark, this forces the Catalyst optimizer to evaluate partition filters first, preventing spatial index bypass during the initial scan.

```python
# Spark Session Configuration for RLS Precedence
# Disable dynamic partition pruning to prevent spatial predicates from
# short-circuiting before tenant_id is resolved
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "false")
# Enforce metadata-only planning for partition stat collection
spark.conf.set("spark.sql.optimizer.metadataOnly", "true")
# Disable Adaptive Query Execution in security-sensitive contexts to prevent
# the runtime optimizer from reordering security filters
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB
```

Re-enable `spark.sql.adaptive.enabled` for non-sensitive analytical sessions after validating RLS ordering.

### 3. RLS Policy Enforcement

Implement RLS as a metadata scan filter, not a compute-layer UDF. Use engine-native security abstractions (e.g., Unity Catalog row filters, Apache Ranger) or explicit query rewriting that injects `tenant_id` predicates at parse time.

```sql
-- Policy Injection Pattern (Engine-Agnostic)
-- The tenant_id predicate reaches PushedFilters and prunes partitions
-- before ST_Intersects is evaluated
SELECT asset_id, geom, created_at
FROM analytics.spatial_assets
WHERE tenant_id = current_user_tenant()
  AND ST_Intersects(
        ST_GeomFromWKB(geom),
        ST_GeomFromText('POLYGON(...)')
      )
```

## Execution Validation & Debugging

Verification requires inspecting the physical query plan to confirm filter precedence. Use `EXPLAIN FORMATTED` to trace execution order.

**Correct Execution Order:**
```
== Physical Plan ==
*(1) Project [asset_id, geom, created_at]
+- *(1) Filter (tenant_id = 'acme_corp') AND (ST_Intersects(geom, ...))
   +- *(1) FileScan iceberg analytics.spatial_assets [asset_id, tenant_id, geom, created_at]
      PartitionFilters: [isnotnull(tenant_id), (tenant_id = acme_corp)]
      PushedFilters: [IsNotNull(tenant_id), EqualTo(tenant_id, acme_corp)]
```

**Failure Mode (Spatial Index Bypass):**
```
== Physical Plan ==
*(1) Project [asset_id, geom, created_at]
+- *(1) Filter (tenant_id = 'acme_corp')
   +- *(1) SpatialJoin (ST_Intersects(geom, ...))
      +- *(1) FileScan iceberg analytics.spatial_assets [asset_id, tenant_id, geom, created_at]
         PushedFilters: [ST_Intersects(geom, ...)]  -- SECURITY FAILURE: spatial before tenant
```

### Explicit Failure Resolution Steps

1. **Symptom:** `ST_Contains` or `ST_Intersects` appears in `PushedFilters` before `tenant_id` equality.
2. **Diagnosis:** The query planner evaluated spatial bounds against the Z-order manifest before applying RLS.
3. **Resolution:**
   - Add `tenant_id` to the partition spec and rebuild the manifest.
   - Enable bloom filters on `tenant_id` to accelerate metadata pruning.
   - Set `spark.sql.optimizer.dynamicPartitionPruning.enabled=false` to force partition-first evaluation.
   - Re-run `EXPLAIN FORMATTED` and verify `tenant_id` appears in `PartitionFilters` above spatial predicates.
4. **Validation:** Execute a cross-tenant query with `EXPLAIN`. Confirm zero file reads for unauthorized partitions via the `Files Read` metric in Spark UI or Delta transaction logs.

For detailed manifest pruning behavior and spatial type specifications, consult the [Apache Iceberg Specification](https://iceberg.apache.org/spec/) and [Delta Lake Documentation](https://docs.delta.io/latest/). Enforcing this configuration guarantees that spatial compute never executes outside authorized metadata boundaries, eliminating index bypass and ensuring compliance at the storage layer.

## Why the Planner and the Policy Disagree

The failure this page opens with — a query planner that prunes on spatial statistics before the security predicate is applied — is worth drawing, because the fix follows directly from seeing where the two decisions meet.

<figure class="diagram">
<svg viewBox="0 0 720 272" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Two query plans compared: an unsafe plan where file pruning happens on user-supplied bounds before the entitlement filter, and a safe plan where the entitlement predicate is injected into the scan itself">
<defs>
<marker id="rls-plan-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#0e6e7d"/></marker>
</defs>
<rect x="0" y="0" width="720" height="272" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Where the entitlement predicate has to sit</text>
<text x="196" y="60" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#9a5a17">applied too late</text>
<rect x="72" y="76" width="248" height="42" rx="6" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="196" y="102" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">scan: prune on caller&#8217;s bbox</text>
<rect x="72" y="132" width="248" height="42" rx="6" fill="#ffffff" stroke="#9a5a17" stroke-width="2"/>
<text x="196" y="158" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">project columns</text>
<rect x="72" y="188" width="248" height="42" rx="6" fill="#ffffff" stroke="#9a5a17" stroke-width="2"/>
<text x="196" y="214" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">filter: entitlement</text>
<line x1="196" y1="118" x2="196" y2="132" stroke="#9a5a17" stroke-width="2" marker-end="url(#rls-plan-arrow)"/>
<line x1="196" y1="174" x2="196" y2="188" stroke="#9a5a17" stroke-width="2" marker-end="url(#rls-plan-arrow)"/>
<text x="196" y="256" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">correct results, but every file is opened</text>
<text x="584" y="60" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#2f6e49">applied at the scan</text>
<rect x="460" y="76" width="248" height="42" rx="6" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="584" y="102" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">scan: prune on cell IN (entitled)</text>
<rect x="460" y="132" width="248" height="42" rx="6" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<text x="584" y="158" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">filter: caller&#8217;s bbox</text>
<rect x="460" y="188" width="248" height="42" rx="6" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<text x="584" y="214" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">refine: boundary cells only</text>
<line x1="584" y1="118" x2="584" y2="132" stroke="#2f6e49" stroke-width="2" marker-end="url(#rls-plan-arrow)"/>
<line x1="584" y1="174" x2="584" y2="188" stroke="#2f6e49" stroke-width="2" marker-end="url(#rls-plan-arrow)"/>
<text x="584" y="256" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">same results, a fraction of the files</text>
</svg>
</figure>

Both plans return identical rows, so a correctness test cannot tell them apart. The difference is entirely in cost, and it is large — on a continental table, the left-hand plan reads every file in the caller's requested window regardless of entitlement, and on a table where entitlement is a small fraction of the whole, that is a hundredfold difference in bytes.

The fix is to express the entitlement as a predicate on the **partition column**, because that is the only predicate the scan operator can act on. An entitlement expressed as a geometric containment test can never be pushed into a scan, no matter how the optimiser is configured, because the scan has no geometry available before it reads the data.

## Making Entitlements Cheap to Evaluate

The entitlement cell table is the piece of machinery that makes the safe plan possible, and its design decides whether the whole scheme is fast or merely correct.

<figure class="diagram">
<svg viewBox="0 0 760 260" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Decomposition of an entitlement polygon into interior cells that need no geometry test and boundary cells that do, with the resulting counts showing how few cells require refinement">
<rect x="0" y="0" width="760" height="260" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Decomposing one entitlement area into cells</text>
<rect x="140" y="62" width="60" height="52" fill="#e6f0ea" stroke="#2f6e49" stroke-width="1.5"/>
<rect x="200" y="62" width="60" height="52" fill="#e6f0ea" stroke="#2f6e49" stroke-width="1.5"/>
<rect x="260" y="62" width="60" height="52" fill="#f2e8da" stroke="#9a5a17" stroke-width="1.5"/>
<rect x="140" y="114" width="60" height="52" fill="#e6f0ea" stroke="#2f6e49" stroke-width="1.5"/>
<rect x="200" y="114" width="60" height="52" fill="#e6f0ea" stroke="#2f6e49" stroke-width="1.5"/>
<rect x="260" y="114" width="60" height="52" fill="#f2e8da" stroke="#9a5a17" stroke-width="1.5"/>
<rect x="140" y="166" width="60" height="52" fill="#f2e8da" stroke="#9a5a17" stroke-width="1.5"/>
<rect x="200" y="166" width="60" height="52" fill="#f2e8da" stroke="#9a5a17" stroke-width="1.5"/>
<rect x="260" y="166" width="60" height="52" fill="#f2e8da" stroke="#9a5a17" stroke-width="1.5"/>
<path d="M150 78 L305 70 L312 200 L175 212 Z" fill="none" stroke="#0e6e7d" stroke-width="2.5"/>
<text x="230" y="244" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">entitlement outline over the grid</text>
<rect x="392" y="70" width="356" height="66" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="570" y="96" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">interior cells — is_partial = false</text>
<text x="570" y="118" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">membership proves containment; no ST_ call</text>
<rect x="392" y="152" width="356" height="66" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="570" y="178" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">boundary cells — is_partial = true</text>
<text x="570" y="200" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">exact test runs here, on a small minority of rows</text>
</svg>
</figure>

Two properties make the decomposition safe. It must be **conservative at the boundary** — a cell may only be marked interior when the cell's full extent lies inside the entitlement geometry, tested against the cell polygon rather than its centroid — because an incorrectly-marked interior cell leaks every row it contains. And it must be **refreshed when entitlements change**, which means treating the decomposition as derived data with an explicit dependency, not as a table somebody populates by hand.

The resolution choice trades table size against refinement cost. Coarser cells mean fewer rows in the entitlement table and a larger proportion of boundary cells; finer cells mean the opposite. A useful heuristic is to pick the resolution at which boundary cells are under 10% of the total for a typical entitlement, then check that the entitlement table stays small enough to broadcast — because if it does not, the join that applies it becomes a shuffle and the whole scheme loses its advantage.

## Testing That the Policy Cannot Be Bypassed

A row-level security implementation needs a test suite that attacks it, not one that confirms the happy path.

<figure class="diagram">
<svg viewBox="0 0 762 242" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Four bypass attempts every spatial row level security implementation should be tested against: direct object storage access, a different engine, an aggregate that reveals suppressed rows, and a join that reintroduces filtered rows">
<rect x="0" y="0" width="762" height="242" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Four bypasses worth a test each</text>
<rect x="30" y="58" width="352" height="80" rx="8" fill="#ffffff" stroke="#9a5a17" stroke-width="2"/>
<text x="206" y="84" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">read the Parquet directly</text>
<text x="206" y="110" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">expect: denied by storage permissions</text>
<rect x="398" y="58" width="352" height="80" rx="8" fill="#ffffff" stroke="#0e6e7d" stroke-width="2"/>
<text x="574" y="84" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">query through a second engine</text>
<text x="574" y="110" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">expect: same filtered result set</text>
<rect x="30" y="150" width="352" height="80" rx="8" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<text x="206" y="176" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">aggregate over the full table</text>
<text x="206" y="202" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">expect: totals reflect entitled rows only</text>
<rect x="398" y="150" width="352" height="80" rx="8" fill="#ffffff" stroke="#6a3d9a" stroke-width="2"/>
<text x="574" y="176" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">join back to an unfiltered table</text>
<text x="574" y="202" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">expect: no rows outside the entitlement</text>
</svg>
</figure>

The fourth test is the one most often missing and the one most often failed. A secured view joined against an unsecured lookup table can reintroduce filtered rows through the lookup side, particularly with an outer join, and the result looks like an ordinary query returning ordinary data. Audit every table that a secured view can be joined to, and apply the filter to the join inputs rather than only to the primary table.

## Operating the Entitlement Table

The entitlement decomposition is derived data, and like all derived data it needs an owner, a refresh schedule and a staleness metric. Treat it as a first-class table rather than as an implementation detail of the security view.

Refresh should be **event-driven with a periodic backstop**. When an entitlement is granted, revoked or its geometry edited, recompute the affected principal's cells immediately, because a revocation that takes effect at the next nightly run is a revocation that did not happen when it was requested. The periodic full rebuild exists to catch the cases the event path missed — a manual edit, a failed job, a restored backup — and running it weekly against a small table costs nothing.

The staleness metric is simply the age of the newest recomputation per principal, and it belongs on the same dashboard as the audit signals. A principal whose decomposition has not been recomputed since before their last entitlement change is being served by a stale policy, and that is a compliance fact rather than an operational one.

Keep the decomposition immutable per version rather than updating rows in place. A new version is written, validated for the conservative-containment property, and then swapped in by pointer. That makes the swap atomic, makes rollback trivial when a bad entitlement geometry produces an over-broad decomposition, and leaves an audit trail showing exactly which decomposition was in force at any past moment — which is the question that gets asked when someone reviews what a principal could see last quarter.
