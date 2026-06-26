# Implementing Row-Level Security for Geospatial Datasets: Preventing Spatial Index Bypass in Lakehouse Query Planners

In production spatial lakehouse deployments, deterministic access control is a non-negotiable infrastructure requirement. The primary failure mode occurs when distributed query planners prioritize spatial index pruning over row-level security (RLS) predicates. Engines like Apache Spark, Trino, and Presto routinely push bounding-box or spatial-join predicates to the storage layer before resolving tenant or role-based access controls. This execution order triggers premature file scans, exposes spatial extents to unauthorized principals, and circumvents security boundaries during early-return optimizations. The engineering mandate is unambiguous: RLS evaluation must resolve at the metadata scan phase, strictly preceding spatial predicate execution.

## Metadata-First Access Control Architecture

To enforce this execution sequence, access control must bind directly to the table format's metadata layer. As established in [Spatial Lakehouse Fundamentals & Architecture](/spatial-lakehouse-fundamentals-architecture/), geometry columns stored as raw WKB or WKT require full deserialization by the query planner to extract spatial bounds, which forces a file scan before any security filter can apply. This architectural gap enables spatial index bypass.

Modern table formats mitigate this by exposing structured metadata and manifest-level partition pruning. The access control pipeline must treat tenant identifiers as first-class partition or metadata columns, enabling the storage layer to prune files before spatial compute begins. Refer to [Security Boundaries for GIS Data](/spatial-lakehouse-fundamentals-architecture/security-boundaries-for-gis-data/) for the threat model governing this execution order and the compliance implications of premature spatial evaluation.

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
