# Managing Spatial Schema Evolution in Open Table Formats

Silent geometry drift during schema evolution remains the primary failure vector in production spatial lakehouses. When engineering teams execute `ALTER TABLE` operations on spatial columns, open table formats advance the schema ID but treat underlying WKB/WKT payloads as opaque binary. Compute engines defer spatial validation until query execution, triggering downstream `ST_*` function failures, spatial index desynchronization, and SRID corruption. This guide details a deterministic, additive migration workflow to configure and automate backward-incompatible spatial type transitions without breaking read compatibility.

## Versioning Mechanics and Spatial Metadata Isolation

Spatial tables must operate as versioned state machines rather than static file collections. As documented in [Spatial Lakehouse Fundamentals & Architecture](/spatial-lakehouse-fundamentals-architecture/), spatial metadata—including SRID assignments, coordinate precision, and bounding box constraints—resides outside core manifest files. Schema evolution does not automatically propagate these constraints to the query planner. Under [Open Table Format Versioning](/spatial-lakehouse-fundamentals-architecture/open-table-format-versioning/), both Iceberg and Delta track structural changes via incremental schema IDs and snapshot lineage. However, spatial type mutations are inherently backward-incompatible. In-place column mutations force readers to deserialize legacy payloads against new type definitions, causing silent drift. The only production-safe pattern is additive evolution: provision a target column, transform payloads via spatial UDFs, and execute a metadata-level column swap.

## Engine-Specific Type Resolution

Iceberg and Delta implement spatial columns differently, which dictates migration syntax, validation gates, and compute engine configurations.

**Apache Iceberg** maps `geometry` and `geography` logical types to `binary` storage with custom metadata annotations. Iceberg catalogs track schema IDs but do not enforce SRID consistency across partitions. You must explicitly register target SRIDs in table properties and ensure compute engines load matching spatial extensions. Vectorized reads bypass WKB deserialization overhead but require strict alignment between the physical binary layout and the logical type definition. Enable `spark.sql.iceberg.vectorization.enabled=true` only after payload transformation completes.

**Delta Lake** lacks native spatial logical types. Spatial data is stored as `binary` or `string` with validation deferred to external UDFs. Delta enforces schema evolution via `delta.columnMapping.mode = 'name'` and `spark.databricks.delta.schema.autoMerge.enabled = true`. Because Delta does not parse spatial semantics at the metadata layer, you must implement explicit WKB validation gates before committing schema changes. The [Delta Lake Schema Evolution documentation](https://docs.delta.io/latest/delta-update.html#schema-evolution) confirms that type widening is permitted, but spatial binary truncation during `ALTER` operations will silently corrupt geometries.

## Automated Additive Migration Workflow

Execute the following deterministic sequence to migrate from `GEOMETRY` (planar) to `GEOGRAPHY` (ellipsoidal) or enforce explicit SRID constraints.

### Step 1: Pre-Migration Validation & Schema Freeze
Audit existing payloads for mixed SRIDs, invalid topologies, or truncated WKB. Block the migration if validation fails.

```sql
-- PySpark SQL: Validate existing WKB payloads
SELECT 
  id,
  ST_IsValid(ST_GeomFromWKB(geom_wkb)) AS topology_valid,
  ST_SRID(ST_GeomFromWKB(geom_wkb)) AS current_srid,
  LENGTH(geom_wkb) AS wkb_byte_length
FROM spatial_lakehouse.raw_assets
WHERE NOT ST_IsValid(ST_GeomFromWKB(geom_wkb))
   OR ST_SRID(ST_GeomFromWKB(geom_wkb)) NOT IN (4326, 3857);
```

### Step 2: Additive Column Provisioning
Introduce the target column without modifying existing data. Configure engine-specific parameters to prevent automatic compaction during the transition.

```sql
-- Iceberg
ALTER TABLE spatial_lakehouse.assets 
ADD COLUMN geom_geog GEOGRAPHY(SRID=4326);

-- Delta
ALTER TABLE spatial_lakehouse.assets 
ADD COLUMN geom_geog BINARY;
```

### Step 3: Payload Transformation & SRID Enforcement
Transform legacy payloads using spatial UDFs. Enforce explicit coordinate system transformation and topology validation. Disable vectorized reads during transformation to prevent deserialization mismatches.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

spark = SparkSession.builder \
    .config("spark.sql.iceberg.vectorization.enabled", "false") \
    .config("spark.databricks.delta.optimizeWrite.enabled", "false") \
    .getOrCreate()

# Transform WKB -> GEOGRAPHY with explicit SRID casting and topology validation
df = spark.table("spatial_lakehouse.assets")
df_transformed = df.withColumn(
    "geom_geog",
    expr("ST_Transform(ST_GeomFromWKB(geom_wkb), 4326)")
).filter(expr("ST_IsValid(geom_geog) = true"))

df_transformed.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("spatial_lakehouse.assets_staging")
```

### Step 4: Index Rebuild & Metadata Swap
Rebuild spatial indexes (Z-Order/Hilbert curves) on the new column. Execute a metadata-level column swap to preserve snapshot lineage.

```sql
-- Iceberg: Rebuild sort order for spatial partition pruning
ALTER TABLE spatial_lakehouse.assets_staging 
WRITE ORDERED BY geom_geog;

-- Delta: Optimize data skipping and Z-Order
OPTIMIZE spatial_lakehouse.assets_staging 
ZORDER BY (geom_geog);

-- Metadata swap (Iceberg/Delta compatible)
ALTER TABLE spatial_lakehouse.assets RENAME COLUMN geom_wkb TO geom_wkb_legacy;
ALTER TABLE spatial_lakehouse.assets_staging RENAME COLUMN geom_geog TO geom_geog;
-- Swap table pointers via catalog API or metastore transaction
```

### Step 5: Post-Migration Audit & Cleanup
Verify snapshot lineage, confirm spatial index statistics, and drop legacy columns after a 7-day observation window.

```sql
-- Validate index alignment and query planner stats
DESCRIBE EXTENDED spatial_lakehouse.assets_staging;
SELECT COUNT(*) FROM spatial_lakehouse.assets_staging WHERE geom_geog IS NULL;
```

## Failure Modes and Debugging Protocols

| Symptom | Root Cause | Resolution |
|---------|------------|------------|
| `ST_*` returns `NULL` or throws `IllegalArgumentException: Invalid WKB` | Mixed SRID payloads or binary truncation during `ALTER` | Disable vectorized reads (`spark.sql.iceberg.vectorization.enabled=false`), run explicit `ST_Transform`, validate with `ST_IsValid`, then re-enable. |
| Spatial index desync after compaction | Z-Order/Hilbert curves computed on legacy column bounds | Rebuild sort order on the target column, verify `delta.dataSkippingNumIndexedCols` or Iceberg sort metadata, and run `OPTIMIZE` with spatial partitioning. |
| Query planner ignores spatial predicates | Missing SRID annotation in table properties | Register explicit SRID in table metadata (`'write.spatial.srid'='4326'`), ensure compute engine spatial extension matches the OGC Simple Features specification. |
| Backward-incompatible read failures during transition | Consumers reading legacy schema against new manifest | Enforce schema ID pinning via `spark.sql.iceberg.schema.id` or Delta `timeTravel` until all downstream pipelines consume the new column. |

Automating spatial schema evolution requires strict adherence to additive patterns, explicit SRID enforcement, and engine-specific configuration gates. By isolating geometry drift to a controlled migration phase and validating payloads before committing schema changes, platform teams can maintain read compatibility while advancing spatial data models.