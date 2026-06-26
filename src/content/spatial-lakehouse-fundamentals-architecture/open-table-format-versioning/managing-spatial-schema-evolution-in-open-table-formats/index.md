# Managing Spatial Schema Evolution in Open Table Formats

Silent geometry drift during schema evolution remains the primary failure vector in production spatial lakehouses. When engineering teams execute `ALTER TABLE` operations on spatial columns, open table formats advance the schema ID but treat underlying WKB/WKT payloads as opaque binary. Compute engines defer spatial validation until query execution, triggering downstream `ST_*` function failures, spatial index desynchronization, and SRID corruption. This guide details a deterministic, additive migration workflow to configure and automate backward-incompatible spatial type transitions without breaking read compatibility.

## Versioning Mechanics and Spatial Metadata Isolation

Spatial tables must operate as versioned state machines rather than static file collections. As documented in [Spatial Lakehouse Fundamentals & Architecture](/spatial-lakehouse-fundamentals-architecture/), spatial metadata—including SRID assignments, coordinate precision, and bounding box constraints—resides outside core manifest files. Schema evolution does not automatically propagate these constraints to the query planner. Under [Open Table Format Versioning](/spatial-lakehouse-fundamentals-architecture/open-table-format-versioning/), both Iceberg and Delta track structural changes via incremental schema IDs and snapshot lineage. However, spatial type mutations are inherently backward-incompatible. In-place column mutations force readers to deserialize legacy payloads against new type definitions, causing silent drift. The only production-safe pattern is additive evolution: provision a target column, transform payloads via spatial UDFs, and execute a metadata-level column swap.

## Engine-Specific Type Resolution

Iceberg and Delta implement spatial columns differently, which dictates migration syntax, validation gates, and compute engine configurations.

**Apache Iceberg** maps spatial geometry to `binary` storage with WKB encoding. Iceberg catalogs track schema IDs but do not enforce SRID consistency across partitions. You must explicitly record target SRIDs in table properties and ensure compute engines load matching spatial extensions. Enable `spark.sql.iceberg.vectorization.enabled=true` only after payload transformation completes.

**Delta Lake** lacks native spatial logical types. Spatial data is stored as `binary` or `string` with validation deferred to external UDFs. Delta enforces schema evolution via `delta.columnMapping.mode = 'name'`. Because Delta does not parse spatial semantics at the metadata layer, you must implement explicit WKB validation gates before committing schema changes. The [Delta Lake Schema Evolution documentation](https://docs.delta.io/latest/delta-update.html#schema-evolution) confirms that type widening is permitted, but spatial binary truncation during `ALTER` operations will silently corrupt geometries.

## Automated Additive Migration Workflow

Execute the following deterministic sequence to migrate spatial columns or enforce explicit SRID constraints.

### Step 1: Pre-Migration Validation & Schema Freeze

Audit existing payloads for mixed SRIDs, invalid topologies, or truncated WKB. Block the migration if validation fails.

```sql
-- Spark SQL with Apache Sedona extensions: validate existing WKB payloads
SELECT
  id,
  ST_IsValid(ST_GeomFromWKB(geom_wkb)) AS topology_valid,
  ST_SRID(ST_GeomFromWKB(geom_wkb))    AS current_srid,
  LENGTH(geom_wkb)                      AS wkb_byte_length
FROM spatial_lakehouse.raw_assets
WHERE NOT ST_IsValid(ST_GeomFromWKB(geom_wkb))
   OR ST_SRID(ST_GeomFromWKB(geom_wkb)) NOT IN (4326, 3857);
```

### Step 2: Additive Column Provisioning

Introduce the target column without modifying existing data. Configure engine-specific parameters to prevent automatic compaction during the transition.

```sql
-- Iceberg: add new binary column for transformed geometry
ALTER TABLE spatial_lakehouse.assets
ADD COLUMN geom_wkb_v2 BINARY;

-- Delta: same approach
ALTER TABLE spatial_lakehouse.assets
ADD COLUMN geom_wkb_v2 BINARY;
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

# Transform WKB to target CRS (EPSG:4326) using Sedona ST_Transform
df = spark.table("spatial_lakehouse.assets")
df_transformed = df.withColumn(
    "geom_wkb_v2",
    expr("ST_AsBinary(ST_Transform(ST_GeomFromWKB(geom_wkb), 'EPSG:4326'))")
).filter(
    expr("ST_IsValid(ST_GeomFromWKB(geom_wkb_v2)) = true")
)

df_transformed.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("spatial_lakehouse.assets_staging")
```

### Step 4: Index Rebuild & Metadata Swap

Rebuild spatial indexes (Z-Order) on the new column. Execute a metadata-level column rename to preserve snapshot lineage.

```sql
-- Delta: Optimize data skipping and Z-Order on new column
OPTIMIZE spatial_lakehouse.assets_staging
ZORDER BY (geom_wkb_v2);

-- Iceberg: Rewrite files with sort on new column's bbox derivatives
-- (bbox columns must already exist; sort on those)
CALL spark_catalog.system.rewrite_data_files(
  table => 'spatial_lakehouse.assets_staging',
  strategy => 'sort',
  sort_order => 'bbox_min_x ASC, bbox_min_y ASC'
);

-- Rename legacy column and promote new column
ALTER TABLE spatial_lakehouse.assets_staging
RENAME COLUMN geom_wkb TO geom_wkb_legacy;

ALTER TABLE spatial_lakehouse.assets_staging
RENAME COLUMN geom_wkb_v2 TO geom_wkb;
```

### Step 5: Post-Migration Audit & Cleanup

Verify snapshot lineage, confirm spatial index statistics, and drop legacy columns after a 7-day observation window.

```sql
-- Validate index alignment and query planner stats
DESCRIBE EXTENDED spatial_lakehouse.assets_staging;
SELECT COUNT(*) FROM spatial_lakehouse.assets_staging WHERE geom_wkb IS NULL;

-- After observation window, drop legacy column
ALTER TABLE spatial_lakehouse.assets_staging DROP COLUMN geom_wkb_legacy;
```

## Failure Modes and Debugging Protocols

| Symptom | Root Cause | Resolution |
|---------|------------|------------|
| `ST_*` returns `NULL` or throws `IllegalArgumentException: Invalid WKB` | Mixed SRID payloads or binary truncation during `ALTER` | Disable vectorized reads (`spark.sql.iceberg.vectorization.enabled=false`), run explicit `ST_Transform`, validate with `ST_IsValid`, then re-enable. |
| Spatial index desync after compaction | Z-Order computed on legacy column bounds | Rebuild sort order on the target column; run `OPTIMIZE` with spatial partitioning on new bbox columns. |
| Query planner ignores spatial predicates | Missing SRID annotation in table properties | Register explicit SRID in table metadata as a table property (`'crs'='EPSG:4326'`); ensure the compute engine spatial extension matches the OGC Simple Features specification. |
| Backward-incompatible read failures during transition | Consumers reading legacy schema against new manifest | Enforce schema ID pinning via Iceberg time-travel (`VERSION AS OF <snapshot_id>`) or Delta `VERSION AS OF` until all downstream pipelines consume the new column. |

Automating spatial schema evolution requires strict adherence to additive patterns, explicit SRID enforcement, and engine-specific configuration gates. By isolating geometry drift to a controlled migration phase and validating payloads before committing schema changes, platform teams can maintain read compatibility while advancing spatial data models.
