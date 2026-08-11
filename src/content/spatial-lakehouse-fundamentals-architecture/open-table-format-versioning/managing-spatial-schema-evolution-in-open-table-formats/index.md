# Managing Spatial Schema Evolution in Open Table Formats

Silent geometry drift during schema evolution remains the primary failure vector in production spatial lakehouses. When engineering teams execute `ALTER TABLE` operations on spatial columns, open table formats advance the schema ID but treat underlying WKB/WKT payloads as opaque binary. Compute engines defer spatial validation until query execution, triggering downstream `ST_*` function failures, spatial index desynchronization, and SRID corruption. This guide details a deterministic, additive migration workflow to configure and automate backward-incompatible spatial type transitions without breaking read compatibility.

## Versioning Mechanics and Spatial Metadata Isolation

Spatial tables must operate as versioned state machines rather than static file collections. As documented in [Spatial Lakehouse Fundamentals & Architecture](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/), spatial metadata—including SRID assignments, coordinate precision, and bounding box constraints—resides outside core manifest files. Schema evolution does not automatically propagate these constraints to the query planner. Under [Open Table Format Versioning](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/open-table-format-versioning/), both Iceberg and Delta track structural changes via incremental schema IDs and snapshot lineage. However, spatial type mutations are inherently backward-incompatible. In-place column mutations force readers to deserialize legacy payloads against new type definitions, causing silent drift. The only production-safe pattern is additive evolution: provision a target column, transform payloads via spatial UDFs, and execute a metadata-level column swap.

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

## The Three Kinds of Spatial Schema Change

Schema changes on a geometry column separate into three groups with completely different risk profiles, and conflating them is what makes evolution feel dangerous.

<figure class="diagram">
<svg viewBox="0 0 764 266" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three categories of spatial schema change: additive changes that are always safe, reinterpreting changes that alter what existing bytes mean, and structural changes that require both a rewrite and a consumer migration">
<rect x="0" y="0" width="764" height="266" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Three categories, three levels of risk</text>
<rect x="26" y="58" width="230" height="196" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="141" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#2f6e49">additive</text>
<text x="141" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">new attribute column</text>
<text x="141" y="136" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">new derived bbox column</text>
<text x="141" y="158" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">second geometry column</text>
<text x="141" y="180" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">widened numeric type</text>
<text x="141" y="216" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">old readers unaffected;</text>
<text x="141" y="234" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">backfill can lag safely</text>
<rect x="274" y="58" width="230" height="196" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="389" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#9a5a17">reinterpreting</text>
<text x="389" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">declared CRS changed</text>
<text x="389" y="136" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">edge semantics changed</text>
<text x="389" y="158" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">2D &#8596; 3D</text>
<text x="389" y="180" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">units changed</text>
<text x="389" y="216" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">no error anywhere;</text>
<text x="389" y="234" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">old files now mean something else</text>
<rect x="522" y="58" width="230" height="196" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="637" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#6a3d9a">structural</text>
<text x="637" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">encoding changed</text>
<text x="637" y="136" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">geometry column dropped</text>
<text x="637" y="158" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">column name reused</text>
<text x="637" y="180" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">partition spec replaced</text>
<text x="637" y="216" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">readers break loudly;</text>
<text x="637" y="234" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">at least the failure is visible</text>
</svg>
</figure>

The counterintuitive ranking is that the **middle** category is the dangerous one, not the right-hand one. A structural change breaks readers immediately and visibly, which is unpleasant but self-limiting: somebody notices within minutes and rolls back. A reinterpreting change breaks nothing, returns results, and produces answers that are wrong by an amount nobody measures until a downstream consumer complains about something apparently unrelated.

The discipline that follows is to treat any reinterpreting change as a **new column**, never as an in-place redefinition. Add `geometry_v2` alongside `geometry`, backfill it, migrate consumers one at a time with both columns present, and drop the old one only when nothing reads it. This costs storage for the overlap period and removes the entire class of silent-meaning-change incidents.

## Migrating Consumers Without a Flag Day

The additive pattern only helps if there is a way to tell when the migration is finished. Without that, the old column lives forever because nobody is willing to be the one who drops it.

<figure class="diagram">
<svg viewBox="0 0 768 220" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Four-step consumer migration: add the new column, backfill it, observe per-column read counts until the old column shows no reads for a full cycle, then drop it">
<defs>
<marker id="evo-mig-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#0e6e7d"/></marker>
</defs>
<rect x="0" y="0" width="768" height="220" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Let the read counters decide when it is safe to drop</text>
<rect x="24" y="72" width="164" height="80" rx="8" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<text x="106" y="104" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">1. add column</text>
<text x="106" y="126" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">nullable, no backfill yet</text>
<rect x="212" y="72" width="164" height="80" rx="8" fill="#ffffff" stroke="#0e6e7d" stroke-width="2"/>
<text x="294" y="104" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">2. backfill</text>
<text x="294" y="126" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">partition by partition</text>
<rect x="400" y="72" width="164" height="80" rx="8" fill="#ffffff" stroke="#0e6e7d" stroke-width="2"/>
<text x="482" y="104" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">3. observe</text>
<text x="482" y="126" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">who still reads the old one</text>
<rect x="588" y="72" width="168" height="80" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="672" y="104" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">4. drop</text>
<text x="672" y="126" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">after a full quiet cycle</text>
<line x1="188" y1="112" x2="212" y2="112" stroke="#0e6e7d" stroke-width="2" marker-end="url(#evo-mig-arrow)"/>
<line x1="376" y1="112" x2="400" y2="112" stroke="#0e6e7d" stroke-width="2" marker-end="url(#evo-mig-arrow)"/>
<line x1="564" y1="112" x2="588" y2="112" stroke="#0e6e7d" stroke-width="2" marker-end="url(#evo-mig-arrow)"/>
<text x="390" y="204" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">&#8220;A full cycle&#8221; means the longest scheduled job that touches the table — usually monthly</text>
</svg>
</figure>

Step three is the one that requires infrastructure rather than intent. Query history in Trino, Spark's plan listeners and Databricks system tables all record which columns a query read, and a weekly aggregation of that by column name tells you exactly who is left. Without it, the decision to drop rests on someone's memory of who they told, which is how a monthly report breaks three weeks after a migration everybody believed was complete.

## Recording the Contract Version on Every Write

The cheapest insurance against a reinterpreting change is a version number written into the data itself. Add a small integer column — `spatial_contract_v` — set by the writer from a constant in the pipeline, and increment it whenever the meaning of the geometry column changes for any reason: a CRS change, a dimensionality change, a switch in edge interpretation, a change in the units of a derived measurement column.

The value of this is that it converts an archaeological question into a filter. When somebody reports that distances computed over a two-year window look inconsistent, the first query is a group-by on the contract version, and the answer arrives in seconds instead of after a week of reading commit history. It also makes a partial backfill safe to leave partially complete, because a reader can select only the rows written under the contract it understands rather than assuming the whole table is homogeneous.

Keep the mapping from version number to meaning in the repository next to the pipeline, as a plain table in a markdown file, and treat adding a row to it as part of the change that bumps the version. A version number without a decoder is only marginally better than no version number, and the decoder is three lines of text per revision.

Finally, keep the evolution plan and the rollback plan in the same document. Every additive migration has a trivial rollback while both columns exist, and no rollback at all once the old one is dropped — so the drop is the only irreversible step in the sequence, and it deserves the same review that a schema change to a production database would get. Scheduling it as a separate change, weeks after the migration itself, keeps the risky part small and isolated from the work that made it possible.

## A Worked Example: Adding a Third Dimension

<figure class="diagram">
<svg viewBox="0 0 754 216" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Timeline of adding elevation to a geometry column using the additive pattern: a new 3D column is added, backfilled where elevation is known, consumers migrate individually, and the 2D column is dropped only after the read counters go quiet">
<rect x="0" y="0" width="754" height="216" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Adding elevation without breaking a single reader</text>
<line x1="60" y1="96" x2="720" y2="96" stroke="#cfe3e7" stroke-width="6" stroke-linecap="round"/>
<circle cx="130" cy="96" r="11" fill="#2f6e49"/><circle cx="326" cy="96" r="11" fill="#0e6e7d"/>
<circle cx="522" cy="96" r="11" fill="#9a5a17"/><circle cx="684" cy="96" r="11" fill="#6a3d9a"/>
<text x="130" y="134" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">add geometry_3d</text>
<text x="130" y="152" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">nullable, week 0</text>
<text x="326" y="134" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">backfill by partition</text>
<text x="326" y="152" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">null where unknown</text>
<text x="522" y="134" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">consumers migrate</text>
<text x="522" y="152" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">one at a time</text>
<text x="684" y="134" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">drop geometry</text>
<text x="684" y="152" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">after a quiet cycle</text>
<text x="390" y="200" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">At no point does an existing query change meaning or return a different answer</text>
</svg>
</figure>

Nullability is doing important work in this sequence. Elevation is frequently unavailable for historical features, and a nullable 3D column lets the partial state be explicit rather than encoded as a zero that later gets mistaken for sea level. Consumers that require elevation filter on `geometry_3d IS NOT NULL`; consumers that do not keep reading the 2D column until they are ready. Neither has to coordinate with the other, which is the property that makes the migration finish at all.
