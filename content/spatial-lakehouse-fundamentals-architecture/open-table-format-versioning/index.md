# Open Table Format Versioning in Spatial Lakehouse Architectures

Version control for analytical datasets has shifted from fragile file overwrites to ACID-compliant snapshot isolation. In spatial lakehouse deployments, this capability must reconcile geometric precision, coordinate reference system (CRS) metadata, and high-cardinality spatial partitions. Building on the architectural patterns established in [Spatial Lakehouse Fundamentals & Architecture](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/), this guide operationalizes versioning for Iceberg and Delta tables containing GIS workloads, with explicit focus on partitioning strategies, index maintenance, and CI/CD integration.

## Snapshot Mechanics & Spatial Metadata Propagation

Open table formats implement versioning through immutable data files paired with transactional metadata logs. Iceberg maintains a three-tier metadata structure (metadata.json → manifest lists → manifest files), enabling efficient snapshot pruning and predicate pushdown. Delta Lake relies on a linear transaction log (`_delta_log`) with periodic Parquet checkpoints to accelerate log replay and state reconstruction.

When versioning spatial datasets, the critical constraint is how geometric types are serialized and tracked across snapshots. Iceberg treats spatial columns as `BINARY` (WKB), requiring explicit bounding box columns alongside the geometry to surface coordinate bounds in manifest statistics. Silent coordinate truncation during schema changes is a real risk: always validate bbox statistics after any `ALTER TABLE` operation. Delta Lake similarly treats geometry as binary blobs or user-defined types, requiring explicit schema annotations and reader-side deserialization logic.

For implementation specifics on WKB/WKT serialization pipelines and CRS propagation across commits, refer to [Iceberg Spatial Type Support](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/iceberg-spatial-type-support/). Serialization trade-offs and checkpoint validation steps are documented in [Delta Lake Geometry Handling](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/delta-lake-geometry-handling/).

To maintain interoperability across engines, enforce strict adherence to [OGC Simple Features](https://www.ogc.org/standards/sfs) geometry validation during ingestion. This prevents malformed polygons from corrupting downstream spatial joins or breaking time-travel queries.

## Partitioning & Spatial Indexing Under Version Control

Spatial queries rarely align with traditional hash or range partitions. Versioned spatial tables require partitioning strategies that survive snapshot evolution without triggering excessive file rewrites. The production standard partitions by temporal buckets (e.g., `ingest_date` or `event_hour`) combined with spatial clustering (Z-order or Hilbert curves) rather than hard partitioning on raw geometry columns.

**Iceberg Configuration:**
```sql
CREATE TABLE analytics.spatial_events (
  event_id BIGINT,
  geom BINARY,        -- WKB
  ingest_ts TIMESTAMP,
  bbox_min_x DOUBLE,
  bbox_min_y DOUBLE,
  bbox_max_x DOUBLE,
  bbox_max_y DOUBLE
)
USING iceberg
PARTITIONED BY (days(ingest_ts))
TBLPROPERTIES (
  'format-version'='2',
  'write.distribution-mode'='range',
  'write.sort-order'='bbox_min_x ASC, bbox_min_y ASC, bbox_max_x ASC, bbox_max_y ASC',
  'write.metadata.delete-after-commit.enabled'='true',
  'write.metadata.previous-versions-max'='5'
);
```

**Delta Configuration:**
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

df.write.format("delta") \
  .option("path", "s3://lakehouse/spatial_events") \
  .partitionBy("ingest_date") \
  .mode("overwrite") \
  .save()

# Post-ingest Z-order clustering on bounding box columns
spark.sql("""
  OPTIMIZE delta.`s3://lakehouse/spatial_events`
  ZORDER BY (bbox_min_x, bbox_min_y)
""")
```

Delta's data skipping relies on min/max statistics per column, which work well for the explicit bounding box columns (`bbox_min_x`, `bbox_max_x`, etc.). By precomputing these columns during ingestion and Z-ordering on them, you expose geometry locality to the data skipping engine. Set `spark.databricks.delta.optimize.maxFileSize` (or the open-source equivalent `delta.targetFileSize`) to `134217728` (128MB) to balance query parallelism against manifest overhead.

## What Time Travel Is Actually For in Geospatial Work

Snapshot history is often justified with a generic argument about rollbacks, which undersells it. Geospatial data has three specific properties that make versioning structurally more valuable than it is for ordinary tabular data.

<figure class="diagram">
<svg viewBox="0 0 743 284" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Timeline of a boundary table across four snapshots showing a municipal boundary revision, an erroneous bulk load, a rollback, and a corrected reload, with an audit query pinned to the pre-revision snapshot">
<rect x="0" y="0" width="743" height="284" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">The same table, four snapshots, two different questions</text>
<line x1="60" y1="140" x2="726" y2="140" stroke="#cfe3e7" stroke-width="6" stroke-linecap="round"/>
<circle cx="130" cy="140" r="12" fill="#2f6e49"/>
<circle cx="326" cy="140" r="12" fill="#9a5a17"/>
<circle cx="522" cy="140" r="12" fill="#0e6e7d"/>
<circle cx="682" cy="140" r="12" fill="#6a3d9a"/>
<text x="130" y="112" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#2f6e49">s1</text>
<text x="326" y="112" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#9a5a17">s2</text>
<text x="522" y="112" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0e6e7d">s3</text>
<text x="682" y="112" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#6a3d9a">s4</text>
<text x="130" y="176" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">boundaries as of</text>
<text x="130" y="192" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">the 2023 census</text>
<text x="326" y="176" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">bulk load in the</text>
<text x="326" y="192" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">wrong projection</text>
<text x="522" y="176" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">rollback to s1</text>
<text x="682" y="176" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">corrected reload</text>
<rect x="56" y="216" width="300" height="56" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="206" y="238" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">audit query pins s1</text>
<text x="206" y="257" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">&#8220;what did we report in March?&#8221;</text>
<rect x="430" y="216" width="300" height="56" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="580" y="238" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">operational query reads s4</text>
<text x="580" y="257" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">&#8220;where is the boundary now?&#8221;</text>
<line x1="206" y1="216" x2="140" y2="156" stroke="#2f6e49" stroke-width="2"/>
<line x1="580" y1="216" x2="676" y2="156" stroke="#6a3d9a" stroke-width="2"/>
<text x="390" y="66" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">Both questions are legitimate and both must remain answerable at the same time</text>
</svg>
</figure>

**Boundaries are legally consequential and they move.** A municipal boundary revision, a redistricting, a change to a flood zone — each changes the answer to questions that were already asked and answered. Without snapshots, reproducing a report issued last March means reconstructing the boundary set from backups. With them, it is a query with a snapshot identifier. This is not a convenience; in regulated contexts it is the difference between defensible and indefensible analysis.

**Geometry errors are hard to detect and easy to propagate.** A bulk load in the wrong projection passes every row-count check and every null check, and only fails when someone looks at a map. By the time it is noticed, downstream tables have consumed it. Snapshot rollback restores the source in seconds; the downstream rebuild is then mechanical rather than forensic.

**Model reproducibility depends on the exact geometry.** A routing model or a risk score trained against a road network is not reproducible unless the network version is pinned. Recording the snapshot identifier alongside the model artefact costs nothing and makes the training set recoverable years later.

The practical implication for retention policy is that spatial tables often warrant *longer* retention than the operational default, and that the right retention unit is frequently not "days" but "named versions". Tag the snapshot that corresponds to each quarterly reference release, exclude tagged snapshots from expiry, and let the untagged ones age out on a short schedule. That combination keeps metadata small while preserving exactly the states anyone will ever ask for.

## Reading a Snapshot Without Restoring It

A rollback is a destructive-feeling operation and teams hesitate over it, which is unfortunate because the far more common need — reading an old state without changing the current one — carries no risk at all and is under-used.

Every engine exposes it. In Spark SQL against Iceberg, `FOR SYSTEM_VERSION AS OF` and `FOR SYSTEM_TIME AS OF` read a prior snapshot as an ordinary table. In Delta, `VERSION AS OF` and `TIMESTAMP AS OF` do the same. Neither writes anything, so both are safe to run in production during an incident.

The pattern that resolves most geometry incidents is a **diff between two snapshots scoped to a region**. Rather than asking "what changed", which returns an unreadable volume, ask "which features in this area have a different geometry hash than they did last week":

```sql
-- Iceberg 1.4+ / Spark 3.5. Region-scoped geometry diff between two snapshots.
WITH now AS (
  SELECT boundary_id, md5(geom_wkb) AS h
  FROM lakehouse.spatial.boundaries
  WHERE region_code = 'DE-BY'
),
before AS (
  SELECT boundary_id, md5(geom_wkb) AS h
  FROM lakehouse.spatial.boundaries FOR SYSTEM_VERSION AS OF 3921887632109
  WHERE region_code = 'DE-BY'
)
SELECT COALESCE(n.boundary_id, b.boundary_id) AS boundary_id,
       CASE WHEN b.h IS NULL THEN 'added'
            WHEN n.h IS NULL THEN 'removed'
            ELSE 'modified' END AS change
FROM now n FULL OUTER JOIN before b USING (boundary_id)
WHERE n.h IS NULL OR b.h IS NULL OR n.h <> b.h;
```

Two details make this reliable. Hash the **canonicalised** geometry, not the raw bytes: two encodings of the same polygon with different vertex start points or ring orientation produce different WKB and identical geometry, so a raw-byte diff reports spurious changes after any rewrite. And scope the diff spatially before hashing, because hashing an entire continental table to answer a question about one district is exactly the kind of full scan the rest of this site exists to avoid.


## Retention, Compaction & Metadata Hygiene

Unmanaged snapshot accumulation degrades query performance and inflates metadata storage. Spatial tables exacerbate this due to large bounding box statistics and frequent append patterns. Retention policies must align with compliance SLAs while preserving enough history for rollback and audit.

**Iceberg Retention & Cleanup:**
```sql
ALTER TABLE analytics.spatial_events SET TBLPROPERTIES (
  'history.expire.max-snapshot-age-ms'='604800000',  -- 7 days
  'history.expire.min-snapshots-to-keep'='3'
);

-- Execute cleanup via Spark stored procedure
CALL spark_catalog.system.expire_snapshots(
  table => 'analytics.spatial_events',
  older_than => TIMESTAMPADD(DAY, -7, CURRENT_TIMESTAMP),
  retain_last => 3
);
```

**Delta Retention & Cleanup:**
```sql
ALTER TABLE delta.`s3://lakehouse/spatial_events` SET TBLPROPERTIES (
  'delta.logRetentionDuration' = 'interval 30 days',
  'delta.deletedFileRetentionDuration' = 'interval 7 days'
);
-- VACUUM removes files no longer referenced by any snapshot
VACUUM delta.`s3://lakehouse/spatial_events` RETAIN 168 HOURS;
```

Always run retention jobs during low-traffic windows. Spatial compaction should be scheduled after Z-ordering to prevent index fragmentation. When evolving schemas across versions, review [Managing spatial schema evolution in open table formats](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/open-table-format-versioning/managing-spatial-schema-evolution-in-open-table-formats/) to avoid CRS drift or precision loss during `ALTER TABLE` operations.

## Branches and Tags for Spatial Reference Data

Snapshot identifiers are precise and unmemorable, which limits how far a team will actually use them. Named branches and tags fix that, and for spatial reference data they enable a workflow that is otherwise impractical.

The pattern is a **staging branch for a reference release**. Administrative boundaries, road networks, land-use classifications and flood zones all arrive as periodic releases from an authority, and each release needs validating against the previous one before anything downstream sees it. Writing the new release to a branch rather than to the main line means the validation can be arbitrarily thorough — full geometry diffs, area-change distributions, topology checks against neighbouring features — while every existing consumer continues reading the current state undisturbed.

When validation passes, the branch is fast-forwarded into the main line as a single atomic commit, so consumers move from the old release to the new one between queries rather than seeing a partially-loaded intermediate state. When it fails, the branch is dropped and nothing downstream ever observed it. This is materially better than the common alternative, which is loading into a side table and swapping names, because the swap is not atomic across engines and because the side table has no shared history with the real one.

```sql
-- Iceberg 1.4+ / Spark 3.5. Validate a boundary release on a branch, then publish atomically.
ALTER TABLE lakehouse.spatial.boundaries CREATE BRANCH `release_2026q1`;

INSERT INTO lakehouse.spatial.boundaries.branch_release_2026q1
SELECT * FROM staging.boundaries_2026q1;

-- Validation runs against the branch; consumers still read main.
SELECT count(*) AS invalid
FROM lakehouse.spatial.boundaries.branch_release_2026q1
WHERE NOT ST_IsValid(ST_GeomFromWKB(geom_wkb));

-- Publish in one commit, and tag the state for future audit queries.
CALL lakehouse.system.fast_forward('spatial.boundaries', 'main', 'release_2026q1');
ALTER TABLE lakehouse.spatial.boundaries CREATE TAG `boundaries_2026q1`;
```

**Tags** solve the retention problem described earlier. A tagged snapshot is excluded from expiry, so the quarterly reference states survive indefinitely while the thousands of intermediate streaming commits age out on a seven-day policy. The result is a table whose metadata stays small and whose meaningful history stays complete — the two goals that otherwise pull against each other.

There is a caution about branches on high-churn tables. A long-lived branch on a table receiving continuous appends pins every file the branch references, so storage does not fall when the main line expires snapshots. For reference data updated quarterly this is irrelevant. For a telemetry table it can quietly double storage, so branches there should be short-lived and dropped explicitly rather than left behind after a completed migration.

## Retention Policy as an Explicit Contract

Retention is usually configured once, by whoever created the table, using whatever default the platform suggested — and then never revisited until either storage costs or a compliance question forces the issue. Writing it down as a contract per table class avoids both surprises.

Three classes cover almost everything on a spatial platform. **Streaming telemetry** commits constantly, is reproducible from an upstream source, and nobody will ever ask what it looked like at 14:03 last Tuesday: expire snapshots after three to seven days, keeping just enough to roll back a bad deployment. **Derived analytical tables** are rebuilt from their inputs and need only enough history to bridge a failed run: seven to thirty days. **Reference and regulated data** is not reproducible — the authority publishes a release and moves on — and needs indefinite retention of the published states, achieved through tags rather than through a long expiry window.

The number that makes the policy concrete is metadata size, not storage size. Track the count of snapshots and the total size of manifest files per table, alert when planning time on a selective query exceeds a threshold, and treat that as the signal to tighten expiry. A table with 40,000 live snapshots will plan slowly regardless of how little data it holds, and no amount of compute will fix it.

One trap deserves naming because it catches careful teams: **expiring snapshots does not delete data files that other snapshots still reference**, and orphan files left by failed writes are not referenced by any snapshot at all, so they are never cleaned by expiry. A separate orphan-file cleanup, run with a conservative age threshold well beyond the longest possible in-flight write, is required to actually reclaim the space. Running it with an aggressive threshold on a table with long-running writers will delete files a live commit is about to reference, which is the one genuinely destructive maintenance operation on this page.


## CI/CD Integration for Spatial Versioning

Automated pipelines must validate spatial schemas, enforce partition bounds, and pin table versions before deployment:

```yaml
name: Spatial Table Versioning Pipeline
on:
  push:
    paths: ['spatial_models/**']
jobs:
  validate-and-deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Setup Python & Geospatial Libs
        run: |
          pip install pyspark==3.5.5 shapely==2.1.1 pyproj==3.7.1
      - name: Validate CRS & Partition Bounds
        run: |
          python -c "
          from pyspark.sql import SparkSession

          spark = SparkSession.builder.appName('spatial-validate').getOrCreate()
          df = spark.read.format('delta').load('s3://lakehouse/staging/spatial_events')

          # Enforce EPSG:4326 tag column
          crs_check = df.select('crs').distinct().collect()
          assert all(row['crs'] == 'EPSG:4326' for row in crs_check), 'CRS mismatch detected'

          # Validate bbox bounds are within global WGS84 range
          bounds = df.selectExpr('min(bbox_min_x)', 'max(bbox_max_x)',
                                  'min(bbox_min_y)', 'max(bbox_max_y)').collect()[0]
          assert bounds[0] >= -180.0 and bounds[1] <= 180.0, 'X bounds out of EPSG:4326 range'
          assert bounds[2] >= -90.0  and bounds[3] <= 90.0,  'Y bounds out of EPSG:4326 range'
          print('Validation passed')
          "
      - name: Deploy Versioned Table
        run: |
          spark-submit --packages io.delta:delta-spark_2.12:3.3.0 deploy_spatial.py
```

## How a Snapshot Chain Grows on a Spatial Table

The shape of a snapshot chain differs between a boundary table and a telemetry table, and the retention policy that suits one is wrong for the other.

<figure class="diagram">
<svg viewBox="0 0 732 248" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Two snapshot chains contrasted: a reference table with four large quarterly commits worth tagging, and a telemetry table with hundreds of tiny minute-level commits that should expire quickly">
<rect x="0" y="0" width="732" height="248" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Two tables, two entirely different retention answers</text>
<text x="80" y="76" font-family="sans-serif" font-size="12" font-weight="700" fill="#2f6e49">reference data</text>
<line x1="80" y1="100" x2="720" y2="100" stroke="#cfe3e7" stroke-width="5" stroke-linecap="round"/>
<circle cx="140" cy="100" r="13" fill="#2f6e49"/><circle cx="320" cy="100" r="13" fill="#2f6e49"/>
<circle cx="500" cy="100" r="13" fill="#2f6e49"/><circle cx="680" cy="100" r="13" fill="#2f6e49"/>
<text x="400" y="132" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">four commits a year, each meaningful — tag every one, never expire</text>
<text x="80" y="176" font-family="sans-serif" font-size="12" font-weight="700" fill="#9a5a17">telemetry</text>
<line x1="80" y1="200" x2="720" y2="200" stroke="#cfe3e7" stroke-width="5" stroke-linecap="round"/>
<circle cx="96" cy="200" r="5" fill="#9a5a17"/><circle cx="128" cy="200" r="5" fill="#9a5a17"/><circle cx="160" cy="200" r="5" fill="#9a5a17"/>
<circle cx="192" cy="200" r="5" fill="#9a5a17"/><circle cx="224" cy="200" r="5" fill="#9a5a17"/><circle cx="256" cy="200" r="5" fill="#9a5a17"/>
<circle cx="288" cy="200" r="5" fill="#9a5a17"/><circle cx="320" cy="200" r="5" fill="#9a5a17"/><circle cx="352" cy="200" r="5" fill="#9a5a17"/>
<circle cx="384" cy="200" r="5" fill="#9a5a17"/><circle cx="416" cy="200" r="5" fill="#9a5a17"/><circle cx="448" cy="200" r="5" fill="#9a5a17"/>
<circle cx="480" cy="200" r="5" fill="#9a5a17"/><circle cx="512" cy="200" r="5" fill="#9a5a17"/><circle cx="544" cy="200" r="5" fill="#9a5a17"/>
<circle cx="576" cy="200" r="5" fill="#9a5a17"/><circle cx="608" cy="200" r="5" fill="#9a5a17"/><circle cx="640" cy="200" r="5" fill="#9a5a17"/>
<circle cx="672" cy="200" r="5" fill="#9a5a17"/><circle cx="704" cy="200" r="5" fill="#9a5a17"/>
<text x="400" y="232" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">1,440 commits a day, none individually meaningful — expire after seven</text>
</svg>
</figure>

Tables of the second kind are where metadata growth becomes a performance problem, and tables of the first kind are where an aggressive global expiry policy destroys the history someone will eventually need. A single platform-wide retention setting will get one of them wrong, which is the argument for classifying tables explicitly rather than inheriting a default.

## Troubleshooting Production Issues

| Symptom | Root Cause | Resolution |
|---|---|---|
| `Snapshot expired` errors during time-travel queries | Retention policy too aggressive or manual vacuum without `DRY RUN` | Increase `min-snapshots-to-keep` to 3–5. For Delta, run `VACUUM ... DRY RUN` before production cleanup. |
| Query skew on spatial joins | Z-order applied to raw WKB instead of bbox/centroid columns | Precompute `bbox_min_x`, `bbox_max_y`, etc. and re-run `OPTIMIZE ZORDER BY` on those columns. |
| CRS drift across versions | Schema evolution added geometry column without explicit CRS annotation | Enforce schema registry checks in CI. Add `crs` as a `STRING` column with `NOT NULL` constraint. |
| Manifest bloat (>500MB) | High-frequency appends with overlapping bounding boxes | Enable `write.metadata.delete-after-commit.enabled=true`. Reduce commit frequency by batching micro-appends. |

## Schema Evolution Against a Live Geometry Column

Adding a column is safe in both formats. The operations that are not safe on a spatial table are the ones that change what existing bytes mean, and they are easy to reach for by accident.

<figure class="diagram">
<svg viewBox="0 0 752 232" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Schema changes on a spatial table sorted into safe operations such as adding columns and renaming by field id, and unsafe operations such as changing dimensionality, changing the declared CRS or reusing a dropped column name">
<rect x="0" y="0" width="752" height="232" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Which schema changes are safe on a geometry column</text>
<rect x="40" y="56" width="340" height="164" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="210" y="82" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#2f6e49">safe</text>
<text x="210" y="108" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">add a nullable attribute column</text>
<text x="210" y="130" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">rename (Iceberg tracks field ids)</text>
<text x="210" y="152" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">add derived bbox columns</text>
<text x="210" y="174" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">widen int to long</text>
<text x="210" y="200" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">add a new geometry column</text>
<rect x="400" y="56" width="340" height="164" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="570" y="82" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#9a5a17">requires a rewrite</text>
<text x="570" y="108" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">changing the declared CRS</text>
<text x="570" y="130" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">2D to 3D, or 3D to 2D</text>
<text x="570" y="152" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">WKB to any other encoding</text>
<text x="570" y="174" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">reusing a dropped column name</text>
<text x="570" y="200" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">changing edge interpretation</text>
</svg>
</figure>

The right-hand column shares a property: none of these raises an error, and all of them make older files mean something different from newer ones. Treat each as a table migration with a rewrite and a version bump rather than as a schema edit, and see [managing spatial schema evolution in open table formats](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/open-table-format-versioning/managing-spatial-schema-evolution-in-open-table-formats/) for the mechanics.

## Operational Checklist
- [ ] Pin CRS at table creation; never rely on implicit defaults.
- [ ] Partition by temporal columns; cluster spatially via Z-order on bbox columns.
- [ ] Schedule `OPTIMIZE`/compaction after bulk loads, not during streaming ingestion.
- [ ] Validate binary geometry schemas before checkpointing.
- [ ] Monitor metadata size; enforce snapshot expiration aligned with SLA requirements.
- [ ] Log commit IDs alongside spatial query metrics for auditability.

Open table format versioning transforms spatial data lakes into reliable, auditable platforms. By aligning snapshot mechanics with spatial indexing, enforcing strict retention policies, and automating validation in CI/CD, engineering teams can scale GIS analytics without sacrificing transactional integrity.

### Testing the Rollback Before Needing It

A rollback path that has never been exercised is a hypothesis. Schedule a quarterly drill on a non-production copy: load a deliberately corrupted batch, detect it with the same alerting that runs in production, roll back to the prior snapshot, and rebuild the downstream tables that consumed the bad data. Time the whole sequence. Teams that run this drill discover the same two things almost every time — that the detection step is slower than assumed because the alert thresholds were never tuned, and that at least one downstream table has no defined rebuild procedure at all. Both are cheap to fix on a Tuesday afternoon and expensive to discover during an incident, which is the entire argument for the drill.
