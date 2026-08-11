# Spatial Lakehouse Fundamentals & Architecture

The transition from monolithic spatial databases to a spatial data lakehouse is not a storage migration; it is a fundamental re-architecture of how geospatial data is serialized, versioned, indexed, and queried at scale. Traditional GIS stacks tightly couple storage, compute, and spatial indexing into a single RDBMS process, creating hard ceilings on concurrency, storage elasticity, and multi-engine interoperability. A spatial lakehouse decouples these planes, anchoring immutable data in cloud object storage while delegating transactional control to open table formats and pushing spatial compute to distributed query engines.

<figure class="diagram">
<svg viewBox="0 0 751 228" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three decoupled planes of a spatial lakehouse: object storage of GeoParquet and WKB files, the table format and catalog holding metadata, snapshots and bbox stats, and the compute engines Spark, Trino and DuckDB">
<defs>
<marker id="fund-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#0e6e7d"/></marker>
</defs>
<rect x="0" y="0" width="751" height="228" fill="#f7fbfc"/>
<text x="380" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Decoupled spatial lakehouse stack</text>
<rect x="20" y="55" width="205" height="130" rx="8" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<text x="122" y="80" text-anchor="middle" font-family="sans-serif" font-size="14" font-weight="700" fill="#0d3b45">Object storage</text>
<text x="122" y="99" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">S3 / ADLS / GCS</text>
<text x="122" y="126" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">GeoParquet files</text>
<text x="122" y="146" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">WKB geometry columns</text>
<text x="122" y="166" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">immutable objects</text>
<rect x="277" y="55" width="205" height="130" rx="8" fill="#ffffff" stroke="#0e6e7d" stroke-width="2"/>
<text x="379" y="80" text-anchor="middle" font-family="sans-serif" font-size="14" font-weight="700" fill="#0d3b45">Table format + catalog</text>
<text x="379" y="99" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">Iceberg / Delta</text>
<text x="379" y="126" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">manifest metadata</text>
<text x="379" y="146" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">snapshots (ACID)</text>
<text x="379" y="166" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">bbox min/max stats</text>
<rect x="534" y="55" width="205" height="130" rx="8" fill="#ffffff" stroke="#6a3d9a" stroke-width="2"/>
<text x="636" y="80" text-anchor="middle" font-family="sans-serif" font-size="14" font-weight="700" fill="#0d3b45">Compute engines</text>
<text x="636" y="99" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">distributed spatial</text>
<text x="636" y="126" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">Spark + Sedona</text>
<text x="636" y="146" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">Trino / Presto</text>
<text x="636" y="166" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">DuckDB spatial</text>
<text x="251" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">file refs</text>
<text x="251" y="124" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">+ stats</text>
<line x1="225" y1="130" x2="277" y2="130" stroke="#0e6e7d" stroke-width="2" marker-end="url(#fund-arrow)"/>
<text x="508" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">pruned</text>
<text x="508" y="124" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">file list</text>
<line x1="482" y1="130" x2="534" y2="130" stroke="#0e6e7d" stroke-width="2" marker-end="url(#fund-arrow)"/>
<text x="380" y="212" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">Each plane scales independently; only file references and metadata cross the boundaries</text>
</svg>
</figure>

## The Decoupled Spatial Stack

A production spatial lakehouse operates across three isolated planes:

1. **Immutable Object Storage Plane**: Cloud storage (S3, ADLS, GCS) acts as the single source of truth. Data is persisted in columnar formats optimized for analytical I/O. Target file sizes of 128MB–1GB balance metadata overhead against parallel read throughput. Over-partitioning spatial datasets into thousands of sub-10MB files degrades query performance due to excessive listing and metadata resolution.
2. **Transactional Catalog Layer**: The catalog maintains schema definitions, transaction logs, and snapshot pointers. It enforces ACID guarantees without locking the underlying storage layer. Engines attach to the catalog to resolve table states, enabling concurrent reads and writes without data corruption.
3. **Distributed Compute Plane**: Query engines (Spark, Trino, DuckDB, Databricks, Snowflake) attach to the catalog, execute spatial predicates, and materialize results. Compute is stateless and horizontally scalable. Spatial operations are pushed down to the storage layer where possible, leveraging file-level statistics to skip irrelevant data blocks.

## Reference Architecture: From Ingestion to Serving

The three planes describe *what* the pieces are; a working platform also needs a defined order in which data moves through them. Almost every production spatial lakehouse converges on the same six-stage path, and most incidents can be traced to a stage that was skipped or implemented as an afterthought.

<figure class="diagram">
<svg viewBox="0 0 774 298" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Six-stage spatial ingestion pipeline: raw sources land, geometry is validated and repaired, CRS is normalised to 4326, WKB and bounding box columns are derived, data is committed to the table format, and maintenance jobs compact and sort before engines serve queries">
<defs>
<marker id="fund-pipe-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#0e6e7d"/></marker>
</defs>
<rect x="0" y="0" width="774" height="298" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">The six stages every spatial write passes through</text>
<rect x="18" y="58" width="138" height="76" rx="8" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<text x="87" y="84" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">1. Land</text>
<text x="87" y="103" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">Shapefile, GeoJSON,</text>
<text x="87" y="119" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">PostGIS dump, Kafka</text>
<rect x="174" y="58" width="138" height="76" rx="8" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<text x="243" y="84" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">2. Validate</text>
<text x="243" y="103" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">ST_IsValid, repair</text>
<text x="243" y="119" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">self-intersections</text>
<rect x="330" y="58" width="138" height="76" rx="8" fill="#ffffff" stroke="#0e6e7d" stroke-width="2"/>
<text x="399" y="84" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">3. Normalise CRS</text>
<text x="399" y="103" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">reproject to 4326</text>
<text x="399" y="119" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">record source SRID</text>
<rect x="486" y="58" width="138" height="76" rx="8" fill="#ffffff" stroke="#0e6e7d" stroke-width="2"/>
<text x="555" y="84" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">4. Derive</text>
<text x="555" y="103" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">WKB + bbox cols</text>
<text x="555" y="119" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">+ grid cell id</text>
<rect x="642" y="58" width="120" height="76" rx="8" fill="#ffffff" stroke="#6a3d9a" stroke-width="2"/>
<text x="702" y="84" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">5. Commit</text>
<text x="702" y="103" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">snapshot written</text>
<text x="702" y="119" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">to the catalog</text>
<line x1="156" y1="96" x2="174" y2="96" stroke="#0e6e7d" stroke-width="2" marker-end="url(#fund-pipe-arrow)"/>
<line x1="312" y1="96" x2="330" y2="96" stroke="#0e6e7d" stroke-width="2" marker-end="url(#fund-pipe-arrow)"/>
<line x1="468" y1="96" x2="486" y2="96" stroke="#0e6e7d" stroke-width="2" marker-end="url(#fund-pipe-arrow)"/>
<line x1="624" y1="96" x2="642" y2="96" stroke="#0e6e7d" stroke-width="2" marker-end="url(#fund-pipe-arrow)"/>
<rect x="174" y="182" width="288" height="70" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="318" y="208" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">6. Maintain (asynchronous)</text>
<text x="318" y="228" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">compaction, sort rewrite, snapshot expiry</text>
<text x="318" y="243" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">runs on its own schedule, never in the write path</text>
<rect x="500" y="182" width="262" height="70" rx="8" fill="#ffffff" stroke="#6a3d9a" stroke-width="2"/>
<text x="631" y="208" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">Serving</text>
<text x="631" y="228" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">Sedona, Trino and DuckDB read the</text>
<text x="631" y="243" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">same committed snapshot</text>
<line x1="702" y1="134" x2="702" y2="182" stroke="#0e6e7d" stroke-width="2" marker-end="url(#fund-pipe-arrow)"/>
<line x1="399" y1="134" x2="399" y2="182" stroke="#0e6e7d" stroke-width="2" marker-end="url(#fund-pipe-arrow)"/>
<line x1="462" y1="217" x2="500" y2="217" stroke="#0e6e7d" stroke-width="2" marker-end="url(#fund-pipe-arrow)"/>
<text x="390" y="282" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">Stages 2–4 are the contract: skip any of them and the cost surfaces later as a query-time full scan</text>
</svg>
</figure>

Stages two through four are where the architecture is actually decided. **Validation** must happen before the write, because an invalid polygon — a self-intersecting ring, a bowtie, a hole outside its shell — will not fail loudly; it will silently return the wrong answer from `ST_Intersects` months later, in one partition, for one customer. Running `ST_IsValid` and `ST_MakeValid` at ingest costs single-digit milliseconds per feature and removes an entire class of unreproducible bug reports.

**CRS normalisation** is the second contract. A lakehouse table has exactly one coordinate reference system, and every engine that touches it assumes that system without asking. Storing a mix of 4326 and 3857 geometries in one column produces joins that return zero rows with no error at all, because coordinate values in degrees never intersect coordinate values in metres. The discipline described in [CRS management pipelines](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/crs-management-pipelines/) — reproject at the boundary, record the source SRID in a sidecar column, and assert the invariant in CI — is what keeps that from happening.

**Derivation** is the cheapest performance work available. Computing four `double` bounding-box columns and a grid cell identifier at write time costs a few percent of ingest throughput and unlocks file-level skipping for every reader afterwards, in every engine, without any of them needing to understand geometry at all. A query planner that can compare `bbox_max_x` against a constant will prune 95% of the files in a well-sorted table; a planner that must decode WKB to make the same decision has already paid for the read.

Stage six deserves particular attention because it is the stage teams most often leave out of the design and later bolt on under pressure. Compaction, sort rewriting and snapshot expiry are not optional housekeeping — they are the mechanism by which a streaming spatial table stays queryable. Ingesting 5 million points an hour in one-minute micro-batches produces 1,440 files a day per partition, and the metadata cost of planning across them will eventually exceed the cost of reading them. Treat maintenance as a first-class scheduled workload, described in [lakehouse maintenance automation](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/lakehouse-maintenance-automation/), and give it its own compute budget.


## Geometry Serialization & Engine Interoperability

Unlike scalar types, spatial objects require deterministic serialization to guarantee cross-engine compatibility and efficient predicate evaluation. The industry standard for compact, binary-safe geometry representation remains WKB (Well-Known Binary), while WKT (Well-Known Text) is reserved for debugging and human-readable logs. Modern implementations increasingly adopt [GeoParquet](https://www.ogc.org/standard/geoparquet/) as the de facto columnar spatial format, embedding coordinate reference systems (CRS) and bounding box metadata directly in Parquet schema extensions.

When implementing Apache Iceberg, teams must explicitly configure spatial type extensions to ensure WKB payloads are recognized by the schema registry and pushed down to the storage layer. Proper configuration of [Iceberg Spatial Type Support](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/iceberg-spatial-type-support/) is mandatory for enabling native `ST_Intersects`, `ST_Contains`, and `ST_DWithin` predicate pushdown, which reduces I/O by orders of magnitude compared to post-scan filtering.

Delta Lake approaches spatial serialization through explicit column typing and data skipping strategies rather than native geometry primitives. Because Delta relies on Parquet's underlying type system, geometries are typically stored as binary WKB or string WKT columns, requiring explicit UDF registration and index-aware partitioning. Understanding the nuances of [Delta Lake Geometry Handling](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/delta-lake-geometry-handling/) is essential for avoiding full-table scans during spatial joins and ensuring that clustering keys align with query access patterns.

**Trade-off**: Native spatial extensions (Iceberg) reduce UDF overhead and simplify schema evolution but may require engine-specific runtime libraries. Binary WKB in Delta offers broader compatibility across legacy GIS tools but shifts spatial indexing responsibility to the query planner and requires explicit Z-ordering on coordinate bounds.

## Transactional Semantics & Snapshot Management

Geospatial pipelines frequently require time-travel for regulatory audits, model reproducibility, and safe rollbacks after failed ETL runs. Open table formats implement snapshot isolation by tracking manifest files and data file references. Each transaction generates a new snapshot pointer, allowing readers to access consistent historical states without blocking writers.

Implementing [Open Table Format Versioning](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/open-table-format-versioning/) requires explicit retention policies. Spatial datasets often accumulate rapidly due to high-frequency sensor ingestion or daily boundary updates. Aggressive snapshot cleanup reduces storage costs but eliminates rollback windows. Conversely, retaining every snapshot inflates catalog metadata and slows table resolution during concurrent spatial joins.

**Copy-on-Write (CoW) vs Merge-on-Read (MoR)**: CoW rewrites entire files on update, guaranteeing optimal read performance but incurring high write amplification for large spatial polygons. MoR writes delta files and merges at query time, reducing write latency but increasing spatial join complexity. For high-frequency point data (e.g., IoT telemetry), MoR is preferred. For static administrative boundaries or cadastral layers, CoW delivers predictable query performance.

## Indexing Strategies & Predicate Pushdown

Spatial predicates are computationally expensive. Without proper indexing, engines resort to full-table scans, decoding every geometry to evaluate intersection or containment. Lakehouse architectures rely on three complementary indexing mechanisms:

1. **Partitioning by Spatial Grids**: Partitioning by H3, S2, or fixed bounding box grids aligns physical storage with query access patterns. Over-partitioning (>10,000 partitions) creates small files and catalog bloat. Under-partitioning forces unnecessary block reads.
2. **Z-Ordering on Coordinates**: Sorting data by interleaved X/Y coordinates colocates spatially proximate records within the same Parquet row groups. This dramatically improves data skipping during range scans and spatial joins.
3. **Bounding Box Statistics**: Modern engines extract min/max coordinate bounds per Parquet file. When a query specifies a spatial filter, the planner skips files whose bounding boxes do not intersect the query window.

**Performance Trade-off**: Z-ordering improves spatial join latency by 3–10x but increases write costs due to sorting overhead. Grid partitioning accelerates localized queries but degrades performance for cross-boundary or global spatial aggregations. Production teams typically combine coarse grid partitioning (e.g., by region or H3 resolution 5) with Z-ordering on coordinate bounds within partitions.

## Production Implementation Patterns

### PySpark Write Configuration (Iceberg)
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = SparkSession.builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.lakehouse.type", "hadoop") \
    .config("spark.sql.catalog.lakehouse.warehouse", "s3://lakehouse-bucket/spatial/") \
    .getOrCreate()

df = spark.read.parquet("s3://raw/telemetry/") \
    .withColumn("geom_wkb", expr("ST_AsBinary(ST_Point(lon, lat))"))

df.writeTo("lakehouse.spatial.telemetry") \
    .using("iceberg") \
    .partitionBy("region_code") \
    .option("write.parquet.compression-codec", "zstd") \
    .option("write.sort-order", "bbox_min_x ASC, bbox_min_y ASC") \
    .mode("append") \
    .save()
```

### SQL Spatial Join with Predicate Pushdown
```sql
-- Trino / Spark SQL compatible syntax
SELECT
    p.asset_id,
    b.boundary_name,
    p.timestamp
FROM lakehouse.spatial.telemetry p
JOIN lakehouse.spatial.admin_boundaries b
  ON ST_Intersects(
       ST_GeomFromWKB(p.geom_wkb),
       ST_GeomFromWKB(b.geom_wkb)
     )
WHERE p.timestamp >= TIMESTAMP '2024-01-01 00:00:00'
  AND p.bbox_min_x >= -122.5 AND p.bbox_max_x <= -122.0
  AND p.bbox_min_y >= 37.5  AND p.bbox_max_y <= 38.0
```

### CI/CD Schema Validation (GitHub Actions)
```yaml
name: Validate Spatial Lakehouse Schema
on: [pull_request]
jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Install dependencies
        run: pip install pyarrow shapely pyiceberg
      - name: Run spatial schema validation
        run: python scripts/validate_spatial_schema.py
```

## Migrating From PostGIS Without a Big Bang

Most teams arriving at a spatial lakehouse already run PostGIS, and the migration question is rarely "can it work" but "how do we get there without a freeze". A cutover in one step fails for a predictable reason: the lakehouse cannot serve the low-latency single-feature lookups that the existing application tier depends on, so the moment the database is switched off, unrelated services break. The workable pattern keeps both systems live and moves *workloads*, not tables.

<figure class="diagram">
<svg viewBox="0 0 782 264" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Four-phase PostGIS to lakehouse migration timeline: mirror analytical extracts, dual-write with reconciliation, shift analytical reads to the lakehouse, and finally retain PostGIS only for transactional feature serving">
<rect x="0" y="0" width="782" height="264" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Moving workloads, not tables</text>
<line x1="40" y1="88" x2="740" y2="88" stroke="#cfe3e7" stroke-width="6" stroke-linecap="round"/>
<circle cx="110" cy="88" r="11" fill="#2f6e49"/>
<circle cx="303" cy="88" r="11" fill="#0e6e7d"/>
<circle cx="497" cy="88" r="11" fill="#0e6e7d"/>
<circle cx="690" cy="88" r="11" fill="#6a3d9a"/>
<text x="110" y="66" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#2f6e49">Phase 1</text>
<text x="303" y="66" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0e6e7d">Phase 2</text>
<text x="497" y="66" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0e6e7d">Phase 3</text>
<text x="690" y="66" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#6a3d9a">Phase 4</text>
<rect x="30" y="112" width="160" height="104" rx="8" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<text x="110" y="136" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">Mirror</text>
<text x="110" y="156" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">nightly extract to</text>
<text x="110" y="172" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">GeoParquet, read-only</text>
<text x="110" y="196" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">risk: none</text>
<rect x="223" y="112" width="160" height="104" rx="8" fill="#ffffff" stroke="#0e6e7d" stroke-width="2"/>
<text x="303" y="136" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">Dual-write</text>
<text x="303" y="156" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">both systems written,</text>
<text x="303" y="172" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">counts reconciled daily</text>
<text x="303" y="196" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">risk: write divergence</text>
<rect x="417" y="112" width="160" height="104" rx="8" fill="#ffffff" stroke="#0e6e7d" stroke-width="2"/>
<text x="497" y="136" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">Shift reads</text>
<text x="497" y="156" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">analytics and BI move</text>
<text x="497" y="172" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">to the lakehouse</text>
<text x="497" y="196" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">risk: result drift</text>
<rect x="610" y="112" width="160" height="104" rx="8" fill="#ffffff" stroke="#6a3d9a" stroke-width="2"/>
<text x="690" y="136" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">Specialise</text>
<text x="690" y="156" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">PostGIS keeps OLTP</text>
<text x="690" y="172" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">feature serving only</text>
<text x="690" y="196" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">risk: two schemas</text>
<text x="390" y="248" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">Each phase is independently reversible; nothing is deleted until phase 4 has run unattended for a quarter</text>
</svg>
</figure>

**Phase one** is a one-way mirror. A nightly job exports the PostGIS tables to GeoParquet and registers them as a lakehouse table. Nothing depends on it, so nothing can break, and the team gets a real dataset against which to benchmark engines and validate that geometry round-trips byte-for-byte. Compare `ST_AsBinary` output hashes between source and target rather than comparing row counts; a truncated coordinate precision setting will preserve the count and corrupt every geometry.

**Phase two** introduces dual-write. The ingestion service writes to PostGIS as before and appends to the lakehouse table in the same logical transaction boundary — accepting that the two systems have no shared transaction. Reconciliation is therefore mandatory: a daily job that compares row counts, geometry hash aggregates and bounding-box extents per partition, and alerts on divergence beyond a threshold. Most teams discover here that their PostGIS writes include updates and deletes they had forgotten about, which is exactly the discovery this phase exists to force.

**Phase three** moves the analytical read workloads — dashboards, batch scoring, tile pre-generation, spatial aggregation reports — onto the lakehouse. Run both for a period and diff the outputs. Expect small, explainable differences: PostGIS and GEOS-backed engines can disagree at the boundary of `ST_Intersects` for geometries that merely touch, and floating-point reprojection is not associative. Differences that are not explainable are bugs, usually in CRS handling.

**Phase four** is the steady state, and for most organisations it is not "PostGIS is gone". It is PostGIS serving the transactional single-feature reads and edits it is genuinely excellent at, against a much smaller working set, while the lakehouse owns history, scale and multi-engine analytics. The cost saving comes from the database no longer needing to hold five years of telemetry to answer a question about last Tuesday.


## Security, Governance & Access Control

Geospatial datasets frequently contain sensitive infrastructure coordinates, proprietary survey boundaries, or regulated environmental data. Implementing [Security Boundaries for GIS Data](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/security-boundaries-for-gis-data/) requires enforcing row-level security (RLS) based on spatial containment, column-level masking for coordinate precision, and IAM policies that restrict catalog access to authorized compute clusters.

Encryption at rest (SSE-KMS) and in-transit (TLS 1.3) are baseline requirements. For multi-tenant platforms, spatial metadata should be isolated using catalog namespaces, and query engines must validate user roles before resolving table snapshots. Auditing spatial data access requires logging predicate filters and bounding box resolutions, not just table-level hits, to detect unauthorized spatial reconnaissance.

## Cross-Cloud Deployment & Catalog Federation

Organizations operating across AWS, Azure, and GCP require consistent spatial data access without vendor lock-in. Catalog federation options include Unity Catalog, Nessie, and Apache Polaris. Replicating manifest files via object storage sync and standardizing on engine-agnostic formats like GeoParquet preserve interoperability.

Cross-cloud spatial joins introduce latency penalties due to egress costs and network round-trips. Mitigation strategies include:
- Pre-materializing spatial indexes in the target cloud
- Using Delta Sharing or Iceberg REST Catalog for metadata-only federation
- Restricting cross-cloud queries to aggregated results rather than raw geometry transfers

## Failure Modes and Operational Gotchas

Spatial lakehouse incidents cluster into a small number of recurring shapes. Each of the following has a cheap detection and a known mitigation; the expensive part is always discovering it in production rather than in CI.

- **Small-file explosion.** Streaming ingest at high frequency produces thousands of files per partition, and query planning time grows with file count long before scan time does. *Detect:* track files-per-partition and mean file size as a metric, alerting under 32 MB. *Mitigate:* schedule `rewrite_data_files` (Iceberg) or `OPTIMIZE` (Delta) with a target of 128 MB–1 GB, and consider buffering micro-batches to a five-minute commit interval instead of one.
- **Manifest bloat.** Every commit appends manifest metadata. A table with a year of minute-level snapshots can spend more time resolving its own metadata than reading data. *Detect:* time `SELECT count(*)` against an empty predicate — if planning dominates, the metadata is the problem. *Mitigate:* expire snapshots on a retention policy, and run manifest rewrite so the manifest list stays proportional to live data, not to write history.
- **CRS drift.** An upstream provider changes its export projection without announcing it, and the new files land in a table whose other files are in 4326. Joins quietly return nothing. *Detect:* assert coordinate ranges at ingest — latitudes outside ±90 are a projected CRS masquerading as geographic. *Mitigate:* the drift detection covered in [detecting CRS drift in ingestion pipelines](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/crs-management-pipelines/detecting-crs-drift-in-ingestion-pipelines/).
- **Bounding-box statistics that do not exist.** Data skipping depends on per-file min/max statistics, and Parquet writers only collect them for the first N columns by default (32 in many implementations). A geometry-adjacent bbox column placed at position 40 in a wide schema gets no statistics and no skipping. *Detect:* read the Parquet footer and confirm statistics are present for the bbox columns. *Mitigate:* raise the statistics column limit or move the bbox columns to the front of the schema.
- **The antimeridian and the poles.** A bounding box around a geometry that crosses ±180° longitude spans the entire globe, so every file appears to match every query. A handful of shipping-route or flight-path polygons can therefore defeat skipping for an entire table. *Detect:* flag any file whose bbox width exceeds 180°. *Mitigate:* split crossing geometries at the antimeridian at ingest, or store a pair of bounding boxes.
- **Write amplification on wide polygons.** Copy-on-write updates rewrite whole files. When a file contains multi-megabyte administrative boundaries, a single-row correction can rewrite hundreds of megabytes. *Detect:* monitor bytes-written against rows-changed. *Mitigate:* choose merge-on-read for tables that receive scattered updates, and keep large-geometry tables physically separate from high-churn attribute tables.
- **Concurrent maintenance versus concurrent writes.** A compaction job and a streaming writer targeting the same partition will race, and the loser retries — repeatedly, if the compaction is long. *Detect:* count commit conflicts. *Mitigate:* partition-scope the maintenance job, run it against partitions that are no longer receiving writes, and cap its runtime.
- **Geometry that is valid but pathological.** A polygon with 400,000 vertices is valid, passes every check, and will make a distributed join hang on one straggler task. *Detect:* record vertex counts as a column at ingest and alert on outliers. *Mitigate:* simplify for analytical layers with a tolerance appropriate to the query resolution, retaining the exact geometry in a reference table.


## What Actually Drives the Bill

Cost conversations about lakehouses usually start with storage, which is almost never the expensive part. A petabyte of GeoParquet at standard object-storage rates is a rounding error next to the compute that scans it badly. Four factors dominate, and all four are architectural rather than commercial.

**Bytes scanned per query.** This is the single largest lever, and it is set by partitioning and sort order rather than by engine choice. A table with a coherent grid partition and coordinate-sorted files answers a metropolitan-area query by reading 0.5% of its bytes; the same data written in arrival order reads all of it. Every guide under [spatial partitioning and indexing strategies](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/) is ultimately about this number.

**Geometry decode CPU.** Decoding WKB to an in-memory geometry is not free, and an engine that decodes before filtering pays it on every row rather than on the survivors. Numeric bounding-box predicates that run before any decode routinely cut CPU by an order of magnitude — which is why [predicate pushdown optimization](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/predicate-pushdown-optimization/) matters more for spatial workloads than for scalar ones.

**Request count against object storage.** Listing and `GET` requests are billed per operation, and a small-file problem inflates them dramatically. Ten thousand 1 MB files cost far more in requests than eighty 128 MB files holding the same data, and the difference shows up on the storage invoice rather than the compute one, which is why it is often missed.

**Idle and oversized compute.** Spatial joins tempt teams into large clusters that then sit idle between batches. A broadcast join against a small boundary table on modest hardware frequently outperforms a shuffle join on hardware three times the size; the guidance in [broadcast spatial joins with Apache Sedona](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/sedona-distributed-spatial-compute/broadcast-spatial-joins-with-apache-sedona/) is as much a cost technique as a performance one. Size for the join strategy, not for the raw data volume.

A useful discipline is to attach a cost annotation to every scheduled spatial job — bytes scanned, files touched, wall-clock and vCPU-hours — and review the top ten monthly. In most platforms, three jobs account for two-thirds of the spend, and at least one of them is scanning a table that nobody has partitioned since it was created.


## When a Spatial Lakehouse Is the Wrong Answer

The architecture has a shape, and workloads that do not fit that shape are better served elsewhere. Being explicit about this early prevents a migration that succeeds technically and fails operationally.

Object storage has a floor on latency measured in tens of milliseconds, and the table format adds a metadata resolution step on top. That makes the lakehouse a poor fit for **single-feature transactional lookups** — "fetch parcel 88213 and return it to the map client in 40 ms" — where PostGIS with a GiST index is not merely adequate but genuinely better by an order of magnitude. It is equally a poor fit for **interactive editing**, where a user drags a vertex and expects the change to be visible to a colleague immediately: snapshot isolation is the wrong concurrency model for collaborative editing, and file rewrite costs make per-vertex updates absurd.

**Small datasets do not justify the machinery.** A 20 GB set of administrative boundaries that changes quarterly does not need snapshots, manifests, compaction schedules or a catalog service. GeoPackage on a disk, or a single Parquet file read by DuckDB, will answer every question faster and with a fraction of the operational surface. The lakehouse earns its complexity somewhere above the point where one machine can no longer hold the working set, or where more than one engine must read the same governed copy.

Finally, **raster-dominant workloads** sit awkwardly. Open table formats model rows, and a raster is a tiled array; forcing it into a row-per-tile table works, but Zarr or Cloud-Optimised GeoTIFF with a spatial catalogue on top is the more natural fit. The hybrid pattern in [bucket mapping for raster data](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/bucket-mapping-for-raster-data/) — keep pixels in their native format, keep footprints and metadata in the lakehouse — usually beats either extreme.

The honest test is workload-shaped rather than size-shaped: if the dominant access pattern is analytical scans over large extents, versioned history matters, and more than one compute engine needs the same governed copy, the lakehouse is right. If the dominant pattern is point lookups and edits under a latency budget, it is not.


## Operational Readiness Checklist

- [ ] Enforce 128MB–1GB Parquet file targets; run `OPTIMIZE` / `rewrite_data_files` for spatial partitions
- [ ] Validate WKB serialization consistency across ingestion pipelines
- [ ] Configure Z-ordering on coordinate bounds or H3 grid cells
- [ ] Establish snapshot retention policies aligned with compliance requirements
- [ ] Implement RLS and coordinate precision masking for sensitive boundaries
- [ ] Benchmark spatial join performance with and without predicate pushdown
- [ ] Automate schema validation in CI/CD to prevent geometry drift

The spatial lakehouse is not a drop-in replacement for PostGIS or Oracle Spatial. It is a distributed, versioned, and compute-agnostic architecture that demands explicit spatial engineering. Teams that treat geometry as a first-class lakehouse primitive—rather than an opaque binary blob—achieve scalable, reproducible, and cost-efficient geospatial analytics.
