# Iceberg vs Delta Lake for Spatial Data

This guide compares Apache Iceberg and Delta Lake specifically for spatial workloads — how each handles geometry, partitioning, clustering, engine access, and maintenance — and gives a recommendation matrix so you can pick the right table format for the way your GIS data is actually queried.

## Context and prerequisites

Both Iceberg and Delta are ACID table formats over columnar files, and neither has a native geometry type in its stable, cross-engine core: spatial data lives as WKB in a binary column, augmented by explicit bounding-box columns that expose coordinate locality to file-skipping. That shared baseline is covered in [Open Table Format Versioning](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/open-table-format-versioning/). The differences that matter for spatial work are not about geometry types at all — they are about *how each format prunes files by location*, which engines can read the result, and how compaction reorganizes spatial data. This comparison assumes you store geometry as WKB in the EPSG:4326 convention and want to decide which format to standardize on. Runtime baselines below use Iceberg 1.9.0 jars (format-version 2) on Spark 3.5, and Delta 3.x on the same Spark.

## The core difference: how each format prunes by location

Neither format indexes geometry directly; both skip files using min/max column statistics. What differs is *how those statistics get organized and declared*.

Iceberg uses **hidden partitioning**: you declare a partition transform on a column and the engine derives partition values automatically, so writers and readers never hand-maintain a partition column. For spatial data this is powerful because transforms like `bucket(N, cell_id)` or `truncate` can partition on a derived grid cell — for example an [H3 or geohash cell mapped to a partition](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/spatial-partitioning-schemes/implementing-h3-hexagon-partitioning-in-delta-lake/) — without a physical column the query author must remember to filter. A predicate on the source column prunes partitions transparently.

Delta has no hidden partitioning. Partition columns are physical, and pruning depends on the query filtering that exact column. Delta's answer to spatial locality is **clustering**: classic `OPTIMIZE ZORDER BY (bbox_min_x, bbox_min_y)` and, on Delta 3.1+, **liquid clustering** (`CLUSTER BY`), which decouples the clustering layout from partitioning and lets you cluster on bbox columns without committing to a rigid partition scheme. Iceberg's equivalent is a table `write.sort-order` (Z-order/Hilbert on bbox columns) applied during writes and compaction.

<figure class="diagram">
<svg viewBox="0 0 760 250" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Side by side comparison of Iceberg hidden partitioning with sort order versus Delta physical partitions with liquid clustering for spatial file pruning">
<title>Iceberg vs Delta spatial pruning models</title>
<desc>Iceberg derives partitions from transforms on a source column and sorts files by a spatial sort order; Delta uses physical partition columns plus Z-order or liquid clustering on bounding-box columns.</desc>
<defs>
<marker id="arw-icevsdelta" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
<path d="M0 0 L10 5 L0 10 z" fill="#0e6e7d"/>
</marker>
</defs>
<rect x="0" y="0" width="760" height="250" fill="#f7fbfc"/>
<text x="190" y="26" text-anchor="middle" font-family="sans-serif" font-size="14" font-weight="bold" fill="#0e6e7d">Apache Iceberg</text>
<text x="570" y="26" text-anchor="middle" font-family="sans-serif" font-size="14" font-weight="bold" fill="#6a3d9a">Delta Lake</text>
<line x1="380" y1="36" x2="380" y2="238" stroke="#cfe3e7" stroke-width="1.5"/>
<rect x="40" y="48" width="140" height="48" rx="6" fill="#ffffff" stroke="#cfe3e7" stroke-width="1.5"/>
<text x="110" y="68" text-anchor="middle" font-family="sans-serif" font-size="11.5" font-weight="bold" fill="#0d3b45">source column</text>
<text x="110" y="85" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">cell_id / geom</text>
<rect x="40" y="122" width="140" height="48" rx="6" fill="#ffffff" stroke="#0e6e7d" stroke-width="1.5"/>
<text x="110" y="142" text-anchor="middle" font-family="sans-serif" font-size="11.5" font-weight="bold" fill="#0d3b45">hidden partition</text>
<text x="110" y="159" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">bucket() / truncate()</text>
<rect x="40" y="196" width="140" height="44" rx="6" fill="#ffffff" stroke="#2f6e49" stroke-width="1.5"/>
<text x="110" y="215" text-anchor="middle" font-family="sans-serif" font-size="11.5" font-weight="bold" fill="#0d3b45">sort-order</text>
<text x="110" y="231" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">Z/Hilbert on bbox</text>
<rect x="220" y="122" width="130" height="48" rx="6" fill="#ffffff" stroke="#9a5a17" stroke-width="1.5"/>
<text x="285" y="142" text-anchor="middle" font-family="sans-serif" font-size="11.5" font-weight="bold" fill="#9a5a17">manifest stats</text>
<text x="285" y="159" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">prune files</text>
<line x1="110" y1="96" x2="110" y2="118" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-icevsdelta)"/>
<line x1="110" y1="170" x2="110" y2="192" stroke="#2f6e49" stroke-width="2" marker-end="url(#arw-icevsdelta)"/>
<line x1="180" y1="146" x2="216" y2="146" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-icevsdelta)"/>
<rect x="420" y="48" width="140" height="48" rx="6" fill="#ffffff" stroke="#cfe3e7" stroke-width="1.5"/>
<text x="490" y="68" text-anchor="middle" font-family="sans-serif" font-size="11.5" font-weight="bold" fill="#0d3b45">physical partition</text>
<text x="490" y="85" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">ingest_date col</text>
<rect x="420" y="122" width="140" height="48" rx="6" fill="#ffffff" stroke="#6a3d9a" stroke-width="1.5"/>
<text x="490" y="142" text-anchor="middle" font-family="sans-serif" font-size="11.5" font-weight="bold" fill="#0d3b45">liquid cluster</text>
<text x="490" y="159" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">CLUSTER BY bbox</text>
<rect x="420" y="196" width="140" height="44" rx="6" fill="#ffffff" stroke="#2f6e49" stroke-width="1.5"/>
<text x="490" y="215" text-anchor="middle" font-family="sans-serif" font-size="11.5" font-weight="bold" fill="#0d3b45">OPTIMIZE</text>
<text x="490" y="231" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">ZORDER bbox</text>
<rect x="600" y="122" width="130" height="48" rx="6" fill="#ffffff" stroke="#9a5a17" stroke-width="1.5"/>
<text x="665" y="142" text-anchor="middle" font-family="sans-serif" font-size="11.5" font-weight="bold" fill="#9a5a17">delta log stats</text>
<text x="665" y="159" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">data skipping</text>
<line x1="490" y1="96" x2="490" y2="118" stroke="#6a3d9a" stroke-width="2" marker-end="url(#arw-icevsdelta)"/>
<line x1="490" y1="170" x2="490" y2="192" stroke="#2f6e49" stroke-width="2" marker-end="url(#arw-icevsdelta)"/>
<line x1="560" y1="146" x2="596" y2="146" stroke="#6a3d9a" stroke-width="2" marker-end="url(#arw-icevsdelta)"/>
</svg>
</figure>

## Feature comparison for spatial workloads

| Dimension | Apache Iceberg (1.9.0, fmt v2) | Delta Lake (3.x) |
|---|---|---|
| Geometry storage | WKB in `BINARY` column + bbox columns; a Geometry/Geography type is emerging in the spec but not yet universal across engines. See [Iceberg spatial type support](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/iceberg-spatial-type-support/). | WKB in `BINARY` column + bbox columns; geometry via UDT/reader logic. See [Delta Lake geometry handling](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/delta-lake-geometry-handling/). |
| Partitioning | Hidden partitioning with transforms (`bucket`, `truncate`, `days`); derives partition from a grid/cell column automatically. | Physical partition columns only; query must filter the partition column explicitly. |
| Spatial clustering | Table `write.sort-order` (Z-order/Hilbert on bbox); applied on write + compaction. | `OPTIMIZE ZORDER BY` and liquid `CLUSTER BY` (Delta 3.1+) on bbox columns. |
| File pruning | Manifest-level min/max on bbox columns; prunes whole manifests before touching data files. | `_delta_log` per-file min/max stats; data skipping on bbox columns. |
| Partition evolution | Yes — change partition spec without rewriting old data. | No true partition evolution; requires rewrite. Liquid clustering softens this. |
| Engine read support | Spark, Trino, Flink, DuckDB (iceberg ext), PyIceberg, Sedona. Broadest multi-engine reach. | Spark (first-class), Trino/Presto, DuckDB (delta ext), delta-rs/Python; Sedona via Spark. |
| Maintenance | `rewrite_data_files`, `expire_snapshots`, `rewrite_manifests` procedures. | `OPTIMIZE`, `VACUUM`, `REORG`. |
| Best-fit engine | Multi-engine / Trino-centric lakehouses. | Databricks / Spark-centric platforms. |

## Engine support, the deciding factor for many teams

For spatial analytics the engine mix often decides the format before any feature does. If your query layer is **Trino or DuckDB** doing federated spatial SQL, Iceberg has the more mature, longer-standing connector story and is the safer default — DuckDB and Trino both read Iceberg spatial tables cleanly, and Iceberg's hidden partitioning means those engines prune without the query author writing partition predicates. If your platform is **Databricks or Spark-centric**, Delta is first-class: liquid clustering, photon-accelerated skipping, and the tightest `OPTIMIZE`/`VACUUM` integration live there. **Apache Sedona** — the workhorse for distributed spatial joins — runs on Spark and reads both formats through the Spark catalog, so it does not force the choice, though Iceberg's broader non-Spark reach matters if you also query from Trino/DuckDB outside Sedona.

## Runnable snippets for each format

Create an equivalent spatial table in both formats, partition/cluster on bbox locality, and run the same maintenance intent.

**Apache Iceberg (Spark 3.5, Iceberg 1.9.0):**

```sql
-- Spark SQL. Hidden partition on a coarse grid cell + spatial sort order.
CREATE TABLE lake.gis.parcels (
  parcel_id BIGINT,
  geom      BINARY,          -- WKB, EPSG:4326
  cell_id   BIGINT,          -- coarse grid cell (e.g. H3 res 5)
  bbox_min_x DOUBLE, bbox_min_y DOUBLE,
  bbox_max_x DOUBLE, bbox_max_y DOUBLE
)
USING iceberg
PARTITIONED BY (bucket(64, cell_id))
TBLPROPERTIES (
  'format-version'='2',
  'write.distribution-mode'='range',
  'write.sort-order'='bbox_min_x ASC, bbox_min_y ASC'
);

-- Maintenance: compact + re-cluster small files by the sort order.
CALL lake.system.rewrite_data_files(
  table => 'gis.parcels',
  strategy => 'sort',
  sort_order => 'bbox_min_x ASC, bbox_min_y ASC'
);
```

**Delta Lake (Spark 3.5, Delta 3.x):**

```sql
-- Spark SQL. Liquid clustering on bbox columns (no rigid partitioning).
CREATE TABLE lake.gis.parcels (
  parcel_id BIGINT,
  geom      BINARY,          -- WKB, EPSG:4326
  cell_id   BIGINT,
  bbox_min_x DOUBLE, bbox_min_y DOUBLE,
  bbox_max_x DOUBLE, bbox_max_y DOUBLE
)
USING delta
CLUSTER BY (bbox_min_x, bbox_min_y);

-- Maintenance: incremental clustering, then reclaim unreferenced files.
OPTIMIZE lake.gis.parcels;
VACUUM lake.gis.parcels RETAIN 168 HOURS;
```

The maintenance intent is identical — cluster small files by spatial locality, then reclaim space — but the mechanics differ. Iceberg's `rewrite_data_files` with `strategy => 'sort'` compacts and re-sorts in one procedure; deep-dive in [Compacting spatial Iceberg tables with rewrite_data_files](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/lakehouse-maintenance-automation/compacting-spatial-iceberg-tables-with-rewrite-data-files/). Delta separates layout (`OPTIMIZE`) from garbage collection (`VACUUM`), and `VACUUM` retention is a correctness-sensitive knob covered in [Scheduling VACUUM for spatial Delta tables](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/lakehouse-maintenance-automation/scheduling-vacuum-for-spatial-delta-tables/).

For programmatic verification of pruning, compare the physical plan's scanned-file count with and without a bbox predicate:

```python
# Both formats: confirm a bbox predicate prunes files.
spark.sql("""
  EXPLAIN FORMATTED
  SELECT parcel_id FROM lake.gis.parcels
  WHERE bbox_min_x > -74.05 AND bbox_max_x < -73.90
    AND bbox_min_y >  40.70 AND bbox_max_y <  40.85
""").show(truncate=False)
```

## Recommendation matrix by scenario

| Scenario | Recommended format | Why |
|---|---|---|
| Multi-engine query layer (Trino + DuckDB + Spark) | Iceberg | Broadest reader support; hidden partitioning prunes without partition-aware SQL. |
| Grid-partitioned data (H3/geohash cell as partition) | Iceberg | Hidden `bucket`/`truncate` transforms partition on the cell column automatically. |
| Databricks / Spark-only platform | Delta | Liquid clustering + Photon skipping + tightest OPTIMIZE/VACUUM integration. |
| Frequently changing partition strategy | Iceberg | Partition evolution rewrites nothing; Delta needs a full rewrite. |
| Layout you want decoupled from partitioning | Delta (liquid clustering) | `CLUSTER BY` re-clusters incrementally without a partition commitment. |
| Sedona-driven distributed spatial joins | Either (lean Iceberg) | Both read via Spark; Iceberg wins only if you also query outside Spark. |
| Streaming appends with heavy time-travel/audit | Iceberg | Snapshot/manifest model prunes metadata efficiently at high snapshot counts. |

**Bottom line:** default to **Iceberg** when your spatial platform is multi-engine or Trino/DuckDB-centric, when you partition on derived grid cells, or when partition strategy is still in flux — its hidden partitioning and partition evolution are decisive for spatial locality. Choose **Delta** when you live inside the Databricks/Spark ecosystem, where liquid clustering on bbox columns and the mature `OPTIMIZE`/`VACUUM` tooling give the smoothest operational path. Neither format's geometry handling should drive the decision today — both store WKB plus bbox columns — so let engine reach, partitioning model, and maintenance ergonomics decide. Once you have chosen, lock in the CRS and schema discipline from [Managing spatial schema evolution in open table formats](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/open-table-format-versioning/managing-spatial-schema-evolution-in-open-table-formats/) so the format you picked stays consistent as the data evolves.

For authoritative detail, consult the [Apache Iceberg documentation](https://iceberg.apache.org/docs/latest/) on partitioning and table maintenance, and the [Delta Lake documentation](https://docs.delta.io/latest/index.html) on liquid clustering and `OPTIMIZE`.
