# Compacting Spatial Iceberg Tables with rewrite_data_files

This guide gives a complete, runnable recipe for compacting a small-file-ridden spatial Iceberg table using the `rewrite_data_files` procedure with a bounding-box sort order, tuned target file size, and partial-progress safety, then verifying the result through the files metadata table.

## Context and prerequisites

Spatial ingestion — streaming GPS pings, per-tile raster footprints, incremental vector loads — produces many small Parquet files, each with a wide bounding-box statistics footprint. Query planning slows because the engine must open and filter thousands of manifests, and bbox-based [predicate pushdown](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/z-ordering-for-geospatial-queries/) prunes poorly when co-located geometries are scattered across files. The fix is a sort-based rewrite that both merges small files toward a target size and physically clusters rows by their bounding box. This is one of the maintenance disciplines automated in [lakehouse maintenance automation](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/lakehouse-maintenance-automation/), and it assumes Apache Iceberg 1.4+ (examples use the 1.9.0 Spark runtime) on Spark 3.5, with a table carrying `bbox_minx/miny/maxx/maxy` columns computed at ingest.

## Complete working solution

The following script builds a Spark session against a REST catalog, checks whether the table is fragmented enough to warrant a rewrite, runs the sort-based `rewrite_data_files` with spatial sort order, and prints the procedure's result row (rewritten and added file counts).

```python
from pyspark.sql import SparkSession

def build_spark(app: str = "iceberg-spatial-compaction") -> SparkSession:
    return (
        SparkSession.builder.appName(app)
        .config("spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.0")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "rest")
        .config("spark.sql.catalog.lake.uri", "http://catalog:8181")
        .config("spark.sql.catalog.lake.warehouse", "s3://lakehouse/wh")
        .config("spark.sql.shuffle.partitions", "64")
        .getOrCreate()
    )

def fragmentation_ratio(spark, table_fqn: str) -> tuple[int, float]:
    """Return (file_count, avg_file_mb) from the files metadata table."""
    row = spark.sql(f"""
        SELECT count(*) AS n,
               avg(file_size_in_bytes) / 1048576 AS avg_mb
        FROM lake.{table_fqn}.files
    """).collect()[0]
    return int(row["n"]), float(row["avg_mb"] or 0.0)

def compact(spark, table_fqn: str,
            target_bytes: int = 134_217_728,   # 128 MB
            min_input_files: int = 5) -> None:
    """Sort-based rewrite clustering rows on the bounding box."""
    result = spark.sql(f"""
        CALL lake.system.rewrite_data_files(
          table => '{table_fqn}',
          strategy => 'sort',
          sort_order => 'bbox_minx ASC NULLS LAST, bbox_miny ASC NULLS LAST',
          options => map(
            'target-file-size-bytes', '{target_bytes}',
            'min-input-files', '{min_input_files}',
            'max-concurrent-file-group-rewrites', '4',
            'partial-progress.enabled', 'true',
            'partial-progress.max-commits', '10',
            'rewrite-job-order', 'files-desc'
          )
        )
    """)
    result.show(truncate=False)

if __name__ == "__main__":
    spark = build_spark()
    table = "gis.telemetry"
    n_before, avg_before = fragmentation_ratio(spark, table)
    print(f"before: {n_before} files, avg {avg_before:.1f} MB")

    # only rewrite when fragmentation actually justifies the compute
    if avg_before < 64.0 or n_before > 200:
        compact(spark, table)
        n_after, avg_after = fragmentation_ratio(spark, table)
        print(f"after:  {n_after} files, avg {avg_after:.1f} MB")
    else:
        print("table already well-sized; skipping rewrite")
    spark.stop()
```

## Step-by-step walkthrough

1. **`build_spark`** wires the Iceberg 1.9.0 Spark 3.5 runtime, registers the `IcebergSparkSessionExtensions` (required for the `CALL` procedure syntax), and points at a REST catalog named `lake`. `spark.sql.shuffle.partitions` is set to 64 to match a modest executor pool — this governs how the sort output is partitioned and therefore how many files the rewrite emits.

2. **`fragmentation_ratio`** reads the built-in `.files` metadata table, which exposes one row per data file with `file_size_in_bytes`. Computing the average size and count here is cheap (metadata-only) and lets you skip the expensive rewrite when it is not needed.

3. **`strategy => 'sort'`** selects a sort-based rewrite rather than the default binpack. Binpack only merges files by size; sort additionally reorders rows. The **`sort_order`** clusters rows by `bbox_minx` then `bbox_miny`, so geometries near each other in space land in the same file — the property that makes file-level bbox pruning effective.

4. **`target-file-size-bytes` = 128 MB** is the size the rewriter aims each output file toward. It is the single most important spatial tuning knob: 128 MB balances scan efficiency against pruning granularity.

5. **`min-input-files` = 5** stops the procedure from rewriting file groups that already have fewer than five files, avoiding pointless churn on nearly-compacted partitions.

6. **`max-concurrent-file-group-rewrites` = 4** bounds parallelism to keep object-storage request rates under the throttling threshold.

7. **`partial-progress.enabled` = true** with **`max-commits` = 10** checkpoints the rewrite across up to ten commits, so a failure late in a large rewrite keeps the work already done instead of discarding all of it.

8. **`rewrite-job-order` = 'files-desc'** processes the most-fragmented file groups first, so the biggest planning wins land earliest even if the job is interrupted.

The gate before `compact` is the same non-blocking, threshold-driven pattern used across [async execution patterns](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/async-execution-patterns/) — never pay for a rewrite the table does not need.

### Why sort, not binpack

Iceberg offers two rewrite strategies and choosing wrong wastes the entire operation for spatial workloads. **Binpack** is a pure size operation: it concatenates adjacent small files into target-sized ones without touching row order. It is fast and cheap, but it does nothing for spatial locality — if your small files were each written from a different ingest batch spanning the whole globe, binpacking them produces large files that *each still span the whole globe*, so a bounding-box filter cannot skip any of them. **Sort** pays the extra cost of a full shuffle-and-sort so that rows are reordered by the sort key before being written into target-sized files. With `bbox_minx, bbox_miny` as the key, each output file ends up covering a compact, contiguous slice of space, and its min/max statistics become tight. That tightness is what the query planner reads to skip files. The rule: use binpack only when the table is already spatially partitioned and you just need to defragment within a partition; use sort whenever file-level bbox pruning matters, which for a queryable spatial table is almost always.

The single-dimension sort used here (X then Y) is simpler than a space-filling curve but has a known weakness: it clusters tightly along longitude and only loosely along latitude. When query patterns are truly two-dimensional — bounding-box windows rather than longitude strips — a Z-order or Hilbert interleaving of the bbox columns clusters better in both dimensions at once. That variant, and when it is worth the extra cost, is covered in [Z-ordering for geospatial queries](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/z-ordering-for-geospatial-queries/); for a table whose queries are dominated by one axis (a north-south transport corridor, say) the linear sort here is both cheaper and sufficient.

## Common errors and fixes

| Error | Cause | Fix |
|---|---|---|
| `Cannot find procedure rewrite_data_files` | Iceberg Spark extensions not registered | Add `spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions` |
| File count barely changes after run | Files already ≥ target size, or `min-input-files` too high | Lower `min-input-files`; confirm the table is genuinely fragmented via the `.files` query |
| `503 SlowDown` from S3 mid-rewrite | Too many concurrent file-group rewrites | Reduce `max-concurrent-file-group-rewrites` to 3; `partial-progress` preserves committed groups |
| Output files smaller than target | `spark.sql.shuffle.partitions` too high, splitting the sort output | Lower shuffle partitions toward total executor core count |

## Verification

Confirm the rewrite merged files and left the data unchanged. First, per-partition file health from the metadata table:

```sql
-- Spark SQL
SELECT partition,
       count(*) AS file_count,
       cast(avg(file_size_in_bytes) / 1048576 AS int) AS avg_mb
FROM lake.gis.telemetry.files
GROUP BY partition
ORDER BY file_count DESC;
```

Then prove the rewrite was layout-only by comparing row count and aggregate bounding-box extent against the pre-rewrite values — they must be identical:

```sql
SELECT count(*) AS n,
       min(bbox_minx) AS xmin, min(bbox_miny) AS ymin,
       max(bbox_maxx) AS xmax, max(bbox_maxy) AS ymax
FROM lake.gis.telemetry;
```

Inspect the manifests directly to confirm the rewrite consolidated them and that the bbox statistics are now tight. The `manifests` metadata table lists each manifest with its partition-value bounds and the count of data files it tracks; after a successful sort-rewrite you should see fewer manifests, each referencing more files, with narrower spatial ranges:

```sql
-- Spark SQL: one row per manifest after the rewrite
SELECT path,
       added_data_files_count + existing_data_files_count AS files_tracked,
       partition_summaries
FROM lake.gis.telemetry.manifests
ORDER BY files_tracked DESC;
```

Finally, inspect that a new snapshot with a `replace` operation was recorded — a sort-based `rewrite_data_files` commits as a `replace`, never an `append` or `overwrite`, which is your proof the operation was layout-only:

```sql
SELECT snapshot_id, operation, summary['total-data-files'] AS files
FROM lake.gis.telemetry.snapshots
ORDER BY committed_at DESC LIMIT 3;
```

If `operation` reads anything other than `replace`, the rewrite did more than reorganize files and you should investigate before trusting the result. You can also compare `summary['total-data-files']` between the two most recent snapshots to quantify the reduction — going from, say, 512 to 47 files is the number to log and alert on.

<figure class="diagram">
<svg viewBox="0 0 718 221" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Many small spatially-scattered files merged and bbox-sorted into fewer large files">
<defs>
<marker id="arw-ice-compact" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#0e6e7d"/></marker>
</defs>
<rect x="0" y="0" width="718" height="221" fill="#f7fbfc"/>
<text x="380" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Sort-based rewrite: small scattered files to bbox-clustered files</text>
<text x="140" y="58" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="600" fill="#33707d">before: 12 small files</text>
<rect x="40" y="70" width="46" height="30" rx="3" fill="#ffffff" stroke="#9a5a17" stroke-width="1.5"/>
<rect x="96" y="70" width="46" height="30" rx="3" fill="#ffffff" stroke="#9a5a17" stroke-width="1.5"/>
<rect x="152" y="70" width="46" height="30" rx="3" fill="#ffffff" stroke="#9a5a17" stroke-width="1.5"/>
<rect x="40" y="110" width="46" height="30" rx="3" fill="#ffffff" stroke="#9a5a17" stroke-width="1.5"/>
<rect x="96" y="110" width="46" height="30" rx="3" fill="#ffffff" stroke="#9a5a17" stroke-width="1.5"/>
<rect x="152" y="110" width="46" height="30" rx="3" fill="#ffffff" stroke="#9a5a17" stroke-width="1.5"/>
<rect x="40" y="150" width="46" height="30" rx="3" fill="#ffffff" stroke="#9a5a17" stroke-width="1.5"/>
<rect x="96" y="150" width="46" height="30" rx="3" fill="#ffffff" stroke="#9a5a17" stroke-width="1.5"/>
<rect x="152" y="150" width="46" height="30" rx="3" fill="#ffffff" stroke="#9a5a17" stroke-width="1.5"/>
<text x="120" y="205" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#3d5a63">avg 12 MB, bbox scattered</text>
<rect x="300" y="95" width="160" height="60" rx="8" fill="#ffffff" stroke="#6a3d9a" stroke-width="2"/>
<text x="380" y="120" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="600" fill="#0d3b45">rewrite_data_files</text>
<text x="380" y="138" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">sort by bbox_minx, bbox_miny</text>
<line x1="205" y1="125" x2="300" y2="125" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-ice-compact)"/>
<line x1="460" y1="125" x2="560" y2="125" stroke="#0e6e7d" stroke-width="2" marker-end="url(#arw-ice-compact)"/>
<text x="640" y="58" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="600" fill="#33707d">after: 2 large files</text>
<rect x="575" y="80" width="130" height="42" rx="4" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<text x="640" y="105" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">128 MB · bbox A-block</text>
<rect x="575" y="132" width="130" height="42" rx="4" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<text x="640" y="157" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">128 MB · bbox B-block</text>
<text x="640" y="205" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#3d5a63">clustered → prunable</text>
</svg>
</figure>

The same clustering principle underpins Z-ordering; for the multi-dimensional variant used on spatial joins see [Z-ordering for geospatial queries](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/z-ordering-for-geospatial-queries/), and for the Delta-side equivalent of this whole flow see [scheduling VACUUM for spatial Delta tables](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/lakehouse-maintenance-automation/scheduling-vacuum-for-spatial-delta-tables/). The authoritative parameter reference is the Iceberg [Spark procedures documentation](https://iceberg.apache.org/docs/latest/spark-procedures/#rewrite_data_files) and the [maintenance guide](https://iceberg.apache.org/docs/latest/maintenance/#compact-data-files).

## Choosing a Rewrite Strategy

`rewrite_data_files` offers more than one strategy, and the choice changes both the cost and the benefit substantially on a spatial table.

<figure class="diagram">
<svg viewBox="0 0 764 244" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three rewrite strategies compared: binpack which only merges files, sort which orders rows within the rewritten set, and zorder which interleaves two spatial columns, with their relative cost and effect on pruning">
<rect x="0" y="0" width="764" height="244" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Three strategies, three cost-benefit points</text>
<rect x="26" y="56" width="230" height="176" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="141" y="84" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">binpack</text>
<text x="141" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">merges small files only</text>
<text x="141" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">cost: lowest, no shuffle</text>
<text x="141" y="164" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">fixes: file count</text>
<text x="141" y="188" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">does not fix: clustering</text>
<text x="141" y="214" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">use hourly</text>
<rect x="274" y="56" width="230" height="176" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="389" y="84" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">sort</text>
<text x="389" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">orders by declared columns</text>
<text x="389" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">cost: shuffle within scope</text>
<text x="389" y="164" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">fixes: one-dimensional locality</text>
<text x="389" y="188" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">good for time-first keys</text>
<text x="389" y="214" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">use daily</text>
<rect x="522" y="56" width="230" height="176" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="637" y="84" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">zorder</text>
<text x="637" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">interleaves two coordinates</text>
<text x="637" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">cost: highest</text>
<text x="637" y="164" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">fixes: two-dimensional locality</text>
<text x="637" y="188" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">the one spatial queries want</text>
<text x="637" y="214" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">use on partition close</text>
</svg>
</figure>

The pattern that balances cost against benefit is to run binpack frequently against the open partition — it is cheap, needs no shuffle, and keeps the file count under control as micro-batches arrive — and to run the Z-order rewrite once, when the partition closes and will not be written to again. The expensive operation then happens exactly once per partition for the lifetime of the table.

## Scoping the Rewrite

<figure class="diagram">
<svg viewBox="0 0 764 202" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Filters that scope a rewrite: a partition predicate limiting which data is touched, a file size threshold selecting only small files, and a maximum group size bounding each task">
<rect x="0" y="0" width="764" height="202" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Three limits that keep a rewrite bounded</text>
<rect x="26" y="58" width="230" height="132" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="141" y="86" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">partition predicate</text>
<text x="141" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">where clause on the key</text>
<text x="141" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">avoids touching history</text>
<text x="141" y="162" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">and avoids write conflicts</text>
<rect x="274" y="58" width="230" height="132" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="389" y="86" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">min-file-size</text>
<text x="389" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">only rewrite what is small</text>
<text x="389" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">already-large files are</text>
<text x="389" y="162" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">left untouched</text>
<rect x="522" y="58" width="230" height="132" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="637" y="86" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">max-file-group-size</text>
<text x="637" y="112" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">bounds each task&#8217;s input</text>
<text x="637" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">prevents one enormous</text>
<text x="637" y="162" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">straggler task</text>
</svg>
</figure>

The middle limit is the one most often left at its default and most worth tuning on a spatial table, because geometry-heavy files reach the target size at far lower row counts than scalar data. Setting it from the observed size distribution rather than from a general default avoids rewriting files that were already fine.
Measure the distribution once with a metadata query, set the threshold from it, and revisit only when the data shape changes.
