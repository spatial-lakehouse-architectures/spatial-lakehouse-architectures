# Materializing Bbox Columns for Pushdown

This guide adds the four numeric bounding-box columns to an existing spatial table, places them where statistics will be collected, backfills history without a maintenance window, and verifies that file pruning actually improves.

## Context and prerequisites

Every pruning mechanism in a lakehouse compares numbers. A geometry column is opaque to all of them, so a table without derived bounding-box columns cannot prune on location no matter how it is partitioned or sorted. This recipe works with Iceberg 1.4 or Delta 3.x on Spark 3.5; the mechanism is explained in [predicate pushdown optimization](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/predicate-pushdown-optimization/), and the join-side benefit in [reducing candidate pairs with bbox covering columns](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/spatial-join-optimization/reducing-candidate-pairs-with-bbox-covering-columns/).

## Where the columns must sit

<figure class="diagram">
<svg viewBox="0 0 742 242" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Schema layout showing the bounding box columns placed inside the statistics window near the front, with the geometry payload and wide attributes after it, and the consequence of placing them beyond the limit">
<rect x="0" y="0" width="742" height="242" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Position in the schema decides whether they work</text>
<rect x="50" y="58" width="680" height="88" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2.5"/>
<text x="390" y="84" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">inside the statistics window</text>
<rect x="70" y="96" width="130" height="36" rx="5" fill="#ffffff" stroke="#2f6e49" stroke-width="1.5"/>
<text x="135" y="120" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">id, event_day</text>
<rect x="212" y="96" width="130" height="36" rx="5" fill="#ffffff" stroke="#2f6e49" stroke-width="1.5"/>
<text x="277" y="120" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">partition key</text>
<rect x="354" y="96" width="240" height="36" rx="5" fill="#d7e8de" stroke="#2f6e49" stroke-width="2"/>
<text x="474" y="120" text-anchor="middle" font-family="sans-serif" font-size="11" font-weight="700" fill="#0d3b45">bbox_min_x … bbox_max_y</text>
<rect x="606" y="96" width="106" height="36" rx="5" fill="#ffffff" stroke="#2f6e49" stroke-width="1.5"/>
<text x="659" y="120" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">attributes</text>
<rect x="50" y="164" width="680" height="66" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="390" y="190" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">beyond it: geometry payload, free text, rarely-filtered columns</text>
<text x="390" y="214" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">statistics here would be meaningless anyway — and would inflate the metadata</text>
</svg>
</figure>

The failure this prevents is entirely silent. A table with the columns present but positioned past the statistics limit has all the right data, all the right properties, and no file pruning whatsoever: the planner reads every file because it has no bounds to compare against. Nothing in the query, the plan or the result indicates it.

## Complete working solution

```sql
-- Iceberg. Add the columns, then set metrics explicitly — the default truncates.
ALTER TABLE lakehouse.spatial.telemetry ADD COLUMN bbox_min_x DOUBLE AFTER h3_r5;
ALTER TABLE lakehouse.spatial.telemetry ADD COLUMN bbox_min_y DOUBLE AFTER bbox_min_x;
ALTER TABLE lakehouse.spatial.telemetry ADD COLUMN bbox_max_x DOUBLE AFTER bbox_min_y;
ALTER TABLE lakehouse.spatial.telemetry ADD COLUMN bbox_max_y DOUBLE AFTER bbox_max_x;

ALTER TABLE lakehouse.spatial.telemetry SET TBLPROPERTIES (
  'write.metadata.metrics.column.bbox_min_x' = 'full',
  'write.metadata.metrics.column.bbox_min_y' = 'full',
  'write.metadata.metrics.column.bbox_max_x' = 'full',
  'write.metadata.metrics.column.bbox_max_y' = 'full',
  'write.metadata.metrics.column.geom_wkb'   = 'none'
);
```

```python
# Backfill one partition at a time. Idempotent; safe to re-run a failed partition.
from pyspark.sql import functions as F

def backfill_day(spark, table: str, day: str) -> None:
    src = spark.table(table).where(F.col("event_day") == day)
    filled = src.selectExpr(
        "*",
        "ST_XMin(ST_GeomFromWKB(geom_wkb)) AS new_min_x",
        "ST_YMin(ST_GeomFromWKB(geom_wkb)) AS new_min_y",
        "ST_XMax(ST_GeomFromWKB(geom_wkb)) AS new_max_x",
        "ST_YMax(ST_GeomFromWKB(geom_wkb)) AS new_max_y",
    ).drop("bbox_min_x", "bbox_min_y", "bbox_max_x", "bbox_max_y") \
     .withColumnRenamed("new_min_x", "bbox_min_x") \
     .withColumnRenamed("new_min_y", "bbox_min_y") \
     .withColumnRenamed("new_max_x", "bbox_max_x") \
     .withColumnRenamed("new_max_y", "bbox_max_y")

    (filled.sortWithinPartitions("bbox_min_x", "bbox_min_y")
           .writeTo(table).overwritePartitions())
```

```python
# And in the write path, from now on — derived in the same expression as everything else.
enriched = raw.selectExpr(
    "*",
    "ST_XMin(ST_GeomFromWKB(geom_wkb)) AS bbox_min_x",
    "ST_YMin(ST_GeomFromWKB(geom_wkb)) AS bbox_min_y",
    "ST_XMax(ST_GeomFromWKB(geom_wkb)) AS bbox_max_x",
    "ST_YMax(ST_GeomFromWKB(geom_wkb)) AS bbox_max_y")
```

## Step-by-step walkthrough

1. **Add the columns before the geometry, not after it.** `AFTER` clauses give explicit control over position; without them, added columns land at the end, which is exactly where they must not be on a wide table.

2. **Set the metrics mode explicitly.** The default is commonly a truncated mode that is useless for doubles, so a column with default metrics has bounds that no predicate can use. Setting the geometry column to `none` is equally deliberate: min/max of WKB bytes is meaningless and inflates the manifests.

3. **Backfill per partition with an overwrite.** Overwriting a partition is atomic and idempotent, so a failed partition can simply be re-run. Attempting a table-wide update instead makes a partial failure very awkward to reason about.

4. **Sort during the backfill.** The rewrite is happening anyway, so applying the sort order costs the shuffle once instead of requiring a second pass later.

5. **Derive in the write path in the same expression chain as the geometry.** Any arrangement in which the geometry is written by one step and the bounding box by another has a window where they can disagree.

## Common errors and fixes

| Symptom | Cause | Fix |
|---|---|---|
| No pruning improvement after backfill | Columns beyond the statistics limit | Reposition them, or raise the limit, then rewrite |
| Bounds present but pruning still poor | Files unsorted, so per-file boxes are wide | Sort on the bbox columns during compaction |
| Some rows have null bounds | Backfill missed a partition, or geometry is null | Re-run the partition; treat null geometry as null bounds deliberately |
| Boxes do not contain their geometry | Derived before a reprojection or repair | Recompute from the stored geometry; assert coverage |
| Manifest size grew sharply | Geometry column still has full metrics | Set the geometry column's metrics to `none` |

## Verification

<figure class="diagram">
<svg viewBox="0 0 732 240" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Before and after file counts for a city scoped query, showing the scan dropping from every file to a small fraction once bounding box statistics are present">
<rect x="0" y="0" width="732" height="240" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">The acceptance test is files scanned</text>
<text x="200" y="64" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#9a5a17">before</text>
<rect x="60" y="78" width="280" height="40" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="200" y="104" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">18 400 of 18 400 files</text>
<text x="200" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">query time 6 min 40 s</text>
<text x="580" y="64" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#2f6e49">after</text>
<rect x="440" y="78" width="14" height="40" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<rect x="454" y="78" width="266" height="40" fill="none" stroke="#cfe3e7" stroke-width="1.5" stroke-dasharray="4 4"/>
<text x="580" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">query time 4.1 s</text>
<text x="580" y="104" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">— 210 files</text>
<text x="390" y="196" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">Same query, same data, same engine — only the statistics changed</text>
<text x="390" y="224" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">Measure with a cold cache; a warm one flatters both</text>
</svg>
</figure>

```python
def assert_pruning(spark, table: str, predicate: str, max_fraction: float = 0.05):
    total = spark.sql(f"SELECT count(*) n FROM {table}.files").collect()[0]["n"]
    plan = spark.sql(f"SELECT count(*) FROM {table} WHERE {predicate}") \
                .queryExecution.executedPlan.toString()
    scanned = int(plan.split("numFiles=")[1].split(",")[0].strip(") "))
    assert scanned / total < max_fraction, (
        f"pruning regressed: {scanned}/{total} files scanned")
```

Wire the assertion into the pipeline that writes the table so a future schema change that pushes the columns past the statistics limit fails a build rather than a customer's query. This is the specific regression the whole exercise exists to prevent, and it is invisible in every other kind of test.

Record the before-and-after file counts. They are the evidence that the backfill achieved something, and having them written down makes the same argument easy for the next table.

## Making the Columns Easy to Use

Materialising the columns is half the job; the other half is ensuring queries actually reference them, because a column nobody filters on prunes nothing.

<figure class="diagram">
<svg viewBox="0 0 764 222" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three ways to ensure callers supply the numeric predicate: a view exposing the columns, a table valued function taking a window, and a client helper that builds the SQL from a geometry">
<rect x="0" y="0" width="764" height="222" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Make the fast query the easy one to write</text>
<rect x="26" y="58" width="230" height="152" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="141" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">document the columns</text>
<text x="141" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">table and column comments</text>
<text x="141" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">visible in every catalogue</text>
<text x="141" y="166" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">browser and DESCRIBE</text>
<text x="141" y="192" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">cheapest, weakest</text>
<rect x="274" y="58" width="230" height="152" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="389" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">a helper function</text>
<text x="389" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">takes a window, returns rows</text>
<text x="389" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">the predicate cannot be</text>
<text x="389" y="166" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">omitted by accident</text>
<text x="389" y="192" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">strongest where supported</text>
<rect x="522" y="58" width="230" height="152" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="637" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">a plan assertion</text>
<text x="637" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">in the scheduled jobs</text>
<text x="637" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">a slow query becomes</text>
<text x="637" y="166" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">a failing build</text>
<text x="637" y="192" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#6a3d9a">catches what the others miss</text>
</svg>
</figure>

The table comment is the highest-value single line available, because it reaches a caller at the exact moment they are writing the query. Something as short as "filter bbox_min_x/max_x and bbox_min_y/max_y to prune; ST_Intersects alone reads the whole table" appears in every schema panel and prevents the most common mistake without any tooling at all.

The plan assertion is what catches the queries that ignore all of the above. Applied to scheduled jobs — which are where the sustained cost lives — it converts an invisible waste into a visible failure, and it is a dozen lines.

## Cost and Timing of the Backfill

The backfill is a full rewrite of the partitions it touches, so its cost is a table copy and its duration is predictable from the table size and cluster.

Two things reduce it. **Restricting the backfill to partitions that are actually queried** is legitimate on a table with a long tail of cold history: filling the last two years and leaving the rest unfilled costs a fraction and captures nearly all the benefit, provided the query path tolerates null bounds by falling back to a scan for those partitions. **Combining it with a compaction that was due anyway** makes the rewrite serve two purposes for one cost.

The timing consideration is contention. Overwriting partitions conflicts with a streaming writer targeting the same ones, so backfill closed partitions and leave the current one until the write path has been updated to derive the columns natively. That ordering means the backfill never contends with ingest and the current partition acquires its bounds by ordinary writing rather than by rewrite.

Run the pruning assertion against a backfilled partition before continuing with the rest. A backfill that runs to completion and produces no pruning improvement — because of the statistics-window mistake — is a great deal of compute spent for nothing, and one partition is enough to find out.

## Handling Null and Empty Geometry

A spatial table almost always contains rows whose geometry is missing, and the bounding-box columns need an explicit policy for them rather than whatever the derivation happens to produce.

**Null geometry gives null bounds.** This is the correct behaviour and it has a useful property: a null in any bound column means every range comparison evaluates to unknown, so the row is excluded from every spatial filter — which is what a row with no location should do. Nothing further is needed.

**Empty geometry is the trap.** An empty polygon has no coordinates, and different implementations return either nulls, zeros, or infinities for its bounds. Zeros are the dangerous case: they place the row at the origin off the coast of West Africa, where it will be returned by any query covering that area and will contribute a spurious point to every extent calculation. Normalise empty geometry to null at ingest, as the validation gate in [geometry validation and repair](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/geometry-validation-and-repair/) does, and the case disappears.

**A single point gives a degenerate box.** Minimum equals maximum on both axes, which is correct and works fine in every range comparison. No special handling is needed, but a validation asserting `min <= max` should use a non-strict comparison or it will reject every point in the table.

Assert the policy rather than assuming it. A count of rows where the geometry is non-null and any bound is null, and a count where the bounds are zero but the geometry is not at the origin, are two cheap checks that catch both failure modes and take seconds on any table.
Both belong in the same scheduled audit as the pruning assertion, since all three answer the same underlying question about whether the table still supports the layout it was designed for.
Running them together, on a schedule, keeps the answer current rather than remembered. A remembered answer is one nobody can act on.
