# Planning Spatial Scans With PyIceberg and DuckDB

This guide separates scan planning from execution: PyIceberg resolves a snapshot and prunes the file list using the table's own metadata, and DuckDB executes full spatial SQL over exactly those files — with no cluster and no loss of the table format's guarantees.

## Context and prerequisites

Each tool does one thing well. PyIceberg knows what the table is, which files a predicate needs, and which snapshot is current; DuckDB reads Parquet and evaluates `ST_*` functions very fast. Combining them gives governed, snapshot-consistent spatial SQL in a single process. This recipe uses PyIceberg 0.7+ and DuckDB 1.1+; the positioning is in [PyIceberg spatial workflows](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/pyiceberg-spatial-workflows/) and [DuckDB geospatial analytics](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/duckdb-geospatial-analytics/).

## The division of labour

<figure class="diagram">
<svg viewBox="0 0 766 240" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="PyIceberg resolving the snapshot and pruning files by partition and statistics, handing a file list to DuckDB which evaluates the exact spatial predicate">
<defs>
<marker id="pspd-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#0e6e7d"/></marker>
</defs>
<rect x="0" y="0" width="766" height="240" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Planning and execution, cleanly separated</text>
<rect x="26" y="66" width="300" height="120" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="176" y="94" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">PyIceberg — planning</text>
<text x="176" y="122" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">resolve the snapshot</text>
<text x="176" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">prune by partition and statistics</text>
<text x="176" y="166" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">return file paths + delete files</text>
<rect x="454" y="66" width="300" height="120" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="604" y="94" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">DuckDB — execution</text>
<text x="604" y="122" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">read exactly those files</text>
<text x="604" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">decode WKB, evaluate ST_*</text>
<text x="604" y="166" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">aggregate and return</text>
<line x1="326" y1="126" x2="454" y2="126" stroke="#0e6e7d" stroke-width="2.5" marker-end="url(#pspd-arrow)"/>
<text x="390" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" font-weight="700" fill="#0d3b45">a file list</text>
<text x="390" y="224" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">Neither tool is asked to do the other&#8217;s job, which is why both are fast</text>
</svg>
</figure>

## Complete working solution

```python
import duckdb
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThanOrEqual, EqualTo

def spatial_scan(catalog_name: str, identifier: str,
                 window: tuple[float, float, float, float],
                 day: str) -> list[tuple]:
    minx, miny, maxx, maxy = window
    table = load_catalog(catalog_name).load_table(identifier)
    snapshot_id = table.current_snapshot().snapshot_id

    # 1. PyIceberg prunes: partition predicate + bbox statistics.
    scan = table.scan(
        row_filter=And(
            EqualTo("event_day", day),
            GreaterThanOrEqual("bbox_max_x", minx),
            LessThanOrEqual("bbox_min_x", maxx),
            GreaterThanOrEqual("bbox_max_y", miny),
            LessThanOrEqual("bbox_min_y", maxy),
        ),
        selected_fields=("asset_id", "event_ts", "geom_wkb",
                         "bbox_min_x", "bbox_min_y", "bbox_max_x", "bbox_max_y"),
    )
    tasks = list(scan.plan_files())
    data_files   = [t.file.file_path for t in tasks]
    delete_files = [d.file_path for t in tasks for d in t.delete_files]
    if delete_files:
        raise NotImplementedError(
            "merge-on-read deletes present — apply them or use the Iceberg extension")

    # 2. DuckDB executes exactly what survived planning.
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("CREATE OR REPLACE SECRET s (TYPE S3, PROVIDER credential_chain);")

    return con.execute("""
        SELECT asset_id, event_ts
        FROM read_parquet($files)
        WHERE ST_Intersects(ST_GeomFromWKB(geom_wkb),
                            ST_MakeEnvelope($minx, $miny, $maxx, $maxy))
    """, {"files": data_files, "minx": minx, "miny": miny,
          "maxx": maxx, "maxy": maxy}).fetchall(), snapshot_id
```

## Step-by-step walkthrough

1. **Pass the numeric predicate to the planner, not the geometry one.** PyIceberg evaluates the filter against partition values and file statistics; it cannot evaluate `ST_Intersects`. The bounding-box comparisons are what let it prune, and they are conservative so nothing is lost.

2. **Select only the fields needed.** The projection is passed through to the Parquet read, so a table whose bulk is attribute columns transfers only what the query uses.

3. **Check for delete files and refuse rather than ignore.** A merge-on-read table returns delete files alongside data files, and reading the data files alone returns rows that were deleted. Failing loudly is far better than the silent wrong answer.

4. **Record the snapshot identifier.** The result is a point-in-time answer, and returning the snapshot alongside it makes the result reproducible and comparable.

5. **Let DuckDB do the exact predicate.** The bounding-box filter admits false positives by design; the geometric test removes them. Skipping it returns rows whose boxes overlap and whose geometries do not.

## Common errors and fixes

| Symptom | Cause | Fix |
|---|---|---|
| Every file returned by the planner | Predicate uses geometry, not the numeric columns | Filter on the bbox columns in the row filter |
| Deleted rows appear in results | Delete files ignored | Detect and refuse, or apply them explicitly |
| Results differ between runs | A concurrent commit changed the table | Pin the snapshot and reuse it for the session |
| `IO Error 403` | Credentials configured in one tool but not the other | Both PyIceberg and DuckDB need object-storage access |
| Slow despite few files | Projection not pushed; whole rows read | Pass `selected_fields` and select the same columns in SQL |

## Verifying the pruning

<figure class="diagram">
<svg viewBox="0 0 762 222" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Comparing files planned against files in the table for a scoped window, and the rows returned against the rows read, as two independent measures of whether the split is working">
<rect x="0" y="0" width="762" height="222" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Two ratios tell you whether each half is working</text>
<rect x="30" y="58" width="352" height="152" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="206" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">planning ratio</text>
<text x="206" y="118" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">files planned ÷ files in table</text>
<text x="206" y="146" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">under 2% for a scoped window</text>
<text x="206" y="174" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">tests PyIceberg&#8217;s half</text>
<rect x="398" y="58" width="352" height="152" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="574" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">refinement ratio</text>
<text x="574" y="118" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">rows returned ÷ rows read</text>
<text x="574" y="146" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">above 20% on a healthy layout</text>
<text x="574" y="174" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">tests the sort order</text>
</svg>
</figure>

```python
def measure(table, scan, returned_rows: int) -> dict:
    total = len(list(table.scan().plan_files()))
    planned = list(scan.plan_files())
    rows_read = sum(t.file.record_count for t in planned)
    return {
        "planning_ratio": len(planned) / total,
        "refinement_ratio": returned_rows / rows_read if rows_read else 0.0,
    }
```

A poor planning ratio means the table's statistics are not helping — usually the bounding-box columns are outside the statistics window. A poor refinement ratio means the files are unsorted, so each one spans a wide area and most of its rows are irrelevant. The two failures have different remedies and only the pair of measurements distinguishes them.

## When to reach for this pattern

<figure class="diagram">
<svg viewBox="0 0 762 222" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Situations that suit the plan and execute split: scheduled extracts, validation jobs and embedded serving, contrasted with cases better served by a full engine">
<rect x="0" y="0" width="762" height="222" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Where the split earns its keep</text>
<rect x="30" y="58" width="352" height="152" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="206" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#2f6e49">a good fit</text>
<text x="206" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">scheduled extracts and reports</text>
<text x="206" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">CI validation over a sample</text>
<text x="206" y="164" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">embedded serving of small results</text>
<text x="206" y="188" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">notebooks and investigation</text>
<rect x="398" y="58" width="352" height="152" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="574" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#9a5a17">use a full engine instead</text>
<text x="574" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">many concurrent users</text>
<text x="574" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">merge-on-read tables with deletes</text>
<text x="574" y="164" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">working sets beyond node memory</text>
<text x="574" y="188" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">governed multi-tenant access</text>
</svg>
</figure>

The delete-file limitation is the sharpest boundary and worth restating: this pattern is safe on copy-on-write tables and unsafe on merge-on-read tables unless the deletes are applied. Detecting them and refusing, as the code above does, keeps the failure loud rather than silent.

Everything else on the right-hand list is a question of scale or governance rather than correctness, and the pattern degrades gracefully across all of them — it simply stops being the fastest option before it stops being a correct one.

## Handling Merge-on-Read Tables

Where the table does carry delete files, three options exist and they differ in effort rather than in correctness.

**Use a native Iceberg reader.** DuckDB's Iceberg extension, where available for the table's format version, handles snapshots and deletes itself and removes the whole question. This is the right answer when it applies, and the plan-and-execute split becomes unnecessary.

**Apply the deletes manually.** Position deletes name a data file and a set of row positions, so they can be read and applied as an anti-join on row number. It is a dozen lines and it is exact. Equality deletes are harder — they apply by predicate rather than by position — and applying them correctly means evaluating each delete predicate against the data, which is close to reimplementing the reader.

**Restrict the pattern to copy-on-write tables.** The simplest option, and often the right one: use this pattern for the analytical tables that are written in bulk and read many times, and use a full engine for the tables that receive scattered updates. Most platforms have both kinds, and the division is natural rather than arbitrary.

Whichever is chosen, the detection must stay in place. A table that starts as copy-on-write and later has merge-on-read enabled — a common change when update patterns shift — would otherwise silently begin returning deleted rows, and the assertion is the only thing standing between that change and a wrong answer.

## Reusing the Plan

For a session that issues several queries against the same window, planning once and reusing the file list saves a catalog round trip per query and, more importantly, makes the results mutually consistent.

```python
class PinnedScan:
    """Plan once, query many times, all against the same snapshot."""
    def __init__(self, catalog, identifier, row_filter, fields):
        table = catalog.load_table(identifier)
        self.snapshot_id = table.current_snapshot().snapshot_id
        scan = table.scan(row_filter=row_filter, selected_fields=fields,
                          snapshot_id=self.snapshot_id)
        tasks = list(scan.plan_files())
        assert not any(t.delete_files for t in tasks), "merge-on-read table"
        self.files = [t.file.file_path for t in tasks]
        self.con = duckdb.connect()
        self.con.execute("INSTALL spatial; LOAD spatial;")

    def query(self, sql: str, **params):
        return self.con.execute(sql, {"files": self.files, **params}).fetchall()
```

Pinning the snapshot explicitly is what makes several queries in one session comparable. Without it, a compaction between two queries changes the file list and the two results reflect different states — individually correct and jointly confusing, which is a difficult class of bug to diagnose because both queries look right.

The cost of pinning is that the session's view of the table ages. For an interactive investigation lasting minutes that is exactly what is wanted; for a long-running service it needs a refresh policy, and re-planning on a timer is the simplest one that works.

## Performance Expectations

Some concrete numbers help calibrate whether this pattern suits a given workload, and they are easy to reproduce on any table with the layout described here.

Planning a scan over a table with tens of thousands of files takes on the order of a second, dominated by reading manifests from object storage. It is independent of the data volume and depends only on the file count, which is another reason to keep the file count under control through compaction.

Execution scales with the bytes DuckDB reads, which the planning has already minimised. A window selecting two hundred files of 128 MB each, projecting three columns out of twenty, transfers a few gigabytes and aggregates in seconds on a modest instance. The geometry decode, which is the expensive per-row work, applies only to rows that survive the numeric filter.

The point at which this stops being the right tool is when the surviving working set no longer fits in memory — a join whose build side is large, an aggregation with hundreds of millions of groups, a sort over the whole result. Those are the cases the section overview routes to a cluster, and the boundary is the node rather than anything about the pattern.

For everything below that boundary, the combination is frequently faster than a cluster because it has no scheduling, no shuffle and no start-up cost. A query that a Spark job answers in four minutes — of which three are session start-up — this pattern answers in twenty seconds, and the difference is entirely in what was not done.
The saving is in the work that is never scheduled rather than in the work that runs faster.
