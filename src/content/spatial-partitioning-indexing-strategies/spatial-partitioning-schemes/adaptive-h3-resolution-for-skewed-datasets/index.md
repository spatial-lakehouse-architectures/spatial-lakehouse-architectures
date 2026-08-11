# Adaptive H3 Resolution for Skewed Datasets

This guide builds a versioned depth mapping that assigns each region of a dataset the grid resolution at which its partitions land inside the target size band, so a globally uniform resolution stops producing partitions that differ by three orders of magnitude.

## Context and prerequisites

A single H3 resolution applied worldwide produces partitions whose sizes follow population density: a cell over a metropolitan area holds millions of rows while a cell over ocean holds none. Adaptive resolution assigns depth per region instead. This recipe uses PySpark 3.5 with the H3 bindings; the sizing arithmetic is in [spatial partitioning schemes](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/spatial-partitioning-schemes/), and the join-side consequences in [handling skew in large spatial joins](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/spatial-join-optimization/handling-skew-in-large-spatial-joins/).

## What the mapping does

<figure class="diagram">
<svg viewBox="0 0 722 274" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="A coarse base grid where dense cells are subdivided one or two levels deeper while sparse cells remain coarse, producing partitions of comparable size">
<rect x="0" y="0" width="722" height="274" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Same target size, different depth per region</text>
<rect x="70" y="66" width="120" height="90" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="130" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">r4 — ocean</text>
<rect x="190" y="66" width="120" height="90" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="250" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">r4 — rural</text>
<rect x="310" y="66" width="60" height="45" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="1.5"/>
<rect x="370" y="66" width="60" height="45" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="1.5"/>
<rect x="310" y="111" width="60" height="45" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="1.5"/>
<rect x="370" y="111" width="60" height="45" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="1.5"/>
<text x="370" y="176" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">r5 — suburban</text>
<rect x="430" y="66" width="30" height="22" fill="#f2e8da" stroke="#9a5a17" stroke-width="1"/>
<rect x="460" y="66" width="30" height="22" fill="#f2e8da" stroke="#9a5a17" stroke-width="1"/>
<rect x="430" y="88" width="30" height="23" fill="#f2e8da" stroke="#9a5a17" stroke-width="1"/>
<rect x="460" y="88" width="30" height="23" fill="#f2e8da" stroke="#9a5a17" stroke-width="1"/>
<rect x="430" y="111" width="30" height="22" fill="#f2e8da" stroke="#9a5a17" stroke-width="1"/>
<rect x="460" y="111" width="30" height="22" fill="#f2e8da" stroke="#9a5a17" stroke-width="1"/>
<rect x="430" y="133" width="30" height="23" fill="#f2e8da" stroke="#9a5a17" stroke-width="1"/>
<rect x="460" y="133" width="30" height="23" fill="#f2e8da" stroke="#9a5a17" stroke-width="1"/>
<text x="460" y="176" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">r6 — urban</text>
<rect x="530" y="66" width="180" height="90" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="620" y="96" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">result</text>
<text x="620" y="120" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">every partition</text>
<text x="620" y="138" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">128 MB – 1 GB</text>
<text x="390" y="230" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">Depth is chosen per base cell from a measured row count</text>
<text x="390" y="258" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">The mapping is small: one row per non-empty base cell</text>
</svg>
</figure>

The mapping is a table of a few thousand rows at most, because it is keyed on a coarse base resolution. That size matters: it can be broadcast to every writer and every reader without any concern, and it can be inspected and diffed by a human when a decision looks wrong.

## Complete working solution

```python
import h3
from pyspark.sql import functions as F, types as T

BASE_RES     = 4          # the resolution the mapping is keyed on
MAX_RES      = 9          # never subdivide beyond this
TARGET_ROWS  = 4_000_000  # rows per partition at the target file size
MAPPING_VER  = 3          # bump whenever the mapping changes

def build_depth_mapping(spark, table: str, sample: float = 0.01):
    """One row per base cell: the depth at which its partitions hit the target."""
    counts = (spark.table(table).sample(sample)
        .selectExpr(f"h3_point_to_cell(bbox_min_x, bbox_min_y, {BASE_RES}) AS base")
        .groupBy("base").count()
        .withColumn("est_rows", F.col("count") / F.lit(sample)))

    @F.udf(T.IntegerType())
    def choose_depth(est_rows):
        depth = BASE_RES
        rows = float(est_rows)
        while rows > TARGET_ROWS and depth < MAX_RES:
            depth += 1
            rows /= 7.0          # each level divides a cell roughly sevenfold
        return depth

    return (counts.withColumn("depth", choose_depth("est_rows"))
                  .withColumn("mapping_version", F.lit(MAPPING_VER))
                  .select("base", "depth", "est_rows", "mapping_version"))

def apply_mapping(spark, df, mapping):
    """Derive the partition column at the depth this row's base cell was assigned."""
    with_base = df.selectExpr("*",
        f"h3_point_to_cell(bbox_min_x, bbox_min_y, {BASE_RES}) AS base")
    joined = with_base.join(F.broadcast(mapping.select("base", "depth",
                                                       "mapping_version")),
                            "base", "left")
    return (joined
        .withColumn("depth", F.coalesce("depth", F.lit(BASE_RES)))
        .withColumn("mapping_version",
                    F.coalesce("mapping_version", F.lit(MAPPING_VER)))
        .selectExpr("*", "h3_point_to_cell(bbox_min_x, bbox_min_y, depth) AS h3_cell")
        .drop("base"))
```

## Step-by-step walkthrough

1. **Key the mapping on a coarse base resolution.** Keying it on the final resolution would make it as large as the partition count, which defeats the purpose. A base two or three levels above the finest depth keeps the mapping broadcastable.

2. **Estimate from a sample, not a full count.** The mapping needs the right order of magnitude per cell, not an exact figure, and a one-percent sample gives that in a fraction of the time.

3. **Divide by seven per level.** Each H3 resolution step subdivides a cell into approximately seven children, so a cell with fifty million rows reaches the target in two levels. The approximation is good enough because the target is a band rather than a point.

4. **Cap the maximum depth.** Without a cap, an extremely dense cell subdivides until the partition count explodes. A cap of five levels below the base is generous; hitting it is a signal that the base resolution is wrong rather than that more depth is needed.

5. **Record the mapping version on every row.** This is what makes a mapping change survivable: rows written under version 3 stay findable, and a query can restrict itself to one version when the layouts must not be mixed.

## Common errors and fixes

| Symptom | Cause | Fix |
|---|---|---|
| Partition count exploded | No depth cap, or base resolution too fine | Cap the depth; raise the base resolution |
| New data lands in the wrong depth | Mapping not rebuilt after a new source onboarded | Rebuild quarterly and on significant ingest changes |
| Queries miss rows near cell borders | Reader expands the window at a single depth | Expand at every depth present, using the parent relation |
| Mapping broadcast is large | Keyed on too fine a base resolution | Coarsen the base; the mapping should be thousands of rows, not millions |
| Historical data reshuffled unexpectedly | Mapping changed without a version bump | Version every change; re-derive only in a planned rewrite |

## Reading a mixed-depth table

<figure class="diagram">
<svg viewBox="0 0 762 246" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Expanding a query window against a mixed depth table: the window is converted to cells at every depth present and the union of the cell lists is used as the partition predicate">
<defs>
<marker id="ahr-read-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#2f6e49"/></marker>
</defs>
<rect x="0" y="0" width="762" height="246" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">The reader expands at every depth in the table</text>
<rect x="26" y="70" width="180" height="76" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="116" y="100" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">query window</text>
<text x="116" y="124" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">a bounding box</text>
<rect x="256" y="46" width="180" height="56" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="346" y="80" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">cells at r4</text>
<rect x="256" y="112" width="180" height="56" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="346" y="146" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">cells at r5</text>
<rect x="256" y="178" width="180" height="56" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="346" y="212" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">cells at r6</text>
<rect x="486" y="112" width="264" height="56" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="618" y="138" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">h3_cell IN (union of all three)</text>
<text x="618" y="158" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">one predicate, prunes correctly</text>
<line x1="206" y1="96" x2="256" y2="74" stroke="#2f6e49" stroke-width="2" marker-end="url(#ahr-read-arrow)"/>
<line x1="206" y1="108" x2="256" y2="140" stroke="#2f6e49" stroke-width="2" marker-end="url(#ahr-read-arrow)"/>
<line x1="206" y1="120" x2="256" y2="206" stroke="#2f6e49" stroke-width="2" marker-end="url(#ahr-read-arrow)"/>
<line x1="436" y1="140" x2="486" y2="140" stroke="#2f6e49" stroke-width="2" marker-end="url(#ahr-read-arrow)"/>
</svg>
</figure>

The union is larger than a single-depth expansion, which is the cost of the scheme: a query touching a mixed-depth region names more cells. In practice the increase is small — the depths present in any one area are one or two — and it is far outweighed by the elimination of the straggler.

Build this expansion into a helper view or function rather than leaving it to callers, and read the set of depths present from the mapping table rather than hard-coding it. A caller who expands at one depth against a mixed-depth table gets a silently partial result, which is the worst available failure mode.

## Verification

```python
def verify_mapping(spark, table: str, target: int = 4_000_000, tol: float = 4.0):
    counts = (spark.table(table).groupBy("h3_cell").count()
                   .selectExpr("percentile_approx(count, 0.5) med", "max(count) mx"))
    row = counts.collect()[0]
    ratio = row["mx"] / row["med"]
    assert ratio < tol, f"skew persists after adaptive resolution: {ratio:.1f}×"
    assert row["med"] < target * 2, "median partition above target — base too coarse"
```

Run it after the first write under a new mapping version, and again quarterly. A ratio that has crept back above the tolerance means density has shifted since the mapping was built, which is the expected outcome over a year and the signal to rebuild — a scheduled change rather than an incident.

## Versioning the Mapping Properly

The mapping is the piece of state that makes this scheme work and the piece most likely to be mishandled. Treating it as an ordinary configuration file is what turns a well-designed layout into an unexplainable one.

<figure class="diagram">
<svg viewBox="0 0 768 248" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Lifecycle of a versioned depth mapping: version one written to history, version two applied to new writes only, and a planned rewrite bringing history forward, with rows always carrying their version">
<defs>
<marker id="ahr-ver-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#0e6e7d"/></marker>
</defs>
<rect x="0" y="0" width="768" height="248" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">A mapping change never silently reshuffles history</text>
<rect x="24" y="70" width="164" height="80" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="106" y="100" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">v1 in use</text>
<text x="106" y="124" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">history written under it</text>
<rect x="212" y="70" width="164" height="80" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="294" y="100" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">v2 published</text>
<text x="294" y="124" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">new writes only</text>
<rect x="400" y="70" width="164" height="80" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="482" y="100" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">mixed table</text>
<text x="482" y="124" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">both versions present</text>
<rect x="588" y="70" width="168" height="80" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="672" y="100" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">planned rewrite</text>
<text x="672" y="124" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">history brought forward</text>
<line x1="188" y1="110" x2="212" y2="110" stroke="#0e6e7d" stroke-width="2" marker-end="url(#ahr-ver-arrow)"/>
<line x1="376" y1="110" x2="400" y2="110" stroke="#0e6e7d" stroke-width="2" marker-end="url(#ahr-ver-arrow)"/>
<line x1="564" y1="110" x2="588" y2="110" stroke="#0e6e7d" stroke-width="2" marker-end="url(#ahr-ver-arrow)"/>
<text x="390" y="204" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">Every row carries its mapping version; queries can restrict to one if they must</text>
<text x="390" y="232" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">The rewrite is optional and schedulable, never forced by the change</text>
</svg>
</figure>

Store the mapping as a table rather than a file, with the version as part of the key, so every historical version remains readable. That makes the third box tractable: a reader encountering rows under two versions can resolve both, because both mappings still exist.

Publish a new version by writing new rows rather than by updating existing ones, and have the write path read the current version from a pointer. That makes activation atomic, makes rollback a pointer change, and leaves an audit trail of which mapping was in force when — which is the question that arises when a partition size looks wrong in retrospect.

The rewrite that brings history forward is optional, and treating it as optional is the point. A mixed-version table works; it is simply slightly less well pruned in the regions where the versions differ. Scheduling the rewrite for a quiet period, partition by partition, converts what would otherwise be a blocking migration into ordinary background maintenance.

## When Uniform Resolution Is Still Right

Adaptive resolution is machinery, and machinery has a maintenance cost. Three situations do not justify it.

**A geographically bounded dataset.** Data covering one metropolitan area, one country of modest extent, or one utility network has a density range narrow enough that a single well-chosen resolution produces acceptable partitions. Measure the skew ratio at a uniform resolution before assuming otherwise — under four, there is nothing to fix.

**A dataset whose density is stable and known.** Where the distribution has been the same for years and will remain so, the simpler answer is a hand-maintained list of exceptions rather than a derived mapping: three cities get one level deeper, everything else stays uniform. It is the same idea with a tenth of the machinery, and it is honest about the fact that somebody is making the decision.

**Small tables.** Below a few hundred gigabytes the partition sizes are small enough that skew costs little in absolute terms, and the operational overhead of a versioned mapping outweighs the benefit. Revisit when the table grows.

The general principle is that adaptive resolution earns its complexity when the density range across the dataset spans more than about two orders of magnitude and the table is large enough that a straggler costs real time. Continental and global datasets almost always qualify; regional ones frequently do not, and adopting it there produces a system that is harder to reason about for no measurable gain.

Whichever route is taken, record the decision and the measured skew ratio in the table properties. The next person to look at partition sizes will want to know whether the current arrangement was chosen or inherited, and a one-line answer saves them re-deriving the whole analysis.
It also documents the intent, which a partition specification on its own never does.
A partition spec says what the layout is; the recorded reasoning says why, and only the second survives a change of team.
Two sentences in the table properties are worth more than a design document nobody can find.
