# Simplifying Geometries for Analytical Layers

This guide produces a simplified copy of a polygon layer at a tolerance matched to how it will be queried, verifies that the simplification preserves topology and containment decisions, and shows when the reduced copy is worth its storage.

## Context and prerequisites

A boundary digitised for cartography at 1:1,000 carries far more vertices than a point-in-polygon join at city scale can use, and every one of those vertices is decoded, transferred and compared on every query. Simplification trades a controlled amount of positional accuracy for a large reduction in cost. This recipe runs on Shapely 2.0+ with GEOS 3.10+; the storage-layout consequences are covered in [spatial partitioning and indexing strategies](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/), and the join-side benefit in [broadcast spatial joins with Apache Sedona](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/sedona-distributed-spatial-compute/broadcast-spatial-joins-with-apache-sedona/).

## Choosing the tolerance from the query, not the geometry

<figure class="diagram">
<svg viewBox="0 0 764 266" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Tolerance selection driven by the resolution at which results are consumed: metre level for parcel assignment, decametre for city dashboards, and hundred metre for national summaries, with the corresponding vertex reduction">
<rect x="0" y="0" width="764" height="266" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Tolerance follows the consumer, not the source</text>
<rect x="26" y="58" width="230" height="196" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="141" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">1 m tolerance</text>
<text x="141" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">parcel and address work</text>
<text x="141" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">vertices: −40% to −60%</text>
<text x="141" y="168" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">assignments unchanged</text>
<text x="141" y="196" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">except within 1 m of an edge</text>
<text x="141" y="228" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">safe default for joins</text>
<rect x="274" y="58" width="230" height="196" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="389" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">10 m tolerance</text>
<text x="389" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">city-scale dashboards</text>
<text x="389" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">vertices: −85% to −95%</text>
<text x="389" y="168" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">visually identical at zoom 12</text>
<text x="389" y="196" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">counts shift by a fraction of a percent</text>
<text x="389" y="228" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">the usual analytical copy</text>
<rect x="522" y="58" width="230" height="196" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="637" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">100 m tolerance</text>
<text x="637" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">national summaries, overviews</text>
<text x="637" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">vertices: −98% or more</text>
<text x="637" y="168" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">small features may vanish</text>
<text x="637" y="196" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">boundaries visibly straighten</text>
<text x="637" y="228" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">presentation only</text>
</svg>
</figure>

The reduction is steep because vertex density in cartographic data is dominated by detail far below the scale most analysis uses. Removing everything under ten metres from an administrative boundary typically discards nine vertices in ten while moving no edge further than ten metres — which changes no point assignment except for points that were within ten metres of a boundary, a population small enough to quantify and usually small enough to ignore.

The number that decides the tolerance is the **consumer's resolution**, not the source's. A dashboard that renders at 1,000 pixels across a 50-kilometre city has a pixel of 50 metres; storing boundaries with sub-metre detail for it is storing 2,500 times more geometry than the output can express.

## Complete working solution

```python
import pyarrow as pa
from shapely import (from_wkb, to_wkb, simplify, is_valid, make_valid,
                     area, get_num_coordinates, intersection, union_all)

TOLERANCE_DEG = 1e-4          # ~11 m at the equator
AREA_TOLERANCE = 0.02         # reject a simplification that moves area over 2%

def simplify_layer(batch: pa.RecordBatch) -> pa.RecordBatch:
    """Produce a reduced copy, preserving topology where it can be preserved."""
    out_geom, out_vertices, out_ratio, out_note = [], [], [], []

    for raw in batch.column("geometry").to_pylist():
        if raw is None:
            out_geom.append(None); out_vertices.append(0)
            out_ratio.append(None); out_note.append(None)
            continue

        original = from_wkb(raw)
        before_n = get_num_coordinates(original)
        before_a = area(original)

        # preserve_topology keeps the result valid and prevents rings collapsing.
        reduced = simplify(original, TOLERANCE_DEG, preserve_topology=True)
        if not is_valid(reduced):
            reduced = make_valid(reduced)

        after_a = area(reduced)
        drift = abs(after_a - before_a) / before_a if before_a > 0 else 0.0

        if drift > AREA_TOLERANCE or reduced.is_empty:
            # Keep the original rather than ship a misleading shape.
            out_geom.append(to_wkb(original)); out_vertices.append(before_n)
            out_ratio.append(1.0); out_note.append("kept_original")
        else:
            after_n = get_num_coordinates(reduced)
            out_geom.append(to_wkb(reduced)); out_vertices.append(after_n)
            out_ratio.append(after_n / before_n if before_n else 1.0)
            out_note.append(None)

    return pa.RecordBatch.from_arrays(
        [batch.column("feature_id"), pa.array(out_geom, pa.binary()),
         pa.array(out_vertices, pa.int32()), pa.array(out_ratio, pa.float64()),
         pa.array(out_note, pa.string())],
        names=["feature_id", "geometry", "vertex_count",
               "vertex_ratio", "simplify_note"])
```

## Step-by-step walkthrough

1. **Always preserve topology.** Without it, simplification can produce self-intersecting rings, collapse thin features to nothing, and turn a polygon into an invalid shape that then needs repair. The topology-preserving variant is slower and is the only defensible choice for stored data.

2. **Check area drift, not just validity.** A simplification can be valid and wrong — a peninsula removed, an island dropped. The area comparison catches the cases where the shape changed more than the tolerance implies, which happens when a feature's detail is concentrated rather than evenly distributed.

3. **Fall back to the original.** A feature that cannot be simplified within tolerance keeps its full geometry. That leaves a mixed layer, which is fine: the reduction is a storage and speed optimisation, not a contract, and a handful of full-detail features cost far less than a handful of wrong ones.

4. **Record the reduction ratio.** It is the metric that tells you whether the tolerance is doing anything. A layer whose median ratio is 0.95 is not being simplified meaningfully and the copy is not worth keeping; one whose median is 0.05 has been reduced twentyfold.

5. **Keep the identifier stable.** The simplified layer must join to the full one, so the feature identifier is carried through unchanged and the two are related rows rather than related tables with a fuzzy match.

## Common errors and fixes

| Symptom | Cause | Fix |
|---|---|---|
| Adjacent polygons develop gaps or overlaps | Each simplified independently | Simplify shared boundaries once, or accept gaps and document the tolerance |
| Small features disappear entirely | Tolerance exceeds the feature's size | Keep features below a size threshold at full detail |
| Result is invalid despite topology preservation | Input was already invalid | Validate before simplifying, not after |
| Area drift is high on coastlines | Detail is genuinely at the tolerance scale | Use a smaller tolerance for that layer, or accept and document |
| Simplified layer is barely smaller | Tolerance far below the source's vertex spacing | Raise it; measure the vertex ratio before committing |

## The topology problem, and when it matters

<figure class="diagram">
<svg viewBox="0 0 707 252" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Two adjacent polygons simplified independently develop a gap and an overlap along their shared boundary, whereas simplifying the shared boundary once keeps them coincident">
<rect x="0" y="0" width="707" height="252" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Independent simplification breaks shared boundaries</text>
<text x="196" y="62" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#9a5a17">simplified separately</text>
<path d="M70 90 L180 84 L186 200 L74 206 Z" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<path d="M196 92 L310 88 L306 204 L190 198 Z" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="196" y="236" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">a gap here, an overlap there</text>
<text x="584" y="62" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#2f6e49">shared boundary simplified once</text>
<path d="M460 90 L570 84 L574 200 L464 206 Z" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<path d="M574 200 L570 84 L690 88 L694 204 Z" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="584" y="236" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">the boundary stays coincident</text>
</svg>
</figure>

Whether this matters depends entirely on the use. For a point-in-polygon join, a gap of a few metres means a handful of points match nothing and a handful match twice, which is usually acceptable and always worth quantifying. For an area-summing report that must total to the parent, gaps produce a shortfall that a reviewer will notice.

Where it matters, the fix is to simplify the **shared linework** rather than the polygons: decompose the layer into its boundary segments, simplify each segment once, and rebuild the polygons from the simplified segments. It is more machinery and it is the only approach that guarantees the result still tiles. Where it does not matter, independent simplification is far simpler and the gaps are a documented property rather than a defect.

## Verification

<figure class="diagram">
<svg viewBox="0 0 764 210" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three acceptance tests for a simplified layer: vertex reduction achieved, area preserved within tolerance, and point assignment agreement measured against the full detail layer">
<rect x="0" y="0" width="764" height="210" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Three acceptance tests before publishing the copy</text>
<rect x="26" y="58" width="230" height="140" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="141" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">reduction achieved</text>
<text x="141" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">median vertex ratio</text>
<text x="141" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">below 0.2, or the copy</text>
<text x="141" y="166" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">is not worth keeping</text>
<rect x="274" y="58" width="230" height="140" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="389" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">area preserved</text>
<text x="389" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">per feature and in total</text>
<text x="389" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">within the declared</text>
<text x="389" y="166" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">tolerance, both ways</text>
<rect x="522" y="58" width="230" height="140" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="637" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">assignments agree</text>
<text x="637" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">sample points, both layers</text>
<text x="637" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">disagreement rate is the</text>
<text x="637" y="166" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">number consumers need</text>
</svg>
</figure>

The third test is the one that turns simplification from a guess into a decision. Take a large sample of representative points, assign each to a polygon using both layers, and report the disagreement rate. A rate of 0.02% is a fact a consumer can accept or reject; "we simplified to ten metres" is not, because nobody can convert it into an error bar on their own numbers.

```python
# Measure the assignment disagreement between full and simplified layers.
def assignment_agreement(points, full_index, simple_index):
    disagreements = 0
    for pt in points:
        a = full_index.query_nearest_containing(pt)
        b = simple_index.query_nearest_containing(pt)
        if a != b:
            disagreements += 1
    return disagreements / len(points)
```

Publish that rate alongside the layer, in its table comment, and consumers can decide for themselves whether the reduced copy suits their question. Without it, every consumer either assumes the copy is exact or refuses to use it, and both outcomes waste the work.

## Storing the Reduced Copy

Where the simplified layer lives affects how it is used and how it stays in step with its source.

**As a column on the same table** is the simplest arrangement: `geometry` and `geometry_simplified` side by side, written together, always consistent. Queries choose which to reference, joins pick the reduced one, and there is no synchronisation problem because there is only one write. The cost is that every read of the table carries both columns in the schema, and a reader selecting `*` transfers geometry twice.

**As a separate table** keeps the reduced layer small and independently partitioned, which matters when the intended use is broadcasting it to executors — a table containing nothing but identifiers and simplified geometry may be a few megabytes where the source is gigabytes. The cost is a synchronisation obligation: the copy must be rebuilt when the source changes, and a stale copy produces results that disagree with the source in ways nobody expects.

The synchronisation problem has a clean answer in a versioned table format. Record the source snapshot identifier as a property on the simplified table, rebuild from a pinned snapshot rather than from the live table, and expose the identifier so a consumer can tell which version they are reading. A staleness check then becomes a comparison of two numbers rather than a data diff.

For most platforms the practical arrangement is both: a simplified column on the source table for convenience, and a separate small broadcast table for the joins that need it, built from the same tolerance and rebuilt in the same job. The duplication is a few megabytes and it removes an entire category of "which layer did that number come from" questions.

## When Not to Simplify

Three cases where the reduced copy is not worth building, and building it anyway adds a maintenance obligation for nothing.

**Point layers.** A point has one coordinate pair and nothing to remove. Simplification of a point layer is a no-op that costs a pipeline stage.

**Layers that are already coarse.** A dataset of national boundaries at 1:10,000,000 has a vertex every few kilometres; simplifying it at a ten-metre tolerance removes nothing. Measure the median vertex ratio on a sample before building the pipeline — if it is above about 0.8, there is nothing to gain.

**Analyses whose answer depends on the detail.** Coastal erosion, floodplain edges, parcel boundary disputes and anything measured *at* the boundary rather than *inside* it need the full geometry by definition. For those, the reduced layer is not a faster path to the same answer; it is a different answer.

The unifying test is whether the question is about the interior or about the edge. Interior questions — which region does this point fall in, how many events occurred in this area — tolerate simplification well and benefit from it substantially. Edge questions do not tolerate it at all, and offering them a simplified layer without saying so is the way a simplification becomes an incident rather than an optimisation.
