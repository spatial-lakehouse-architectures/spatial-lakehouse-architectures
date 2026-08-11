# Generating Vector Tiles From Lakehouse Tables

This guide builds a vector-tile pipeline on top of an ordinary spatial lakehouse table, reusing the same derived columns and partitioning the rest of the platform relies on, and precomputing only the tiles that are actually requested.

## Context and prerequisites

A tile is an aggregate over a fixed grid cell, which makes tile generation structurally identical to the summaries covered in [spatial aggregation and tiling](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/spatial-aggregation-and-tiling/) rather than a separate discipline. This recipe uses PySpark 3.5 with Sedona for the geometric work and writes tiles to object storage; the simplification it applies per zoom level is covered in [simplifying geometries for analytical layers](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/geometry-validation-and-repair/simplifying-geometries-for-analytical-layers/).

## The pipeline

<figure class="diagram">
<svg viewBox="0 0 774 246" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Five stages of tile generation: select features by tile extent, clip to the tile with a buffer, simplify for the zoom level, encode the payload, and write keyed by zoom x and y">
<defs>
<marker id="gvt-pipe-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#0e6e7d"/></marker>
</defs>
<rect x="0" y="0" width="774" height="246" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Five stages, one of which touches the lakehouse</text>
<rect x="18" y="66" width="138" height="76" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="87" y="94" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">1. select</text>
<text x="87" y="118" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">bbox prune by tile</text>
<rect x="174" y="66" width="138" height="76" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="243" y="94" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">2. clip</text>
<text x="243" y="118" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">tile + small buffer</text>
<rect x="330" y="66" width="138" height="76" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="399" y="94" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">3. simplify</text>
<text x="399" y="118" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">tolerance per zoom</text>
<rect x="486" y="66" width="138" height="76" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="555" y="94" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">4. encode</text>
<text x="555" y="118" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">tile format payload</text>
<rect x="642" y="66" width="120" height="76" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="702" y="94" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">5. write</text>
<text x="702" y="118" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">z/x/y key</text>
<line x1="156" y1="104" x2="174" y2="104" stroke="#0e6e7d" stroke-width="2" marker-end="url(#gvt-pipe-arrow)"/>
<line x1="312" y1="104" x2="330" y2="104" stroke="#0e6e7d" stroke-width="2" marker-end="url(#gvt-pipe-arrow)"/>
<line x1="468" y1="104" x2="486" y2="104" stroke="#0e6e7d" stroke-width="2" marker-end="url(#gvt-pipe-arrow)"/>
<line x1="624" y1="104" x2="642" y2="104" stroke="#0e6e7d" stroke-width="2" marker-end="url(#gvt-pipe-arrow)"/>
<text x="390" y="200" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">Stage 1 is the only one that reads the table; stages 2–4 are pure geometry</text>
<text x="390" y="230" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">So the layout work pays here exactly as it does for any other query</text>
</svg>
</figure>

## Complete working solution

```python
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

# Tolerance per zoom: roughly one screen pixel at that zoom, in degrees.
TOLERANCE = {z: 360.0 / (256 * 2 ** z) for z in range(0, 16)}
BUFFER_FRACTION = 0.05          # clip slightly beyond the tile to avoid seam artefacts

def tile_bounds(z: int, x: int, y: int) -> tuple[float, float, float, float]:
    import math
    n = 2 ** z
    lon1 = x / n * 360.0 - 180.0
    lon2 = (x + 1) / n * 360.0 - 180.0
    lat1 = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n))))
    lat2 = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n))))
    return (lon1, min(lat1, lat2), lon2, max(lat1, lat2))

def build_tiles(spark, table: str, z: int, tiles: list[tuple[int, int]]):
    tol = TOLERANCE[z]
    for x, y in tiles:
        minx, miny, maxx, maxy = tile_bounds(z, x, y)
        bx = (maxx - minx) * BUFFER_FRACTION
        by = (maxy - miny) * BUFFER_FRACTION

        features = (spark.table(table)
            .where(
                (F.col("bbox_max_x") >= minx - bx) & (F.col("bbox_min_x") <= maxx + bx) &
                (F.col("bbox_max_y") >= miny - by) & (F.col("bbox_min_y") <= maxy + by))
            .selectExpr(
                "feature_id", "category",
                f"ST_Intersection("
                f"  ST_SimplifyPreserveTopology(ST_GeomFromWKB(geom_wkb), {tol}),"
                f"  ST_MakeEnvelope({minx-bx}, {miny-by}, {maxx+bx}, {maxy+by})"
                f") AS geom")
            .where("NOT ST_IsEmpty(geom)"))

        payload = encode_tile(features.collect(), z, x, y)
        write_object(f"tiles/{z}/{x}/{y}.mvt", payload)
```

## Step-by-step walkthrough

1. **Filter on the bounding-box columns.** This is the stage that determines whether tile generation is affordable, and it is the same numeric predicate every other query on the platform uses. A tile pipeline that filters with `ST_Intersects` against the tile envelope reads the whole table per tile.

2. **Simplify before clipping, not after.** Simplifying the clipped fragment produces artefacts at the tile edge, because the clip introduces vertices on the boundary that the simplifier then moves. Simplifying first and clipping second keeps edges aligned between adjacent tiles.

3. **Clip with a small buffer.** Features clipped exactly at the tile boundary produce visible seams when rendered, because line widths and symbols extend beyond the geometry. A five-percent buffer is a common default and the renderer discards the overflow.

4. **Drop empty results.** A feature whose simplified geometry no longer intersects the tile contributes nothing and should not be encoded. At low zooms this removes a large fraction of candidates.

5. **Key the output by zoom, column and row.** The conventional path layout means any tile server or static host can serve the result without an index.

## Common errors and fixes

| Symptom | Cause | Fix |
|---|---|---|
| Generation is extremely slow | Filtering with a geometry predicate instead of bbox columns | Use the numeric covering; it is the whole optimisation |
| Visible seams between tiles | Clipped exactly at the boundary | Clip with a buffer and let the renderer trim |
| Edges misaligned between adjacent tiles | Simplified after clipping | Simplify first, then clip |
| Small features vanish at low zoom | Tolerance exceeds the feature size | Expected; filter by size and drop them explicitly for clarity |
| Tile payloads enormous at low zoom | No simplification, or too many features | Increase tolerance with zoom; consider a feature-count cap per tile |

## Precomputing only what is browsed

<figure class="diagram">
<svg viewBox="146 0 589 258" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="The theoretical tile pyramid grows fourfold per zoom level while actual requests concentrate on a small fraction, so precomputation should follow the request log rather than the pyramid">
<rect x="146" y="0" width="589" height="258" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">The pyramid is enormous; the browsed portion is not</text>
<rect x="330" y="56" width="120" height="26" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="1.5"/>
<text x="470" y="74" font-family="sans-serif" font-size="11" fill="#33707d">z6 — 4 096 tiles</text>
<rect x="290" y="86" width="200" height="26" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="1.5"/>
<text x="510" y="104" font-family="sans-serif" font-size="11" fill="#33707d">z8 — 65 536</text>
<rect x="230" y="116" width="320" height="26" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="1.5"/>
<text x="570" y="134" font-family="sans-serif" font-size="11" fill="#33707d">z10 — 1 048 576</text>
<rect x="150" y="146" width="480" height="26" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="1.5"/>
<text x="650" y="164" font-family="sans-serif" font-size="11" fill="#33707d">z12 — 16.7 M</text>
<path d="M330 178 h14 v10 h-14 z" fill="#2f6e49"/>
<line x1="337" y1="56" x2="337" y2="178" stroke="#2f6e49" stroke-width="2" stroke-dasharray="4 4"/>
<text x="390" y="212" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#2f6e49">the shaded sliver is what request logs show is actually fetched</text>
<text x="390" y="242" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">Precompute that; generate the rest on demand and cache it</text>
</svg>
</figure>

Precomputing a full pyramid to zoom 14 is a common early decision and is almost always wasted: the overwhelming majority of tiles cover ocean, empty land or areas nobody looks at. Request logs from even a few days of real use identify the hot set, and it is typically a few percent of the theoretical space.

The workable arrangement is to precompute the hot set on the summary refresh schedule, generate anything else on first request, and cache the result. A cold tile costs a second or two to generate — acceptable for an unusual view — and becomes warm for everyone afterwards.

## Verification

```python
def verify_tile(payload, z, x, y, source_count_in_bounds):
    tile = decode_tile(payload)
    assert tile.layers, f"tile {z}/{x}/{y} has no layers"
    # Every geometry must lie within the tile plus its buffer.
    minx, miny, maxx, maxy = tile_bounds(z, x, y)
    for feature in tile.features:
        fminx, fminy, fmaxx, fmaxy = feature.bounds
        assert fminx >= minx - 1e-6 and fmaxx <= maxx + 1e-6, "feature escapes the tile"
    # Feature count must not exceed the source count in the same window.
    assert len(tile.features) <= source_count_in_bounds
```

The second assertion catches a duplication bug that is easy to introduce when a feature spans several tiles and the pipeline replicates rather than clips. It is worth having because the visual result of that bug is invisible — the map looks correct and the payloads are several times larger than they should be.

Sample a handful of tiles at each zoom rather than checking all of them, and include one tile at a boundary and one at the antimeridian, which are the two places tile pipelines most often go wrong.

## Controlling Payload Size

Tile size is the metric that determines whether a map feels responsive, and three techniques keep it bounded without degrading the view.

<figure class="diagram">
<svg viewBox="0 0 764 222" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three payload reduction techniques for vector tiles: simplification tolerance rising with zoom out, dropping features below a size threshold, and limiting attributes carried per feature">
<rect x="0" y="0" width="764" height="222" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Three levers, applied per zoom level</text>
<rect x="26" y="58" width="230" height="152" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="141" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">tolerance by zoom</text>
<text x="141" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">one pixel at that zoom</text>
<text x="141" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">removes detail nobody</text>
<text x="141" y="166" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">can see anyway</text>
<text x="141" y="192" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">the largest single saving</text>
<rect x="274" y="58" width="230" height="152" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="389" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">size threshold</text>
<text x="389" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">drop sub-pixel features</text>
<text x="389" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">a building at z6 is</text>
<text x="389" y="166" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">smaller than a pixel</text>
<text x="389" y="192" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">be explicit about it</text>
<rect x="522" y="58" width="230" height="152" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="637" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">attribute budget</text>
<text x="637" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">carry what the style uses</text>
<text x="637" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">an identifier and a category,</text>
<text x="637" y="166" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">not forty columns</text>
<text x="637" y="192" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">frequently half the payload</text>
</svg>
</figure>

The attribute budget is the most commonly overlooked. A tile carrying every column of the source table ships attributes that the map style never references, and on feature-dense tiles the attribute block can exceed the geometry block. Selecting the two or three fields the style actually uses, and fetching the rest on click from an ordinary lookup, typically halves the payload.

Dropping sub-pixel features should be explicit rather than left to the simplifier. A polygon smaller than a pixel at the current zoom contributes nothing visible, and filtering on a size threshold removes it deterministically — whereas relying on simplification to collapse it produces inconsistent results and occasional invalid geometry.

Set a payload budget per tile and measure against it. A few hundred kilobytes is a common ceiling; tiles consistently exceeding it at a given zoom mean the tolerance or the threshold for that zoom needs adjusting, and the measurement makes that a tuning exercise rather than a guess.

## Refreshing Tiles Incrementally

A full pyramid rebuild after every data change is untenable, and the incremental path is straightforward because the affected tiles are computable.

When a batch of features changes, compute the union of their bounding boxes, expand it into tile coordinates at each zoom level, and regenerate only those tiles. For a localised update — a district's boundaries revised, a day of new observations in one city — this is a few hundred tiles rather than millions.

Two details make it correct. The **buffer must be accounted for**: a feature near a tile edge affects the neighbouring tile too, so expand the affected set by one tile in each direction. And **deletions must be handled**: a feature removed from the source still appears in the cached tile until that tile is regenerated, so deletions must trigger regeneration exactly as insertions do — which requires knowing where the deleted feature was, and therefore recording its extent before removing it.

Track the tile cache's own staleness alongside the summary staleness described in the parent topic. A tile that was generated from a snapshot three days old is serving three-day-old geometry, and a client has no way to know unless the pipeline says so.

## Where the Lakehouse Helps

Building tiles from a lakehouse table rather than from a dedicated spatial database has three specific advantages worth naming, because they are the reason to do it this way rather than to maintain a separate serving database.

**One copy of the data.** The tiles are derived from the same table the analytics query, so a boundary revision reaches the map and the report at the same moment and from the same source. Platforms that maintain a separate rendering database inevitably see the two diverge, and reconciling them is a recurring cost.

**The same layout work pays twice.** The bounding-box columns, the partitioning and the sort order that make analytical queries fast make tile generation fast, for exactly the same reason: both are selecting a small spatial window from a large table. No tile-specific indexing is needed.

**Snapshot-consistent tiles.** Generating a pyramid from a pinned snapshot means every tile in it reflects the same state, which a live database cannot guarantee during a bulk update. For a map whose tiles are compared against each other — a before-and-after, an animation across time — that consistency is the difference between a coherent result and a flickering one.

The cost is latency on the generation path, which is why the precomputation and caching described above matter. A lakehouse will not serve a cold tile in ten milliseconds; it will serve a warm one from object storage in single-digit milliseconds, which is the same thing from the client's perspective and requires no additional database to operate.
