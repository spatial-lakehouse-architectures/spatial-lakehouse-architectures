# Handling Antimeridian-Crossing Geometries

This guide shows how to detect geometries that cross ±180° longitude, split them so their bounding boxes stay compact, and keep queries correct on both sides — because a single uncorrected crossing feature can defeat data skipping for an entire table.

## Context and prerequisites

A bounding box is computed as the minimum and maximum of each coordinate. For a shipping route running from Tokyo to Los Angeles, the longitudes present are near +140 and near -120, so the box spans from -120 to +140 — covering almost the entire planet rather than the narrow corridor the route occupies. Every query anywhere on Earth then finds that box overlapping, so the file containing it is always read. This recipe runs on Shapely 2.0+ and applies to any table whose extent reaches the Pacific; the wider layout context is in [spatial partitioning and indexing strategies](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/), and the validation gate it plugs into is in [geometry validation and repair](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/geometry-validation-and-repair/).

## Why one feature poisons a whole file

<figure class="diagram">
<svg viewBox="0 0 732 282" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="A single antimeridian crossing route inside a file causes the file bounding box to span the entire longitude range, so every query overlaps it and the file is never skipped">
<rect x="0" y="0" width="732" height="282" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">One crossing feature, one useless bounding box</text>
<rect x="60" y="66" width="660" height="120" fill="#ffffff" stroke="#33707d" stroke-width="1.5"/>
<line x1="390" y1="66" x2="390" y2="186" stroke="#cfe3e7" stroke-width="1.5"/>
<text x="70" y="206" font-family="sans-serif" font-size="11" fill="#33707d">-180</text>
<text x="378" y="206" font-family="sans-serif" font-size="11" fill="#33707d">0</text>
<text x="694" y="206" font-family="sans-serif" font-size="11" fill="#33707d">+180</text>
<circle cx="120" cy="110" r="5" fill="#2f6e49"/><circle cx="140" cy="122" r="5" fill="#2f6e49"/>
<circle cx="660" cy="118" r="5" fill="#2f6e49"/><circle cx="680" cy="106" r="5" fill="#2f6e49"/>
<text x="130" y="152" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">west end</text>
<text x="670" y="152" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">east end</text>
<rect x="112" y="92" width="576" height="42" fill="#f2e8da" fill-opacity="0.55" stroke="#9a5a17" stroke-width="2.5"/>
<text x="400" y="120" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#9a5a17">computed bounding box: -175 … +178</text>
<text x="390" y="240" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">The route occupies two narrow strips; the box claims the whole world</text>
<text x="390" y="266" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">Every spatial query now reads this file, wherever on Earth it is scoped</text>
</svg>
</figure>

The damage is disproportionate because file-level statistics are aggregated: the file's box is the union of its rows' boxes, so one crossing feature widens the whole file. A table with a hundred such features spread across a hundred files has a hundred files that can never be skipped, and if those files are large, that is the majority of the scan cost for every query on the table.

The same effect appears one level up in partition statistics and one level down in row-group statistics, so the cost compounds rather than staying local.

## Complete working solution

```python
from shapely import from_wkb, to_wkb, box, intersection, get_type_id
from shapely.geometry import MultiPolygon
from shapely.ops import unary_union

WEST = box(-180.0, -90.0, 0.0, 90.0)
EAST = box(0.0, -90.0, 180.0, 90.0)

def crosses_antimeridian(geom, width_threshold=180.0) -> bool:
    """A geometry whose longitude span exceeds half the globe is crossing, not global."""
    minx, _, maxx, _ = geom.bounds
    return (maxx - minx) > width_threshold

def split_at_antimeridian(geom):
    """Return a list of (part_geometry, hemisphere) with compact bounding boxes.

    The input is assumed to use the convention that a crossing feature has some
    vertices near +180 and some near -180 (rather than longitudes beyond 180).
    """
    if not crosses_antimeridian(geom):
        return [(geom, "single")]

    # Shift the western vertices east by 360 so the feature becomes contiguous,
    # then clip the contiguous version back into two hemispheres.
    shifted = _shift_west_by_360(geom)
    east_part = intersection(shifted, box(0.0, -90.0, 180.0, 90.0))
    far_part  = intersection(shifted, box(180.0, -90.0, 360.0, 90.0))
    far_part  = _shift_by(far_part, -360.0)         # back into -180 … 0

    parts = []
    if not east_part.is_empty:
        parts.append((east_part, "east"))
    if not far_part.is_empty:
        parts.append((far_part, "west"))
    return parts

def _shift_by(geom, dx):
    from shapely.affinity import translate
    return translate(geom, xoff=dx)

def _shift_west_by_360(geom):
    """Move vertices with negative longitude into the 180 … 360 range."""
    from shapely import transform
    import numpy as np
    def _fn(coords):
        out = coords.copy()
        out[:, 0] = np.where(out[:, 0] < 0, out[:, 0] + 360.0, out[:, 0])
        return out
    return transform(geom, _fn)

def prepare_row(raw_wkb):
    """Yield one output row per hemisphere, each with a compact bbox."""
    geom = from_wkb(raw_wkb)
    for part, hemisphere in split_at_antimeridian(geom):
        minx, miny, maxx, maxy = part.bounds
        yield {
            "geometry": to_wkb(part),
            "hemisphere": hemisphere,
            "bbox_min_x": minx, "bbox_min_y": miny,
            "bbox_max_x": maxx, "bbox_max_y": maxy,
            "is_split_part": hemisphere != "single",
        }
```

Splitting produces more rows than the input, so the downstream contract has to say what that means: either the parts are treated as separate features with a shared identifier, or consumers reassemble them. Both work; leaving it undecided does not.

## Step-by-step walkthrough

1. **Detect by span, not by value.** Testing whether any vertex is near ±180 produces false positives for features that merely touch the line, and false negatives for features whose vertices happen to skip it. A longitude span exceeding half the globe is the reliable signal, and it correctly ignores genuinely global features such as a worldwide coverage polygon.

2. **Shift, then clip.** Moving the western vertices east by 360 makes the feature contiguous in a temporary coordinate space where ordinary clipping works. Attempting to clip in the original space cuts the feature along a line it does not cross in that representation, producing garbage.

3. **Shift the far part back.** The portion beyond +180 has to return to the -180…0 range so that stored coordinates remain valid geographic longitudes. Skipping this leaves values outside the declared range, which will fail the CRS assertions described in [detecting CRS drift in ingestion pipelines](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/crs-management-pipelines/detecting-crs-drift-in-ingestion-pipelines/).

4. **Emit a hemisphere marker.** It makes the split visible to consumers, allows a reassembly query to find the parts, and lets a validation job assert that split parts always come in pairs.

5. **Recompute the bounding box per part.** The whole point of the exercise is compact boxes; deriving them from the original geometry would defeat it entirely.

## Common errors and fixes

| Symptom | Cause | Fix |
|---|---|---|
| Every global-coverage polygon is split | Detection threshold too aggressive | Exempt features whose span exceeds 350°; those are genuinely global |
| Split parts do not meet at the line | Clipping applied before the shift | Shift first, clip in the contiguous space, shift the far part back |
| Coordinates above +180 in the table | Far part never shifted back | Add the reverse translate and assert coordinate ranges after the split |
| Row counts no longer match the source | Splitting adds rows, correctly | Compare distinct feature identifiers rather than row counts in reconciliation |
| Areas differ slightly after splitting | Clipping introduces vertices on the boundary | Expected; assert total area to a tolerance rather than exactly |

## Verification

<figure class="diagram">
<svg viewBox="0 0 732 264" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Before and after bounding boxes for a split antimeridian feature, showing one box spanning the globe replaced by two narrow boxes at each edge">
<rect x="0" y="0" width="732" height="264" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">One useless box becomes two useful ones</text>
<text x="390" y="60" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#9a5a17">before</text>
<rect x="60" y="72" width="660" height="34" fill="#ffffff" stroke="#33707d" stroke-width="1.5"/>
<rect x="70" y="72" width="640" height="34" fill="#f2e8da" fill-opacity="0.6" stroke="#9a5a17" stroke-width="2"/>
<text x="390" y="94" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">-175 … +178 — matches every query</text>
<text x="390" y="146" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#2f6e49">after</text>
<rect x="60" y="158" width="660" height="34" fill="#ffffff" stroke="#33707d" stroke-width="1.5"/>
<rect x="66" y="158" width="46" height="34" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<rect x="662" y="158" width="52" height="34" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="120" y="216" font-family="sans-serif" font-size="11" fill="#0d3b45">-180 … -168</text>
<text x="600" y="216" font-family="sans-serif" font-size="11" fill="#0d3b45">+166 … +180</text>
<text x="390" y="248" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">Queries away from the Pacific now skip the file entirely</text>
</svg>
</figure>

Assert the property rather than inspecting it. After the split, no row's bounding box may span more than a defined width, and the total area of the parts must match the original within a tolerance.

```python
def verify_split(original_wkb, rows, max_span=180.0, area_tol=1e-9):
    original = from_wkb(original_wkb)
    parts = [from_wkb(r["geometry"]) for r in rows]

    for r in rows:
        span = r["bbox_max_x"] - r["bbox_min_x"]
        assert span <= max_span, f"part still spans {span:.1f} degrees"
        assert -180.0 <= r["bbox_min_x"] <= 180.0, "longitude escaped the valid range"
        assert -180.0 <= r["bbox_max_x"] <= 180.0, "longitude escaped the valid range"

    total = sum(p.area for p in parts)
    assert abs(total - original.area) < max(area_tol, original.area * 1e-6), \
        f"area changed: {original.area} -> {total}"
```

## Querying across the split

<figure class="diagram">
<svg viewBox="0 0 762 212" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="A query window that itself crosses the antimeridian must be split into two ranges before being expanded into partition cells, otherwise it selects the whole world">
<rect x="0" y="0" width="762" height="212" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">The query window needs the same treatment</text>
<rect x="30" y="60" width="352" height="140" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="206" y="90" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">naive: one range</text>
<text x="206" y="118" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">bbox_min_x &gt;= 170 AND bbox_max_x &lt;= -170</text>
<text x="206" y="146" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">an empty range — returns nothing</text>
<text x="206" y="172" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">or, if written as OR, matches everything</text>
<rect x="398" y="60" width="352" height="140" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="574" y="90" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">correct: two ranges</text>
<text x="574" y="118" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">(x between 170 and 180)</text>
<text x="574" y="140" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">OR (x between -180 and -170)</text>
<text x="574" y="168" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">both ranges prune independently</text>
</svg>
</figure>

Build this into the helper view or table function that constructs spatial predicates rather than leaving it to callers. A window that crosses the line is rare enough that nobody remembers to handle it and common enough — anyone querying the Pacific — that it will eventually be needed, and the failure mode of the naive form is either silently empty results or a full scan.

The same reasoning applies when expanding a window into grid cells: split the window first, expand each part separately, and union the cell lists. A cell-expansion routine given a wrapped window will either return nothing or return the entire globe, and neither failure produces an error message.

## Detecting the Problem in an Existing Table

Most teams meet this issue on a table that already exists, usually while investigating why a query that should be selective is reading everything. The diagnosis is a single metadata query, and it does not touch the data.

```sql
-- Iceberg 1.4+. Files whose bounding box spans an implausible longitude range.
SELECT file_path,
       upper_bounds['bbox_max_x'] - lower_bounds['bbox_min_x'] AS span_degrees,
       record_count
FROM lakehouse.spatial.routes.files
WHERE upper_bounds['bbox_max_x'] - lower_bounds['bbox_min_x'] > 180
ORDER BY record_count DESC;
```

A handful of files with a span near 355 degrees, in a table whose data covers one ocean, is the signature. The record counts tell you how much work the fix involves: if the offending files hold a small fraction of the rows, a targeted rewrite of those files after splitting is enough, and the rest of the table is untouched.

Delta exposes the same information through the transaction log's `minValues` and `maxValues`, and a plain Parquet dataset through the footers, so the diagnosis works regardless of format. In every case it is metadata-only and takes seconds, which makes it worth adding to the periodic table audit rather than running only when something is slow.

## Prevention Versus Repair

Splitting at ingest is straightforward; retrofitting it onto a populated table is a migration, and the two paths differ enough to plan for.

Preventing the problem costs one detection check and an occasional extra row. Applied from the first write, the table never acquires a wide box and nobody ever investigates a mysterious full scan. This is the cheap path and it requires only that somebody anticipated the case — which is the entire reason this guide exists, because the Pacific is not where most teams start.

Repairing an existing table means rewriting the affected files with the split applied, and then dealing with the row-count change in every downstream consumer that counted rows rather than features. Scope the rewrite to the files the metadata query identified rather than the whole table, keep the feature identifier stable across the split parts so joins still work, and announce the row-count change before making it rather than after somebody's reconciliation fails.

Where a rewrite is genuinely impractical, a partial mitigation is to move the crossing features into a separate table and query it as a union. It is uglier and it works: the main table regains its compact statistics, and the small crossing table is read in full every time, which costs almost nothing because it is small. Treat that as a stopgap with an owner and a date rather than as an architecture.
