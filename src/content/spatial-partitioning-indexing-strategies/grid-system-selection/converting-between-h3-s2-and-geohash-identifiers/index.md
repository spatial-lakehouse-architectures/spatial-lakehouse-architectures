# Converting Between H3, S2 and Geohash Identifiers

This guide converts grid cell identifiers between the three common systems safely, explains why a direct identifier-to-identifier mapping does not exist, and shows the two correct approaches with the accuracy each provides.

## Context and prerequisites

Cell identifiers arrive with data: a mobility dataset carries H3, an index built on search infrastructure carries S2, an older publication carries geohash. Joining them requires a conversion, and the naive expectation — that a cell in one system corresponds to a cell in another — is false. This recipe uses Python with the `h3` and `s2sphere` bindings and Shapely 2.x; the system comparison is in [H3 vs S2 vs geohash for lakehouse partitioning](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/grid-system-selection/h3-vs-s2-vs-geohash-for-lakehouse-partitioning/).

## Why there is no direct mapping

<figure class="diagram">
<svg viewBox="0 0 762 282" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Overlaid hexagonal and square grids showing that no cell of one system aligns with any cell of the other, so a conversion is always many to many">
<rect x="0" y="0" width="762" height="282" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">The tilings do not align at any resolution</text>
<rect x="120" y="60" width="90" height="60" fill="none" stroke="#0e6e7d" stroke-width="1.8"/>
<rect x="210" y="60" width="90" height="60" fill="none" stroke="#0e6e7d" stroke-width="1.8"/>
<rect x="300" y="60" width="90" height="60" fill="none" stroke="#0e6e7d" stroke-width="1.8"/>
<rect x="120" y="120" width="90" height="60" fill="none" stroke="#0e6e7d" stroke-width="1.8"/>
<rect x="210" y="120" width="90" height="60" fill="#e4f0f2" fill-opacity="0.7" stroke="#0e6e7d" stroke-width="2.5"/>
<rect x="300" y="120" width="90" height="60" fill="none" stroke="#0e6e7d" stroke-width="1.8"/>
<rect x="120" y="180" width="90" height="60" fill="none" stroke="#0e6e7d" stroke-width="1.8"/>
<rect x="210" y="180" width="90" height="60" fill="none" stroke="#0e6e7d" stroke-width="1.8"/>
<rect x="300" y="180" width="90" height="60" fill="none" stroke="#0e6e7d" stroke-width="1.8"/>
<path d="M175 90 l38 22 v44 l-38 22 l-38 -22 v-44 z" fill="none" stroke="#9a5a17" stroke-width="2"/>
<path d="M251 90 l38 22 v44 l-38 22 l-38 -22 v-44 z" fill="none" stroke="#9a5a17" stroke-width="2"/>
<path d="M327 90 l38 22 v44 l-38 22 l-38 -22 v-44 z" fill="none" stroke="#9a5a17" stroke-width="2"/>
<path d="M213 156 l38 22 v44 l-38 22 l-38 -22 v-44 z" fill="none" stroke="#9a5a17" stroke-width="2"/>
<path d="M289 156 l38 22 v44 l-38 22 l-38 -22 v-44 z" fill="none" stroke="#9a5a17" stroke-width="2"/>
<text x="255" y="266" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">one square cell overlaps four or five hexagons</text>
<rect x="450" y="76" width="300" height="150" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="600" y="106" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">consequences</text>
<text x="600" y="134" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">no exact identifier mapping exists</text>
<text x="600" y="158" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">every conversion is many-to-many</text>
<text x="600" y="182" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">counts cannot be reassigned exactly</text>
<text x="600" y="206" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">go via geometry, or via the source points</text>
</svg>
</figure>

The consequence that matters most is the third one. An aggregate computed per H3 cell cannot be converted into an aggregate per S2 cell without either returning to the underlying points or accepting an areal-interpolation approximation. Teams frequently attempt the conversion at the aggregate level and produce numbers that are wrong by an amount that varies with the local density — invisible in a map and material in a report.

## Complete working solution

```python
import h3
import s2sphere
from shapely import from_wkb, Polygon, box, intersection, area

def cell_to_polygon(system: str, cell) -> Polygon:
    """The canonical bridge: every system can produce a boundary polygon."""
    if system == "h3":
        return Polygon([(lng, lat) for lat, lng in h3.cell_to_boundary(cell)])
    if system == "s2":
        c = s2sphere.Cell(s2sphere.CellId(int(cell)))
        pts = []
        for i in range(4):
            v = s2sphere.LatLng.from_point(c.get_vertex(i))
            pts.append((v.lng().degrees, v.lat().degrees))
        return Polygon(pts)
    if system == "geohash":
        return _geohash_box(cell)
    raise ValueError(system)

def convert_cell(src_system: str, cell, dst_system: str, dst_res: int) -> list:
    """All destination cells overlapping the source cell. Many-to-many by nature."""
    poly = cell_to_polygon(src_system, cell)
    if dst_system == "h3":
        return sorted(h3.geo_to_cells(poly.__geo_interface__, dst_res))
    if dst_system == "s2":
        coverer = s2sphere.RegionCoverer()
        coverer.min_level = coverer.max_level = dst_res
        rect = _to_s2_rect(poly.bounds)
        return sorted(str(c.id()) for c in coverer.get_covering(rect))
    if dst_system == "geohash":
        return sorted(_geohash_cover(poly, dst_res))
    raise ValueError(dst_system)

def reassign_points(df, lon_col: str, lat_col: str, dst_system: str, dst_res: int):
    """The exact route: derive the destination cell from the coordinates."""
    if dst_system == "h3":
        return df.assign(dst_cell=[h3.latlng_to_cell(la, lo, dst_res)
                                   for lo, la in zip(df[lon_col], df[lat_col])])
    raise NotImplementedError("same shape for the other two systems")
```

## Step-by-step walkthrough

1. **Go via geometry, always.** Every system can produce a cell boundary polygon and every system can cover a polygon with cells. That round trip is the only general conversion, and it is exact in the sense that it returns every destination cell the source cell touches.

2. **Prefer re-deriving from coordinates.** Where the original points are available, deriving the destination cell directly is both exact and cheaper than a geometry conversion. Use the polygon route only when the points are gone and the cells are all that remains.

3. **Choose the destination resolution deliberately.** A conversion to a coarser resolution loses information; to a much finer one produces a large cell list. Matching approximate cell areas between systems, rather than matching level numbers, is the sensible default — the numbering schemes are unrelated.

4. **Return a sorted list.** Deterministic ordering makes the output comparable across runs, which matters for the reconciliation tests that should accompany any conversion.

5. **Never convert aggregates without saying so.** If a count per source cell must become a count per destination cell, the result is an interpolation and must be labelled as one.

## Common errors and fixes

| Symptom | Cause | Fix |
|---|---|---|
| Cell counts explode after conversion | Destination resolution much finer than source | Match approximate cell areas, not level numbers |
| Some points fall in no destination cell | Covering computed from the bounding box, not the polygon | Cover the boundary polygon; a box over-covers, a bad cover under-covers |
| Converted totals do not match the source | Aggregates interpolated rather than recomputed | Recompute from points where possible; otherwise document the approximation |
| Results differ between library versions | Cell boundary definitions changed | Pin the library and record the version alongside the data |
| Conversion very slow | Per-cell geometry round trip on millions of cells | Convert the distinct source cells once and join the mapping |

## Choosing comparable resolutions

<figure class="diagram">
<svg viewBox="0 0 762 256" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Approximate cell edge lengths for comparable resolutions in the three systems, showing that level numbers are unrelated and areas should be matched instead">
<rect x="0" y="0" width="762" height="256" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Match areas, not level numbers</text>
<rect x="30" y="52" width="180" height="34" rx="6" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="1.5"/>
<text x="120" y="75" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">approx. cell size</text>
<rect x="220" y="52" width="170" height="34" rx="6" fill="#e6f0ea" stroke="#2f6e49" stroke-width="1.5"/>
<text x="305" y="75" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">H3</text>
<rect x="400" y="52" width="170" height="34" rx="6" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="1.5"/>
<text x="485" y="75" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">S2</text>
<rect x="580" y="52" width="170" height="34" rx="6" fill="#f2e8da" stroke="#9a5a17" stroke-width="1.5"/>
<text x="665" y="75" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">geohash</text>
<text x="40" y="114" font-family="sans-serif" font-size="12" fill="#0d3b45">~150 km</text>
<text x="305" y="114" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">res 3</text>
<text x="485" y="114" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">level 6</text>
<text x="665" y="114" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">3 chars</text>
<line x1="30" y1="128" x2="750" y2="128" stroke="#cfe3e7" stroke-width="1.5"/>
<text x="40" y="156" font-family="sans-serif" font-size="12" fill="#0d3b45">~5 km</text>
<text x="305" y="156" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">res 6</text>
<text x="485" y="156" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">level 11</text>
<text x="665" y="156" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">5 chars</text>
<line x1="30" y1="170" x2="750" y2="170" stroke="#cfe3e7" stroke-width="1.5"/>
<text x="40" y="198" font-family="sans-serif" font-size="12" fill="#0d3b45">~150 m</text>
<text x="305" y="198" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">res 9</text>
<text x="485" y="198" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">level 15</text>
<text x="665" y="198" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#33707d">7 chars</text>
<text x="390" y="240" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">Figures are approximate and vary with latitude — verify for your extent</text>
</svg>
</figure>

The table is a starting point rather than an authority: cell areas vary with latitude in all three systems, by more in geohash than in the other two, so a pairing that matches well at the equator will not match as well at high latitudes. Where the conversion's accuracy matters, compute the actual areas over the dataset's own extent rather than relying on a global figure.

## Verification

```python
def verify_conversion(src_system, src_cells, dst_system, dst_res, sample_points):
    """Every point in a source cell must land in one of its converted cells."""
    for cell in src_cells:
        dst = set(convert_cell(src_system, cell, dst_system, dst_res))
        for lon, lat in sample_points_in(cell):
            assigned = derive_cell(dst_system, lon, lat, dst_res)
            assert assigned in dst, (
                f"point {lon},{lat} in {cell} mapped to {assigned}, not in {dst}")
```

This is the assertion that catches an under-covering conversion, which is the dangerous direction: an over-covering result includes cells that contain no data and costs a little extra scanning, while an under-covering one silently loses rows. Sample points from the interior *and* near the boundary, because boundary points are where a covering computed from a bounding box rather than a boundary polygon fails.

Record the library versions used for the conversion alongside its output. Cell boundary definitions are stable but not immutable, and a conversion recomputed two years later with a different library version that disagrees at the margins produces a diff nobody can explain without that record.

## The Aggregate Interpolation Problem

When the underlying points are gone and only per-cell counts remain, converting to another system is an interpolation. It is sometimes the only option, and it should always be labelled.

<figure class="diagram">
<svg viewBox="0 0 762 292" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Areal interpolation of a per cell count into an overlapping cell of another system, assuming uniform density within the source cell, with the error this assumption introduces">
<rect x="0" y="0" width="762" height="292" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Areal interpolation assumes what is usually false</text>
<path d="M180 70 l60 34 v68 l-60 34 l-60 -34 v-68 z" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2.5"/>
<rect x="160" y="96" width="130" height="80" fill="#e4f0f2" fill-opacity="0.55" stroke="#0e6e7d" stroke-width="2.5"/>
<text x="180" y="228" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">source cell, count = 1200</text>
<text x="180" y="250" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">overlap covers 38% of its area</text>
<rect x="380" y="76" width="370" height="120" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="565" y="106" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">the interpolation</text>
<text x="565" y="132" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">assign 1200 × 0.38 = 456</text>
<text x="565" y="158" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">correct only if density is uniform</text>
<text x="565" y="180" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">inside the source cell — it never is</text>
<text x="390" y="276" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">Error scales with how concentrated the data is within each source cell</text>
</svg>
</figure>

The assumption fails worst exactly where it matters: in dense areas, where population and activity are concentrated in a fraction of the cell, an area-proportional split can be wrong by a large factor. In sparse areas it is closer to correct and also less consequential.

Two mitigations improve it materially. Using a **weighting surface** — population, building footprints, road density — instead of raw area redistributes the count according to something correlated with where the data actually is, and typically halves the error. And converting **at the finest available source resolution** before aggregating up limits how much can be misassigned, because a finer source cell has less room for internal concentration.

Neither makes the result exact, and the honest treatment is to publish the interpolated figures with the method and an error estimate attached rather than as though they were measured. Where an exact figure is required and the points are gone, the answer is that the conversion cannot supply it — which is a more useful thing to say than a number that looks precise.

## Building a Reusable Mapping

For a conversion that will be applied repeatedly — two datasets in different systems joined every night — computing the cell correspondence once and storing it beats recomputing per run.

The mapping is a table of source cell, destination cell and overlap fraction, built once from the distinct source cells present. It is typically small relative to the data, it is deterministic, and it makes the nightly job an ordinary join rather than a geometry computation. Version it alongside the library versions used to build it, for the reason described above.

Rebuild it when either dataset's cell set grows beyond the mapping's coverage, which a left join with a null check detects for free during the nightly run. A missing source cell should fail the job rather than silently drop rows — the whole point of the mapping is that it is complete over the data it serves.

## Avoiding the Conversion Altogether

The best conversion is the one that does not happen, and two arrangements avoid it.

**Derive both identifiers at ingest.** Where a dataset will be joined against data in another system, computing both cell identifiers from the coordinates at write time costs microseconds per row and removes the conversion permanently. Both are exact, both prune, and the join becomes an ordinary equi-join in whichever system suits the query. The storage cost is eight bytes per additional identifier, which is negligible against the geometry column.

**Standardise on one system at the platform boundary.** Where data arrives carrying a foreign system's identifiers, deriving the platform's own identifier from the coordinates at ingest — and keeping the foreign one as a provenance column — means every table in the lakehouse shares one grid. Joins then need no conversion at all, and the foreign identifier remains available for reconciliation against the source.

Both approaches share the same principle that runs through the rest of this section: derive at write time, once, from the coordinates, rather than converting at read time, repeatedly, from a derived value. The write-time derivation is exact and cheap; the read-time conversion is approximate and expensive, and the difference compounds with every query.

Reserve the conversion techniques above for the cases where neither is available — historical data whose coordinates were discarded, or an external dataset that cannot be re-ingested — and label their output as the approximation it is.
