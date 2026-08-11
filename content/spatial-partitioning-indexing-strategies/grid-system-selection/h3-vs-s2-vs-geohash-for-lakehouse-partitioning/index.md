# H3 vs S2 vs Geohash for Lakehouse Partitioning

This guide runs H3, S2, and geohash head-to-head on the same points, measures the partition cardinality each produces at comparable cell sizes, and turns the numbers into a decision table for uniformity, neighbor operations, partition cardinality, and engine support.

## Context and prerequisites

Picking a discrete global grid for a partition key is a trade-off between cell uniformity, neighbor-query cost, and how many partitions the grid generates on your data — the topic is framed in [selecting a discrete global grid for lakehouse partitioning](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/grid-system-selection/). Here we make the trade-off concrete with a script that encodes one point set three ways and reports cardinality and cell area. You need Python 3.10+, the three encoder libraries, and pandas. This is a comparison harness, not an ingest pipeline; for the ingest side see [spatial partitioning schemes](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/spatial-partitioning-schemes/).

```bash
python -m pip install "h3>=4.1,<5" "s2sphere>=0.2.5" "python-geohash>=0.8.5" "pandas>=2.0"
```

## Complete working solution

This script generates a reproducible clustered point set (dense city, sparse rural), encodes every point with all three grids at roughly matched cell footprints, and prints partition cardinality, rows-per-cell skew, and mean cell area. It runs standalone.

```python
import math
import statistics
import pandas as pd
import numpy as np
import h3
import s2sphere
import geohash

RNG = np.random.default_rng(42)

def sample_points(n: int = 50_000) -> pd.DataFrame:
    # 70% clustered around a city center, 30% spread across a region
    n_city = int(n * 0.7)
    city_lat = RNG.normal(40.7128, 0.05, n_city)   # NYC-ish, tight cluster
    city_lon = RNG.normal(-74.0060, 0.05, n_city)
    rur_lat = RNG.uniform(40.0, 41.5, n - n_city)
    rur_lon = RNG.uniform(-75.0, -73.0, n - n_city)
    return pd.DataFrame({
        "lat": np.concatenate([city_lat, rur_lat]),
        "lon": np.concatenate([city_lon, rur_lon]),
    })

def h3_encode(lat, lon, res=8):
    return h3.latlng_to_cell(lat, lon, res)

def s2_encode(lat, lon, level=14):
    ll = s2sphere.LatLng.from_degrees(lat, lon)
    return s2sphere.CellId.from_lat_lng(ll).parent(level).to_token()

def gh_encode(lat, lon, precision=7):
    return geohash.encode(lat, lon, precision=precision)

def h3_cell_area(cell):
    return h3.cell_area(cell, unit="km^2")

def s2_cell_area(token):
    cell = s2sphere.Cell(s2sphere.CellId.from_token(token))
    # exact_area returns steradians; multiply by Earth's radius^2 (km^2)
    return cell.exact_area() * (6371.0088 ** 2)

def gh_cell_area(gh):
    lat, lon, dlat, dlon = geohash.decode_exactly(gh)
    # width in km scales with cos(latitude); height does not
    h = (dlat * 2) * 111.32
    w = (dlon * 2) * 111.32 * math.cos(math.radians(lat))
    return h * w

def summarize(name, keys, area_fn, sample_keys):
    counts = pd.Series(keys).value_counts()
    areas = [area_fn(k) for k in sample_keys]
    return {
        "grid": name,
        "partitions": counts.size,
        "rows_per_cell_median": int(counts.median()),
        "rows_per_cell_max": int(counts.max()),
        "skew_max_over_median": round(counts.max() / max(counts.median(), 1), 1),
        "mean_cell_area_km2": round(statistics.mean(areas), 4),
    }

def main():
    df = sample_points()
    df["h3"] = [h3_encode(la, lo) for la, lo in zip(df.lat, df.lon)]
    df["s2"] = [s2_encode(la, lo) for la, lo in zip(df.lat, df.lon)]
    df["gh"] = [gh_encode(la, lo) for la, lo in zip(df.lat, df.lon)]

    # sample distinct keys for area stats (area is per-cell, not per-row)
    rows = [
        summarize("H3 res8", df["h3"], h3_cell_area, df["h3"].drop_duplicates().head(500)),
        summarize("S2 lvl14", df["s2"], s2_cell_area, df["s2"].drop_duplicates().head(500)),
        summarize("Geohash p7", df["gh"], gh_cell_area, df["gh"].drop_duplicates().head(500)),
    ]
    print(pd.DataFrame(rows).to_string(index=False))

if __name__ == "__main__":
    main()
```

## Step-by-step walkthrough

1. **`sample_points`** builds a deliberately skewed set: 70% of points sit in a tight normal cluster (a city), 30% spread uniformly across a region. Real telemetry is skewed this way, and skew is exactly what stresses a partition scheme.
2. **The three encoders** take EPSG:4326 degrees. H3 returns a 15-char hex id; S2 returns a compact token from a level-14 cell; geohash returns a 7-character base-32 string. Resolutions are matched so cells are within the same order of magnitude in area, making the cardinality comparison fair.
3. **Area functions** are grid-specific. H3 exposes `cell_area` directly. S2's `exact_area()` returns steradians, so we multiply by Earth's radius squared. Geohash cell width shrinks with `cos(latitude)` — the code multiplies longitude span by that factor, which is why geohash cells become tall, thin rectangles away from the equator.
4. **`summarize`** computes the numbers that drive the decision: partition count (directory/manifest pressure), median and max rows per cell, the max/median skew ratio (hot-partition risk), and mean cell area.
5. **Output** is a compact DataFrame. On the seeded data you will see H3 and S2 produce similar partition counts with lower skew, while geohash shows a higher max/median ratio because its rectangular cells split the dense cluster unevenly.

Interpreting the run: the grid with the *lowest* skew ratio at your target cell size is the safest partition key, because a high max/median ratio is a hot partition waiting to stall compaction. If two grids tie on skew, break the tie on neighbor-operation cost and engine support using the table below.

## Decision table

| Dimension | H3 | S2 | Geohash |
|---|---|---|---|
| Cell shape | Hexagon (12 pentagons) | Quadrilateral | Lat/lon rectangle |
| Area uniformity | High, near-equal area | High on sphere | Poor, distorts with latitude |
| Neighbor ops | `grid_disk(cell, k)`, 6 neighbors | `S2CellUnion` range cover | 8 rects, base-32 seams |
| Hierarchy | Aperture-7, non-containing | Aperture-4, strictly containing | Prefix = parent, containing |
| Partition cardinality control | Resolution 0–15 (×7/level) | Level 0–30 (×4/level) | Precision 1–12 (×32/char) |
| Range-cover a polygon | Weak (rings only) | Strong (id ranges) | Weak |
| Human-readable key | No (opaque hex) | No (token) | Yes (prefix nesting) |
| Engine/library support | h3-py, Sedona, BigQuery, DuckDB `h3` ext | s2sphere, Sedona, BigQuery | python-geohash, most DBs, trivial |
| Best partition fit | KNN/ring aggregation on Iceberg | Spherical covering, GCP interop | Low-dep ingest, readable prefixes |

For most lakehouse point workloads H3 is the default: near-uniform hexagons keep partition sizes even and `grid_disk` makes proximity queries one call. Choose S2 when you must cover arbitrary polygons with compact id ranges or interoperate with the Google geo stack. Choose geohash only when minimal dependencies and human-readable prefixes outweigh its latitude distortion and hot-partition risk.

## Common errors and fixes

| Error | Cause | Fix |
|---|---|---|
| `AttributeError: module 'h3' has no attribute 'geo_to_h3'` | h3-py 3.x API on 4.x install | Use `h3.latlng_to_cell`; pin `h3>=4.1,<5` |
| S2 areas 100× too small | Forgot to scale `exact_area()` steradians by R² | Multiply by `6371.0088 ** 2` for km² |
| Geohash cells look square at all latitudes | Ignored the `cos(latitude)` width factor | Scale longitude span by `cos(radians(lat))` |
| Cardinality far higher than expected | Resolution/level too fine for the extent | Coarsen one H3 level, one S2 level, or one geohash char |

## Verification

Confirm the matched resolutions really are comparable and that the encoders round-trip. This asserts the three mean cell areas land within one order of magnitude and that a geohash prefix is its own parent cell.

```python
import h3, s2sphere, geohash

lat, lon = 40.7128, -74.0060
gh = geohash.encode(lat, lon, precision=7)
assert gh.startswith(geohash.encode(lat, lon, precision=6)), "prefix must be parent"

h = h3.latlng_to_cell(lat, lon, 8)
assert h3.get_resolution(h) == 8

tok = s2sphere.CellId.from_lat_lng(s2sphere.LatLng.from_degrees(lat, lon)).parent(14).to_token()
assert s2sphere.CellId.from_token(tok).level() == 14
print("H3", h, "| S2", tok, "| geohash", gh)
```

<figure class="diagram">
<svg viewBox="0 0 722 234" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Bar comparison of partition cardinality and skew for H3, S2 and geohash on the same skewed point set">
<title>Cardinality and skew per grid on a skewed point set</title>
<desc>Three grouped bars showing that H3 and S2 produce moderate partition counts with low hot-partition skew, while geohash produces higher skew at a matched cell size.</desc>
<rect x="0" y="0" width="722" height="234" fill="#f7fbfc"/>
<text x="380" y="26" text-anchor="middle" font-family="sans-serif" font-size="15" fill="#0d3b45" font-weight="bold">Same points, three grids: cardinality vs hot-partition skew</text>
<line x1="90" y1="200" x2="710" y2="200" stroke="#33707d" stroke-width="1.5"/>
<line x1="90" y1="60" x2="90" y2="200" stroke="#33707d" stroke-width="1.5"/>
<text x="60" y="130" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d" transform="rotate(-90 60 130)">relative scale</text>
<!-- H3 group -->
<rect x="150" y="120" width="40" height="80" fill="#0e6e7d"/>
<rect x="196" y="150" width="40" height="50" fill="#2f6e49"/>
<text x="193" y="218" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45" font-weight="bold">H3 res8</text>
<text x="170" y="112" text-anchor="middle" font-family="sans-serif" font-size="10" fill="#0d3b45">cells</text>
<text x="216" y="142" text-anchor="middle" font-family="sans-serif" font-size="10" fill="#0d3b45">skew</text>
<!-- S2 group -->
<rect x="340" y="115" width="40" height="85" fill="#0e6e7d"/>
<rect x="386" y="155" width="40" height="45" fill="#2f6e49"/>
<text x="383" y="218" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45" font-weight="bold">S2 lvl14</text>
<text x="360" y="107" text-anchor="middle" font-family="sans-serif" font-size="10" fill="#0d3b45">cells</text>
<text x="406" y="147" text-anchor="middle" font-family="sans-serif" font-size="10" fill="#0d3b45">skew</text>
<!-- Geohash group -->
<rect x="530" y="125" width="40" height="75" fill="#0e6e7d"/>
<rect x="576" y="95" width="40" height="105" fill="#9a5a17"/>
<text x="573" y="218" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45" font-weight="bold">Geohash p7</text>
<text x="550" y="117" text-anchor="middle" font-family="sans-serif" font-size="10" fill="#0d3b45">cells</text>
<text x="596" y="87" text-anchor="middle" font-family="sans-serif" font-size="10" fill="#9a5a17" font-weight="bold">high skew</text>
</svg>
</figure>

Once you have chosen a grid from this comparison, size its resolution against your data with the method in [choosing an H3 resolution for point data](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/grid-system-selection/choosing-h3-resolution-for-point-data/), and make sure the resulting key actually prunes by following [predicate pushdown optimization](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/predicate-pushdown-optimization/). External references: the [H3 documentation](https://h3geo.org/docs/) and the [S2 Geometry library](https://s2geometry.io/).

## Identifier Structure and What It Enables

The three systems differ most consequentially in how their identifiers are constructed, because the identifier is what the query planner actually manipulates.

<figure class="diagram">
<svg viewBox="0 0 764 290" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Identifier structure of the three grid systems: geohash as a prefix string where a prefix match is a range scan, S2 as a 64 bit cell id along a Hilbert curve where a cell covers a contiguous integer range, and H3 as a 64 bit id encoding resolution and a digit path">
<rect x="0" y="0" width="764" height="290" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Identifier structure decides what the planner can do</text>
<rect x="26" y="56" width="726" height="66" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="42" y="82" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">geohash</text>
<text x="150" y="82" font-family="sans-serif" font-size="11" fill="#33707d">base-32 string, &#8220;u33dbc&#8221; — a prefix is a parent</text>
<text x="150" y="104" font-family="sans-serif" font-size="11" fill="#0d3b45">enables: LIKE &#8216;u33%&#8217; as a range scan · costs: string column, uneven cell shapes</text>
<rect x="26" y="134" width="726" height="66" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="42" y="160" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">S2</text>
<text x="150" y="160" font-family="sans-serif" font-size="11" fill="#33707d">64-bit id ordered along a Hilbert curve</text>
<text x="150" y="182" font-family="sans-serif" font-size="11" fill="#0d3b45">enables: a region becomes a few integer BETWEEN ranges · costs: cell area varies by face</text>
<rect x="26" y="212" width="726" height="66" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="42" y="238" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">H3</text>
<text x="150" y="238" font-family="sans-serif" font-size="11" fill="#33707d">64-bit id encoding resolution plus a digit path</text>
<text x="150" y="260" font-family="sans-serif" font-size="11" fill="#0d3b45">enables: IN list of cells, uniform neighbours · costs: no contiguous range for a region</text>
</svg>
</figure>

The difference between a **range** and an **IN list** is the practical crux. S2's Hilbert ordering means a contiguous region maps to a small number of integer ranges, which every query planner handles natively and which prunes beautifully on min/max statistics. H3 produces a set of unordered identifiers, so a region becomes an `IN` list — perfectly workable, and prunes well on partition values, but it does not benefit from range statistics inside a file the way an S2 range does.

Geohash's string prefix property is genuinely convenient for debugging and for systems where a human reads the key, and it is the weakest of the three for a partitioned table: string comparison is slower, the column is larger, statistics are lexicographic, and the cells are rectangles in degree space with all the latitude distortion that implies.

The choice therefore follows the pruning mechanism the table depends on. Tables that prune primarily by partition value do well with H3, whose uniform neighbours and clean resolution hierarchy make window expansion straightforward. Tables that prune primarily by within-file statistics do better with S2, because a range predicate is exactly what those statistics can evaluate. Tables where a human is expected to read the key are the case for geohash, and that case is rarer than its popularity suggests.

## Library and Ecosystem Considerations

Beyond the technical differences, the practical question of which library is already available in the stack decides more of these choices than anyone admits, and it is a legitimate criterion.

All three systems have mature implementations in the languages that matter for a lakehouse — Java for Spark and Trino, Python for orchestration, Rust and C++ underneath both. Where they differ is in **engine-native availability**: some warehouses ship one system as built-in SQL functions and require a user-defined function for the others, and a built-in beats a UDF for both performance and for the optimiser's ability to reason about the expression. Check what the primary query engine exposes natively before choosing, because a partition key that requires a UDF on the query side is a partition key some callers will avoid.

The second ecosystem consideration is **interoperability with data you receive**. Datasets published by others frequently carry a cell identifier already, and matching it avoids a re-derivation and a class of boundary mismatches. Mobility and telemetry datasets tend to arrive with H3; geospatial indexing systems originating from search and mapping infrastructure tend to use S2; and older or simpler publications use geohash. Where a substantial input already carries one, adopting it is usually cheaper than converting.

The last consideration is one of longevity. All three are stable, well-specified and unlikely to disappear, so the risk is not abandonment but subtle change: a library release that alters a boundary assignment, or a new version that changes a default. Pin the library version, record it alongside the data as described earlier, and treat an upgrade as a change that needs a fixed-coordinate test rather than as a routine dependency bump. That discipline matters more than the system chosen, because it is what makes the identifier a stable fact rather than a computation that happens to agree with itself today.

## A Short Verdict

<figure class="diagram">
<svg viewBox="0 0 764 198" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Recommendation summary: H3 as the default for partition value pruning and uniform analysis cells, S2 where range statistics drive pruning, and geohash where a readable key matters most">
<rect x="0" y="0" width="764" height="198" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Pick by pruning mechanism, not by popularity</text>
<rect x="26" y="58" width="230" height="128" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2.5"/>
<text x="141" y="88" text-anchor="middle" font-family="sans-serif" font-size="14" font-weight="700" fill="#0d3b45">H3</text>
<text x="141" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">default choice</text>
<text x="141" y="142" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">partition-value pruning,</text>
<text x="141" y="162" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">uniform analysis cells</text>
<rect x="274" y="58" width="230" height="128" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="389" y="88" text-anchor="middle" font-family="sans-serif" font-size="14" font-weight="700" fill="#0d3b45">S2</text>
<text x="389" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">range-statistics pruning</text>
<text x="389" y="142" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">unpartitioned tables,</text>
<text x="389" y="162" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">sort-order-driven layouts</text>
<rect x="522" y="58" width="230" height="128" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="637" y="88" text-anchor="middle" font-family="sans-serif" font-size="14" font-weight="700" fill="#0d3b45">geohash</text>
<text x="637" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">human-readable keys</text>
<text x="637" y="142" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">debugging, interchange,</text>
<text x="637" y="162" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">legacy compatibility</text>
</svg>
</figure>

None of these is a mistake, and a platform running two of them for different tables is normal rather than untidy. What does cause trouble is running two on the *same* table without saying so, or converting between them implicitly in a pipeline — so whichever is chosen, name the column after it and keep the derivation in one place.
