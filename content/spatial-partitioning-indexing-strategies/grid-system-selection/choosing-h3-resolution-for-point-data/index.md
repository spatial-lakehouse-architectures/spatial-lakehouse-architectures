# Choosing an H3 Resolution for Point Data

This guide gives you a repeatable method — plus a runnable script that samples your dataset and prints a recommendation — for picking the H3 resolution that hits your target rows-per-partition while keeping your query radius inside a small ring of cells.

## Context and prerequisites

H3 resolution is the knob that decides partition cardinality: each finer resolution splits a cell into seven children, so moving one level multiplies your partition count by roughly seven and divides rows-per-cell by the same factor. Pick it wrong and you either get multi-gigabyte hot partitions or millions of tiny files. This page assumes you have already chosen H3 over S2 and geohash — that decision is covered in [selecting a discrete global grid for lakehouse partitioning](/spatial-partitioning-indexing-strategies/grid-system-selection/) and benchmarked in [H3 vs S2 vs geohash for lakehouse partitioning](/spatial-partitioning-indexing-strategies/grid-system-selection/h3-vs-s2-vs-geohash-for-lakehouse-partitioning/). You need Python 3.10+ and h3-py 4.x.

```bash
python -m pip install "h3>=4.1,<5" "pandas>=2.0" "pyarrow>=14"
```

The method balances two constraints. First, a **capacity** constraint: at your target rows-per-partition, which resolution keeps the busiest cell near that target? Second, a **query-radius** constraint: your typical proximity query radius should be covered by a small H3 `grid_disk` k-ring, not hundreds of cells, or point lookups fan out across too many partitions.

## Complete working solution

This script reads a Parquet sample of your points, evaluates every candidate resolution, and recommends the finest resolution whose 95th-percentile cell load stays at or below your target rows-per-partition while the query radius fits within a k-ring of at most `max_k`. It runs standalone against any Parquet file with `lat`/`lon` columns.

```python
import argparse
import math
import pandas as pd
import pyarrow.parquet as pq
import h3

# average H3 edge length in meters, by resolution (h3geo.org table, res 0-12)
H3_EDGE_M = {
    0: 1_281_256, 1: 483_057, 2: 182_513, 3: 68_979, 4: 26_072,
    5: 9_854, 6: 3_724, 7: 1_406, 8: 531, 9: 200,
    10: 75.9, 11: 28.7, 12: 10.8,
}

def load_sample(path: str, sample_rows: int) -> pd.DataFrame:
    df = pq.read_table(path, columns=["lat", "lon"]).to_pandas()
    df = df.dropna(subset=["lat", "lon"])
    if len(df) > sample_rows:
        df = df.sample(sample_rows, random_state=42)
    return df

def cells_to_cover_radius(res: int, radius_m: float) -> int:
    # k such that a k-ring of res-cells spans the query radius
    edge = H3_EDGE_M[res]
    return max(1, math.ceil(radius_m / (edge * math.sqrt(3))))

def evaluate(df: pd.DataFrame, target_rpp: int, radius_m: float,
             max_k: int, scale_factor: float,
             res_range=range(4, 13)) -> pd.DataFrame:
    rows = []
    for res in res_range:
        cells = df.apply(lambda r: h3.latlng_to_cell(r.lat, r.lon, res), axis=1)
        load = cells.value_counts()
        # scale sample counts up to the full dataset
        p95 = float(load.quantile(0.95)) * scale_factor
        peak = int(load.max()) * scale_factor
        k = cells_to_cover_radius(res, radius_m)
        rows.append({
            "res": res,
            "distinct_cells_in_sample": load.size,
            "p95_rows_per_cell": round(p95, 1),
            "peak_rows_per_cell": round(peak, 1),
            "k_ring_for_radius": k,
            "meets_capacity": p95 <= target_rpp,
            "meets_radius": k <= max_k,
        })
    return pd.DataFrame(rows)

def recommend(report: pd.DataFrame) -> int | None:
    ok = report[report.meets_capacity & report.meets_radius]
    # finest resolution that satisfies both constraints
    return int(ok.res.max()) if not ok.empty else None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("parquet")
    ap.add_argument("--total-rows", type=int, required=True,
                    help="row count of the FULL dataset")
    ap.add_argument("--sample-rows", type=int, default=200_000)
    ap.add_argument("--target-rpp", type=int, default=5_000_000,
                    help="target rows per partition after write")
    ap.add_argument("--query-radius-m", type=float, default=1_000.0)
    ap.add_argument("--max-k", type=int, default=3)
    args = ap.parse_args()

    df = load_sample(args.parquet, args.sample_rows)
    scale = args.total_rows / len(df)
    report = evaluate(df, args.target_rpp, args.query_radius_m, args.max_k, scale)
    print(report.to_string(index=False))
    rec = recommend(report)
    if rec is None:
        print("\nNo resolution meets both constraints; relax target-rpp or max-k.")
    else:
        print(f"\nRecommended H3 resolution: {rec}")

if __name__ == "__main__":
    main()
```

Invoke it against a Parquet extract of your table:

```bash
python h3_resolution_picker.py points.parquet \
    --total-rows 4200000000 \
    --target-rpp 5000000 \
    --query-radius-m 800 \
    --max-k 3
```

## Step-by-step walkthrough

1. **`load_sample`** reads only `lat`/`lon` and subsamples to keep the scan cheap. A 200k-row sample estimates cell load distribution well enough to choose a resolution; you do not need the full 4-billion-row table.
2. **`scale_factor`** (`total_rows / sample_rows`) projects the sampled per-cell counts up to the full dataset, so `p95_rows_per_cell` is an estimate of the real partition load, not the sample load.
3. **`evaluate`** encodes the sample at each candidate resolution, then computes the 95th-percentile and peak cell load. It uses the 95th percentile rather than the mean because partition sizing must survive the busy cells, not the average one.
4. **`cells_to_cover_radius`** converts your query radius into a k-ring count using H3's average edge length per resolution. A finer resolution needs a larger `k` to cover the same radius; when `k` exceeds `max_k`, proximity queries touch too many partitions.
5. **`recommend`** returns the *finest* resolution satisfying both the capacity and radius constraints — finest, because smaller cells give better predicate selectivity, and you want the smallest cell you can afford before partitions get too big or ring queries fan out.
6. If no resolution qualifies, the two constraints conflict: either your target rows-per-partition is too small for your query radius, or the data is too dense. Relax `--target-rpp` upward or `--max-k` and rerun.

The default `--target-rpp` of 5 million rows aligns with a 128 MB–1 GB Parquet file at typical point-row widths. Once you have the resolution, materialize it as a partition column and wrap it in a `bucket` transform to tame hot cells, exactly as shown for [spatial partitioning schemes](/spatial-partitioning-indexing-strategies/spatial-partitioning-schemes/) and [implementing H3 hexagon partitioning in Delta Lake](/spatial-partitioning-indexing-strategies/spatial-partitioning-schemes/implementing-h3-hexagon-partitioning-in-delta-lake/).

## Common errors and fixes

| Error | Cause | Fix |
|---|---|---|
| `KeyError` in `H3_EDGE_M` | `res_range` extends past 12 | Extend the edge table from the h3geo.org resolution page |
| p95 looks unrealistically low | Sample too small or not representative | Raise `--sample-rows`; sample across time, not one hour |
| Recommendation flips between runs | No fixed random seed on a fresh extract | Sampling is seeded (`random_state=42`); pin the Parquet extract |
| `meets_radius` always False | Query radius large vs. cell size | Raise `--max-k` or accept a coarser resolution |
| Peak cell dwarfs p95 | A single mega-cell (stadium, port) | Plan to `bucket()` that partition; do not chase it with resolution |

## Verification

After ingesting at the recommended resolution, confirm the real partition load matches the estimate. This Spark SQL check reports the 95th-percentile and max rows per H3 cell on the written table; both should sit near your target.

```sql
-- Spark SQL on the Iceberg/Delta table, after ingest
SELECT
    percentile_approx(cnt, 0.95) AS p95_rows_per_cell,
    MAX(cnt)                     AS peak_rows_per_cell,
    COUNT(*)                     AS distinct_cells
FROM (
    SELECT h3_res8 AS cell, COUNT(*) AS cnt
    FROM lake.geo.events
    GROUP BY h3_res8
);
```

If `peak_rows_per_cell` runs far above `p95`, a few hot cells dominate — handle those with a `bucket` transform rather than by globally raising resolution, which would over-fragment every rural cell. Then confirm the key still prunes with [predicate pushdown optimization](/spatial-partitioning-indexing-strategies/predicate-pushdown-optimization/), and pair the partition with intra-file [Z-ordering for geospatial queries](/spatial-partitioning-indexing-strategies/z-ordering-for-geospatial-queries/) on `lon, lat`.

<figure class="diagram">
<svg viewBox="0 0 760 250" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Two opposing constraints on H3 resolution converging on a recommended level">
<title>Capacity and query-radius constraints select an H3 resolution</title>
<desc>As resolution increases, rows per partition falls (good until too small) while the k-ring needed to cover a fixed query radius grows (bad past a limit). The recommended resolution is the finest level inside both bounds.</desc>
<rect x="0" y="0" width="760" height="250" fill="#f7fbfc"/>
<text x="380" y="26" text-anchor="middle" font-family="sans-serif" font-size="15" fill="#0d3b45" font-weight="bold">Two constraints pick the resolution</text>
<line x1="90" y1="200" x2="700" y2="200" stroke="#33707d" stroke-width="1.5"/>
<text x="395" y="228" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">coarse  &#8592;  H3 resolution  &#8594;  fine</text>
<!-- capacity curve: rows per partition falls -->
<path d="M110,80 C250,90 330,150 690,190" fill="none" stroke="#0e6e7d" stroke-width="2.5"/>
<text x="150" y="72" font-family="sans-serif" font-size="11" fill="#0e6e7d" font-weight="bold">rows / partition</text>
<!-- radius curve: k-ring grows -->
<path d="M110,190 C400,185 470,120 690,70" fill="none" stroke="#9a5a17" stroke-width="2.5"/>
<text x="560" y="62" font-family="sans-serif" font-size="11" fill="#9a5a17" font-weight="bold">k-ring for radius</text>
<!-- feasible window -->
<rect x="360" y="60" width="90" height="140" fill="#2f6e49" fill-opacity="0.12" stroke="#2f6e49" stroke-dasharray="4 3"/>
<text x="405" y="52" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49" font-weight="bold">feasible</text>
<circle cx="428" cy="128" r="6" fill="#6a3d9a"/>
<line x1="434" y1="128" x2="470" y2="128" stroke="#6a3d9a" stroke-width="1.2" stroke-dasharray="3 3"/>
<text x="474" y="132" text-anchor="start" font-family="sans-serif" font-size="11" fill="#6a3d9a" font-weight="bold">recommend</text>
</svg>
</figure>

Authoritative references for the numbers above: the [H3 resolution table](https://h3geo.org/docs/core-library/restable/) and the broader [H3 documentation](https://h3geo.org/docs/).
