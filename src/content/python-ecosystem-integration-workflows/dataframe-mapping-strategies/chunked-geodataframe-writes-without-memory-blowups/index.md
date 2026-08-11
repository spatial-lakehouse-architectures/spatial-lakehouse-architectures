# Chunked GeoDataFrame Writes Without Memory Blowups

This guide writes a GeoDataFrame larger than memory to a lakehouse table, keeping peak memory flat regardless of input size and producing files with tight spatial statistics rather than fragments that prune for nobody.

## Context and prerequisites

The naive path — load the whole frame, convert, write — has a memory profile several times the input size, because shapely geometry objects are far larger than their serialised form. This recipe uses GeoPandas 1.0, Shapely 2.x and PyArrow 15+; the schema discipline it applies is in [mapping GeoPandas dataframes to Arrow schemas](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/dataframe-mapping-strategies/mapping-geopandas-dataframes-to-arrow-schemas/), and the wider mapping context in [dataframe mapping strategies](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/dataframe-mapping-strategies/).

## Where the memory goes

<figure class="diagram">
<svg viewBox="0 0 732 238" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Memory profile of a whole frame conversion showing the source frame, the geometry object graph, the Arrow buffers and the Parquet write buffer all live at once, against a chunked profile where only one chunk is live">
<rect x="0" y="0" width="732" height="238" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Four copies live at once, or one</text>
<text x="196" y="62" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#9a5a17">whole-frame</text>
<rect x="60" y="76" width="270" height="30" fill="#f2e8da" stroke="#9a5a17" stroke-width="1.5"/>
<text x="195" y="97" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">source frame</text>
<rect x="60" y="108" width="270" height="34" fill="#f2e8da" stroke="#9a5a17" stroke-width="1.5"/>
<text x="195" y="131" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">shapely object graph — the largest</text>
<rect x="60" y="144" width="270" height="26" fill="#f2e8da" stroke="#9a5a17" stroke-width="1.5"/>
<text x="195" y="163" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">Arrow buffers</text>
<rect x="60" y="172" width="270" height="22" fill="#f2e8da" stroke="#9a5a17" stroke-width="1.5"/>
<text x="195" y="188" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">write buffer</text>
<text x="195" y="222" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">peak ≈ 4–8× the file size</text>
<text x="584" y="62" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#2f6e49">chunked</text>
<rect x="450" y="76" width="270" height="118" fill="none" stroke="#cfe3e7" stroke-width="1.5" stroke-dasharray="4 4"/>
<rect x="450" y="150" width="60" height="44" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="584" y="176" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">one chunk live at a time</text>
<text x="584" y="222" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">peak ≈ one chunk, whatever the input</text>
</svg>
</figure>

The second band is the one that surprises people. A shapely polygon carries Python object overhead per geometry and per coordinate array, so a column that serialises to 2 GB of WKB can occupy 10 GB as objects. Converting the whole frame therefore fails at input sizes well below what the machine's memory would suggest.

## Complete working solution

```python
import geopandas as gpd
import pyarrow as pa
import pyarrow.parquet as pq
from shapely import to_wkb, bounds, force_2d, set_precision

CHUNK_BYTES = 256 * 1024 * 1024        # target serialised size per chunk
SCHEMA = pa.schema([
    ("feature_id",  pa.int64()),
    ("category",    pa.string()),
    ("h3_r7",       pa.int64()),
    ("bbox_min_x",  pa.float64()), ("bbox_min_y", pa.float64()),
    ("bbox_max_x",  pa.float64()), ("bbox_max_y", pa.float64()),
    ("geometry",    pa.binary()),
], metadata={b"geo": GEO_METADATA})

def estimate_rows_per_chunk(gdf: gpd.GeoDataFrame, sample: int = 5000) -> int:
    head = gdf.head(sample)
    wkb = to_wkb(force_2d(head.geometry.values))
    per_row = sum(len(b) for b in wkb) / len(head) + 64      # + attribute overhead
    return max(int(CHUNK_BYTES / per_row), 1000)

def to_arrow(chunk: gpd.GeoDataFrame) -> pa.Table:
    geoms = set_precision(force_2d(chunk.geometry.values), 1e-9)
    mins_maxs = bounds(geoms)                                 # vectorised, one call
    return pa.Table.from_arrays([
        pa.array(chunk["feature_id"].to_numpy(), pa.int64()),
        pa.array(chunk["category"].astype("string"), pa.string()),
        pa.array(chunk["h3_r7"].to_numpy(), pa.int64()),
        pa.array(mins_maxs[:, 0], pa.float64()), pa.array(mins_maxs[:, 1], pa.float64()),
        pa.array(mins_maxs[:, 2], pa.float64()), pa.array(mins_maxs[:, 3], pa.float64()),
        pa.array(to_wkb(geoms), pa.binary()),
    ], schema=SCHEMA)

def write_chunked(gdf: gpd.GeoDataFrame, path: str) -> int:
    gdf = gdf.sort_values("h3_r7")                # spatial coherence per chunk
    rows = estimate_rows_per_chunk(gdf)
    written = 0
    with pq.ParquetWriter(path, SCHEMA, compression="zstd") as writer:
        for start in range(0, len(gdf), rows):
            chunk = gdf.iloc[start:start + rows]
            writer.write_table(to_arrow(chunk))
            written += len(chunk)
            del chunk
    return written
```

## Step-by-step walkthrough

1. **Sort before chunking.** Chunking an arbitrarily-ordered frame gives every row group a bounding box covering the whole extent, so within-file skipping achieves nothing. One sort on the cell column makes each chunk spatially coherent and each row group's statistics tight.

2. **Estimate the chunk size from a sample.** The serialised bytes per row vary by two orders of magnitude between point and polygon data, so a fixed row count produces an unpredictable memory footprint. Measuring five thousand rows takes milliseconds.

3. **Use the vectorised bounds call.** `bounds()` over the whole geometry array returns all four values at once in native code. Iterating geometries to compute them individually is the single largest avoidable cost in this pipeline.

4. **Declare the schema outside the loop.** Every chunk must produce an identical Arrow schema or the write fails partway through, and deriving it from the first chunk works until a chunk has an all-null column.

5. **Force 2D and snap before serialising.** Both are vectorised, both remove a class of downstream inconsistency, and both are far cheaper here than as a later repair pass.

## Common errors and fixes

| Symptom | Cause | Fix |
|---|---|---|
| Memory grows across chunks | References to previous chunks retained | Delete the chunk explicitly; avoid accumulating in a list |
| Write fails on a later chunk | Schema inferred rather than declared | Declare the schema once and cast every chunk to it |
| Files prune poorly despite chunking | Frame not sorted before chunking | Sort on the spatial key first |
| Conversion is far slower than expected | Row-wise `.apply()` for bounds or WKB | Use the array-level functions throughout |
| Peak memory still high | Chunk sized by rows, not by bytes | Estimate bytes per row from a sample |

## Reading the memory profile

<figure class="diagram">
<svg viewBox="0 0 742 246" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Memory over time for a chunked write showing a flat sawtooth as each chunk is created and released, against a growing profile indicating a retained reference">
<rect x="0" y="0" width="742" height="246" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">A flat sawtooth is correct; a rising one is a leak</text>
<line x1="70" y1="200" x2="730" y2="200" stroke="#33707d" stroke-width="1.5"/>
<line x1="70" y1="56" x2="70" y2="200" stroke="#33707d" stroke-width="1.5"/>
<path d="M80 190 L140 120 L142 188 L202 118 L204 190 L264 120 L266 188 L326 118 L328 190 L388 120 L390 188 L450 118"
      fill="none" stroke="#2f6e49" stroke-width="2.5"/>
<text x="250" y="100" font-family="sans-serif" font-size="11" font-weight="700" fill="#2f6e49">correct: flat sawtooth</text>
<path d="M470 186 L520 150 L522 178 L572 132 L574 162 L624 110 L626 142 L676 84 L678 118 L720 70"
      fill="none" stroke="#9a5a17" stroke-width="2.5"/>
<text x="560" y="72" font-family="sans-serif" font-size="11" font-weight="700" fill="#9a5a17">a retained reference</text>
<text x="400" y="230" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">chunks processed &#8594;</text>
</svg>
</figure>

The rising profile is almost always one of three things: chunks accumulated in a list rather than written and released, a closure capturing a chunk, or a writer buffering more row groups than expected. Measuring peak resident memory across a hundred chunks in a test is the cheapest way to catch it, and the test is worth having permanently because the regression is easy to reintroduce.

## Verification

```python
def test_memory_is_flat(tmp_path, big_gdf):
    import tracemalloc
    tracemalloc.start()
    write_chunked(big_gdf, str(tmp_path / "out.parquet"))
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert peak < 2 * CHUNK_BYTES, f"peak {peak/1e6:.0f} MB exceeds two chunks"

def test_row_groups_are_spatially_tight(tmp_path):
    md = pq.ParquetFile(str(tmp_path / "out.parquet")).metadata
    spans = []
    for rg in range(md.num_row_groups):
        col = md.row_group(rg).column(3)          # bbox_min_x
        spans.append(col.statistics.max - col.statistics.min)
    assert max(spans) < 5.0, "row groups span too wide an extent — sort first"
```

The second test is the one that distinguishes a chunked write from a good chunked write. Both produce a valid file; only the sorted one produces row groups a reader can skip. Setting the threshold from the data's own extent — a few percent of it — makes the assertion meaningful rather than arbitrary.

Record the resulting file's row-group count and size distribution after the first run, and compare on subsequent ones. A change in either usually means the input's composition shifted, which is worth knowing before the downstream queries notice.

## Reading Back the Same Way

The reverse direction has the same failure mode and the same remedy, and it is more commonly hit because reading feels cheaper than writing.

Reading a large table into a single GeoDataFrame materialises every geometry as a Python object, which is exactly the expensive band from the first diagram. For a table of ten million features that is several gigabytes of objects on top of the buffers, and the analysis that follows usually did not need geometry objects at all.

The pattern that scales is to **read in batches and materialise geometry only where it is needed**. Arrow's dataset reader yields record batches; attribute work, filtering and aggregation all run on those directly at native speed, and only the rows that survive are converted to shapely for a geometric operation.

```python
import pyarrow.dataset as ds
from shapely import from_wkb

dataset = ds.dataset("s3://lakehouse/boundaries/", format="parquet")
filt = (ds.field("bbox_min_x") <= 13.8) & (ds.field("bbox_max_x") >= 13.0)

total_area = 0.0
for batch in dataset.to_batches(filter=filt, columns=["feature_id", "geometry"]):
    geoms = from_wkb(batch.column("geometry").to_pylist())   # only survivors
    total_area += sum(g.area for g in geoms)
    del geoms
```

Pushing the filter into the reader is what makes this cheap: entire row groups never leave storage, so the batches that arrive are already the relevant ones. A filter applied after loading has already paid the transfer and the decode.

Where a GeoDataFrame genuinely is the required output — a plotting library, a legacy function — build it from the filtered subset rather than from the table, and accept that its size is bounded by the filter rather than by the source.

## Choosing the Chunk Size

Two hundred and fifty-six megabytes is a reasonable default and is not universally right. Three considerations move it.

**Available memory per process.** The peak is roughly two chunks — one being built, one being written — plus the writer's own buffers, so a chunk should be well under half the memory the process may use. In a container with a hard limit, size for the limit rather than for the host.

**Target file size.** Where each chunk becomes a row group and several chunks become a file, the chunk size sets the row-group size, and row groups of 128–256 MB are a good general target. Much smaller row groups inflate footer metadata; much larger ones reduce skipping granularity.

**Sort quality within the chunk.** A larger chunk contains more of the sorted sequence and therefore has tighter bounds relative to the whole file. This argues mildly for larger chunks, and it is the weakest of the three considerations because the sort has already done most of the work.

The practical approach is to start at 256 MB, measure the peak memory and the resulting row-group statistics, and adjust once. It is not a parameter that repays continuous tuning, and a value recorded with its reasoning is worth more than a value optimised to the last percent.

## A Note on Parallelism

Chunked writing parallelises cleanly and the parallelism belongs in processes rather than threads, for the reason that recurs across this section: the conversion is CPU-bound Python and the interpreter lock makes threads useless for it.

The straightforward arrangement is a process pool where each worker takes a slice of the sorted frame, converts it and returns encoded Arrow bytes, and the parent writes them in order. Returning encoded bytes rather than objects across the process boundary avoids a pickle round trip that can cost more than the conversion.

Sorting before splitting matters even more with parallelism than without it, because each worker's slice must be spatially coherent for the resulting row groups to be useful. Splitting an unsorted frame across eight workers produces eight sets of row groups each spanning the full extent.

Where the input is already partitioned on disk — a directory of files, a set of source extracts — the natural unit of parallelism is the source file rather than a slice of a loaded frame, which avoids loading anything into the parent at all. That is the arrangement to prefer when it is available, since it keeps the parent's memory bounded by its bookkeeping rather than by the data.

<figure class="diagram">
<svg viewBox="0 0 762 196" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Parallel chunked writing where each worker process converts a sorted slice and returns encoded Arrow bytes to a parent that writes them in order">
<defs>
<marker id="cgw-par-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#2f6e49"/></marker>
</defs>
<rect x="0" y="0" width="762" height="196" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Sort once, split, convert in parallel, write in order</text>
<rect x="26" y="66" width="180" height="76" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="116" y="96" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">sorted frame</text>
<text x="116" y="120" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">split into slices</text>
<rect x="258" y="46" width="150" height="42" rx="6" fill="#e6f0ea" stroke="#2f6e49" stroke-width="1.5"/>
<text x="333" y="72" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">worker 1</text>
<rect x="258" y="94" width="150" height="42" rx="6" fill="#e6f0ea" stroke="#2f6e49" stroke-width="1.5"/>
<text x="333" y="120" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">worker 2</text>
<rect x="258" y="142" width="150" height="42" rx="6" fill="#e6f0ea" stroke="#2f6e49" stroke-width="1.5"/>
<text x="333" y="168" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">worker 3</text>
<rect x="470" y="86" width="280" height="70" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="610" y="114" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">parent writes row groups in order</text>
<text x="610" y="136" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">receives encoded Arrow, not objects</text>
<line x1="206" y1="90" x2="258" y2="67" stroke="#2f6e49" stroke-width="2" marker-end="url(#cgw-par-arrow)"/>
<line x1="206" y1="104" x2="258" y2="115" stroke="#2f6e49" stroke-width="2" marker-end="url(#cgw-par-arrow)"/>
<line x1="206" y1="118" x2="258" y2="163" stroke="#2f6e49" stroke-width="2" marker-end="url(#cgw-par-arrow)"/>
<line x1="408" y1="115" x2="470" y2="118" stroke="#2f6e49" stroke-width="2" marker-end="url(#cgw-par-arrow)"/>
</svg>
</figure>

Writing in order preserves the sort across row groups, which is what keeps the file's statistics useful. Workers finishing out of order and being written as they complete produces a file whose row groups interleave spatially, undoing most of the benefit of having sorted at all.
