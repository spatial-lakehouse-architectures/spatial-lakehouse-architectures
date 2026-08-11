# Detecting and Fixing Invalid Polygons at Ingest

This guide is a complete, runnable ingest gate that classifies every incoming polygon as clean, repairable or ambiguous, repairs what it safely can, quarantines the rest, and writes an audit column so a later reconciliation can tell which rows were modified and why.

## Context and prerequisites

An invalid polygon returns wrong answers rather than errors, so the only reliable place to deal with it is before the write. This recipe runs on Shapely 2.0 or later with GEOS 3.10+, and PyArrow 14+ for the batch handling; it assumes the incoming batch already carries geometry as WKB, as produced by any of the ingest paths under [Python ecosystem and integration workflows](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/). The classification thresholds it uses are explained in [geometry validation and repair](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/geometry-validation-and-repair/); here the focus is on getting one gate implemented and verified.

## Complete working solution

```python
import pyarrow as pa
from shapely import (from_wkb, to_wkb, is_valid, is_valid_reason, make_valid,
                     set_precision, area, get_type_id, is_empty)

AREA_TOLERANCE = 0.01     # repairs beyond 1% area change go to quarantine
PRECISION_GRID = 1e-9     # ~0.1 mm at the equator, in degrees
VERTEX_ALERT   = 100_000  # geometries above this are flagged, not rejected

def gate_batch(batch: pa.RecordBatch) -> tuple[pa.RecordBatch, pa.RecordBatch]:
    """Split a batch into (accepted, quarantined). Accepted gains audit columns."""
    geoms_wkb, repaired_flag, reason, vertices = [], [], [], []
    q_wkb, q_reason, q_id = [], [], []
    ids = batch.column("feature_id").to_pylist()

    for fid, raw in zip(ids, batch.column("geometry").to_pylist()):
        if raw is None:
            geoms_wkb.append(None); repaired_flag.append(False)
            reason.append(None);    vertices.append(0)
            continue

        geom = from_wkb(raw)
        if is_empty(geom):
            geoms_wkb.append(None); repaired_flag.append(False)
            reason.append("empty_to_null"); vertices.append(0)
            continue

        # Tier 1 — snapping removes float artefacts without reinterpreting anything.
        snapped = set_precision(geom, PRECISION_GRID)
        n = len(to_wkb(snapped)) // 16          # cheap vertex proxy for alerting

        if is_valid(snapped):
            geoms_wkb.append(to_wkb(snapped)); repaired_flag.append(False)
            reason.append(None); vertices.append(n)
            continue

        # Tier 2 — repair, then decide whether the repair is trustworthy.
        why  = is_valid_reason(snapped)
        fixed = make_valid(snapped)
        before, after = area(snapped), area(fixed)
        drift = abs(after - before) / before if before > 0 else 1.0
        type_changed = get_type_id(fixed) != get_type_id(snapped)

        if type_changed or drift > AREA_TOLERANCE:
            q_wkb.append(raw); q_id.append(fid)
            q_reason.append(f"{why} | drift={drift:.5f} | type_changed={type_changed}")
        else:
            geoms_wkb.append(to_wkb(fixed)); repaired_flag.append(True)
            reason.append(why); vertices.append(n)

    accepted = pa.RecordBatch.from_arrays(
        [batch.column("feature_id"), pa.array(geoms_wkb, pa.binary()),
         pa.array(repaired_flag, pa.bool_()), pa.array(reason, pa.string()),
         pa.array(vertices, pa.int32())],
        names=["feature_id", "geometry", "geometry_repaired",
               "repair_reason", "vertex_count"])

    quarantined = pa.RecordBatch.from_arrays(
        [pa.array(q_id, pa.int64()), pa.array(q_wkb, pa.binary()),
         pa.array(q_reason, pa.string())],
        names=["feature_id", "geometry", "quarantine_reason"])

    return accepted, quarantined
```

The accepted batch is written to the table; the quarantined batch is appended to a sibling quarantine table with the same partition key, so an operator can query defects by region and date rather than by file.

## Step-by-step walkthrough

1. **Snap before testing.** `set_precision` collapses coordinates onto a grid finer than the data's real accuracy. A large share of "invalid" geometries in production are invalid only because reprojection produced two vertices a picometre apart; snapping resolves them with no interpretation and no information loss. Testing validity before this step produces a much larger repair queue for no benefit.

2. **Record the reason, not just the fact.** `is_valid_reason` returns a string such as `Self-intersection[13.4 52.5]` including the offending coordinate. Storing it makes a class of defects diagnosable in aggregate — a hundred rows all self-intersecting at the same latitude points at a producer bug rather than at bad luck.

3. **Compare area before and after.** This is the trust test. A repair that preserves area to within a fraction of a percent resolved a numerical artefact; one that changes it substantially has chosen an interpretation, and the interpretation may not be the one the source intended.

4. **Treat a type change as ambiguous.** `make_valid` on a bowtie returns a multipolygon. That is arguably correct and it changes what the row is, so it goes to quarantine rather than into a table whose consumers expect polygons.

5. **Keep the original bytes in quarantine.** Storing the repaired version would defeat the purpose; the operator needs to see what the provider actually sent in order to decide whether to accept, fix upstream, or reject.

## Common errors and fixes

| Symptom | Cause | Fix |
|---|---|---|
| Everything is invalid after reprojection | Transform collapsed near-duplicate vertices | Snap after reprojection, then re-test; most resolve |
| `make_valid` raises on some inputs | Geometry has NaN or infinite coordinates | Filter non-finite coordinates before the repair step |
| Quarantine fills with identical reasons | An upstream producer changed its export | Group by reason and date; fix at the source, then re-ingest |
| Area drift is 1.0 for many rows | Original area was zero — degenerate geometry | Treat zero-area polygons as a separate class; usually they are lines |
| Repaired rows fail a downstream type check | Consumer expects polygons, got multipolygons | Declare the table as accepting multipolygons, or keep quarantining type changes |

## Verification

Confirm the gate behaves on known-bad input before trusting it on real data. A handful of hand-written geometries exercises more of the logic than a large sample.

```python
from shapely import to_wkb
from shapely.geometry import Polygon

BOWTIE   = Polygon([(0, 0), (1, 1), (1, 0), (0, 1), (0, 0)])   # self-intersecting
SLIVER   = Polygon([(0, 0), (1, 0), (1, 1e-12), (0, 0)])       # near-degenerate
GOOD     = Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])

batch = pa.RecordBatch.from_arrays(
    [pa.array([1, 2, 3], pa.int64()),
     pa.array([to_wkb(BOWTIE), to_wkb(SLIVER), to_wkb(GOOD)], pa.binary())],
    names=["feature_id", "geometry"])

accepted, quarantined = gate_batch(batch)
assert quarantined.num_rows == 1                       # the bowtie changes type
assert accepted.column("geometry_repaired").to_pylist() == [False, True]
print(accepted.to_pydict())
```

## How the gate classifies real data

<figure class="diagram">
<svg viewBox="0 0 692 280" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Typical classification outcome of an ingest gate on a national boundary dataset: the large majority clean, a small band repaired by snapping, a smaller band repaired by make_valid, and a handful quarantined">
<rect x="0" y="0" width="692" height="280" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Where a real batch lands</text>
<rect x="60" y="66" width="530" height="46" fill="#e6f0ea" stroke="#2f6e49" stroke-width="1.5"/>
<text x="325" y="95" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">clean on arrival — typically 97–99%</text>
<rect x="60" y="118" width="98" height="46" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="1.5"/>
<text x="230" y="147" font-family="sans-serif" font-size="12" fill="#0d3b45">resolved by snapping alone</text>
<rect x="60" y="170" width="44" height="46" fill="#f2e8da" stroke="#9a5a17" stroke-width="1.5"/>
<text x="230" y="199" font-family="sans-serif" font-size="12" fill="#0d3b45">repaired within tolerance</text>
<rect x="60" y="222" width="14" height="46" fill="#faf8fc" stroke="#6a3d9a" stroke-width="1.5"/>
<text x="230" y="251" font-family="sans-serif" font-size="12" fill="#0d3b45">quarantined for a human</text>
<text x="640" y="255" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#3d5a63">bars to scale</text>
</svg>
</figure>

The proportions matter for capacity planning. Because the quarantine band is small on well-behaved sources, an operator reviewing it weekly is reviewing tens of rows rather than thousands — which is what makes a manual decision affordable. A source that pushes the quarantine band above a fraction of a percent is a source with a systematic problem, and the right response is a conversation with its owner rather than a larger review team.

## What the audit columns buy later

<figure class="diagram">
<svg viewBox="0 0 764 210" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three questions the audit columns answer months later: whether a row was modified on ingest, why it was modified, and whether it still matches the source">
<rect x="0" y="0" width="764" height="210" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Three questions, answerable in one query each</text>
<rect x="26" y="58" width="230" height="140" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="141" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">was it modified?</text>
<text x="141" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">geometry_repaired</text>
<text x="141" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">explains a hash mismatch</text>
<text x="141" y="166" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">against the source</text>
<rect x="274" y="58" width="230" height="140" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="389" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">why?</text>
<text x="389" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">repair_reason</text>
<text x="389" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">groups defects by kind</text>
<text x="389" y="166" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">and by producer</text>
<rect x="522" y="58" width="230" height="140" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="637" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">is it pathological?</text>
<text x="637" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">vertex_count</text>
<text x="637" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">explains a straggler task</text>
<text x="637" y="166" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">in a distributed join</text>
</svg>
</figure>

The third column earns its place in an unrelated investigation. When a nightly join that normally finishes in twelve minutes takes two hours, the first useful query is a maximum over `vertex_count` in the partitions it touched — and a single boundary with four hundred thousand vertices, freshly ingested that week, is the answer often enough to make the column worth having.

Wire the gate into the same job that performs the coordinate-system assertions rather than as a separate stage, so a batch either satisfies the table's whole contract or is rejected as a unit. Splitting the two produces batches that pass one gate and fail the other, which leaves partial data in an ambiguous state that nobody has defined a policy for.

## Tuning the Thresholds

The two numbers in the gate — the precision grid and the area tolerance — should be set from the data rather than copied, and both have a defensible derivation.

<figure class="diagram">
<svg viewBox="0 0 762 234" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Deriving the two gate thresholds: the precision grid from the data's real positional accuracy, and the area tolerance from the smallest change that would matter to a consumer">
<rect x="0" y="0" width="762" height="234" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Both numbers come from the data, not from a default</text>
<rect x="30" y="58" width="352" height="164" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="206" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">precision grid</text>
<text x="206" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">set finer than real accuracy</text>
<text x="206" y="142" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">survey data: 1e-9 degrees (~0.1 mm)</text>
<text x="206" y="164" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">phone GPS: 1e-7 degrees (~1 cm)</text>
<text x="206" y="186" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">digitised maps: 1e-6 (~10 cm)</text>
<text x="206" y="210" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">too fine wastes the benefit</text>
<rect x="398" y="58" width="352" height="164" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="574" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">area tolerance</text>
<text x="574" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">set from what a consumer would notice</text>
<text x="574" y="142" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">cadastral: 0.001 (parcels are legal)</text>
<text x="574" y="164" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">administrative: 0.01</text>
<text x="574" y="186" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">coverage extents: 0.05</text>
<text x="574" y="210" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">too loose accepts reinterpretation</text>
</svg>
</figure>

Setting the grid finer than the data's real accuracy is not a conservative choice — it is a wasted one. A grid at 1e-12 degrees preserves noise that has no physical meaning and leaves in place exactly the near-duplicate vertices the snapping step exists to remove. The right value is the coarsest one that does not move a coordinate further than its own uncertainty, and for most datasets that is several orders of magnitude coarser than the double precision the values are stored in.

The area tolerance is a business decision wearing a numerical disguise. For parcel boundaries with legal significance, a one percent area change is enormous and the correct tolerance is nearer a tenth of a percent, with more rows going to a human. For a coverage extent used to decide which satellite scenes to fetch, five percent is immaterial and a tighter tolerance just generates review work. Set it per table, record it in the table properties, and revisit it when the consumer set changes.

Both values belong in configuration rather than in the code, because the same gate serves several tables with different requirements — and because an incident is a poor moment to redeploy a pipeline in order to loosen a threshold by a factor of two.

## Running It at Scale

The gate as written processes one batch in a single process, which is the right shape for it: the work is CPU-bound and embarrassingly parallel, so it belongs in a worker pool rather than on an event loop. Size batches by serialised bytes rather than row count, because a batch of complex boundaries occupies far more memory than the same number of points, and return encoded Arrow rather than Python objects across the process boundary to avoid a pickle round trip that can exceed the validation cost itself.

For datasets large enough to need distribution, the same logic runs unchanged inside a Spark or Sedona job as a pandas UDF over the geometry column. Nothing about the classification is order-dependent or stateful, so partitioning is free — the only coordination needed is that the quarantined rows from every task land in the same quarantine table, which an ordinary append handles.

Whichever runtime is used, emit the four counts — clean, snapped, repaired, quarantined — as job metrics rather than only as log lines. The ratio between them is the health signal that tells you a source has changed, and it is far more useful as a time series than as a number somebody reads once during an incident. The wider practice around that monitoring is covered in [spatial data observability](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/spatial-data-observability/).
