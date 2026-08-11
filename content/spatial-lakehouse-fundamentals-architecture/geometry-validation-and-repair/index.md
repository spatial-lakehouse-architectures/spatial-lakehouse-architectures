# Geometry Validation and Repair in Spatial Lakehouses

An invalid geometry does not raise an error. It sits in the table, participates in joins, contributes to aggregates, and returns answers that are wrong in ways nobody notices until a customer disputes a number. This topic covers the validation contract a spatial lakehouse needs at ingest, the repair strategies that are safe to apply automatically, and the ones that require a human — because the difference between those two categories is where most geometry incidents originate.

## Why Invalidity Is a Storage Problem, Not a Rendering One

The instinct is to treat geometric validity as a cartographic concern, something that matters when a shape is drawn. In an analytical lakehouse it is the opposite: an invalid polygon renders perfectly and computes incorrectly. A self-intersecting ring has no well-defined interior, so `ST_Contains` against it returns whatever the underlying library's traversal happens to produce, and two engines using different library versions will disagree.

<figure class="diagram">
<svg viewBox="0 0 740 282" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Four invalid polygon shapes and their consequences: a bowtie with self intersection, a ring with a repeated vertex, a hole outside its shell, and unclosed rings, each with the predicate behaviour it produces">
<rect x="0" y="0" width="740" height="282" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Four ways a polygon is invalid, and what each breaks</text>
<path d="M60 70 L170 70 L60 150 L170 150 Z" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="115" y="176" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">bowtie</text>
<text x="115" y="198" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">self-intersection</text>
<text x="115" y="222" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">area is engine-dependent</text>
<path d="M230 70 L340 70 L340 150 L285 150 L285 150 L230 150 Z" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<circle cx="285" cy="150" r="5" fill="#0e6e7d"/>
<text x="285" y="176" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">repeated vertex</text>
<text x="285" y="198" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">zero-length segment</text>
<text x="285" y="222" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">breaks some overlay ops</text>
<path d="M400 70 h110 v80 h-110 z" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<path d="M524 100 h56 v40 h-56 z" fill="#f7fbfc" stroke="#2f6e49" stroke-width="2" stroke-dasharray="5 4"/>
<line x1="510" y1="120" x2="524" y2="120" stroke="#2f6e49" stroke-width="1.5" stroke-dasharray="3 3"/>
<text x="470" y="176" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">hole outside shell</text>
<text x="470" y="198" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">ring not contained</text>
<text x="470" y="222" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">area can go negative</text>
<path d="M600 70 L710 70 L710 150 L600 150" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="655" y="176" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">unclosed ring</text>
<text x="655" y="198" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">first ≠ last vertex</text>
<text x="655" y="222" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">rejected on read by some</text>
<text x="390" y="266" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">All four are storable, most are queryable, and none announces itself</text>
</svg>
</figure>

The bowtie is the archetype. Its two lobes have opposite winding, so an area calculation may return the difference rather than the sum — and depending on the implementation, a negative number. A pipeline that sums areas across a region and gets a plausible total has no way to know that one feature contributed a negative value. This is why validity belongs at the write boundary rather than in a downstream check: once the bad geometry is in the table, every consumer inherits the ambiguity.

The repeated-vertex case is more common and less dramatic. Many operations tolerate it; overlay operations sometimes do not, and the failure appears as an exception deep inside a join with a message about topology. Because it is produced routinely by simplification and by coordinate rounding, it is worth normalising rather than rejecting.

Holes outside their shell almost always indicate a producer bug or a conversion error, and they are the case where automatic repair is least appropriate — the correct interpretation is genuinely ambiguous, and silently choosing one produces data that looks fine and means something the source did not intend.

## The Validation Contract at Ingest

A workable contract has three tiers, separated by what the pipeline should do rather than by how severe the defect sounds.

<figure class="diagram">
<svg viewBox="0 0 764 244" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three validation tiers at ingest: normalise silently for cosmetic issues, repair and record for recoverable invalidity, and quarantine for ambiguous cases needing a human decision">
<rect x="0" y="0" width="764" height="244" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Three tiers, separated by what the pipeline does next</text>
<rect x="26" y="56" width="230" height="176" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="141" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#2f6e49">normalise silently</text>
<text x="141" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">repeated vertices</text>
<text x="141" y="136" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">ring orientation</text>
<text x="141" y="158" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">unclosed rings</text>
<text x="141" y="180" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">empty to null</text>
<text x="141" y="210" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">no information is lost</text>
<rect x="274" y="56" width="230" height="176" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="389" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0e6e7d">repair and record</text>
<text x="389" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">simple self-intersections</text>
<text x="389" y="136" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">sliver artefacts</text>
<text x="389" y="158" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">near-duplicate vertices</text>
<text x="389" y="180" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">flag the row as repaired</text>
<text x="389" y="210" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">the change must be auditable</text>
<rect x="522" y="56" width="230" height="176" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="637" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#9a5a17">quarantine</text>
<text x="637" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">hole outside shell</text>
<text x="637" y="136" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">area change over 1%</text>
<text x="637" y="158" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">coordinates out of range</text>
<text x="637" y="180" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">repair changed the type</text>
<text x="637" y="210" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">a human decides</text>
</svg>
</figure>

The middle tier's discipline — **repair and record** — is what separates a maintainable pipeline from one that gradually diverges from its source. A repaired geometry is no longer the geometry the provider published, and a reconciliation job comparing hashes will report every repaired row as changed forever unless the repair is recorded. Add a boolean column and a repair-reason column; both compress to almost nothing and make the divergence explainable.

The quarantine criterion worth building first is **area change**. Run the repair, compare the area before and after, and quarantine anything that moved by more than a threshold. A repair that changes area by 0.0001% fixed a numerical artefact; one that changes it by 30% has reinterpreted the geometry, and the reinterpretation may be wrong. This single check catches most of the cases where automatic repair does harm.

```python
# Shapely 2.x. Three-tier validation applied to a batch before the write.
from shapely import (from_wkb, to_wkb, is_valid, make_valid, set_precision,
                     area, is_empty, get_type_id)

AREA_TOLERANCE = 0.01          # 1% — beyond this, a human decides
GRID = 1e-9                    # ~0.1 mm at the equator

def validate_batch(wkb_values):
    kept, repaired, quarantined = [], [], []
    for raw in wkb_values:
        if raw is None:
            kept.append((None, False, None))
            continue
        geom = from_wkb(raw)
        if is_empty(geom):
            kept.append((None, False, "empty_to_null"))
            continue

        geom = set_precision(geom, GRID)          # tier 1: normalise
        if is_valid(geom):
            kept.append((to_wkb(geom), False, None))
            continue

        fixed = make_valid(geom)                  # tier 2: repair
        before, after = area(geom), area(fixed)
        type_changed = get_type_id(fixed) != get_type_id(geom)
        drift = abs(after - before) / before if before else 1.0

        if type_changed or drift > AREA_TOLERANCE:
            quarantined.append((raw, f"drift={drift:.4f} type_changed={type_changed}"))
        else:
            repaired.append((to_wkb(fixed), True, "make_valid"))
    return kept, repaired, quarantined
```

`set_precision` before the validity test is deliberate and does more work than it appears to. Snapping coordinates to a grid finer than the data's real accuracy removes the near-duplicate vertices and micro-slivers that arise from floating-point arithmetic during reprojection, and a large fraction of "invalid" geometries in practice become valid at that step alone — without any interpretation being applied.

## Repair Strategies and What Each One Assumes

`make_valid` is not one algorithm; implementations offer at least two structurally different approaches, and they produce different answers for the same input.

The **structure-preserving** approach tries to keep the input's ring structure, splitting self-intersecting rings into separate valid polygons. A bowtie becomes a multipolygon of two triangles, preserving total area and changing the geometry type. The **linework** approach dissolves the input into its constituent line segments and rebuilds valid polygons from them; a bowtie becomes the same two triangles, but a polygon with an overlapping hole resolves differently.

The consequence for a pipeline is that **the repair method must be pinned and recorded**, exactly like the library version. A table repaired with one method and later reprocessed with another will differ, and the difference will look like a data change. Recording the method alongside the repaired flag makes the reprocessing decision explicit rather than accidental.

The type change deserves special attention because it propagates. A table declared as holding polygons that now contains multipolygons after repair will fail a schema assertion in a downstream consumer that checks geometry types, and the failure will occur far from the repair. Either declare the table as accepting multipolygons from the outset — the usual right answer — or quarantine geometries whose repair changes their type, which is what the code above does.

## Validity Is Not the Only Correctness Property

Validity is a local property of a single geometry. Several equally important properties are relational, and no per-geometry check will find them.

**Topological consistency** across a set of polygons — that administrative boundaries tile their parent without gaps or overlaps — is the property users assume and nobody validates. A gap of a few centimetres between two municipalities means points falling in it belong to neither, and a point-in-polygon join silently loses them. Checking it requires a union operation over the set and a comparison against the parent boundary, which is expensive enough to run on a schedule rather than per batch, and valuable enough to be worth scheduling.

**Coordinate plausibility** is the CRS question covered in [CRS management pipelines](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/crs-management-pipelines/): coordinates within the declared system's range, and within the dataset's expected extent. A valid polygon in the wrong hemisphere is still valid.

**Vertex-count sanity** catches a different failure. A boundary with four hundred thousand vertices is valid, plausible and will become a straggler in every distributed join that touches it. Recording vertex counts as a column at ingest, and alerting on outliers, turns an unexplained job hang into a data-quality ticket. The remedy is usually a simplified copy for analytical use, retaining the exact geometry for reference.

**Duplicate detection** matters more for spatial data than for scalar because the duplicates are rarely exact: two records of the same feature with coordinates differing in the ninth decimal place are not equal by any byte comparison and are the same thing. Hashing the snapped geometry rather than the raw one turns near-duplicates into exact ones and makes deduplication tractable.

## Operating the Quarantine

A quarantine that nobody empties is a deferral rather than a control, and quarantines fill quickly on real data.

Make the quarantine a **table rather than a directory**, with the original bytes, the failure reason, the source identifier and the timestamp. That makes it queryable, which means the common case — a hundred rows all failing for the same reason from the same provider — is one group-by away from being understood, and the fix is one conversation with the provider rather than a hundred individual decisions.

Give it a **service level**: a maximum age before a row is either resolved or explicitly written off. Without one, the quarantine becomes an archive and the data it holds is lost as certainly as if it had been dropped, with the added cost of storing it.

Track **quarantine rate as a metric** alongside the ingest volume. A stable low rate is the normal state of a pipeline consuming real-world data. A step change is a signal about the upstream source that is usually more valuable than the individual rows — it means the provider changed something, and finding out what is easier while the change is recent.

Finally, make **re-ingestion from quarantine a supported path** rather than a manual repair. When the resolution is "the provider fixed it and resent", the quarantined rows should be discarded; when it is "our validator was too strict", the rows should flow through the normal pipeline with the corrected rule. Both are routine, and a quarantine with no exit path makes both awkward enough to be skipped.

The individual techniques — detecting and repairing invalid polygons, handling geometries that cross the antimeridian, and producing simplified analytical copies — are covered in the guides below, each with a runnable implementation and the verification step that proves it worked.

## Where Validation Sits in the Write Path

Placement matters as much as the checks themselves, because the same rule applied at a different stage has a different cost and catches a different set of problems.

<figure class="diagram">
<svg viewBox="0 0 772 246" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Validation placed at four stages of a write path: at the source boundary, after reprojection, before deriving bounding boxes, and as a scheduled audit over committed data, with what each position catches">
<defs>
<marker id="gvr-place-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#0e6e7d"/></marker>
</defs>
<rect x="0" y="0" width="772" height="246" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Four positions, four different catches</text>
<rect x="20" y="70" width="168" height="80" rx="8" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<text x="104" y="98" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">at the source</text>
<text x="104" y="120" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">provider defects</text>
<text x="104" y="138" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">encoding errors</text>
<rect x="208" y="70" width="168" height="80" rx="8" fill="#ffffff" stroke="#0e6e7d" stroke-width="2"/>
<text x="292" y="98" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">after reprojection</text>
<text x="292" y="120" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">slivers created by</text>
<text x="292" y="138" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">the transform itself</text>
<rect x="396" y="70" width="168" height="80" rx="8" fill="#ffffff" stroke="#0e6e7d" stroke-width="2"/>
<text x="480" y="98" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">before deriving</text>
<text x="480" y="120" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">so bbox and cell id</text>
<text x="480" y="138" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">describe valid input</text>
<rect x="584" y="70" width="176" height="80" rx="8" fill="#ffffff" stroke="#6a3d9a" stroke-width="2"/>
<text x="672" y="98" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">scheduled audit</text>
<text x="672" y="120" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">topology across rows</text>
<text x="672" y="138" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">gaps, overlaps, drift</text>
<line x1="188" y1="110" x2="208" y2="110" stroke="#0e6e7d" stroke-width="2" marker-end="url(#gvr-place-arrow)"/>
<line x1="376" y1="110" x2="396" y2="110" stroke="#0e6e7d" stroke-width="2" marker-end="url(#gvr-place-arrow)"/>
<line x1="564" y1="110" x2="584" y2="110" stroke="#0e6e7d" stroke-width="2" marker-end="url(#gvr-place-arrow)"/>
<text x="390" y="200" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#0d3b45">The second position is the one most often missing</text>
<text x="390" y="230" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">Reprojection creates invalidity that was not in the source</text>
</svg>
</figure>

The post-reprojection check is worth arguing for specifically. A transform maps each vertex independently, and two vertices that were distinct in the source can land on the same output coordinate — producing a zero-length segment or a collapsed ring in a geometry that was perfectly valid before. Validating only at the source boundary misses this entirely, and the resulting defect looks like a source problem when it is not.

The third position matters because the derived columns must describe the geometry that is actually stored. Deriving a bounding box from a pre-repair geometry and storing the post-repair one produces a box that may not cover its own geometry, which breaks data skipping in the direction that loses rows rather than the direction that reads extra ones.

The scheduled audit is the only place relational properties can be checked at all, since they are properties of a set rather than of a row. Run it against a snapshot so the result is reproducible, scope it to one layer at a time so it stays affordable, and treat its output as a work queue rather than as an alert — topological defects are rarely urgent and are almost never fixable in the pipeline that found them.

## What to Store Alongside the Geometry

The columns that make validation auditable are small and worth declaring from the first version of the schema, because adding them later means backfilling a value nobody recorded.

A boolean **`geometry_repaired`** flag, a short **`repair_reason`** string, the **`vertex_count`**, and the **`source_geometry_hash`** together answer every question that arises later: whether this row was modified on the way in, why, whether it is pathological in size, and whether it still matches what the provider sent. None of them costs meaningful storage — the flag and the reason compress to nothing on a table where most rows are unrepaired — and together they turn a class of unanswerable questions into lookups.

The hash deserves one implementation note. Hash the **canonicalised** geometry — snapped, with normalised ring orientation and a deterministic starting vertex — rather than the raw bytes, because two byte sequences that describe the same shape are the same feature for every purpose except a byte comparison. A raw-byte hash will report a change every time the writing library's output format shifts, which makes it useless for exactly the reconciliation task it exists to serve.

## Performance: What Validation Actually Costs

The objection to validating at ingest is throughput, and it is worth quantifying rather than assuming, because the measured cost is usually far below what the objection anticipates.

For **point data** the cost is essentially zero. A point is valid by construction, and the check reduces to a null test. Pipelines ingesting telemetry can validate every row without measurable overhead.

For **simple polygons** — administrative boundaries with tens to hundreds of vertices — validity checking runs at hundreds of thousands of geometries per second per core, which is faster than the WKB decode that precedes it and far faster than the object-storage write that follows. On any realistic ingest the check disappears into the I/O.

For **complex polygons** the cost is real and scales with vertex count, because the self-intersection test is not linear. A coastline with a hundred thousand vertices takes milliseconds rather than microseconds, and a batch dominated by such features will notice. This is the case where sampling is a legitimate compromise: validate every geometry below a vertex threshold and sample above it, on the reasoning that large complex geometries come from a small number of authoritative sources whose quality is more stable than a long tail of small ones.

Repair is a different cost profile. `make_valid` on a valid geometry is cheap because it short-circuits; on an invalid one it performs a full overlay computation, which is expensive. Since invalid geometries are a small fraction of most datasets, the aggregate cost is dominated by the check rather than the repair — which is the argument for checking first and repairing only the failures, rather than running the repair unconditionally as a normalisation step.

The measurement worth taking before making any of these decisions is simply the wall-clock split of one representative batch: decode, validate, repair, derive, encode, write. On most pipelines the answer is that validation is under five percent and the write dominates, at which point the throughput objection has been settled empirically and the conversation can move on.

## Related Practice

Validation composes with the rest of the ingest contract rather than standing apart from it. The coordinate-system assertions in [CRS management pipelines](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/crs-management-pipelines/) catch a class of defect that geometric validity cannot see, and the schema assertions in the [schema validation pipeline for geospatial tables](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/lakehouse-maintenance-automation/schema-validation-pipeline-for-geospatial-tables/) catch a third. Running all three at the same boundary, as one gate, means a batch either meets the table's full contract or does not enter — which is a much simpler property to reason about than three independent checks at three stages.

## Readiness Checklist

- [ ] Every geometry is validated before the write, not after
- [ ] Coordinates are snapped to a precision grid finer than the data's real accuracy before the validity test
- [ ] Repairs are applied only within a declared area-change tolerance; anything beyond it is quarantined
- [ ] A repair that changes the geometry type is quarantined rather than accepted silently
- [ ] `geometry_repaired`, `repair_reason`, `vertex_count` and `source_geometry_hash` are columns on the table
- [ ] Validation runs again after reprojection, not only at the source boundary
- [ ] Derived bounding boxes are computed from the geometry that is actually stored
- [ ] The quarantine is a queryable table with an agreed maximum age and a defined exit path
- [ ] Quarantine rate is tracked as a metric and alerted on step changes rather than on absolute volume
- [ ] Topological consistency across related layers is audited on a schedule against a pinned snapshot
- [ ] The repair method and library version are pinned and recorded as table properties
