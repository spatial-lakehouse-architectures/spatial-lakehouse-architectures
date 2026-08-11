# Security Boundaries for GIS Data

Spatial lakehouses require security boundaries that extend beyond traditional tabular ACLs. Geospatial workloads introduce distinct attack surfaces: coordinate precision leakage, spatial index metadata exposure, and topology inference from aggregated statistics. Within the architectural hierarchy defined by [Spatial Lakehouse Fundamentals & Architecture](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/), securing GIS datasets demands a layered approach that integrates storage-level isolation, query-time policy enforcement, and format-aware partitioning strategies. This guide details operational configurations for production environments, focusing on how partitioning, indexing, CI/CD workflows, and maintenance routines must adapt to enforce strict security boundaries.

## Spatial Partitioning and Index Isolation

Traditional spatial partitioning (H3, S2, or bounding-box grids) directly impacts security posture. Coarse spatial partitions can inadvertently expose sensitive geometries when query engines push down filters or when spatial statistics are exposed at the catalog level. Security-aligned partitioning requires decoupling spatial indexing from access control boundaries. Partition by jurisdictional codes, data classification tiers, or tenant identifiers (e.g., `security_zone=restricted`, `data_class=pii_geospatial`, `region=NA_EAST`) to ensure storage-level pruning aligns with IAM or Unity Catalog policies.

When spatial indexes like Z-order curves or GeoParquet metadata are applied, verify that index manifest files do not leak coordinate bounds to unauthorized principals. Debug index exposure by auditing catalog metadata:
```sql
DESCRIBE EXTENDED analytics.gis_infrastructure_assets;
-- Inspect the 'statistics' section for raw bounding box values (min_x, max_x, min_y, max_y)
-- If exposed, switch to partition-level aggregation using coarse grid references
```
Always normalize geometries to a consistent CRS (e.g., `EPSG:4326` for global storage) before partitioning to prevent coordinate drift across security zones.

## Format-Specific Security Controls

The choice between Apache Iceberg and Delta Lake dictates how spatial types are serialized, versioned, and secured. Iceberg's native support for complex types and schema evolution allows for precise column-level masking of geometry fields without breaking downstream consumers. When leveraging [Iceberg Spatial Type Support](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/iceberg-spatial-type-support/), engineers must restrict catalog read permissions to metadata-scoped roles. Iceberg's manifest structure can expose bounding box metadata even when geometry columns are masked at query time, so apply catalog-level ACLs aligned with [Apache Iceberg Security Documentation](https://iceberg.apache.org/docs/latest/spark-security/).

Delta Lake relies on Parquet's native geometry encoding and transaction log management. Delta's `_delta_log` directory must be explicitly isolated from public read access. Configure Delta tables with `delta.enableChangeDataFeed=true` only for audited roles, and enforce `delta.columnMapping.mode=name` to prevent schema inference attacks. For detailed encoding constraints and transaction log hardening, refer to [Delta Lake Geometry Handling](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/delta-lake-geometry-handling/). Both formats require explicit Parquet footer encryption when storing high-precision coordinates.

## Row-Level Enforcement and Dynamic Geometry Masking

Query-time security must dynamically adapt geometry precision based on principal roles. Implementing [Implementing row-level security for geospatial datasets](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/security-boundaries-for-gis-data/implementing-row-level-security-for-geospatial-datasets/) requires runtime functions that reduce coordinate precision or simplify topology before result serialization. Below is a Spark SQL implementation using dynamic masking:
```sql
CREATE OR REPLACE VIEW secured.gis_assets_masked AS
SELECT
  asset_id,
  CASE
    WHEN current_role() IN ('admin', 'gis_lead') THEN geometry
    WHEN current_role() = 'contractor' THEN ST_ReducePrecision(geometry, 3) -- ~111m precision
    ELSE ST_Centroid(ST_Simplify(geometry, 0.01)) -- Fallback to simplified centroid
  END AS geometry,
  metadata
FROM raw.gis_assets
WHERE security_zone = current_user_zone();
```

For Python-based validation pipelines, use `shapely` and `pyarrow` to enforce precision thresholds before writing to the lakehouse:
```python
import pyarrow.parquet as pq
from shapely import wkb
from shapely.ops import transform as shp_transform

def mask_geometry_precision(wkb_bytes: bytes, precision: int = 4) -> bytes:
    geom = wkb.loads(wkb_bytes)

    def round_coords(x, y, z=None):
        if z is not None:
            return (round(x, precision), round(y, precision), round(z, precision))
        return (round(x, precision), round(y, precision))

    masked = shp_transform(round_coords, geom)
    return masked.wkb

# Apply during write pipeline before committing to Delta/Iceberg
```

## The Threat Model Specific to Geospatial Data

Generic data governance treats a column as sensitive or not. Geospatial data breaks that model, because a coordinate is rarely sensitive in isolation and frequently sensitive in combination — and because precision, not presence, is usually the control that matters.

<figure class="diagram">
<svg viewBox="0 0 758 312" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Four geospatial disclosure risks arranged by the control that mitigates each: precision reduction for re-identification, row filtering for jurisdictional access, aggregation thresholds for small-count inference, and query auditing for reconnaissance through repeated bounded queries">
<rect x="0" y="0" width="758" height="312" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Four disclosure risks, four different controls</text>
<rect x="34" y="60" width="348" height="112" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="208" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">Re-identification by precision</text>
<text x="208" y="110" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">a &#8220;anonymous&#8221; trip endpoint at 7 decimal</text>
<text x="208" y="126" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">places is a specific front door</text>
<text x="208" y="152" text-anchor="middle" font-family="sans-serif" font-size="11" font-weight="700" fill="#9a5a17">control: truncate coordinates / snap to grid</text>
<rect x="398" y="60" width="348" height="112" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="572" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">Cross-jurisdiction access</text>
<text x="572" y="110" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">an analyst entitled to one region</text>
<text x="572" y="126" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">queries the whole continent</text>
<text x="572" y="152" text-anchor="middle" font-family="sans-serif" font-size="11" font-weight="700" fill="#0e6e7d">control: row filter by spatial containment</text>
<rect x="34" y="188" width="348" height="112" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="208" y="214" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">Small-count inference</text>
<text x="208" y="238" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">an aggregate over a cell containing</text>
<text x="208" y="254" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">one household reveals that household</text>
<text x="208" y="280" text-anchor="middle" font-family="sans-serif" font-size="11" font-weight="700" fill="#2f6e49">control: minimum-count suppression</text>
<rect x="398" y="188" width="348" height="112" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="572" y="214" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">Reconnaissance by iteration</text>
<text x="572" y="238" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">many small permitted queries reassemble</text>
<text x="572" y="254" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">a map nobody was permitted to see</text>
<text x="572" y="280" text-anchor="middle" font-family="sans-serif" font-size="11" font-weight="700" fill="#6a3d9a">control: audit the predicate, not just the table</text>
</svg>
</figure>

**Precision is the primary control, and it is continuous rather than binary.** A coordinate rounded to three decimal places locates a feature to roughly a hundred metres; at five places it is a metre; at seven it is a specific doorway. Publishing "anonymised" movement data at full precision does not anonymise anything, because a home location plus a workplace location identifies almost everyone uniquely. The correct control is to define, per dataset and per audience, the coarsest precision that still answers the question, and to enforce it in the view rather than trusting the consumer.

**Containment is the natural expression of entitlement.** Spatial permissions are almost never "these rows" and almost always "this area" — a regulator entitled to one state, a contractor entitled to one site, a partner entitled to their own service territory. Expressing that as a spatial predicate against an entitlement table keeps the policy readable and keeps it correct when the underlying data changes, which a list of row identifiers does not.

**Aggregation does not automatically protect.** A count per grid cell looks safe until a cell contains one property, at which point the aggregate is the record. Minimum-count suppression — returning null for any cell below a threshold — is the standard mitigation, and the threshold has to account for the cell size: five is adequate at kilometre resolution and inadequate at ten metres.

**Repeated permitted queries are an attack.** Each query respects the row filter; the union of two hundred of them reconstructs what the filter existed to prevent. This is the risk that table-level audit logs miss entirely, because every individual query was authorised. Logging the spatial predicate alongside the query — the bounding box, the cell list, the buffer distance — is what makes the pattern visible.

## Enforcing Containment Without Destroying Performance

The obvious implementation of spatial row-level security is also the slowest one: a policy function that runs `ST_Contains` between each row's geometry and the user's entitlement polygon. On a table of any size this defeats every pruning mechanism, because the predicate is opaque to the planner and must be evaluated after decode on every row.

The performant implementation pushes the entitlement into the same numeric space the rest of the table is optimised for. Resolve the user's entitlement to a set of grid cells once, at session or policy-evaluation time, and filter on the partition column. The engine then prunes files before reading them, and the expensive geometric test runs only where it changes the answer — at the boundary of the entitlement area.

```sql
-- Trino. Entitlement resolved to grid cells, then refined only at the boundary.
CREATE VIEW secure.telemetry AS
WITH grants AS (
  SELECT cell_id, is_partial
  FROM governance.entitlement_cells
  WHERE principal = current_user
)
SELECT t.asset_id, t.event_ts, t.geom_wkb
FROM lakehouse.spatial.telemetry t
JOIN grants g ON t.h3_r5 = g.cell_id
WHERE NOT g.is_partial                      -- interior cells: no geometry test needed
   OR ST_Contains(                          -- boundary cells only
        (SELECT ST_Union(area_geom) FROM governance.entitlements
          WHERE principal = current_user),
        ST_GeomFromBinary(t.geom_wkb));
```

The `is_partial` flag is what makes this fast. Cells wholly inside the entitlement area need no geometric evaluation at all — membership in the cell is proof of containment — and typically account for 95% or more of the matching data. Only cells that straddle the boundary require the exact test, and there are few of them by construction. Precomputing the cell decomposition per entitlement, and refreshing it when entitlements change rather than when queries run, moves the entire cost off the query path.

Two correctness details matter. The decomposition must be **conservative**: a cell marked as fully contained that is not fully contained leaks data, so the containment test used to build the table must use the cell's exact boundary rather than its centroid. And the decomposition must be **invalidated on entitlement change**, which means the entitlement table needs a version and the view needs to reference the current one — otherwise a revoked permission remains effective until the next refresh.

## Auditing What Was Actually Asked

Table-level access logs answer "who read this table" and are close to useless for geospatial governance, because the sensitive fact is usually *which area* was read. An audit trail worth having records the spatial extent of each query alongside the identity and the timestamp.

Most engines can be made to emit this. Trino's event listener receives the full query text and the resolved plan; Spark's query execution listener sees the physical plan including pushed filters; Databricks system tables record the statement. Extracting a bounding box from the plan is a small parsing job and only needs to be approximately right — the purpose is anomaly detection, not forensic reconstruction.

What to do with the extents is where the value is. Three signals repay the effort. **Extent growth**: a principal whose queried area expands steadily over weeks is either onboarding a new region legitimately, or enumerating. **Precision escalation**: a shift from thousand-metre aggregates to individual features is worth a question regardless of entitlement. **Coverage ratio**: the fraction of a principal's entitled area that they have actually queried in a period; a ratio approaching one for a principal whose job involves a handful of sites is the clearest single indicator of bulk extraction.

None of these is conclusive alone, and all of them produce false positives during legitimate bulk work. Treat them as review triggers with a human in the loop rather than as automatic blocks, and tune the thresholds against a quarter of real traffic before enabling alerts. An audit system that pages three times a week will be disabled within a month, and an audit system that never fires is indistinguishable from not having one — the discipline is to calibrate against observed behaviour, then revisit after any change in entitlement scope.


## CI/CD Policy Validation and Infrastructure Guardrails

Security boundaries must be codified and validated before deployment. Use Open Policy Agent (OPA) or custom Python validators in CI/CD to enforce partition alignment, CRS consistency, and metadata exposure limits:
```yaml
name: Validate Spatial Security Boundaries
on: [pull_request]
jobs:
  validate-gis-security:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Check Partition Alignment & CRS
        run: |
          python -c "
          import json, sys
          policy = json.load(open('security/policy.json'))
          table_meta = json.load(open('table_metadata.json'))
          assert table_meta['partition_columns'][0] in policy['allowed_partitions'], \
              'Partition violates security boundary'
          assert table_meta['srs'] == 'EPSG:4326', 'CRS mismatch detected'
          print('Security validation passed')
          "
      - name: Scan for Coordinate Leakage in Manifests
        run: |
          python scripts/audit_manifest_stats.py --table analytics.gis_assets --threshold 5
```

## Precision Masking as a Graduated Control

The strongest argument for treating precision as the primary geospatial control is that it is graduated: the same table can serve four audiences at four resolutions, from one source of truth, with no copies to keep in sync.

<figure class="diagram">
<svg viewBox="0 0 772 286" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Four audience tiers reading one table through views of decreasing coordinate precision: exact geometry for the data owner, ten metre snapping for internal analysts, cell aggregation for partners, and suppressed low-count cells for public release">
<rect x="0" y="0" width="772" height="286" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">One table, four resolutions, no duplicate copies</text>
<rect x="290" y="52" width="200" height="48" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="390" y="82" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">base table (exact WKB)</text>
<rect x="24" y="150" width="172" height="92" rx="8" fill="#ffffff" stroke="#0e6e7d" stroke-width="2"/>
<text x="110" y="176" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">data owner</text>
<text x="110" y="198" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">full precision</text>
<text x="110" y="222" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">~1 cm</text>
<rect x="212" y="150" width="172" height="92" rx="8" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<text x="298" y="176" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">internal analyst</text>
<text x="298" y="198" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">snapped to grid</text>
<text x="298" y="222" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">~10 m</text>
<rect x="400" y="150" width="172" height="92" rx="8" fill="#ffffff" stroke="#9a5a17" stroke-width="2"/>
<text x="486" y="176" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">partner</text>
<text x="486" y="198" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">cell aggregate</text>
<text x="486" y="222" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">~500 m</text>
<rect x="588" y="150" width="172" height="92" rx="8" fill="#ffffff" stroke="#6a3d9a" stroke-width="2"/>
<text x="674" y="176" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">public</text>
<text x="674" y="198" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">counts &lt; 5 suppressed</text>
<text x="674" y="222" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">~5 km</text>
<line x1="340" y1="100" x2="140" y2="150" stroke="#0e6e7d" stroke-width="2"/>
<line x1="370" y1="100" x2="300" y2="150" stroke="#0e6e7d" stroke-width="2"/>
<line x1="410" y1="100" x2="480" y2="150" stroke="#0e6e7d" stroke-width="2"/>
<line x1="440" y1="100" x2="650" y2="150" stroke="#0e6e7d" stroke-width="2"/>
<text x="390" y="270" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">Each tier is a view; entitlement selects the view, not the table</text>
</svg>
</figure>

Implementing the tiers as views rather than as materialised copies matters for a reason beyond storage. A copy is a snapshot of a policy decision, and it goes stale the moment either the data or the policy changes — which means a revoked entitlement leaves a coarse copy sitting in a bucket that someone still has access to. A view re-evaluates on every query, so a policy change takes effect immediately and there is nothing to clean up.

Snapping rather than rounding is the correct primitive for the middle tiers. Rounding coordinates independently distorts geometry — a polygon can self-intersect after its vertices are rounded — whereas snapping to a grid and then repairing keeps the geometry valid. For polygon data, simplification with a tolerance matched to the target resolution is better still, because it reduces vertex count at the same time and makes the coarse tiers genuinely cheaper to query.

The last tier needs a mechanism the others do not: **suppression must be applied after aggregation, and the suppressed cells must not be inferable from the ones that remain**. A public map showing counts for 400 cells and blanks for 12 tells a determined reader that those 12 have between one and four features, which is frequently enough. Where that matters, suppress a random additional margin of cells, or publish a jittered count rather than a blank.

## Where Enforcement Should Live

A policy can be enforced at four layers, and each catches a different set of bypasses. Choosing one is a common mistake; the layers are complementary rather than alternative.

**Storage-layer permissions** — bucket policies, key prefixes, KMS grants — are the only control that survives an engine being bypassed entirely. Somebody with the object path and credentials reads Parquet directly, and no view or row filter is involved. This layer should be the narrowest: engines get access, humans do not.

**Catalog permissions** decide who can resolve a table at all, and are the right place for coarse tenancy boundaries. They are also the layer most likely to be inconsistent across engines, because each engine's catalog integration maps roles differently; test each engine's behaviour rather than assuming the catalog is authoritative everywhere.

**View and row-filter policies** carry the spatial logic, because they are the only layer that can express containment. They are also the layer users interact with, so their performance characteristics matter — a filter that defeats pruning will be routed around by someone who needs their query to finish.

**Audit** is not prevention, and it is the layer that catches what the others structurally cannot: legitimate access at illegitimate scale. It is also the only layer that produces evidence after the fact, which is what a regulator will ask for.

The arrangement that holds up is narrow storage permissions, coarse catalog tenancy, spatial logic in views, and audit on the predicate. Each layer is simple enough to reason about alone, and the failure of any one of them does not open the whole surface.

## The Order in Which Filters and Pruning Are Applied

A security predicate and a performance predicate compete for the same position in a query plan, and which one the optimiser applies first decides both correctness and cost.

<figure class="diagram">
<svg viewBox="0 0 776 216" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Query plan ordering: the entitlement cell join is applied at the scan so file pruning still works, followed by row filtering and only then the exact geometry containment test at the entitlement boundary">
<defs>
<marker id="sec-ord-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="#0e6e7d"/></marker>
</defs>
<rect x="0" y="0" width="776" height="216" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Entitlement first, as a numeric predicate — not last, as geometry</text>
<rect x="24" y="76" width="164" height="76" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="106" y="104" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">entitlement cells</text>
<text x="106" y="126" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">resolved per principal</text>
<rect x="216" y="76" width="164" height="76" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="298" y="104" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">file pruning</text>
<text x="298" y="126" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">partition = cell id</text>
<rect x="408" y="76" width="164" height="76" rx="8" fill="#ffffff" stroke="#0e6e7d" stroke-width="2"/>
<text x="490" y="104" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">user predicate</text>
<text x="490" y="126" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">time, attributes, bbox</text>
<rect x="600" y="76" width="164" height="76" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="682" y="104" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">boundary refine</text>
<text x="682" y="126" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">ST_Contains, few rows</text>
<line x1="188" y1="114" x2="216" y2="114" stroke="#0e6e7d" stroke-width="2" marker-end="url(#sec-ord-arrow)"/>
<line x1="380" y1="114" x2="408" y2="114" stroke="#0e6e7d" stroke-width="2" marker-end="url(#sec-ord-arrow)"/>
<line x1="572" y1="114" x2="600" y2="114" stroke="#0e6e7d" stroke-width="2" marker-end="url(#sec-ord-arrow)"/>
<text x="390" y="200" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#3d5a63">Reversing this order is safe but slow; omitting the last stage is fast and wrong</text>
</svg>
</figure>

Verify the ordering in the plan rather than trusting it. An optimiser that pushes the user's bounding-box predicate below the entitlement join is still correct — both are conjunctive filters — but one that evaluates the exact containment test before pruning has turned a governed query into a full scan, and the symptom is a security feature being blamed for a layout problem.

## Operational Maintenance, Retention, and Troubleshooting

Spatial tables require specialized maintenance routines. Standard `VACUUM` or `OPTIMIZE` operations can inadvertently expose deleted geometries in transaction logs if retention windows are misconfigured. Enforce strict retention policies aligned with compliance requirements: `retention_period=730d` for environmental telemetry, `retention_period=1825d` for cadastral records, and `retention_period=90d` for real-time IoT tracking.

**Troubleshooting Matrix:**
- *Symptom:* Query returns high-precision coordinates despite masking policy.
  *Root Cause:* Predicate pushdown bypasses view-level masking due to direct table access.
  *Fix:* Enforce catalog-level row filters and disable direct table access for non-admin roles. In Databricks, use Unity Catalog row filters; in open-source deployments, use Apache Ranger or OPA Gatekeeper.
- *Symptom:* Spatial index drift causes partition pruning failures.
  *Root Cause:* Mixed CRS across partitions or unaligned Z-order curves.
  *Fix:* Standardize to a single CRS during ingestion (`ST_Transform` to `EPSG:4326`) and rebuild spatial indexes with `OPTIMIZE ... ZORDER BY (security_zone, region)`.
- *Symptom:* GeoParquet metadata leaks bounding boxes in catalog UI.
  *Root Cause:* Unencrypted Parquet footers and exposed `geo` schema.
  *Fix:* Enable footer encryption (`parquet.encryption.footer.key`), strip `geo` metadata via `pyarrow.parquet.write_table(..., schema=schema_without_geo_metadata)`, and restrict catalog `DESCRIBE` privileges.

For authoritative guidance on spatial metadata standards and coordinate precision handling, consult the [OGC GeoParquet Specification v1.1.0](https://geoparquet.org/releases/v1.1.0/).
