# Security Boundaries for GIS Data

Spatial lakehouses require security boundaries that extend beyond traditional tabular ACLs. Geospatial workloads introduce distinct attack surfaces: coordinate precision leakage, spatial index metadata exposure, and topology inference from aggregated statistics. Within the architectural hierarchy defined by [Spatial Lakehouse Fundamentals & Architecture](/spatial-lakehouse-fundamentals-architecture/), securing GIS datasets demands a layered approach that integrates storage-level isolation, query-time policy enforcement, and format-aware partitioning strategies. This guide details operational configurations for production environments, focusing on how partitioning, indexing, CI/CD workflows, and maintenance routines must adapt to enforce strict security boundaries.

## Spatial Partitioning and Index Isolation

Traditional spatial partitioning (H3, S2, or bounding-box grids) directly impacts security posture. Coarse spatial partitions can inadvertently expose sensitive geometries when query engines push down filters or when spatial statistics are exposed at the catalog level. Security-aligned partitioning requires decoupling spatial indexing from access control boundaries. Partition by jurisdictional codes, data classification tiers, or tenant identifiers (e.g., `security_zone=restricted`, `data_class=pii_geospatial`, `region=NA_EAST`) to ensure storage-level pruning aligns with IAM or Unity Catalog policies.

When spatial indexes like Z-order curves or GeoParquet metadata are applied, verify that index manifest files do not leak coordinate bounds to unauthorized principals. Debug index exposure by auditing catalog metadata:
```sql
DESCRIBE EXTENDED analytics.gis_infrastructure_assets;
-- Inspect the 'statistics' column for raw bounding box values (min_x, max_x, min_y, max_y)
```
If exposed, configure catalog-level metadata masking or switch to partition-level aggregation that only publishes coarse grid references. Always normalize geometries to a consistent CRS (e.g., `EPSG:4326` for global storage, `EPSG:3857` for web rendering) before partitioning to prevent coordinate drift across security zones.

## Format-Specific Security Controls

The choice between Apache Iceberg and Delta Lake dictates how spatial types are serialized, versioned, and secured. Iceberg’s native support for complex types and schema evolution allows for precise column-level masking of geometry fields without breaking downstream consumers. When leveraging [Iceberg Spatial Type Support](/spatial-lakehouse-fundamentals-architecture/iceberg-spatial-type-support/), engineers must configure manifest-level compression and restrict catalog read permissions to metadata-scoped roles. Iceberg’s manifest structure can expose bounding box metadata even when geometry columns are masked at query time, so enforce `iceberg.metadata.compression=snappy` and apply catalog-level ACLs aligned with [Apache Iceberg Security Documentation](https://iceberg.apache.org/docs/latest/spark-security/).

Conversely, Delta Lake relies on Parquet’s native geometry encoding and transaction log management. Delta’s `_delta_log` directory must be explicitly isolated from public read access. Configure Delta tables with `delta.enableChangeDataFeed=true` only for audited roles, and enforce `delta.columnMapping.mode=name` to prevent schema inference attacks. For detailed encoding constraints and transaction log hardening, refer to [Delta Lake Geometry Handling](/spatial-lakehouse-fundamentals-architecture/delta-lake-geometry-handling/). Both formats require explicit Parquet footer encryption when storing high-precision coordinates.

## Row-Level Enforcement and Dynamic Geometry Masking

Query-time security must dynamically adapt geometry precision based on principal roles. Implementing [Implementing row-level security for geospatial datasets](/spatial-lakehouse-fundamentals-architecture/security-boundaries-for-gis-data/implementing-row-level-security-for-geospatial-datasets/) requires runtime functions that reduce coordinate precision or simplify topology before result serialization. Below is a Spark SQL implementation using dynamic masking:
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
        return (round(x, precision), round(y, precision), *(round(z, precision) if z else ()))
    masked = shp_transform(round_coords, geom)
    return masked.wkb

# Apply during write pipeline before committing to Delta/Iceberg
```

## CI/CD Policy Validation and Infrastructure Guardrails

Security boundaries must be codified and validated before deployment. Use Open Policy Agent (OPA) or custom Python validators in CI/CD to enforce partition alignment, CRS consistency, and metadata exposure limits. Example GitHub Actions workflow:
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
          assert table_meta['partition_columns'][0] in policy['allowed_partitions'], 'Partition violates security boundary'
          assert table_meta['srs'] == 'EPSG:4326', 'CRS mismatch detected'
          print('Security validation passed')
          "
      - name: Scan for Coordinate Leakage in Manifests
        run: |
          python scripts/audit_manifest_stats.py --table analytics.gis_assets --threshold 5
```

## Operational Maintenance, Retention, and Troubleshooting

Spatial tables require specialized maintenance routines. Standard `VACUUM` or `OPTIMIZE` operations can inadvertently expose deleted geometries in transaction logs if retention windows are misconfigured. Enforce strict retention policies aligned with compliance requirements: `retention_period=730d` for environmental telemetry, `retention_period=1825d` for cadastral records, and `retention_period=90d` for real-time IoT tracking.

**Troubleshooting Matrix:**
- *Symptom:* Query returns high-precision coordinates despite masking policy.
 *Root Cause:* Predicate pushdown bypasses view-level masking due to direct table access.
 *Fix:* Enforce catalog-level row filters (`spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension`) and disable direct table access for non-admin roles.
- *Symptom:* Spatial index drift causes partition pruning failures.
 *Root Cause:* Mixed CRS across partitions or unaligned Z-order curves.
 *Fix:* Run `ST_Transform(geom, 'EPSG:3857', 'EPSG:4326')` during ingestion and rebuild spatial indexes with `OPTIMIZE ... ZORDER BY (security_zone, region)`.
- *Symptom:* GeoParquet metadata leaks bounding boxes in catalog UI.
 *Root Cause:* Unencrypted Parquet footers and exposed `geo` schema.
 *Fix:* Enable footer encryption (`parquet.encryption.footer.key`), strip `geo` metadata via `pyarrow.parquet.write_table(..., metadata=None)`, and restrict catalog `DESCRIBE` privileges.

For authoritative guidance on spatial metadata standards and coordinate precision handling, consult the [OGC GeoParquet Specification v1.0.0](https://geoparquet.org/releases/v1.0.0/).