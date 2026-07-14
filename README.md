<a href="https://www.spatial-lakehouse-architectures.org">
  <img src="https://www.spatial-lakehouse-architectures.org/assets/icons/og-image.png" alt="Spatial Lakehouse Architectures" width="100%">
</a>

# Spatial Lakehouse Architectures

**Production patterns for spatial data on Apache Iceberg and Delta Lake** — partitioning, predicate pushdown, Python integration, query engines, compaction, vacuum, and CI/CD.

### → [www.spatial-lakehouse-architectures.org](https://www.spatial-lakehouse-architectures.org)

A production-focused engineering reference for teams moving geospatial data out of monolithic GIS databases and into open table formats. It documents the contracts that make a spatial lakehouse actually deliver: deterministic geometry serialization (WKB / GeoParquet), partition strategies that align with real query patterns, predicate pushdown that genuinely prunes files, the compute engines that run spatial SQL at scale, and the Python orchestration that keeps it all reproducible.

No vendor pitches, no toy examples — every guide ships runnable code (PySpark, Spark SQL, Trino, DuckDB, PyIceberg, delta-rs), hand-drawn architecture diagrams, and the failure modes and operational checklists that survive contact with petabyte-scale data.

## Who it's for

Data engineers, platform architects, GIS backend developers, and cloud/infrastructure teams who own a geospatial pipeline end-to-end — from object storage and catalog manifests to engine configuration and CI/CD validation gates.

## What it covers

The site is organized into four areas, each with in-depth topic guides and task-focused how-tos:

- **[Spatial Lakehouse Fundamentals & Architecture](https://www.spatial-lakehouse-architectures.org/spatial-lakehouse-fundamentals-architecture/)** — the decoupled storage/catalog/compute model, geometry serialization (WKB, GeoParquet), snapshot and schema-evolution semantics, CRS management, access control, and the Iceberg vs Delta Lake trade-offs that govern a production spatial stack.
- **[Spatial Partitioning & Indexing Strategies](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/)** — hierarchical grids, Z-ordering and Hilbert curves, predicate pushdown mechanics, raster/vector hybrid layouts, and choosing a discrete global grid (H3 / S2 / geohash) for sub-second queries at scale.
- **[Python Ecosystem & Integration Workflows](https://www.spatial-lakehouse-architectures.org/python-ecosystem-integration-workflows/)** — Arrow schemas, PyIceberg, delta-rs, async catalog orchestration, and automated maintenance (compaction, vacuum, schema-validation) that keeps spatial pipelines reproducible and fast.
- **[Spatial Query Engines & Compute Optimization](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/)** — DuckDB, Trino, and Apache Sedona against lakehouse tables: spatial SQL functions, distributed joins, catalog federation, and the benchmarks that decide which engine owns each workload.

## Why it's different

- **Runnable, versioned code.** Iceberg 1.9 / Spark 3.5, DuckDB spatial 1.x, Trino, Sedona, delta-rs — real imports, real configuration, no placeholders.
- **Every hard concept gets a diagram.** Original, hand-authored inline SVGs for data-flow, pruning, and engine-selection decisions — theme-aware and accessible.
- **Decision-first.** Comparison guides (Iceberg vs Delta, GeoParquet vs WKB, H3 vs S2 vs geohash) give you an explicit recommendation per workload, not a feature list.
- **Operations included.** Failure modes, tuning ranges with concrete numbers, and production readiness checklists on every guide.

## Tech

Built as a static site with [Eleventy](https://www.11ty.dev/), hand-written HTML/CSS with no client-side framework, and deployed on Cloudflare Pages. Content is authored in Markdown with custom shortcodes for code highlighting, interactive checklists, and inline SVG diagrams.

```bash
npm install
npm run build     # build the static site into _site/
npm run serve     # local dev server with live reload
```

## Contributing

Corrections, sharper examples, and new production patterns are welcome — open an issue or a pull request describing the scenario and the engine/version it applies to.
