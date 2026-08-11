module.exports = {
  name: "Spatial Lakehouse Architectures",
  shortName: "Spatial Lakehouse",
  url: "https://www.spatial-lakehouse-architectures.org",
  description:
    "A production-focused resource for implementing, optimizing, and maintaining spatial data in open table formats (Apache Iceberg, Delta Lake).",
  tagline:
    "Production patterns for spatial data on Apache Iceberg and Delta Lake — partitioning, predicate pushdown, Python integration, compaction, vacuum, and CI/CD.",
  audience:
    "Data engineers, platform architects, GIS backend developers, and cloud/infrastructure teams.",
  themeColor: "#0e6e7d",
  backgroundColor: "#f6f3ec",
  // Hand-picked entry points for the homepage "Start here" grid.
  featured: [
    "/spatial-lakehouse-fundamentals-architecture/geoparquet-encoding-standards/geoparquet-vs-wkb-column-storage-trade-offs/",
    "/spatial-lakehouse-fundamentals-architecture/open-table-format-versioning/iceberg-vs-delta-lake-for-spatial-data/",
    "/spatial-partitioning-indexing-strategies/grid-system-selection/h3-vs-s2-vs-geohash-for-lakehouse-partitioning/",
    "/spatial-partitioning-indexing-strategies/predicate-pushdown-optimization/materializing-bbox-columns-for-pushdown/",
    "/spatial-partitioning-indexing-strategies/spatial-join-optimization/choosing-between-broadcast-and-partitioned-spatial-joins/",
    "/spatial-query-engines-compute/duckdb-geospatial-analytics/how-to-run-st-intersects-in-duckdb-on-geoparquet/",
    "/python-ecosystem-integration-workflows/streaming-spatial-ingestion/writing-kafka-geospatial-streams-to-iceberg/",
    "/python-ecosystem-integration-workflows/lakehouse-maintenance-automation/compacting-spatial-iceberg-tables-with-rewrite-data-files/",
    "/spatial-query-engines-compute/spatial-aggregation-and-tiling/aggregating-points-to-h3-cells-in-sql/",
  ],

  // Topic areas added in the latest build, surfaced on the homepage.
  latestTopics: [
    "/spatial-lakehouse-fundamentals-architecture/geometry-validation-and-repair/",
    "/spatial-lakehouse-fundamentals-architecture/spatial-data-observability/",
    "/spatial-partitioning-indexing-strategies/spatial-join-optimization/",
    "/python-ecosystem-integration-workflows/streaming-spatial-ingestion/",
    "/spatial-query-engines-compute/spatial-aggregation-and-tiling/",
  ],

  sections: [
    {
      slug: "spatial-lakehouse-fundamentals-architecture",
      title: "Spatial Lakehouse Fundamentals & Architecture",
      summary:
        "Decouple storage, catalog, and compute. Master geometry serialization (WKB/GeoParquet), snapshot semantics, and the Iceberg/Delta trade-offs that govern production spatial stacks.",
      icon: "layers",
      color: "#0e6e7d",
    },
    {
      slug: "spatial-partitioning-indexing-strategies",
      title: "Spatial Partitioning & Indexing Strategies",
      summary:
        "Hierarchical grids, Z-ordering, Hilbert curves, predicate pushdown, and raster/vector hybrid layouts engineered for sub-second queries at petabyte scale.",
      icon: "grid",
      color: "#2f6e49",
    },
    {
      slug: "python-ecosystem-integration-workflows",
      title: "Python Ecosystem & Integration Workflows",
      summary:
        "Arrow schemas, PyIceberg, delta-rs, async catalog orchestration, and CI/CD validation — the Python contract that keeps spatial pipelines reproducible and fast.",
      icon: "python",
      color: "#9a5a17",
    },
    {
      slug: "spatial-query-engines-compute",
      title: "Spatial Query Engines & Compute Optimization",
      summary:
        "DuckDB, Trino, and Apache Sedona against lakehouse tables — spatial SQL functions, distributed joins, catalog federation, and the benchmarks that decide which engine owns each workload.",
      icon: "cpu",
      color: "#6a3d9a",
    },
  ],
};
