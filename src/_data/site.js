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
