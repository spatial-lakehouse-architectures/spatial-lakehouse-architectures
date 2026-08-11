# Tuning Sedona Partitioning for Skewed Joins

This guide configures Sedona's spatial partitioner for a large-versus-large join over unevenly distributed data, choosing the grid type and partition count from measurements rather than defaults, and verifying that the straggler is actually gone.

## Context and prerequisites

When neither side of a spatial join fits in a broadcast, the partitioned join is the remaining strategy — and its performance is entirely determined by whether the partitioner divides the *data* evenly rather than the *space* evenly. This recipe uses Spark 3.5 with Sedona; the strategy choice is in [choosing between broadcast and partitioned spatial joins](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/spatial-join-optimization/choosing-between-broadcast-and-partitioned-spatial-joins/), and the engine context in [Sedona distributed spatial compute](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/sedona-distributed-spatial-compute/).

## Grid type is the decision that matters

<figure class="diagram">
<svg viewBox="0 0 717 260" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="A uniform grid dividing space evenly and therefore data unevenly, against a tree based partitioner subdividing dense regions so each partition holds a comparable number of features">
<rect x="0" y="0" width="717" height="260" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Divide the data, not the space</text>
<text x="196" y="62" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#9a5a17">uniform grid</text>
<rect x="86" y="78" width="220" height="140" fill="#ffffff" stroke="#9a5a17" stroke-width="2"/>
<line x1="141" y1="78" x2="141" y2="218" stroke="#9a5a17" stroke-width="1.5"/>
<line x1="196" y1="78" x2="196" y2="218" stroke="#9a5a17" stroke-width="1.5"/>
<line x1="251" y1="78" x2="251" y2="218" stroke="#9a5a17" stroke-width="1.5"/>
<line x1="86" y1="125" x2="306" y2="125" stroke="#9a5a17" stroke-width="1.5"/>
<line x1="86" y1="171" x2="306" y2="171" stroke="#9a5a17" stroke-width="1.5"/>
<circle cx="160" cy="140" r="2.5" fill="#0d3b45"/><circle cx="168" cy="148" r="2.5" fill="#0d3b45"/>
<circle cx="176" cy="138" r="2.5" fill="#0d3b45"/><circle cx="164" cy="156" r="2.5" fill="#0d3b45"/>
<circle cx="180" cy="152" r="2.5" fill="#0d3b45"/><circle cx="172" cy="162" r="2.5" fill="#0d3b45"/>
<circle cx="158" cy="132" r="2.5" fill="#0d3b45"/><circle cx="186" cy="146" r="2.5" fill="#0d3b45"/>
<circle cx="110" cy="100" r="2.5" fill="#0d3b45"/><circle cx="270" cy="196" r="2.5" fill="#0d3b45"/>
<text x="196" y="244" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">one cell holds most of the data</text>
<text x="584" y="62" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#2f6e49">tree partitioner</text>
<rect x="474" y="78" width="220" height="140" fill="#ffffff" stroke="#2f6e49" stroke-width="2"/>
<line x1="584" y1="78" x2="584" y2="218" stroke="#2f6e49" stroke-width="1.5"/>
<line x1="474" y1="148" x2="584" y2="148" stroke="#2f6e49" stroke-width="1.5"/>
<line x1="529" y1="118" x2="584" y2="118" stroke="#2f6e49" stroke-width="1.5"/>
<line x1="529" y1="78" x2="529" y2="148" stroke="#2f6e49" stroke-width="1.5"/>
<line x1="556" y1="118" x2="556" y2="148" stroke="#2f6e49" stroke-width="1.5"/>
<line x1="556" y1="133" x2="584" y2="133" stroke="#2f6e49" stroke-width="1.5"/>
<circle cx="560" cy="124" r="2.5" fill="#0d3b45"/><circle cx="568" cy="128" r="2.5" fill="#0d3b45"/>
<circle cx="574" cy="122" r="2.5" fill="#0d3b45"/><circle cx="562" cy="140" r="2.5" fill="#0d3b45"/>
<circle cx="572" cy="142" r="2.5" fill="#0d3b45"/><circle cx="578" cy="138" r="2.5" fill="#0d3b45"/>
<circle cx="500" cy="100" r="2.5" fill="#0d3b45"/><circle cx="650" cy="190" r="2.5" fill="#0d3b45"/>
<text x="584" y="244" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">dense regions subdivide; counts even out</text>
</svg>
</figure>

The tree partitioner builds its boundaries from a sample of the data, so its cells are small where the data is dense and large where it is sparse. That is exactly the property a uniform grid lacks and exactly the property a skewed spatial join needs.

The cost is a sampling pass before the join and a partitioner that is data-dependent rather than fixed — which means it must be rebuilt when the distribution changes, and that two runs over different data are not directly comparable.

## Complete working solution

```python
from pyspark.sql import SparkSession, functions as F
from sedona.spark import SedonaContext

spark = SedonaContext.create(SparkSession.builder
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .getOrCreate())

def choose_partition_count(spark, table: str, target_mb: int = 128) -> int:
    """Partition count from data volume, not from a default."""
    total = spark.sql(f"""
        SELECT sum(file_size_in_bytes) AS b FROM {table}.files
    """).collect()[0]["b"]
    return max(int(total / (target_mb * 1e6)), spark.sparkContext.defaultParallelism)

def configure(spark, parcels_table: str, buildings_table: str) -> None:
    n = max(choose_partition_count(spark, parcels_table),
            choose_partition_count(spark, buildings_table))
    spark.conf.set("sedona.join.gridtype", "kdbtree")   # adapts to the distribution
    spark.conf.set("sedona.join.numpartition", n)
    spark.conf.set("sedona.join.indexbuildside", "left")
    spark.conf.set("sedona.join.spatitionside", "left")
    spark.conf.set("spark.sql.shuffle.partitions", n)

def run_join(spark):
    return spark.sql("""
        SELECT p.parcel_id, b.building_id
        FROM   lakehouse.spatial.parcels   p
        JOIN   lakehouse.spatial.buildings b
          ON   ST_Intersects(ST_GeomFromWKB(p.geom_wkb), ST_GeomFromWKB(b.geom_wkb))
    """)
```

## Step-by-step walkthrough

1. **Register the Kryo serialiser.** Geometry serialises poorly through the default Java path, and on a partitioned join every feature crosses the network. This single configuration typically reduces shuffle volume by a factor of two to four.

2. **Choose a tree-based grid type.** A uniform grid divides space evenly, which for spatial data means dividing the workload unevenly. The tree variants sample the data and place boundaries where the density is.

3. **Derive the partition count from volume.** Aim for roughly 128 MB of input per partition, floored at the cluster's default parallelism so small jobs still use the whole cluster. A fixed number tuned for one dataset is wrong for the next.

4. **Enable adaptive skew handling as a backstop.** Spark's adaptive execution splits skewed shuffle partitions at runtime, which catches residual skew the partitioner did not anticipate. It is a safety net rather than a substitute for the partitioner.

5. **Index the side with more features per partition.** The tree index is built per partition on one side and probed by the other; building it on the denser side gives the probe more work to skip.

## Common errors and fixes

| Symptom | Cause | Fix |
|---|---|---|
| One task runs many times longer | Uniform grid on skewed data | Switch to a tree-based grid type |
| Shuffle volume enormous | Default Java serialisation | Register the Kryo serialiser |
| Too many tiny tasks | Partition count far above the data volume | Derive it from bytes, not from a habit |
| Duplicate result rows | Features replicated across partitions, not deduplicated | Deduplicate on the identifier pair |
| Job fails on executor memory | Index built on the side with huge geometries | Build the index on the other side; check vertex counts |

## Verifying the fix

<figure class="diagram">
<svg viewBox="0 0 752 244" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Task duration distributions before and after switching to a tree partitioner, showing a long tail collapsing into a tight band">
<rect x="0" y="0" width="752" height="244" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">The distribution, not the total, is the evidence</text>
<line x1="60" y1="200" x2="380" y2="200" stroke="#33707d" stroke-width="1.5"/>
<text x="220" y="62" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#9a5a17">uniform grid</text>
<rect x="78" y="182" width="20" height="18" fill="#9a5a17"/>
<rect x="104" y="180" width="20" height="20" fill="#9a5a17"/>
<rect x="130" y="184" width="20" height="16" fill="#9a5a17"/>
<rect x="156" y="181" width="20" height="19" fill="#9a5a17"/>
<rect x="182" y="183" width="20" height="17" fill="#9a5a17"/>
<rect x="208" y="180" width="20" height="20" fill="#9a5a17"/>
<rect x="234" y="182" width="20" height="18" fill="#9a5a17"/>
<rect x="286" y="76" width="20" height="124" fill="#9a5a17"/>
<rect x="312" y="112" width="20" height="88" fill="#9a5a17"/>
<text x="220" y="228" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">max ÷ median = 14</text>
<line x1="440" y1="200" x2="740" y2="200" stroke="#33707d" stroke-width="1.5"/>
<text x="590" y="62" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#2f6e49">kdb-tree</text>
<rect x="458" y="148" width="20" height="52" fill="#2f6e49"/>
<rect x="484" y="142" width="20" height="58" fill="#2f6e49"/>
<rect x="510" y="152" width="20" height="48" fill="#2f6e49"/>
<rect x="536" y="145" width="20" height="55" fill="#2f6e49"/>
<rect x="562" y="150" width="20" height="50" fill="#2f6e49"/>
<rect x="588" y="143" width="20" height="57" fill="#2f6e49"/>
<rect x="614" y="149" width="20" height="51" fill="#2f6e49"/>
<rect x="640" y="146" width="20" height="54" fill="#2f6e49"/>
<rect x="666" y="151" width="20" height="49" fill="#2f6e49"/>
<text x="590" y="228" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">max ÷ median = 1.2</text>
</svg>
</figure>

```python
def assert_no_straggler(spark, stage_id: int, max_ratio: float = 3.0):
    info = spark.sparkContext.statusTracker().getStageInfo(stage_id)
    durations = sorted(t.duration for t in info.taskInfos)
    median = durations[len(durations) // 2]
    ratio = durations[-1] / median
    assert ratio <= max_ratio, f"straggler remains: slowest task {ratio:.1f}× median"
```

The ratio is the acceptance test, and it is more informative than the total runtime because it separates two questions: whether the work is balanced, and whether there is too much of it. A job with a good ratio that is still slow needs more resources; one with a poor ratio needs a better partitioner, and adding resources will not help.

Record the ratio with each run. Skew re-emerges as the data grows, and a recorded series turns the eventual re-tuning into a scheduled change rather than a surprise.

## Duplication at Partition Boundaries

A spatially partitioned join replicates any feature that spans a partition boundary into every partition it touches, and that replication has two consequences worth handling deliberately.

<figure class="diagram">
<svg viewBox="0 0 762 264" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="A polygon spanning four spatial partitions being replicated into each, producing duplicate matches that must be removed after the exact predicate">
<rect x="0" y="0" width="762" height="264" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Replication in, deduplication out</text>
<rect x="86" y="70" width="110" height="76" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="1.5"/>
<rect x="196" y="70" width="110" height="76" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="1.5"/>
<rect x="86" y="146" width="110" height="76" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="1.5"/>
<rect x="196" y="146" width="110" height="76" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="1.5"/>
<path d="M150 108 L250 100 L258 190 L158 198 Z" fill="#f2e8da" fill-opacity="0.8" stroke="#9a5a17" stroke-width="2.5"/>
<text x="196" y="248" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">one feature, four partitions, four copies</text>
<rect x="380" y="80" width="370" height="60" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="565" y="106" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">shuffle volume grows with the replication</text>
<text x="565" y="126" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">a finer partitioner replicates more</text>
<rect x="380" y="156" width="370" height="60" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="565" y="182" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">a pair can match in several partitions</text>
<text x="565" y="202" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">deduplicate, or count duplicates as results</text>
</svg>
</figure>

The first consequence sets an upper bound on how fine the partitioning should be: past a certain point, extra partitions replicate more features than they balance, and the shuffle grows faster than the parallelism helps. Measuring shuffle bytes against input bytes reveals it — a ratio much above two means the partitioner is dividing more finely than the geometry sizes justify.

The second consequence is a correctness issue. A pair of features sharing several partitions matches in each, so the join emits duplicates. Deduplicating on the identifier pair is correct and adds a shuffle; assigning each pair to a canonical partition and filtering to it avoids the shuffle and is worth the extra clause on large joins.

Neither problem appears in a broadcast join, which is one more reason to reduce the smaller side and broadcast where it is at all possible.

## When the Partitioner Is Not the Problem

Three situations produce a straggler that no partitioner setting fixes, and recognising them saves a lot of fruitless tuning.

**One enormous geometry.** A single feature with hundreds of thousands of vertices costs more to evaluate than a hundred thousand simple ones, and it lives in one partition by definition. The remedy is to simplify it for join purposes or to split it into pieces sharing an identifier — a data change rather than a configuration one.

**A genuinely dense region.** Where a metropolitan area really does contain a third of the data and the join is many-to-many within it, the work is irreducibly concentrated. Adaptive execution can split the shuffle partition, but the candidate pairs remain, and the honest answer is that this partition takes longer.

**A skewed result rather than a skewed input.** Both inputs may be balanced while the *output* concentrates — a flood zone overlapping every parcel in a district produces a fan-out that no input partitioning anticipates. Here the fix is in the query: aggregate earlier, or restrict the join to the pairs the analysis actually needs.

The diagnostic that distinguishes these from ordinary partitioner skew is to compare the input rows and the output rows per partition. Input skew is a partitioner problem; output skew is a query problem; and a partition with balanced input, balanced output and a long duration contains a pathological geometry.

## A Tuning Sequence That Converges

Working through the settings in a fixed order avoids the common experience of changing several things at once and being unable to attribute the improvement.

Start by **registering Kryo** and measuring. It is a one-line change with a large effect on shuffle volume and it interacts with nothing else, so its contribution is clean.

Then **switch the grid type** to a tree-based partitioner and measure the task-duration ratio. This is the change that addresses skew directly, and if the ratio does not improve, the skew is one of the three non-partitioner cases above and further partitioner tuning is wasted.

Then **set the partition count** from the data volume and measure both the ratio and the shuffle bytes. Too few partitions leaves large tasks; too many inflates replication. The measurement distinguishes them and one adjustment usually suffices.

Finally **enable adaptive skew handling** as a backstop and confirm it is not doing much — if it is splitting many partitions, the partitioner is still not fitting the data and the previous step needs revisiting.

Record all four measurements at each step, and record the cluster shape alongside them. A tuning result is only meaningful for the cluster it was measured on, and a configuration inherited from a differently-sized cluster is a common source of settings nobody can explain.
Recording the cluster shape alongside the settings is what lets a future reader tell an inherited configuration from a measured one.
The distinction matters because an inherited configuration is safe to change and a measured one is not.
Write down which it is, next to the setting, and the next tuning session starts from knowledge rather than from guesswork.
A settings file with reasons is worth several with better values.
Reasons outlive values.
