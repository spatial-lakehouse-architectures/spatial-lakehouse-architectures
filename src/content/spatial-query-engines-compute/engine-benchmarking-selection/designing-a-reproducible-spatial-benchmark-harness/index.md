# Designing a Reproducible Spatial Benchmark Harness

This guide builds a benchmark harness whose results can be reproduced, compared across months and defended in a review — which requires pinning far more than the engine version.

## Context and prerequisites

Most spatial benchmarks are not wrong so much as unrepeatable: the layout was not recorded, the queries differed subtly between engines, the caches were warm for one run and cold for another. This recipe uses Python to orchestrate DuckDB, Trino and Spark with Sedona; the design principles are in [engine benchmarking and selection](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/engine-benchmarking-selection/), and the layout that must be pinned in [spatial partitioning schemes](https://www.spatial-lakehouse-architectures.org/spatial-partitioning-indexing-strategies/spatial-partitioning-schemes/).

## What has to be pinned

<figure class="diagram">
<svg viewBox="0 0 762 284" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Five things a reproducible spatial benchmark must pin: the data generator, the table layout, the query set with verified equivalent semantics, the environment, and the cache state">
<rect x="0" y="0" width="762" height="284" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Five artefacts, or the result is an anecdote</text>
<rect x="30" y="56" width="352" height="100" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="206" y="82" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">1. the data</text>
<text x="206" y="106" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">a seeded generator, or a pinned snapshot</text>
<text x="206" y="130" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">with realistic density skew, not uniform</text>
<rect x="398" y="56" width="352" height="100" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="574" y="82" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">2. the layout</text>
<text x="574" y="106" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">partition spec, sort order, statistics config</text>
<text x="574" y="130" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">as executable DDL, not prose</text>
<rect x="30" y="172" width="352" height="100" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="206" y="198" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">3. the queries</text>
<text x="206" y="222" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">one file per engine, results proven identical</text>
<text x="206" y="246" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">not merely similar-looking SQL</text>
<rect x="398" y="172" width="352" height="100" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="574" y="198" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">4 &amp; 5. environment and cache</text>
<text x="574" y="222" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">versions, instance types, storage class</text>
<text x="574" y="246" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">cold and warm reported separately</text>
</svg>
</figure>

The third artefact is where spatial benchmarks most often lose their validity. Function names and semantics differ between engines — one includes touching geometries in an intersection, another may not; distance functions differ in their units — so two queries that look equivalent can answer different questions. Proving the result sets match is the only way to know, and it should be a gate rather than a spot check.

## Complete working solution

```python
import hashlib, json, time
from dataclasses import dataclass, asdict

@dataclass
class RunResult:
    engine: str
    query_id: str
    cache: str            # "cold" or "warm"
    wall_ms: float
    rows: int
    result_hash: str
    bytes_scanned: int | None
    files_scanned: int | None

def result_fingerprint(rows) -> str:
    """Order-independent hash so engines returning different orders still match."""
    h = hashlib.sha256()
    for line in sorted("|".join(map(str, r)) for r in rows):
        h.update(line.encode())
    return h.hexdigest()[:16]

def run_query(engine, query_id: str, sql: str, cache: str) -> RunResult:
    if cache == "cold":
        engine.drop_caches()
    start = time.perf_counter()
    rows = engine.execute(sql)
    wall = (time.perf_counter() - start) * 1000
    stats = engine.last_query_stats()
    return RunResult(engine.name, query_id, cache, wall, len(rows),
                     result_fingerprint(rows),
                     stats.get("bytes_scanned"), stats.get("files_scanned"))

def benchmark(engines, queries: dict[str, dict[str, str]], repeats: int = 3):
    results, fingerprints = [], {}
    for qid, per_engine_sql in queries.items():
        for engine in engines:
            sql = per_engine_sql[engine.name]
            for cache in ("cold", "warm"):
                runs = [run_query(engine, qid, sql, cache) for _ in range(repeats)]
                runs.sort(key=lambda r: r.wall_ms)
                results.append(runs[len(runs) // 2])          # the median run
            fingerprints.setdefault(qid, {})[engine.name] = runs[0].result_hash

    for qid, per_engine in fingerprints.items():
        distinct = set(per_engine.values())
        assert len(distinct) == 1, (
            f"query {qid}: engines disagree on the result — {per_engine}")
    return results
```

## Step-by-step walkthrough

1. **Fingerprint results order-independently.** Engines return rows in different orders, so a naive hash of the concatenated output differs for identical result sets. Sorting the row strings before hashing removes the false difference while keeping real ones.

2. **Assert agreement before reporting timings.** A benchmark comparing engines that return different answers is measuring nothing. Making the agreement check a hard assertion means a semantic difference stops the run rather than appearing as a suspiciously fast engine.

3. **Take the median of several runs.** Cluster contention, storage variability and JIT warm-up all add noise. Three runs and the median is enough to be stable without making the harness slow.

4. **Report cold and warm separately.** They answer different questions — cold measures the layout, warm measures the engine — and averaging them produces a number that answers neither.

5. **Capture bytes and files scanned where available.** Wall time tells you which engine was faster on the day; bytes scanned tells you why, and survives a change of hardware.

## Common errors and fixes

| Symptom | Cause | Fix |
|---|---|---|
| One engine is implausibly fast | Its query answers a different question | Enforce the result fingerprint check |
| Results vary run to run | Warm caches, or a busy cluster | Report cold and warm; take medians; note contention |
| Ranking reverses on the real table | Benchmarked against an unsorted export | Benchmark the layout you will ship |
| A benchmark cannot be repeated | Data and layout not pinned | Check in the generator and the DDL |
| Timings improve after an unrelated change | Storage cache warmed by an earlier run | Drop caches between cold runs, or use fresh paths |

## Reporting the result

<figure class="diagram">
<svg viewBox="0 0 762 270" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="A benchmark report structure with the decision under test, the pinned configuration, per query cold and warm results, and the threshold that would have changed the conclusion">
<rect x="0" y="0" width="762" height="270" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">A report somebody can act on a year later</text>
<rect x="30" y="56" width="720" height="46" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="390" y="84" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">1. the decision this was run to inform — stated before the numbers</text>
<rect x="30" y="112" width="720" height="46" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="390" y="140" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">2. the pinned configuration — data, layout, queries, environment</text>
<rect x="30" y="168" width="720" height="46" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="390" y="196" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">3. per-query results, cold and warm, with bytes scanned</text>
<rect x="30" y="224" width="720" height="34" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="390" y="246" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">4. the result that would have changed the conclusion</text>
</svg>
</figure>

The fourth section is what makes the report reusable. Stating "we chose Trino; had DuckDB been within 20% on the twenty-user concurrent mix, we would have chosen it" turns a future re-evaluation into a single measurement rather than a repeat of the whole exercise — and it makes the reasoning auditable by someone who was not in the room.

Publish per-query results rather than an aggregate. A geometric mean hides the case that matters, which is usually the one where an engine was an order of magnitude slower rather than the several where it was slightly faster.

## Keeping the harness honest over time

Re-run the benchmark on a schedule rather than only when a decision is due, and store the results in the same metrics table the rest of the platform's observability uses. Engine releases change performance, the data grows, and a decision made eighteen months ago against versions nobody runs any more is not evidence.

The scheduled run also catches something a one-off cannot: a regression in the platform's own layout. If the same query against the same pinned dataset slows by a factor of three, the engine did not change — the table did, and the harness has just found a layout regression that no other check was looking for.

## Designing the Query Set

The queries are the part of the harness that most determines whether its conclusion transfers to production, and a set assembled from intuition rarely does.

<figure class="diagram">
<svg viewBox="0 0 768 244" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Four query shapes that a spatial benchmark set should cover: a small window selective read, a wide extent aggregation, a join against a reference layer, and a concurrent mix">
<rect x="0" y="0" width="768" height="244" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Four shapes, each stressing something different</text>
<rect x="26" y="56" width="172" height="176" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="112" y="86" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">small window</text>
<text x="112" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">a city, one day</text>
<text x="112" y="142" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">stresses: pruning</text>
<text x="112" y="170" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">and metadata planning</text>
<text x="112" y="204" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">the layout&#8217;s test</text>
<rect x="212" y="56" width="172" height="176" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="298" y="86" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">wide aggregation</text>
<text x="298" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">counts per cell, nationally</text>
<text x="298" y="142" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">stresses: scan throughput</text>
<text x="298" y="170" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">and aggregation memory</text>
<text x="298" y="204" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">the engine&#8217;s test</text>
<rect x="398" y="56" width="172" height="176" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="484" y="86" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">reference join</text>
<text x="484" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">points to boundaries</text>
<text x="484" y="142" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">stresses: join strategy</text>
<text x="484" y="170" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">and geometry evaluation</text>
<text x="484" y="204" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">the most variable</text>
<rect x="584" y="56" width="172" height="176" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="670" y="86" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">concurrent mix</text>
<text x="670" y="114" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">twenty of the above at once</text>
<text x="670" y="142" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">stresses: isolation</text>
<text x="670" y="170" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">and admission control</text>
<text x="670" y="204" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#6a3d9a">most often omitted</text>
</svg>
</figure>

The fourth shape is the one that reverses conclusions. An in-process engine that wins the first three convincingly can lose the fourth badly, because concurrency is where its architecture differs most from a coordinated one. A benchmark that omits it is not a simplification of the production question; it is a different question.

Derive the query set from the query log rather than from imagination. The ten shapes that consume the most total time on the current platform are the ten that matter, and they are usually less exotic than a hand-designed set — mostly small windows and simple aggregations, which is itself a useful finding.

## Interpreting the Numbers

A benchmark produces a table; turning it into a decision needs a few interpretive habits.

Compare **bytes scanned before wall time**. Two engines reading the same bytes at different speeds is an engine difference; two engines reading different bytes is a pushdown difference, and the second is usually larger and more actionable.

Treat differences under twenty percent as noise unless the harness has demonstrated tighter repeatability. Spatial workloads on shared infrastructure vary enough that a smaller apparent difference rarely survives a re-run.

Weight the query shapes by their production frequency rather than treating them equally. An engine ten percent slower on the shape that runs a thousand times a day matters more than one twice as slow on the shape that runs weekly.

And check the **failure behaviour**, not only the success timings. An engine that answers the concurrent mix quickly but fails one query in fifty under load is worse than a slower one that never fails, and a benchmark reporting only successful runs will not show it.

## Cost of the Harness

A reproducible harness is more work than an ad-hoc timing script, and the difference is worth quantifying because it is smaller than it appears.

The generator, the layout DDL and the query files are written once and are mostly things the platform needs anyway — a representative dataset and a set of canonical queries are useful for testing and for onboarding as well as for benchmarking. The orchestration is a couple of hundred lines. The result fingerprinting is twenty.

The ongoing cost is the scheduled run, which for a moderate dataset is minutes of compute and can happen overnight. Against that, the harness answers "is the platform still performing as it did" continuously rather than only when somebody asks, and it catches layout regressions that no other check looks for.

The case where it is not worth building is a one-off decision on a platform that will not be re-evaluated — a single engine choice, made once, never revisited. Those are rarer than they seem: engine versions change, data grows, and the question comes back. Building the harness the first time the question is asked means the second time costs an afternoon rather than a fortnight.
And an afternoon is short enough that the question gets answered with evidence rather than with the loudest opinion in the room.
For the design principles the harness implements — which dimensions to vary, and why a single-query benchmark misleads on a concurrent platform — see [engine benchmarking and selection](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/engine-benchmarking-selection/).
Read that page first if the harness is being built to settle a live disagreement, because the design decisions it covers determine whether the numbers will be believed.
Credibility is the harness’s real output.
A fast engine nobody believes is slower, in practice, than a slower one everybody trusts — because the first gets re-litigated every quarter and the second gets built on.
