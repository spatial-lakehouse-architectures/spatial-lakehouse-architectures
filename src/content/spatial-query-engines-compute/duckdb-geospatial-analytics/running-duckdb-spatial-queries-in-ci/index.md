# Running DuckDB Spatial Queries in CI

This guide runs real spatial SQL against real lakehouse data inside a continuous-integration pipeline, fast enough to gate every pull request, so a change that breaks a spatial query fails a build rather than a dashboard.

## Context and prerequisites

Spatial correctness regressions are usually silent — a predicate that stops pruning, a layout change that breaks a join, a repair that alters a geometry — so a test that actually executes the queries is worth more than any amount of review. DuckDB makes this affordable: it starts in milliseconds, needs no cluster, and evaluates the same `ST_*` functions production uses. This recipe needs DuckDB 1.1+ with the spatial extension; the engine positioning is in [DuckDB geospatial analytics](https://www.spatial-lakehouse-architectures.org/spatial-query-engines-compute/duckdb-geospatial-analytics/).

## What to test, and against what data

<figure class="diagram">
<svg viewBox="0 0 764 264" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Three tiers of continuous integration spatial tests: fixture based correctness tests, sampled production data contract tests, and a nightly full extract for pruning and performance">
<rect x="0" y="0" width="764" height="264" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Three tiers, three cadences</text>
<rect x="26" y="56" width="230" height="196" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="141" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">fixtures</text>
<text x="141" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">a few hundred rows in the repo</text>
<text x="141" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">predicate correctness</text>
<text x="141" y="166" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">boundary and edge cases</text>
<text x="141" y="196" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">runs in under a second</text>
<text x="141" y="226" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#2f6e49">every commit</text>
<rect x="274" y="56" width="230" height="196" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="389" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">sampled real data</text>
<text x="389" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">one partition, read directly</text>
<text x="389" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">schema and contract checks</text>
<text x="389" y="166" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">CRS, encoding, bbox coverage</text>
<text x="389" y="196" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">tens of seconds</text>
<text x="389" y="226" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0e6e7d">every pull request</text>
<rect x="522" y="56" width="230" height="196" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="637" y="86" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#0d3b45">full extract</text>
<text x="637" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">a representative day</text>
<text x="637" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">pruning ratios</text>
<text x="637" y="166" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">query timings</text>
<text x="637" y="196" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">minutes</text>
<text x="637" y="226" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">nightly</text>
</svg>
</figure>

The middle tier is the one most platforms lack and the one that catches the most. Fixtures test the code; the sampled tier tests the *table* — that its schema still carries what queries assume, that its statistics still exist, that its CRS has not changed. Those are properties of production data that no fixture can express.

## Complete working solution

```python
# conftest.py — one connection per session, extensions baked into the image.
import duckdb, pytest

@pytest.fixture(scope="session")
def con():
    c = duckdb.connect()
    c.execute("LOAD spatial;")            # pre-installed in the CI image
    c.execute("SET memory_limit='2GB';")  # bounded, so a runaway test fails fast
    yield c
    c.close()

@pytest.fixture(scope="session")
def sample(con):
    """One partition of the real table, read directly from object storage."""
    con.execute("LOAD httpfs;")
    con.execute("CREATE OR REPLACE SECRET s (TYPE S3, PROVIDER credential_chain);")
    con.execute("""
        CREATE TABLE sample AS
        SELECT * FROM read_parquet('s3://lakehouse/telemetry/event_day=2026-03-11/*.parquet')
        LIMIT 200000
    """)
    return "sample"
```

```python
# test_spatial_contract.py — properties of the table, not of the code.
def test_geometry_decodes(con, sample):
    bad = con.execute(f"""
        SELECT count(*) FROM {sample}
        WHERE TRY(ST_GeomFromWKB(geom_wkb)) IS NULL AND geom_wkb IS NOT NULL
    """).fetchone()[0]
    assert bad == 0, f"{bad} rows have undecodable geometry"

def test_bbox_covers_geometry(con, sample):
    violations = con.execute(f"""
        SELECT count(*) FROM {sample}
        WHERE NOT ST_Covers(
            ST_MakeEnvelope(bbox_min_x, bbox_min_y, bbox_max_x, bbox_max_y),
            ST_GeomFromWKB(geom_wkb))
    """).fetchone()[0]
    assert violations == 0, f"{violations} bounding boxes do not cover their geometry"

def test_coordinates_are_geographic(con, sample):
    outside = con.execute(f"""
        SELECT count(*) FROM {sample}
        WHERE abs(bbox_min_x) > 180 OR abs(bbox_max_x) > 180
           OR abs(bbox_min_y) > 90  OR abs(bbox_max_y) > 90
    """).fetchone()[0]
    assert outside == 0, "coordinates outside the declared geographic range"

def test_all_geometries_valid(con, sample):
    invalid = con.execute(f"""
        SELECT count(*) FROM {sample}
        WHERE NOT ST_IsValid(ST_GeomFromWKB(geom_wkb))
    """).fetchone()[0]
    assert invalid == 0, f"{invalid} invalid geometries reached the table"
```

## Step-by-step walkthrough

1. **Bake the extensions into the image.** Installing `spatial` at test time adds a network dependency to every run and fails in air-gapped builds. Pre-installing removes both problems and shaves seconds off each run.

2. **Bound the memory limit.** A test that accidentally scans far more than intended should fail quickly rather than exhaust the runner. A limit well below the runner's memory turns a runaway into a fast, clear failure.

3. **Read one partition directly.** The sample does not need to be representative in volume, only in shape. One day of one partition exercises every contract property and downloads a manageable amount.

4. **Test properties, not values.** Asserting that every bounding box covers its geometry is stable across data changes; asserting a specific row count is not, and will fail every day for reasons nobody cares about.

5. **Use `TRY` around decoding.** A malformed geometry should produce a counted failure rather than an exception that aborts the test and hides how many others are affected.

## Common errors and fixes

| Symptom | Cause | Fix |
|---|---|---|
| Tests fail intermittently on data changes | Asserting exact values rather than properties | Assert invariants that hold for any valid data |
| CI run takes several minutes | Sample too large, or downloaded per test | Session-scoped fixture; limit the sample |
| Extension download fails in the build | Installing at runtime | Bake extensions into the CI image |
| Credentials work locally, not in CI | Different credential chain in the runner | Use a dedicated read-only role and configure it explicitly |
| Memory errors on the runner | No memory limit set | Set `memory_limit` well below the runner's capacity |

## Testing that pruning still works

<figure class="diagram">
<svg viewBox="0 0 762 222" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="A continuous integration assertion comparing row groups scanned against row groups available for a scoped query, catching a layout regression that produces correct but slow results">
<rect x="0" y="0" width="762" height="222" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">The regression that no correctness test catches</text>
<rect x="30" y="58" width="352" height="152" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="206" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#2f6e49">before the change</text>
<text x="206" y="118" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">row groups scanned: 4 of 220</text>
<text x="206" y="146" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">result: 12 480 rows</text>
<text x="206" y="176" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">correct and fast</text>
<rect x="398" y="58" width="352" height="152" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="574" y="88" text-anchor="middle" font-family="sans-serif" font-size="13" font-weight="700" fill="#9a5a17">after a schema reorder</text>
<text x="574" y="118" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">row groups scanned: 220 of 220</text>
<text x="574" y="146" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">result: 12 480 rows</text>
<text x="574" y="176" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#9a5a17">correct and fifty times slower</text>
</svg>
</figure>

```python
def test_scoped_query_prunes(con, sample_path):
    plan = con.execute(f"""
        EXPLAIN ANALYZE
        SELECT count(*) FROM read_parquet('{sample_path}')
        WHERE bbox_min_x >= 13.0 AND bbox_max_x <= 13.8
          AND bbox_min_y >= 52.3 AND bbox_max_y <= 52.7
    """).fetchall()
    text = "\n".join(row[1] for row in plan)
    scanned, total = parse_row_groups(text)
    assert scanned / total < 0.10, (
        f"pruning regressed: {scanned}/{total} row groups scanned")
```

This is the assertion worth having above all others, because the regression it catches is invisible in every other kind of test: the results are identical, no error is raised, and the only symptom is cost. A schema change that pushes the bounding-box columns past the statistics limit, or a write path that stops sorting, produces exactly this and nothing else notices.

Run it against a fixed extract stored with the tests rather than against live data, so the assertion measures the code's effect rather than the day's data. Refresh the extract deliberately, as a reviewed change, when the table's shape genuinely changes.

## Keeping the Suite Fast

A spatial CI suite that takes ten minutes will be skipped under deadline pressure, and a skipped test catches nothing. Four practices keep it under a minute.

<figure class="diagram">
<svg viewBox="0 0 768 210" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Four practices that keep a spatial continuous integration suite fast: a session scoped connection, a local fixture extract, extensions baked into the image, and property assertions rather than large comparisons">
<rect x="0" y="0" width="768" height="210" fill="#f7fbfc"/>
<text x="390" y="28" text-anchor="middle" font-family="sans-serif" font-size="16" font-weight="700" fill="#0d3b45">Four practices, each worth seconds per run</text>
<rect x="26" y="58" width="172" height="140" rx="8" fill="#e4f0f2" stroke="#0e6e7d" stroke-width="2"/>
<text x="112" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">one connection</text>
<text x="112" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">session-scoped fixture</text>
<text x="112" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">extension load once,</text>
<text x="112" y="164" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">not per test</text>
<rect x="212" y="58" width="172" height="140" rx="8" fill="#e6f0ea" stroke="#2f6e49" stroke-width="2"/>
<text x="298" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">local extract</text>
<text x="298" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">a checked-in Parquet file</text>
<text x="298" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">no network in the</text>
<text x="298" y="164" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">per-commit tier</text>
<rect x="398" y="58" width="172" height="140" rx="8" fill="#f2e8da" stroke="#9a5a17" stroke-width="2"/>
<text x="484" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">baked extensions</text>
<text x="484" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">in the CI image</text>
<text x="484" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">no download, works</text>
<text x="484" y="164" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">air-gapped</text>
<rect x="584" y="58" width="172" height="140" rx="8" fill="#faf8fc" stroke="#6a3d9a" stroke-width="2"/>
<text x="670" y="88" text-anchor="middle" font-family="sans-serif" font-size="12" font-weight="700" fill="#0d3b45">property assertions</text>
<text x="670" y="116" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#33707d">counts, not row sets</text>
<text x="670" y="144" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">aggregate in SQL,</text>
<text x="670" y="164" text-anchor="middle" font-family="sans-serif" font-size="11" fill="#0d3b45">compare one number</text>
</svg>
</figure>

The last practice is the one that most often goes wrong. A test that fetches a result set into Python and compares it row by row transfers and materialises data the assertion did not need; the same test expressed as a `count(*)` of violations returns one integer and runs in a fraction of the time. Push the comparison into the query.

The second practice creates a tension worth naming: a checked-in extract is fast and can go stale, while reading live data is current and slow. The resolution is the tiering described earlier — the per-commit tier uses the extract, the per-pull-request tier reads one live partition, and the extract is refreshed as a deliberate, reviewed change when the table's shape moves.

## What This Cannot Test

Being explicit about the boundary avoids false confidence.

A single-node engine cannot test **distributed behaviour**: shuffle correctness, skew, broadcast thresholds and partitioner behaviour are properties of the cluster runtime and need a test there. What it can test is that the SQL is semantically correct, which is usually where the bugs are.

It cannot test **concurrency**: resource-group behaviour, admission control and multi-user isolation belong to the serving engine. A CI suite that passes says nothing about whether the platform holds up under twenty simultaneous users.

And it cannot test **the full data volume**, so a query that is correct on a sample and pathological on the whole table — a join whose fan-out explodes at scale, an aggregation whose cardinality exceeds memory — will pass. The nightly full-extract tier exists precisely to narrow that gap, and it narrows rather than closes it.

Within those limits, the coverage is substantial and the cost is close to zero, which is an unusually good trade for a test suite. The failures it catches — a broken predicate, a lost sort order, a changed coordinate system, an invalid geometry reaching the table — account for most spatial incidents, and all four are silent everywhere else.

## Wiring It Into the Pipeline

Where the checks run matters as much as what they check, because a gate that runs after a deployment protects nothing.

The **fixture tier** belongs in the ordinary unit-test job, running on every push, with no credentials and no network. Treating it as an ordinary test suite rather than as something special keeps it maintained.

The **contract tier** belongs in the pull-request job, with read-only credentials scoped to the sampled table. It is the tier that needs the most care around access: a CI runner with broad read access to a governed spatial platform is a security surface, so scope the role to exactly the tables the tests read and nothing else.

The **nightly tier** belongs on a schedule with its results published to the same metrics table the observability layer uses, rather than as a build that fails. Its purpose is trend detection — pruning ratios and timings drifting over weeks — and a failing nightly build tends to be muted while a trend on a dashboard gets reviewed.

One last piece is worth adding: run the contract tier against the *table the change targets*, resolved from the diff. A change touching one pipeline should test that pipeline's table rather than every table on the platform, which keeps the pull-request job fast and keeps its failures relevant to the change under review.
Scoping the run to the change also keeps the failure attributable, which is what makes a red build get fixed rather than retried.
A relevant failure gets fixed; an unattributable one gets retried.
