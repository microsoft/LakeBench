# Integration Tests

Each test file runs all supported benchmarks for a single engine. Tests are marked `@pytest.mark.integration`; select a file with the commands below, or exclude this directory with `--ignore=tests/integration` for a unit-only run. Missing engine extras skip their modules.

> **Scale factor:** Data is generated at SF 0.1 (≈ 10% of SF 1) to keep CI runs fast.

---

## Prerequisites

- [uv](https://docs.astral.sh/uv/) installed
- Java 17+ on `PATH` (Spark / Sail only)

---

## Running a single engine

### DuckDB
```bash
uv sync --group dev --extra duckdb --extra tpch_datagen --extra tpcds_datagen
uv run pytest tests/integration/test_duckdb.py -v -s
```

### Daft
> Unsupported as of LakeBench 2.0.0 and excluded from CI: Daft cannot read the Arrow `Utf8View` the TPC generators emit, so every TPC-H, TPC-DS, and ELTBench table fails to load. The tests are skipped unless you opt in, so support can be re-checked in one command once Daft handles the type.
```bash
uv sync --group dev --extra daft --extra tpch_datagen --extra tpcds_datagen
LAKEBENCH_RUN_DAFT_INTEGRATION=1 uv run pytest tests/integration/test_daft.py -v -s
```

### Polars
```bash
uv sync --group dev --extra polars --extra tpch_datagen --extra tpcds_datagen
uv run pytest tests/integration/test_polars.py -v -s
```

### Spark
> `spark` and `sail` extras are mutually exclusive — use a separate venv if you need both.
```bash
uv sync --group dev --extra spark --extra tpch_datagen --extra tpcds_datagen
uv run pytest tests/integration/test_spark.py -v -s
```

### Sail
```bash
uv sync --group dev --extra sail --extra tpch_datagen --extra tpcds_datagen
uv run pytest tests/integration/test_sail.py -v -s
```

---

## Running a single benchmark for an engine

Each test module has one function per benchmark named `test_<benchmark>_<engine>`. Pass it after `::` to run only that scenario:

```bash
# TPC-H only for DuckDB
uv run pytest tests/integration/test_duckdb.py::test_tpch_duckdb -v -s

# ELTBench only for Spark
uv run pytest tests/integration/test_spark.py::test_eltbench_spark -v -s

# ClickBench only for Polars
uv run pytest tests/integration/test_polars.py::test_clickbench_polars -v -s
```

Available test names per engine follow the pattern `test_{tpch,tpcds,clickbench,eltbench}_{engine}`.

---

## Running DuckDB and Polars together

DuckDB and Polars share no conflicts and can run in one sync:

```bash
uv sync --group dev --extra duckdb --extra polars \
        --extra tpch_datagen --extra tpcds_datagen
uv run pytest tests/integration/test_duckdb.py \
              tests/integration/test_polars.py -v -s
```

Daft must be synced on its own: it pins `deltalake` to 1.5.x (Daft cannot read the Arrow `Utf8View` parquet that `deltalake` 1.6.x emits from `MERGE`), so `daft` is declared as conflicting with `duckdb`, `polars`, and `sail` in `[tool.uv]`.

---

## Benchmarks per engine

| Benchmark  | DuckDB | Daft | Polars | Spark | Sail |
|------------|:------:|:----:|:------:|:-----:|:----:|
| TPC-H      | ✅     | ❌   | ⚠️     | ✅    | ✅   |
| TPC-DS     | ✅     | ❌   | ⚠️     | ✅    | ✅   |
| ClickBench | ✅     | —    | ⚠️     | ✅    | ✅   |
| ELTBench   | ✅     | ❌   | ⚠️     | ✅    | ✅   |

Daft's ❌ entries are the `Utf8View` load failure described above, not missing coverage; the tests exist and are skipped by default.

Polars's ⚠️ entries denote a partial-support assertion policy, not a claim that every benchmark has failures. Query/task failures remain warnings provided at least one succeeds; the complete workload and successful Load-phase results are still required. Per-query known-good/known-failure baselines are tracked separately in #121.

ClickBench uses the committed 100-row sample at `tests/integration/data/clickbench_sample.parquet`.

---

## Pass / fail semantics

1. **Any unexpected run exception fails**, including a crash before any results, an abort after partial success, or an exception after all results were recorded. The exception and available results are retained in the compatibility report.
2. **Completeness is mandatory, independently of pass rate.** Every caller supplies `expected_results=expected_workload(BenchmarkClass, mode)`. Missing, unexpected, duplicate, or zero results fail. Keys include phase, sub-phase, item, and iteration; an optimization result cannot substitute for a load, and repeated ELT merges must have distinct expected iterations.
3. **Every Load-phase table load must succeed.** One failed load fails a load-and-query benchmark, even when queries are allowed partial support. ELTBench records its five loads as tasks and applies the task-success threshold below.
4. **DuckDB, Spark, and Sail require 100% query/task success** (`min_pass_rate=1.0`). Polars and opt-in Daft explicitly retain partial support (`min_pass_rate=0.0`): individual failures warn, but all queries/tasks failing fails the test. This threshold never permits incomplete work or a run exception.

The default `load_and_query` inventories are 8 loads + 22 queries for TPC-H, 24 loads + 103 executable queries (including template variants) for TPC-DS, and 1 load + 43 queries for ClickBench. `load` requires only loads; `query` and `power_test` require only queries. ELTBench `light` requires all 12 tasks: five loads, fact-table creation, three merge iterations, optimize, vacuum, and the ad-hoc query.

`expected_workload` describes the default complete query/table selection without optional load optimization or analysis. Tests using custom query selections or maintenance options must explicitly supply the corresponding workload keys instead of deriving expectations from the observed results. Failed and incomplete runs still generate reports; an interrupted or incomplete workload is never given a success icon.
