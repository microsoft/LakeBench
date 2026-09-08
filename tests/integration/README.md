# Integration Tests

Each test file runs all supported benchmarks for a single engine.  
Tests are marked `@pytest.mark.integration` and are **not** collected by default — you must opt in via the commands below.

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
```bash
uv sync --group dev --extra daft --extra tpch_datagen --extra tpcds_datagen
uv run pytest tests/integration/test_daft.py -v -s
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

Each test module has one function per benchmark named `test_<benchmark>_<engine>`.  
Pass it after `::` to run only that scenario:

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

Daft must be synced on its own: it pins `deltalake` to 1.5.x (Daft cannot read the
Arrow `Utf8View` parquet that `deltalake` 1.6.x emits from `MERGE`), so `daft` is
declared as conflicting with `duckdb`, `polars`, and `sail` in `[tool.uv]`.

---

## Benchmarks per engine

| Benchmark  | DuckDB | Daft | Polars | Spark | Sail |
|------------|:------:|:----:|:------:|:-----:|:----:|
| TPC-H      | ✅     | ✅   | ✅     | ✅    | ✅   |
| TPC-DS     | ✅     | ✅   | ✅     | ✅    | ✅   |
| ClickBench | ✅     | —    | —      | ✅    | ✅   |
| ELTBench   | ✅     | ✅   | ✅     | ✅    | ✅   |

ClickBench uses the committed 100-row sample at `tests/integration/data/clickbench_sample.parquet`.

---

## Pass / fail semantics

- **Individual query failures** → `UserWarning`, test still passes.  
- **All queries fail** or **all tables fail to load** → test fails.  
- **Engine crash before any results** → `UserWarning`, test still passes (graceful degradation).
