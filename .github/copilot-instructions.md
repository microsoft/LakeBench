# LakeBench Codebase Reference

> Quick-reference for Copilot and contributors. Keep this in sync when adding major features.

---

## What is LakeBench?

LakeBench is a **Python-native, multi-modal benchmarking framework** for evaluating performance across multiple lakehouse compute engines and ELT scenarios. It supports industry-standard benchmarks (TPC-DS, TPC-H, ClickBench) and a novel ELT-focused benchmark (ELTBench), all installable via `pip`.

---

## Project Layout

```
src/lakebench/
├── __init__.py
│
├── benchmarks/
│   ├── base.py                  # BaseBenchmark ABC — result schema, timing, post_results()
│   ├── elt_bench/               # ELTBench: load, transform, merge, maintain, query
│   ├── tpcds/                   # TPC-DS: 99 queries, 24 tables
│   ├── tpch/                    # TPC-H: 22 queries, 8 tables
│   └── clickbench/              # ClickBench: 43 queries on clickstream data
│
├── datagen/
│   ├── _tpcgen_rs.py            # Shared bundled tpcgen-cli generation, sizing, and output normalization
│   ├── tpch.py                  # TPCHDataGenerator (bundled tpcgen-cli)
│   ├── tpcds.py                 # TPCDSDataGenerator (bundled tpcgen-cli; DuckDB fallback)
│   └── clickbench.py            # Downloads dataset from ClickHouse host
│
├── engines/
│   ├── base.py                  # BaseEngine ABC — fsspec, runtime detection, result writing
│   ├── spark.py                 # Generic Spark engine
│   ├── fabric_spark.py          # Microsoft Fabric Spark (auto-authenticates via notebookutils)
│   ├── synapse_spark.py         # Azure Synapse Spark
│   ├── hdi_spark.py             # HDInsight Spark
│   ├── duckdb.py                # DuckDB
│   ├── polars.py                # Polars
│   ├── daft.py                  # Daft
│   ├── sail.py                  # Sail (PySpark-compatible engine)
│   └── delta_rs.py              # Shared DeltaRs write helper (used by non-Spark engines)
│
└── utils/
    ├── query_utils.py           # transpile_and_qualify_query(), get_table_name_from_ddl()
    ├── path_utils.py            # abfss_to_https(), to_unix_path()
    ├── schema_utils.py          # DDL -> reader schemas for the native TPC text format
    └── timer.py                 # Context-manager timer; stores results for post_results()
```

---

## Core Abstractions

### `BaseEngine` (`engines/base.py`)
Abstract base for all compute engines.

| Attribute | Description |
|---|---|
| `SQLGLOT_DIALECT` | SQLGlot dialect string for auto-transpilation (e.g. `"duckdb"`) |
| `SUPPORTS_SCHEMA_PREP` | Whether the engine can create an empty schema-defined table before data load |
| `SUPPORTS_MOUNT_PATH` | Whether the engine can use mount-style URIs (`/mnt/...`) |
| `TABLE_FORMAT` | Always `'delta'` |
| `schema_or_working_directory_uri` | Base path where Delta tables are stored |
| `storage_options` | Dict passed through to DeltaRs / fsspec for cloud auth |
| `extended_engine_metadata` | Dict of key/value pairs appended to benchmark results |

Key methods: `get_total_cores()`, `get_compute_size()`, `get_job_cost(duration_ms)`, `create_schema_if_not_exists()`, `_append_results_to_delta()`.

Runtime is auto-detected at init via `_detect_runtime()` — returns `"fabric"`, `"synapse"`, `"databricks"`, `"colab"`, or `"local_unknown"`.

### `BaseBenchmark` (`benchmarks/base.py`)
Abstract base for all benchmarks.

| Attribute | Description |
|---|---|
| `BENCHMARK_IMPL_REGISTRY` | `Dict[EngineClass → ImplClass]` — maps engines to optional engine-specific implementations |
| `RESULT_SCHEMA` | Canonical 24-column result schema (see below) |
| `VERSION` | Benchmark version string |

The result schema includes: `run_id`, `run_datetime`, `lakebench_version`, `engine`, `engine_version`, `benchmark`, `benchmark_version`, `mode`, `scale_factor`, `scenario`, `total_cores`, `compute_size`, `phase`, `sub_phase`, `test_item`, `start_datetime`, `duration_ms`, `estimated_retail_job_cost`, `iteration`, `success`, `error_message`, `sql_text`, `engine_properties` (MAP), `execution_telemetry` (MAP).

`sql_text` holds the exact SQL string handed to the engine, after normalization
and transpilation. It is set from `TimerContext.sql_text` and is populated for
the `Query` phase of every load-and-query benchmark (TPC-H, TPC-DS,
ClickBench), including failed queries. It is NULL for test items that do not
execute a single SQL statement — load, optimize, analyze, and all ELTBench
phases, some of which use DataFrame APIs rather than SQL.

`post_results()` collects timer results → builds result rows → optionally appends to a Delta table via `engine._append_results_to_delta()`.

---

## Engine & Benchmark Registration

Benchmarks declare engine support via `BENCHMARK_IMPL_REGISTRY`. If an engine uses only shared `BaseEngine` methods, the value is `None`; otherwise it maps to a specialized implementation class.

```python
# Register a custom engine with an existing benchmark
from lakebench.benchmarks import TPCDS
TPCDS.register_engine(MyNewEngine, None)           # use shared methods
TPCDS.register_engine(MyNewEngine, MyTPCDSImpl)   # use custom impl class
```

To add a new engine, subclass an existing one:
```python
from lakebench.engines import BaseEngine

class MyEngine(BaseEngine):
    SQLGLOT_DIALECT = "duckdb"  # or whichever dialect applies
    ...

from lakebench.benchmarks.elt_bench import ELTBench
ELTBench.register_engine(MyEngine, None)
benchmark = ELTBench(engine=MyEngine(...), ...)
benchmark.run()
```

---

## Query Resolution Strategy

Per-benchmark rule inventories and rationale live in `docs/query-normalization/`
(`README.md` for the shared pipeline, plus `tpch.md`, `tpcds.md`,
`clickbench.md`). Update the relevant page whenever rules change. The
invariants below must hold regardless.

TPC-H and TPC-DS always load immutable generated ANSI from
`resources/queries/canonical/sf<scale>/q*.sql`. SQL-file overrides are not searched.
Both use SQLGlot's built-in `tsql` reader through `parse_tpc_ansi_statements`,
with minimal generator-specific lexical adaptations; there is no custom dialect.
The reader accepts TOP and preserves typed, non-safe division and COUNT syntax.
Its implicit NULL ordering is first for ascending and last for descending;
explicit NULL ordering is preserved. Never serialize to intermediate T-SQL:
render the normalized AST directly to the engine's target dialect.
SQLGlot is pinned to 30.18.0 on Python 3.9+ and 26.30.0 on Python 3.8.
Maintain adapters for both AST layouts (FROM/WITH keys, DROP target lists, and
GROUPING nodes). Review SQL diffs before updating the 750 Spark/T-SQL/Fabric output
fingerprints in tests/fixtures/tpc_query_rendering.json or the 215 in
tests/fixtures/clickbench_query_rendering.json. Both fixtures share a
`versions`/`sha256`/`overrides` shape, where `overrides` records only the
queries a given pinned SQLGlot version renders differently; run the rendering
tests under both pins. Version-dependent cosmetic rendering differences belong
in `overrides`, never in a normalizer rule.
The runtime stages are source parsing, registered `SOURCE_NORMALIZERS` (including
TPC-H q15 lowering), `QUERY_NORMALIZERS`, `ENGINE_QUERY_NORMALIZERS`, then direct
AST qualification and target rendering. Each registry applies `"*"` before the
query ID; engine registries are keyed by class and inherited base-first.
Rule context.dialect is the source dialect; context.target_dialect is the
engine's output dialect. Do not normalize identifier case before case-sensitive
binding checks.
Shared `normalize_date_interval_arithmetic` lowers DATE casts +/- whole
DAY/MONTH/YEAR intervals to portable `DateAdd` nodes.
Subtraction uses a negative amount with an `exp.Neg` node, not `DateSub`
(unsupported T-SQL output) or a negative numeric Literal (invalid DuckDB output).
Generated dates, magnitudes, and operation directions must be preserved.
Overflow fixes must widen the aggregate input, never cast its result: casting
after aggregation cannot prevent INT overflow inside COUNT/SUM/AVG. AVG must
widen to a real type; BIGINT or DECIMAL(38,0) stop the overflow but leave T-SQL
truncating the average.
Neither TPC benchmark registers join normalization by default: WHERE join
predicates remain in place even if SQLGlot renders commas as CROSS JOIN. The shared
join-normalization function is retained for explicit registration. Compatibility fixes must be
registered structural rules, preserve generated substitutions, bump the module's
`NORMALIZER_VERSION`, and document semantic accommodations on the benchmark's
doc page. Per-query execution telemetry records applied rule IDs.

ClickBench uses the same pipeline with the official ClickHouse query set as its
immutable source: `resources/queries/canonical/q1..q43.sql` are the exact lines
of upstream `clickhouse/queries.sql` at a pinned commit, one statement per file,
parsed with SQLGlot's `clickhouse` reader. SQL-file overrides are not searched.
`source_manifest.json` pins the commit, blob hash, and per-query hashes; never
edit a canonical query, and regenerate the manifest if the pin moves.
Do not emit OCTET_LENGTH for DuckDB: it is BLOB-only there, and upstream's own
per-engine query sets use each engine's native `length`. Engine-specific DDL
fallback is unchanged.

Tables are automatically qualified with catalog and schema when applicable. To inspect the resolved query:

```python
benchmark = TPCH(engine=MyEngine(...))
print(benchmark._return_query_definition('q14'))
```

---

## Optional Dependency Groups

Install only what you need:

| Extra | Installs |
|---|---|
| `duckdb` | `duckdb`, `deltalake`, `pyarrow` |
| `polars` | `polars`, `deltalake`, `pyarrow` |
| `daft` | `daft`, `deltalake`, `pyarrow` |
| `tpcds_datagen` | Compatibility extra; generator is bundled in supported platform wheels |
| `tpcds_duckdb_datagen` | Legacy DuckDB TPC-DS generator |
| `tpch_datagen` | Compatibility extra; generator is bundled in supported platform wheels |
| `sparkmeasure` | `sparkmeasure` |
| `sail` | `pysail`, `pyspark[connect]`, `deltalake`, `pyarrow` |

```bash
pip install lakebench[duckdb,polars,tpch_datagen]
```

---

## Supported Runtimes & Storage

**Runtimes**: Local (Windows), Microsoft Fabric, Azure Synapse, HDInsight, Google Colab (experimental)

**Storage**: Local filesystem, OneLake, ADLS Gen2 (Fabric/Synapse/HDInsight), S3 (experimental), GCS (experimental)

**Table format**: Delta Lake only (via `delta-rs` for non-Spark engines)

---

## Timer (`utils/timer.py`)

`timer` is a context-manager function with a `.results` list attached. Use it inside benchmark `run()` implementations to time each phase/test item:

```python
with self.timer(phase="load", test_item="q1", engine=self.engine) as t:
    t.execution_telemetry = {"rows": 1000}   # optional metadata
    t.sql_text = "SELECT ..."                # optional; lands in the sql_text column
    do_work()

self.post_results()   # flush timer.results → self.results → optionally Delta
```

Set `sql_text` *before* executing, so the statement is still recorded when the
engine raises.

---

## Key Conventions

- **All Delta writes for non-Spark engines** go through `engines/delta_rs.py` (`DeltaRs().write_deltalake(...)`).
- **SQLGlot transpilation** is the default path; TPC-H/TPC-DS compatibility changes must use registered AST rules, never engine-specific SQL files.
- **`storage_options`** on `BaseEngine` is the single place for cloud auth credentials (bearer token, SAS, etc.).
- **`extended_engine_metadata`** on `BaseEngine` is the right place to attach runtime-specific metadata that ends up in the `engine_properties` MAP column of results.
- **TPC-DS / TPC-H spec compliance**: LakeBench intentionally diverges from `spark-sql-perf` to follow the official specs (see `customer.c_last_review_date_sk` and `store.s_tax_percentage` fixes in README).
- **New benchmarks** should subclass `BaseBenchmark`, define `RESULT_SCHEMA`, `BENCHMARK_IMPL_REGISTRY`, `VERSION`, and implement `run()`.

## Native TPC Generator Format

TPC-H and TPC-DS support the generators' pipe-delimited text output alongside
Parquet: `TPCxDataGenerator(output_format="native")` and
`TPCx(input_format="native")`. TPC-DS uses subcommand/extension `dat`, TPC-H
uses `tbl`; `tpcgen-cli tpcds dat` rejects `--num-threads`, and neither native
subcommand accepts `--compression` or `--row-group-bytes`.

The format has no header and no types, so reader schemas come from the
benchmark's resolved DDL via `utils/schema_utils.py`. Every generated line ends
with a trailing delimiter, so readers declare one extra trailing column
(`TRAILING_DELIMITER_COLUMN`) and drop it; quoting must be disabled and empty
fields must read as NULL. Engines implement `load_delimited_to_delta`; `Sail`
subclasses `BaseEngine` rather than `Spark`, so it needs its own copy. Daft's
CSV reader mislabels decimal precision, so decimals are read as text and cast.

Native part counts use `NATIVE_SIZE_FACTOR_DICT` (native bytes ÷ uncompressed
Parquet bytes, measured per table at SF1). Do not derive these from
`SF1000_SIZE_GB_DICT` at a different scale factor: that dict assumes linear
scaling, which is false for TPC-DS's fixed-size dimensions.
