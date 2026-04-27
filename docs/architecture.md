# LakeBench Architecture

Internals reference: how the pluggable benchmark/engine system fits together,
how SQL queries are resolved per engine, and the invariants that make
cross-engine result tables comparable.

---

## Two pluggable axes: Benchmarks × Engines

The core abstraction is a registry mapping
`(Benchmark, Engine) → optional engine-specific implementation class`.
Each `BaseBenchmark` subclass holds a class-level dict
`BENCHMARK_IMPL_REGISTRY: Dict[Type[BaseEngine], Type]`.

- `None` value → the benchmark uses the engine's generic methods.
- Class value → a benchmark-specific subclass overrides behavior for that engine.

Adding a new engine: subclass `lakebench.engines.base.BaseEngine`
(or an existing engine like `Spark`). Register it with a benchmark:

```python
from lakebench.benchmarks import TPCDS
TPCDS.register_engine(MyNewEngine, None)
```

`register_engine` is the only supported way to extend the registry — it lets
external "extension libraries" add custom engines/benchmarks without modifying
core.

---

## Source layout

| Path | Purpose |
|---|---|
| `src/lakebench/benchmarks/` | One subpackage per benchmark: `tpch/`, `tpcds/`, `clickbench/`, `elt_bench/`, `tpcdi/`. Each has a `resources/` tree with SQL queries (see resolution below) and schema definitions. Shared load/query plumbing lives under `_load_and_query/`. |
| `src/lakebench/engines/` | One module per engine: `duckdb`, `polars`, `daft`, `spark` (generic), `fabric_spark`, `synapse_spark`, `hdi_spark`, `databricks`, `spark_connect`, `sail`, `livy`, plus `delta_rs`. Each engine declares a `SQLGLOT_DIALECT` constant used for SQL transpilation. |
| `src/lakebench/datagen/` | Data generators: `tpch.py` (wraps `tpchgen-cli`), `tpcds.py` (wraps DuckDB's TPC-DS extension; targets ~128MB row groups by default), `clickbench.py` (downloads from ClickHouse host), `tpcdi.py`, plus `_tpc.py` / `_tpc_rs.py` shared helpers. |
| `src/lakebench/utils/` | `path_utils.py`, `query_utils.py` (SQLGlot transpilation + query resolution), `timer.py` (phase timing). |
| `src/lakebench/cli.py`, `config.py` | CLI and the `.lakebench.json` profile loader (two-tier: `~/.lakebench.json` global + `./lakebench.json` project override). |
| `src/lakebench/results.py`, `reporting.py` | Result schema (`BaseBenchmark.RESULT_SCHEMA`) and Delta result-table writing. |
| `tests/integration/` | One file per engine, each running TPC-H, TPC-DS, ClickBench, and ELTBench at SF 0.1. ClickBench reads the committed `tests/integration/data/clickbench_sample.parquet`. |

---

## Hierarchical SQL query resolution

For each engine/query, queries are resolved in this priority order —
understanding this is essential when working on benchmark queries:

1. **Engine-specific override**:
   `benchmarks/<bench>/resources/queries/<engine>/qN.sql`
   (e.g. `tpch/resources/queries/daft/q14.sql` works around Daft's
   decimal-multiplication issues).
2. **Parent engine class override**: e.g. `.../queries/spark/qN.sql`
   (rarely used today).
3. **Canonical + SQLGlot transpilation** (the common case):
   `.../queries/canonical/qN.sql` is written in SparkSQL and transpiled to the
   engine's `SQLGLOT_DIALECT` at runtime.

Tables are auto-qualified with catalog/schema where applicable. To inspect
what will actually run:

```python
print(benchmark._return_query_definition('q14'))
```

When adding queries, prefer extending the canonical form. Only add an
engine-specific override when transpilation cannot produce a valid query
(e.g. Polars lacks non-equi joins; Daft lacks `DATE_ADD`, `CROSS JOIN`,
subqueries, `CASE` with operand).

---

## Result schema invariants

`BaseBenchmark.RESULT_SCHEMA` is the canonical column list for the results
Delta table. Fields like `engine_properties` and `execution_telemetry` are
`MAP<STRING,STRING>` for engine-specific metadata. When extending benchmarks,
append to existing rows via these maps rather than introducing new top-level
columns to keep cross-engine result tables uniform.

---

## Storage / table format

- Only **Delta Lake** is currently supported as a table format.
- Storage backends: local filesystem, OneLake, ADLS gen2 (in
  Fabric / Synapse / HDInsight), and experimental S3/GS.

---

## Spark-Measure telemetry

When `spark_measure_telemetry=True` is passed to a Spark engine, install via
the `sparkmeasure` extra **and** install the Spark-Measure JAR from Maven
(`ch.cern.sparkmeasure:spark-measure_2.13:0.24`) on the cluster.

---

## BYO data caveats (TPC-DS / spark-sql-perf)

Datasets generated via Databricks `spark-sql-perf` have two schema bugs that
break LakeBench (it follows the spec strictly). Before use:

- `customer.c_last_review_date` (string) → rename/cast to
  `c_last_review_date_sk` (int).
- `store.s_tax_precentage` → rename to `s_tax_percentage`.

See `README.md` "Is BYO Data Supported?" for the exact PySpark fix snippets.

---

## Pass/fail semantics for integration tests

- Individual query failure → `UserWarning`, test still passes.
- All queries fail OR all tables fail to load → test fails.
- Engine crash before any results → `UserWarning`, test still passes
  (graceful degradation).

This deliberately tolerates partial engine support so the suite can produce
coverage reports (`reports/coverage/<engine>.md`) rather than blocking CI on
known-unsupported queries.
