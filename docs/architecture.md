# LakeBench Architecture

Internals reference for contributors. Covers the pluggable benchmark/engine
system, the CLI/profile/results/reporting layer that sits on top, query
resolution, the engine base contract, and the invariants that keep
cross-engine result tables comparable.

If you only want to *use* LakeBench, see
[`cli-quickstart.md`](./cli-quickstart.md) and the README. If you only want
to *run tests*, see [`development.md`](./development.md).

---

## Top-level shape

```
                ┌────────────────────────────────────────────┐
                │            CLI (lakebench …)               │
                │  cli.py · config.py · discover.py          │
                │  results.py · reporting.py                 │
                └────────────────┬───────────────────────────┘
                                 │ instantiates
                ┌────────────────┴──────────────┐
                │     BENCHMARK_IMPL_REGISTRY   │
                │  (benchmark, engine) → impl   │
                └────────────────┬──────────────┘
              instantiates       │      instantiates
              ┌─────────┐        │        ┌─────────┐
              │BaseBench│◀───────┴───────▶│BaseEng. │
              └─────────┘                 └─────────┘
              tpch / tpcds /              spark / duckdb /
              clickbench /                polars / daft /
              elt_bench /                 sail / fabric_spark /
              tpcdi                       synapse_spark / hdi_spark /
                                          databricks / spark_connect /
                                          livy / delta_rs
```

The CLI layer is **purely additive** — every `lakebench …` subcommand is a
thin wrapper around the same Python API (`profile → engine → benchmark.run()`).
Library consumers can keep using the Python API unchanged.

---

## Two pluggable axes: Benchmarks × Engines

The core abstraction is a class-level dict on each `BaseBenchmark` subclass:

```python
BENCHMARK_IMPL_REGISTRY: Dict[Type[BaseEngine], Optional[Type]]
```

- `None` value → use the engine's generic methods (the common case).
- Class value → a benchmark-specific subclass overrides behavior for that
  engine (used heavily for TPC-DI per-engine ETL implementations).

Adding a new engine: subclass `lakebench.engines.base.BaseEngine` (or an
existing engine like `Spark`). Register it with each benchmark you support:

```python
from lakebench.benchmarks import TPCDS
TPCDS.register_engine(MyNewEngine, None)
```

`register_engine` is the only supported way to extend the registry. External
"extension libraries" can add custom engines/benchmarks without modifying
core.

---

## Source layout

| Path | Purpose |
|---|---|
| `src/lakebench/benchmarks/` | One subpackage per benchmark: `tpch/`, `tpcds/`, `clickbench/`, `elt_bench/`, `tpcdi/`. Each has a `resources/` tree of SQL queries (see resolution below) and DDL. Shared load/query plumbing lives under `_load_and_query/`. |
| `src/lakebench/benchmarks/tpcdi/engine_impl/` | Per-engine TPC-DI ETL implementations (`spark.py`, `duckdb.py`, `polars.py`, `daft.py`, `sail.py`). TPC-DI's heterogeneous-source ETL doesn't reduce cleanly to a SQL query, so each engine gets its own implementation class registered against `TPCDI`. |
| `src/lakebench/engines/` | One module per engine: `duckdb`, `polars`, `daft`, `spark` (generic), `fabric_spark`, `synapse_spark`, `hdi_spark`, `databricks`, `spark_connect`, `sail`, `livy`, plus `delta_rs`. Each declares a `SQLGLOT_DIALECT` constant used for SQL transpilation. |
| `src/lakebench/datagen/` | Data generators: `tpch.py` (wraps `tpchgen-cli`), `tpcds.py` (wraps DuckDB's TPC-DS extension; targets ~128 MB row groups by default), `clickbench.py` (downloads from ClickHouse host), `tpcdi.py` (wraps the official `DIGen.jar`), plus shared `_tpc.py` / `_tpc_rs.py`. |
| `src/lakebench/utils/` | `path_utils.py`, `query_utils.py` (SQLGlot transpilation, multi-part name qualification), `timer.py` (phase timing). |
| `src/lakebench/cli.py` | The `lakebench` entry point. argparse-based; one function per command (`cmd_run`, `cmd_datagen`, `cmd_discover`, `cmd_doctor`, `cmd_results_*`, `cmd_report_*`, `cmd_profiles_*`, `cmd_list_modes`). |
| `src/lakebench/config.py` | Profile loader for `~/.lakebench.json` + `./lakebench.json`. Handles env-var expansion, `extends:` composition (cycle-detected), deep `engine_options` merge, validation, and `resolve_engine` / `resolve_benchmark` / `resolve_datagen` factories. |
| `src/lakebench/discover.py` | Catalog fingerprinting: takes a list of table names from a schema, scores each against the known table sets of TPC-H / TPC-DS / TPC-DI / ClickBench / ELTBench, returns confidence scores. Powers `lakebench discover`. |
| `src/lakebench/results.py` | `ResultsManager`: per-run record store under `~/.lakebench/results/<run_id>/`, with prefix-based ID resolution, tags, notes. |
| `src/lakebench/reporting.py` | `report_summary`, `report_compare`, `report_history`, `export_results` — formatted tables with `_format_duration`, delta-pct columns, etc. |
| `tests/integration/` | One file per engine. Each runs TPC-H, TPC-DS, ClickBench, and ELTBench at SF 0.1. ClickBench reads the committed `tests/integration/data/clickbench_sample.parquet`. |
| `tests/test_cli.py` | 100+ tests covering the full CLI surface. |
| `docs/` | This file plus `cli-quickstart.md`, `cli-reference.md`, `development.md`, `install-fabric.md`, `install-databricks.md`. |

---

## The CLI / profile / results layer

All three modules sit on top of the existing benchmark+engine API. They
exist so end users don't need to write a Python driver script per run.

### Profile resolution (`config.py`)

Two-tier lookup, with project-level overriding global:

1. **`~/.lakebench.json`** — global user defaults, shared across projects.
2. **`./lakebench.json`** — project-level, takes precedence.

A profile names an `engine` plus its `engine_options`, plus optional
`extends:` composition (deeply merged, cycle-detected). Env-var expansion
runs on every string value: `"$DATABRICKS_TOKEN"` → looked up at load time.
Tokens themselves are never stored; profiles only reference env-var *names*
(`token_env: "DATABRICKS_TOKEN"`).

Order of precedence at run time, lowest to highest:

```
profile defaults  →  profile fields  →  CLI flags (--mode, --scenario, …)
                                     →  -E key=val (engine option overrides)
                                     →  --conf key=val (Spark conf overrides)
```

`resolve_engine(profile)` instantiates the engine class. `resolve_benchmark`
and `resolve_datagen` do the same for benchmarks and datagens. Adding a new
engine to the CLI requires no CLI change — `config.py` resolves classes
dynamically by name.

### Catalog discovery (`discover.py`)

`fingerprint_schema(table_names)` Jaccard-scores the input against each
benchmark's known table set. `lakebench discover --profile <p>` calls
`engine.list_databases()` then `engine.list_tables(db)` and prints scored
matches. Useful for "what's already in this lakehouse?" before kicking off
a run.

This is why `BaseEngine` declares `list_databases()` / `list_tables(db)` —
overridden by Spark-family, DuckDB, and Livy.

### Results store (`results.py`)

Each run writes a directory under `~/.lakebench/results/<run_id>/`:

```
metadata.json     # engine, benchmark, scenario, scale, status, tags, notes, …
results.parquet   # per-query timing rows (ResultsManager-managed schema)
log.txt           # captured stdout/stderr
```

`ResultsManager` exposes `list/get/delete/tag/notes/purge/stats` plus prefix
ID resolution (so `lakebench results show abc1` matches `abc1234…`).
Run records are intentionally local-first — the cross-run reporting layer
(`reporting.py`) operates on this store, not on the result Delta table.

### Reporting (`reporting.py`)

- `report_summary(rm, run_id)` — single-run breakdown.
- `report_compare(rm, baseline, candidate)` — query-by-query delta with
  pct-change, sorted/highlighted.
- `report_history(rm, …)` — multi-run timeseries.
- `export_results(...)` — flatten to CSV/JSON/Parquet.

All of these are pure functions over `ResultsManager` records, so they're
testable without spinning up an engine.

---

## The engine base contract

`BaseEngine` (in `engines/base.py`) is the substrate every engine builds
on. Key surface:

| Member | Purpose |
|---|---|
| `SQLGLOT_DIALECT` | Required class constant. Names the SQLGlot dialect to transpile canonical SparkSQL into. |
| `SUPPORTS_SCHEMA_PREP` | If `True`, the engine can `CREATE SCHEMA` / `DROP SCHEMA` before a run. Set `False` for cluster-managed catalogs (e.g. Livy on Fabric uses the lakehouse's schema). |
| `query_timeout_seconds` | Optional per-query wall-clock cap. `None` = no LakeBench-imposed cap. Engines may translate this into engine-native cancellation. |
| `extended_engine_metadata` | Dict written into the result record (e.g. cluster ID, session ID). |
| `list_databases()` / `list_tables(db)` | Default raises `NotImplementedError`; overridden by Spark family, DuckDB, Livy. Powers `lakebench discover`. |
| `execute_sql_query` / `execute_sql_statement` | Workhorses. Subclasses route through engine-native APIs. |
| `load_parquet_to_delta` | Bulk load for benchmark setup. |
| `optimize_table` / `vacuum_table` / `create_schema_if_not_exists` / `_create_empty_table` | Lifecycle hooks called by benchmark phases. |

### Engine families

- **Local in-process**: `DuckDB`, `Polars`, `Daft`, `Sail` — execute in the
  current Python process; talk to local files or object storage via their
  own connectors.
- **Local SparkSession**: `Spark` — embedded JVM, used for Spark-flavored
  benchmarks against local data.
- **Workspace-tagged Spark**: `FabricSpark`, `SynapseSpark`, `HDISpark` —
  thin subclasses of `Spark` that record workspace identity in
  `extended_engine_metadata`. They run *inside* the corresponding cluster
  (you submit the driver script there).
- **Remote-via-protocol** (added by the CLI work):
  - **`SparkConnect`** — generic Spark Connect client (`sc://host:port`).
  - **`Databricks`** — `databricks-connect` against a Databricks cluster.
    Includes 3-phase auto-alignment to keep the installed
    `databricks-connect` major.minor in sync with the cluster's DBR
    (proactive REST check → reactive on `ImportError` → reactive on the
    cluster's "Unsupported combination …" rejection). On mismatch it
    `pip install --force-reinstall`s the matching wheel and `os.execvpe`s
    the current process so the new `pyspark` loads cleanly. A sentinel
    env var (`LAKEBENCH_DATABRICKS_REEXECED`) prevents re-exec loops.
  - **`Livy`** — Apache Livy REST. Submits PySpark snippets to a remote
    session. No local SparkSession. Supports OSS Livy, HDInsight, Synapse,
    and Fabric. Auth: `none` / `basic` / `kerberos` / `bearer` / `az`
    (Azure CLI token, refreshed before expiry). Per-statement timeout
    POSTs to the cancel endpoint and marks the session "wedged"; the next
    call recreates the session before submitting.

### Endpoint-specific quirks (Livy)

The Livy engine sniffs the URL host to inject endpoint-specific behavior:

- **Synapse** (`*.azuresynapse.net`) — its session-create API rejects
  payloads missing `spark.executor.instances`, even with dynamic
  allocation. The engine auto-defaults it to
  `spark.dynamicAllocation.minExecutors` (or `2` if unset).
- **Fabric / HDInsight / OSS Livy** — no such injection.

This is the pattern to follow for any future endpoint-flavor-specific
workarounds: detect via host suffix in a `_is_<flavor>_endpoint()` helper,
mutate the payload before submission.

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
   `.../queries/canonical/qN.sql` is written in SparkSQL and transpiled to
   the engine's `SQLGLOT_DIALECT` at runtime.

Tables are auto-qualified with catalog/schema where applicable — the
qualifier supports **multi-part names** (e.g. Fabric's
`workspace.lakehouse.schema`, Unity Catalog's `catalog.schema`). This is
the bug fix that made the new cloud engines work cleanly; the previous
qualifier only handled two-part names.

To inspect what will actually run:

```python
print(benchmark._return_query_definition('q14'))
```

When adding queries, prefer extending the canonical form. Only add an
engine-specific override when transpilation cannot produce a valid query
(e.g. Polars lacks non-equi joins; Daft lacks `DATE_ADD`, `CROSS JOIN`,
subqueries, `CASE` with operand).

---

## Result schema invariants

`BaseBenchmark.RESULT_SCHEMA` is the canonical column list for the optional
results Delta table (separate from the local `~/.lakebench/results/` store).
Fields like `engine_properties` and `execution_telemetry` are
`MAP<STRING,STRING>` for engine-specific metadata.

When extending benchmarks, **append to existing rows via these maps** rather
than introducing new top-level columns — this is what keeps cross-engine
result tables joinable and comparable.

---

## Storage / table format

- Only **Delta Lake** is currently supported as a table format.
- Storage backends: local filesystem, OneLake, ADLS gen2 (in
  Fabric / Synapse / HDInsight), and experimental S3 / GS.
- Engines that talk to remote storage accept a `storage_options` dict that
  is forwarded to the underlying connector (object-store credentials,
  endpoint overrides, etc.).

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

---

## Where to look next

- **`docs/development.md`** — how to set up a dev env, run tests, and
  navigate the codebase.
- **`docs/cli-reference.md`** — every CLI flag and subcommand.
- **`docs/cli-quickstart.md`** — 5-minute end-user tour.
- **`docs/install-fabric.md`** / **`docs/install-databricks.md`** —
  cloud-specific setup, including auth and profile examples.
