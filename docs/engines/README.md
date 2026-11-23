# Engine documentation

LakeBench engines provide a consistent benchmark interface while preserving each runtime's native storage, authentication, telemetry, and pricing behavior.

| Engine | Install | Primary runtime | Documentation | Coverage |
|---|---|---|---|---|
| Generic Spark | `lakebench[spark]` | Local or Spark runtime | [Spark](spark/README.md) | [Report](../../reports/coverage/spark.md) |
| Fabric Spark | Runtime-provided Spark | Microsoft Fabric | [Fabric Spark](fabric-spark/README.md) | — |
| Synapse Spark | Runtime-provided Spark | Azure Synapse | [Synapse Spark](synapse-spark/README.md) | — |
| HDInsight Spark | Runtime-provided Spark | Azure HDInsight | [HDInsight Spark](hdi-spark/README.md) | — |
| Databricks Spark | `lakebench[databricks_spark]` | Databricks on AWS, Azure, or GCP | [Databricks Spark](databricks-spark/README.md) | — |
| Databricks SQL Warehouse | `lakebench[databricks_sql_warehouse]` | Databricks SQL | [Databricks SQL Warehouse](databricks-sql-warehouse/README.md) | — |
| Fabric Data Warehouse | `lakebench[fabric_data_warehouse]` | Microsoft Fabric | [Fabric Data Warehouse](fabric-data-warehouse/README.md) | — |
| DuckDB | `lakebench[duckdb]` | Local or notebook runtime | [DuckDB](duckdb/README.md) | [Report](../../reports/coverage/duckdb.md) |
| Polars | `lakebench[polars]` | Local or notebook runtime | [Polars](polars/README.md) | [Report](../../reports/coverage/polars.md) |
| Daft | `lakebench[daft]` | Local or notebook runtime | [Daft](daft/README.md) | [Report](../../reports/coverage/daft.md) |
| Sail | `lakebench[sail]` | Local or notebook runtime | [Sail](sail/README.md) | [Report](../../reports/coverage/sail.md) |

All engines write benchmark results through the same shared result contract. A benchmark accepts the engine instance, so the benchmark workflow remains the same after engine construction:

```python
from lakebench.benchmarks import TPCH

benchmark = TPCH(
    engine=engine,
    scenario_name="sf10",
    scale_factor=10,
    input_folder_uri="path-or-cloud-uri/tpch/sf10",
)
benchmark.run()
```

## Destructive reset boundary (compatibility change)

**Loading a benchmark or running ELTBench requires `allow_destructive_reset=True`
on the engine.** This is an explicit declaration that its entire configured
working directory or schema is dedicated, disposable benchmark output. It is
not permission to reset a shared location. Existing configurations without the
opt-in now raise `ValueError` before preparing tables or loading data; query-only
runs (`mode="query"` or `"power_test"`) do not reset storage and need no opt-in.

```python
from lakebench.engines import DuckDB
from lakebench.benchmarks import TPCH

engine = DuckDB(
    schema_or_working_directory_uri="/tmp/lakebench/tpch-output",
    allow_destructive_reset=True,  # This directory and everything in it is disposable.
)
benchmark = TPCH(
    engine=engine,
    scenario_name="sf1",
    scale_factor=1,
    input_folder_uri="/tmp/lakebench/tpch-input",
    result_table_uri="/tmp/lakebench/results",
    save_results=True,
)
benchmark.run(mode="load_and_query")
benchmark.run(mode="load_and_query")  # Same dedicated output can be reset on repeat runs.
benchmark.run(mode="query")           # Reuse the loaded data without resetting it.
```

The boundary is the **whole directory** for filesystem-backed engines and
**one schema, including its tables/views**, for schema-backed engines. Spark,
Databricks and Warehouse resets can drop objects; never register unrelated
objects in the dedicated schema. Polars, Daft and Sail retain their existing
per-table overwrite behavior, but require the same consent and destination
preflight; they do not gain recursive directory cleanup.

Consent cannot override these refusals:

1. Filesystem/drive/share/bucket/container roots, local home/current-directory
   ancestors, mount roots and known shared runtime boundaries (including
   `/lakehouse/default/Files` and `/lakehouse/default/Tables`). OneLake targets
   must be below `workspace/item/Files-or-Tables/dedicated-child`.
2. Default/shared/system schemas, including `default`, `dbo`, `public`, `main`,
   `sys` and `information_schema`. Use a schema such as `lakebench_tpch`, not
   the runtime's default schema. Reset identifiers must be single, unquoted
   names, not qualified SQL or wildcard expressions.
3. Input or result locations equal to, inside, or containing the output path,
   even when result saving is disabled. Local real paths (including symlinks),
   path components and common cloud URI aliases are compared, not raw string
   prefixes. Sibling paths such as `tpch-input` and `tpch-output` are valid.
4. Unsupported/ambiguous URI schemes or unverifiable Spark/Databricks schema
   locations. Existing schema/table locations are checked before any `DROP`;
   newly created schemas' resolved locations are checked before preparing
   tables, including when storage is inherited from the catalog. A failed
   post-create check leaves the empty schema in place and raises an error.

Use separate, fully qualified locations consistently. The guard cannot infer
that a runtime mount and a cloud URI, or different cloud workspace/item aliases,
refer to the same physical data. This is a configuration safety boundary, not a
replacement for storage permissions, backups or exclusive access: do not run
concurrent resets against the same target or change aliases during a run.

Direct `engine.create_schema_if_not_exists()` calls now default to
`drop_before_create=False` and preserve existing contents. To request a reset
directly, use an opted-in engine and pass `drop_before_create=True` plus
`protected_uris=[input_uri, result_uri]`; benchmark callers supply these
protections automatically. No directory is silently redirected or automatically
claimed as owned. To migrate, choose dedicated output, move input/results to
siblings, then opt in. Remove the opt-in to disable future benchmark resets.

## Shared pricing contract

Engines backed by vCore compute can accept either `cost_per_vcore_hour` or `cost_per_hour`; the inputs are mutually exclusive. A total hourly rate should include every cost component the user wants represented in `estimated_retail_job_cost`.

Automatic estimates use public list prices and are not invoice calculations. They do not account for negotiated discounts, commitments, credits, taxes, or other private billing adjustments.

## Adding an engine

Contributor guidance for custom engines remains in the root [README](../../README.md). Internal helpers such as `DeltaRs` do not have standalone engine pages.
