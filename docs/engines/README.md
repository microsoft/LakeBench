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

## Shared pricing contract

Engines backed by vCore compute can accept either `cost_per_vcore_hour` or `cost_per_hour`; the inputs are mutually exclusive. A total hourly rate should include every cost component the user wants represented in `estimated_retail_job_cost`.

Automatic estimates use public list prices and are not invoice calculations. They do not account for negotiated discounts, commitments, credits, taxes, or other private billing adjustments.

## Adding an engine

Contributor guidance for custom engines remains in the root [README](../../README.md). Internal helpers such as `DeltaRs` do not have standalone engine pages.
