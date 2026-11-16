# Synapse Spark

`SynapseSpark` runs LakeBench in an Azure Synapse serverless Apache Spark pool.

## Support

- Benchmarks: ELTBench, TPC-DS, TPC-H, and ClickBench.
- Runtime: Azure Synapse Spark only.
- Storage: ADLS Gen2 and runtime-accessible mount paths.
- Authentication: the Synapse notebook/runtime identity.

## Installation

Synapse supplies PySpark. Install LakeBench in the notebook environment:

```python
%pip install lakebench
```

## Example

```python
from lakebench.engines import SynapseSpark

engine = SynapseSpark(
    schema_name="lakebench",
    schema_uri="abfss://container@account.dfs.core.windows.net/lakebench",
    allow_destructive_reset=True,
)
```

The whole schema and its contents must be dedicated disposable output, separate
from input/results. Read the [shared reset boundary](../README.md#destructive-reset-boundary-compatibility-change).

## Configuration

- `schema_name`: benchmark schema/database.
- `schema_uri`: Delta table root.
- `spark_measure_telemetry`: optional Spark Measure stage metrics.
- `cost_per_vcore_hour` or `cost_per_hour`: mutually exclusive manual pricing overrides.
- `tblproperties`: Delta table properties.

## Pricing and metadata

Without a manual rate, LakeBench queries the Azure Retail Prices API for the regional Azure Synapse serverless Spark vCore rate and derives the total hourly rate from active cores. Results also include the Synapse region, Spark history URL, and selected runtime configuration.

Construction fails outside a detected Synapse runtime.

Native TPC `.tbl` and `.dat` input is supported through the shared Spark loader.
