# Generic Spark

The generic `Spark` engine runs LakeBench against a local Spark session or an existing Spark runtime.

## Support

- Benchmarks: ELTBench, TPC-DS, TPC-H, and ClickBench.
- Storage: local files, mount paths, and storage exposed through the runtime's Hadoop configuration.
- Table format: Delta Lake.
- Coverage: [Spark coverage report](../../../reports/coverage/spark.md).

## Installation

```bash
pip install "lakebench[spark]"
```

Install `lakebench[sparkmeasure]` as well when using stage telemetry.

## Example

```python
from lakebench.engines import Spark

engine = Spark(
    schema_name="lakebench",
    schema_uri="file:///tmp/lakebench",
)
```

For an existing managed Spark runtime, use that runtime's schema path and optional catalog:

```python
engine = Spark(
    catalog_name="catalog",
    schema_name="lakebench",
    schema_uri="abfss://container@account.dfs.core.windows.net/lakebench",
)
```

## Configuration

- `schema_name`: schema/database used for benchmark tables.
- `catalog_name`: optional metastore catalog.
- `schema_uri`: Delta table root; required for local Spark.
- `spark_measure_telemetry`: captures stage metrics when Spark Measure is installed and configured.
- `cost_per_vcore_hour` or `cost_per_hour`: mutually exclusive manual pricing inputs.
- `tblproperties`: Delta table properties applied during table creation.

Use the benchmark's `analyze` option for statistics. `compute_stats_all_cols` is deprecated.

## Runtime behavior

When LakeBench detects a local runtime, it creates a local Spark session with Delta extensions and uses all local cores. On a managed runtime it reuses the runtime's Spark session and authentication.

Spark Measure also requires the matching Spark Measure JAR. See the [sparkmeasure package documentation](https://github.com/LucaCanali/sparkMeasure).

Native TPC `.tbl` and `.dat` input is supported.
