# Fabric Spark

`FabricSpark` specializes the generic Spark engine for Microsoft Fabric notebooks and Lakehouses.

## Support

- Benchmarks: ELTBench, TPC-DS, TPC-H, and ClickBench.
- Runtime: Microsoft Fabric Spark.
- Storage: OneLake and Lakehouse schemas.
- Authentication: the Fabric notebook identity and runtime-provided `notebookutils`.

## Installation

Fabric supplies PySpark. Install LakeBench in the notebook:

```python
%pip install lakebench
```

For stage telemetry:

```python
%pip install "lakebench[sparkmeasure]"
```

## Example

```python
from lakebench.engines import FabricSpark

engine = FabricSpark(
    lakehouse_name="lakehouse",
    lakehouse_schema_name="lakebench",
)
```

## Configuration

- `lakehouse_name`: Fabric Lakehouse name.
- `lakehouse_schema_name`: schema used for benchmark tables.
- `spark_measure_telemetry`: enables Spark Measure stage metrics.
- `collect_stats_on_write`: enables Fabric Delta extended statistics during writes; defaults to `True`.
- `cost_per_vcore_hour` or `cost_per_hour`: mutually exclusive manual overrides.
- `tblproperties`: Delta properties applied at table creation.

`compute_stats_all_cols` is a deprecated alias for `collect_stats_on_write`. Explicit benchmark statistics should use the benchmark's `analyze` option.

## Pricing and metadata

When no manual rate is supplied, LakeBench derives a Fabric vCore rate from visible capacity metadata. Results include the capacity ID, computed hourly cost, Spark history URL, runtime version, and relevant Fabric Spark configuration.

Fast optimize is temporarily disabled while LakeBench runs explicit compaction so the measured operation uses the intended Delta optimization path.

Native TPC `.tbl` and `.dat` input is supported through the shared Spark loader.
