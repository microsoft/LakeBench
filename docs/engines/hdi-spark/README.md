# HDInsight Spark

`HDISpark` adapts the generic Spark engine for Azure HDInsight Spark clusters.

## Support

- Benchmarks: ELTBench, TPC-DS, TPC-H, and ClickBench.
- Runtime: HDInsight Spark.
- Storage: storage configured for the HDInsight cluster.
- Authentication: cluster/runtime credentials and Hadoop configuration.

## Installation

HDInsight supplies PySpark. Install LakeBench on the cluster or notebook:

```python
%pip install lakebench
```

## Example

```python
from lakebench.engines import HDISpark

engine = HDISpark(schema_name="lakebench")
```

## Configuration

- `schema_name`: benchmark schema/database.
- `spark_measure_telemetry`: optional Spark Measure stage metrics.
- `cost_per_vcore_hour` or `cost_per_hour`: mutually exclusive manual pricing inputs.
- `tblproperties`: Delta table properties.

## Pricing

HDInsight does not automatically assemble cluster pricing. Supply an all-inclusive `cost_per_hour` or a `cost_per_vcore_hour` appropriate for the cluster when estimated job cost is required.

Native TPC `.tbl` and `.dat` input is supported through the shared Spark loader.
