# Databricks Spark

`DatabricksSpark` runs LakeBench on Databricks clusters across AWS, Azure, and Google Cloud.

## Support

- Benchmarks: ELTBench, TPC-DS, TPC-H, and ClickBench.
- Compute: Jobs Compute and All-Purpose Compute.
- Runtime: Databricks Runtime with PySpark in **Dedicated** access mode.
- Storage: S3 on AWS, ADLS Gen2 on Azure, and Google Cloud Storage on GCP.
- Catalogs: Hive metastore or Unity Catalog.

## Installation

```python
%pip install lakebench
```

## Example

```python
from lakebench.engines import DatabricksSpark

engine = DatabricksSpark(
    catalog_name="main",
    schema_name="lakebench",
    schema_uri="abfss://container@account.dfs.core.windows.net/lakebench" #optional
)
```

Use `s3://...` on AWS or `gs://...` on GCP.

## Compute and access mode

LakeBench supports both Databricks Jobs Compute and All-Purpose Compute. The compute type is automatically detected and logged in the results.

The compute must use **Dedicated** access mode. LakeBench uses the driver `SparkContext` to set a job description for every timed phase and test item. Those descriptions include labels such as benchmark phase and query ID, making individual operations easy to identify in the Spark UI. Databricks compute modes that do not expose `SparkContext` cannot provide this labeling or the other Spark runtime metadata LakeBench requires.

## Configuration

- `catalog_name`: defaults to `hive_metastore`; use a Unity Catalog name when applicable.
- `schema_name` and `schema_uri`: benchmark table namespace and Delta root.
- `cost_per_hour` or `cost_per_vcore_hour`: mutually exclusive manual rates.
- `spark_measure_telemetry` and `tblproperties`: shared Spark options.

## Pricing

### Azure

For fixed-size clusters, LakeBench combines:

1. Driver and worker DBUs/hour from the published workload-, Photon-, and VM-specific mapping.
2. The regional DBU price from Azure Retail Prices.
3. Regional driver and worker VM infrastructure prices from Azure Retail Prices.

Autoscaling or missing cluster metadata causes a warning and leaves cost unset unless a manual rate is supplied. Public prices do not include negotiated discounts or commitments.

Refresh the generated mapping with:

```bash
uv run python scripts/refresh_databricks_azure_dbu_map.py
```

### AWS and GCP

Supply an all-inclusive `cost_per_hour` or `cost_per_vcore_hour`. The rate should include both Databricks DBUs and cloud infrastructure. Without a manual rate, the benchmark continues and estimated cost remains unset.

Native TPC `.tbl` and `.dat` input is supported through the shared Spark loader.
