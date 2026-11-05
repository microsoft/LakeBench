# Databricks Spark

`DatabricksSpark` runs LakeBench on Databricks clusters and serverless compute across AWS, Azure, and Google Cloud.

## Support

- Benchmarks: ELTBench, TPC-DS, TPC-H, and ClickBench.
- Compute: Jobs Compute, All-Purpose Compute, and serverless compute (notebooks and jobs).
- Access mode: **Dedicated** or **Standard** (shared).
- Storage: S3 on AWS, ADLS Gen2 on Azure, and Google Cloud Storage on GCP.
- Catalogs: Hive metastore or Unity Catalog. Serverless requires Unity Catalog.

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

LakeBench supports Databricks Jobs Compute, All-Purpose Compute, and serverless compute. The compute type is detected automatically and logged in the results as `workload_type`.

Both **Dedicated** and **Standard** (shared) access modes are supported. Their differences:

| Capability | Dedicated | Standard (shared) | Serverless |
|---|---|---|---|
| Phase labels | Spark UI job descriptions | Spark Connect operation tags (not shown in query history) | Spark Connect operation tags (not shown in query history) |
| `spark_measure_telemetry` | ✅ | ❌ (needs `SparkContext`) | ❌ (needs `SparkContext`) |
| `total_cores` / `compute_size` | From live executors | From cluster tags | `0` / `Serverless` |

### Serverless compute

Serverless is detected from the `IS_SERVERLESS` environment variable. Pass `serverless=True` to override detection. On serverless:

- `catalog_name` must be a Unity Catalog catalog.
- Photon and Unity Catalog are always reported as enabled.
- `cost_per_vcore_hour` isn't supported. See [Serverless pricing](#serverless).
- `VACUUM` with `retention_check=False` can't disable the retention check, because serverless restricts Spark configs. A warning is logged.

```python
engine = DatabricksSpark(catalog_name="main", schema_name="lakebench")
```

## Configuration

- `catalog_name`: defaults to `hive_metastore`; use a Unity Catalog name when applicable.
- `schema_name` and `schema_uri`: benchmark table namespace and Delta root.
- `cost_per_hour` or `cost_per_vcore_hour`: mutually exclusive manual rates.
- `spark_measure_telemetry` and `tblproperties`: shared Spark options.
- `serverless`: override serverless detection.
- `job_run_id` and `notebook_id`: override the serverless billing attribution IDs that are otherwise read from the notebook context.

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

### Serverless

Serverless is billed by consumed DBUs, not an hourly rate, so `estimated_retail_job_cost` stays unset during the run (`pricing_source=billing_lookup_pending`). Pass `cost_per_hour` to use a manual rate instead.

To get the actual cost after the run, look up usage in `system.billing.usage`, joined to `system.billing.list_prices`:

```python
benchmark.run()

billed = engine.get_billed_cost()  # window defaults to engine creation -> now
print(billed.dbus, billed.list_cost_usd, billed.may_be_incomplete)

# Optional: spread the billed total across result rows by duration (an approximation).
rows = engine.backfill_results_cost(benchmark.results, billed)
```

- Usage is attributed by job run ID on jobs and by notebook ID in interactive notebooks. Notebook attribution includes all serverless usage from that notebook during the window, so run LakeBench by itself for accurate numbers.
- Billing data can take a few hours to appear. `may_be_incomplete` is `True` when the window ended less than 3 hours ago. Re-run the lookup later with explicit `start_time` / `end_time` (e.g. from `run_datetime` in the results).
- Requires `SELECT` on `system.billing.usage` and `system.billing.list_prices`. Costs are list prices and don't include negotiated discounts.

Native TPC `.tbl` and `.dat` input is supported through the shared Spark loader.
