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
- `cost_per_hour` and `cost_per_vcore_hour` aren't supported. Serverless isn't billed at an hourly rate, so item-level cost isn't estimated. See [Serverless pricing](#serverless).
- `VACUUM` with `retention_check=False` can't disable the retention check, because serverless restricts Spark configs. A warning is logged.

```python
engine = DatabricksSpark(catalog_name="main", schema_name="lakebench")
```

## Configuration

- `catalog_name`: defaults to `hive_metastore`; use a Unity Catalog name when applicable.
- `schema_name` and `schema_uri`: benchmark table namespace and Delta root.
- `cost_per_hour` or `cost_per_vcore_hour`: mutually exclusive manual rates (classic compute only).
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

Serverless uses `run_billed` cost attribution. [Cost reporting](../../cost/README.md) explains the model, the post-run workflow and the shared `BilledCost` schema. Serverless is billed by consumed DBUs, not an hourly rate, so LakeBench doesn't estimate per-item cost on serverless. `estimated_retail_job_cost` stays unset (`pricing_source=billing_lookup_pending`), and `cost_per_hour` / `cost_per_vcore_hour` are rejected.

#### How the cost is looked up

- **Source:** `system.billing.usage`, priced with `system.billing.list_prices`. Each usage record is priced at the effective list price of its own SKU in effect at the time.
- **Usage included:** only DBU usage (`usage_unit = 'DBU'`, `product_features.is_serverless = true`). Other usage types, such as networking, are excluded.
- **Attribution:**
  - Jobs: `usage_metadata.job_run_id`. This is exact if the job run contains only this LakeBench run, so use one single-task job run per benchmark run.
  - Interactive notebooks: `usage_metadata.notebook_id`. This includes all serverless usage from that notebook during the window. Billing has no session ID, so for accurate numbers run benchmarks as jobs.
  - The IDs are read from the notebook context and stored in `engine_properties` as `job_run_id`, `job_id` and `notebook_id`. Override them with the `job_run_id` / `notebook_id` arguments.
- **Performance mode:** the cost reflects whichever serverless mode was billed, because each record carries its own SKU and quantity. `product_features.performance_target` is recorded as `performance_mode` (`STANDARD`, `PERFORMANCE_OPTIMIZED`, or `MIXED`, with a warning).
- **Lag:** billing data can take a few hours to arrive. `may_be_incomplete` is `True` when the window ended less than 3 hours ago.
- **Permissions:** `SELECT` on `system.billing.usage` and `system.billing.list_prices`.
- **Scope:** the total covers everything billed to the job run, including startup and result writes, not just timed phases.
- **`details` keys:** `sku_names`, `job_ids`, `performance_targets`, `currency_codes`.

#### Same session

```python
benchmark.run()
billed = engine.get_billed_cost(run_id=benchmark.header_detail_dict["run_id"])
```

#### Later session (scheduled cost job)

```python
from lakebench.billing import BILLED_COST_SCHEMA, run_window_from_results
from lakebench.engines.resources.databricks_pricing import query_billed_cost

rows = [r.asDict() for r in spark.read.format("delta").load(result_table_uri).where(f"run_id = '{run_id}'").collect()]
props = rows[0]["engine_properties"]
start, end = run_window_from_results(rows)
billed = query_billed_cost(
    spark,
    start_time=start,
    end_time=end,
    job_run_id=props.get("job_run_id"),
    notebook_id=props.get("notebook_id"),
    run_id=run_id,
    cloud_provider=props.get("cloud_provider"),
    compute_region=props.get("compute_region"),
    compute_type=props.get("workload_type"),
)
engine._append_results_to_delta(cost_table_uri, [billed.to_row()], BILLED_COST_SCHEMA)
```

Run the lookup on Databricks. `start_datetime` values are naive local timestamps, and serverless runs in UTC.

Native TPC `.tbl` and `.dat` input is supported through the shared Spark loader.
