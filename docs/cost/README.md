# Cost reporting

LakeBench reports cost in one of two ways. Which one applies depends on how the platform bills.

| Mode (`cost_attribution`) | When it applies | What you get |
|---|---|---|
| `item_estimate` | Compute billed for provisioned time at a known hourly rate, e.g. fixed-size clusters, single-node runtimes, sized SQL warehouses | `estimated_retail_job_cost` on every result row, calculated while the benchmark runs as `hourly rate × duration` |
| `run_billed` | Compute billed by consumption that can't be tied to a statement while it runs, e.g. Databricks serverless | No per-item cost. The actual billed cost of the whole run, looked up afterwards with a post-run billing lookup |
| `not_configured` | No rate was supplied or resolved, and no lookup is available | No cost |

Each benchmark run records its mode in `engine_properties['cost_attribution']`. Use it to tell whether a run's per-item `estimated_retail_job_cost` is meaningful.

## Why not every engine has per-item cost

An item-level estimate is only valid when the compute costs the same per second, whatever runs on it. Consumption-billed compute breaks that assumption:

- It bills for resources used, which can scale up and down during a run.
- Billing records are aggregated by job run, notebook, or capacity, often in hourly buckets, and arrive with a lag of hours.
- No billing source exposes cost per statement.

For these engines LakeBench doesn't fill `estimated_retail_job_cost` and rejects hourly rate inputs such as `cost_per_hour`, because an hourly rate would produce numbers that look precise but aren't. Report cost **per run** instead.

## Post-run billed cost lookup

For `run_billed` engines:

1. **Run the benchmark so its usage can be isolated.** Make it the only work billed to the platform's attribution unit. For example, use a dedicated single-task job run for each LakeBench run. Each engine page lists its attribution unit.
2. **Look up cost after billing data arrives.** That usually takes a few hours. Use a separate scheduled job for the lookup rather than waiting inside the benchmark job, which bills the idle time.
3. **Store the result in a cost table** keyed by `run_id`, and join it to the results table.

In the same session as the run (the result may be incomplete):

```python
billed = engine.get_billed_cost(run_id=benchmark.header_detail_dict["run_id"])
```

In a later session, use the engine's lookup function with the identifiers stored in the saved results. Engine pages document the function and which `engine_properties` keys to pass. `run_window_from_results` gives the time window:

```python
from lakebench.billing import run_window_from_results

rows = [r.asDict() for r in spark.read.format("delta").load(result_table_uri).where(f"run_id = '{run_id}'").collect()]
start, end = run_window_from_results(rows)
# billed = <engine lookup function>(..., start_time=start, end_time=end, run_id=run_id)
```

Write the result with the engine's generic result writer:

```python
from lakebench.billing import BILLED_COST_SCHEMA

engine._append_results_to_delta(cost_table_uri, [billed.to_row()], BILLED_COST_SCHEMA)
# or, on Spark and Databricks SQL Warehouse, a catalog table (created if missing):
engine._append_results_to_delta(None, [billed.to_row()], BILLED_COST_SCHEMA, table_name=cost_table_name)
```

If `may_be_incomplete` is `True`, look the cost up again later and replace the row. A `MERGE` on `run_id` keeps the table idempotent.

`engine.supports_billed_cost_lookup` reports whether the current engine configuration supports `get_billed_cost()`. Other engines raise `NotImplementedError`.

### Optional per-item estimate

`lakebench.billing.backfill_results_cost(results, billed)` splits a run's billed total across result rows in proportion to `duration_ms`. The output is an estimate, not billed per-item cost, so label it that way wherever you report it.

## Billed cost schema

`lakebench.billing.BilledCost` and `BILLED_COST_SCHEMA` are platform-neutral. Every engine's lookup returns the same shape, so runs from different platforms can share one cost table. Platform-specific detail goes in `details`, not in new columns.

| Column | Type | Description |
|---|---|---|
| `run_id` | STRING | LakeBench run ID; join key to results |
| `platform` | STRING | Billing platform, e.g. `databricks` |
| `cloud_provider` | STRING | e.g. `aws`, `azure`, `gcp` |
| `compute_region` | STRING | Region of the compute |
| `compute_type` | STRING | e.g. `serverless-jobs-compute` |
| `performance_mode` | STRING | Platform performance mode as billed, e.g. `STANDARD`, `PERFORMANCE_OPTIMIZED`; `MIXED` if the run spans several; null if the platform has none |
| `billing_unit` | STRING | Unit the platform bills in, e.g. `DBU` |
| `billed_units` | DECIMAL(18,10) | Quantity billed in `billing_unit` |
| `list_cost` | DECIMAL(18,10) | Cost at list price |
| `currency` | STRING | Currency of `list_cost` |
| `attribution_type` | STRING | How usage was matched to the run, e.g. `job_run_id` |
| `attribution_id` | STRING | The identifier matched |
| `window_start` | TIMESTAMP | Start of the lookup window (UTC) |
| `window_end` | TIMESTAMP | End of the lookup window (UTC) |
| `latest_usage_end` | TIMESTAMP | End of the newest matched usage record (UTC); shows how far billing data has arrived |
| `usage_record_count` | BIGINT | Usage records matched |
| `unpriced_record_count` | BIGINT | Matched records with no list price, excluded from `list_cost` |
| `may_be_incomplete` | BOOLEAN | `True` when the window ended too recently for billing data to be final |
| `pricing_source` | STRING | Source of the cost, e.g. `databricks_system_billing_usage` |
| `looked_up_at` | TIMESTAMP | When the lookup ran (UTC) |
| `details` | MAP<STRING, STRING> | Platform-specific extras, e.g. SKUs and job IDs |

Costs are list prices. They don't reflect negotiated discounts, commitments, credits or taxes.

## Engine support

| Engine | `item_estimate` | `run_billed` lookup |
|---|---|---|
| [Databricks Spark](../engines/databricks-spark/README.md), classic compute | Manual rate; automatic on Azure fixed-size clusters | — |
| [Databricks Spark](../engines/databricks-spark/README.md), serverless | Not supported | ✅ `system.billing.usage` |
| [Databricks SQL Warehouse](../engines/databricks-sql-warehouse/README.md) | Automatic for serverless; manual for classic/pro | — |
| [Fabric Spark](../engines/fabric-spark/README.md) | Automatic from capacity metadata | — |
| [Fabric Data Warehouse](../engines/fabric-data-warehouse/README.md) | Not reported | — |
| [Synapse Spark](../engines/synapse-spark/README.md) | Automatic regional vCore rate | — |
| Spark, HDInsight | Manual rate | — |
| DuckDB, Polars, Daft, Sail | Manual rate; automatic on Fabric runtimes | — |

## Adding a post-run lookup to an engine

1. **Return a `BilledCost`.** Implement a lookup function that returns `BilledCost`, filling `platform`, `billing_unit`, `attribution_type` / `attribution_id`, and the completeness fields. Put anything platform-specific in `details`.
2. **Declare run-billed attribution.** Override `cost_attribution` to return `CostAttribution.RUN_BILLED` and `supports_billed_cost_lookup` to return `True` for the configurations that are billed this way. Override `get_billed_cost()` to call the lookup with the engine's identifiers.
3. **Store the identifiers.** Record the attribution identifiers the lookup needs in `extended_engine_metadata`, so a later session can find them in `engine_properties`.
4. **Reject hourly rates.** In those configurations, reject hourly rate inputs and leave `cost_per_hour` unset, so no per-item cost is produced.
5. **Document it.** On the engine page, document the attribution unit, the billing lag, the required permissions, and the later-session lookup call.
