# Databricks SQL Warehouse

`DatabricksSQLWarehouse` runs LakeBench through the Databricks SQL connector and Warehouses REST API on AWS, Azure, or GCP.

## Support

- Benchmarks: ELTBench, TPC-DS, TPC-H, and ClickBench.
- Warehouse types: serverless, classic, and pro.
- Storage: cloud object storage accessible to the warehouse.
- Result persistence: SQL executed by the remote warehouse, including result-table appends.

## Installation

```bash
pip install "lakebench[databricks_sql_warehouse]"
```

## Example

```python
import os

from lakebench.engines import DatabricksSQLWarehouse

engine = DatabricksSQLWarehouse(
    server_hostname="dbc-....cloud.databricks.com",
    warehouse_name="Serverless Starter Warehouse",
    catalog_name="main",
    schema_name="lakebench",
    access_token=os.environ["DATABRICKS_TOKEN"],
)
```

## Configuration

- `server_hostname`: workspace hostname without `https://`.
- `warehouse_name`: Warehouses API display name.
- `catalog_name` and `schema_name`: target namespace.
- `access_token`: used by the REST API and SQL connector; never written to result metadata.
- `schema_uri`: optional managed/external schema location.
- `enable_result_caching`: defaults to `False` for reproducible benchmark execution.
- `cost_per_hour`: all-inclusive manual hourly rate.
- `dbu_rate`: optional serverless DBU-rate override.

SQL Warehouse intentionally does not accept `cost_per_vcore_hour`.

## Serverless pricing

Warehouse-size DBUs/hour are multiplied by a DBU list rate. Resolution order is:

1. Explicit `dbu_rate`.
2. Exact recent warehouse SKU from `system.billing.usage`, then its active USD DBU rate from `system.billing.list_prices`.
3. When no recent usage exists, AWS regional SKU matching if all matching pricing tiers have one rate.
4. On Azure, the regional `Premium Serverless SQL DBU` meter from the public Azure Retail Prices API.

Insufficient billing-table permissions are logged at warning level. Azure can still use its public catalog; AWS and GCP continue without an estimate unless a manual rate is supplied. Classic and pro warehouses require `cost_per_hour` for estimated cost.

List prices are estimates and do not include negotiated discounts, commitments, or credits.

Native TPC `.tbl` and `.dat` input is loaded with `read_files` and the benchmark DDL.
