# Fabric Data Warehouse

`FabricDataWarehouse` runs LakeBench through the Microsoft Fabric Warehouse SQL endpoint.

## Support

- Benchmarks: ELTBench, TPC-DS, TPC-H, and ClickBench.
- Runtime: Microsoft Fabric notebook.
- Storage: OneLake, ADLS Gen2 sources supported by Warehouse SQL, Parquet, and native TPC delimited files.
- SQL dialect: Fabric Warehouse T-SQL.

## Installation

Use Python 3.10 or later and install:

```python
%pip install "lakebench[fabric_data_warehouse]"
```

The host must also have Microsoft ODBC Driver 18 for SQL Server. Pip cannot install this system driver.

## Example

```python
from lakebench.engines import FabricDataWarehouse

engine = FabricDataWarehouse(
    warehouse_name="warehouse",
    warehouse_server="xxxxx.datawarehouse.fabric.microsoft.com",
    schema_name="dbo",
)
```

Warehouse and schema names cannot contain spaces.

## Authentication

The engine uses the current Fabric notebook identity to obtain a Fabric access token. The identity must be able to connect to the Warehouse, create and modify benchmark tables, and read source storage.

## Engine behavior

- ODBC connections use encryption and token authentication.
- Parquet loads use Warehouse-native SQL paths.
- TPC native delimited files are loaded with the generator's trailing delimiter handled explicitly.
- Result-log appends use DeltaRs when required by the result destination.
- Capacity SKU, Warehouse version, server, Warehouse name, and V-Order state are recorded when visible.

## Pricing

`estimated_retail_job_cost` remains unset. Fabric capacity cost cannot be reliably attributed to an individual Warehouse query or load, so LakeBench does not report a misleading per-job estimate.

`FabricWarehouse` is retained as a compatibility alias for `FabricDataWarehouse`.
