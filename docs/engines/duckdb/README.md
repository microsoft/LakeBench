# DuckDB

The `DuckDB` engine provides an embedded local analytical engine with Delta Lake persistence.

## Support

- Benchmarks: ELTBench, TPC-DS, TPC-H, and ClickBench.
- Storage: local filesystem, mount-style paths, ADLS Gen2, and OneLake with appropriate credentials.
- Coverage: [DuckDB coverage report](../../../reports/coverage/duckdb.md).

## Installation

```bash
pip install "lakebench[duckdb]"
```

## Example

```python
from lakebench.engines import DuckDB

engine = DuckDB(
    schema_or_working_directory_uri="file:///tmp/lakebench",
)
```

## Configuration

- `schema_or_working_directory_uri`: Delta table root.
- `storage_options`: options forwarded to DeltaRs and filesystem clients.
- `cost_per_vcore_hour` or `cost_per_hour`: mutually exclusive manual pricing inputs.

For OneLake `abfss://` paths, set `AZURE_STORAGE_TOKEN`; LakeBench creates a DuckDB Azure secret using that bearer token.

## Engine behavior

DuckDB creates typed empty tables in memory from benchmark DDL and persists them as Delta tables. Delta writes use the shared DeltaRs helper. Native TPC delimited input is supported.
