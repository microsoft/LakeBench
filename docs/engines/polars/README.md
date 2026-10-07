# Polars

The `Polars` engine executes LakeBench with Polars lazy DataFrames and SQL context while persisting tables in Delta Lake.

## Support

- Benchmarks: ELTBench; partial TPC-DS, TPC-H, and ClickBench support.
- Storage: local filesystem, mount paths, ADLS Gen2, and OneLake through storage options.
- Coverage: [Polars coverage report](../../../reports/coverage/polars.md).

## Installation

```bash
pip install "lakebench[polars]"
```

## Example

```python
from lakebench.engines import Polars

engine = Polars(
    schema_or_working_directory_uri="file:///tmp/lakebench",
)
```

## Configuration

- `schema_or_working_directory_uri`: Delta table root.
- `storage_options`: cloud filesystem and DeltaRs authentication options.
- `cost_per_vcore_hour` or `cost_per_hour`: mutually exclusive manual pricing inputs.

## Engine behavior and limitations

Polars uses lazy Parquet scans and a Polars SQL context. Delta writes use DeltaRs. Native TPC delimited input is supported.

Some benchmark queries remain unsupported because Polars SQL does not implement every required construct, including some non-equi joins. Use the coverage report for current query-level results.
