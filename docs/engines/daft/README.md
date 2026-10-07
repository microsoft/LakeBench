# Daft

The `Daft` engine integrates Daft DataFrames with LakeBench and Delta Lake.

## Current support status

Daft is unsupported for ELTBench, TPC-H, and TPC-DS as of LakeBench 2.0.0. The bundled TPC generators emit Parquet containing Arrow `Utf8View`, which the pinned Daft reader cannot load. ClickBench remains partially usable because its sample does not use that generated schema.

See the [Daft coverage report](../../../reports/coverage/daft.md).

## Installation

```bash
pip install "lakebench[daft]"
```

## Example

```python
from lakebench.engines import Daft

engine = Daft(
    schema_or_working_directory_uri="file:///tmp/lakebench",
)
```

## Configuration

- `schema_or_working_directory_uri`: Delta table root.
- `cost_per_vcore_hour` or `cost_per_hour`: mutually exclusive manual pricing inputs.

For ADLS Gen2, set `AZURE_STORAGE_TOKEN`. OneLake paths are rejected; use an ADLS Gen2 path instead.

## Engine behavior

Daft reads and writes Delta tables through its native Delta integration and the shared DeltaRs helper where appropriate. Native TPC `.tbl` and `.dat` input is implemented, but the generated Parquet limitation currently prevents supported end-to-end TPC/ELT execution.
