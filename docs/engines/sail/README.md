# Sail

The `Sail` engine runs LakeBench through a local Sail Spark Connect server.

## Support

- Benchmarks: ELTBench, TPC-DS, TPC-H, and ClickBench.
- Storage: local files, mount-style paths, and OneLake/cloud paths supported by Sail and DeltaRs.
- Coverage: [Sail coverage report](../../../reports/coverage/sail.md).

## Installation

```bash
pip install "lakebench[sail]"
```

Sail and the generic Spark extra conflict because they require different Spark distributions. Install only the engine needed in that environment.

## Example

```python
from lakebench.engines import Sail

engine = Sail(
    schema_or_working_directory_uri="file:///tmp/lakebench",
)
```

## Configuration

- `schema_or_working_directory_uri`: Delta table root and Spark warehouse directory.
- `storage_options`: options passed to DeltaRs/filesystem access.
- `cost_per_vcore_hour` or `cost_per_hour`: mutually exclusive manual pricing inputs.

## Runtime behavior

LakeBench starts one background Sail Spark Connect server per Python process and reuses its Spark session across engine instances. Join reordering is enabled for benchmark execution.

After upgrading Sail packages in a live notebook kernel, a kernel restart may be required before engine initialization.

Native TPC `.tbl` and `.dat` input is supported.
