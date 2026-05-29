# Development

LakeBench is a Python-native, multi-engine benchmarking library for lakehouse
compute engines. Published to PyPI as `lakebench`, packaged with `hatchling`,
sources under `src/lakebench/`. Dependencies are managed with
[`uv`](https://docs.astral.sh/uv/).

## Install dev environment

Dependencies are split into many optional extras in `pyproject.toml` — sync the
extras matching the engines you need.

```bash
# Unit tests only (no engine extras required)
uv sync --group dev

# Add an engine + its datagen
uv sync --group dev --extra duckdb --extra tpch_datagen --extra tpcds_datagen
```

## Running tests

```bash
# Unit tests
uv run pytest tests/ --ignore=tests/integration -v --tb=short

# Integration tests for one engine (data generated at SF 0.1)
uv run pytest tests/integration/test_duckdb.py -v -s

# A single benchmark for a single engine
uv run pytest tests/integration/test_duckdb.py::test_tpch_duckdb -v -s

# CLI tests only
uv run pytest tests/test_cli.py -v --tb=short
```

## Running the CLI from source

```bash
uv run lakebench --help
uv run lakebench profiles list
uv run lakebench run --profile local-duckdb --benchmark tpch \
                     --scenario sf1 --scale-factor 1 --input-uri /tmp/tpch_sf1
uv run lakebench datagen --benchmark tpch --scale-factor 1 --output /tmp/tpch_sf1
```

(End users install via `pip install lakebench[<extras>]` and run plain
`lakebench …` — see `docs/cli-quickstart.md`.)

## Notes & gotchas

- The `spark` and `sail` extras are **mutually exclusive** (declared as a uv
  conflict). Use separate venvs if you need both.
- Spark / Sail integration tests require **Java 17+** on `PATH`.
- CI matrix in `.github/workflows/tests.yml` runs unit tests across Python
  3.8–3.13 and integration tests per engine.
- Pass/fail semantics for integration tests are intentionally tolerant of
  partial engine support — see `docs/architecture.md`.

## Where to look next

- **`docs/architecture.md`** — registry, source layout, query resolution,
  result schema invariants, integration-test semantics.
- **`docs/cli-reference.md`** — every CLI flag, every subcommand.
- **`docs/cli-quickstart.md`** — 5-minute end-user tour.
- **`docs/install-fabric.md`** / **`docs/install-databricks.md`** — cloud setup.
