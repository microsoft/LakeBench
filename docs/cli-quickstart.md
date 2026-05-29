# LakeBench CLI — Quick Start

A 5-minute tour of the `lakebench` CLI. Get from zero to a measured benchmark
run on your laptop without touching any Python.

---

## 1. Install

```bash
# pip — pick the engines you want; DuckDB has the smallest footprint
pip install 'lakebench[duckdb,tpch_datagen]'
```

Verify:

```bash
lakebench --version
lakebench --help
```

> **Using `uv` instead of `pip`?** Every command below works with the same
> arguments — just prefix with `uv run`, e.g. `uv run lakebench --version`.
> To set up the dev environment from a clone:
> `uv sync --group dev --extra duckdb --extra tpch_datagen`
> Install `uv` with `curl -LsSf https://astral.sh/uv/install.sh | sh`.

---

## 2. Generate some data (optional)

```bash
lakebench datagen \
    --benchmark tpch \
    --scale-factor 1 \
    --output /tmp/tpch_sf1
```

That writes the 8 TPC-H tables as parquet under `/tmp/tpch_sf1/`. Use scale
factor `0.1` if you want it to finish in seconds.

---

## 3. Run a benchmark — zero config

You can run with no profile at all:

```bash
lakebench run \
    --engine duckdb \
    --benchmark tpch --scenario sf1 --scale-factor 1 \
    --input-uri /tmp/tpch_sf1
```

`--engine` builds an ad-hoc profile inline. Local engines (`duckdb`, `polars`,
`daft`, `sail`) get a working-directory URI under `$TMPDIR/lakebench-scratch`
unless you override with `-E schema_or_working_directory_uri=...`.

Drop `--engine` and the CLI will **auto-create `~/.lakebench.json`** the first
time, picking the first installed local engine (priority: duckdb → polars →
daft → spark → sail). You'll see one warning line:

```
WARNING lakebench: No profile config found — created starter at /home/you/.lakebench.json
                   (re-run with --engine to override).
```

After that, future runs use the saved default with no flags needed.

---

## 4. Create a named profile (for repeated runs)

For more than one engine or non-default settings, create
`./lakebench.json` in the repo root (project-level):

```json
{
  "defaults": { "profile": "local-duckdb" },
  "profiles": {
    "local-duckdb": {
      "engine": "duckdb",
      "engine_options": {
        "schema_or_working_directory_uri": "/tmp/lakebench-duckdb"
      }
    }
  }
}
```

Inspect what the CLI actually sees:

```bash
lakebench profiles list
lakebench profiles show local-duckdb
```

---

## 5. Run with the profile

```bash
lakebench run \
    --benchmark tpch \
    --scenario sf1 \
    --scale-factor 1 \
    --input-uri /tmp/tpch_sf1
```

Because `defaults.profile` is set, you didn't need `--profile`. Add
`--print-config` (or `--dry-run`) first if you want to see the merged config
without actually launching an engine:

```bash
lakebench run --benchmark tpch --scenario sf1 \
    --scale-factor 1 --input-uri /tmp/tpch_sf1 --print-config
```

---

## 6. Inspect results

```bash
lakebench results latest                    # most recent run
lakebench results list --benchmark tpch     # filter
lakebench results show <run_id_prefix>      # 6-char prefix is enough
lakebench results stats --benchmark tpch    # n / mean / p50 / p95
```

Runs land in `./results/` by default — change with `--results-dir DIR` or
`LAKEBENCH_RESULTS_DIR`.

---

## 6a. Discover datasets already in your lakehouse

Pointing LakeBench at a Fabric workspace or Databricks catalog for the first
time? Ask it what's there:

```bash
lakebench discover --profile my-fabric
```

Example output:

```
catalog        schema        benchmark          confidence   matched/expected
spark_catalog  tpcds_sf1000  tpcds | eltbench   100%         24/24
spark_catalog  tpch_sf1000   tpch               100%         8/8
spark_catalog  clickbench    clickbench         100%         1/1
```

Now you know which schema to pass as `--input-uri` / `schema_name` in a
subsequent `lakebench run`. Also works with `--engine duckdb` against a local
scratch dir. `--min-confidence 0.8` hides partial matches; `--format json`
emits machine-readable output for scripting.

### Benchmark against an existing database

Once `discover` tells you what's in the lakehouse, run queries against it
without re-loading. Use `--mode query`, `--database <schema>`, and (for
multi-catalog engines) `--catalog <name>`:

```bash
# Fabric / Synapse / HDInsight via Livy
lakebench run --profile my-fabric \
    --benchmark tpcds --scenario sf1000 --scale-factor 1000 \
    --database tpcds_sf1000 --mode query

# Databricks (Unity Catalog or hive_metastore)
lakebench run --profile my-databricks \
    --benchmark tpch --scenario sf100 --scale-factor 100 \
    --catalog hive_metastore --database tpch_sf100 --mode query
```

`--database` (alias: `--schema`) overlays onto `engine_options.schema_name`,
and `--catalog` onto `engine_options.catalog_name`. Queries are auto-qualified
with the resolved catalog/schema, so no SQL edits are required.

---

## 7. Check your environment

Before debugging a flaky run, ask the CLI to self-check:

```bash
lakebench doctor
lakebench doctor --profile local-duckdb
```

Catches missing extras, broken profile, datagen tools not on PATH, unwritable
results dir, and missing/unauthenticated `az` CLI when any profile uses
`auth: az` (Fabric / Databricks / Synapse / HDInsight).

---

## 8. Tweak engine settings without editing the profile

Two override flags, last-one-wins, deep-merged into the profile:

```bash
# -E: any key under engine_options (JSON-aware, dotted nesting)
lakebench run --benchmark tpch --scenario sf1 \
    --scale-factor 1 --input-uri /tmp/tpch_sf1 \
    -E "compute_stats_all_cols=true"

# --conf: shortcut for engine_options.session_conf.<key>
lakebench run --benchmark tpch --scenario sf1 ... \
    --conf spark.sql.shuffle.partitions=200
```

Both also have file forms: `--engine-options-file foo.json`,
`--conf-file foo.properties`.

---

## 9. Tab completion (optional)

```bash
# bash
eval "$(lakebench --shell-init bash)"
# zsh
eval "$(lakebench --shell-init zsh)"
# fish
lakebench --shell-init fish | source
```

Requires `argcomplete` (`pip install argcomplete`); otherwise this is a no-op.

---

## Common recipes

| Task | Command |
|---|---|
| List supported run modes for a benchmark | `lakebench list-modes tpch` |
| Compare two runs side-by-side | `lakebench results compare <a> <b>` |
| Tag a run | `lakebench results tag <run_id> baseline production` |
| Add a note | `lakebench results notes <run_id> "warm cache, after vacuum"` |
| Export to CSV / Markdown | `lakebench results export --format md --output report.md` |
| Purge old runs | `lakebench results purge --older-than 30d` |
| Get full traceback on error | add `--debug` |
| Continue past engine crash, exit 2 instead of 3 | add `--continue-on-error` |

---

## Where to next

- **`docs/cli-reference.md`** — every flag, every subcommand, all defaults.
- **`docs/install-fabric.md`** — Fabric-specific install + first run.
- **`docs/install-databricks.md`** — Databricks-specific install + first run.
- **`README.md`** — Python-API usage, custom benchmarks/engines.
- **`lakebench doctor`** — first stop when something doesn't work.
