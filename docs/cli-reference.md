# LakeBench CLI — Reference

Complete reference for every `lakebench` subcommand and flag.

For a 5-minute walkthrough see [`cli-quickstart.md`](./cli-quickstart.md).

---

## Synopsis

```text
lakebench [--version] [-v|-vv|-q] [--debug] [--shell-init bash|zsh|fish]
          [--results-dir DIR] [--config FILE]
          {run | doctor | list-modes | datagen | profiles | results | report} ...
```

## Exit codes

| Code | Meaning | Triggered by |
|---|---|---|
| **0** | Success | Normal completion |
| **1** | User error | Bad CLI args, missing profile, unknown engine/benchmark, validation failure |
| **2** | Partial failure | Some queries failed, OR engine crashed under `--continue-on-error` |
| **3** | Engine crash | Unhandled engine exception without `--continue-on-error` |

Use `--debug` to print full tracebacks for any non-zero exit.

---

## Top-level options

| Flag | Default | Purpose |
|---|---|---|
| `--version`, `-V` | — | Print package version and exit |
| `-v`, `--verbose` | 0 | Increase log level (`-v`=INFO, `-vv`=DEBUG) |
| `-q`, `--quiet` | false | Suppress all logging below ERROR |
| `--debug` | false | On error, print full Python traceback instead of one-line message |
| `--shell-init {bash,zsh,fish}` | — | Print completion-init snippet and exit; pair with `argcomplete` |
| `--results-dir DIR` | `~/.lakebench/results` | Where run records are stored |
| `--config FILE` | — | Use only this profile config; skip `~/.lakebench.json` + `./lakebench.json` discovery |

### Profile discovery

Without `--config`, two files are merged (project wins for same profile name):

1. `~/.lakebench.json` (global user defaults)
2. The nearest `lakebench.json` walking up from `cwd` (project overrides)

Profile values support **`${VAR}` and `${VAR:-default}`** expansion at load time, and a profile may set `"extends": "<other-profile>"` to inherit + override (one-level deep merge for `engine_options`).

### Auto-config on first run

If you call `lakebench run` with no `--profile`, no `--engine`, and no
discoverable config file, the CLI **auto-creates** `~/.lakebench.json` with a
starter profile pointing at the first installed local engine (priority:
`duckdb → polars → daft → spark → sail`), prints one warning line, then
proceeds:

```
WARNING lakebench: No profile config found — created starter at /home/you/.lakebench.json
                   (re-run with --engine to override).
```

Subsequent runs use the saved profile silently. To bypass the auto-created
config for a one-off, use `--engine NAME` (which never reads or writes the
config file).

The auto-create is only attempted when **no** config exists; if a
`~/.lakebench.json` is present but defines no `defaults.profile` and you
didn't pass `--profile`, you still get the original error.

---

## `lakebench run` — execute a benchmark

```text
lakebench run --benchmark NAME
              [--profile P] [--scenario S] [--scale-factor N] [--input-uri URI]
              [--save-results | --no-save-results] [--result-uri URI]
              [--run-id ID] [--mode M] [--query-list q1,q2,...]
              [--fail-on-run-id-collision]
              [-E KEY=VAL ...] [--conf KEY=VAL ...]
              [--engine-options-file FILE] [--conf-file FILE]
              [--retry N] [--continue-on-error]
              [--dry-run | --print-config]
```

| Flag | Default | Notes |
|---|---|---|
| `--benchmark`, `-b` (req.) | — | One of: `tpch`, `tpcds`, `tpcdi`, `eltbench`, `clickbench` |
| `--profile`, `-p` | `defaults.profile` | Profile name from config. Mutually exclusive with `--engine` |
| `--engine` | — | Inline engine name (e.g. `duckdb`) for **profile-less runs**. Synthesizes an in-memory profile from `--engine` + `-E`/`--conf` overlays. Local engines default `schema_or_working_directory_uri` to `$TMPDIR/lakebench-scratch` |
| `--scenario`, `-s` | — | Scenario label (e.g. `sf1`, `sf100`); recorded with results |
| `--scale-factor` | — | Integer scale factor passed to the benchmark |
| `--input-uri` | — | Where input parquet lives |
| `--database` / `--schema` | — | Point the engine at an existing catalog database. Overlays onto `engine_options.schema_name`. Pair with `--mode query` to benchmark data that's already loaded. |
| `--catalog` | — | Catalog name for multi-catalog engines (`hive_metastore`, `spark_catalog`, a Unity Catalog name, …). Overlays onto `engine_options.catalog_name`. |
| `--save-results / --no-save-results` | `false` | Persist a Delta result row alongside local results |
| `--result-uri` | — | Required when `--save-results` is set; remote Delta table |
| `--run-id` | auto | Custom run identifier; collides → warn+suffix unless `--fail-on-run-id-collision` |
| `--mode` | benchmark default | Validated against `BENCHMARK.MODE_REGISTRY` (e.g. `power_test`, `load_and_query`, `light`) |
| `--query-list` | all | Comma-separated subset (e.g. `q1,q3,q7`) |
| `-E KEY=VAL` | — | Repeatable engine-option override, JSON-aware, dotted nesting (e.g. `-E session_conf.spark.sql.shuffle.partitions=400`) |
| `--conf KEY=VAL` | — | Repeatable shortcut for `engine_options.session_conf.<KEY>`; never JSON-parses |
| `--engine-options-file FILE` | — | JSON object loaded **before** `-E` (CLI flags win) |
| `--conf-file FILE` | — | Java `.properties` or JSON loaded **before** `--conf` |
| `--retry N` | 0 | Reserved (stored on benchmark but not yet honored by all engines) |
| `--continue-on-error` | false | Engine crash → exit 2 (partial) instead of exit 3 |
| `--query-timeout SECONDS` | — | Per-query wall-clock cap. The engine cancels the running statement and surfaces a `TimeoutError` after this many seconds. **Honored by Livy today** (Fabric / Synapse / HDInsight); other engines ignore. Pair with Livy's auto-recovery (below) so subsequent queries don't cascade-fail. |
| `--dry-run` / `--print-config` | false | Resolve everything and print effective config, never starts the engine |

### Override precedence (last wins)

```
profile defaults  <  --engine-options-file  <  -E
                  <  --conf-file            <  --conf
```

`--conf` is essentially `-E session_conf.<KEY>=<VAL>` with string-only parsing; if you set the same key with both flags, `--conf` wins because it's applied after `-E`.

### Examples

```bash
# Smallest invocation (with defaults.profile set)
lakebench run -b tpch -s sf1 --scale-factor 1 --input-uri /tmp/tpch_sf1

# Override a Spark conf without editing the profile
lakebench run -b tpcds -p prod-spark --conf spark.sql.shuffle.partitions=800

# JSON-typed override into engine_options
lakebench run -b tpch -E '{"compute_stats_all_cols": true}'
lakebench run -b tpch -E compute_stats_all_cols=true   # JSON-aware bool

# Dry-run shows the post-overlay profile
lakebench run -b tpch -p prod-spark --conf spark.sql.shuffle.partitions=800 --print-config
```

---

## `lakebench discover` — find benchmark datasets in a catalog

```text
lakebench discover [--profile P | --engine NAME] [--catalog C]
                   [--min-confidence 0-1] [--include-empty]
                   [--format human|table|json|csv|yaml]
                   [-E KEY=VAL]... [--conf KEY=VAL]...
```

Connects via the given profile (or `--engine` ad-hoc), calls
`engine.list_databases()` / `list_tables(db)`, and fingerprints every schema
against the known benchmark table sets (tpch / tpcds / tpcdi / clickbench /
eltbench). Prints the matches with a confidence score:

```
catalog        schema              benchmark          confidence   matched/expected
spark_catalog  tpcds_sf1000        tpcds | eltbench   100%         24/24
spark_catalog  tpch_sf1000         tpch               100%         8/8
spark_catalog  tpcds_sf100_partial tpcds | eltbench   83%          20/24
spark_catalog  clickbench          clickbench         100%         1/1
```

| Flag | Notes |
|---|---|
| `--profile`, `-p` | Named profile from `lakebench.json`. Mutually exclusive with `--engine`. |
| `--engine` | Inline engine name (e.g. `duckdb`, `livy`) for profile-less runs. |
| `--catalog` | (Spark family) issues `USE CATALOG <name>` before scanning. |
| `--min-confidence` | Hide schemas below this match ratio (0.0–1.0). Default 0.0 shows every non-empty match. |
| `--include-empty` | Also list schemas with no benchmark match (labeled `-`). |
| `--format` | `human`/`table` (default), `json`, `csv`, `yaml`. |
| `-E`, `--conf` | Same override semantics as `lakebench run`. Useful for pointing DuckDB at a different working dir without editing the profile. |

Supported engines today: `spark`, `spark_connect`, `fabric_spark`,
`synapse_spark`, `hdi_spark`, `databricks`, `livy` (Fabric), `duckdb`.
Catalog-less engines (`polars`, `daft`, `sail`, `delta_rs`) raise a friendly
"does not support catalog discovery" and exit 1.

**ELTBench vs TPC-DS.** The two share the same 24-table schema, so a
matched TPC-DS dataset always shows both labels — which benchmark the data
"is" depends on how you generated it.

### Examples

```bash
# Fabric — show every discovered dataset in the lakehouse
lakebench discover --profile fabric-westus --format table

# Databricks — scan a specific catalog
lakebench discover --profile my-databricks --catalog hive_metastore

# Local DuckDB — point at an existing scratch dir
lakebench discover --engine duckdb \
    -E schema_or_working_directory_uri=/tmp/lakebench-scratch

# Only show "definitely-a-benchmark" datasets, as JSON for scripting
lakebench discover --profile fabric-westus --min-confidence 0.8 --format json
```

---

## `lakebench doctor` — environment sanity checks

```text
lakebench doctor [--profile P]
```

Probes:
- Profile config exists and parses (with optional `--profile` selecting one to load)
- Engine importable (`lakebench[<engine>]` extra installed)
- Datagen tools on `PATH` (`tpchgen-cli`, `duckdb`, `DIGen.jar`)
- Results dir exists and is writable

---

## `lakebench list-modes` — what `--mode` values are valid

```text
lakebench list-modes [BENCHMARK]
```

`BENCHMARK` is one of `tpch | tpcds | tpcdi | eltbench | clickbench`. With no
arg, prints modes for all benchmarks. The CLI uses the same registry to
validate `--mode` at runtime.

---

## `lakebench datagen` — generate parquet input

```text
lakebench datagen --benchmark NAME --scale-factor N --output PATH [--digen-jar PATH]
```

| Flag | Notes |
|---|---|
| `--benchmark` (req.) | One of: `tpch`, `tpcds`, `tpcdi`, `clickbench` |
| `--scale-factor` (req.) | Integer SF |
| `--output`, `-o` (req.) | Local dir or URI |
| `--digen-jar` | Path to `DIGen.jar` (TPC-DI only) |

ClickBench downloads from the upstream ClickHouse host; SF is ignored.

---

## `lakebench profiles` — manage `lakebench.json`

```text
lakebench profiles list
lakebench profiles show NAME
```

`list` enumerates all merged profiles. `show NAME` prints the
fully-resolved (post-`extends`, post-env-expansion) profile dict.

---

## `lakebench results` — manage saved runs

```text
lakebench results list    [--benchmark X] [--engine X] [--scenario X] [--limit N] [--format F]
lakebench results latest  [--limit N] [--format F]
lakebench results show    <run_id>
lakebench results delete  <run_id>
lakebench results tag     <run_id> <tag> [tag ...]
lakebench results notes   <run_id> <text>
lakebench results compare <run_id_a> <run_id_b> [--format F]
lakebench results stats   [--benchmark X] [--engine X] [--scenario X] [--format F]
lakebench results purge   --older-than DUR [--benchmark X] [--engine X] [--scenario X]
                          [--dry-run] [--yes]
lakebench results export  [--run-id X] [--format csv|json|md] [--output PATH]
```

### Subcommand-level details

| Sub | Notes |
|---|---|
| `list` | `--limit` defaults to 20; `--format` ∈ `human,table,json,csv,yaml` (default `human`) |
| `latest` | Same `--format` set; `--limit` default `1` |
| `show` / `delete` / `tag` / `notes` / `compare` | `<run_id>` may be a **prefix** (≥6 chars typical). Ambiguous prefix prints "did you mean…" candidates and exits 1 |
| `compare` | `--format` ∈ `table,json,csv,yaml` (default `table`); shows per-query delta-pct |
| `stats` | Aggregates `duration_ms` per query: n / mean / p50 / p95 / min / max |
| `purge` | `--older-than` accepts `30d`, `12h`, `15m`, `90s`. Requires `--yes` to actually delete; pair with `--dry-run` to preview |
| `export` | Single-run when `--run-id` set, otherwise everything; formats `csv,json,md`; `-o -` or omitted → stdout |

### Run-id prefix resolution

Most commands accept a short prefix instead of the full UUID — 6 characters is usually enough. If multiple runs match, you get a "Did you mean: aaaa, bbbb, …" message and exit 1.

---

## `lakebench report` — comparison & history reports

```text
lakebench report summary [--run-id X]
lakebench report compare [--benchmark X] [--scenario X] [--engines X,Y] [--run-ids A,B]
lakebench report history [--benchmark X] [--engine X] [--scenario X] [--limit N] [--format F]
```

| Sub | Notes |
|---|---|
| `summary` | One run, full breakdown; default = latest |
| `compare` | Cross-engine on the same benchmark/scenario; can pin runs via `--run-ids` |
| `history` | Time-series of past runs; same formats as `results list` |

---

## Profile file format

```jsonc
{
  "defaults": {
    "profile": "local-duckdb",          // pick when --profile omitted
    "save_results": false                // common keys also propagate
  },
  "profiles": {
    "local-duckdb": {
      "engine": "duckdb",
      "engine_options": {
        "schema_or_working_directory_uri": "/tmp/lakebench-duckdb"
      }
    },
    "prod-spark": {
      "extends": "local-spark",          // inherit, then override
      "engine_options": {
        "session_conf": {
          "spark.sql.shuffle.partitions": "400",
          "spark.databricks.delta.optimizeWrite.enabled": "true"
        }
      }
    },
    "fabric": {
      "engine": "fabric_spark",
      "engine_options": {
        "token_env": "FABRIC_TOKEN",      // reads $FABRIC_TOKEN at runtime
        "workspace_id": "${WORKSPACE_ID}",
        "lakehouse_id": "${LAKEHOUSE_ID:-default-lh}"
      }
    }
  }
}
```

### Validation (cheap, fail-fast)

`load_profile` checks before handing the dict to `resolve_engine`:

- `engine` must be a non-empty string in `ENGINE_REGISTRY`
- `engine_options` must be a dict
- `engine_options.session_conf` must be a dict
- All `session_conf` values must be scalar (`str | int | float | bool`) — Spark doesn't accept anything else, and the most common typo (`partitions: 400` instead of `"400"`) is caught here

### `extends:` composition

```
parent: { engine: spark, engine_options: { session_conf: { a: "1", b: "2" } } }
child:  { extends: parent, engine_options: { session_conf: { b: "20", c: "30" } } }

resolved:
  engine: spark
  engine_options:
    session_conf: { a: "1", b: "20", c: "30" }   # parent + child, child wins
```

Cycles are detected and produce a friendly error.

### Env expansion

Any string value matching `${VAR}` or `${VAR:-default}` is replaced with `os.environ[VAR]` (or the default) at load time — both in `defaults` and inside profiles, recursively through dicts and lists.

---

## Logging

| Flag | Level | Use when |
|---|---|---|
| (none) | WARNING | Normal CI |
| `-v` | INFO | See what the CLI is doing |
| `-vv` | DEBUG | Full plumbing detail (profile merge, override application) |
| `-q` | ERROR | Pipe-friendly silence |

All `lakebench` loggers go to stderr in the format
`HH:MM:SS LEVEL  lakebench.<sub>: <msg>`.

---

## Tab completion

```bash
pip install argcomplete
eval "$(lakebench --shell-init bash)"   # also: zsh, fish
```

`--shell-init` only emits the snippet — it doesn't install `argcomplete`. If
`argcomplete` isn't importable when `lakebench` runs, completion is a silent
no-op; the CLI still works normally.

---

## Files & paths

| Path | Purpose |
|---|---|
| `~/.lakebench.json` | Global profile config |
| `./lakebench.json` | Project profile config (overrides global) |
| `~/.lakebench/results/` | Default per-run record dir (override with `--results-dir` or `LAKEBENCH_RESULTS_DIR`) |
| `~/.lakebench/results/index.json` | Run-id index used by prefix resolution |

---

## Environment variables

| Variable | Effect |
|---|---|
| `LAKEBENCH_RESULTS_DIR` | Default for `--results-dir` |
| Anything referenced by `${VAR}` in a profile | Expanded at config load time |
| `*_env` keys in `engine_options` (e.g. `token_env`) | Read at engine-instantiation; missing → `EnvironmentError` |

---

## See also

- [`cli-quickstart.md`](./cli-quickstart.md) — 5-minute first run
- `README.md` — Python-API usage, custom benchmarks/engines, BYO data caveats
- `lakebench doctor` — when in doubt, run this first
