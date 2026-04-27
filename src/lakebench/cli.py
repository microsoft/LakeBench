"""
LakeBench CLI — run benchmarks, generate data, manage results, and generate reports.

Usage:
    lakebench run --profile <name> --benchmark <name> [options]
    lakebench datagen --benchmark <name> --scale-factor <N> --output <path>
    lakebench profiles list
    lakebench profiles show <name>
    lakebench results list [--benchmark X] [--engine X] [--limit N]
    lakebench results show <run_id>
    lakebench results delete <run_id>
    lakebench results export [--run-id X] [--format csv|json|md] [--output path]
    lakebench report summary [--run-id X]
    lakebench report compare [--benchmark X] [--scenario X] [--engines X,Y]
    lakebench report history [--benchmark X] [--engine X] [--limit N]
"""

import argparse
import json
import logging
import os
import sys
import warnings

from lakebench.config import (
    list_profiles,
    load_profile,
    resolve_benchmark,
    resolve_datagen,
    resolve_engine,
    load_config,
    BENCHMARK_REGISTRY,
    ENGINE_REGISTRY,
)
from lakebench.results import ResultsManager
from lakebench import reporting


# Exit codes (mirrored at module level for tests / scripts)
EXIT_OK = 0
EXIT_USER_ERROR = 1
EXIT_PARTIAL_FAILURE = 2
EXIT_ENGINE_CRASH = 3

log = logging.getLogger("lakebench")


def _configure_logging(verbosity: int, quiet: bool):
    """Verbosity: 0=WARNING (default), 1=INFO (-v), 2+=DEBUG (-vv). --quiet forces ERROR."""
    if quiet:
        level = logging.ERROR
    elif verbosity <= 0:
        level = logging.WARNING
    elif verbosity == 1:
        level = logging.INFO
    else:
        level = logging.DEBUG
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


def _parse_value(raw: str):
    """Parse a CLI value as JSON if it looks like JSON; otherwise return the raw string.

    Accepts: {..}, [..], "..", numbers, true/false/null. Falls back to string on
    any JSON decode error so ``--conf spark.sql.foo=bar`` still works.
    """
    s = raw.strip()
    if not s:
        return raw
    first = s[0]
    looks_jsonish = (
        first in "{[\""
        or s in ("true", "false", "null")
        or (first == "-" and len(s) > 1 and s[1].isdigit())
        or first.isdigit()
    )
    if looks_jsonish:
        try:
            return json.loads(s)
        except json.JSONDecodeError:
            pass
    return raw


def _set_dotted(target: dict, dotted_key: str, value):
    """Set a value in a nested dict using a dotted path.

    Unknown spark.* keys stay as single literal keys (no nesting) because
    Spark conf keys naturally contain dots, but callers can force nesting with
    explicit bracket syntax later if ever needed. Here we only special-case:
    if the FIRST segment matches a known nestable container (session_conf,
    engine_options, benchmark_options), walk into it; after that, the rest of
    the key is used as a single flat key.

    Note: nesting is exactly one level deep beyond the NESTABLE head. Keys like
    ``benchmark_options.scenarios.foo.bar`` set the literal key
    ``"scenarios.foo.bar"`` on ``benchmark_options`` rather than recursively
    descending. Use ``-E benchmark_options={...}`` with a JSON value if you
    need deeper structure.
    """
    NESTABLE = {"session_conf", "engine_options", "benchmark_options"}
    if "." not in dotted_key:
        target[dotted_key] = value
        return
    head, rest = dotted_key.split(".", 1)
    if head in NESTABLE:
        sub = target.setdefault(head, {})
        if not isinstance(sub, dict):
            raise ValueError(
                f"Cannot overlay into '{head}' — existing value is not a dict"
            )
        sub[rest] = value
    else:
        # Flat: spark.sql.foo stays as the literal key
        target[dotted_key] = value


def _apply_overrides(profile: dict, eopts: list, confs: list):
    """Apply -E / --conf overrides onto the profile dict.

    -E KEY=VALUE overlays onto profile['engine_options']. KEY may be dotted to
    reach into session_conf (e.g. session_conf.spark.sql.shuffle.partitions).
    VALUE is parsed as JSON when it looks like JSON, otherwise as a string.

    --conf KEY=VALUE is a shortcut that always targets
    engine_options.session_conf[KEY] with VALUE kept as a string (Spark confs
    are typed at use-time).

    Precedence (last wins): profile defaults < -E overlays < --conf overlays.
    Within the same flag, later occurrences win. This means if both flags
    target the same session_conf key, --conf is the final word.
    """
    engine_options = profile.setdefault("engine_options", {})

    for opt in eopts:
        if "=" not in opt:
            raise ValueError(f"--engine-option must be KEY=VALUE, got: {opt}")
        k, v = opt.split("=", 1)
        _set_dotted(engine_options, k, _parse_value(v))

    if confs:
        session_conf = engine_options.setdefault("session_conf", {})
        if not isinstance(session_conf, dict):
            raise ValueError(
                "engine_options.session_conf must be a dict to apply --conf"
            )
        for opt in confs:
            if "=" not in opt:
                raise ValueError(f"--conf must be KEY=VALUE, got: {opt}")
            k, v = opt.split("=", 1)
            session_conf[k] = v  # Spark confs are stringly-typed by convention


def _load_eopts_file(path: str) -> list:
    """Load -E overrides from a JSON file (object of KEY:VALUE) into KEY=VALUE strings.

    Values are JSON-serialized so _parse_value's JSON path picks them back up.
    Strings stay as bare strings so spark.foo=bar works."""
    with open(os.path.expanduser(path)) as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"--engine-options-file must contain a JSON object, got {type(data).__name__}")
    out = []
    for k, v in data.items():
        if isinstance(v, str):
            out.append(f"{k}={v}")
        else:
            out.append(f"{k}={json.dumps(v)}")
    return out


def _load_conf_file(path: str) -> list:
    """Load --conf overrides from a Java .properties-style or JSON file."""
    p = os.path.expanduser(path)
    with open(p) as f:
        text = f.read()
    out = []
    if text.lstrip().startswith("{"):
        data = json.loads(text)
        for k, v in data.items():
            out.append(f"{k}={v}")
        return out
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or line.startswith("//"):
            continue
        if "=" not in line:
            raise ValueError(f"--conf-file entry missing '=': {line!r}")
        out.append(line)
    return out


def _format_records(records, fmt: str = "table") -> str:
    """Render a list of dict records in the requested format."""
    if not records:
        return "(no rows)"
    if fmt == "json":
        return json.dumps(records, indent=2, default=str)
    if fmt == "csv":
        import csv
        import io
        buf = io.StringIO()
        cols = list(records[0].keys())
        w = csv.DictWriter(buf, fieldnames=cols)
        w.writeheader()
        for r in records:
            w.writerow({k: r.get(k, "") for k in cols})
        return buf.getvalue().rstrip("\n")
    if fmt == "yaml":
        # Minimal YAML emitter — avoids a PyYAML dependency
        out = []
        for r in records:
            out.append("- " + "\n  ".join(f"{k}: {v}" for k, v in r.items()))
        return "\n".join(out)
    # default: table
    cols = list(records[0].keys())
    widths = {c: max(len(str(c)), max(len(str(r.get(c, ""))) for r in records)) for c in cols}
    header = "  ".join(f"{c:<{widths[c]}}" for c in cols)
    sep = "  ".join("-" * widths[c] for c in cols)
    rows = ["  ".join(f"{str(r.get(c, '')):<{widths[c]}}" for c in cols) for r in records]
    return "\n".join([header, sep, *rows])


def _get_results_manager(args=None) -> ResultsManager:
    """Get ResultsManager, using results-dir from args or default."""
    results_dir = getattr(args, "results_dir", None)
    if results_dir:
        return ResultsManager(results_dir)
    return ResultsManager()


def cmd_run(args):
    """Run a benchmark using a profile.

    Returns an exit code (0=ok, 2=partial failure, 3=engine crash). User-input
    validation errors raise instead so ``main`` maps them to EXIT_USER_ERROR.
    """
    # Mutually exclusive: --engine NAME (ad-hoc) vs --profile NAME (named).
    if getattr(args, "engine", None) and getattr(args, "profile", None):
        raise ValueError("--engine and --profile are mutually exclusive")

    if getattr(args, "engine", None):
        # Inline / profile-less path: build the profile dict from --engine.
        profile = _synthesize_profile(args.engine)
    else:
        try:
            profile = load_profile(args.profile, config_path=getattr(args, "config", None))
        except ValueError as e:
            # First-run path: no profile name specified AND no default configured.
            # Try to write a starter ~/.lakebench.json once, then retry.
            if (
                "No profile name specified" in str(e)
                and not getattr(args, "config", None)
                and not getattr(args, "profile", None)
            ):
                created = _maybe_auto_create_config()
                if created:
                    log.warning(
                        "No profile config found — created starter at %s "
                        "(re-run with --engine to override).",
                        created,
                    )
                    profile = load_profile(None, config_path=None)
                else:
                    raise
            else:
                raise

    # Apply --engine-option / --conf overrides — file-based overlays first,
    # then CLI flag overlays so that explicit CLI args win.
    eopts_from_file = []
    confs_from_file = []
    if getattr(args, "engine_options_file", None):
        eopts_from_file = _load_eopts_file(args.engine_options_file)
    if getattr(args, "conf_file", None):
        confs_from_file = _load_conf_file(args.conf_file)
    _apply_overrides(
        profile,
        eopts=eopts_from_file + (getattr(args, "engine_option", []) or []),
        confs=confs_from_file + (getattr(args, "conf", []) or []),
    )

    # --database / --catalog: ergonomic shortcuts for benchmarking against an
    # existing catalog dataset (typically paired with --mode query). These
    # overlay onto engine_options.{schema_name,catalog_name} after the other
    # override channels so the CLI flags win.
    _eo = profile.setdefault("engine_options", {})
    if getattr(args, "database", None):
        _eo["schema_name"] = args.database
    if getattr(args, "catalog", None):
        _eo["catalog_name"] = args.catalog
    if getattr(args, "query_timeout", None) is not None:
        _eo["query_timeout_seconds"] = args.query_timeout

    # Validate --mode early so dry-run can flag bad modes too
    if args.mode:
        bench_modes = _supported_modes(args.benchmark)
        if bench_modes and args.mode not in bench_modes:
            raise ValueError(
                f"Mode '{args.mode}' not supported for {args.benchmark}. "
                f"Supported modes: {bench_modes}"
            )

    # --print-config / --dry-run short-circuits: never instantiate engine
    if getattr(args, "print_config", False) or getattr(args, "dry_run", False):
        print(json.dumps(profile, indent=2, default=str))
        log.info("dry-run / print-config requested; skipping engine + benchmark")
        return EXIT_OK

    engine = resolve_engine(profile)

    # Different benchmarks name their input arg differently. TPC-DI takes
    # `input_batch_folder_uri` (Batch1/Batch2/Batch3); the rest take
    # `input_parquet_folder_uri`. The CLI exposes a single `--input-uri`
    # that we map per-benchmark here.
    _INPUT_URI_KEY = {
        "tpcdi": "input_batch_folder_uri",
    }
    input_kwarg = _INPUT_URI_KEY.get(args.benchmark, "input_parquet_folder_uri")

    overrides = {
        "scenario_name": args.scenario,
        "scale_factor": args.scale_factor,
        input_kwarg: args.input_uri,
        "save_results": args.save_results,
        "result_table_uri": args.result_uri,
        "run_id": args.run_id,
    }
    if args.query_list:
        overrides["query_list"] = args.query_list.split(",")

    benchmark = resolve_benchmark(args.benchmark, engine, profile, **overrides)

    log.info("Running %s with engine '%s'...", args.benchmark, profile.get("engine"))
    try:
        if args.mode:
            benchmark.run(mode=args.mode)
        else:
            benchmark.run()
    except Exception as e:
        log.error("Engine crashed before completing: %s", e)
        rm = _get_results_manager(args)
        if getattr(benchmark, "results", None):
            rm.save_run(
                benchmark=benchmark,
                profile_name=args.profile or profile.get("profile"),
                profile_config=profile,
                fail_on_collision=getattr(args, "fail_on_run_id_collision", False),
            )
        return EXIT_PARTIAL_FAILURE if getattr(args, "continue_on_error", False) else EXIT_ENGINE_CRASH
    log.info("Benchmark complete.")

    # Auto-save results locally
    rm = _get_results_manager(args)
    exit_code = EXIT_OK
    if benchmark.results:
        fail_on_collision = getattr(args, "fail_on_run_id_collision", False)
        run_dir = rm.save_run(
            benchmark=benchmark,
            profile_name=args.profile or profile.get("profile"),
            profile_config=profile,
            fail_on_collision=fail_on_collision,
        )
        if any(not r.get("success", True) for r in benchmark.results):
            exit_code = EXIT_PARTIAL_FAILURE

        print(f"\n{reporting.report_summary(rm, benchmark.header_detail_dict['run_id'])}")

    return exit_code


def _supported_modes(benchmark_name: str):
    """Return MODE_REGISTRY for a benchmark name, or None if it can't be resolved."""
    if benchmark_name not in BENCHMARK_REGISTRY:
        return None
    module_path, class_name = BENCHMARK_REGISTRY[benchmark_name]
    try:
        import importlib
        module = importlib.import_module(module_path)
        cls = getattr(module, class_name)
        return list(getattr(cls, "MODE_REGISTRY", []) or []) or None
    except Exception:
        return None


def cmd_datagen(args):
    """Generate benchmark data."""
    kwargs = {
        "scale_factor": args.scale_factor,
    }

    # Map output to the correct parameter name per generator
    if args.benchmark == "tpcdi":
        kwargs["target_folder"] = args.output
        if args.digen_jar:
            kwargs["digen_jar_path"] = args.digen_jar
    elif args.benchmark == "clickbench":
        kwargs["target_folder_uri"] = args.output
    else:
        kwargs["target_folder_uri"] = args.output

    datagen = resolve_datagen(args.benchmark, **kwargs)
    print(f"Generating {args.benchmark} data (SF={args.scale_factor})...")
    datagen.run()
    print("Data generation complete.")


def cmd_profiles_list(args):
    """List available profiles."""
    profiles = list_profiles()
    if not profiles:
        # First-touch UX: try to auto-create a starter ~/.lakebench.json
        # the same way `lakebench run` does.
        created = _maybe_auto_create_config()
        if created:
            log.warning(
                "No profile config found — created starter at %s "
                "(re-run with --engine to override).",
                created,
            )
            profiles = list_profiles()
        if not profiles:
            print(
                "No profiles found. Create ~/.lakebench.json or ./lakebench.json, "
                "or run `lakebench run --engine duckdb ...` for a profile-less run."
            )
            return
    for name in profiles:
        print(f"  {name}")


def cmd_profiles_show(args):
    """Show a specific profile."""
    profile = load_profile(args.name)
    print(json.dumps(profile, indent=2))


# --- Results commands ---

def cmd_results_list(args):
    """List saved benchmark runs."""
    rm = _get_results_manager(args)
    runs = rm.list_runs(
        benchmark=args.benchmark,
        engine=args.engine,
        scenario=args.scenario,
        limit=args.limit,
    )
    if not runs:
        print("No runs found.")
        return
    fmt = getattr(args, "format", None)
    if fmt and fmt != "human":
        print(_format_records(runs, fmt))
    else:
        print(reporting.report_history(rm, args.benchmark, args.engine, args.scenario, args.limit))


def cmd_results_show(args):
    """Show details of a specific run."""
    rm = _get_results_manager(args)
    print(reporting.report_summary(rm, _resolve_run_id(rm, args.run_id)))


def cmd_results_delete(args):
    """Delete a specific run."""
    rm = _get_results_manager(args)
    if rm.delete_run(_resolve_run_id(rm, args.run_id)):
        print(f"Run '{args.run_id}' deleted.")
    else:
        print(f"Run '{args.run_id}' not found.", file=sys.stderr)
        sys.exit(EXIT_USER_ERROR)


def cmd_results_export(args):
    """Export results."""
    rm = _get_results_manager(args)
    result = reporting.export_results(
        rm,
        run_id=args.run_id,
        fmt=args.format,
        output_path=args.output,
    )
    print(result)


# --- Report commands ---

def cmd_report_summary(args):
    """Print run summary report."""
    rm = _get_results_manager(args)
    print(reporting.report_summary(rm, args.run_id))


def cmd_report_compare(args):
    """Print cross-engine comparison report."""
    rm = _get_results_manager(args)
    engines = args.engines.split(",") if args.engines else None
    run_ids = args.run_ids.split(",") if args.run_ids else None
    print(reporting.report_compare(
        rm,
        benchmark=args.benchmark,
        scenario=args.scenario,
        engines=engines,
        run_ids=run_ids,
    ))


def cmd_report_history(args):
    """Print historical runs report."""
    rm = _get_results_manager(args)
    fmt = getattr(args, "format", None)
    if fmt and fmt != "human":
        runs = rm.list_runs(
            benchmark=args.benchmark, engine=args.engine,
            scenario=args.scenario, limit=args.limit,
        )
        print(_format_records(runs, fmt))
        return
    print(reporting.report_history(
        rm,
        benchmark=args.benchmark,
        engine=args.engine,
        scenario=args.scenario,
        limit=args.limit,
    ))


def _lakebench_version() -> str:
    """Return the installed lakebench version, or 'unknown' if metadata is missing."""
    try:
        from importlib.metadata import version, PackageNotFoundError
        try:
            return version("lakebench")
        except PackageNotFoundError:
            return "unknown"
    except Exception:
        return "unknown"


# ---------------------------------------------------------------------------
# Zero-config support: synthesize an in-memory profile from --engine NAME, and
# auto-create a starter ~/.lakebench.json on first run.
# ---------------------------------------------------------------------------

# Default engine_options seed for engines that work locally with no creds.
# Engines requiring remote endpoints (databricks/livy/fabric_*/synapse_*/hdi_*/
# spark_connect) are intentionally absent — they MUST be configured explicitly.
_LOCAL_ENGINE_DEFAULTS = {
    "duckdb":  {"schema_or_working_directory_uri": None},
    "polars":  {"schema_or_working_directory_uri": None},
    "daft":    {"schema_or_working_directory_uri": None},
    "sail":    {"schema_or_working_directory_uri": None},
    "spark":   {"schema_name": "lakebench"},
}

# Priority order for auto-pick (cheapest local engines first).
_AUTO_ENGINE_PRIORITY = ("duckdb", "polars", "daft", "spark", "sail")


def _synthesize_profile(engine_name: str) -> dict:
    """Build an in-memory profile dict for ``--engine NAME`` runs.

    Local engines that need only a working-directory URI default it to a
    stable tmp path so the user can run with no other flags. Users can still
    override via ``-E schema_or_working_directory_uri=...``.
    """
    if engine_name not in ENGINE_REGISTRY:
        available = ", ".join(sorted(ENGINE_REGISTRY))
        raise ValueError(
            f"Unknown engine '{engine_name}'. Available engines: {available}"
        )
    eo = dict(_LOCAL_ENGINE_DEFAULTS.get(engine_name, {}))
    if eo.get("schema_or_working_directory_uri") is None and "schema_or_working_directory_uri" in eo:
        import tempfile
        eo["schema_or_working_directory_uri"] = os.path.join(
            tempfile.gettempdir(), "lakebench-scratch"
        )
    return {"engine": engine_name, "engine_options": eo}


def _maybe_auto_create_config():
    """If ``~/.lakebench.json`` doesn't exist, write a starter config.

    Probes installable local engines in priority order and picks the first one
    that imports cleanly. Returns the path written, or ``None`` if a config
    already exists or no local engine is available.
    """
    import importlib
    from lakebench.config import GLOBAL_CONFIG_PATH

    if os.path.exists(GLOBAL_CONFIG_PATH):
        return None

    for engine_name in _AUTO_ENGINE_PRIORITY:
        if engine_name not in ENGINE_REGISTRY:
            continue
        module_path, _ = ENGINE_REGISTRY[engine_name]
        try:
            importlib.import_module(module_path)
        except ImportError:
            continue
        profile_name = f"local-{engine_name}"
        cfg = {
            "defaults": {"profile": profile_name},
            "profiles": {profile_name: _synthesize_profile(engine_name)},
        }
        try:
            with open(GLOBAL_CONFIG_PATH, "w") as f:
                json.dump(cfg, f, indent=2)
        except OSError:
            return None
        return GLOBAL_CONFIG_PATH
    return None


def cmd_list_modes(args):
    """Print supported modes for one or all benchmarks."""
    if args.benchmark:
        modes = _supported_modes(args.benchmark)
        if modes is None:
            print(f"Unknown benchmark: {args.benchmark}", file=sys.stderr)
            sys.exit(1)
        for m in modes:
            print(m)
        return
    for name in BENCHMARK_REGISTRY:
        modes = _supported_modes(name) or []
        print(f"{name}: {', '.join(modes) if modes else '(none)'}")


def _resolve_run_id(rm: ResultsManager, run_id: str) -> str:
    """Resolve a possibly-prefix run_id against the index, raising on ambiguity.

    Returns the full run_id. Empty/None returns as-is (caller may interpret as
    'latest').
    """
    if not run_id:
        return run_id
    import os
    import pyarrow.parquet as pq
    if os.path.exists(rm.index_path):
        table = pq.read_table(rm.index_path)
        ids = table.column("run_id").to_pylist()
        exact = [r for r in ids if r == run_id]
        if exact:
            return exact[0]
        prefix = [r for r in ids if r.startswith(run_id)]
        if len(prefix) == 1:
            return prefix[0]
        if len(prefix) > 1:
            raise ValueError(
                f"Ambiguous run_id prefix '{run_id}'. Did you mean one of: "
                + ", ".join(prefix[:10]) + ("..." if len(prefix) > 10 else "")
            )
    return run_id


def cmd_results_latest(args):
    """Show the N most recent runs (default 1) in the chosen format."""
    rm = _get_results_manager(args)
    runs = rm.list_runs(limit=args.limit)  # already sorted desc by run_datetime
    if not runs:
        print("No runs found.")
        return EXIT_OK
    fmt = getattr(args, "format", "human")
    if fmt == "human":
        # default: print summary of the single latest run
        first = runs[0]
        print(reporting.report_summary(rm, first["run_id"]))
    else:
        print(_format_records(runs, fmt))
    return EXIT_OK


def _parse_duration(s: str) -> float:
    """Parse a short duration like '30d', '12h', '15m', '90s' into seconds.

    Bare integers are treated as seconds for back-compat.
    """
    s = s.strip().lower()
    if not s:
        raise ValueError("empty duration")
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 86400 * 7}
    if s[-1] in units:
        try:
            n = float(s[:-1])
        except ValueError as e:
            raise ValueError(f"invalid duration {s!r}: {e}")
        return n * units[s[-1]]
    try:
        return float(s)
    except ValueError:
        raise ValueError(f"invalid duration {s!r}: expected e.g. '30d', '12h', '15m'")


def cmd_results_purge(args):
    """Delete runs older than --older-than, optionally filtered."""
    from datetime import datetime, timezone, timedelta
    rm = _get_results_manager(args)
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=_parse_duration(args.older_than))

    runs = rm.list_runs(
        benchmark=args.benchmark, engine=args.engine, scenario=args.scenario,
        limit=10_000_000,
    )
    victims = []
    for r in runs:
        ts = r.get("run_datetime")
        if isinstance(ts, datetime):
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            if ts < cutoff:
                victims.append(r)

    if not victims:
        print("No runs older than the cutoff matched the filters.")
        return EXIT_OK

    print(f"Would delete {len(victims)} run(s) older than {args.older_than}:")
    for r in victims:
        print(f"  - {r['run_id']}  ({r.get('run_datetime')})  {r.get('benchmark')}/{r.get('scenario')}")

    if getattr(args, "dry_run", False):
        print("(dry-run; nothing deleted)")
        return EXIT_OK
    if not getattr(args, "yes", False):
        print("\nRefusing to delete without --yes (or pass --dry-run to preview).", file=sys.stderr)
        return EXIT_USER_ERROR

    deleted = 0
    for r in victims:
        if rm.delete_run(r["run_id"]):
            deleted += 1
    print(f"\nDeleted {deleted} run(s).")
    return EXIT_OK


def cmd_results_stats(args):
    """Aggregate per-query duration_ms stats across runs of one benchmark."""
    import statistics
    rm = _get_results_manager(args)
    table = rm.get_all_results(
        benchmark=args.benchmark, engine=args.engine, scenario=args.scenario,
    )
    if table is None or table.num_rows == 0:
        print("No results found for the requested filters.")
        return EXIT_OK

    cols = table.to_pydict()
    items = cols.get("test_item", [])
    durs = cols.get("duration_ms", [])
    success = cols.get("success", [True] * len(items))

    grouped: dict = {}
    for i, q in enumerate(items):
        if not success[i]:
            continue
        d = durs[i]
        if d is None:
            continue
        grouped.setdefault(q, []).append(d)

    rows = []
    for q in sorted(grouped):
        ds = sorted(grouped[q])
        n = len(ds)
        rows.append({
            "query": q,
            "n": n,
            "mean_ms": int(statistics.fmean(ds)),
            "min_ms": ds[0],
            "p50_ms": ds[n // 2],
            "p95_ms": ds[min(n - 1, int(round(0.95 * (n - 1))))],
            "max_ms": ds[-1],
        })
    fmt = getattr(args, "format", "table")
    print(_format_records(rows, fmt))
    return EXIT_OK


_SHELL_INIT_TEMPLATES = {
    "bash": 'eval "$(register-python-argcomplete lakebench)"\n',
    "zsh": (
        "autoload -U bashcompinit && bashcompinit\n"
        'eval "$(register-python-argcomplete lakebench)"\n'
    ),
    "fish": "register-python-argcomplete --shell fish lakebench | source\n",
}


def cmd_discover(args):
    """Probe a catalog engine for databases that match known benchmarks.

    Connects via a profile (or --engine ad-hoc profile), lists every database
    in the catalog, fingerprints each by table-name overlap with the known
    benchmark table sets (tpch/tpcds/tpcdi/clickbench/eltbench), and prints
    the matches (confidence + matched/expected) through the existing
    _format_records plumbing.
    """
    from lakebench import discover as discover_mod

    if getattr(args, "engine", None) and getattr(args, "profile", None):
        raise ValueError("--engine and --profile are mutually exclusive")

    if getattr(args, "engine", None):
        profile = _synthesize_profile(args.engine)
    else:
        profile = load_profile(
            getattr(args, "profile", None),
            config_path=getattr(args, "config", None),
        )

    # Reuse the same override path as cmd_run so users can -E
    # schema/catalog overrides at discovery time too.
    _apply_overrides(
        profile,
        eopts=getattr(args, "engine_option", []) or [],
        confs=getattr(args, "conf", []) or [],
    )

    engine_name = profile.get("engine")
    log.info("Connecting to %s for catalog discovery...", engine_name)
    try:
        engine = resolve_engine(profile)
    except Exception as e:
        print(f"Error: failed to instantiate engine '{engine_name}': {e}")
        return EXIT_USER_ERROR

    # Optionally set the current catalog (Spark family only).
    if getattr(args, "catalog", None):
        try:
            engine.execute_sql_statement(f"USE CATALOG `{args.catalog}`")
        except Exception as e:
            log.warning("Could not USE CATALOG %s: %s", args.catalog, e)

    try:
        databases = engine.list_databases()
    except NotImplementedError as e:
        print(f"Error: {e}")
        return EXIT_USER_ERROR
    except Exception as e:
        print(f"Error: listing databases failed: {e}")
        return EXIT_USER_ERROR

    log.info("Found %d databases; fingerprinting against %d benchmarks...",
             len(databases), len(discover_mod.BENCHMARK_TABLES))

    rows = []
    min_conf = float(getattr(args, "min_confidence", 0.0) or 0.0)
    include_empty = bool(getattr(args, "include_empty", False))
    catalog_label = getattr(args, "catalog", None) or "-"

    for db in databases:
        try:
            tables = engine.list_tables(db)
        except Exception as e:
            log.warning("Could not list tables in %s: %s", db, e)
            if include_empty:
                rows.append({
                    "catalog": catalog_label, "schema": db,
                    "benchmark": "(error)", "confidence": "-",
                    "matched/expected": "-",
                })
            continue

        matches = discover_mod.all_equal_top_matches(tables)
        if not matches:
            if include_empty:
                rows.append({
                    "catalog": catalog_label, "schema": db,
                    "benchmark": "-", "confidence": "-",
                    "matched/expected": f"0/{len(tables)}",
                })
            continue

        bench_label = " | ".join(m[0] for m in matches)
        matched, expected = matches[0][1], matches[0][2]
        ratio = matched / expected if expected else 0.0
        if ratio < min_conf:
            continue

        rows.append({
            "catalog": catalog_label,
            "schema": db,
            "benchmark": bench_label,
            "confidence": f"{ratio * 100:.0f}%",
            "matched/expected": f"{matched}/{expected}",
        })

    fmt = getattr(args, "format", "human")
    if fmt == "human":
        fmt = "table"
    if not rows:
        if fmt in ("json", "csv", "yaml"):
            print(_format_records([], fmt=fmt))
        else:
            print("(no benchmark datasets discovered)")
        return EXIT_OK

    print(_format_records(rows, fmt=fmt))
    return EXIT_OK


def cmd_doctor(args):
    """Sanity-check the environment.

    Checks: profile loads, engine extras importable, datagen tools present,
    Java available if any Spark engine is in any profile, write perms on
    results dir.
    """
    import importlib
    import shutil
    import subprocess
    rc = EXIT_OK

    def ok(msg): print(f"  \u2713 {msg}")
    def bad(msg):
        nonlocal rc
        rc = EXIT_USER_ERROR
        print(f"  \u2717 {msg}")

    print("=== Profile / config ===")
    try:
        cfg = load_config(getattr(args, "config", None))
        profiles = cfg.get("profiles", {})
        ok(f"loaded {len(profiles)} profile(s): {', '.join(sorted(profiles)) or '(none)'}")
        if args.profile:
            try:
                load_profile(args.profile, config_path=getattr(args, "config", None))
                ok(f"profile '{args.profile}' resolves cleanly")
            except Exception as e:
                bad(f"profile '{args.profile}' failed: {e}")
    except Exception as e:
        bad(f"config load failed: {e}")

    print("\n=== Engine extras ===")
    for name, (mod, cls) in sorted(ENGINE_REGISTRY.items()):
        try:
            importlib.import_module(mod)
            getattr(importlib.import_module(mod), cls)
            ok(f"{name}: import OK")
        except Exception as e:
            print(f"  \u00b7 {name}: not installed ({type(e).__name__})")

    print("\n=== Datagen tools ===")
    for tool in ("tpchgen-cli", "duckdb", "java"):
        path = shutil.which(tool)
        if path:
            ok(f"{tool}: {path}")
        else:
            print(f"  \u00b7 {tool}: not on PATH (only needed for some workflows)")

    print("\n=== Cloud auth ===")
    az_path = shutil.which("az")
    if az_path:
        ok(f"az: {az_path}")
        # Check for an active login (cheap; no network call required)
        try:
            r = subprocess.run(
                ["az", "account", "show", "-o", "tsv", "--query", "user.name"],
                capture_output=True, text=True, timeout=10
            )
            if r.returncode == 0 and r.stdout.strip():
                ok(f"az login OK (user: {r.stdout.strip()})")
            else:
                print("  \u00b7 az: not logged in. Run 'az login' before using "
                      "Fabric / Databricks / Synapse / HDInsight profiles "
                      "with auth=az.")
        except Exception as e:
            print(f"  \u00b7 az login check skipped ({type(e).__name__})")
    else:
        # Only flag this if at least one profile uses az auth
        uses_az = any(
            (p.get("engine_options") or {}).get("auth") == "az"
            for p in (locals().get("cfg", {}).get("profiles", {})).values()
        )
        if uses_az:
            bad("az CLI not on PATH but at least one profile uses auth=az.")
            print("    Install: https://learn.microsoft.com/cli/azure/install-azure-cli")
            print("      macOS:   brew install azure-cli")
            print("      Ubuntu:  curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash")
            print("      Windows: winget install -e --id Microsoft.AzureCLI")
        else:
            print("  \u00b7 az: not on PATH (needed only for Fabric / Databricks / "
                  "Synapse / HDInsight with auth=az)")

    print("\n=== Results directory ===")
    rd = getattr(args, "results_dir", None) or os.path.expanduser("~/.lakebench/results")
    try:
        os.makedirs(rd, exist_ok=True)
        # write probe
        probe = os.path.join(rd, ".doctor-probe")
        with open(probe, "w") as f:
            f.write("ok")
        os.remove(probe)
        ok(f"writable: {rd}")
    except Exception as e:
        bad(f"results dir not writable: {rd} ({e})")

    return rc


def cmd_results_tag(args):
    """Add or replace tags on a saved run's metadata.json."""
    rm = _get_results_manager(args)
    rid = _resolve_run_id(rm, args.run_id)
    run_dir = rm._find_run_dir(rid)
    if not run_dir:
        print(f"Run '{args.run_id}' not found.", file=sys.stderr)
        return EXIT_USER_ERROR
    meta_path = os.path.join(run_dir, "metadata.json")
    with open(meta_path) as f:
        meta = json.load(f)
    tags = set(meta.get("tags", []))
    for t in args.tag:
        tags.add(t)
    meta["tags"] = sorted(tags)
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2, default=str)
    print(f"Tags now: {', '.join(meta['tags'])}")
    return EXIT_OK


def cmd_results_notes(args):
    """Set the 'notes' field on a saved run's metadata.json."""
    rm = _get_results_manager(args)
    rid = _resolve_run_id(rm, args.run_id)
    run_dir = rm._find_run_dir(rid)
    if not run_dir:
        print(f"Run '{args.run_id}' not found.", file=sys.stderr)
        return EXIT_USER_ERROR
    meta_path = os.path.join(run_dir, "metadata.json")
    with open(meta_path) as f:
        meta = json.load(f)
    meta["notes"] = args.note
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2, default=str)
    print(f"Notes saved on {args.run_id}")
    return EXIT_OK


def cmd_results_compare(args):
    """Side-by-side comparison of two run_ids."""
    rm = _get_results_manager(args)
    rid_a = _resolve_run_id(rm, args.run_id_a)
    rid_b = _resolve_run_id(rm, args.run_id_b)
    a = rm.get_run(rid_a)
    b = rm.get_run(rid_b)
    if not a:
        print(f"Run '{args.run_id_a}' not found.", file=sys.stderr); return EXIT_USER_ERROR
    if not b:
        print(f"Run '{args.run_id_b}' not found.", file=sys.stderr); return EXIT_USER_ERROR

    def by_query(run):
        out = {}
        results = run.get("results", {})
        items = results.get("test_item", [])
        durs = results.get("duration_ms", [])
        for i, item in enumerate(items):
            out.setdefault(item, []).append(durs[i] if i < len(durs) else None)
        return out

    qa, qb = by_query(a), by_query(b)
    keys = sorted(set(qa) | set(qb))
    rows = []
    for k in keys:
        ma = sum(qa.get(k, []) or [0]) / max(1, len(qa.get(k, []) or [1]))
        mb = sum(qb.get(k, []) or [0]) / max(1, len(qb.get(k, []) or [1]))
        delta = (mb - ma) / ma * 100 if ma else 0
        rows.append({
            "query": k,
            f"{rid_a[:12]}_ms": int(ma),
            f"{rid_b[:12]}_ms": int(mb),
            "delta_pct": f"{delta:+.1f}%",
        })
    fmt = getattr(args, "format", "table")
    print(_format_records(rows, fmt))
    return EXIT_OK


def build_parser():
    """Build the argument parser."""
    parser = argparse.ArgumentParser(
        prog="lakebench",
        description="LakeBench — Multi-modal lakehouse benchmarking framework",
    )
    parser.add_argument(
        "--version", "-V", action="version",
        version=f"lakebench {_lakebench_version()}",
    )
    parser.add_argument(
        "-v", "--verbose", action="count", default=0,
        help="Increase logging verbosity (-v=INFO, -vv=DEBUG).",
    )
    parser.add_argument(
        "-q", "--quiet", action="store_true",
        help="Suppress non-error logging.",
    )
    parser.add_argument(
        "--debug", action="store_true",
        help="On error, print the full traceback (default: single-line message).",
    )
    parser.add_argument(
        "--shell-init", choices=["bash", "zsh", "fish"], default=None,
        help="Print the shell snippet to enable tab completion (e.g. "
             "`eval \"$(lakebench --shell-init bash)\"`) and exit.",
    )
    parser.add_argument(
        "--results-dir", type=str, default=None,
        help="Override results storage directory (default: ~/.lakebench/results)",
    )
    parser.add_argument(
        "--config", type=str, default=None,
        help="Explicit profile config file (replaces ~/.lakebench.json + ./lakebench.json discovery).",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # --- run ---
    run_parser = subparsers.add_parser("run", help="Run a benchmark")
    run_parser.add_argument(
        "--profile", "-p", type=str, default=None,
        help="Profile name from .lakebench.json (uses default if not specified)",
    )
    run_parser.add_argument(
        "--engine", type=str, default=None,
        choices=sorted(ENGINE_REGISTRY.keys()),
        help="Inline engine name for profile-less runs. Mutually exclusive with "
             "--profile. Builds an ad-hoc profile from --engine + -E/--conf overlays. "
             "Local engines (duckdb, polars, daft, sail) only need a working-directory "
             "URI, which defaults to a tmp dir if not provided via -E.",
    )
    run_parser.add_argument(
        "--benchmark", "-b", type=str, required=True,
        choices=["tpch", "tpcds", "tpcdi", "eltbench", "clickbench"],
        help="Benchmark to run",
    )
    run_parser.add_argument("--scenario", "-s", type=str, default=None, help="Scenario name")
    run_parser.add_argument("--scale-factor", type=int, default=None, help="Scale factor")
    run_parser.add_argument("--input-uri", type=str, default=None, help="Input data URI")
    run_parser.add_argument(
        "--database", "--schema", dest="database", type=str, default=None,
        metavar="NAME",
        help="Point the engine at an existing catalog database/schema (sets "
             "engine_options.schema_name). Use with --mode query to benchmark "
             "pre-loaded data. Pair with --catalog for multi-catalog engines.",
    )
    run_parser.add_argument(
        "--catalog", type=str, default=None, metavar="NAME",
        help="Catalog name for multi-catalog engines (sets "
             "engine_options.catalog_name). Example: hive_metastore, "
             "spark_catalog, <unity-catalog>.",
    )
    run_parser.add_argument(
        "--save-results",
        action=argparse.BooleanOptionalAction, default=False,
        help="Also save results to remote Delta table (use --no-save-results to disable).",
    )
    run_parser.add_argument("--result-uri", type=str, default=None, help="Remote result table URI (requires --save-results)")
    run_parser.add_argument("--run-id", type=str, default=None, help="Run identifier")
    run_parser.add_argument("--mode", type=str, default=None,
        help="Benchmark mode. Validated against the target benchmark's "
             "MODE_REGISTRY (e.g. tpcds/tpch: load|query|power_test|load_and_query; "
             "eltbench: light; tpcdi: full|historical_only)")
    run_parser.add_argument("--query-list", type=str, default=None, help="Comma-separated list of queries to run (e.g., q1,q3,q7)")
    run_parser.add_argument(
        "--engine-option", "-E", action="append", default=[], metavar="KEY=VALUE",
        help="Override engine option (repeatable). VALUE is parsed as JSON when it "
             "looks like JSON, else kept as string. KEY may be dotted to reach into "
             "session_conf/engine_options/benchmark_options, e.g. "
             "-E session_conf.spark.sql.shuffle.partitions=400",
    )
    run_parser.add_argument(
        "--conf", action="append", default=[], metavar="KEY=VALUE",
        help="Shortcut that overlays onto engine_options.session_conf (repeatable). "
             "Equivalent to -E session_conf.KEY=VALUE but never JSON-parses VALUE, "
             "so Spark confs like spark.sql.shuffle.partitions=400 always land as "
             "strings. Example: --conf spark.sql.join.preferSortMergeJoin=true",
    )
    run_parser.add_argument(
        "--engine-options-file", type=str, default=None, metavar="FILE",
        help="Load engine-option overrides from a JSON object file (applied "
             "before -E so CLI flags win).",
    )
    run_parser.add_argument(
        "--conf-file", type=str, default=None, metavar="FILE",
        help="Load --conf overrides from a Java .properties or JSON file (applied "
             "before --conf so CLI flags win).",
    )
    run_parser.add_argument(
        "--fail-on-run-id-collision", action="store_true",
        help="Fail instead of warn+suffix when the provided --run-id already exists "
             "in the results store.",
    )
    run_parser.add_argument(
        "--retry", type=int, default=0, metavar="N",
        help="Reserved: retry transient query failures up to N times. Currently "
             "stored on the benchmark but not yet honored by all engines.",
    )
    run_parser.add_argument(
        "--continue-on-error", action="store_true",
        help="Treat an engine-level crash as a partial failure (exit 2) instead "
             "of an engine crash (exit 3) so chained CI steps can keep going.",
    )
    run_parser.add_argument(
        "--query-timeout", type=int, default=None, metavar="SECONDS",
        help="Per-query wall-clock cap. The engine cancels the running statement "
             "and surfaces a TimeoutError after this many seconds, instead of "
             "waiting for the engine's default cap (Livy: 3 hours). Honored by "
             "Livy today; other engines ignore.",
    )
    run_parser.add_argument(
        "--dry-run", action="store_true",
        help="Resolve profile + apply overlays + validate --mode, then print the "
             "effective config and exit. Never instantiates the engine.",
    )
    run_parser.add_argument(
        "--print-config", action="store_true",
        help="Alias for --dry-run that highlights the intent of inspecting the "
             "post-overlay profile.",
    )
    run_parser.set_defaults(func=cmd_run)

    # --- doctor ---
    doctor_parser = subparsers.add_parser("doctor", help="Sanity-check the environment")
    doctor_parser.add_argument("--profile", "-p", type=str, default=None,
        help="If supplied, additionally try to resolve this profile.")
    doctor_parser.set_defaults(func=cmd_doctor)

    # --- discover ---
    discover_parser = subparsers.add_parser(
        "discover",
        help="Probe a catalog engine for databases that match known benchmarks.",
    )
    discover_parser.add_argument(
        "--profile", "-p", type=str, default=None,
        help="Named profile from lakebench.json. Mutually exclusive with --engine.",
    )
    discover_parser.add_argument(
        "--engine", type=str, default=None,
        choices=sorted(ENGINE_REGISTRY.keys()),
        help="Inline engine name for profile-less discovery.",
    )
    discover_parser.add_argument(
        "--catalog", type=str, default=None,
        help="Restrict scan to this catalog (Spark family only; issues USE CATALOG).",
    )
    discover_parser.add_argument(
        "--min-confidence", type=float, default=0.0,
        help="Hide schemas below this match ratio (0.0-1.0; default 0.0 shows all matches).",
    )
    discover_parser.add_argument(
        "--include-empty", action="store_true",
        help="Also show schemas with no benchmark match.",
    )
    discover_parser.add_argument(
        "--format", choices=("human", "table", "json", "csv", "yaml"), default="human",
        help="Output format (default: human table).",
    )
    discover_parser.add_argument(
        "-E", "--engine-option", action="append", default=[], metavar="KEY=VAL",
        help="Override an engine option (same semantics as `lakebench run`).",
    )
    discover_parser.add_argument(
        "--conf", action="append", default=[], metavar="KEY=VAL",
        help="Override a session_conf key (same semantics as `lakebench run`).",
    )
    discover_parser.set_defaults(func=cmd_discover)

    # --- list-modes ---
    modes_parser = subparsers.add_parser(
        "list-modes", help="Print supported modes for a benchmark"
    )
    modes_parser.add_argument(
        "benchmark", nargs="?", default=None,
        choices=["tpch", "tpcds", "tpcdi", "eltbench", "clickbench"],
        help="Benchmark name (omit to list modes for all benchmarks)",
    )
    modes_parser.set_defaults(func=cmd_list_modes)

    # --- datagen ---
    datagen_parser = subparsers.add_parser("datagen", help="Generate benchmark data")
    datagen_parser.add_argument(
        "--benchmark", "-b", type=str, required=True,
        choices=["tpch", "tpcds", "tpcdi", "clickbench"],
        help="Benchmark data to generate",
    )
    datagen_parser.add_argument("--scale-factor", type=int, required=True, help="Scale factor")
    datagen_parser.add_argument("--output", "-o", type=str, required=True, help="Output directory/URI")
    datagen_parser.add_argument("--digen-jar", type=str, default=None, help="Path to DIGen.jar (TPC-DI only)")
    datagen_parser.set_defaults(func=cmd_datagen)

    # --- profiles ---
    profiles_parser = subparsers.add_parser("profiles", help="Manage profiles")
    profiles_sub = profiles_parser.add_subparsers(dest="profiles_command")

    list_parser = profiles_sub.add_parser("list", help="List available profiles")
    list_parser.set_defaults(func=cmd_profiles_list)

    show_parser = profiles_sub.add_parser("show", help="Show a profile")
    show_parser.add_argument("name", type=str, help="Profile name")
    show_parser.set_defaults(func=cmd_profiles_show)

    # --- results ---
    results_parser = subparsers.add_parser("results", help="Manage saved results")
    results_sub = results_parser.add_subparsers(dest="results_command")

    res_list = results_sub.add_parser("list", help="List saved runs")
    res_list.add_argument("--benchmark", type=str, default=None, help="Filter by benchmark")
    res_list.add_argument("--engine", type=str, default=None, help="Filter by engine")
    res_list.add_argument("--scenario", type=str, default=None, help="Filter by scenario")
    res_list.add_argument("--limit", type=int, default=20, help="Max runs to show")
    res_list.add_argument("--format", type=str, default="human",
        choices=["human", "table", "json", "csv", "yaml"],
        help="Output format (default: human-readable report).")
    res_list.set_defaults(func=cmd_results_list)

    res_show = results_sub.add_parser("show", help="Show a run's details")
    res_show.add_argument("run_id", type=str, help="Run ID (or prefix)")
    res_show.set_defaults(func=cmd_results_show)

    res_delete = results_sub.add_parser("delete", help="Delete a run")
    res_delete.add_argument("run_id", type=str, help="Run ID (or prefix)")
    res_delete.set_defaults(func=cmd_results_delete)

    res_tag = results_sub.add_parser("tag", help="Add tags to a run's metadata.json")
    res_tag.add_argument("run_id", type=str, help="Run ID (or prefix)")
    res_tag.add_argument("tag", nargs="+", help="One or more tags to add")
    res_tag.set_defaults(func=cmd_results_tag)

    res_notes = results_sub.add_parser("notes", help="Set the 'notes' field on a run")
    res_notes.add_argument("run_id", type=str, help="Run ID (or prefix)")
    res_notes.add_argument("note", type=str, help="Free-form text")
    res_notes.set_defaults(func=cmd_results_notes)

    res_compare = results_sub.add_parser("compare", help="Side-by-side compare of two runs")
    res_compare.add_argument("run_id_a", type=str, help="First run id (or prefix)")
    res_compare.add_argument("run_id_b", type=str, help="Second run id (or prefix)")
    res_compare.add_argument("--format", type=str, default="table",
        choices=["table", "json", "csv", "yaml"], help="Output format")
    res_compare.set_defaults(func=cmd_results_compare)

    res_latest = results_sub.add_parser("latest", help="Show the N most recent runs")
    res_latest.add_argument("--limit", type=int, default=1, help="How many runs to show (default 1)")
    res_latest.add_argument("--format", type=str, default="human",
        choices=["human", "table", "json", "csv", "yaml"],
        help="Output format (human prints the report_summary of the single newest run).")
    res_latest.set_defaults(func=cmd_results_latest)

    res_purge = results_sub.add_parser("purge", help="Bulk-delete runs older than a duration")
    res_purge.add_argument("--older-than", type=str, required=True, metavar="DUR",
        help="Cutoff duration like 30d, 12h, 15m, 90s.")
    res_purge.add_argument("--benchmark", type=str, default=None, help="Filter by benchmark")
    res_purge.add_argument("--engine", type=str, default=None, help="Filter by engine")
    res_purge.add_argument("--scenario", type=str, default=None, help="Filter by scenario")
    res_purge.add_argument("--dry-run", action="store_true",
        help="Preview the deletion list without removing anything.")
    res_purge.add_argument("--yes", action="store_true",
        help="Required to actually delete (safety belt).")
    res_purge.set_defaults(func=cmd_results_purge)

    res_stats = results_sub.add_parser("stats",
        help="Aggregate per-query duration_ms across runs (n, mean, p50, p95, min, max).")
    res_stats.add_argument("--benchmark", type=str, default=None, help="Filter by benchmark")
    res_stats.add_argument("--engine", type=str, default=None, help="Filter by engine")
    res_stats.add_argument("--scenario", type=str, default=None, help="Filter by scenario")
    res_stats.add_argument("--format", type=str, default="table",
        choices=["table", "json", "csv", "yaml"], help="Output format")
    res_stats.set_defaults(func=cmd_results_stats)

    res_export = results_sub.add_parser("export", help="Export results")
    res_export.add_argument("--run-id", type=str, default=None, help="Export specific run (default: all)")
    res_export.add_argument("--format", type=str, default="csv", choices=["csv", "json", "md"], help="Output format")
    res_export.add_argument("--output", "-o", type=str, default=None, help="Output file path (default: stdout)")
    res_export.set_defaults(func=cmd_results_export)

    # --- report ---
    report_parser = subparsers.add_parser("report", help="Generate reports")
    report_sub = report_parser.add_subparsers(dest="report_command")

    rep_summary = report_sub.add_parser("summary", help="Run summary report")
    rep_summary.add_argument("--run-id", type=str, default=None, help="Run ID (default: latest)")
    rep_summary.set_defaults(func=cmd_report_summary)

    rep_compare = report_sub.add_parser("compare", help="Cross-engine comparison")
    rep_compare.add_argument("--benchmark", type=str, default=None, help="Filter by benchmark")
    rep_compare.add_argument("--scenario", type=str, default=None, help="Filter by scenario")
    rep_compare.add_argument("--engines", type=str, default=None, help="Comma-separated engine names")
    rep_compare.add_argument("--run-ids", type=str, default=None, help="Comma-separated run IDs to compare")
    rep_compare.set_defaults(func=cmd_report_compare)

    rep_history = report_sub.add_parser("history", help="Historical runs")
    rep_history.add_argument("--benchmark", type=str, default=None, help="Filter by benchmark")
    rep_history.add_argument("--engine", type=str, default=None, help="Filter by engine")
    rep_history.add_argument("--scenario", type=str, default=None, help="Filter by scenario")
    rep_history.add_argument("--limit", type=int, default=20, help="Max runs to show")
    rep_history.add_argument("--format", type=str, default="human",
        choices=["human", "table", "json", "csv", "yaml"],
        help="Output format (default: human-readable report).")
    rep_history.set_defaults(func=cmd_report_history)

    return parser


def main():
    """CLI entry point."""
    parser = build_parser()
    # Optional tab-completion via argcomplete (no-op if not installed)
    try:
        import argcomplete
        argcomplete.autocomplete(parser)
    except ImportError:
        pass
    args = parser.parse_args()

    _configure_logging(getattr(args, "verbose", 0), getattr(args, "quiet", False))

    # --shell-init short-circuits everything else.
    if getattr(args, "shell_init", None):
        print(_SHELL_INIT_TEMPLATES[args.shell_init], end="")
        sys.exit(EXIT_OK)

    if not args.command:
        parser.print_help()
        sys.exit(EXIT_USER_ERROR)

    for subcmd in ("profiles", "results", "report"):
        if args.command == subcmd and not hasattr(args, "func"):
            parser.parse_args([subcmd, "--help"])
            sys.exit(EXIT_USER_ERROR)

    try:
        rc = args.func(args)
    except (KeyError, ValueError, EnvironmentError) as e:
        if getattr(args, "debug", False):
            import traceback
            traceback.print_exc()
        else:
            log.error("%s", e)
            print(f"Error: {e}", file=sys.stderr)
        sys.exit(EXIT_USER_ERROR)
    sys.exit(int(rc) if isinstance(rc, int) else EXIT_OK)


if __name__ == "__main__":
    main()
