"""
LakeBench profile configuration system.

Loads and merges profiles from:
- ~/.lakebench.json  (global user defaults)
- ./lakebench.json   (project-level overrides)
- Optional explicit path supplied via load_config(config_path=...)

Project profiles override global profiles with the same name.

Two convenience features at load time:

1. Environment variable expansion: any string value matching ``${VAR}`` or
   ``${VAR:-default}`` is replaced with ``os.environ[VAR]`` (or the default).
2. Profile composition: a profile may declare ``"extends": "<other-profile>"``
   to inherit and then override its parent. ``engine_options`` is merged at
   one level deep; everything else is shallow-overridden.
"""

import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

GLOBAL_CONFIG_PATH = os.path.expanduser("~/.lakebench.json")
PROJECT_CONFIG_NAME = "lakebench.json"

# Engine name -> (module_path, class_name) for lazy imports
ENGINE_REGISTRY = {
    "spark": ("lakebench.engines.spark", "Spark"),
    "fabric_spark": ("lakebench.engines.fabric_spark", "FabricSpark"),
    "synapse_spark": ("lakebench.engines.synapse_spark", "SynapseSpark"),
    "hdi_spark": ("lakebench.engines.hdi_spark", "HDISpark"),
    "duckdb": ("lakebench.engines.duckdb", "DuckDB"),
    "polars": ("lakebench.engines.polars", "Polars"),
    "daft": ("lakebench.engines.daft", "Daft"),
    "sail": ("lakebench.engines.sail", "Sail"),
    "spark_connect": ("lakebench.engines.spark_connect", "SparkConnect"),
    "databricks": ("lakebench.engines.databricks", "Databricks"),
    "livy": ("lakebench.engines.livy", "Livy"),
}

# Benchmark name -> (module_path, class_name)
BENCHMARK_REGISTRY = {
    "tpch": ("lakebench.benchmarks.tpch", "TPCH"),
    "tpcds": ("lakebench.benchmarks.tpcds", "TPCDS"),
    "tpcdi": ("lakebench.benchmarks.tpcdi", "TPCDI"),
    "eltbench": ("lakebench.benchmarks.elt_bench", "ELTBench"),
    "clickbench": ("lakebench.benchmarks.clickbench", "ClickBench"),
}

# Data generator name -> (module_path, class_name)
DATAGEN_REGISTRY = {
    "tpch": ("lakebench.datagen.tpch", "TPCHDataGenerator"),
    "tpcds": ("lakebench.datagen.tpcds", "TPCDSDataGenerator"),
    "tpcdi": ("lakebench.datagen.tpcdi", "TPCDIDataGenerator"),
    "clickbench": ("lakebench.datagen.clickbench", "ClickBenchDataGenerator"),
}


def _load_json(path: str) -> Dict[str, Any]:
    """Load a JSON file, returning empty dict if not found."""
    try:
        with open(path, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


def _expand_env(obj):
    """Recursively expand ${VAR} and ${VAR:-default} in all string values."""
    if isinstance(obj, str):

        def repl(m):
            var, default = m.group(1), m.group(2)
            return os.environ.get(var, default if default is not None else m.group(0))

        return _ENV_PATTERN.sub(repl, obj)
    if isinstance(obj, dict):
        return {k: _expand_env(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_expand_env(v) for v in obj]
    return obj


def _find_project_config() -> Optional[str]:
    """Walk up from cwd to find lakebench.json."""
    current = Path.cwd()
    for parent in [current] + list(current.parents):
        candidate = parent / PROJECT_CONFIG_NAME
        if candidate.is_file():
            return str(candidate)
    return None


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load and merge configs.

    Parameters
    ----------
    config_path : str, optional
        Explicit profile file path. When provided, *replaces* both the global
        and project-level discovery and is the only file consulted.

    Returns merged config dict with 'defaults' and 'profiles' keys with
    environment-variable expansion already applied.
    """
    if config_path:
        merged = _load_json(os.path.expanduser(config_path))
        merged = {
            "defaults": merged.get("defaults", {}),
            "profiles": merged.get("profiles", {}),
        }
        return _expand_env(merged)

    global_cfg = _load_json(GLOBAL_CONFIG_PATH)
    project_path = _find_project_config()
    project_cfg = _load_json(project_path) if project_path else {}

    # Merge: project wins
    merged = {
        "defaults": {**global_cfg.get("defaults", {}), **project_cfg.get("defaults", {})},
        "profiles": {**global_cfg.get("profiles", {}), **project_cfg.get("profiles", {})},
    }
    return _expand_env(merged)


def list_profiles(config_path: Optional[str] = None) -> List[str]:
    """Return list of available profile names."""
    config = load_config(config_path)
    return sorted(config.get("profiles", {}).keys())


def _resolve_extends(profile_name: str, profiles: dict, _seen: Optional[set] = None) -> Dict[str, Any]:
    """Resolve a profile's `extends` chain into a fully merged dict.

    Parent values are overlaid first, then child values override. ``engine_options``
    is merged one level deep so that ``session_conf`` from parent + child can
    coexist; deeper keys are shallow-overridden.
    """
    _seen = _seen or set()
    if profile_name in _seen:
        raise ValueError(f"Cyclic 'extends' detected involving profile '{profile_name}'")
    if profile_name not in profiles:
        available = ", ".join(sorted(profiles.keys())) or "(none)"
        raise KeyError(f"Profile '{profile_name}' not found. Available profiles: {available}")
    _seen = _seen | {profile_name}
    profile = dict(profiles[profile_name])
    parent_name = profile.pop("extends", None)
    if not parent_name:
        return profile
    parent = _resolve_extends(parent_name, profiles, _seen)
    merged = {**parent, **profile}
    # One-level merge for engine_options (so child session_conf doesn't wipe parent's)
    if "engine_options" in parent and "engine_options" in profile:
        merged_eo = {**parent["engine_options"], **profile["engine_options"]}
        for key in ("session_conf", "benchmark_options"):
            if key in parent["engine_options"] and key in profile["engine_options"]:
                merged_eo[key] = {
                    **parent["engine_options"][key],
                    **profile["engine_options"][key],
                }
        merged["engine_options"] = merged_eo
    return merged


def load_profile(
    profile_name: Optional[str] = None,
    config_path: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Load a specific profile by name.

    If profile_name is None, uses the default profile from config.
    Returns the profile dict with 'engine', 'engine_options', and any
    benchmark-level defaults merged in.

    Raises
    ------
    KeyError
        If the profile name is not found.
    ValueError
        If no profile name is specified and no default is configured.
    """
    config = load_config(config_path)
    defaults = config.get("defaults", {})
    profiles = config.get("profiles", {})

    if profile_name is None:
        profile_name = defaults.get("profile")
        if profile_name is None:
            raise ValueError(
                "No profile name specified and no default profile configured. "
                "Set 'defaults.profile' in ~/.lakebench.json or ./lakebench.json, "
                "or pass --profile <name>."
            )

    if profile_name not in profiles:
        available = ", ".join(sorted(profiles.keys())) or "(none)"
        raise KeyError(f"Profile '{profile_name}' not found. Available profiles: {available}")

    profile = _resolve_extends(profile_name, profiles)

    # Merge defaults into profile (profile values take precedence)
    result = {**defaults, **profile}
    result.pop("profile", None)  # Remove the meta 'profile' key from defaults
    _validate_profile(profile_name, result)
    return result


def _validate_profile(name: str, profile: Dict[str, Any]) -> None:
    """Cheap structural validation that produces friendly errors.

    Catches the most common typos before we hand the dict to ``resolve_engine``,
    where a missing key would produce a cryptic stack trace.
    """
    engine = profile.get("engine")
    if not isinstance(engine, str) or not engine:
        raise ValueError(f"Profile '{name}' is missing a non-empty 'engine' (string). Got: {engine!r}")
    if engine not in ENGINE_REGISTRY:
        available = ", ".join(sorted(ENGINE_REGISTRY))
        raise ValueError(f"Profile '{name}' references unknown engine '{engine}'. Available engines: {available}")
    eo = profile.get("engine_options", {})
    if not isinstance(eo, dict):
        raise ValueError(f"Profile '{name}': engine_options must be a dict, got {type(eo).__name__}")
    sc = eo.get("session_conf", {})
    if not isinstance(sc, dict):
        raise ValueError(f"Profile '{name}': engine_options.session_conf must be a dict, got {type(sc).__name__}")
    for k, v in sc.items():
        # Spark expects strings; non-strings here usually indicate a yaml/json typo
        # (e.g. partitions: 400 instead of "400").
        if not isinstance(v, (str, int, float, bool)):
            raise ValueError(
                f"Profile '{name}': session_conf['{k}'] must be a scalar (str/int/float/bool), got {type(v).__name__}"
            )


def _import_class(module_path: str, class_name: str):
    """Lazily import a class from a module path."""
    import importlib

    module = importlib.import_module(module_path)
    return getattr(module, class_name)


def resolve_engine(profile: Dict[str, Any]):
    """
    Instantiate an engine from a profile dict.

    Parameters
    ----------
    profile : dict
        Must contain 'engine' (str) and optionally 'engine_options' (dict).

    Returns
    -------
    BaseEngine
        An instantiated engine object.

    Raises
    ------
    ValueError
        If the engine name is not recognized.
    """
    engine_name = profile.get("engine")
    if engine_name not in ENGINE_REGISTRY:
        available = ", ".join(sorted(ENGINE_REGISTRY.keys()))
        raise ValueError(f"Unknown engine '{engine_name}'. Available engines: {available}")

    module_path, class_name = ENGINE_REGISTRY[engine_name]
    engine_cls = _import_class(module_path, class_name)

    engine_options = dict(profile.get("engine_options", {}))

    # Inspect the engine constructor up front so the *_env handling below can
    # honor what the engine actually accepts.
    import inspect as _inspect

    sig = _inspect.signature(engine_cls.__init__)
    accepted = set(sig.parameters)
    has_var_kw = any(p.kind == _inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())

    # Handle ``*_env`` references (e.g. ``token_env``, ``password_env``).
    #
    # Two engine conventions exist and both must work:
    #   1. The engine accepts the *_env key itself (Databricks, Livy) and does
    #      its own ``os.environ.get`` so the raw secret never leaves the engine.
    #      In that case we pass the env-var NAME through untouched.
    #   2. The engine accepts only the bare key (e.g. ``token``) or has a
    #      ``**kwargs`` catch-all. Then we resolve the env var to its value here
    #      and substitute the bare key.
    #
    # The previous implementation always stripped ``token_env`` -> ``token``,
    # which broke convention-1 engines: the bare ``token`` was then dropped by
    # the signature filter, leaving the engine with no credential at all.
    for key, value in list(engine_options.items()):
        if not (key.endswith("_env") and isinstance(value, str)):
            continue
        bare_key = key[:-4]  # e.g., token_env -> token
        engine_wants_env_key = key in accepted
        engine_wants_bare_key = bare_key in accepted
        if engine_wants_env_key and not engine_wants_bare_key:
            # Convention 1: leave the env-var name in place for the engine.
            continue
        if engine_wants_bare_key or has_var_kw:
            # Convention 2: resolve the env var to its value now.
            env_value = os.environ.get(value)
            if env_value is None:
                raise EnvironmentError(f"Environment variable '{value}' (referenced by '{key}') is not set.")
            engine_options[bare_key] = env_value
            del engine_options[key]
        # Otherwise the engine accepts neither form; leave it to be dropped by
        # the signature filter below.

    # Drop generic engine options that this engine's __init__ doesn't accept,
    # so cross-engine flags (e.g. --query-timeout, --database, --catalog) can
    # be set globally without breaking engines that don't know them. Only
    # filter when the engine has no **kwargs catch-all.
    if not has_var_kw:
        engine_options = {k: v for k, v in engine_options.items() if k in accepted}

    return engine_cls(**engine_options)


def resolve_benchmark(benchmark_name: str, engine, profile: Dict[str, Any], **overrides):
    """
    Instantiate a benchmark from a name, engine, profile, and CLI overrides.

    Parameters
    ----------
    benchmark_name : str
        One of: tpch, tpcds, tpcdi, eltbench, clickbench
    engine : BaseEngine
        Instantiated engine.
    profile : dict
        Profile dict (may contain benchmark_options).
    **overrides
        CLI overrides (scenario_name, scale_factor, input_parquet_folder_uri, etc.)

    Returns
    -------
    BaseBenchmark
        An instantiated benchmark object.
    """
    if benchmark_name not in BENCHMARK_REGISTRY:
        available = ", ".join(sorted(BENCHMARK_REGISTRY.keys()))
        raise ValueError(f"Unknown benchmark '{benchmark_name}'. Available: {available}")

    module_path, class_name = BENCHMARK_REGISTRY[benchmark_name]
    benchmark_cls = _import_class(module_path, class_name)

    # Merge profile benchmark_options with CLI overrides
    benchmark_options = dict(profile.get("benchmark_options", {}))
    for k, v in overrides.items():
        if v is not None:
            benchmark_options[k] = v

    # Map common profile keys into benchmark kwargs
    for key in ("save_results", "result_table_uri", "run_id"):
        if key in profile and key not in benchmark_options:
            benchmark_options[key] = profile[key]

    return benchmark_cls(engine=engine, **benchmark_options)


def resolve_datagen(benchmark_name: str, **kwargs):
    """
    Instantiate a data generator for a benchmark.

    Parameters
    ----------
    benchmark_name : str
        One of: tpch, tpcds, tpcdi, clickbench
    **kwargs
        Passed to the data generator constructor.

    Returns
    -------
    DataGenerator instance.
    """
    if benchmark_name not in DATAGEN_REGISTRY:
        available = ", ".join(sorted(DATAGEN_REGISTRY.keys()))
        raise ValueError(f"No data generator for '{benchmark_name}'. Available: {available}")

    module_path, class_name = DATAGEN_REGISTRY[benchmark_name]
    datagen_cls = _import_class(module_path, class_name)
    return datagen_cls(**kwargs)
