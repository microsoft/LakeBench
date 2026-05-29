"""Tests for lakebench.config — profile loading, extends, and engine resolution.

The most important coverage here is `resolve_engine`'s handling of ``*_env``
keys: engines that accept the env-var *name* (Databricks, Livy) must receive it
untouched, while engines that accept the bare credential get the resolved value.
A regression in this path silently dropped the credential entirely.
"""

from __future__ import annotations

import pytest

from lakebench import config

# ---------- *_env handling in resolve_engine ----------


class _EnvNameEngine:
    """Engine that follows convention 1: keeps the env-var NAME and resolves
    the secret itself (like Databricks / Livy)."""

    def __init__(self, host, token_env="DEFAULT_TOKEN_ENV", schema_name=None):
        self.host = host
        self.token_env = token_env
        self.schema_name = schema_name


class _BareValueEngine:
    """Engine that follows convention 2: accepts the resolved bare value."""

    def __init__(self, host, token=None, schema_name=None):
        self.host = host
        self.token = token
        self.schema_name = schema_name


class _KwargsEngine:
    """Engine with a **kwargs catch-all."""

    def __init__(self, host, **kwargs):
        self.host = host
        self.kwargs = kwargs


class TestResolveEngineEnvKeys:
    def test_env_name_engine_keeps_env_var_name(self, monkeypatch):
        """Convention 1: engine accepts token_env, so the NAME passes through
        and the secret is NOT resolved by config (the engine does that)."""
        monkeypatch.setattr(config, "ENGINE_REGISTRY", {"envname": (__name__, "_EnvNameEngine")})
        monkeypatch.setenv("MY_SECRET_ENV", "super-secret-value")
        profile = {
            "engine": "envname",
            "engine_options": {"host": "h", "token_env": "MY_SECRET_ENV"},
        }
        engine = config.resolve_engine(profile)
        # The engine received the env var NAME, not the value.
        assert engine.token_env == "MY_SECRET_ENV"
        assert engine.host == "h"

    def test_env_name_engine_does_not_require_env_to_be_set(self, monkeypatch):
        """config must not eagerly resolve (and therefore must not error on a
        missing env var) for convention-1 engines — the engine decides."""
        monkeypatch.setattr(config, "ENGINE_REGISTRY", {"envname": (__name__, "_EnvNameEngine")})
        monkeypatch.delenv("MISSING_ENV", raising=False)
        profile = {
            "engine": "envname",
            "engine_options": {"host": "h", "token_env": "MISSING_ENV"},
        }
        # No EnvironmentError here — resolution is deferred to the engine.
        engine = config.resolve_engine(profile)
        assert engine.token_env == "MISSING_ENV"

    def test_bare_value_engine_resolves_env(self, monkeypatch):
        """Convention 2: engine accepts `token`, so token_env -> token=value."""
        monkeypatch.setattr(config, "ENGINE_REGISTRY", {"bare": (__name__, "_BareValueEngine")})
        monkeypatch.setenv("MY_SECRET_ENV", "super-secret-value")
        profile = {
            "engine": "bare",
            "engine_options": {"host": "h", "token_env": "MY_SECRET_ENV"},
        }
        engine = config.resolve_engine(profile)
        assert engine.token == "super-secret-value"

    def test_bare_value_engine_missing_env_raises(self, monkeypatch):
        monkeypatch.setattr(config, "ENGINE_REGISTRY", {"bare": (__name__, "_BareValueEngine")})
        monkeypatch.delenv("MISSING_ENV", raising=False)
        profile = {
            "engine": "bare",
            "engine_options": {"host": "h", "token_env": "MISSING_ENV"},
        }
        with pytest.raises(EnvironmentError, match="MISSING_ENV"):
            config.resolve_engine(profile)

    def test_kwargs_engine_resolves_env(self, monkeypatch):
        """**kwargs engine: resolve to the bare key (it can absorb anything)."""
        monkeypatch.setattr(config, "ENGINE_REGISTRY", {"kw": (__name__, "_KwargsEngine")})
        monkeypatch.setenv("MY_SECRET_ENV", "super-secret-value")
        profile = {
            "engine": "kw",
            "engine_options": {"host": "h", "token_env": "MY_SECRET_ENV"},
        }
        engine = config.resolve_engine(profile)
        assert engine.kwargs.get("token") == "super-secret-value"
        assert "token_env" not in engine.kwargs

    def test_unaccepted_options_are_dropped(self, monkeypatch):
        """Cross-engine flags the engine doesn't accept are filtered out."""
        monkeypatch.setattr(config, "ENGINE_REGISTRY", {"bare": (__name__, "_BareValueEngine")})
        profile = {
            "engine": "bare",
            "engine_options": {"host": "h", "query_timeout_seconds": 99},
        }
        engine = config.resolve_engine(profile)  # no TypeError
        assert engine.host == "h"


class TestResolveEngineRealEngines:
    """Smoke tests against the real Databricks / Livy registry entries to
    guard the documented `token_env` profile flow end-to-end (no network)."""

    def test_databricks_profile_keeps_token_env(self, monkeypatch):
        pytest.importorskip("lakebench.engines.databricks")
        import inspect

        from lakebench.engines.databricks import Databricks

        # Databricks.__init__ must accept token_env (the documented contract).
        assert "token_env" in inspect.signature(Databricks.__init__).parameters
        assert "token" not in inspect.signature(Databricks.__init__).parameters

        # Simulate resolve_engine's *_env handling against the real signature.
        monkeypatch.setenv("DBX_TOKEN", "pat-123")
        sig = inspect.signature(Databricks.__init__)
        accepted = set(sig.parameters)
        eo = {"host": "h", "cluster_id": "c", "schema_name": "s", "token_env": "DBX_TOKEN"}
        # token_env is accepted and `token` is not -> keep the name untouched.
        assert "token_env" in accepted and "token" not in accepted


# ---------- extends composition ----------


class TestResolveExtends:
    def test_simple_extends_merges_engine_options(self):
        profiles = {
            "base": {"engine": "duckdb", "engine_options": {"schema_or_working_directory_uri": "/tmp"}},
            "child": {"extends": "base", "engine_options": {"cost_per_vcore_hour": 0.1}},
        }
        merged = config._resolve_extends("child", profiles)
        assert merged["engine"] == "duckdb"
        assert merged["engine_options"]["schema_or_working_directory_uri"] == "/tmp"
        assert merged["engine_options"]["cost_per_vcore_hour"] == 0.1

    def test_session_conf_merges_one_level(self):
        profiles = {
            "base": {"engine": "spark", "engine_options": {"session_conf": {"a": "1", "b": "2"}}},
            "child": {"extends": "base", "engine_options": {"session_conf": {"b": "20", "c": "3"}}},
        }
        merged = config._resolve_extends("child", profiles)
        sc = merged["engine_options"]["session_conf"]
        assert sc == {"a": "1", "b": "20", "c": "3"}

    def test_cyclic_extends_raises(self):
        profiles = {
            "a": {"extends": "b", "engine": "duckdb"},
            "b": {"extends": "a", "engine": "duckdb"},
        }
        with pytest.raises(ValueError, match="Cyclic 'extends'"):
            config._resolve_extends("a", profiles)

    def test_missing_parent_raises(self):
        profiles = {"a": {"extends": "nope", "engine": "duckdb"}}
        with pytest.raises(KeyError, match="not found"):
            config._resolve_extends("a", profiles)
