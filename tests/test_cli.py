"""
Smoke tests for the LakeBench CLI surface.

These tests focus on argparse plumbing and override merge logic. They do NOT
execute real benchmarks or touch engines. The CLI code path that instantiates
engines is exercised indirectly by monkey-patching ``resolve_engine`` and
``resolve_benchmark``.
"""

import json
import os
from pathlib import Path
from unittest import mock

import pytest

from lakebench import cli

# --- _parse_value: JSON-aware scalar parsing ---------------------------------


class TestParseValue:
    def test_plain_string_stays_string(self):
        assert cli._parse_value("hello") == "hello"
        # spark conf keys/values with dots are strings, not JSON
        assert cli._parse_value("spark.sql.foo") == "spark.sql.foo"

    def test_integer_string_becomes_int(self):
        assert cli._parse_value("400") == 400

    def test_negative_integer(self):
        assert cli._parse_value("-1") == -1

    def test_bool_literals(self):
        assert cli._parse_value("true") is True
        assert cli._parse_value("false") is False

    def test_null_literal(self):
        assert cli._parse_value("null") is None

    def test_quoted_string(self):
        assert cli._parse_value('"400"') == "400"

    def test_json_object(self):
        assert cli._parse_value('{"a": 1}') == {"a": 1}

    def test_json_array(self):
        assert cli._parse_value("[1, 2, 3]") == [1, 2, 3]

    def test_malformed_json_falls_back_to_string(self):
        # Starts with { but is not valid JSON -> keep as string
        assert cli._parse_value("{broken") == "{broken"


# --- _set_dotted: targeted nested overlays -----------------------------------


class TestSetDotted:
    def test_flat_key(self):
        d = {}
        cli._set_dotted(d, "schema_name", "foo")
        assert d == {"schema_name": "foo"}

    def test_dotted_into_session_conf(self):
        d = {}
        cli._set_dotted(d, "session_conf.spark.sql.shuffle.partitions", "400")
        assert d == {"session_conf": {"spark.sql.shuffle.partitions": "400"}}

    def test_dotted_merges_with_existing_session_conf(self):
        d = {"session_conf": {"spark.executor.cores": "8"}}
        cli._set_dotted(d, "session_conf.spark.sql.shuffle.partitions", "400")
        assert d["session_conf"] == {
            "spark.executor.cores": "8",
            "spark.sql.shuffle.partitions": "400",
        }

    def test_non_nestable_head_stays_flat(self):
        # spark.* is not a NESTABLE head, so it's stored as a single literal key
        d = {}
        cli._set_dotted(d, "spark.sql.shuffle.partitions", "400")
        assert d == {"spark.sql.shuffle.partitions": "400"}

    def test_session_conf_not_a_dict_raises(self):
        d = {"session_conf": "oops"}
        with pytest.raises(ValueError, match="not a dict"):
            cli._set_dotted(d, "session_conf.foo", "bar")


# --- _apply_overrides: full -E / --conf overlay ------------------------------


class TestApplyOverrides:
    def test_eopts_flat(self):
        profile = {"engine_options": {}}
        cli._apply_overrides(profile, ["schema_name=mydb"], [])
        assert profile["engine_options"] == {"schema_name": "mydb"}

    def test_eopts_dotted_session_conf(self):
        profile = {"engine_options": {"session_conf": {"spark.executor.cores": "8"}}}
        cli._apply_overrides(
            profile,
            ["session_conf.spark.sql.shuffle.partitions=400"],
            [],
        )
        sc = profile["engine_options"]["session_conf"]
        assert sc["spark.executor.cores"] == "8"
        assert sc["spark.sql.shuffle.partitions"] == 400  # int (JSON-parsed)

    def test_eopts_json_value(self):
        profile = {"engine_options": {}}
        cli._apply_overrides(
            profile,
            ['session_conf={"spark.sql.shuffle.partitions": "400"}'],
            [],
        )
        assert profile["engine_options"]["session_conf"] == {"spark.sql.shuffle.partitions": "400"}

    def test_conf_shortcut(self):
        profile = {"engine_options": {}}
        cli._apply_overrides(
            profile,
            [],
            ["spark.sql.join.preferSortMergeJoin=true", "spark.sql.shuffle.partitions=400"],
        )
        sc = profile["engine_options"]["session_conf"]
        # --conf always stores as strings (Spark expects strings anyway)
        assert sc == {
            "spark.sql.join.preferSortMergeJoin": "true",
            "spark.sql.shuffle.partitions": "400",
        }

    def test_conf_merges_with_existing_session_conf(self):
        profile = {"engine_options": {"session_conf": {"spark.executor.cores": "8"}}}
        cli._apply_overrides(profile, [], ["spark.sql.shuffle.partitions=400"])
        assert profile["engine_options"]["session_conf"] == {
            "spark.executor.cores": "8",
            "spark.sql.shuffle.partitions": "400",
        }

    def test_missing_equals_in_eopts_raises(self):
        profile = {"engine_options": {}}
        with pytest.raises(ValueError, match="--engine-option"):
            cli._apply_overrides(profile, ["no_equals"], [])

    def test_missing_equals_in_conf_raises(self):
        profile = {"engine_options": {}}
        with pytest.raises(ValueError, match="--conf"):
            cli._apply_overrides(profile, [], ["no_equals"])


# --- _supported_modes: benchmark mode lookup ---------------------------------


class TestSupportedModes:
    def test_tpcds(self):
        modes = cli._supported_modes("tpcds")
        assert modes is not None
        assert "query" in modes and "power_test" in modes and "load" in modes

    def test_tpch(self):
        modes = cli._supported_modes("tpch")
        assert modes is not None
        assert "query" in modes

    def test_tpcdi(self):
        modes = cli._supported_modes("tpcdi")
        assert modes is not None
        assert "full" in modes

    def test_eltbench(self):
        modes = cli._supported_modes("eltbench")
        assert modes is not None
        assert "light" in modes

    def test_unknown_benchmark_returns_none(self):
        assert cli._supported_modes("does_not_exist") is None


# --- argparse surface: parser builds and --mode is validated -----------------


class TestParser:
    def test_build_parser_ok(self):
        parser = cli.build_parser()
        # Parse a minimal `run` invocation - should not raise
        args = parser.parse_args(
            [
                "run",
                "--profile",
                "p",
                "--benchmark",
                "tpcds",
                "--mode",
                "query",
                "-E",
                "session_conf.spark.sql.shuffle.partitions=400",
                "--conf",
                "spark.sql.join.preferSortMergeJoin=true",
            ]
        )
        assert args.benchmark == "tpcds"
        assert args.mode == "query"
        assert args.engine_option == ["session_conf.spark.sql.shuffle.partitions=400"]
        assert args.conf == ["spark.sql.join.preferSortMergeJoin=true"]

    def test_missing_benchmark_fails(self):
        parser = cli.build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["run"])

    def test_fail_on_run_id_collision_flag_present(self):
        parser = cli.build_parser()
        args = parser.parse_args(
            [
                "run",
                "--benchmark",
                "tpch",
                "--fail-on-run-id-collision",
            ]
        )
        assert args.fail_on_run_id_collision is True

    def test_invalid_benchmark_choice(self):
        parser = cli.build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["run", "--benchmark", "nosuchbench"])


# --- cmd_run: mode validation rejects invalid modes --------------------------


class TestCmdRunModeValidation:
    def _args(self, **kw):
        # Build a Namespace with the minimum fields cmd_run reads
        defaults = dict(
            profile=None,
            benchmark="tpcds",
            mode="bogus_mode",
            scenario=None,
            scale_factor=None,
            input_uri=None,
            save_results=False,
            result_uri=None,
            run_id=None,
            query_list=None,
            engine_option=[],
            conf=[],
            results_dir=None,
            fail_on_run_id_collision=False,
        )
        defaults.update(kw)
        import argparse

        return argparse.Namespace(**defaults)

    def test_invalid_mode_rejected(self):
        args = self._args(mode="bogus_mode")
        with mock.patch("lakebench.cli.load_profile", return_value={"engine": "duckdb", "engine_options": {}}):
            with mock.patch("lakebench.cli.resolve_engine", return_value=mock.Mock()):
                with pytest.raises(ValueError, match="not supported"):
                    cli.cmd_run(args)

    def test_valid_mode_passes_validation(self):
        """The benchmark itself is mocked, so we only verify validation doesn't raise."""
        args = self._args(mode="query")
        fake_bench = mock.Mock(results=[], header_detail_dict={"run_id": "x"})
        with mock.patch("lakebench.cli.load_profile", return_value={"engine": "duckdb", "engine_options": {}}):
            with mock.patch("lakebench.cli.resolve_engine", return_value=mock.Mock()):
                with mock.patch("lakebench.cli.resolve_benchmark", return_value=fake_bench):
                    # No raise = pass
                    cli.cmd_run(args)


# --- ResultsManager: run_id collision detection ------------------------------


class TestRunIdCollision:
    """Verify the warn-and-suffix / fail-on-collision paths in save_run."""

    def _fake_benchmark(self, run_id="test-run-1"):
        from datetime import datetime, timezone

        return mock.Mock(
            results=[
                {
                    "run_id": run_id,
                    "run_datetime": datetime.now(timezone.utc),
                    "phase": "Query",
                    "test_item": "q1",
                    "start_datetime": datetime.now(timezone.utc),
                    "duration_ms": 123,
                    "estimated_retail_job_cost": None,
                    "iteration": 1,
                    "success": True,
                    "error_message": "",
                    "engine_properties": {},
                    "execution_telemetry": {},
                    "lakebench_version": "x",
                    "engine": "duckdb",
                    "engine_version": "x",
                    "benchmark": "tpch",
                    "benchmark_version": "x",
                    "mode": "query",
                    "scale_factor": 1,
                    "scenario": "test",
                    "total_cores": 1,
                    "compute_size": "tiny",
                }
            ],
            header_detail_dict={
                "run_id": run_id,
                "run_datetime": datetime.now(timezone.utc),
                "benchmark": "tpch",
                "scenario": "test",
                "engine": "duckdb",
                "engine_version": "x",
                "lakebench_version": "x",
                "scale_factor": 1,
                "total_cores": 1,
                "compute_size": "tiny",
            },
            engine=mock.Mock(
                extended_engine_metadata={},
                spark_configs={},
                mode="query",
                runtime="local",
                get_compute_size=lambda: "tiny",
            ),
        )

    def test_warn_and_suffix_on_collision(self, tmp_path, caplog):
        from lakebench.results import ResultsManager

        rm = ResultsManager(str(tmp_path))
        bench = self._fake_benchmark()
        # First save - clean
        d1 = rm.save_run(bench)
        # Second save with same run_id - should suffix and warn
        with caplog.at_level("WARNING", logger="lakebench.results"):
            d2 = rm.save_run(bench)
        assert d1 != d2
        assert "__2" in d2
        assert any("already exists" in r.message for r in caplog.records)

    def test_fail_on_collision_raises(self, tmp_path):
        from lakebench.results import ResultsManager

        rm = ResultsManager(str(tmp_path))
        bench = self._fake_benchmark()
        rm.save_run(bench)
        with pytest.raises(FileExistsError, match="already exists"):
            rm.save_run(bench, fail_on_collision=True)


# --- New surface (waves A-D): version, list-modes, dry-run, exit codes,
# --- file overrides, env expansion, profile extends, format flag, doctor,
# --- compare/tag/notes, prefix resolution, override-mixing precedence ----


class TestVersionFlag:
    def test_version_prints_and_exits(self, capsys):
        parser = cli.build_parser()
        with pytest.raises(SystemExit) as ei:
            parser.parse_args(["--version"])
        assert ei.value.code == 0
        out = capsys.readouterr().out
        assert out.startswith("lakebench ")


class TestListModes:
    def test_list_modes_for_one(self, capsys):
        import argparse

        ns = argparse.Namespace(benchmark="tpcds")
        cli.cmd_list_modes(ns)
        out = capsys.readouterr().out.splitlines()
        assert "query" in out

    def test_list_modes_all(self, capsys):
        import argparse

        ns = argparse.Namespace(benchmark=None)
        cli.cmd_list_modes(ns)
        out = capsys.readouterr().out
        assert "tpcds:" in out and "query" in out


class TestSaveResultsBoolFlag:
    def test_no_save_results_false(self):
        parser = cli.build_parser()
        args = parser.parse_args(["run", "--benchmark", "tpch", "--no-save-results"])
        assert args.save_results is False

    def test_save_results_true(self):
        parser = cli.build_parser()
        args = parser.parse_args(["run", "--benchmark", "tpch", "--save-results"])
        assert args.save_results is True

    def test_default_false(self):
        parser = cli.build_parser()
        args = parser.parse_args(["run", "--benchmark", "tpch"])
        assert args.save_results is False


class TestDryRun:
    def _ns(self, **kw):
        import argparse

        defaults = dict(
            profile=None,
            benchmark="tpcds",
            mode=None,
            scenario=None,
            scale_factor=None,
            input_uri=None,
            save_results=False,
            result_uri=None,
            run_id=None,
            query_list=None,
            engine_option=[],
            conf=[],
            engine_options_file=None,
            conf_file=None,
            results_dir=None,
            fail_on_run_id_collision=False,
            dry_run=True,
            print_config=False,
            retry=0,
            continue_on_error=False,
            config=None,
        )
        defaults.update(kw)
        return argparse.Namespace(**defaults)

    def test_dry_run_skips_engine(self, capsys):
        args = self._ns(dry_run=True)
        with mock.patch("lakebench.cli.load_profile", return_value={"engine": "duckdb", "engine_options": {}}):
            with mock.patch("lakebench.cli.resolve_engine") as re_mock:
                rc = cli.cmd_run(args)
        assert rc == cli.EXIT_OK
        re_mock.assert_not_called()
        assert "duckdb" in capsys.readouterr().out

    def test_dry_run_validates_mode(self, capsys):
        args = self._ns(mode="bogus", dry_run=True)
        with mock.patch("lakebench.cli.load_profile", return_value={"engine": "duckdb", "engine_options": {}}):
            with pytest.raises(ValueError, match="not supported"):
                cli.cmd_run(args)


class TestExitCodes:
    def test_constants(self):
        assert cli.EXIT_OK == 0
        assert cli.EXIT_USER_ERROR == 1
        assert cli.EXIT_PARTIAL_FAILURE == 2
        assert cli.EXIT_ENGINE_CRASH == 3


class TestFileOverlays:
    def test_eopts_file(self, tmp_path):
        f = tmp_path / "e.json"
        f.write_text('{"schema_name": "from_file", "session_conf": {"a": "1"}}')
        out = cli._load_eopts_file(str(f))
        assert "schema_name=from_file" in out
        assert any(o.startswith("session_conf=") for o in out)

    def test_conf_file_properties(self, tmp_path):
        f = tmp_path / "spark.conf"
        f.write_text("# comment\nspark.foo=bar\n  spark.baz=qux  \n")
        out = cli._load_conf_file(str(f))
        assert out == ["spark.foo=bar", "spark.baz=qux"]

    def test_conf_file_json(self, tmp_path):
        f = tmp_path / "spark.json"
        f.write_text('{"spark.foo":"bar","spark.baz":"qux"}')
        out = cli._load_conf_file(str(f))
        assert sorted(out) == ["spark.baz=qux", "spark.foo=bar"]


class TestEnvExpansionAndExtends:
    def test_env_expansion_in_profile(self, tmp_path, monkeypatch):
        monkeypatch.setenv("LB_TEST_VAR", "hello")
        cfg = tmp_path / "p.json"
        cfg.write_text('{"profiles":{"p":{"engine":"duckdb","engine_options":{"x":"${LB_TEST_VAR}-world"}}}}')
        from lakebench.config import load_profile

        prof = load_profile("p", config_path=str(cfg))
        assert prof["engine_options"]["x"] == "hello-world"

    def test_env_expansion_default(self, tmp_path, monkeypatch):
        monkeypatch.delenv("LB_NO_SUCH_VAR", raising=False)
        cfg = tmp_path / "p.json"
        cfg.write_text('{"profiles":{"p":{"engine":"duckdb","engine_options":{"x":"${LB_NO_SUCH_VAR:-fallback}"}}}}')
        from lakebench.config import load_profile

        prof = load_profile("p", config_path=str(cfg))
        assert prof["engine_options"]["x"] == "fallback"

    def test_extends_merges_session_conf(self, tmp_path):
        cfg = tmp_path / "p.json"
        cfg.write_text(
            '{"profiles":{'
            '"base":{"engine":"duckdb","engine_options":{"session_conf":{"a":"1","b":"2"}}},'
            '"child":{"extends":"base","engine_options":{"session_conf":{"b":"X","c":"3"}}}'
            "}}"
        )
        from lakebench.config import load_profile

        prof = load_profile("child", config_path=str(cfg))
        assert prof["engine_options"]["session_conf"] == {"a": "1", "b": "X", "c": "3"}

    def test_extends_cycle_detected(self, tmp_path):
        cfg = tmp_path / "p.json"
        cfg.write_text('{"profiles":{"a":{"extends":"b","engine":"duckdb"},"b":{"extends":"a","engine":"duckdb"}}}')
        from lakebench.config import load_profile

        with pytest.raises(ValueError, match="Cyclic"):
            load_profile("a", config_path=str(cfg))


class TestFormatRecords:
    def test_table(self):
        out = cli._format_records([{"a": 1, "b": "x"}, {"a": 2, "b": "yy"}], "table")
        assert "a" in out and "b" in out and "yy" in out

    def test_json(self):
        out = cli._format_records([{"a": 1}], "json")
        assert json.loads(out) == [{"a": 1}]

    def test_csv(self):
        out = cli._format_records([{"a": 1, "b": 2}, {"a": 3, "b": 4}], "csv")
        assert out.startswith("a,b") and "1,2" in out

    def test_empty(self):
        assert cli._format_records([], "json") == "(no rows)"


class TestPrefixResolution:
    def test_unique_prefix(self, tmp_path):
        from datetime import datetime, timezone

        from lakebench.results import ResultsManager

        rm = ResultsManager(str(tmp_path))
        bench = mock.Mock(
            results=[
                {
                    "run_id": "abcd1234-full-id",
                    "run_datetime": datetime.now(timezone.utc),
                    "phase": "Query",
                    "test_item": "q1",
                    "start_datetime": datetime.now(timezone.utc),
                    "duration_ms": 1,
                    "estimated_retail_job_cost": None,
                    "iteration": 1,
                    "success": True,
                    "error_message": "",
                    "engine_properties": {},
                    "execution_telemetry": {},
                    "lakebench_version": "x",
                    "engine": "duckdb",
                    "engine_version": "x",
                    "benchmark": "tpch",
                    "benchmark_version": "x",
                    "mode": "query",
                    "scale_factor": 1,
                    "scenario": "test",
                    "total_cores": 1,
                    "compute_size": "tiny",
                }
            ],
            header_detail_dict={
                "run_id": "abcd1234-full-id",
                "run_datetime": datetime.now(timezone.utc),
                "benchmark": "tpch",
                "scenario": "test",
                "engine": "duckdb",
                "engine_version": "x",
                "lakebench_version": "x",
                "scale_factor": 1,
                "total_cores": 1,
                "compute_size": "tiny",
            },
            engine=mock.Mock(
                extended_engine_metadata={},
                spark_configs={},
                mode="query",
                runtime="local",
                get_compute_size=lambda: "tiny",
            ),
        )
        rm.save_run(bench)
        assert cli._resolve_run_id(rm, "abcd") == "abcd1234-full-id"
        assert cli._resolve_run_id(rm, "abcd1234-full-id") == "abcd1234-full-id"

    def test_missing_index_passes_through(self, tmp_path):
        from lakebench.results import ResultsManager

        rm = ResultsManager(str(tmp_path))
        # No index yet — should just return what we passed in
        assert cli._resolve_run_id(rm, "anything") == "anything"


class TestOverridePrecedence:
    def test_conf_wins_over_eopt_for_same_key(self):
        profile = {"engine_options": {}}
        cli._apply_overrides(
            profile,
            eopts=["session_conf.spark.foo=eopt_value"],
            confs=["spark.foo=conf_value"],
        )
        assert profile["engine_options"]["session_conf"]["spark.foo"] == "conf_value"

    def test_eopt_dict_then_conf_layer(self):
        profile = {"engine_options": {}}
        cli._apply_overrides(
            profile,
            eopts=['session_conf={"a":"1"}'],
            confs=["b=2"],
        )
        assert profile["engine_options"]["session_conf"] == {"a": "1", "b": "2"}


# --- Wave E: results latest/purge/stats, --debug, --shell-init, validation -----


class TestParseDuration:
    def test_seconds(self):
        assert cli._parse_duration("90s") == 90.0

    def test_minutes(self):
        assert cli._parse_duration("15m") == 15 * 60

    def test_hours(self):
        assert cli._parse_duration("12h") == 12 * 3600

    def test_days(self):
        assert cli._parse_duration("30d") == 30 * 86400

    def test_weeks(self):
        assert cli._parse_duration("2w") == 2 * 7 * 86400

    def test_bare_int(self):
        assert cli._parse_duration("60") == 60.0

    def test_invalid(self):
        with pytest.raises(ValueError):
            cli._parse_duration("nonsense")


class TestShellInit:
    def test_bash_template(self):
        out = cli._SHELL_INIT_TEMPLATES["bash"]
        assert "register-python-argcomplete" in out and "lakebench" in out

    def test_zsh_template(self):
        out = cli._SHELL_INIT_TEMPLATES["zsh"]
        assert "bashcompinit" in out

    def test_fish_template(self):
        out = cli._SHELL_INIT_TEMPLATES["fish"]
        assert "fish" in out and "source" in out


class TestProfileSchemaValidation:
    def _write(self, tmp_path, body):
        p = tmp_path / "p.json"
        p.write_text(body)
        return str(p)

    def test_missing_engine(self, tmp_path):
        cfg = self._write(tmp_path, '{"profiles":{"p":{}}}')
        from lakebench.config import load_profile

        with pytest.raises(ValueError, match="missing a non-empty 'engine'"):
            load_profile("p", config_path=cfg)

    def test_unknown_engine(self, tmp_path):
        cfg = self._write(tmp_path, '{"profiles":{"p":{"engine":"nonsense"}}}')
        from lakebench.config import load_profile

        with pytest.raises(ValueError, match="unknown engine"):
            load_profile("p", config_path=cfg)

    def test_engine_options_must_be_dict(self, tmp_path):
        cfg = self._write(tmp_path, '{"profiles":{"p":{"engine":"duckdb","engine_options":[]}}}')
        from lakebench.config import load_profile

        with pytest.raises(ValueError, match="engine_options must be a dict"):
            load_profile("p", config_path=cfg)

    def test_session_conf_must_be_dict(self, tmp_path):
        cfg = self._write(tmp_path, '{"profiles":{"p":{"engine":"duckdb","engine_options":{"session_conf":"oops"}}}}')
        from lakebench.config import load_profile

        with pytest.raises(ValueError, match="session_conf must be a dict"):
            load_profile("p", config_path=cfg)

    def test_session_conf_value_must_be_scalar(self, tmp_path):
        cfg = self._write(
            tmp_path,
            '{"profiles":{"p":{"engine":"duckdb","engine_options":{"session_conf":{"k":["array","not","scalar"]}}}}}',
        )
        from lakebench.config import load_profile

        with pytest.raises(ValueError, match="must be a scalar"):
            load_profile("p", config_path=cfg)

    def test_valid_profile_passes(self, tmp_path):
        cfg = self._write(
            tmp_path,
            '{"profiles":{"p":{"engine":"duckdb","engine_options":{"session_conf":{"a":"1","b":2,"c":true}}}}}',
        )
        from lakebench.config import load_profile

        prof = load_profile("p", config_path=cfg)
        assert prof["engine"] == "duckdb"


class TestResultsLatest:
    def test_latest_empty(self, tmp_path, capsys):
        import argparse

        from lakebench.results import ResultsManager

        rm = ResultsManager(str(tmp_path))
        ns = argparse.Namespace(results_dir=str(tmp_path), limit=1, format="human")
        rc = cli.cmd_results_latest(ns)
        assert rc == cli.EXIT_OK
        assert "No runs found" in capsys.readouterr().out


class TestResultsStats:
    def _make(self, tmp_path, query, durations):
        from datetime import datetime, timezone

        from lakebench.results import ResultsManager

        rm = ResultsManager(str(tmp_path))
        for i, d in enumerate(durations):
            bench = mock.Mock(
                results=[
                    {
                        "run_id": f"run-{i}",
                        "run_datetime": datetime.now(timezone.utc),
                        "phase": "Query",
                        "test_item": query,
                        "start_datetime": datetime.now(timezone.utc),
                        "duration_ms": d,
                        "estimated_retail_job_cost": None,
                        "iteration": 1,
                        "success": True,
                        "error_message": "",
                        "engine_properties": {},
                        "execution_telemetry": {},
                        "lakebench_version": "x",
                        "engine": "duckdb",
                        "engine_version": "x",
                        "benchmark": "tpch",
                        "benchmark_version": "x",
                        "mode": "query",
                        "scale_factor": 1,
                        "scenario": "test",
                        "total_cores": 1,
                        "compute_size": "tiny",
                    }
                ],
                header_detail_dict={
                    "run_id": f"run-{i}",
                    "run_datetime": datetime.now(timezone.utc),
                    "benchmark": "tpch",
                    "scenario": "test",
                    "engine": "duckdb",
                    "engine_version": "x",
                    "lakebench_version": "x",
                    "scale_factor": 1,
                    "total_cores": 1,
                    "compute_size": "tiny",
                },
                engine=mock.Mock(
                    extended_engine_metadata={},
                    spark_configs={},
                    mode="query",
                    runtime="local",
                    get_compute_size=lambda: "tiny",
                ),
            )
            rm.save_run(bench)
        return rm

    def test_stats_aggregates(self, tmp_path, capsys):
        import argparse

        rm = self._make(tmp_path, "q1", [100, 200, 300, 400, 500])
        capsys.readouterr()  # drain any prior captured output
        ns = argparse.Namespace(results_dir=str(tmp_path), benchmark="tpch", engine=None, scenario=None, format="json")
        rc = cli.cmd_results_stats(ns)
        assert rc == cli.EXIT_OK
        out = json.loads(capsys.readouterr().out)
        assert len(out) == 1
        row = out[0]
        assert row["query"] == "q1"
        assert row["n"] == 5
        assert row["min_ms"] == 100 and row["max_ms"] == 500
        assert row["mean_ms"] == 300


class TestResultsPurge:
    def test_purge_dry_run(self, tmp_path, capsys):
        import argparse
        from datetime import datetime, timedelta, timezone

        from lakebench.results import ResultsManager

        rm = ResultsManager(str(tmp_path))
        old_dt = datetime.now(timezone.utc) - timedelta(days=60)
        new_dt = datetime.now(timezone.utc)
        for rid, dt in [("old-run", old_dt), ("new-run", new_dt)]:
            bench = mock.Mock(
                results=[
                    {
                        "run_id": rid,
                        "run_datetime": dt,
                        "phase": "Query",
                        "test_item": "q1",
                        "start_datetime": dt,
                        "duration_ms": 1,
                        "estimated_retail_job_cost": None,
                        "iteration": 1,
                        "success": True,
                        "error_message": "",
                        "engine_properties": {},
                        "execution_telemetry": {},
                        "lakebench_version": "x",
                        "engine": "duckdb",
                        "engine_version": "x",
                        "benchmark": "tpch",
                        "benchmark_version": "x",
                        "mode": "query",
                        "scale_factor": 1,
                        "scenario": "test",
                        "total_cores": 1,
                        "compute_size": "tiny",
                    }
                ],
                header_detail_dict={
                    "run_id": rid,
                    "run_datetime": dt,
                    "benchmark": "tpch",
                    "scenario": "test",
                    "engine": "duckdb",
                    "engine_version": "x",
                    "lakebench_version": "x",
                    "scale_factor": 1,
                    "total_cores": 1,
                    "compute_size": "tiny",
                },
                engine=mock.Mock(
                    extended_engine_metadata={},
                    spark_configs={},
                    mode="query",
                    runtime="local",
                    get_compute_size=lambda: "tiny",
                ),
            )
            rm.save_run(bench)
        ns = argparse.Namespace(
            results_dir=str(tmp_path),
            older_than="30d",
            benchmark=None,
            engine=None,
            scenario=None,
            dry_run=True,
            yes=False,
        )
        rc = cli.cmd_results_purge(ns)
        assert rc == cli.EXIT_OK
        out = capsys.readouterr().out
        assert "old-run" in out
        assert "new-run" not in out
        assert "dry-run" in out

    def test_purge_refuses_without_yes(self, tmp_path, capsys):
        import argparse
        from datetime import datetime, timedelta, timezone

        from lakebench.results import ResultsManager

        rm = ResultsManager(str(tmp_path))
        bench = mock.Mock(
            results=[
                {
                    "run_id": "old",
                    "run_datetime": datetime.now(timezone.utc) - timedelta(days=60),
                    "phase": "Query",
                    "test_item": "q1",
                    "start_datetime": datetime.now(timezone.utc),
                    "duration_ms": 1,
                    "estimated_retail_job_cost": None,
                    "iteration": 1,
                    "success": True,
                    "error_message": "",
                    "engine_properties": {},
                    "execution_telemetry": {},
                    "lakebench_version": "x",
                    "engine": "duckdb",
                    "engine_version": "x",
                    "benchmark": "tpch",
                    "benchmark_version": "x",
                    "mode": "query",
                    "scale_factor": 1,
                    "scenario": "test",
                    "total_cores": 1,
                    "compute_size": "tiny",
                }
            ],
            header_detail_dict={
                "run_id": "old",
                "run_datetime": datetime.now(timezone.utc) - timedelta(days=60),
                "benchmark": "tpch",
                "scenario": "test",
                "engine": "duckdb",
                "engine_version": "x",
                "lakebench_version": "x",
                "scale_factor": 1,
                "total_cores": 1,
                "compute_size": "tiny",
            },
            engine=mock.Mock(
                extended_engine_metadata={},
                spark_configs={},
                mode="query",
                runtime="local",
                get_compute_size=lambda: "tiny",
            ),
        )
        rm.save_run(bench)
        ns = argparse.Namespace(
            results_dir=str(tmp_path),
            older_than="30d",
            benchmark=None,
            engine=None,
            scenario=None,
            dry_run=False,
            yes=False,
        )
        rc = cli.cmd_results_purge(ns)
        assert rc == cli.EXIT_USER_ERROR
        assert "without --yes" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# Wave F: zero-config run (--engine flag + auto-create ~/.lakebench.json)
# ---------------------------------------------------------------------------


class TestZeroConfRun:
    def _ns(self, **kw):
        import argparse

        defaults = dict(
            profile=None,
            engine=None,
            benchmark="tpcds",
            mode=None,
            scenario=None,
            scale_factor=None,
            input_uri=None,
            save_results=False,
            result_uri=None,
            run_id=None,
            query_list=None,
            engine_option=[],
            conf=[],
            engine_options_file=None,
            conf_file=None,
            results_dir=None,
            fail_on_run_id_collision=False,
            dry_run=True,
            print_config=False,
            retry=0,
            continue_on_error=False,
            config=None,
        )
        defaults.update(kw)
        return argparse.Namespace(**defaults)

    # -- _synthesize_profile --------------------------------------------------

    def test_synthesize_profile_duckdb_defaults_working_dir(self):
        p = cli._synthesize_profile("duckdb")
        assert p["engine"] == "duckdb"
        assert p["engine_options"]["schema_or_working_directory_uri"]
        assert "lakebench-scratch" in p["engine_options"]["schema_or_working_directory_uri"]

    def test_synthesize_profile_unknown_engine(self):
        with pytest.raises(ValueError, match="Unknown engine"):
            cli._synthesize_profile("does-not-exist")

    def test_synthesize_profile_spark_uses_schema_name(self):
        p = cli._synthesize_profile("spark")
        assert p["engine"] == "spark"
        assert p["engine_options"]["schema_name"] == "lakebench"

    # -- --engine flag --------------------------------------------------------

    def test_engine_flag_skips_load_profile(self, capsys):
        args = self._ns(engine="duckdb", dry_run=True)
        with mock.patch("lakebench.cli.load_profile", side_effect=AssertionError("load_profile must not be called")):
            rc = cli.cmd_run(args)
        assert rc == cli.EXIT_OK
        out = capsys.readouterr().out
        assert '"engine": "duckdb"' in out

    def test_engine_and_profile_mutually_exclusive(self):
        args = self._ns(engine="duckdb", profile="local-duckdb")
        with pytest.raises(ValueError, match="mutually exclusive"):
            cli.cmd_run(args)

    def test_engine_flag_overlay_lands_on_synthesized_profile(self, capsys):
        args = self._ns(
            engine="duckdb",
            engine_option=["schema_or_working_directory_uri=/tmp/custom-from-cli"],
            dry_run=True,
        )
        rc = cli.cmd_run(args)
        assert rc == cli.EXIT_OK
        assert "/tmp/custom-from-cli" in capsys.readouterr().out

    # -- _maybe_auto_create_config --------------------------------------------

    def test_auto_create_picks_first_installed_engine(self, tmp_path, monkeypatch):
        cfg_path = tmp_path / ".lakebench.json"
        monkeypatch.setattr("lakebench.config.GLOBAL_CONFIG_PATH", str(cfg_path))
        # duckdb is installed in this venv → it should win first
        result = cli._maybe_auto_create_config()
        assert result == str(cfg_path)
        assert cfg_path.exists()
        data = json.loads(cfg_path.read_text())
        assert data["defaults"]["profile"].startswith("local-")
        engine = data["defaults"]["profile"].removeprefix("local-")
        assert engine in cli._AUTO_ENGINE_PRIORITY
        assert data["profiles"][f"local-{engine}"]["engine"] == engine

    def test_auto_create_skipped_when_config_exists(self, tmp_path, monkeypatch):
        cfg_path = tmp_path / ".lakebench.json"
        cfg_path.write_text('{"defaults":{"profile":"keep-me"},"profiles":{}}')
        monkeypatch.setattr("lakebench.config.GLOBAL_CONFIG_PATH", str(cfg_path))
        result = cli._maybe_auto_create_config()
        assert result is None
        # File untouched
        assert json.loads(cfg_path.read_text())["defaults"]["profile"] == "keep-me"

    def test_auto_create_returns_none_when_no_local_engine_importable(self, tmp_path, monkeypatch):
        cfg_path = tmp_path / ".lakebench.json"
        monkeypatch.setattr("lakebench.config.GLOBAL_CONFIG_PATH", str(cfg_path))

        import importlib

        real_import = importlib.import_module

        def fake_import(name, *args, **kwargs):
            # Simulate every local engine being uninstalled
            if name.startswith("lakebench.engines."):
                raise ImportError(f"simulated missing extra for {name}")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr("importlib.import_module", fake_import)
        result = cli._maybe_auto_create_config()
        assert result is None
        assert not cfg_path.exists()

    def test_cmd_run_triggers_auto_create_when_no_profile(self, tmp_path, monkeypatch, capsys):
        cfg_path = tmp_path / ".lakebench.json"
        # Both the cli's view and config's view of GLOBAL_CONFIG_PATH must point
        # at our tmp file so the auto-create writes there AND the subsequent
        # load reads it (instead of falling back to the user's real config).
        monkeypatch.setattr("lakebench.config.GLOBAL_CONFIG_PATH", str(cfg_path))
        monkeypatch.setattr(
            "lakebench.cli.load_profile",
            lambda name=None, config_path=None: __import__("lakebench.config", fromlist=["load_profile"]).load_profile(
                name, config_path=str(cfg_path)
            ),
        )
        # Also ensure project-level ./lakebench.json discovery doesn't trip us.
        monkeypatch.chdir(tmp_path)

        args = self._ns(dry_run=True)
        rc = cli.cmd_run(args)
        assert rc == cli.EXIT_OK
        assert cfg_path.exists(), "auto-create should have written the config"
        data = json.loads(cfg_path.read_text())
        assert data["defaults"]["profile"].startswith("local-")


class TestInputUriRouting:
    """The CLI exposes a single --input-uri but benchmarks name it differently:
    TPC-DI uses input_batch_folder_uri; everything else uses input_parquet_folder_uri.
    """

    def _ns(self, **kw):
        import argparse

        defaults = dict(
            profile=None,
            engine="duckdb",
            benchmark="tpcds",
            mode=None,
            scenario=None,
            scale_factor=None,
            input_uri="/tmp/x",
            save_results=False,
            result_uri=None,
            run_id=None,
            query_list=None,
            engine_option=[],
            conf=[],
            engine_options_file=None,
            conf_file=None,
            results_dir=None,
            fail_on_run_id_collision=False,
            dry_run=False,
            print_config=False,
            retry=0,
            continue_on_error=False,
            config=None,
        )
        defaults.update(kw)
        return argparse.Namespace(**defaults)

    def test_tpcdi_routes_to_input_batch_folder_uri(self):
        captured = {}

        def fake_resolve_benchmark(name, engine, profile, **kwargs):
            captured.update(kwargs)
            return mock.Mock(results=[], header_detail_dict={"run_id": "x"})

        args = self._ns(benchmark="tpcdi", input_uri="/tmp/tpcdi_sf3")
        with mock.patch("lakebench.cli.resolve_engine", return_value=mock.Mock()):
            with mock.patch("lakebench.cli.resolve_benchmark", side_effect=fake_resolve_benchmark):
                cli.cmd_run(args)
        assert captured.get("input_batch_folder_uri") == "/tmp/tpcdi_sf3"
        assert "input_parquet_folder_uri" not in captured

    def test_tpch_routes_to_input_parquet_folder_uri(self):
        captured = {}

        def fake_resolve_benchmark(name, engine, profile, **kwargs):
            captured.update(kwargs)
            return mock.Mock(results=[], header_detail_dict={"run_id": "x"})

        args = self._ns(benchmark="tpch", input_uri="/tmp/tpch_sf1")
        with mock.patch("lakebench.cli.resolve_engine", return_value=mock.Mock()):
            with mock.patch("lakebench.cli.resolve_benchmark", side_effect=fake_resolve_benchmark):
                cli.cmd_run(args)
        assert captured.get("input_parquet_folder_uri") == "/tmp/tpch_sf1"
        assert "input_batch_folder_uri" not in captured


class TestDiscover:
    """Tests for `lakebench discover` — catalog fingerprinting."""

    def _ns(self, **kw):
        import argparse

        defaults = dict(
            profile=None,
            engine=None,
            catalog=None,
            min_confidence=0.0,
            include_empty=False,
            format="table",
            engine_option=[],
            conf=[],
            config=None,
            results_dir=None,
        )
        defaults.update(kw)
        return argparse.Namespace(**defaults)

    # --- fingerprint_schema pure logic ---------------------------------------

    def test_fingerprint_full_tpcds(self):
        from lakebench import discover

        tpcds_tables = list(discover.BENCHMARK_TABLES["tpcds"])
        result = discover.fingerprint_schema(tpcds_tables)
        # TPC-DS and ELTBench share the same table set → both top at 100%.
        top = result[0]
        assert top[0] in ("tpcds", "eltbench")
        assert top[1] == top[2] == 24

    def test_fingerprint_partial_tpch(self):
        from lakebench import discover

        # 6 of the 8 TPC-H tables
        result = discover.fingerprint_schema(
            [
                "customer",
                "lineitem",
                "nation",
                "orders",
                "part",
                "partsupp",
            ]
        )
        assert result[0] == ("tpch", 6, 8)

    def test_fingerprint_case_insensitive(self):
        from lakebench import discover

        result = discover.fingerprint_schema(["CUSTOMER", "LineItem", "nation"])
        # should still count these as TPC-H matches
        tpch = next((r for r in result if r[0] == "tpch"), None)
        assert tpch is not None
        assert tpch[1] == 3

    def test_fingerprint_no_match_returns_empty(self):
        from lakebench import discover

        assert discover.fingerprint_schema(["foo", "bar"]) == []

    def test_all_equal_top_matches_eltbench_collision(self):
        from lakebench import discover

        tpcds_tables = list(discover.BENCHMARK_TABLES["tpcds"])
        tied = discover.all_equal_top_matches(tpcds_tables)
        labels = {t[0] for t in tied}
        # same table set → both benchmarks tied at 100%
        assert {"tpcds", "eltbench"}.issubset(labels)

    # --- cmd_discover wiring -------------------------------------------------

    def _fake_engine(self, db_to_tables):
        m = mock.Mock()
        m.list_databases.return_value = list(db_to_tables.keys())
        m.list_tables.side_effect = lambda db: db_to_tables.get(db, [])
        return m

    def test_cmd_discover_uses_engine_methods(self, capsys):
        from lakebench import discover as discover_mod

        tpch_tables = list(discover_mod.BENCHMARK_TABLES["tpch"])
        fake = self._fake_engine(
            {
                "tpch_sf1": tpch_tables,
                "misc": ["not_a_benchmark_table"],
            }
        )
        args = self._ns(engine="duckdb", format="csv")
        with mock.patch("lakebench.cli.resolve_engine", return_value=fake):
            rc = cli.cmd_discover(args)
        assert rc == cli.EXIT_OK
        out = capsys.readouterr().out
        assert "tpch_sf1" in out
        assert "tpch" in out
        assert "100%" in out
        # misc has no match and --include-empty is off → not shown
        assert "misc" not in out

    def test_cmd_discover_respects_min_confidence(self, capsys):
        from lakebench import discover as discover_mod

        partial = list(discover_mod.BENCHMARK_TABLES["tpcds"])[:5]  # 5/24 ≈ 21%
        full = list(discover_mod.BENCHMARK_TABLES["tpch"])  # 8/8 = 100%
        fake = self._fake_engine(
            {
                "partial_tpcds": partial,
                "full_tpch": full,
            }
        )
        args = self._ns(engine="duckdb", min_confidence=0.8, format="csv")
        with mock.patch("lakebench.cli.resolve_engine", return_value=fake):
            cli.cmd_discover(args)
        out = capsys.readouterr().out
        assert "full_tpch" in out
        assert "partial_tpcds" not in out

    def test_cmd_discover_engine_unsupported(self, capsys):
        fake = mock.Mock()
        fake.list_databases.side_effect = NotImplementedError("polars does not support catalog discovery")
        args = self._ns(engine="polars")
        with mock.patch("lakebench.cli.resolve_engine", return_value=fake):
            rc = cli.cmd_discover(args)
        assert rc == cli.EXIT_USER_ERROR
        assert "does not support catalog discovery" in capsys.readouterr().out

    def test_cmd_discover_engine_and_profile_mutex(self):
        args = self._ns(engine="duckdb", profile="local-duckdb")
        with pytest.raises(ValueError, match="mutually exclusive"):
            cli.cmd_discover(args)

    def test_cmd_discover_include_empty(self, capsys):
        fake = self._fake_engine({"empty_db": ["random_table"]})
        args = self._ns(engine="duckdb", include_empty=True, format="csv")
        with mock.patch("lakebench.cli.resolve_engine", return_value=fake):
            cli.cmd_discover(args)
        out = capsys.readouterr().out
        assert "empty_db" in out

    def test_cmd_discover_no_matches_default(self, capsys):
        fake = self._fake_engine({"empty_db": ["random_table"]})
        args = self._ns(engine="duckdb")
        with mock.patch("lakebench.cli.resolve_engine", return_value=fake):
            rc = cli.cmd_discover(args)
        assert rc == cli.EXIT_OK
        assert "no benchmark datasets discovered" in capsys.readouterr().out
