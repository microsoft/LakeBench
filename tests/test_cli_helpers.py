"""Tests for the extracted CLI helpers (cli._overrides, cli._format)."""

from __future__ import annotations

import json

import pytest

from lakebench.cli._format import format_records
from lakebench.cli._overrides import (
    apply_overrides,
    load_conf_file,
    load_eopts_file,
    parse_value,
    set_dotted,
)

# ---------- parse_value ----------


class TestParseValue:
    def test_returns_string_for_plain(self):
        assert parse_value("hello") == "hello"

    def test_parses_int(self):
        assert parse_value("42") == 42

    def test_parses_negative_int(self):
        assert parse_value("-7") == -7

    def test_parses_float(self):
        assert parse_value("3.14") == 3.14

    def test_parses_bool(self):
        assert parse_value("true") is True
        assert parse_value("false") is False

    def test_parses_null(self):
        assert parse_value("null") is None

    def test_parses_json_object(self):
        assert parse_value('{"a":1}') == {"a": 1}

    def test_parses_json_array(self):
        assert parse_value("[1,2,3]") == [1, 2, 3]

    def test_falls_back_to_string_on_invalid_json(self):
        # Looks JSON-ish (starts with `{`) but invalid → keep raw string.
        assert parse_value("{not json") == "{not json"

    def test_empty_returns_raw(self):
        assert parse_value("   ") == "   "


# ---------- set_dotted ----------


class TestSetDotted:
    def test_flat_key(self):
        d = {}
        set_dotted(d, "foo", 1)
        assert d == {"foo": 1}

    def test_dotted_key_outside_nestable_stays_flat(self):
        # spark.* keys should NOT be nested.
        d = {}
        set_dotted(d, "spark.sql.shuffle.partitions", "200")
        assert d == {"spark.sql.shuffle.partitions": "200"}

    def test_dotted_key_into_session_conf(self):
        d = {}
        set_dotted(d, "session_conf.spark.foo", "bar")
        assert d == {"session_conf": {"spark.foo": "bar"}}

    def test_into_engine_options(self):
        d = {}
        set_dotted(d, "engine_options.timeout", 30)
        assert d == {"engine_options": {"timeout": 30}}

    def test_raises_when_nestable_target_not_dict(self):
        d = {"session_conf": "oops"}
        with pytest.raises(ValueError, match="not a dict"):
            set_dotted(d, "session_conf.x", 1)


# ---------- apply_overrides ----------


class TestApplyOverrides:
    def test_eopt_creates_engine_options(self):
        prof = {}
        apply_overrides(prof, ["timeout=30"], [])
        assert prof == {"engine_options": {"timeout": 30}}

    def test_conf_creates_session_conf(self):
        prof = {}
        apply_overrides(prof, [], ["spark.sql.shuffle.partitions=200"])
        assert prof == {"engine_options": {"session_conf": {"spark.sql.shuffle.partitions": "200"}}}

    def test_conf_wins_over_eopt_for_session_conf(self):
        # Last writer wins; --conf is documented as the final word.
        prof = {}
        apply_overrides(
            prof,
            ["session_conf.spark.foo=bar_eopt"],
            ["spark.foo=bar_conf"],
        )
        assert prof["engine_options"]["session_conf"]["spark.foo"] == "bar_conf"

    def test_eopt_missing_equals_raises(self):
        with pytest.raises(ValueError, match="--engine-option must be KEY=VALUE"):
            apply_overrides({}, ["just_a_key"], [])

    def test_conf_missing_equals_raises(self):
        with pytest.raises(ValueError, match="--conf must be KEY=VALUE"):
            apply_overrides({}, [], ["just_a_key"])


# ---------- load_eopts_file / load_conf_file ----------


class TestLoadFiles:
    def test_load_eopts_json_object(self, tmp_path):
        p = tmp_path / "eopts.json"
        p.write_text(json.dumps({"timeout": 30, "name": "demo"}))
        out = load_eopts_file(str(p))
        # JSON-serialized for non-strings, raw for strings.
        assert "timeout=30" in out
        assert "name=demo" in out

    def test_load_eopts_rejects_non_object(self, tmp_path):
        p = tmp_path / "eopts.json"
        p.write_text("[1,2,3]")
        with pytest.raises(ValueError, match="JSON object"):
            load_eopts_file(str(p))

    def test_load_conf_properties(self, tmp_path):
        p = tmp_path / "conf.properties"
        p.write_text(
            "# header comment\nspark.sql.shuffle.partitions=200\n\n// also a comment\nspark.executor.memory=8g\n"
        )
        out = load_conf_file(str(p))
        assert out == [
            "spark.sql.shuffle.partitions=200",
            "spark.executor.memory=8g",
        ]

    def test_load_conf_json(self, tmp_path):
        p = tmp_path / "conf.json"
        p.write_text(json.dumps({"spark.foo": "bar", "spark.baz": "qux"}))
        out = load_conf_file(str(p))
        assert sorted(out) == ["spark.baz=qux", "spark.foo=bar"]

    def test_load_conf_rejects_malformed_line(self, tmp_path):
        p = tmp_path / "conf.properties"
        p.write_text("not a kv line\n")
        with pytest.raises(ValueError, match="missing '='"):
            load_conf_file(str(p))


# ---------- format_records ----------


class TestFormatRecords:
    def test_empty(self):
        assert format_records([]) == "(no rows)"

    def test_table_default(self):
        out = format_records([{"a": 1, "b": "x"}, {"a": 22, "b": "yyy"}])
        # Has header, separator, two rows.
        assert out.splitlines()[0].startswith("a")
        assert "22" in out and "yyy" in out

    def test_json(self):
        out = format_records([{"a": 1}], fmt="json")
        assert json.loads(out) == [{"a": 1}]

    def test_csv(self):
        out = format_records([{"a": 1, "b": "x"}], fmt="csv")
        assert out.splitlines()[0] == "a,b"
        assert out.splitlines()[1] == "1,x"

    def test_yaml(self):
        out = format_records([{"a": 1, "b": "x"}], fmt="yaml")
        assert out.startswith("- a: 1")
        assert "b: x" in out
