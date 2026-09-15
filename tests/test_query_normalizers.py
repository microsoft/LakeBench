import json
from unittest.mock import MagicMock

import pytest
import sqlglot
from sqlglot import exp

from lakebench.benchmarks._load_and_query import _LoadAndQuery
from lakebench.benchmarks._load_and_query._query_normalizers import (
    TPC_ANSI_READ_DIALECT,
    QueryNormalizerContext,
    apply_query_normalizers,
    apply_source_normalizers,
    normalize_implicit_joins,
    parse_tpc_ansi_statements,
)
from lakebench.engines.base import BaseEngine
from lakebench.utils.timer import timer


@pytest.mark.parametrize("target", ["spark", "tsql", "duckdb", "mysql"])
def test_builtin_ansi_reader_preserves_division_and_count(target):
    source = "SELECT SUM(amount) / SUM(quantity) AS ratio, COUNT(*) AS n FROM t ORDER BY ratio DESC"
    expression = parse_tpc_ansi_statements(source)[0]
    division = expression.find(exp.Div)
    assert division.args["typed"] is True
    assert division.args["safe"] is False
    rendered = expression.sql(dialect=target)
    parsed = sqlglot.parse_one(rendered, read=target)
    assert not list(parsed.find_all(exp.Nullif))
    assert not any(
        cast.args["to"].this in (exp.DataType.Type.FLOAT, exp.DataType.Type.DOUBLE)
        for cast in parsed.find_all(exp.Cast)
    )
    assert "COUNT_BIG" not in rendered
    assert parsed.args["order"].find(exp.Case) is None


def test_builtin_ansi_reader_handles_top_without_rewriting_source_sql():
    expression = parse_tpc_ansi_statements("SELECT TOP 10 a FROM t ORDER BY a")[0]
    assert TPC_ANSI_READ_DIALECT == "tsql"
    assert expression.args["limit"].expression.this == "10"


def test_builtin_ansi_reader_preserves_explicit_null_ordering():
    implicit = parse_tpc_ansi_statements("SELECT a, b FROM t ORDER BY a ASC, b DESC")[0]
    explicit = parse_tpc_ansi_statements("SELECT a, b FROM t ORDER BY a ASC NULLS LAST, b DESC NULLS FIRST")[0]
    assert [ordered.args["nulls_first"] for ordered in implicit.args["order"].expressions] == [True, False]
    assert [ordered.args["nulls_first"] for ordered in explicit.args["order"].expressions] == [False, True]
    duckdb = explicit.sql(dialect="duckdb")
    reparsed = sqlglot.parse_one(duckdb, read="duckdb")
    assert [ordered.args["nulls_first"] for ordered in reparsed.args["order"].expressions] == [False, True]


@pytest.mark.parametrize("side", ["LEFT", "RIGHT", "FULL"])
@pytest.mark.parametrize("placement", ["before", "after"])
def test_join_normalization_keeps_filters_in_scopes_with_outer_joins(side, placement):
    relations = (
        f"a CROSS JOIN b {side} JOIN c ON b.id = c.id"
        if placement == "after"
        else f"a {side} JOIN c ON a.id = c.id CROSS JOIN b"
    )
    source = sqlglot.parse_one(
        f"SELECT a.id AS aid, b.id AS bid, c.id AS cid FROM {relations} WHERE a.id = b.id",
        read="duckdb",
    )
    normalized = apply_query_normalizers(
        source,
        "synthetic",
        {name: {"id": "INT"} for name in ("a", "b", "c")},
        "duckdb",
        {"*": (normalize_implicit_joins,)},
    )
    assert normalized.args["where"] == source.args["where"]
    assert normalized.args["joins"] == source.args["joins"]


@pytest.mark.parametrize("side", ["RIGHT", "FULL"])
def test_later_outer_join_does_not_introduce_unmatched_rows(side):
    duckdb = pytest.importorskip("duckdb")
    query = f"SELECT a.id, b.id, c.id FROM a CROSS JOIN b {side} JOIN c ON b.id = c.id WHERE a.id = b.id"
    normalized = apply_query_normalizers(
        sqlglot.parse_one(query, read="duckdb"),
        "synthetic",
        {name: {"id": "INT"} for name in ("a", "b", "c")},
        "duckdb",
        {"*": (normalize_implicit_joins,)},
    )
    with duckdb.connect() as connection:
        connection.execute("CREATE TABLE a AS SELECT 1 AS id")
        connection.execute("CREATE TABLE b AS SELECT 1 AS id")
        connection.execute("CREATE TABLE c AS SELECT * FROM (VALUES (1), (2)) t(id)")
        assert connection.execute(query).fetchall() == [(1, 1, 1)]
        assert connection.execute(normalized.sql(dialect="duckdb")).fetchall() == [(1, 1, 1)]


def test_outer_join_scope_does_not_disable_inner_subquery_normalization():
    source = sqlglot.parse_one(
        "SELECT * FROM (SELECT a.id FROM a, b WHERE a.id = b.id) s LEFT JOIN c ON s.id = c.id",
        read="duckdb",
    )
    normalized = apply_query_normalizers(
        source,
        "synthetic",
        {name: {"id": "INT"} for name in ("a", "b", "c")},
        "duckdb",
        {"*": (normalize_implicit_joins,)},
    )
    subquery = normalized.find(exp.Subquery).this
    assert subquery.args.get("where") is None
    assert subquery.args["joins"][0].args["on"] is not None
    assert normalized.args["joins"][0].side == "LEFT"


def test_comma_join_becomes_explicit_without_moving_outer_join_filters():
    source = sqlglot.parse_one(
        "SELECT a.id FROM a LEFT JOIN c ON a.id = c.id, b WHERE a.id = b.id",
        read="postgres",
    )
    normalized = apply_query_normalizers(
        source,
        "synthetic",
        {name: {"id": "INT"} for name in ("a", "b", "c")},
        "postgres",
        {"*": (normalize_implicit_joins,)},
    )
    assert normalized.args["joins"][-1].kind == "CROSS"
    assert normalized.args["where"] == source.args["where"]


def test_rule_phases_and_engine_inheritance_order():
    calls = []

    class Parent(BaseEngine):
        pass

    class Child(Parent):
        pass

    def record(name):
        def rule(expression, context):
            calls.append((name, context.query_name))

        return rule

    context = QueryNormalizerContext("q1", {}, "duckdb")
    source = sqlglot.parse_one("SELECT 1")
    rules = []
    lowered = apply_source_normalizers(
        [source],
        context,
        {"*": (record("source-all"),), "q1": (record("source-q1"),)},
        applied_rules=rules,
    )
    apply_query_normalizers(
        lowered,
        "q1",
        {},
        "duckdb",
        {"*": (record("canonical-all"),), "q1": (record("canonical-q1"),)},
        engine_type=Child,
        engine_normalizers={
            Parent: {"*": (record("parent-all"),), "q1": (record("parent-q1"),)},
            Child: {"*": (record("child-all"),), "q1": (record("child-q1"),)},
        },
        applied_rules=rules,
    )
    assert calls == [
        (name, "q1")
        for name in (
            "source-all",
            "source-q1",
            "canonical-all",
            "canonical-q1",
            "parent-all",
            "parent-q1",
            "child-all",
            "child-q1",
        )
    ]
    assert [rule.split(":")[0] for rule in rules] == [
        "source",
        "source",
        "canonical",
        "canonical",
        "engine",
        "engine",
        "engine",
        "engine",
    ]


@pytest.mark.parametrize("sql", ["SELECT 1; SELECT 2", "CREATE TABLE t (id INT)", ""])
def test_unregistered_statement_bundles_fail_explicitly(sql):
    statements = [statement for statement in sqlglot.parse(sql) if statement is not None]
    with pytest.raises(ValueError, match="Expected one executable query"):
        apply_source_normalizers(statements, QueryNormalizerContext("q1", {}, "duckdb"), {})


@pytest.mark.parametrize("fails", [False, True])
def test_executed_rule_provenance_survives_engine_failure(fails):
    benchmark = object.__new__(_LoadAndQuery)
    benchmark.engine = MagicMock()
    benchmark.benchmark_impl = None
    benchmark.query_list = ["q1"]
    benchmark.query_progress = None
    benchmark._return_query_definition = lambda name: "SELECT 1"
    benchmark._applied_query_normalizers = {"q1": ["engine:example.rule"]}
    benchmark.engine.execute_sql_query.return_value = {"rows": "1"}
    if fails:
        benchmark.engine.execute_sql_query.side_effect = RuntimeError("query failed")
    benchmark.timer = timer
    benchmark.post_results = MagicMock()
    original_results = getattr(timer, "results", [])
    timer.results = []
    try:
        benchmark._run_query_test()
        result = timer.results[0]
        assert result[6] is not fails
        telemetry = result[8]
        assert json.loads(telemetry["query_normalization_rules"]) == ["engine:example.rule"]
        # The executed SQL is recorded whether or not the engine raised.
        assert result[9] == "SELECT 1"
        if not fails:
            assert telemetry["rows"] == "1"
    finally:
        timer.results = original_results
