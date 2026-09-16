from pathlib import Path

import pytest
import sqlglot
from sqlglot import exp

from lakebench.benchmarks import TPCDS, TPCH
from lakebench.benchmarks._load_and_query._query_normalizers import (
    QueryNormalizerContext,
    apply_query_normalizers,
    apply_source_normalizers,
    normalize_date_interval_arithmetic,
    parse_tpc_ansi_statements,
)
from lakebench.engines.duckdb import DuckDB
from tests.test_tpch_query_generation import _uninitialized_engine


def _normalize(expression):
    return apply_query_normalizers(
        expression, "date-test", {}, "tsql", {"date-test": (normalize_date_interval_arithmetic,)}
    )


@pytest.mark.parametrize("operator", ["+", "-"])
@pytest.mark.parametrize(("unit", "amount"), [("DAY", 111), ("MONTH", 3), ("YEAR", 1), ("DAY", -2), ("DAY", 0)])
@pytest.mark.parametrize("target", ["tsql", "spark", "duckdb", "mysql"])
def test_date_intervals_preserve_date_amount_and_sign(operator, unit, amount, target):
    source = parse_tpc_ansi_statements(f"SELECT CAST('1998-12-01' AS DATE) {operator} INTERVAL '{amount}' {unit}")[0]
    before = source.copy()
    normalized = _normalize(source)
    addition = normalized.find(exp.DateAdd)
    expected_amount = amount if operator == "+" else -amount

    assert source == before
    assert addition.this == source.expressions[0].this
    assert addition.args["unit"].name == unit
    assert int(addition.expression.sql()) == expected_amount
    assert not list(normalized.find_all(exp.Interval))
    assert _normalize(normalized) == normalized
    sql = normalized.sql(dialect=target, unsupported_level=sqlglot.ErrorLevel.RAISE)
    assert isinstance(sqlglot.parse_one(sql, read=target), exp.Select)
    if target == "tsql":
        assert sql == f"SELECT DATEADD({unit}, {expected_amount}, CAST('1998-12-01' AS DATE))"
        assert "INTERVAL" not in sql
        assert "DATE_SUB" not in sql


@pytest.mark.parametrize(
    "source",
    [
        "SELECT INTERVAL '1' DAY",
        "SELECT CAST('2000-01-01' AS DATE) + INTERVAL '1.5' DAY",
        "SELECT CAST('2000-01-01' AS DATE) + INTERVAL '1' HOUR",
        "SELECT CAST('2000-01-01' AS TIMESTAMP) + INTERVAL '1' DAY",
        "SELECT date_column + INTERVAL '1' DAY FROM t",
        "SELECT INTERVAL '1' DAY + CAST('2000-01-01' AS DATE)",
        "SELECT CAST('2000-01-01' AS DATE) * INTERVAL '1' DAY",
    ],
)
def test_date_interval_rule_rejects_unproven_shapes(source):
    with pytest.raises(ValueError, match="Expected"):
        _normalize(parse_tpc_ansi_statements(source)[0])


def test_date_interval_rule_preserves_unrelated_arithmetic_and_strings():
    source = parse_tpc_ansi_statements("SELECT 17 - 3, price + tax, 'INTERVAL ''111'' DAY' FROM t")[0]
    assert _normalize(source) == source


def test_date_interval_rule_rejects_nonliteral_amounts():
    source = parse_tpc_ansi_statements("SELECT CAST('2000-01-01' AS DATE) + INTERVAL '1' DAY")[0]
    source.find(exp.Interval).set("this", exp.column("amount"))
    with pytest.raises(ValueError, match="literal interval amount"):
        _normalize(source)


@pytest.mark.parametrize(
    ("date", "operator", "amount", "unit"),
    [
        ("1998-12-01", "-", 111, "DAY"),
        ("2000-03-01", "-", 1, "DAY"),
        ("2000-02-29", "+", 1, "YEAR"),
        ("2000-01-31", "+", 1, "MONTH"),
        ("2000-03-31", "-", 1, "MONTH"),
        ("2000-01-01", "-", -1, "DAY"),
    ],
)
def test_normalized_date_values_match_interval_arithmetic(date, operator, amount, unit):
    duckdb = pytest.importorskip("duckdb")
    source = f"SELECT CAST('{date}' AS DATE) {operator} INTERVAL '{amount}' {unit}"
    normalized = _normalize(parse_tpc_ansi_statements(source)[0])
    with duckdb.connect() as connection:
        expected = connection.execute(source).fetchall()
        assert connection.execute(normalized.sql(dialect="duckdb")).fetchall() == expected
        tsql = normalized.sql(dialect="tsql")
        reparsed = sqlglot.parse_one(tsql, read="tsql")
        assert connection.execute(reparsed.sql(dialect="duckdb")).fetchall() == expected


@pytest.mark.parametrize("benchmark_class", [TPCH, TPCDS])
@pytest.mark.parametrize("scale_factor", [1000, 10000])
def test_all_generated_date_intervals_have_registered_rules_and_valid_tsql(benchmark_class, scale_factor):
    engine = _uninitialized_engine(DuckDB)
    engine.SQLGLOT_DIALECT = "tsql"
    benchmark = benchmark_class(
        engine=engine,
        scenario_name="date-intervals",
        scale_factor=scale_factor,
        input_parquet_folder_uri="file:///tmp/tpc",
    )
    root = (
        Path(__file__).parents[1]
        / "src"
        / "lakebench"
        / "benchmarks"
        / benchmark_class.__name__.lower()
        / "resources"
        / "queries"
        / "canonical"
        / f"sf{scale_factor}"
    )
    affected = set()
    for name in benchmark.QUERY_REGISTRY:
        text = (root / f"{name}.sql").read_text(encoding="utf-8")
        source = apply_source_normalizers(
            benchmark._parse_canonical_query(name, text),
            QueryNormalizerContext(name, {}, benchmark.CANONICAL_QUERY_DIALECT, text),
            benchmark.SOURCE_NORMALIZERS,
        )
        intervals = list(source.find_all(exp.Interval))
        if intervals:
            affected.add(name)
        expected = [
            (
                interval.parent.this.sql(dialect="tsql"),
                interval.args["unit"].name,
                int(interval.this.this) * (-1 if isinstance(interval.parent, exp.Sub) else 1),
            )
            for interval in intervals
        ]
        if benchmark_class is TPCDS and name == "q72":
            expected.append(("d1.d_date", "DAY", 5))
        normalized = benchmark._normalize_canonical_query(name, text)
        again = apply_query_normalizers(
            normalized,
            name,
            benchmark._query_normalization_schema(),
            benchmark.CANONICAL_QUERY_DIALECT,
            benchmark.QUERY_NORMALIZERS,
        )
        assert normalized == again, name
        sql = benchmark._return_query_definition(name)
        rendered = sqlglot.parse_one(sql, read="tsql")
        assert not list(rendered.find_all(exp.Interval)), name
        assert "DATE_SUB(" not in sql.upper(), name
        actual = [
            (addition.this.sql(dialect="tsql"), addition.args["unit"].name, int(addition.expression.sql()))
            for addition in rendered.find_all(exp.DateAdd)
        ]
        assert sorted(actual) == sorted(expected), name
        if benchmark_class is TPCH and name == "q1":
            assert "DATEADD(DAY, -111, CAST('1998-12-01' AS DATE))" in sql
    registered = {
        name for name, rules in benchmark.QUERY_NORMALIZERS.items() if normalize_date_interval_arithmetic in rules
    }
    assert registered == affected
