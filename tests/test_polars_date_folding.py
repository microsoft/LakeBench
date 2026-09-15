"""Polars renders DuckDB SQL but cannot parse interval syntax, so constant date
offsets are folded for it. These tests pin the folded values to the arithmetic
the other engines perform at runtime."""

from pathlib import Path

import pytest
import sqlglot
from sqlglot import exp

from lakebench.benchmarks import TPCDS, TPCH
from lakebench.benchmarks._load_and_query._query_normalizers import (
    QueryNormalizerContext,
    apply_query_normalizers,
    fold_constant_date_arithmetic,
    normalize_date_interval_arithmetic,
    parse_tpc_ansi_statements,
)
from lakebench.engines.polars import Polars
from tests.conftest import _uninitialized_engine

_INTERVAL_QUERIES = {
    TPCH: {"q1", "q4", "q5", "q6", "q10", "q12", "q14", "q15", "q20"},
    TPCDS: {
        "q5",
        "q12",
        "q16",
        "q20",
        "q21",
        "q32",
        "q37",
        "q40",
        "q77",
        "q80",
        "q82",
        "q92",
        "q94",
        "q95",
        "q98",
    },
}


def _lower(source: str) -> exp.Expression:
    return apply_query_normalizers(
        parse_tpc_ansi_statements(source)[0],
        "fold-test",
        {},
        "tsql",
        {"fold-test": (normalize_date_interval_arithmetic,)},
    )


def _fold(expression: exp.Expression) -> exp.Expression:
    return apply_query_normalizers(expression, "fold-test", {}, "tsql", {"fold-test": (fold_constant_date_arithmetic,)})


def _polars_benchmark(benchmark_class, scale_factor=1000):
    engine = _uninitialized_engine(Polars)
    engine.SQLGLOT_DIALECT = Polars.SQLGLOT_DIALECT
    return benchmark_class(
        engine=engine,
        scenario_name="fold-test",
        scale_factor=scale_factor,
        input_parquet_folder_uri="file:///tmp/tpc",
    )


@pytest.mark.parametrize(
    ("date", "operator", "amount", "unit"),
    [
        ("1998-12-01", "-", 111, "DAY"),
        ("2000-08-19", "+", 14, "DAY"),
        ("1993-01-01", "+", 1, "YEAR"),
        ("1996-07-01", "+", 3, "MONTH"),
        ("2000-01-31", "+", 1, "MONTH"),
        ("2000-03-31", "-", 1, "MONTH"),
        ("2000-02-29", "+", 1, "YEAR"),
        ("1999-12-31", "+", 1, "DAY"),
        ("2000-01-01", "-", -1, "DAY"),
        ("2000-01-01", "+", 0, "DAY"),
    ],
)
def test_folded_dates_match_interval_arithmetic(date, operator, amount, unit):
    duckdb = pytest.importorskip("duckdb")
    source = f"SELECT CAST('{date}' AS DATE) {operator} INTERVAL '{amount}' {unit}"
    folded = _fold(_lower(source))

    assert not list(folded.find_all(exp.DateAdd))
    assert not list(folded.find_all(exp.Interval))
    with duckdb.connect() as connection:
        # DuckDB's DATE + INTERVAL yields a TIMESTAMP, so compare the date values.
        expected = connection.execute(source.replace("SELECT ", "SELECT CAST(", 1) + " AS DATE)").fetchall()
        assert connection.execute(folded.sql(dialect="duckdb")).fetchall() == expected


def test_folding_accepts_unpadded_generated_literals():
    """dsqgen emits month and day fields without zero padding, as in q16 and q94."""
    folded = _fold(_lower("SELECT CAST('2002-4-01' AS DATE) + INTERVAL '60' DAY"))
    assert folded.sql(dialect="duckdb") == "SELECT CAST('2002-05-31' AS DATE)"


def test_folding_is_idempotent_and_leaves_other_expressions_alone():
    folded = _fold(_lower("SELECT CAST('2000-01-01' AS DATE) + INTERVAL '1' DAY, price + tax FROM t"))
    assert _fold(folded.copy()) == folded
    assert folded.expressions[1].sql() == "price + tax"


@pytest.mark.parametrize(
    "expression",
    [
        "DATEADD(DAY, 5, d1.d_date)",
        "DATEADD(DAY, offset_column, CAST('2000-01-01' AS DATE))",
        "DATEADD(HOUR, 5, CAST('2000-01-01' AS DATE))",
        "DATEADD(DAY, 5, CAST(d_date AS DATE))",
    ],
)
def test_non_constant_offsets_are_left_untouched(expression):
    """There is nothing to evaluate, and substituting a value would change the query."""
    source = sqlglot.parse_one(f"SELECT {expression} FROM t", read="tsql")
    assert _fold(source.copy()) == source


def test_q72_column_offset_survives_folding():
    benchmark = _polars_benchmark(TPCDS)
    sql = benchmark._return_query_definition("q72")
    assert "d1.d_date + INTERVAL 5 DAY" in sql


@pytest.mark.parametrize("benchmark_class", [TPCH, TPCDS])
@pytest.mark.parametrize("scale_factor", [1000, 10000])
def test_polars_queries_render_without_interval_syntax(benchmark_class, scale_factor):
    benchmark = _polars_benchmark(benchmark_class, scale_factor)
    for name in benchmark.QUERY_REGISTRY:
        sql = benchmark._return_query_definition(name)
        remaining = [
            interval
            for interval in sqlglot.parse_one(sql, read="duckdb").find_all(exp.Interval)
            # q72 offsets a date column, so it has no constant to fold.
            if not (benchmark_class is TPCDS and name == "q72")
        ]
        assert not remaining, f"{name} still renders interval syntax for Polars"


@pytest.mark.parametrize("benchmark_class", [TPCH, TPCDS])
def test_folded_dates_match_the_duckdb_engine_rendering(benchmark_class):
    """Polars must filter on exactly the dates DuckDB computes from the same source."""
    duckdb_module = pytest.importorskip("duckdb")
    from lakebench.engines.duckdb import DuckDB

    polars_benchmark = _polars_benchmark(benchmark_class)
    duckdb_engine = _uninitialized_engine(DuckDB)
    duckdb_engine.SQLGLOT_DIALECT = DuckDB.SQLGLOT_DIALECT
    duckdb_benchmark = benchmark_class(
        engine=duckdb_engine,
        scenario_name="fold-test",
        scale_factor=1000,
        input_parquet_folder_uri="file:///tmp/tpc",
    )

    compared = 0
    with duckdb_module.connect() as connection:
        for name in sorted(_INTERVAL_QUERIES[benchmark_class]):
            polars_sql = polars_benchmark._return_query_definition(name)
            duckdb_sql = duckdb_benchmark._return_query_definition(name)
            for interval in sqlglot.parse_one(duckdb_sql, read="duckdb").find_all(exp.Interval):
                offset = interval.parent
                assert isinstance(offset, (exp.Add, exp.Sub)), f"{name}: {offset.sql()}"
                expected = connection.execute(f"SELECT CAST({offset.sql(dialect='duckdb')} AS DATE)").fetchone()[0]
                assert f"CAST('{expected.isoformat()}' AS DATE)" in polars_sql, f"{name}: {offset.sql()}"
                compared += 1
    assert compared >= 9, f"expected folded offsets in {benchmark_class.__name__}"


@pytest.mark.parametrize("benchmark_class", [TPCH, TPCDS])
def test_folding_is_registered_only_for_polars(benchmark_class):
    for engine, rules in benchmark_class.ENGINE_QUERY_NORMALIZERS.items():
        registered = any(fold_constant_date_arithmetic in group for group in rules.values())
        assert registered is (engine is Polars), engine.__name__
    assert benchmark_class.ENGINE_QUERY_NORMALIZERS[Polars] == {"*": (fold_constant_date_arithmetic,)}


def test_source_files_are_untouched_by_folding():
    """Folding is a render-time accommodation; the canonical sources keep their intervals."""
    root = (
        Path(__file__).parents[1]
        / "src"
        / "lakebench"
        / "benchmarks"
        / "tpch"
        / "resources"
        / "queries"
        / "canonical"
        / "sf1000"
    )
    assert "interval" in (root / "q1.sql").read_text(encoding="utf-8").lower()


def test_context_is_available_to_the_rule():
    context = QueryNormalizerContext("q1", {}, "tsql", "", "duckdb")
    expression = _lower("SELECT CAST('2000-01-01' AS DATE) + INTERVAL '1' DAY")
    fold_constant_date_arithmetic(expression, context)
    assert expression.sql(dialect="duckdb") == "SELECT CAST('2000-01-02' AS DATE)"
