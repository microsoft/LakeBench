import hashlib
import json
import logging
from collections import Counter
from pathlib import Path
from unittest.mock import Mock

import pytest
import sqlglot
from sqlglot import exp

from lakebench.benchmarks import TPCDS, TPCH, ClickBench
from lakebench.benchmarks._load_and_query._query_normalizers import (
    TPC_ANSI_READ_DIALECT,
    QueryNormalizerContext,
    apply_query_normalizers,
    apply_source_normalizers,
    normalize_date_interval_arithmetic,
    normalize_implicit_joins,
)
from lakebench.benchmarks.tpcds._query_normalizers import (
    QUERY_NORMALIZERS as TPCDS_QUERY_NORMALIZERS,
)
from lakebench.benchmarks.tpch._query_normalizers import (
    QUERY_NORMALIZERS as TPCH_QUERY_NORMALIZERS,
)
from lakebench.benchmarks.tpch._query_normalizers import (
    SOURCE_NORMALIZERS,
    parse_tpch_ansi_query,
)
from lakebench.engines.daft import Daft
from lakebench.engines.duckdb import DuckDB
from lakebench.engines.polars import Polars
from lakebench.engines.sail import Sail
from lakebench.engines.spark import Spark

CANONICAL_ROOT = (
    Path(__file__).parents[1] / "src" / "lakebench" / "benchmarks" / "tpch" / "resources" / "queries" / "canonical"
)


def _sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _uninitialized_engine(engine_class):
    engine = engine_class.__new__(engine_class)
    engine.version = "test"
    engine.cost_per_vcore_hour = None
    engine.cost_per_hour = None
    engine.extended_engine_metadata = {}
    engine.storage_options = {}
    engine.schema_or_working_directory_uri = "file:///tmp/lakebench"
    engine.runtime = "local_unknown"
    engine.operating_system = "linux"
    engine.catalog_name = None
    engine.schema_name = None
    engine.get_total_cores = lambda: 1
    engine.get_compute_size = lambda: "test"
    return engine


def test_tpc_benchmarks_do_not_register_join_normalization():
    assert TPCH.CANONICAL_QUERY_DIALECT == TPCDS.CANONICAL_QUERY_DIALECT == TPC_ANSI_READ_DIALECT
    assert set(TPCH_QUERY_NORMALIZERS) == {"q1", "q4", "q5", "q6", "q10", "q12", "q14", "q15", "q20"}
    assert {"q36", "q58", "q70", "q72", "q86", "q90", "q97"} <= set(TPCDS_QUERY_NORMALIZERS)
    assert TPCDS_QUERY_NORMALIZERS["q36"] == TPCDS_QUERY_NORMALIZERS["q70"] == TPCDS_QUERY_NORMALIZERS["q86"]
    assert all(normalize_implicit_joins not in rules for rules in TPCDS_QUERY_NORMALIZERS.values())
    assert all(normalize_implicit_joins not in rules for rules in TPCH_QUERY_NORMALIZERS.values())
    assert all(normalize_date_interval_arithmetic in rules for rules in TPCH_QUERY_NORMALIZERS.values())


@pytest.mark.parametrize("scale_factor", [1000, 10000])
@pytest.mark.parametrize("dialect", ["tsql", "spark", "duckdb", "mysql"])
def test_q1_wide_count_changes_only_tsql_count_rendering(scale_factor, dialect):
    engine = _uninitialized_engine(DuckDB)
    engine.SQLGLOT_DIALECT = dialect
    benchmark = TPCH(
        engine=engine,
        scenario_name="wide-count",
        scale_factor=scale_factor,
        input_parquet_folder_uri="file:///tmp/tpch",
    )
    benchmark.QUERY_NORMALIZERS = {"q1": (normalize_date_interval_arithmetic,)}
    before = benchmark._return_query_definition("q1")
    benchmark.QUERY_NORMALIZERS = TPCH_QUERY_NORMALIZERS
    after = benchmark._return_query_definition("q1")

    if dialect == "tsql":
        assert "COUNT(*) AS count_order" in before
        assert after == before.replace("COUNT(*) AS count_order", "COUNT_BIG(*) AS count_order")
        assert "CAST(COUNT(" not in after
    else:
        assert after == before
        assert "COUNT(*) AS count_order" in after
    assert any(rule.endswith("._normalize_q1_wide_count") for rule in benchmark._applied_query_normalizers["q1"])

    source = (CANONICAL_ROOT / f"sf{scale_factor}" / "q1.sql").read_text(encoding="utf-8")
    parsed = benchmark._parse_canonical_query("q1", source)[0]
    assert not parsed.find(exp.Count).args.get("big_int")
    normalized = benchmark._normalize_canonical_query("q1", source)
    assert normalized.find(exp.Count).args["big_int"] is True
    assert (
        apply_query_normalizers(normalized, "q1", {}, benchmark.CANONICAL_QUERY_DIALECT, benchmark.QUERY_NORMALIZERS)
        == normalized
    )


@pytest.mark.parametrize(
    "projection",
    [
        "COUNT(*) AS wrong_name",
        "COUNT(l_orderkey) AS count_order",
        "COUNT(DISTINCT l_orderkey) AS count_order",
        "SUM(l_quantity) AS count_order",
        "COUNT(*) OVER () AS count_order",
        "COUNT(*) AS count_order, COUNT(*) AS count_order",
    ],
)
def test_q1_wide_count_rejects_unexpected_projection(projection):
    with pytest.raises(ValueError, match="count_order"):
        apply_query_normalizers(
            sqlglot.parse_one(f"SELECT {projection} FROM lineitem", read="tsql"),
            "q1",
            {},
            "tsql",
            TPCH_QUERY_NORMALIZERS,
        )


@pytest.mark.parametrize("scale_factor", [1000, 10000])
@pytest.mark.parametrize("query_name", ["q3", "q5", "q10", "q11", "q13", "q16", "q21"])
def test_tsql_ordering_does_not_embed_select_aliases_in_generated_cases(scale_factor, query_name):
    engine = _uninitialized_engine(DuckDB)
    engine.SQLGLOT_DIALECT = "tsql"
    benchmark = TPCH(
        engine=engine,
        scenario_name="tsql-ordering",
        scale_factor=scale_factor,
        input_parquet_folder_uri="file:///tmp/tpch",
    )
    query = benchmark._return_query_definition(query_name)
    expression = sqlglot.parse_one(query, read="tsql")
    assert expression.args["order"].find(exp.Case) is None
    assert all(isinstance(ordered.this, exp.Column) for ordered in expression.args["order"].expressions)
    assert not any("engine:" in rule for rule in benchmark._applied_query_normalizers[query_name])


@pytest.mark.parametrize("scale_factor", [1000, 10000])
@pytest.mark.parametrize("query_name", ["q8", "q14", "q17"])
def test_tsql_rendering_does_not_add_floating_or_safe_division(scale_factor, query_name):
    engine = _uninitialized_engine(DuckDB)
    engine.SQLGLOT_DIALECT = "tsql"
    benchmark = TPCH(
        engine=engine,
        scenario_name="tsql-arithmetic",
        scale_factor=scale_factor,
        input_parquet_folder_uri="file:///tmp/tpch",
    )
    query = benchmark._return_query_definition(query_name)
    expression = sqlglot.parse_one(query, read="tsql")
    assert expression.find(exp.Div) is not None
    assert not list(expression.find_all(exp.Nullif))
    assert not any(
        cast.args["to"].this in (exp.DataType.Type.FLOAT, exp.DataType.Type.DOUBLE)
        for cast in expression.find_all(exp.Cast)
    )


@pytest.mark.parametrize("benchmark_class", [TPCH, TPCDS])
def test_runtime_normalization_preserves_where_join_predicates(benchmark_class):
    benchmark = benchmark_class(
        engine=_uninitialized_engine(DuckDB),
        scenario_name="predicate-placement",
        scale_factor=1000,
        input_parquet_folder_uri="file:///tmp/tpc",
    )
    query = (
        "SELECT l_orderkey FROM lineitem, orders "
        "WHERE l_orderkey = o_orderkey AND l_quantity > 10;\n"
        "--#SET ROWS_FETCH -1"
    )
    source = benchmark._parse_canonical_query("q2", query)[0]
    normalized = benchmark._normalize_canonical_query("q2", query)

    assert normalized == source
    assert normalized.args["where"] is not None
    assert all(join.args.get("on") is None for join in normalized.args["joins"])
    assert not any("normalize_implicit_joins" in rule for rule in benchmark._applied_query_normalizers["q2"])


@pytest.mark.parametrize("benchmark_class", [TPCH, TPCDS])
@pytest.mark.parametrize("scale_factor", [1000, 10000])
def test_canonical_packages_resolve_only_scale_qualified_queries(benchmark_class, scale_factor):
    name = benchmark_class.__name__.lower()
    root = CANONICAL_ROOT.parents[3] / name / "resources" / "queries" / "canonical"
    assert not list(root.glob("q*.sql"))
    assert {path.name for path in root.iterdir() if path.is_dir() and path.name.startswith("sf")} == {
        "sf1000",
        "sf10000",
    }
    benchmark = benchmark_class(
        engine=_uninitialized_engine(DuckDB),
        scenario_name="canonical-package",
        scale_factor=scale_factor,
        input_parquet_folder_uri="file:///tmp/tpc",
    )
    assert benchmark._canonical_query_resource_package(name) == (
        f"lakebench.benchmarks.{name}.resources.queries.canonical.sf{scale_factor}"
    )


def test_canonical_contains_exact_generated_ansi_queries():
    for scale_factor in (1000, 10000):
        scale_directory = CANONICAL_ROOT / f"sf{scale_factor}"
        manifest = json.loads((scale_directory / "generation_manifest.json").read_text(encoding="utf-8"))
        query_paths = sorted(scale_directory.glob("q*.sql"))

        assert manifest["tpch_specification_version"] == "3.0.1"
        assert manifest["qgen_version"] == "3.0.0"
        assert manifest["scale_factor"] == scale_factor
        assert manifest["rng_seed"] == 19620718
        assert manifest["stream"] == 0
        assert len(query_paths) == manifest["query_count"] == 22
        assert {query["query"]: query["ansi_sha256"] for query in manifest["queries"]} == {
            path.stem: _sha256(path) for path in query_paths
        }


def test_registered_source_rules_apply_row_limit_and_convert_q15_to_cte():
    normalized = {}
    for name in ("q2", "q15"):
        sql = (CANONICAL_ROOT / "sf1000" / f"{name}.sql").read_text(encoding="utf-8")
        statements = parse_tpch_ansi_query(sql)
        before = [statement.copy() for statement in statements]
        normalized[name] = apply_source_normalizers(
            statements, QueryNormalizerContext(name, {}, TPCH.CANONICAL_QUERY_DIALECT, sql), SOURCE_NORMALIZERS
        )
        assert statements == before
        if name == "q15":
            assert len(statements) == 3
            assert isinstance(statements[0], exp.Create)
        else:
            assert statements[0].args.get("limit") is None

    q2, q15 = normalized["q2"], normalized["q15"]
    assert q2.args["limit"].expression.this == "100"
    assert isinstance(q15, exp.Select)
    with_clause = q15.find(exp.With)
    assert with_clause is not None
    assert with_clause.expressions[0].alias == "revenue0"
    assert [column.name for column in with_clause.expressions[0].args["alias"].columns] == [
        "supplier_no",
        "total_revenue",
    ]


def test_q15_requires_a_registered_source_lowering_rule():
    sql = (CANONICAL_ROOT / "sf1000" / "q15.sql").read_text(encoding="utf-8")
    with pytest.raises(ValueError, match="Expected one executable query"):
        apply_source_normalizers(
            parse_tpch_ansi_query(sql),
            QueryNormalizerContext("q15", {}, TPCH.CANONICAL_QUERY_DIALECT, sql),
            {"*": SOURCE_NORMALIZERS["*"]},
        )


@pytest.mark.parametrize("replacement", ["DROP VIEW other_view", "DROP TABLE revenue0"])
def test_q15_rejects_mismatched_cleanup(replacement):
    sql = (CANONICAL_ROOT / "sf1000" / "q15.sql").read_text(encoding="utf-8")
    statements = parse_tpch_ansi_query(sql)
    statements[-1] = sqlglot.parse_one(replacement)
    with pytest.raises(ValueError, match="q15"):
        apply_source_normalizers(
            statements, QueryNormalizerContext("q15", {}, TPCH.CANONICAL_QUERY_DIALECT, sql), SOURCE_NORMALIZERS
        )


def test_q15_rejects_multiple_cleanup_targets():
    sql = (CANONICAL_ROOT / "sf1000" / "q15.sql").read_text(encoding="utf-8")
    statements = parse_tpch_ansi_query(sql)
    statements[-1] = exp.Drop(kind="VIEW", tables=[exp.to_table("revenue0"), exp.to_table("other_view")])
    with pytest.raises(ValueError, match="different view names"):
        apply_source_normalizers(
            statements, QueryNormalizerContext("q15", {}, TPCH.CANONICAL_QUERY_DIALECT, sql), SOURCE_NORMALIZERS
        )


def test_q15_rejects_existing_ctes():
    sql = (CANONICAL_ROOT / "sf1000" / "q15.sql").read_text(encoding="utf-8")
    statements = parse_tpch_ansi_query(sql)
    statements[1] = statements[1].with_("other_view", as_=exp.select("1"))
    with pytest.raises(ValueError, match="without existing CTEs"):
        apply_source_normalizers(
            statements, QueryNormalizerContext("q15", {}, TPCH.CANONICAL_QUERY_DIALECT, sql), SOURCE_NORMALIZERS
        )


@pytest.mark.parametrize("directive", ["", "--#SET ROWS_FETCH 0", "--#SET ROWS_FETCH 10\n--#SET ROWS_FETCH 20"])
def test_qgen_limit_rule_rejects_invalid_directives(directive):
    sql = f"SELECT 1;\n{directive}"
    with pytest.raises(ValueError, match="ROWS_FETCH|row limit"):
        apply_source_normalizers(
            parse_tpch_ansi_query(sql),
            QueryNormalizerContext("q1", {}, TPCH.CANONICAL_QUERY_DIALECT, sql),
            SOURCE_NORMALIZERS,
        )


@pytest.mark.parametrize("benchmark_class", [TPCH, TPCDS])
@pytest.mark.parametrize("engine_class", [Daft, Sail, DuckDB])
def test_tpc_resolution_never_searches_engine_sql_files(benchmark_class, engine_class):
    benchmark = benchmark_class(
        engine=_uninitialized_engine(engine_class),
        scenario_name="no-overrides",
        scale_factor=1000,
        input_parquet_folder_uri="file:///tmp/tpc",
    )
    benchmark._engine_query_resource_packages = Mock(side_effect=AssertionError("SQL override lookup"))
    benchmark._return_query_definition("q1")
    benchmark._engine_query_resource_packages.assert_not_called()


def test_clickbench_still_loads_engine_sql_overrides(tmp_path):
    benchmark = ClickBench(
        engine=_uninitialized_engine(Daft),
        scenario_name="overrides",
        input_parquet_folder_uri="file:///tmp/clickbench",
    )
    (tmp_path / "q1.sql").write_text("SELECT 987654 AS override_marker", encoding="utf-8")
    benchmark._engine_query_resource_packages = Mock(return_value=("test.engine.queries",))
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr("importlib.resources.path", lambda package, name: tmp_path / name)
        query = benchmark._return_query_definition("q1")
    assert "987654 AS override_marker" in query
    benchmark._engine_query_resource_packages.assert_called_once()


@pytest.mark.parametrize("scale_factor", [1000, 10000])
@pytest.mark.parametrize(
    ("benchmark_class", "engine_class", "query_name", "double_count"),
    [
        (TPCH, Daft, "q1", 5),
        (TPCH, Daft, "q8", 3),
        (TPCH, Daft, "q9", 4),
        (TPCH, Daft, "q14", 5),
        (TPCDS, Sail, "q12", 0),
    ],
)
def test_engine_rules_preserve_generated_literals_and_are_idempotent(
    scale_factor, benchmark_class, engine_class, query_name, double_count
):
    class DerivedEngine(engine_class):
        pass

    benchmark = benchmark_class(
        engine=_uninitialized_engine(DerivedEngine),
        scenario_name="rules",
        scale_factor=scale_factor,
        input_parquet_folder_uri="file:///tmp/tpc",
    )
    source_path = (
        CANONICAL_ROOT.parents[3]
        / benchmark_class.__name__.lower()
        / "resources"
        / "queries"
        / "canonical"
        / f"sf{scale_factor}"
        / f"{query_name}.sql"
    )
    sql = source_path.read_text(encoding="utf-8")
    source = apply_source_normalizers(
        benchmark._parse_canonical_query(query_name, sql),
        QueryNormalizerContext(query_name, {}, benchmark.CANONICAL_QUERY_DIALECT, sql),
        benchmark.SOURCE_NORMALIZERS,
    )
    normalized = benchmark._normalize_canonical_query(query_name, sql)
    again = apply_query_normalizers(
        normalized,
        query_name,
        benchmark._query_normalization_schema(),
        benchmark.CANONICAL_QUERY_DIALECT,
        benchmark.QUERY_NORMALIZERS,
        engine_type=DerivedEngine,
        engine_normalizers=benchmark.ENGINE_QUERY_NORMALIZERS,
    )
    assert normalized == again
    expected_literals = Counter(literal.this for literal in source.find_all(exp.Literal))
    if engine_class is Sail:
        expected_literals["0"] += 1
        assert len(list(normalized.find_all(exp.Nullif))) == 1
    assert Counter(literal.this for literal in normalized.find_all(exp.Literal)) == expected_literals
    assert (
        sum(cast.args["to"].this == exp.DataType.Type.DOUBLE for cast in normalized.find_all(exp.Cast)) == double_count
    )
    assert any(rule.startswith("engine:") for rule in benchmark._applied_query_normalizers[query_name])

    benchmark.engine = _uninitialized_engine(DuckDB)
    without_engine_rules = benchmark._normalize_canonical_query(query_name, sql)
    assert not list(without_engine_rules.find_all(exp.Nullif))
    assert not any(cast.args["to"].this == exp.DataType.Type.DOUBLE for cast in without_engine_rules.find_all(exp.Cast))
    assert not any(rule.startswith("engine:") for rule in benchmark._applied_query_normalizers[query_name])


@pytest.mark.parametrize(
    ("benchmark_class", "engine_class", "query_name"),
    [(TPCH, Daft, "q1"), (TPCH, Daft, "q8"), (TPCH, Daft, "q9"), (TPCH, Daft, "q14"), (TPCDS, Sail, "q12")],
)
def test_engine_rules_reject_unexpected_query_shapes(benchmark_class, engine_class, query_name):
    with pytest.raises(ValueError, match="Expected"):
        apply_query_normalizers(
            sqlglot.parse_one("SELECT 1"),
            query_name,
            {},
            "duckdb",
            {},
            engine_type=engine_class,
            engine_normalizers=benchmark_class.ENGINE_QUERY_NORMALIZERS,
        )


def test_tpch_scale_specific_substitutions_are_selected():
    sf1000 = TPCH(
        engine=_uninitialized_engine(DuckDB),
        scenario_name="query-resolution",
        scale_factor=1000,
        query_list=["q11"],
        input_parquet_folder_uri="file:///tmp/tpch",
    )
    sf10000 = TPCH(
        engine=_uninitialized_engine(DuckDB),
        scenario_name="query-resolution",
        scale_factor=10000,
        query_list=["q11"],
        input_parquet_folder_uri="file:///tmp/tpch",
    )

    assert "0.0000001000" in sf1000._return_query_definition("q11")
    assert "0.0000000100" in sf10000._return_query_definition("q11")


def test_tpch_unsupported_scale_logs_fallback(caplog):
    with caplog.at_level(
        logging.WARNING,
        logger="lakebench.benchmarks.tpch.tpch",
    ):
        benchmark = TPCH(
            engine=_uninitialized_engine(DuckDB),
            scenario_name="query-resolution",
            scale_factor=1,
            query_list=["q1"],
            input_parquet_folder_uri="file:///tmp/tpch",
        )

    assert benchmark.query_scale_factor == 1000
    assert "Falling back to SF1000" in caplog.text


@pytest.mark.parametrize("scale_factor", [1000, 10000])
@pytest.mark.parametrize(
    ("engine_class", "dialect"),
    [(Spark, "spark"), (DuckDB, "duckdb"), (Polars, "duckdb"), (Daft, "mysql"), (Sail, "spark"), (DuckDB, "tsql")],
)
@pytest.mark.filterwarnings("ignore:path is deprecated:DeprecationWarning")
def test_all_generated_ansi_queries_resolve_for_supported_engine_dialects(
    scale_factor,
    engine_class,
    dialect,
):
    engine = _uninitialized_engine(engine_class)
    engine.SQLGLOT_DIALECT = dialect
    benchmark = TPCH(
        engine=engine,
        scenario_name="query-resolution",
        scale_factor=scale_factor,
        input_parquet_folder_uri="file:///tmp/tpch",
    )

    for query_name in TPCH.QUERY_REGISTRY:
        query = benchmark._return_query_definition(query_name)
        expression = sqlglot.parse_one(query, read=dialect)
        assert isinstance(expression, exp.Query)
        assert not any("normalize_implicit_joins" in rule for rule in benchmark._applied_query_normalizers[query_name])
