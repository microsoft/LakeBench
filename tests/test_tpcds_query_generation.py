import hashlib
import importlib.util
import json
from pathlib import Path

import pytest
import sqlglot
from sqlglot import exp

from lakebench.benchmarks import TPCDS
from lakebench.benchmarks._load_and_query._query_normalizers import (
    QueryNormalizerContext,
    apply_query_normalizers,
    apply_source_normalizers,
    implicit_join_count,
    load_query_schema,
    normalize_implicit_joins,
)
from lakebench.benchmarks.tpcds._query_normalizers import (
    QUERY_NORMALIZERS as TPCDS_QUERY_NORMALIZERS,
)
from lakebench.benchmarks.tpcds._query_normalizers import (
    parse_tpcds_ansi_query,
)
from lakebench.engines.daft import Daft
from lakebench.engines.duckdb import DuckDB
from lakebench.engines.polars import Polars
from lakebench.engines.sail import Sail
from lakebench.engines.spark import Spark

SCRIPT_PATH = (
    Path(__file__).parents[1]
    / ".github"
    / "skills"
    / "tpcds-canonical-query-generation"
    / "scripts"
    / "generate_review.py"
)
SPEC = importlib.util.spec_from_file_location("generate_tpcds_query_review", SCRIPT_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)

CANONICALIZE_SCRIPT_PATH = (
    Path(__file__).parents[1]
    / ".github"
    / "skills"
    / "tpcds-canonical-query-generation"
    / "scripts"
    / "canonicalize_review.py"
)
CANONICALIZE_SPEC = importlib.util.spec_from_file_location(
    "canonicalize_tpcds_query_review",
    CANONICALIZE_SCRIPT_PATH,
)
CANONICALIZE_MODULE = importlib.util.module_from_spec(CANONICALIZE_SPEC)
CANONICALIZE_SPEC.loader.exec_module(CANONICALIZE_MODULE)

CANONICAL_ROOT = (
    Path(__file__).parents[1] / "src" / "lakebench" / "benchmarks" / "tpcds" / "resources" / "queries" / "canonical"
)
TPCDS_SCHEMA = load_query_schema(
    (
        Path(__file__).parents[1]
        / "src"
        / "lakebench"
        / "benchmarks"
        / "tpcds"
        / "resources"
        / "ddl"
        / "canonical"
        / "ddl_v4.0.0.simple.sql"
    ).read_text(encoding="utf-8")
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


def _normalize_tpcds_query(query_name, query):
    return apply_query_normalizers(
        expression=apply_source_normalizers(
            parse_tpcds_ansi_query(query),
            QueryNormalizerContext(query_name, TPCDS_SCHEMA, "tsql", query),
            TPCDS.SOURCE_NORMALIZERS,
        )
        if isinstance(query, str)
        else query,
        query_name=query_name,
        schema=TPCDS_SCHEMA,
        dialect="tsql",
        query_normalizers=TPCDS_QUERY_NORMALIZERS,
    )


def test_tpcds_does_not_silently_drop_extra_source_statements():
    with pytest.raises(ValueError, match="Expected one executable query"):
        _normalize_tpcds_query("q1", "SELECT 1; SELECT 2")


def test_split_sql_statements_ignores_semicolons_in_strings_and_comments():
    sql = """
SELECT ';' AS value; -- ignored ;
SELECT 2 /* ignored ; */;
"""

    assert MODULE.split_sql_statements(sql) == [
        "SELECT ';' AS value;",
        "-- ignored ;\nSELECT 2 /* ignored ; */;",
    ]


def test_normalize_for_transpilation_handles_tpcds_ansi_forms():
    sql = """
SELECT TOP 100 DISTINCT(i_product_name)
FROM item
WHERE d_date BETWEEN CAST('2000-01-01' AS DATE)
  AND CAST('2000-01-01' AS DATE) + 30 days;
"""

    normalized = MODULE.normalize_for_transpilation(sql)

    assert "select distinct top 100 i_product_name" in normalized
    assert "+ INTERVAL '30' DAY" in normalized


def test_patch_ansi_dialect_adds_missing_markers_once(tmp_path):
    dialect_path = tmp_path / "ansi.tpl"
    dialect_path.write_text('define __LIMITA = "";\n', encoding="utf-8")

    MODULE.patch_ansi_dialect(dialect_path)
    MODULE.patch_ansi_dialect(dialect_path)

    content = dialect_path.read_text(encoding="utf-8")
    assert content.count("define _BEGIN") == 1
    assert content.count("define _END") == 1


def test_convert_query_replaces_comma_joins_without_changing_semantics():
    schema = {
        "catalog_sales": {
            "cs_bill_customer_sk": "INT",
            "cs_bill_cdemo_sk": "INT",
        },
        "customer": {
            "c_customer_sk": "INT",
            "c_current_cdemo_sk": "INT",
        },
        "customer_demographics": {"cd_demo_sk": "INT"},
    }
    source_sql = """
SELECT c_customer_sk
FROM catalog_sales, customer, customer_demographics AS cd
WHERE cs_bill_customer_sk = c_customer_sk
  AND c_current_cdemo_sk = cd.cd_demo_sk
  AND cs_bill_cdemo_sk > 0
"""

    candidate = apply_query_normalizers(
        expression=sqlglot.parse_one(source_sql, read="spark"),
        query_name="synthetic",
        schema=schema,
        dialect="spark",
        query_normalizers={"*": (normalize_implicit_joins,)},
    )
    candidate_sql = candidate.sql(dialect="spark", pretty=True, normalize=False)

    assert "FROM catalog_sales" in candidate_sql
    assert "JOIN customer" in candidate_sql
    assert "ON cs_bill_customer_sk = c_customer_sk" in candidate_sql
    assert "JOIN customer_demographics AS cd" in candidate_sql
    assert "ON c_current_cdemo_sk = cd.cd_demo_sk" in candidate_sql
    assert "WHERE\n  cs_bill_cdemo_sk > 0" in candidate_sql


def test_convert_query_preserves_an_intentional_cartesian_relation():
    schema = {
        "catalog_sales": {"cs_call_center_sk": "INT"},
        "catalog_returns": {"cr_call_center_sk": "INT"},
    }
    source_sql = """
SELECT cs_call_center_sk, cr_call_center_sk
FROM catalog_sales, catalog_returns
"""

    candidate = apply_query_normalizers(
        expression=sqlglot.parse_one(source_sql, read="spark"),
        query_name="synthetic",
        schema=schema,
        dialect="spark",
        query_normalizers={"*": (normalize_implicit_joins,)},
    )
    candidate_sql = candidate.sql(dialect="spark", pretty=True, normalize=False)

    assert "CROSS JOIN catalog_returns" in candidate_sql


def test_canonical_runtime_preserves_known_query_hazards():
    for scale_factor in (1000, 10000):
        benchmark = TPCDS(
            engine=_uninitialized_engine(Spark),
            scenario_name="canonical-hazards",
            scale_factor=scale_factor,
            input_parquet_folder_uri="file:///tmp/tpcds",
        )
        q18, q41, q77 = (benchmark._return_query_definition(name) for name in ("q18", "q41", "q77"))
        assert "c_current_cdemo_sk = cd2.cd_demo_sk" in q18
        assert "cs_bill_cdemo_sk = cd2.cd_demo_sk" not in q18
        assert sqlglot.parse_one(q41, read="spark").args["limit"].expression.this == "100"
        assert any(
            select.find(exp.From).this.name == "cs"
            and any(
                join.kind == "CROSS" and join.this.name == "cr" and join.args.get("on") is None
                for join in select.args.get("joins") or []
            )
            for select in sqlglot.parse_one(q77, read="spark").find_all(exp.Select)
            if select.find(exp.From) is not None
        )
        assert "cs_call_center_sk = cr_call_center_sk" not in q77


def test_canonical_contains_exact_generated_ansi_queries():
    for scale_factor in (1000, 10000):
        scale_directory = CANONICAL_ROOT / f"sf{scale_factor}"
        manifest = json.loads((scale_directory / "generation_manifest.json").read_text(encoding="utf-8"))
        query_paths = sorted(scale_directory.glob("q*.sql"))

        assert manifest["tpcds_tools_version"] == "4.0.0"
        assert manifest["scale_factor"] == scale_factor
        assert manifest["rng_seed"] == 19620718
        assert len(query_paths) == manifest["query_count"] == 103
        assert {query["query"]: query["ansi_sha256"] for query in manifest["queries"]} == {
            path.stem: _sha256(path) for path in query_paths
        }


@pytest.mark.parametrize("query_name", ["q36", "q58", "q70", "q72", "q86", "q90", "q97"])
def test_tpcds_ansi_normalizers_are_idempotent(query_name):
    query = (CANONICAL_ROOT / "sf1000" / f"{query_name}.sql").read_text(encoding="utf-8")

    normalized = _normalize_tpcds_query(query_name, query)
    normalized_again = _normalize_tpcds_query(query_name, normalized)

    assert normalized == normalized_again


@pytest.mark.parametrize("scale_factor", [1000, 10000])
@pytest.mark.parametrize("query_name", ["q36", "q70", "q86"])
def test_rollup_order_rule_only_expands_compound_order_aliases(scale_factor, query_name):
    query = (CANONICAL_ROOT / f"sf{scale_factor}" / f"{query_name}.sql").read_text(encoding="utf-8")
    source = parse_tpcds_ansi_query(query)[0]
    normalized = _normalize_tpcds_query(query_name, query)
    definition = next(projection.this for projection in source.expressions if projection.alias == "lochierarchy")
    expected_order = source.args["order"].copy()
    compound_alias = next(
        column for column in expected_order.expressions[1].find_all(exp.Column) if column.name == "lochierarchy"
    )
    compound_alias.replace(exp.Paren(this=definition.copy()))

    assert normalized.args["order"] == expected_order
    assert normalized.args["order"].expressions[0] == source.args["order"].expressions[0]
    unchanged = normalized.copy()
    unchanged.set("order", source.args["order"].copy())
    assert unchanged == source
    assert normalized == _normalize_tpcds_query(query_name, normalized)

    tsql = sqlglot.parse_one(normalized.sql(dialect="tsql"), read="tsql")
    assert not any(column.name == "lochierarchy" for column in tsql.args["order"].expressions[1].find_all(exp.Column))


@pytest.mark.parametrize(
    "query",
    [
        "SELECT 1 ORDER BY 1",
        "SELECT 1 AS lochierarchy ORDER BY CASE WHEN lochierarchy = 0 THEN 1 END",
        "SELECT GROUPING(i_category) + GROUPING(i_class) AS lochierarchy FROM item",
    ],
)
def test_rollup_order_rule_rejects_unexpected_shapes(query):
    with pytest.raises(ValueError, match="Expected"):
        _normalize_tpcds_query("q36", query)


def test_rollup_order_rule_preserves_qualified_and_nested_references():
    source = """
SELECT GROUPING(i_category) + GROUPING(i_class) AS lochierarchy
FROM item
GROUP BY ROLLUP(i_category, i_class)
ORDER BY lochierarchy,
         CASE WHEN other.lochierarchy = 0 THEN 1 END,
         (SELECT MAX(lochierarchy) FROM other)
"""
    normalized = _normalize_tpcds_query("q36", source)
    assert normalized == parse_tpcds_ansi_query(source)[0]


def test_tpcds_ansi_normalizers_apply_known_compatibility_rules():
    normalized = {
        query_name: _normalize_tpcds_query(
            query_name,
            (CANONICAL_ROOT / "sf1000" / f"{query_name}.sql").read_text(encoding="utf-8"),
        )
        for query_name in ("q58", "q72", "q90", "q97")
    }

    normalized_sql = {
        query_name: expression.sql(dialect="spark", pretty=True, normalize=False)
        for query_name, expression in normalized.items()
    }
    assert "ss_items.item_id" in normalized_sql["q58"].split("ORDER BY", 1)[1]
    assert "d1.d_week_seq" in normalized_sql["q72"].split("ORDER BY", 1)[1]
    assert "AS am_counts" in normalized_sql["q90"]
    assert "AS pm_counts" in normalized_sql["q90"]
    q97 = normalized["q97"]
    assert len([cast for cast in q97.find_all(exp.Cast) if cast.args["to"].this == exp.DataType.Type.BIGINT]) == 6


def test_tpcds_ansi_normalizer_fails_when_expected_shape_changes():
    with pytest.raises(ValueError, match="unqualified ORDER BY column 'item_id'"):
        _normalize_tpcds_query("q58", "SELECT item_id FROM item ORDER BY other_id")


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
    benchmark = TPCDS(
        engine=engine,
        scenario_name="query-resolution",
        scale_factor=scale_factor,
        input_parquet_folder_uri="file:///tmp/tpcds",
    )

    for query_name in TPCDS.QUERY_REGISTRY:
        query = benchmark._return_query_definition(query_name)
        expression = sqlglot.parse_one(query, read=dialect)
        assert isinstance(expression, exp.Query)
        assert not any("normalize_implicit_joins" in rule for rule in benchmark._applied_query_normalizers[query_name])
