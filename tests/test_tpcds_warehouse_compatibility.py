from pathlib import Path

import pytest
import sqlglot
from sqlglot import exp
from sqlglot.errors import OptimizeError
from sqlglot.optimizer.qualify_columns import qualify_columns, validate_qualify_columns
from sqlglot.schema import MappingSchema

from lakebench.benchmarks import TPCDS
from lakebench.benchmarks._load_and_query._query_normalizers import apply_query_normalizers
from lakebench.benchmarks.tpcds._query_normalizers import ENGINE_QUERY_NORMALIZERS
from lakebench.engines.duckdb import DuckDB
from lakebench.engines.fabric_data_warehouse import FabricDataWarehouse
from tests.test_tpch_query_generation import _uninitialized_engine

ROOT = Path(__file__).parents[1] / "src" / "lakebench" / "benchmarks" / "tpcds" / "resources" / "queries" / "canonical"
DIALECTS = ["tsql", "spark", "duckdb", "mysql", "fabric"]


def _benchmark(dialect="tsql", scale=1000, engine_class=DuckDB):
    """Builds a benchmark whose engine renders ``dialect``.

    ``engine_class`` selects which engine-registered normalizers apply. Rendering the
    warehouse dialect through ``DuckDB`` therefore isolates the dialect-neutral rules
    from the accommodations registered to :class:`FabricDataWarehouse`.
    """
    engine = _uninitialized_engine(engine_class)
    engine.SQLGLOT_DIALECT = dialect
    engine.schema_name = "dbo"
    return TPCDS(
        engine=engine,
        scenario_name="warehouse-compatibility",
        scale_factor=scale,
        input_parquet_folder_uri="file:///tmp/tpcds",
    )


def _warehouse(scale=1000):
    return _benchmark(dialect="fabric", scale=scale, engine_class=FabricDataWarehouse)


def _apply_warehouse_normalizers(expression, query_name, schema, dialect="fabric"):
    return apply_query_normalizers(
        expression,
        query_name,
        schema,
        "tsql",
        {},
        target_dialect=dialect,
        engine_type=FabricDataWarehouse,
        engine_normalizers=ENGINE_QUERY_NORMALIZERS,
    )


@pytest.mark.parametrize("scale", [1000, 10000])
@pytest.mark.parametrize("name", ["q17", "q29", "q35", "q39a", "q39b"])
def test_sample_stddev_rewrites_the_function_name_only_for_the_warehouse_engine(scale, name):
    """Both benchmarks render ``fabric``; only the engine registration differs."""
    before = _benchmark("fabric", scale)._return_query_definition(name)
    after = _warehouse(scale)._return_query_definition(name)
    assert after == before.replace("STDDEV_SAMP(", "STDEV(")
    assert "STDDEV_SAMP(" not in after
    # STDEVP is the population form, which would change the computed value.
    assert "STDEVP(" not in after
    if scale == 1000 and name in {"q29", "q35"}:
        assert after == before


@pytest.mark.parametrize("scale", [1000, 10000])
@pytest.mark.parametrize("dialect", DIALECTS)
@pytest.mark.parametrize("name", ["q17", "q29", "q35", "q39a", "q39b"])
def test_sample_stddev_is_left_alone_for_every_other_engine(scale, dialect, name):
    benchmark = _benchmark(dialect, scale)
    source = (ROOT / f"sf{scale}" / f"{name}.sql").read_text()
    normalized = benchmark._normalize_canonical_query(name, source)
    assert (
        apply_query_normalizers(
            normalized,
            name,
            benchmark._query_normalization_schema(),
            benchmark.CANONICAL_QUERY_DIALECT,
            benchmark.QUERY_NORMALIZERS,
            target_dialect=dialect,
        )
        == normalized
    )


def test_sample_stddev_preserves_sample_not_population_semantics():
    duckdb = pytest.importorskip("duckdb")
    source = sqlglot.parse_one("SELECT STDDEV_SAMP(x) FROM (VALUES (1.0), (2.0), (3.0), (NULL)) t(x)", read="tsql")
    normalized = _apply_warehouse_normalizers(source, "q17", {}, dialect="tsql")
    assert source.find(exp.StddevSamp) is not None
    assert normalized.find(exp.StddevSamp) is None
    rendered = normalized.sql(dialect="tsql")
    assert "STDEV(x)" in rendered
    with duckdb.connect() as connection:
        assert connection.execute(sqlglot.parse_one(rendered, read="tsql").sql(dialect="duckdb")).fetchone() == (1.0,)


def test_target_dialect_does_not_change_source_reader_or_leak_between_runs():
    benchmark = _warehouse()
    first = benchmark._return_query_definition("q39a")
    benchmark.engine.SQLGLOT_DIALECT = "spark"
    second = benchmark._return_query_definition("q39a")
    benchmark.engine.SQLGLOT_DIALECT = "fabric"
    assert benchmark.CANONICAL_QUERY_DIALECT == "tsql"
    assert "STDEV(" in first
    # The rule is registered to the engine, so it swaps the node regardless of target.
    # Only the rendering differs: Spark spells the same node STDDEV, never STDEV.
    assert "STDDEV(" in second
    assert "STDEV(" not in second.replace("STDDEV(", "")
    assert benchmark._return_query_definition("q39a") == first


def test_sample_stddev_swap_is_scoped_to_the_engine_not_the_dialect():
    """A non-warehouse engine rendering the warehouse dialect keeps the generated node."""
    rendered = _benchmark("fabric")._return_query_definition("q39a")
    assert "STDDEV_SAMP(" in rendered
    assert "STDEV(" not in rendered


@pytest.mark.parametrize("scale", [1000, 10000])
def test_q22_widens_the_average_input_only_for_the_warehouse_engine(scale):
    before = _benchmark("fabric", scale)._return_query_definition("q22")
    after = _warehouse(scale)._return_query_definition("q22")
    assert after == before.replace("AVG(inv_quantity_on_hand)", "AVG(CAST(inv_quantity_on_hand AS BIGINT))")
    # Casting after the aggregate cannot prevent the overflow it is meant to avoid.
    assert "CAST(AVG(" not in after


@pytest.mark.parametrize("scale", [1000, 10000])
@pytest.mark.parametrize("dialect", DIALECTS)
def test_q22_is_left_alone_for_every_other_engine(scale, dialect):
    benchmark = _benchmark(dialect, scale)
    source = (ROOT / f"sf{scale}" / "q22.sql").read_text()
    normalized = benchmark._normalize_canonical_query("q22", source)
    assert (
        apply_query_normalizers(
            normalized,
            "q22",
            benchmark._query_normalization_schema(),
            benchmark.CANONICAL_QUERY_DIALECT,
            benchmark.QUERY_NORMALIZERS,
            target_dialect=dialect,
        )
        == normalized
    )


@pytest.mark.parametrize(
    "projection",
    [
        "SUM(inv_quantity_on_hand) AS qoh",
        "AVG(other_column) AS qoh",
        "AVG(CAST(inv_quantity_on_hand AS DOUBLE)) AS qoh",
        "AVG(inv_quantity_on_hand) AS other_alias",
    ],
)
def test_q22_rejects_unexpected_average_shape(projection):
    benchmark = _warehouse()
    with pytest.raises(ValueError, match="Expected q22"):
        _apply_warehouse_normalizers(
            sqlglot.parse_one(f"SELECT {projection} FROM inventory", read="tsql"),
            "q22",
            benchmark._query_normalization_schema(),
        )


@pytest.mark.parametrize("scale", [1000, 10000])
@pytest.mark.parametrize("dialect", DIALECTS)
def test_q1_only_changes_fee_identifier_case(scale, dialect):
    benchmark = _benchmark(dialect, scale)
    benchmark.QUERY_NORMALIZERS = {}
    before = benchmark._return_query_definition("q1")
    benchmark.QUERY_NORMALIZERS = TPCDS.QUERY_NORMALIZERS
    after = benchmark._return_query_definition("q1")
    assert "SUM(SR_FEE)" in before
    assert after == before.replace("SUM(SR_FEE)", "SUM(sr_fee)")


def test_q1_case_rule_preserves_aliases_and_literals():
    source = sqlglot.parse_one("SELECT SUM(SR_FEE) AS FeeTotal, 'SR_FEE' AS Label FROM store_returns", read="tsql")
    normalized = apply_query_normalizers(
        source, "q1", {"store_returns": {"sr_fee": "DECIMAL(7, 2)"}}, "tsql", TPCDS.QUERY_NORMALIZERS
    )
    assert normalized.expressions[0].alias == "FeeTotal"
    assert normalized.expressions[1] == source.expressions[1]
    assert source.find(exp.Column).name == "SR_FEE"
    assert normalized.find(exp.Column).name == "sr_fee"


@pytest.mark.parametrize("fee", ["[SR_FEE]", "other.SR_FEE", "sr_return_amt"])
def test_q1_case_rule_rejects_unexpected_reference(fee):
    with pytest.raises(ValueError, match="store_returns.sr_fee"):
        apply_query_normalizers(
            sqlglot.parse_one(f"SELECT SUM({fee}) FROM store_returns", read="tsql"),
            "q1",
            {"store_returns": {"sr_fee": "DECIMAL(7, 2)"}},
            "tsql",
            TPCDS.QUERY_NORMALIZERS,
        )


@pytest.mark.parametrize("scale", [1000, 10000])
@pytest.mark.parametrize("dialect", DIALECTS)
def test_q72_only_lowers_the_five_day_offset(scale, dialect):
    benchmark = _benchmark(dialect, scale)
    source = (ROOT / f"sf{scale}" / "q72.sql").read_text()
    benchmark.QUERY_NORMALIZERS = {"q72": TPCDS.QUERY_NORMALIZERS["q72"][:1]}
    before = benchmark._normalize_canonical_query("q72", source)
    benchmark.QUERY_NORMALIZERS = TPCDS.QUERY_NORMALIZERS
    after = benchmark._normalize_canonical_query("q72", source)
    offset = after.find(exp.DateAdd)
    assert offset.this == exp.column("d_date", table="d1")
    assert offset.expression == exp.Literal.number(5)
    assert offset.args["unit"].name == "DAY"
    reverted = after.copy()
    reverted.find(exp.DateAdd).replace(exp.Add(this=offset.this.copy(), expression=offset.expression.copy()))
    assert reverted == before
    if dialect in {"tsql", "fabric"}:
        assert "d3.d_date > DATEADD(DAY, 5, d1.d_date)" in benchmark._return_query_definition("q72")


@pytest.mark.parametrize("replacement", ["d1.d_date + 6", "d1.d_date - 5", "d1.d_date_sk + 5"])
def test_q72_rejects_unexpected_date_offset(replacement):
    benchmark = _benchmark()
    source = (ROOT / "sf1000" / "q72.sql").read_text().replace("d1.d_date + 5", replacement)
    with pytest.raises(ValueError, match="q72"):
        benchmark._normalize_canonical_query("q72", source)


@pytest.mark.parametrize("scale", [1000, 10000])
@pytest.mark.parametrize("dialect", ["tsql", "fabric"])
def test_all_tpcds_tsql_column_bindings_preserve_identifier_case(scale, dialect):
    benchmark = _benchmark(dialect=dialect, scale=scale)
    schema = MappingSchema({"dbo": benchmark._query_normalization_schema()}, normalize=False)
    for name in benchmark.QUERY_REGISTRY:
        expression = sqlglot.parse_one(benchmark._return_query_definition(name), read=dialect)
        qualify_columns(expression, schema, expand_alias_refs=False, infer_schema=False)
        validate_qualify_columns(expression)


def test_case_sensitive_binding_rejects_the_original_sr_fee_spelling():
    schema = MappingSchema({"store_returns": {"sr_fee": "DECIMAL(7,2)"}}, normalize=False)
    expression = sqlglot.parse_one("SELECT SUM(SR_FEE) FROM store_returns AS store_returns", read="tsql")
    with pytest.raises(OptimizeError, match="SR_FEE"):
        qualify_columns(expression, schema, expand_alias_refs=False, infer_schema=False)
        validate_qualify_columns(expression)


@pytest.mark.parametrize("scale", [1000, 10000])
@pytest.mark.parametrize("dialect", DIALECTS)
def test_q9_bucket_counts_widen_only_for_tsql(scale, dialect):
    """T-SQL COUNT(*) returns INT and overflows on q9's fifth-of-a-fact-table buckets."""
    benchmark = _benchmark(dialect, scale)
    benchmark.QUERY_NORMALIZERS = {}
    before = benchmark._return_query_definition("q9")
    benchmark.QUERY_NORMALIZERS = TPCDS.QUERY_NORMALIZERS
    after = benchmark._return_query_definition("q9")

    assert before.count("COUNT(*)") == 5
    assert after == (before.replace("COUNT(*)", "COUNT_BIG(*)") if dialect in {"tsql", "fabric"} else before)
    if dialect in {"tsql", "fabric"}:
        assert after.count("COUNT_BIG(*)") == 5
        assert "COUNT(*)" not in after
        # Widening the counter, not casting its overflowed result.
        assert "CAST(COUNT" not in after.upper()


@pytest.mark.parametrize("scale", [1000, 10000])
def test_q9_widening_preserves_every_threshold_and_average(scale):
    benchmark = _benchmark("tsql", scale)
    source = (ROOT / f"sf{scale}" / "q9.sql").read_text()
    normalized = benchmark._normalize_canonical_query("q9", source)
    original = benchmark._parse_canonical_query("q9", source)[0]

    def summarize(expression):
        return (
            [literal.sql() for literal in expression.find_all(exp.Literal)],
            [average.sql() for average in expression.find_all(exp.Avg)],
            [alias.alias for alias in expression.expressions],
        )

    assert summarize(normalized) == summarize(original)
    assert all(count.args.get("big_int") for count in normalized.find_all(exp.Count))


@pytest.mark.parametrize("scale", [1000, 10000])
def test_q9_widening_is_idempotent(scale):
    benchmark = _benchmark("tsql", scale)
    source = (ROOT / f"sf{scale}" / "q9.sql").read_text()
    normalized = benchmark._normalize_canonical_query("q9", source)
    again = apply_query_normalizers(
        normalized,
        "q9",
        benchmark._query_normalization_schema(),
        benchmark.CANONICAL_QUERY_DIALECT,
        benchmark.QUERY_NORMALIZERS,
        target_dialect="tsql",
    )
    assert again == normalized


@pytest.mark.parametrize(
    "mutation",
    [
        ("(select count(*)", "(select count(ss_quantity)"),
        (") > 2972190", ") + 0 > 2972190"),
    ],
)
def test_q9_rejects_unexpected_bucket_shapes(mutation):
    benchmark = _benchmark()
    source = (ROOT / "sf1000" / "q9.sql").read_text().replace(*mutation, 1)
    with pytest.raises(ValueError, match="q9"):
        benchmark._normalize_canonical_query("q9", source)


def test_other_ungrouped_fact_counts_are_left_alone():
    """q88, q90, and q96 bound their counts with selective dimension joins."""
    benchmark = _benchmark("tsql")
    for name in ("q88", "q90", "q96"):
        assert "COUNT_BIG(" not in benchmark._return_query_definition(name), name
