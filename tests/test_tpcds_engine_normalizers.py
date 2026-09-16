import logging

import pytest
import sqlglot
from sqlglot import exp

from lakebench.benchmarks import TPCDS
from lakebench.engines.duckdb import DuckDB
from lakebench.engines.sail import Sail
from tests.conftest import _uninitialized_engine


class _TestSail(Sail):
    def __init__(self):
        self.version = "test"
        self.cost_per_vcore_hour = None
        self.cost_per_hour = None
        self.extended_engine_metadata = {}
        self.storage_options = {}
        self.schema_or_working_directory_uri = "/tmp/lakebench"
        self.runtime = "local_unknown"
        self.operating_system = "linux"


def _categories(query):
    expression = sqlglot.parse_one(query, read="spark")
    return [
        value.this
        for predicate in expression.find_all(exp.In)
        if predicate.this.name == "i_category"
        for value in predicate.expressions
    ]


def test_sail_q12_uses_registered_safe_denominator(caplog):
    with caplog.at_level(logging.WARNING, logger="lakebench.benchmarks.tpcds.tpcds"):
        benchmark = TPCDS(
            engine=_TestSail(),
            scenario_name="test",
            scale_factor=1,
            query_list=["q12"],
            input_parquet_folder_uri="/tmp/tpcds",
        )

    assert "Falling back to SF1000" in caplog.text
    query = benchmark._return_query_definition("q12")

    assert "NULLIF" in query
    assert "SUM(SUM(ws_ext_sales_price))" in query
    assert _categories(query) == ["Music", "Women", "Jewelry"]
    assert any("engine:" in rule for rule in benchmark._applied_query_normalizers["q12"])
    assert benchmark.query_scale_factor == 1000
    assert benchmark.engine.extended_engine_metadata["query_set_scale_matches_data"] == "false"


@pytest.mark.parametrize(
    ("scale_factor", "categories"),
    [
        (1000, ["Music", "Women", "Jewelry"]),
        (10000, ["Electronics", "Books", "Women"]),
    ],
)
def test_sail_q12_uses_matching_scale_query_set(scale_factor, categories):
    benchmark = TPCDS(
        engine=_TestSail(),
        scenario_name="test",
        scale_factor=scale_factor,
        query_list=["q12"],
        input_parquet_folder_uri="/tmp/tpcds",
    )

    query = benchmark._return_query_definition("q12")

    assert _categories(query) == categories
    assert "NULLIF" in query
    assert benchmark.query_scale_factor == scale_factor
    assert benchmark.engine.extended_engine_metadata["query_set_scale_matches_data"] == "true"


@pytest.mark.parametrize("scale_factor", [1000, 10000])
def test_sail_q90_guards_the_am_pm_ratio_denominator(scale_factor):
    """Sail raises on division by zero; every other engine yields NULL.

    ``pmc`` counts web sales in a two-hour window, so it is legitimately zero at
    small scale factors rather than a sign of bad data.
    """
    benchmark = TPCDS(
        engine=_TestSail(),
        scenario_name="test",
        scale_factor=scale_factor,
        query_list=["q90"],
        input_parquet_folder_uri="/tmp/tpcds",
    )

    query = benchmark._return_query_definition("q90")

    assert "NULLIF(CAST(pmc AS DECIMAL(15, 4)), 0)" in query
    # Only the denominator is guarded; the numerator must stay untouched.
    assert query.count("NULLIF") == 1
    assert "CAST(amc AS DECIMAL(15, 4)) / NULLIF(" in query
    assert any("engine:" in rule for rule in benchmark._applied_query_normalizers["q90"])


def test_q90_denominator_is_unguarded_for_other_engines():
    benchmark = TPCDS(
        engine=_uninitialized_engine(DuckDB),
        scenario_name="test",
        scale_factor=1000,
        query_list=["q90"],
        input_parquet_folder_uri="/tmp/tpcds",
    )

    query = benchmark._return_query_definition("q90")

    assert "NULLIF" not in query
    assert not any("engine:" in rule for rule in benchmark._applied_query_normalizers["q90"])


@pytest.mark.parametrize(
    ("scale_factor", "county"),
    [
        (1000, "Texas County"),
        (10000, "Fillmore County"),
    ],
)
def test_canonical_query_uses_matching_scale_query_set(scale_factor, county):
    benchmark = TPCDS(
        engine=_TestSail(),
        scenario_name="test",
        scale_factor=scale_factor,
        query_list=["q10"],
        input_parquet_folder_uri="/tmp/tpcds",
    )

    query = benchmark._return_query_definition("q10")

    assert county in query


def test_unsupported_scale_canonical_query_falls_back_to_sf1000(caplog):
    with caplog.at_level(logging.WARNING, logger="lakebench.benchmarks.tpcds.tpcds"):
        benchmark = TPCDS(
            engine=_TestSail(),
            scenario_name="test",
            scale_factor=3000,
            query_list=["q10"],
            input_parquet_folder_uri="/tmp/tpcds",
        )

    assert "Falling back to SF1000" in caplog.text
    query = benchmark._return_query_definition("q10")

    assert "Texas County" in query
    assert "Fillmore County" not in query
    assert benchmark.engine.extended_engine_metadata["query_set_scale_factor"] == "1000"
    assert benchmark.engine.extended_engine_metadata["query_set_scale_matches_data"] == "false"
