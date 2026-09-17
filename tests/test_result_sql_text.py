"""Covers the `sql_text` result column end to end: timer capture, result-row
assembly, and persistence through the delta-rs write path."""

import datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from lakebench.benchmarks._load_and_query._load_and_query import _LoadAndQuery
from lakebench.benchmarks.base import BaseBenchmark
from lakebench.utils.timer import timer

SCHEMA_NAMES = [name for name, _ in BaseBenchmark.RESULT_SCHEMA]


def _drain_timer():
    original = getattr(timer, "results", [])
    timer.results = []
    return original


def _result_row(test_item="q1", sql_text=None):
    """A minimal valid result row matching RESULT_SCHEMA."""
    row = {name: None for name, _ in BaseBenchmark.RESULT_SCHEMA}
    row.update(
        {
            "run_id": "r1",
            "run_datetime": datetime.datetime(2026, 1, 1),
            "start_datetime": datetime.datetime(2026, 1, 1),
            "phase": "Query",
            "test_item": test_item,
            "success": True,
            "sql_text": sql_text,
            "engine_properties": {},
            "execution_telemetry": {},
        }
    )
    return row


def test_sql_text_is_in_the_result_schema_as_a_nullable_string():
    assert ("sql_text", "STRING") in BaseBenchmark.RESULT_SCHEMA


def test_map_columns_stay_last_so_the_arrow_writer_can_split_them():
    """`_append_results_to_delta` removes the MAP columns and re-appends them at
    the end, so any new scalar column must sit before them."""
    assert SCHEMA_NAMES[-2:] == ["engine_properties", "execution_telemetry"]


def test_timer_records_none_when_no_sql_text_is_set():
    original = _drain_timer()
    try:
        with timer(phase="Load", test_item="customer"):
            pass
        assert timer.results[0][9] is None
    finally:
        timer.results = original


def test_timer_records_the_sql_text_that_was_set():
    original = _drain_timer()
    try:
        with timer(phase="Query", test_item="q1") as tc:
            tc.sql_text = "SELECT 1"
        assert timer.results[0][9] == "SELECT 1"
    finally:
        timer.results = original


def test_timer_records_sql_text_even_when_the_body_raises():
    original = _drain_timer()
    try:
        with timer(phase="Query", test_item="q1") as tc:
            tc.sql_text = "SELECT bad"
            raise RuntimeError("boom")
        assert timer.results[0][6] is False
        assert timer.results[0][9] == "SELECT bad"
    finally:
        timer.results = original


@pytest.mark.parametrize("value", [123, {"a": "b"}, ["SELECT 1"], object()])
def test_timer_coerces_non_string_sql_text_to_none(value):
    """The column is STRING; a non-string would fail the Arrow conversion."""
    original = _drain_timer()
    try:
        with timer(phase="Query", test_item="q1") as tc:
            tc.sql_text = value
        assert timer.results[0][9] is None
    finally:
        timer.results = original


def test_timer_tuple_arity_matches_the_post_results_unpacking():
    original = _drain_timer()
    try:
        with timer(phase="Query", test_item="q1") as tc:
            tc.sql_text = "SELECT 1"
        # post_results() unpacks 10 names from each tuple.
        assert len(timer.results[0]) == 10
    finally:
        timer.results = original


def _benchmark_with_one_query(query="SELECT 1", fails=False):
    benchmark = object.__new__(_LoadAndQuery)
    benchmark.engine = MagicMock()
    benchmark.engine.extended_engine_metadata = {}
    benchmark.engine.get_job_cost = lambda duration_ms: None
    benchmark.benchmark_impl = None
    benchmark.query_list = ["q1"]
    benchmark.query_progress = None
    benchmark._return_query_definition = lambda name: query
    benchmark._applied_query_normalizers = {}
    benchmark.engine.execute_sql_query.return_value = {"rows": "1"}
    if fails:
        benchmark.engine.execute_sql_query.side_effect = RuntimeError("query failed")
    benchmark.timer = timer
    benchmark.post_results = MagicMock()
    return benchmark


@pytest.mark.parametrize("fails", [False, True])
def test_query_phase_records_the_exact_transpiled_sql(fails):
    query = "SELECT SUM(l_quantity) FROM lineitem"
    benchmark = _benchmark_with_one_query(query, fails=fails)
    original = _drain_timer()
    try:
        benchmark._run_query_test()
        assert timer.results[0][9] == query
    finally:
        timer.results = original


def test_post_results_maps_sql_text_onto_the_result_row():
    benchmark = _benchmark_with_one_query("SELECT 1")
    benchmark.post_results = lambda: BaseBenchmark.post_results(benchmark)
    benchmark.header_detail_dict = {}
    benchmark.mode = "power_test"
    benchmark.results = []
    benchmark.save_results = False
    original = _drain_timer()
    try:
        benchmark._run_query_test()
        row = benchmark.results[0]
        assert row["sql_text"] == "SELECT 1"
        assert set(row) >= {"sql_text", "phase", "test_item"}
    finally:
        timer.results = original


def test_non_query_phases_leave_sql_text_null():
    benchmark = object.__new__(_LoadAndQuery)
    benchmark.engine = MagicMock()
    benchmark.engine.extended_engine_metadata = {}
    benchmark.engine.get_job_cost = lambda duration_ms: None
    benchmark.header_detail_dict = {}
    benchmark.mode = "load"
    benchmark.results = []
    benchmark.save_results = False
    benchmark.timer = timer
    original = _drain_timer()
    try:
        with timer(phase="Load", sub_phase="optimize", test_item="lineitem"):
            pass
        BaseBenchmark.post_results(benchmark)
        assert benchmark.results[0]["sql_text"] is None
    finally:
        timer.results = original


def test_sql_text_round_trips_through_the_delta_writer(tmp_path):
    """Writes with the real delta-rs path, then reads the column back."""
    deltalake = pytest.importorskip("deltalake")
    from lakebench.engines.base import BaseEngine

    engine = BaseEngine.__new__(BaseEngine)
    engine.storage_options = {}

    query = "SELECT\n  SUM(l_quantity)\nFROM lineitem -- multi-line, with 'quotes'"
    table_uri = str(tmp_path / "results")
    engine._append_results_to_delta(table_uri, [_result_row(sql_text=query)], BaseBenchmark.RESULT_SCHEMA)

    written = deltalake.DeltaTable(table_uri).to_pyarrow_table()
    assert "sql_text" in written.column_names
    assert written.column("sql_text").to_pylist() == [query]


def test_appending_to_a_pre_upgrade_result_table_adds_the_column(tmp_path):
    """Result tables written before this change lack `sql_text`; the writer uses
    schema_mode="merge", so an append must add it and back-fill NULL."""
    deltalake = pytest.importorskip("deltalake")
    from lakebench.engines.base import BaseEngine

    engine = BaseEngine.__new__(BaseEngine)
    engine.storage_options = {}
    table_uri = str(tmp_path / "results")

    old_schema = [(name, kind) for name, kind in BaseBenchmark.RESULT_SCHEMA if name != "sql_text"]
    old_row = _result_row(test_item="q1")
    old_row.pop("sql_text")
    engine._append_results_to_delta(table_uri, [old_row], old_schema)

    assert "sql_text" not in deltalake.DeltaTable(table_uri).to_pyarrow_table().column_names

    engine._append_results_to_delta(
        table_uri, [_result_row(test_item="q2", sql_text="SELECT 2")], BaseBenchmark.RESULT_SCHEMA
    )

    written = deltalake.DeltaTable(table_uri).to_pyarrow_table().select(["test_item", "sql_text"]).to_pylist()
    by_item = {row["test_item"]: row["sql_text"] for row in written}
    assert by_item == {"q1": None, "q2": "SELECT 2"}


def test_warehouse_results_append_preserves_maps_and_scalar_types(tmp_path):
    deltalake = pytest.importorskip("deltalake")
    from lakebench.engines.fabric_data_warehouse import FabricDataWarehouse

    engine = FabricDataWarehouse.__new__(FabricDataWarehouse)
    engine.storage_options = {}
    table_uri = str(tmp_path / "warehouse-results")
    timestamp = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
    row = _result_row(test_item="q1", sql_text="SELECT 1")
    row.update(
        run_datetime=timestamp,
        estimated_retail_job_cost=Decimal("0.0123456789"),
        engine_properties={"warehouse_name": "test-warehouse", "size": 64},
        execution_telemetry={"statement_id": "test-statement", "rows": 1},
    )
    engine._append_results_to_delta(table_uri, [row], BaseBenchmark.RESULT_SCHEMA)
    engine._append_results_to_delta(table_uri, [_result_row(test_item="q2")], BaseBenchmark.RESULT_SCHEMA)

    rows = {row["test_item"]: row for row in deltalake.DeltaTable(table_uri).to_pyarrow_table().to_pylist()}
    assert set(rows) == {"q1", "q2"}
    assert rows["q1"]["sql_text"] == "SELECT 1"
    assert rows["q1"]["run_datetime"] == timestamp
    assert rows["q1"]["estimated_retail_job_cost"] == Decimal("0.0123456789")
    assert dict(rows["q1"]["engine_properties"]) == {"warehouse_name": "test-warehouse", "size": "64"}
    assert dict(rows["q1"]["execution_telemetry"]) == {"statement_id": "test-statement", "rows": "1"}
    assert rows["q2"]["sql_text"] is None
    assert rows["q2"]["engine_properties"] == []
    assert rows["q2"]["execution_telemetry"] == []


def test_spark_schema_conversion_covers_sql_text():
    pytest.importorskip("pyspark")
    from pyspark.sql.types import StringType

    from lakebench.engines.spark import Spark

    engine = Spark.__new__(Spark)
    schema = engine._convert_generic_to_specific_schema(BaseBenchmark.RESULT_SCHEMA)
    field = schema["sql_text"]
    assert isinstance(field.dataType, StringType)
    assert field.nullable
