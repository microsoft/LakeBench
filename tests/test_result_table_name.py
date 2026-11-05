"""Writing benchmark results to a catalog table via ``result_table_name``."""

from unittest.mock import MagicMock

import pytest

from lakebench.benchmarks.base import BaseBenchmark
from lakebench.engines.base import BaseEngine
from lakebench.engines.spark import Spark
from lakebench.utils.timer import timer

ROWS = [{"run_id": "run-1"}]
SCHEMA = [("run_id", "STRING")]


def _spark_engine():
    engine = Spark.__new__(Spark)
    engine.spark = MagicMock()
    engine._convert_generic_to_specific_schema = lambda generic_schema: None
    writer = MagicMock()
    # Every builder call returns the same writer so calls can be asserted in one place.
    writer.format.return_value = writer
    writer.option.return_value = writer
    writer.mode.return_value = writer
    engine.spark.createDataFrame.return_value.select.return_value.write = writer
    return engine, writer


def _path_options(writer):
    return [call for call in writer.option.call_args_list if call.args[0] == "path"]


def test_spark_table_name_only_creates_managed_table():
    engine, writer = _spark_engine()

    engine._append_results_to_delta(None, ROWS, SCHEMA, table_name="main.bench.results")

    writer.saveAsTable.assert_called_once_with("main.bench.results")
    writer.save.assert_not_called()
    assert _path_options(writer) == []
    writer.mode.assert_called_once_with("append")


def test_spark_table_name_with_uri_creates_external_table():
    engine, writer = _spark_engine()

    engine._append_results_to_delta("abfss://c@a/results", ROWS, SCHEMA, table_name="main.bench.results")

    writer.saveAsTable.assert_called_once_with("main.bench.results")
    assert [call.args for call in _path_options(writer)] == [("path", "abfss://c@a/results")]


def test_spark_uri_only_keeps_path_write():
    engine, writer = _spark_engine()

    engine._append_results_to_delta("abfss://c@a/results", ROWS, SCHEMA)

    writer.save.assert_called_once_with("abfss://c@a/results")
    writer.saveAsTable.assert_not_called()


def test_base_engine_rejects_table_name():
    engine = BaseEngine.__new__(BaseEngine)

    with pytest.raises(NotImplementedError, match="catalog table"):
        engine._append_results_to_delta(None, ROWS, SCHEMA, table_name="main.bench.results")


class _TestBenchmark(BaseBenchmark):
    def run(self):
        pass


def _benchmark(result_table_uri=None, result_table_name=None):
    benchmark = object.__new__(_TestBenchmark)
    benchmark.results = []
    benchmark.header_detail_dict = {"run_id": "run-1"}
    benchmark.mode = "power_test"
    benchmark.engine = MagicMock()
    benchmark.engine.extended_engine_metadata = {}
    benchmark.engine.get_job_cost.return_value = None
    benchmark.save_results = True
    benchmark.result_table_uri = result_table_uri
    benchmark.result_table_name = result_table_name
    benchmark.timer = timer
    timer.results = [("Query", "q1", None, None, 1000, 1, True, "", {}, "SELECT 1")]
    return benchmark


def test_post_results_passes_table_name_to_engine():
    benchmark = _benchmark(result_table_name="main.bench.results")

    BaseBenchmark.post_results(benchmark)

    call = benchmark.engine._append_results_to_delta.call_args
    assert call.args[0] is None
    assert call.kwargs["table_name"] == "main.bench.results"
    assert timer.results == []


def test_post_results_requires_uri_or_table_name():
    benchmark = _benchmark()

    with pytest.raises(ValueError, match="result_table_name"):
        BaseBenchmark.post_results(benchmark)
    assert timer.results == []
