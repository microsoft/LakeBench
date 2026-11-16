import logging
from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest

from lakebench.engines.spark import Spark
from lakebench.utils.timer import timer


def test_timer_log_includes_sub_phase(caplog):
    timer.clear_results()

    with caplog.at_level(logging.INFO, logger="lakebench.utils.timer"):
        with timer(phase="Load", sub_phase="analyze", test_item="customer"):
            pass

    assert "Load - analyze - customer" in caplog.text


def test_timer_log_includes_progress_without_changing_result_item(caplog):
    timer.clear_results()

    with caplog.at_level(logging.INFO, logger="lakebench.utils.timer"):
        with timer(phase="Query", test_item="q96", progress="1/103"):
            pass

    assert "Query - 1/103 - q96" in caplog.text
    assert timer.results[0][1] == "q96"
    assert timer.results[0][2] is None


def test_timer_error_log_includes_sub_phase(caplog):
    timer.clear_results()

    with caplog.at_level(logging.ERROR, logger="lakebench.utils.timer"):
        with timer(phase="Load", sub_phase="optimize", test_item="lineitem"):
            raise RuntimeError("failed")

    assert "Error during Load - optimize - lineitem" in caplog.text


@pytest.mark.parametrize("clock_adjustment", [-3600, 3600])
def test_timer_duration_ignores_wall_clock_adjustments(monkeypatch, clock_adjustment):
    timer.clear_results()
    wall_start = datetime(2026, 1, 1, 12)
    wall_clock = {"now": wall_start}
    mock_datetime = MagicMock()
    mock_datetime.now.side_effect = lambda: wall_clock["now"]
    monkeypatch.setattr("lakebench.utils.timer.datetime", mock_datetime)
    monkeypatch.setattr("lakebench.utils.timer.time.time", lambda: wall_clock["now"].timestamp())
    ticks = iter([5_000_000_000, 5_123_999_999])
    monkeypatch.setattr("lakebench.utils.timer.time.perf_counter_ns", lambda: next(ticks))

    with timer(phase="Query", test_item="q1"):
        wall_clock["now"] += timedelta(seconds=clock_adjustment)

    result = timer.results[0]
    assert result[3] == wall_start
    assert result[4] == 123


@pytest.mark.parametrize("fails", [False, True])
def test_timer_preserves_spark_labels_metrics_and_failure_details(fails):
    timer.clear_results()
    engine = Spark.__new__(Spark)
    engine.spark = MagicMock()
    engine.spark_measure_telemetry = True
    engine.capture_metrics = MagicMock()
    metrics_frame = engine.capture_metrics.aggregate_stagemetrics_DF.return_value.toPandas.return_value
    metrics_frame.iloc[0].to_dict.return_value = {"executorRunTime": 42}
    spark_context = engine.spark.sparkContext

    with timer("Query", "q1", engine, sub_phase="execute", progress="1/22") as context:
        assert context.context_decorator == "Query - execute - 1/22 - q1 [i:1]"
        spark_context.setJobDescription.assert_called_once_with(context.context_decorator)
        spark_context.setLocalProperty.assert_called_once_with("spark.scheduler.pool", "lakebench")
        engine.capture_metrics.begin.assert_called_once_with()
        context.sql_text = "SELECT 1"
        context.execution_telemetry = {"rows": "1"}
        if fails:
            raise RuntimeError("query failed")

    spark_context.setJobDescription.assert_called_with(None)
    engine.capture_metrics.end.assert_called_once_with()
    result = timer.results[0]
    assert result[6] is not fails
    assert result[7] == ("RuntimeError: query failed" if fails else "")
    assert result[8] == {"rows": "1", "executorRunTime": "42"}
    assert result[9] == "SELECT 1"


def test_timer_handles_spark_without_a_spark_context():
    timer.clear_results()
    engine = Spark.__new__(Spark)

    with timer(phase="Load", test_item="customer", engine=engine) as context:
        context.execution_telemetry = None

    assert timer.results[0][6] is True
    assert timer.results[0][8] == {}
