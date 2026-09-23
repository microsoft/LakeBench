import logging
from unittest.mock import MagicMock

from lakebench.benchmarks.base import BaseBenchmark
from lakebench.utils.timer import timer


class _TestBenchmark(BaseBenchmark):
    def run(self):
        pass


def _benchmark(results):
    benchmark = object.__new__(_TestBenchmark)
    benchmark.results = results
    benchmark.header_detail_dict = {
        "benchmark": "TestBenchmark",
        "engine": "TestEngine",
    }
    benchmark.mode = "power_test"
    return benchmark


def test_benchmark_summary_logs_stats_phase_durations_and_net_duration(caplog):
    benchmark = _benchmark(
        [
            {"phase": "Load", "duration_ms": 90000, "success": True},
            {"phase": "Query", "duration_ms": 30000, "success": True},
            {"phase": "Query", "duration_ms": 15000, "success": False},
        ]
    )

    with caplog.at_level(logging.INFO, logger="lakebench.benchmarks.base"):
        benchmark._log_benchmark_summary(150.0)

    assert "3 total, 2 succeeded, 1 failed (66.67% success)" in caplog.text
    assert "Load: 90.00 seconds (1.50 minutes)" in caplog.text
    assert "Query: 45.00 seconds (0.75 minutes)" in caplog.text
    assert "Net duration: 150.00 seconds (2.50 minutes)" in caplog.text


def test_benchmark_summary_only_includes_results_from_current_run(caplog):
    benchmark = _benchmark(
        [
            {"phase": "Load", "duration_ms": 90000, "success": False},
            {"phase": "Query", "duration_ms": 30000, "success": True},
        ]
    )

    with caplog.at_level(logging.INFO, logger="lakebench.benchmarks.base"):
        benchmark._log_benchmark_summary(30.0, result_start_index=1)

    assert "1 total, 1 succeeded, 0 failed (100.00% success)" in caplog.text
    assert "Load:" not in caplog.text
    assert "Query: 30.00 seconds (0.50 minutes)" in caplog.text


def test_post_results_always_drains_timer_results():
    benchmark = _benchmark([])
    benchmark.engine = MagicMock()
    benchmark.engine.extended_engine_metadata = {}
    benchmark.engine.get_job_cost.return_value = None
    benchmark.save_results = False
    benchmark.result_table_uri = None
    benchmark.timer = timer
    timer.results = [
        ("Query", "q1", None, None, 1000, 1, True, "", {}, "SELECT 1"),
    ]

    BaseBenchmark.post_results(benchmark)

    assert len(benchmark.results) == 1
    assert timer.results == []
