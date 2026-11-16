from unittest.mock import MagicMock

import pytest

from lakebench.benchmarks.base import BaseBenchmark
from lakebench.utils.timer import timer


class _Benchmark(BaseBenchmark):
    def run(self):
        pass


def _benchmark(run_id, save_results=False):
    engine = MagicMock()
    engine.version = "test"
    engine.extended_engine_metadata = {}
    engine.get_job_cost.return_value = None
    return _Benchmark(
        engine=engine,
        scenario_name="timer-isolation",
        input_parquet_folder_uri="",
        result_table_uri=f"results-{run_id}",
        save_results=save_results,
        run_id=run_id,
    )


def test_interleaved_benchmarks_keep_records_iterations_and_clearing_isolated(monkeypatch):
    monkeypatch.setattr(timer, "results", [])
    with timer(phase="Query", test_item="q1"):
        pass
    standalone_results = list(timer.results)

    first = _benchmark("first")
    assert timer.results == standalone_results
    with first.timer(phase="Query", test_item="q1") as context:
        context.sql_text = "SELECT 1"
        context.execution_telemetry = {"owner": "first"}
    first_pending = list(first.timer.results)

    second = _benchmark("second")
    second.post_results()
    assert second.results == []
    assert first.timer.results == first_pending

    with second.timer(phase="Query", test_item="q1") as second_context:
        second_context.sql_text = "SELECT 2"
        second_context.execution_telemetry = {"owner": "second"}
        with first.timer(phase="Query", test_item="q1") as first_context:
            first_context.sql_text = "SELECT bad"
            first_context.execution_telemetry = {"owner": "first-failure"}
            raise RuntimeError("query failed")

    assert [result[5] for result in first.timer.results] == [1, 2]
    assert [result[5] for result in second.timer.results] == [1]
    first_pending = list(first.timer.results)
    second.post_results()
    assert first.timer.results == first_pending
    assert second.timer.results == []
    assert len(second.results) == 1
    assert second.results[0]["run_id"] == "second"
    assert second.results[0]["sql_text"] == "SELECT 2"
    assert second.results[0]["execution_telemetry"] == {"owner": "second"}

    with second.timer(phase="Query", test_item="q1"):
        pass
    assert second.timer.results[0][5] == 1
    second.timer.clear_results()
    assert first.timer.results == first_pending

    first.post_results()
    assert first.timer.results == []
    assert [row["run_id"] for row in first.results] == ["first", "first"]
    assert [row["iteration"] for row in first.results] == [1, 2]
    assert [row["sql_text"] for row in first.results] == ["SELECT 1", "SELECT bad"]
    assert [row["success"] for row in first.results] == [True, False]
    assert first.results[1]["error_message"] == "RuntimeError: query failed"
    assert first.results[1]["execution_telemetry"] == {"owner": "first-failure"}
    assert timer.results == standalone_results


@pytest.mark.parametrize("write_fails", [False, True])
def test_posting_results_only_drains_the_owning_benchmark(write_fails):
    first = _benchmark("first")
    with first.timer(phase="Query", test_item="q1"):
        pass
    first_pending = list(first.timer.results)
    second = _benchmark("second", save_results=True)
    with second.timer(phase="Query", test_item="q2"):
        pass

    if write_fails:
        second.engine._append_results_to_delta.side_effect = RuntimeError("write failed")
        with pytest.raises(RuntimeError, match="write failed"):
            second.post_results()
    else:
        second.post_results()

    second.engine._append_results_to_delta.assert_called_once_with(
        "results-second", second.results, second.RESULT_SCHEMA
    )
    assert [row["test_item"] for row in second.results] == ["q2"]
    assert second.timer.results == []
    assert first.timer.results == first_pending
