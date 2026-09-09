import pytest

from tests.integration import conftest
from tests.integration.conftest import _render_engine_report, report_and_assert


def _result(phase, test_item, success, error_message=""):
    return {
        "phase": phase,
        "test_item": test_item,
        "success": success,
        "error_message": error_message,
    }


def test_partial_table_load_failure_is_rejected():
    results = [
        _result("Load", "customer", True),
        _result("Load", "store", False, "schema mismatch"),
        _result("Query", "q1", True),
    ]

    with pytest.raises(pytest.fail.Exception, match="1 of 2 tables failed to load"):
        report_and_assert(results, "TPC-DS", "TestEngine")


def test_successful_loads_still_use_query_threshold():
    results = [
        _result("Load", "customer", True),
        _result("Load", "store", True),
        _result("Query", "q1", True),
    ]

    report_and_assert(results, "TPC-DS", "TestEngine", min_pass_rate=1.0)


def test_query_benchmark_crash_before_results_is_rejected():
    with pytest.raises(pytest.fail.Exception, match="engine crashed before any queries ran"):
        report_and_assert([], "TPC-DS", "TestEngine", RuntimeError("engine stopped"))


def test_task_benchmark_crash_before_results_is_rejected():
    with pytest.raises(pytest.fail.Exception, match="engine crashed before any tasks ran"):
        report_and_assert([], "ELTBench", "TestEngine", RuntimeError("engine stopped"))


def test_failed_run_is_retained_for_compatibility_report(monkeypatch):
    monkeypatch.setattr(conftest, "_RESULTS", [])
    results = [_result("Load", "store", False, "Unsupported Arrow DataType: Utf8View")]

    with pytest.raises(pytest.fail.Exception):
        report_and_assert(results, "TPC-DS", "Daft")

    report = _render_engine_report("Daft", conftest._RESULTS)
    assert "Compatibility note" in report
    assert "Utf8View" in report
    assert "`store`" in report
