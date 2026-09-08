import pytest

from tests.integration.conftest import report_and_assert


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
