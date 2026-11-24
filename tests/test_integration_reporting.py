import pytest

from tests.integration import conftest
from tests.integration.conftest import _render_engine_report, expected_workload, report_and_assert

_QUERY_WORKLOAD = [
    ("Load", "load", "customer", 1),
    ("Load", "load", "store", 1),
    ("Query", None, "q1", 1),
    ("Query", None, "q2", 1),
]
_TASK_WORKLOAD = [
    ("Read parquet, write delta (x5)", None, "store_sales", 1),
    ("Create fact table", None, "total_sales_fact", 1),
    ("Merge 0.1% into fact table (3x)", None, "total_sales_fact", 1),
    ("Merge 0.1% into fact table (3x)", None, "total_sales_fact", 2),
    ("Merge 0.1% into fact table (3x)", None, "total_sales_fact", 3),
    ("Ad-hoc query (small result aggregation)", None, "total_sales_fact", 1),
]


def _result(phase, test_item, success=True, error_message="", *, sub_phase=None, iteration=1):
    return {
        "phase": phase,
        "sub_phase": sub_phase if sub_phase is not None else ("load" if phase == "Load" else None),
        "test_item": test_item,
        "iteration": iteration,
        "success": success,
        "error_message": error_message,
    }


def _results(workload):
    return [
        _result(phase, test_item, sub_phase=sub_phase, iteration=iteration)
        for phase, sub_phase, test_item, iteration in workload
    ]


@pytest.fixture(autouse=True)
def _isolate_reports(monkeypatch):
    monkeypatch.setattr(conftest, "_RESULTS", [])


def test_partial_table_load_failure_is_rejected():
    results = _results(_QUERY_WORKLOAD)
    results[1].update(success=False, error_message="schema mismatch")

    with pytest.raises(pytest.fail.Exception, match="1 of 2 tables failed to load"):
        report_and_assert(results, "TPC-DS", "TestEngine", expected_results=_QUERY_WORKLOAD)


@pytest.mark.parametrize(("benchmark_name", "workload"), [("TPC-DS", _QUERY_WORKLOAD), ("ELTBench", _TASK_WORKLOAD)])
def test_complete_successful_run_is_accepted(benchmark_name, workload):
    report_and_assert(_results(workload), benchmark_name, "TestEngine", expected_results=workload, min_pass_rate=1.0)


@pytest.mark.parametrize(("benchmark_name", "workload"), [("TPC-DS", _QUERY_WORKLOAD), ("ELTBench", _TASK_WORKLOAD)])
@pytest.mark.parametrize("min_pass_rate", [0.0, 1.0])
def test_crash_before_results_is_rejected(benchmark_name, workload, min_pass_rate):
    with pytest.raises(pytest.fail.Exception, match="run aborted: RuntimeError: engine stopped"):
        report_and_assert(
            [],
            benchmark_name,
            "TestEngine",
            RuntimeError("engine stopped"),
            expected_results=workload,
            min_pass_rate=min_pass_rate,
        )


@pytest.mark.parametrize(("benchmark_name", "workload"), [("TPC-DS", _QUERY_WORKLOAD), ("ELTBench", _TASK_WORKLOAD)])
@pytest.mark.parametrize("min_pass_rate", [0.0, 1.0])
def test_empty_run_without_exception_is_rejected(benchmark_name, workload, min_pass_rate):
    with pytest.raises(pytest.fail.Exception, match="incomplete workload"):
        report_and_assert([], benchmark_name, "TestEngine", expected_results=workload, min_pass_rate=min_pass_rate)


@pytest.mark.parametrize(("benchmark_name", "workload"), [("TPC-DS", _QUERY_WORKLOAD), ("ELTBench", _TASK_WORKLOAD)])
@pytest.mark.parametrize("min_pass_rate", [0.0, 1.0])
@pytest.mark.parametrize("complete", [False, True])
def test_abort_after_success_is_rejected(benchmark_name, workload, min_pass_rate, complete):
    results = _results(workload if complete else workload[-2:-1])
    with pytest.raises(pytest.fail.Exception, match="run aborted.*aborted before next step"):
        report_and_assert(
            results,
            benchmark_name,
            "TestEngine",
            RuntimeError("aborted before next step"),
            expected_results=workload,
            min_pass_rate=min_pass_rate,
        )


@pytest.mark.parametrize("missing", _QUERY_WORKLOAD)
def test_missing_expected_load_or_query_is_rejected(missing):
    workload = [key for key in _QUERY_WORKLOAD if key != missing]
    with pytest.raises(pytest.fail.Exception, match=f"missing results:.*{missing[2]}"):
        report_and_assert(_results(workload), "TPC-DS", "TestEngine", expected_results=_QUERY_WORKLOAD)


@pytest.mark.parametrize("missing", _TASK_WORKLOAD)
def test_missing_expected_task_is_rejected(missing):
    workload = [key for key in _TASK_WORKLOAD if key != missing]
    with pytest.raises(pytest.fail.Exception, match="missing results:"):
        report_and_assert(_results(workload), "ELTBench", "TestEngine", expected_results=_TASK_WORKLOAD)


@pytest.mark.parametrize("duplicate", _QUERY_WORKLOAD)
def test_duplicate_load_or_query_cannot_replace_missing_result(duplicate):
    removed = next(key for key in _QUERY_WORKLOAD if key != duplicate)
    workload = [key for key in _QUERY_WORKLOAD if key != removed] + [duplicate]
    with pytest.raises(pytest.fail.Exception, match="unexpected or duplicate results:"):
        report_and_assert(_results(workload), "TPC-DS", "TestEngine", expected_results=_QUERY_WORKLOAD)


def test_duplicate_merge_iteration_cannot_replace_missing_iteration():
    workload = [*_TASK_WORKLOAD]
    workload[3] = workload[2]
    with pytest.raises(pytest.fail.Exception, match="missing results:.*unexpected or duplicate results:"):
        report_and_assert(_results(workload), "ELTBench", "TestEngine", expected_results=_TASK_WORKLOAD)


def test_maintenance_result_cannot_replace_table_load():
    results = _results(_QUERY_WORKLOAD)
    results[0]["sub_phase"] = "optimize"
    with pytest.raises(pytest.fail.Exception, match="missing results:.*unexpected or duplicate results:"):
        report_and_assert(results, "TPC-DS", "TestEngine", expected_results=_QUERY_WORKLOAD)


@pytest.mark.parametrize(("benchmark_name", "workload"), [("TPC-DS", _QUERY_WORKLOAD), ("ELTBench", _TASK_WORKLOAD)])
def test_complete_partial_support_run_still_uses_threshold(benchmark_name, workload):
    results = _results(workload)
    results[-1].update(success=False, error_message="unsupported query")

    with pytest.warns(UserWarning, match="1 of"):
        report_and_assert(results, benchmark_name, "PartialEngine", expected_results=workload, min_pass_rate=0.0)
    with pytest.warns(UserWarning, match="1 of"), pytest.raises(AssertionError, match="pass rate"):
        report_and_assert(results, benchmark_name, "StrictEngine", expected_results=workload, min_pass_rate=1.0)


@pytest.mark.parametrize(
    ("benchmark_name", "workload"),
    [("TPC-DS", _QUERY_WORKLOAD[2:]), ("ELTBench", _TASK_WORKLOAD)],
)
def test_complete_run_with_all_steps_failed_is_rejected(benchmark_name, workload):
    results = _results(workload)
    for result in results:
        result.update(success=False, error_message="unsupported")
    with pytest.warns(UserWarning), pytest.raises(AssertionError, match="ALL"):
        report_and_assert(results, benchmark_name, "PartialEngine", expected_results=workload, min_pass_rate=0.0)


class _QueryBenchmark:
    TABLE_REGISTRY = ["customer", "store"]
    QUERY_REGISTRY = ["q1", "q2"]


@pytest.mark.parametrize(
    ("mode", "workload"),
    [
        ("load", _QUERY_WORKLOAD[:2]),
        ("query", _QUERY_WORKLOAD[2:]),
        ("power_test", _QUERY_WORKLOAD[2:]),
        ("load_and_query", _QUERY_WORKLOAD),
    ],
)
def test_requested_mode_requires_only_its_declared_workload(mode, workload):
    expected = expected_workload(_QueryBenchmark, mode)
    report_and_assert(_results(workload), "TestBenchmark", "TestEngine", expected_results=expected, min_pass_rate=1.0)


def test_light_mode_requires_all_loads_and_merge_iterations():
    from lakebench.benchmarks import ELTBench

    workload = [
        ("Read parquet, write delta (x5)", None, table, 1)
        for table in ("store_sales", "date_dim", "store", "item", "customer")
    ] + _TASK_WORKLOAD[1:]
    workload.insert(-1, ("OPTIMIZE", None, "total_sales_fact", 1))
    workload.insert(-1, ("VACUUM", None, "total_sales_fact", 1))
    report_and_assert(
        _results(workload),
        "ELTBench",
        "TestEngine",
        expected_results=expected_workload(ELTBench, "light"),
        min_pass_rate=1.0,
    )


@pytest.mark.parametrize("expected", [[], [_QUERY_WORKLOAD[0]] * 2])
def test_empty_or_duplicate_expectations_are_rejected(expected):
    with pytest.raises(ValueError, match="nonempty, unique workload keys"):
        report_and_assert([], "TPC-DS", "TestEngine", expected_results=expected)


def test_undeclared_mode_is_rejected():
    from lakebench.benchmarks import ELTBench

    with pytest.raises(ValueError, match="No integration workload declared"):
        expected_workload(_QueryBenchmark, "typo")
    with pytest.raises(ValueError, match="No ELTBench integration workload declared"):
        expected_workload(ELTBench, "full")


def test_failed_run_is_retained_for_compatibility_report():
    results = [_result("Load", "store", False, "Unsupported Arrow DataType: Utf8View")]

    with pytest.raises(pytest.fail.Exception):
        report_and_assert(results, "TPC-DS", "Daft", expected_results=_QUERY_WORKLOAD)

    report = _render_engine_report("Daft", conftest._RESULTS)
    assert "Compatibility note" in report
    assert "Utf8View" in report
    assert "`store`" in report


@pytest.mark.parametrize("run_exception", [None, RuntimeError("aborted"), RuntimeError("")])
def test_incomplete_or_aborted_report_is_not_marked_successful(run_exception):
    with pytest.raises(pytest.fail.Exception):
        report_and_assert(
            _results(_QUERY_WORKLOAD[-2:-1]),
            "TPC-DS",
            "TestEngine",
            run_exception,
            expected_results=_QUERY_WORKLOAD,
        )
    report = _render_engine_report("TestEngine", conftest._RESULTS)
    assert "Incomplete workload" in report
    assert "missing results:" in report
    assert "❌ **1/1 queries passed" in report
