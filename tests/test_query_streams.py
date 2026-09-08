from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from lakebench.benchmarks._load_and_query import _LoadAndQuery
from lakebench.benchmarks.tpcds import TPCDS
from lakebench.benchmarks.tpch import TPCH


def test_power_test_runs_queries_without_loading():
    benchmark = object.__new__(_LoadAndQuery)
    benchmark.query_list = ["q1"]
    benchmark.query_progress = None
    benchmark._power_test_uses_full_stream = False
    benchmark._run_load_test = MagicMock()
    benchmark._run_query_test = MagicMock()

    benchmark._run_power_test()

    assert benchmark.mode == "power_test"
    benchmark._run_load_test.assert_not_called()
    benchmark._run_query_test.assert_called_once_with()


def test_load_and_query_remains_the_combined_mode():
    benchmark = object.__new__(_LoadAndQuery)
    calls = []
    benchmark._run_load_test = lambda: calls.append("load")
    benchmark._run_query_test = lambda: calls.append("query")

    benchmark._run_load_and_query()

    assert benchmark.mode == "load_and_query"
    assert calls == ["load", "query"]


@pytest.mark.parametrize(
    ("benchmark_class", "stream_count", "query_count"),
    [
        (TPCDS, 21, 99),
        (TPCH, 41, 22),
    ],
)
def test_query_streams_are_complete_permutations(benchmark_class, stream_count, query_count):
    assert benchmark_class.POWER_TEST_STREAM == 0
    assert len(benchmark_class.QUERY_STREAMS) == stream_count
    assert all(len(stream) == query_count for stream in benchmark_class.QUERY_STREAMS)
    assert all(set(stream) == set(range(1, query_count + 1)) for stream in benchmark_class.QUERY_STREAMS)


def test_tpcds_power_test_uses_stream_zero_order():
    benchmark = object.__new__(TPCDS)
    benchmark.query_list = ["q1"]
    benchmark.query_progress = None
    benchmark._power_test_uses_full_stream = True
    captured_query_lists = []
    captured_progress = []

    def capture_query_run():
        captured_query_lists.append(benchmark.query_list.copy())
        captured_progress.append(benchmark.query_progress.copy())

    benchmark._run_query_test = capture_query_run

    benchmark._run_power_test()

    assert benchmark.mode == "power_test"
    assert captured_query_lists[0][:10] == ["q96", "q7", "q75", "q44", "q39a", "q39b", "q80", "q32", "q19", "q25"]
    assert captured_query_lists[0][-10:] == ["q13", "q24a", "q24b", "q4", "q99", "q68", "q83", "q61", "q5", "q76"]
    assert captured_progress[0][:10] == ["1/99", "2/99", "3/99", "4/99", "5/99", "5/99", "6/99", "7/99", "8/99", "9/99"]
    assert len(captured_query_lists[0]) == 103
    assert benchmark.query_list == ["q1"]
    assert benchmark.query_progress is None


def test_tpch_power_test_uses_stream_zero_order():
    benchmark = object.__new__(TPCH)
    benchmark.query_list = ["q1"]
    benchmark.query_progress = None
    benchmark._power_test_uses_full_stream = True
    captured_query_lists = []
    captured_progress = []

    def capture_query_run():
        captured_query_lists.append(benchmark.query_list.copy())
        captured_progress.append(benchmark.query_progress.copy())

    benchmark._run_query_test = capture_query_run

    benchmark._run_power_test()

    assert benchmark.mode == "power_test"
    assert captured_query_lists == [
        [
            "q14",
            "q2",
            "q9",
            "q20",
            "q6",
            "q17",
            "q18",
            "q8",
            "q21",
            "q13",
            "q3",
            "q22",
            "q16",
            "q4",
            "q11",
            "q15",
            "q1",
            "q10",
            "q19",
            "q5",
            "q7",
            "q12",
        ]
    ]
    assert captured_progress == [[f"{sequence_number}/22" for sequence_number in range(1, 23)]]
    assert benchmark.query_list == ["q1"]
    assert benchmark.query_progress is None


@pytest.mark.parametrize(
    ("benchmark_class", "stream_number"),
    [
        (TPCDS, 21),
        (TPCH, 41),
    ],
)
def test_unknown_query_stream_is_rejected(benchmark_class, stream_number):
    with pytest.raises(ValueError, match=f"Unknown .* stream {stream_number}"):
        benchmark_class._query_names_for_stream(stream_number)


@pytest.mark.parametrize("benchmark_class", [TPCDS, TPCH])
def test_power_test_preserves_an_explicit_query_list(benchmark_class):
    benchmark = object.__new__(benchmark_class)
    benchmark.query_list = ["q1", "q2", "q3"]
    benchmark.query_progress = None
    benchmark._power_test_uses_full_stream = False
    benchmark.engine = MagicMock()
    benchmark.benchmark_impl = None
    benchmark._return_query_definition = lambda query_name: query_name
    benchmark.post_results = MagicMock()
    logged_progress = []

    @contextmanager
    def capture_timer(**kwargs):
        logged_progress.append(kwargs["progress"])
        yield SimpleNamespace(execution_telemetry={}, context_decorator="")

    benchmark.timer = capture_timer

    benchmark._run_power_test()

    assert logged_progress == ["1/3", "2/3", "3/3"]
    assert [call.args[0] for call in benchmark.engine.execute_sql_query.call_args_list] == ["q1", "q2", "q3"]
    assert benchmark.query_list == ["q1", "q2", "q3"]


@pytest.mark.parametrize("benchmark_class", [TPCDS, TPCH])
def test_wildcard_power_test_uses_the_complete_stream(benchmark_class):
    benchmark = object.__new__(benchmark_class)
    benchmark.query_list = benchmark_class.QUERY_REGISTRY.copy()
    benchmark.query_progress = None
    benchmark._power_test_uses_full_stream = True
    captured_query_lists = []
    benchmark._run_query_test = lambda: captured_query_lists.append(benchmark.query_list.copy())

    benchmark._run_power_test()

    assert captured_query_lists == [benchmark_class._query_names_for_stream(0)]
