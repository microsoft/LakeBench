import logging

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
