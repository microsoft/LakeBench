import logging

from lakebench.utils.timer import timer


def test_timer_log_includes_sub_phase(caplog):
    timer.clear_results()

    with caplog.at_level(logging.INFO, logger="lakebench.utils.timer"):
        with timer(phase="Load", sub_phase="analyze", test_item="customer"):
            pass

    assert "Load - analyze - customer" in caplog.text


def test_timer_error_log_includes_sub_phase(caplog):
    timer.clear_results()

    with caplog.at_level(logging.ERROR, logger="lakebench.utils.timer"):
        with timer(phase="Load", sub_phase="optimize", test_item="lineitem"):
            raise RuntimeError("failed")

    assert "Error during Load - optimize - lineitem" in caplog.text
