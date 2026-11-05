import logging
import re
import time
from contextlib import contextmanager
from datetime import datetime

from ..engines.spark import Spark

logger = logging.getLogger(__name__)


def _format_timer_label(phase: str, test_item: str, sub_phase: str = None, progress: str = None) -> str:
    return " - ".join(part for part in (phase, sub_phase, progress, test_item) if part)


def _has_spark_context(engine):
    """Check if engine has a usable sparkContext (not available on Spark Connect)."""
    if not isinstance(engine, Spark):
        return False
    return engine._has_spark_context


_INVALID_TAG_CHARS = re.compile(r"[^A-Za-z0-9_.:\-\[\] ]+")


def _to_spark_connect_tag(label: str) -> str:
    """Convert a timer label into a valid Spark Connect operation tag (no commas, non-empty)."""
    tag = _INVALID_TAG_CHARS.sub("_", label).strip()
    return tag or "lakebench"


def _spark_connect_session(engine):
    """Return a tag-capable Spark Connect session for the engine, or ``None``."""
    if not isinstance(engine, Spark) or engine._has_spark_context:
        return None
    spark = getattr(engine, "spark", None)
    if spark is None or not callable(getattr(spark, "addTag", None)):
        return None
    return spark


@contextmanager
def timer(
    phase: str = "Elapsed time",
    test_item: str = "",
    engine: str = None,
    sub_phase: str = None,
    progress: str = None,
):
    if not hasattr(timer, "results"):
        timer.results = []

    iteration = (
        sum(1 for result in timer.results if result[0] == phase and result[1] == test_item and result[2] == sub_phase)
        + 1
    )
    timer_label = _format_timer_label(phase, test_item, sub_phase, progress)

    class TimerContext:
        def __init__(self, label: str, iteration: int):
            self.execution_telemetry = {}
            self.sql_text = None
            self.context_decorator = f"{label} [i:{iteration}]"

    timer_context = TimerContext(timer_label, iteration)

    has_sc = _has_spark_context(engine)
    if has_sc:
        engine.spark.sparkContext.setJobDescription(timer_context.context_decorator)
        if engine.spark_measure_telemetry:
            engine.capture_metrics.begin()
            engine.spark.sparkContext.setLocalProperty("spark.scheduler.pool", "lakebench")

    connect_session = _spark_connect_session(engine)
    connect_tag = None
    if connect_session is not None:
        try:
            connect_tag = _to_spark_connect_tag(timer_context.context_decorator)
            connect_session.addTag(connect_tag)
        except Exception as exc:
            logger.debug("Unable to add Spark Connect tag: %s", exc)
            connect_tag = None

    start = time.time()
    start_datetime = datetime.now()
    success = True
    error_message = None
    error_type = None

    try:
        yield timer_context
    except Exception as e:
        success = False
        error_message = str(e)
        error_type = type(e).__name__  # Capture the error type
        logger.error("Error during %s... %s: %s", timer_label, error_type, error_message)

    finally:
        end = time.time()
        duration = int((end - start) * 1000)
        logger.info(
            "%s%s: %.2f seconds",
            timer_label,
            f" [i:{iteration}]" if iteration > 1 else "",
            duration / 1000,
        )
        # Set execution metadata to an empty dict if it is not set or was set to anything other than a dict
        if not isinstance(timer_context.execution_telemetry, dict):
            timer_context.execution_telemetry = {}

        if connect_tag is not None:
            try:
                connect_session.removeTag(connect_tag)
            except Exception as exc:
                logger.debug("Unable to remove Spark Connect tag: %s", exc)

        if has_sc:
            engine.spark.sparkContext.setJobDescription(None)
            if engine.spark_measure_telemetry:
                engine.capture_metrics.end()
                listener_metrics_agg = engine.capture_metrics.aggregate_stagemetrics_DF()
                listener_metrics_dict = listener_metrics_agg.toPandas().iloc[0].to_dict()
                listener_metrics_str_dict = {k: str(v) for k, v in listener_metrics_dict.items()}
                timer_context.execution_telemetry.update(listener_metrics_str_dict)

        timer.results.append(
            (
                phase,
                test_item,
                sub_phase,
                start_datetime,
                duration,
                iteration,
                success,
                f"{error_type}: {error_message}" if error_message else "",
                timer_context.execution_telemetry,
                timer_context.sql_text if isinstance(timer_context.sql_text, str) else None,
            )
        )


def _clear_results():
    if hasattr(timer, "results"):
        timer.results = []


timer.clear_results = _clear_results
