import logging
import time
from contextlib import contextmanager
from datetime import datetime

from ..engines.spark import Spark

logger = logging.getLogger(__name__)


def _has_spark_context(engine):
    """Check if engine has a usable sparkContext (not available in Databricks Connect)."""
    if not isinstance(engine, Spark):
        return False
    try:
        engine.spark.sparkContext
        return True
    except Exception:
        return False


@contextmanager
def timer(phase: str = "Elapsed time", test_item: str = "", engine: str = None):
    if not hasattr(timer, "results"):
        timer.results = []

    iteration = sum(1 for result in timer.results if result[0] == phase and result[1] == test_item) + 1

    class TimerContext:
        def __init__(self, phase: str, test_item: str, iteration: int):
            self.execution_telemetry = {}
            self.context_decorator = f"{phase} - {test_item} [i:{iteration}]"

    timer_context = TimerContext(phase, test_item, iteration)

    has_sc = _has_spark_context(engine)
    if has_sc:
        engine.spark.sparkContext.setJobDescription(timer_context.context_decorator)
        if engine.spark_measure_telemetry:
            engine.capture_metrics.begin()
            engine.spark.sparkContext.setLocalProperty("spark.scheduler.pool", "lakebench")

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
        logger.error("Error during %s - %s... %s: %s", phase, test_item, error_type, error_message)

    finally:
        end = time.time()
        duration = int((end - start) * 1000)
        logger.info(
            "%s - %s%s: %.2f seconds",
            phase,
            test_item,
            f" [i:{iteration}]" if iteration > 1 else "",
            duration / 1000,
        )
        # Set execution metadata to an empty dict if it is not set or was set to anything other than a dict
        if not isinstance(timer_context.execution_telemetry, dict):
            timer_context.execution_telemetry = {}

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
                start_datetime,
                duration,
                iteration,
                success,
                f"{error_type}: {error_message}" if error_message else "",
                timer_context.execution_telemetry,
            )
        )


def _clear_results():
    if hasattr(timer, "results"):
        timer.results = []


timer.clear_results = _clear_results
