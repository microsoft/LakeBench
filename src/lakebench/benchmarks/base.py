import logging
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from importlib.metadata import version
from typing import Dict, Optional, Type

from ..engines.base import BaseEngine
from ..utils.timer import timer

logger = logging.getLogger(__name__)


def resolve_input_folder_uri(
    input_parquet_folder_uri: Optional[str],
    input_folder_uri: Optional[str],
) -> Optional[str]:
    """Resolve the input location from its two accepted parameter names.

    ``input_folder_uri`` is the preferred name because the input is not
    necessarily Parquet; ``input_parquet_folder_uri`` is retained as an alias.
    """
    if (
        input_parquet_folder_uri is not None
        and input_folder_uri is not None
        and input_parquet_folder_uri != input_folder_uri
    ):
        raise ValueError(
            "'input_folder_uri' and 'input_parquet_folder_uri' are aliases for the same input "
            "location but were given different values. Provide only one."
        )
    return input_folder_uri if input_folder_uri is not None else input_parquet_folder_uri


class BaseBenchmark(ABC):
    """
    Abstract base class for defining benchmarks. This class provides a structure for implementing benchmarks
    with a specific engine and scenario, and includes functionality for timing and saving results.

    Attributes
    ----------
    BENCHMARK_IMPL_REGISTRY : Dict[Type, Type]
        A registry for engines that the benchmark supports. If the engine requires a specific implementation
        that doesn't use the engines existing methods, the dictionary will map engines to the specific implementation
        class rather than. If only shared methods are used, the dictionary value will be None.
    engine : object
        The engine used to execute the benchmark.
    scenario_name : str
        The name of the scenario being benchmarked.
    input_parquet_folder_uri : Optional[str]
        The path to the input files, if applicable. Accepted as
        ``input_folder_uri`` on benchmark constructors; both names are
        supported and refer to the same location.
    result_table_uri : Optional[str]
        The path where benchmark results will be saved, if `save_results` is True.
    save_results : bool
        Flag indicating whether to save benchmark results to a Delta table.
    header_detail_dict : dict
        A dictionary containing metadata about the benchmark run, including run ID, datetime, engine type,
        benchmark name, scenario name, total cores, and compute size.
    timer : object
        A timer object used to measure the duration of benchmark phases.
    results : list
        A list to store benchmark results.

    Methods
    -------
    run()
        Abstract method that must be implemented by subclasses to define the benchmark logic.
    post_results()
        Processes and saves benchmark results. If `save_results` is True, results are appended to a Delta table
        at the specified `result_table_uri`. Clears the timer results after processing.
    """

    BENCHMARK_IMPL_REGISTRY: Dict[Type[BaseEngine], Type] = {}
    RESULT_SCHEMA = [
        ("run_id", "STRING"),
        ("run_datetime", "TIMESTAMP"),
        ("lakebench_version", "STRING"),
        ("engine", "STRING"),
        ("engine_version", "STRING"),
        ("benchmark", "STRING"),
        ("benchmark_version", "STRING"),
        ("mode", "STRING"),
        ("scale_factor", "INT"),
        ("scenario", "STRING"),
        ("total_cores", "SMALLINT"),
        ("compute_size", "STRING"),
        ("phase", "STRING"),
        ("sub_phase", "STRING"),
        ("test_item", "STRING"),
        ("start_datetime", "TIMESTAMP"),
        ("duration_ms", "INT"),
        ("estimated_retail_job_cost", "DECIMAL(18,10)"),
        ("iteration", "TINYINT"),
        ("success", "BOOLEAN"),
        ("error_message", "STRING"),
        ("sql_text", "STRING"),  # Exact SQL sent to the engine; NULL for non-SQL test items
        ("engine_properties", "MAP<STRING, STRING>"),  # Additional Platform configs/metadata
        ("execution_telemetry", "MAP<STRING, STRING>"),  # Test-item execution details
    ]
    VERSION = ""

    def __init__(
        self,
        engine: BaseEngine,
        scenario_name: str,
        input_parquet_folder_uri: Optional[str],
        result_table_uri: Optional[str],
        save_results: bool = False,
        run_id: Optional[str] = None,
    ):
        self.engine = engine
        self.scenario_name = scenario_name
        self.result_table_uri = result_table_uri
        self.save_results = save_results

        if not engine.SUPPORTS_MOUNT_PATH and input_parquet_folder_uri[:1] == "/":
            raise ValueError(
                f"""Mount path is not supported for {type(engine).__name__} engine.
                Please provide fully qualified uri for `input_folder_uri`."""
            )

        #: Canonical input location. ``input_parquet_folder_uri`` is kept as an
        #: alias so existing code and notebooks continue to work.
        self.input_folder_uri = input_parquet_folder_uri
        self.input_parquet_folder_uri = input_parquet_folder_uri

        self.header_detail_dict = {
            "run_id": run_id if run_id is not None else str(uuid.uuid1()),
            "run_datetime": datetime.now(),
            "lakebench_version": version("lakebench"),
            "engine": type(engine).__name__,
            "engine_version": self.engine.version,
            "benchmark": self.__class__.__name__,
            "benchmark_version": self.VERSION,
            "scale_factor": getattr(self, "scale_factor", None),
            "scenario": scenario_name,
            "total_cores": self.engine.get_total_cores(),
            "compute_size": self.engine.get_compute_size(),
        }
        self.timer = timer
        self.timer.clear_results()
        self.results = []
        self.mode: str = None

    @classmethod
    def register_engine(cls, engine_class: Type[BaseEngine], benchmark_impl: Optional[Type] = None):
        """
        Registers a custom engine class and its corresponding benchmark implementation.

        Parameters
        ----------
        engine_class : Type[BaseEngine]
            The engine class to register.
        benchmark_impl : Type[BaseBenchmark], optional
            The benchmark implementation class for the engine. If None, the engine's default methods will be used.
        """
        cls.BENCHMARK_IMPL_REGISTRY[engine_class] = benchmark_impl

    @abstractmethod
    def run(self):
        pass

    def _log_benchmark_summary(self, result_start_index: int = 0):
        run_results = self.results[result_start_index:]
        total = len(run_results)
        succeeded = sum(1 for result in run_results if result["success"])
        failed = total - succeeded
        success_rate = (succeeded / total * 100) if total else 0.0
        net_duration_ms = sum(result["duration_ms"] for result in run_results)

        phase_duration_ms = {}
        for result in run_results:
            phase = result["phase"]
            phase_duration_ms[phase] = phase_duration_ms.get(phase, 0) + result["duration_ms"]

        phase_lines = [
            f"  {phase}: {duration_ms / 1000:.2f} seconds ({duration_ms / 60000:.2f} minutes)"
            for phase, duration_ms in phase_duration_ms.items()
        ]
        if not phase_lines:
            phase_lines.append("  No timed phases recorded")

        logger.info(
            "Benchmark summary for %s on %s (mode: %s)\n"
            "Results: %d total, %d succeeded, %d failed (%.2f%% success)\n"
            "Duration by phase:\n%s\n"
            "Net duration: %.2f seconds (%.2f minutes)",
            self.header_detail_dict["benchmark"],
            self.header_detail_dict["engine"],
            self.mode or "unknown",
            total,
            succeeded,
            failed,
            success_rate,
            "\n".join(phase_lines),
            net_duration_ms / 1000,
            net_duration_ms / 60000,
        )

    def post_results(self):
        """
        Processes and posts benchmark results, saving them to a specified location if save_results is True.
        This method collects timing results from the benchmark execution, formats them into a
        structured array, and optionally saves the results to a Delta table. It also clears the timer
        instance after offloading results to the `self.results` attribute.

        Parameters
        ----------
        None

        Notes
        -----
        - If `save_results` is True, the results are appended to the Delta table specified by
          `result_table_uri` using the `engine.append_array_to_delta` method.
        - After processing, the results are stored in `self.results` and the timer results are cleared.

        Examples
        --------
        >>> benchmark = Benchmark()
        >>> benchmark.post_results()
        # Processes the results and saves them if `save_results` is True.
        # post_results() should be called after each major benchmark phase.
        """

        result_array = [
            {
                **self.header_detail_dict,
                "mode": self.mode.lower() if self.mode else None,
                "phase": phase,
                "sub_phase": sub_phase,
                "test_item": test_item,
                "start_datetime": start_datetime,
                "duration_ms": duration_ms,
                "estimated_retail_job_cost": self.engine.get_job_cost(duration_ms),
                "iteration": iteration,
                "success": success,
                "error_message": error_message,
                "sql_text": sql_text,
                "engine_properties": self.engine.extended_engine_metadata,
                "execution_telemetry": execution_telemetry,
            }
            for phase, test_item, sub_phase, start_datetime, duration_ms, iteration, success, error_message, execution_telemetry, sql_text in self.timer.results
        ]
        self.results.extend(result_array)

        try:
            if self.save_results:
                if self.result_table_uri is None:
                    raise ValueError("result_table_uri must be provided if save_results is True.")
                else:
                    self.engine._append_results_to_delta(self.result_table_uri, result_array, self.RESULT_SCHEMA)
        finally:
            self.timer.clear_results()
