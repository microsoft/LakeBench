import importlib.resources
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from importlib.metadata import version
from typing import Dict, Optional, Tuple, Type

from ..engines.base import BaseEngine
from ..utils.timer import timer


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
        The path to the input parquet files, if applicable.
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
        ("test_item", "STRING"),
        ("start_datetime", "TIMESTAMP"),
        ("duration_ms", "INT"),
        ("estimated_retail_job_cost", "DECIMAL(18,10)"),
        ("iteration", "TINYINT"),
        ("success", "BOOLEAN"),
        ("error_message", "STRING"),
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
                Please provide fully qualified uri for `input_parquet_folder_uri`."""
            )

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

    def _load_resource_with_fallback(
        self,
        kind: str,
        file_name: str,
        benchmark_name: Optional[str] = None,
    ) -> Tuple[str, bool]:
        """
        Resolve a per-engine SQL/DDL resource with the standard fallback chain:

        1. ``<engine_root>.benchmarks.<benchmark>.resources.<kind>.<engine_class>``
        2. ``lakebench.benchmarks.<benchmark>.resources.<kind>.<parent_engine_class>``
        3. ``lakebench.benchmarks.<benchmark>.resources.<kind>.canonical`` (Spark dialect)

        ``kind`` is e.g. ``"ddl"`` or ``"queries"`` — the package directory name.
        ``benchmark_name`` defaults to the lowercased subclass name; pass an
        override to borrow another benchmark's resources (e.g. ELTBench reuses
        TPC-DS DDLs).

        Returns
        -------
        (text, used_canonical) : Tuple[str, bool]
            The file contents and a flag indicating whether the canonical fallback
            was used (so callers can reset their source dialect to ``"spark"``).
        """
        engine_class_name = self.engine.__class__.__name__.lower()
        parent_class_name = self.engine.__class__.__bases__[0].__name__.lower()
        if benchmark_name is None:
            benchmark_name = self.__class__.__name__.lower()
        engine_root = self.engine.__class__.__module__.split(".")[0]

        candidates = [
            (f"{engine_root}.benchmarks.{benchmark_name}.resources.{kind}.{engine_class_name}", False),
            (f"lakebench.benchmarks.{benchmark_name}.resources.{kind}.{parent_class_name}", False),
            (f"lakebench.benchmarks.{benchmark_name}.resources.{kind}.canonical", True),
        ]

        last_err: Optional[Exception] = None
        for pkg, is_canonical in candidates:
            try:
                with importlib.resources.path(pkg, file_name) as path:
                    with open(path, "r") as fh:
                        return fh.read(), is_canonical
            except (ModuleNotFoundError, FileNotFoundError) as exc:
                last_err = exc
                continue

        raise FileNotFoundError(
            f"Could not locate resource '{file_name}' for benchmark "
            f"'{benchmark_name}' under any of: {[c[0] for c in candidates]}"
        ) from last_err

    @abstractmethod
    def run(self):
        pass

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
                "test_item": test_item,
                "start_datetime": start_datetime,
                "duration_ms": duration_ms,
                "estimated_retail_job_cost": self.engine.get_job_cost(duration_ms),
                "iteration": iteration,
                "success": success,
                "error_message": error_message,
                "engine_properties": self.engine.extended_engine_metadata,
                "execution_telemetry": execution_telemetry,
            }
            for phase, test_item, start_datetime, duration_ms, iteration, success, error_message, execution_telemetry in self.timer.results
        ]
        self.results.extend(result_array)

        if self.save_results:
            if self.result_table_uri is None:
                raise ValueError("result_table_uri must be provided if save_results is True.")
            else:
                try:
                    self.engine._append_results_to_delta(self.result_table_uri, result_array, self.RESULT_SCHEMA)
                except Exception as e:
                    raise e
                finally:
                    self.timer.clear_results()
