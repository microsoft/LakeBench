from __future__ import annotations

import posixpath
from typing import Optional

from ...engines.base import BaseEngine
from ...engines.daft import Daft
from ...engines.duckdb import DuckDB
from ...engines.polars import Polars
from ...engines.sail import Sail
from ...engines.spark import Spark
from ...utils.query_utils import get_table_name_from_ddl, transpile_and_qualify_query
from ..base import BaseBenchmark
from ..tpcds.tpcds import TPCDS
from .engine_impl.daft import DaftELTBench
from .engine_impl.duckdb import DuckDBELTBench
from .engine_impl.polars import PolarsELTBench
from .engine_impl.sail import SailELTBench
from .engine_impl.spark import SparkELTBench


class ELTBench(BaseBenchmark):
    """
    Class for running the ELTBench benchmark.

    The ELTBench benchmark is designed to evaluate end-to-end performance of engines in supporting typical Lakehouse architecture patterns. This includes bulk loading data, creation of star schema tables, incrementally merging data into tables, performing maintenance jobs, and running ad-hoc aggregation queries. Supported engines are listed in the `self.BENCHMARK_IMPL_REGISTRY` constant. ELTBench supports two modes: 'light' and 'full'. The 'light' mode represents a small workload, while the 'full' mode includes a larger scope of tests.

    Parameters
    ----------
    engine : BaseEngine
        The engine to use for executing the benchmark.
    scenario_name : str
        The name of the benchmark scenario.
    input_parquet_folder_uri : str, optional
        Path to the input parquet files. Must be the root directory containing a folder named after each table in TABLE_REGISTRY.
    result_table_uri : str, optional
        Table URI where results will be saved. Must be specified if `save_results` is True.
    save_results : bool, optional
        Whether to save the benchmark results. Results can also be accessed via the `self.results` attribute after running the benchmark.

    Methods
    -------
    run(mode='light')
        Runs the benchmark in the specified mode. Valid modes are 'light' and 'full'.
    run_light_mode()
        Executes the 'light' mode of the benchmark, including data loading, table creation, incremental merging, maintenance jobs, and ad-hoc queries.
    """

    BENCHMARK_IMPL_REGISTRY = {
        Spark: SparkELTBench,
        DuckDB: DuckDBELTBench,
        Daft: DaftELTBench,
        Polars: PolarsELTBench,
        Sail: SailELTBench,
    }
    MODE_REGISTRY = ["light"]
    TABLE_REGISTRY = [
        "call_center",
        "catalog_page",
        "catalog_returns",
        "catalog_sales",
        "customer",
        "customer_address",
        "customer_demographics",
        "date_dim",
        "household_demographics",
        "income_band",
        "inventory",
        "item",
        "promotion",
        "reason",
        "ship_mode",
        "store",
        "store_returns",
        "store_sales",
        "time_dim",
        "warehouse",
        "web_page",
        "web_returns",
        "web_sales",
        "web_site",
    ]
    VERSION = "1.0.0"

    def __init__(
        self,
        engine: BaseEngine,
        scenario_name: str,
        scale_factor: Optional[int] = None,
        input_parquet_folder_uri: Optional[str] = None,
        result_table_uri: Optional[str] = None,
        save_results: bool = False,
        run_id: Optional[str] = None,
    ):
        self.scale_factor = scale_factor
        super().__init__(engine, scenario_name, input_parquet_folder_uri, result_table_uri, save_results, run_id)
        for base_engine, benchmark_impl in self.BENCHMARK_IMPL_REGISTRY.items():
            if isinstance(engine, base_engine):
                self.benchmark_impl_class = benchmark_impl
                if self.benchmark_impl_class is None:
                    raise ValueError(
                        f"No benchmark implementation registered for engine type: {type(engine).__name__} "
                        f"in benchmark '{self.__class__.__name__}'."
                    )
                break
        else:
            raise ValueError(
                f"No benchmark implementation registered for engine type: {type(engine).__name__} "
                f"in benchmark '{self.__class__.__name__}'."
            )

        self.engine = engine
        self.scenario_name = scenario_name
        self.benchmark_impl = self.benchmark_impl_class(self.engine)
        self.input_parquet_folder_uri = input_parquet_folder_uri

    def run(self, mode: str = "light"):
        """
        Executes the benchmark in the specified mode.

        Parameters
        ----------
        mode : str, optional
            The mode in which to run the benchmark. Supported modes are:
            - 'light': Runs the benchmark in light mode.
            - 'full': Placeholder for full mode, which is not implemented yet.
        """

        if mode == "light":
            self.run_light_mode()
        elif mode == "full":
            raise NotImplementedError("Full mode is not implemented yet.")
        else:
            raise ValueError(f"Mode '{mode}' is not supported. Supported modes: {self.MODE_REGISTRY}.")

    def _prepare_schema(self, tables: list[str]):

        self.engine.create_schema_if_not_exists(drop_before_create=True)
        self.engine.create_external_location(self.input_parquet_folder_uri)

        self.DDL_FILE_NAME = TPCDS.DDL_FILE_NAME
        ddl, used_canonical = self._load_resource_with_fallback("ddl", self.DDL_FILE_NAME, benchmark_name="tpcds")
        from_dialect = "spark" if used_canonical else self.engine.SQLGLOT_DIALECT

        statements = [s for s in ddl.split(";") if len(s) > 7]
        for statement in statements:
            prepped_ddl = transpile_and_qualify_query(
                query=statement,
                from_dialect=from_dialect,
                to_dialect=self.engine.SQLGLOT_DIALECT,
                catalog=getattr(self.engine, "catalog_name", None),
                schema=getattr(self.engine, "schema_name", None),
            )
            table_name = get_table_name_from_ddl(prepped_ddl)
            # only create tables that are in the specified list
            if table_name in tables:
                self.engine._create_empty_table(table_name=table_name, ddl=prepped_ddl)

    def run_light_mode(self):
        """
        Executes the light mode benchmark workflow for processing and querying data.
        This method performs a series of operations on data tables, including loading data
        from parquet files into Delta tables, creating a fact table, merging data, optimizing
        the table, vacuuming the table, and running an ad-hoc query. The results are posted
        at the end of the workflow.

        Parameters
        ----------
        None
        """
        tables = ["store_sales", "date_dim", "store", "item", "customer"]

        self.mode = "light"
        if self.engine.SUPPORTS_SCHEMA_PREP:
            self._prepare_schema(tables=tables)

        for table_name in tables:
            with self.timer(phase="Read parquet, write delta (x5)", test_item=table_name, engine=self.engine) as tc:
                tc.execution_telemetry = self.engine.load_parquet_to_delta(
                    parquet_folder_uri=posixpath.join(self.input_parquet_folder_uri, f"{table_name}/"),
                    table_name=table_name,
                    table_is_precreated=True,
                    context_decorator=tc.context_decorator,
                )
        with self.timer(phase="Create fact table", test_item="total_sales_fact", engine=self.engine):
            self.benchmark_impl.create_total_sales_fact()

        for _ in range(3):
            with self.timer(phase="Merge 0.1% into fact table (3x)", test_item="total_sales_fact", engine=self.engine):
                self.benchmark_impl.merge_percent_into_total_sales_fact(0.001)

        with self.timer(phase="OPTIMIZE", test_item="total_sales_fact", engine=self.engine):
            self.engine.optimize_table("total_sales_fact")

        with self.timer(phase="VACUUM", test_item="total_sales_fact", engine=self.engine):
            self.engine.vacuum_table("total_sales_fact", retain_hours=0, retention_check=False)

        with self.timer(
            phase="Ad-hoc query (small result aggregation)", test_item="total_sales_fact", engine=self.engine
        ):
            self.benchmark_impl.query_total_sales_fact()

        self.post_results()
