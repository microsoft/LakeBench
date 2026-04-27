from typing import Optional, List
from .._load_and_query import _LoadAndQuery

from ...engines.base import BaseEngine
from ...engines.spark import Spark
from ...engines.duckdb import DuckDB
from ...engines.daft import Daft
from ...engines.polars import Polars
from ...engines.sail import Sail
from ...engines.livy import Livy

from .engine_impl.spark import SparkClickBench
from .engine_impl.duckdb import DuckDBClickBench
from .engine_impl.sail import SailClickBench
from .engine_impl.polars import PolarsClickBench
from .engine_impl.daft import DaftClickBench

class ClickBench(_LoadAndQuery):
    """
    Class for running the ClickBench benchmark.

    This class provides functionality for running the ClickBench benchmark, including loading data,
    executing queries, and performing power tests. Supported engines are listed in the 
    `self.BENCHMARK_IMPL_REGISTRY` constant.

    Parameters
    ----------
    engine : object
        The engine to use for executing the benchmark.
    scenario_name : str
        The name of the benchmark scenario.
    query_list : list of str, optional
        List of queries to execute. Use '*' for all queries. If not specified, all queries will be run.
    input_parquet_folder_uri : str, optional
        Path to the input parquet files.
    result_table_uri : str, optional
        Table URI where results will be saved. Must be specified if `save_results` is True.
    save_results : bool
        Whether to save the benchmark results. Results can also be accessed via the `self.results` 
        attribute after running the benchmark.

    Methods
    -------
    run(mode='power_test')
        Runs the benchmark in the specified mode.
        Supported modes are:
            - 'load': Sequentially executes loading the 24 tables.
            - 'query': Sequentially executes the 99 queries.
            - 'power_test': Executes the load test followed by the query test.
    _run_load_test()
        Loads the data for the benchmark.
    _run_query_test()
        Executes the queries for the benchmark.
    _run_power_test()
        Runs both the load and query tests.
    """
    BENCHMARK_IMPL_REGISTRY = {
        Spark: SparkClickBench,
        DuckDB: DuckDBClickBench,
        Sail: SailClickBench,
        Livy: None,
        Polars: PolarsClickBench,
        Daft: DaftClickBench,
    }
    BENCHMARK_NAME = 'ClickBench'
    TABLE_REGISTRY = [
        'hits'
    ]
    QUERY_REGISTRY = [
        'q1', 'q2', 'q3', 'q4', 'q5', 'q6', 'q7', 'q8', 'q9', 'q10',
        'q11', 'q12', 'q13', 'q14', 'q15', 'q16', 'q17', 'q18', 'q19', 'q20',
        'q21', 'q22', 'q23', 'q24', 'q25', 'q26', 'q27', 'q28', 'q29', 'q30',
        'q31', 'q32', 'q33', 'q34', 'q35', 'q36', 'q37', 'q38', 'q39', 'q40',
        'q41', 'q42', 'q43'
    ]
    DDL_FILE_NAME = 'ddl.sql'
    VERSION = 'UNKNOWN'

    def __init__(
            self, 
            engine: BaseEngine, 
            scenario_name: str,
            query_list: Optional[List[str]] = None,
            input_parquet_folder_uri: Optional[str] = None,
            result_table_uri: Optional[str] = None,
            save_results: bool = False
        ):
        super().__init__(
            engine=engine, 
            scenario_name=scenario_name,
            scale_factor=None,
            query_list=query_list,
            input_parquet_folder_uri=input_parquet_folder_uri,
            result_table_uri=result_table_uri,
            save_results=save_results
        )