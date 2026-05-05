from .._load_and_query import _LoadAndQuery

from ...engines.spark import Spark
from ...engines.duckdb import DuckDB
from ...engines.daft import Daft
from ...engines.polars import Polars
from ...engines.sail import Sail

class TPCDS(_LoadAndQuery):
    """
    Class for running the TPC-DS benchmark.

    This class provides functionality for running the TPC-DS benchmark, including loading data,
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
        Path to the input parquet files. Must be the root directory containing a folder named after 
        each table in TABLE_REGISTRY.
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
        Spark: None,
        DuckDB: None,
        Daft: None,
        Polars: None,
        Sail: None,
    }
    BENCHMARK_NAME = 'TPCDS'
    TABLE_REGISTRY = [
        'call_center', 'catalog_page', 'catalog_returns', 'catalog_sales',
        'customer', 'customer_address', 'customer_demographics', 'date_dim',
        'household_demographics', 'income_band', 'inventory', 'item',
        'promotion', 'reason', 'ship_mode', 'store', 'store_returns',
        'store_sales', 'time_dim', 'warehouse', 'web_page', 'web_returns',
        'web_sales', 'web_site'
    ]
    QUERY_REGISTRY = [
        'q1', 'q2', 'q3', 'q4', 'q5', 'q6', 'q7', 'q8', 'q9', 'q10',
        'q11', 'q12', 'q13', 'q14a', 'q14b', 'q15', 'q16', 'q17', 'q18', 'q19', 'q20',
        'q21', 'q22', 'q23a', 'q23b', 'q24a', 'q24b', 'q25', 'q26', 'q27', 'q28', 'q29', 'q30',
        'q31', 'q32', 'q33', 'q34', 'q35', 'q36', 'q37', 'q38', 'q39a', 'q39b', 'q40',
        'q41', 'q42', 'q43', 'q44', 'q45', 'q46', 'q47', 'q48', 'q49', 'q50',
        'q51', 'q52', 'q53', 'q54', 'q55', 'q56', 'q57', 'q58', 'q59', 'q60',
        'q61', 'q62', 'q63', 'q64', 'q65', 'q66', 'q67', 'q68', 'q69', 'q70',
        'q71', 'q72', 'q73', 'q74', 'q75', 'q76', 'q77', 'q78', 'q79', 'q80',
        'q81', 'q82', 'q83', 'q84', 'q85', 'q86', 'q87', 'q88', 'q89', 'q90',
        'q91', 'q92', 'q93', 'q94', 'q95', 'q96', 'q97', 'q98', 'q99'
    ]
    DDL_FILE_NAME = 'ddl_v3.2.0.sql'
    DDL_VARIANT_REGISTRY = {
        'partitioned': 'ddl_v3.2.0.partitioned.sql',
        'simple': 'ddl_v3.2.0.simple.sql',
    }
    VERSION = '3.2.0'