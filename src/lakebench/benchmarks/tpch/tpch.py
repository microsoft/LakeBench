from ...engines.daft import Daft
from ...engines.duckdb import DuckDB
from ...engines.polars import Polars
from ...engines.sail import Sail
from ...engines.spark import Spark
from .._load_and_query import _LoadAndQuery


class TPCH(_LoadAndQuery):
    """
    Class for running the TPC-H benchmark.

    This class provides functionality for running the TPC-H benchmark, including loading data,
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
        Runs the benchmark in the specified mode. Valid modes are 'load', 'query', and 'power_test'.
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
    BENCHMARK_NAME = "TPCH"
    TABLE_REGISTRY = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
    QUERY_REGISTRY = [
        "q1",
        "q2",
        "q3",
        "q4",
        "q5",
        "q6",
        "q7",
        "q8",
        "q9",
        "q10",
        "q11",
        "q12",
        "q13",
        "q14",
        "q15",
        "q16",
        "q17",
        "q18",
        "q19",
        "q20",
        "q21",
        "q22",
    ]
    DDL_FILE_NAME = "ddl_v3.0.1.sql"
    DDL_VARIANT_REGISTRY = {
        "partitioned": "ddl_v3.0.1.partitioned.sql",
        "clustered": "ddl_v3.0.1.clustered.sql",
        "simple": "ddl_v3.0.1.simple.sql",
    }
    VERSION = "3.0.1"
    ANALYZE_COLUMN_REGISTRY = {
        "customer": [
            "c_comment",
            "c_custkey",
            "c_name",
            "c_address",
            "c_nationkey",
            "c_phone",
            "c_acctbal",
            "c_mktsegment",
        ],
        "lineitem": [
            "l_orderkey",
            "l_partkey",
            "l_suppkey",
            "l_quantity",
            "l_extendedprice",
            "l_discount",
            "l_returnflag",
            "l_linestatus",
            "l_shipdate",
            "l_commitdate",
            "l_receiptdate",
            "l_shipinstruct",
            "l_shipmode",
        ],
        "nation": ["n_nationkey", "n_name", "n_regionkey"],
        "orders": [
            "o_orderkey",
            "o_custkey",
            "o_orderstatus",
            "o_totalprice",
            "o_orderdate",
            "o_orderpriority",
            "o_shippriority",
            "o_comment",
        ],
        "part": ["p_partkey", "p_name", "p_brand", "p_type", "p_size", "p_container"],
        "partsupp": ["ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost"],
        "region": ["r_regionkey", "r_name"],
        "supplier": ["s_comment", "s_suppkey", "s_name", "s_nationkey"],
    }
