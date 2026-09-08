from lakebench.benchmarks import TPCDS
from lakebench.engines.sail import Sail


class _TestSail(Sail):
    def __init__(self):
        self.version = "test"
        self.cost_per_vcore_hour = None
        self.cost_per_hour = None
        self.extended_engine_metadata = {}
        self.storage_options = {}
        self.schema_or_working_directory_uri = "/tmp/lakebench"
        self.runtime = "local_unknown"
        self.operating_system = "linux"


def test_sail_q12_uses_safe_denominator_override():
    benchmark = TPCDS(
        engine=_TestSail(),
        scenario_name="test",
        scale_factor=1,
        query_list=["q12"],
        input_parquet_folder_uri="/tmp/tpcds",
    )

    query = benchmark._return_query_definition("q12")

    assert "NULLIF" in query
    assert "SUM(SUM(ws_ext_sales_price))" in query
