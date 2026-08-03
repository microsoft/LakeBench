import pytest

from lakebench.engines.base import BaseEngine


class _MinimalEngine(BaseEngine):
    """Minimal concrete subclass for testing BaseEngine without optional deps."""

    def __init__(self):
        # Skip full __init__ to avoid cloud/runtime side-effects;
        # initialise only the attributes we test.
        self.version = "test"
        self.cost_per_vcore_hour = None
        self.cost_per_hour = None
        self.extended_engine_metadata = {}
        self.storage_options = {}
        self.schema_or_working_directory_uri = None
        self.fs = None
        self.runtime = "local_unknown"
        self.operating_system = self._detect_os()


class TestDetectOs:
    def test_returns_string(self):
        engine = _MinimalEngine()
        result = engine._detect_os()
        assert isinstance(result, str)

    def test_returns_known_os(self):
        engine = _MinimalEngine()
        result = engine._detect_os()
        assert result in ("windows", "linux", "mac", "unknown")


class TestGetTotalCores:
    def test_returns_positive_int(self):
        engine = _MinimalEngine()
        cores = engine.get_total_cores()
        assert isinstance(cores, int)
        assert cores > 0


class TestGetComputeSize:
    def test_format(self):
        engine = _MinimalEngine()
        size = engine.get_compute_size()
        assert isinstance(size, str)
        assert "vCore" in size

    def test_matches_core_count(self):
        engine = _MinimalEngine()
        cores = engine.get_total_cores()
        assert engine.get_compute_size() == f"{cores}vCore"


class TestGetJobCost:
    def test_returns_none_when_no_cost_set(self):
        engine = _MinimalEngine()
        assert engine.get_job_cost(60000) is None

    def test_calculates_cost_with_per_hour(self):
        engine = _MinimalEngine()
        engine.cost_per_hour = 1.0  # $1/hour
        cost = engine.get_job_cost(3600000)  # 1 hour in ms
        assert cost is not None
        assert float(cost) == pytest.approx(1.0, rel=1e-6)

    def test_calculates_cost_with_per_vcore_hour(self):
        engine = _MinimalEngine()
        engine.cost_per_vcore_hour = 0.1
        cost = engine.get_job_cost(3600000)  # 1 hour
        expected = engine.get_total_cores() * 0.1
        assert cost is not None
        assert float(cost) == pytest.approx(expected, rel=1e-6)


class TestAnalyzeTable:
    def test_raises_not_implemented(self):
        engine = _MinimalEngine()
        with pytest.raises(NotImplementedError, match="does not support analyze_table"):
            engine.analyze_table("some_table")

    def test_raises_not_implemented_with_selected_columns(self):
        engine = _MinimalEngine()
        with pytest.raises(NotImplementedError, match="does not support analyze_table"):
            engine.analyze_table("some_table", columns=["id"])


class _MockSpark:
    """Stub for Spark engine that captures executed DDL without requiring PySpark."""

    def __init__(self):
        self.executed_statements = []
        # Minimal Spark engine attributes
        self.version = "test"
        self.cost_per_vcore_hour = None
        self.cost_per_hour = None
        self.extended_engine_metadata = {}
        self.storage_options = {}
        self.schema_or_working_directory_uri = None
        self.fs = None
        self.runtime = "local_unknown"

    def execute_sql_statement(self, ddl):
        self.executed_statements.append(ddl)


def _make_spark_engine():
    """Create a Spark engine instance that captures DDL without PySpark."""
    from lakebench.engines.spark import Spark

    engine = object.__new__(Spark)
    engine.executed_statements = []
    engine.version = "test"
    engine.cost_per_vcore_hour = None
    engine.cost_per_hour = None
    engine.extended_engine_metadata = {}
    engine.storage_options = {}
    engine.schema_or_working_directory_uri = None
    engine.fs = None
    engine.runtime = "local_unknown"
    engine.operating_system = engine._detect_os()
    engine.tblproperties = {}

    original_execute = (
        engine.execute_sql_statement.__func__ if hasattr(engine.execute_sql_statement, "__func__") else None
    )
    engine.execute_sql_statement = lambda ddl: engine.executed_statements.append(ddl)
    return engine


class TestSparkCreateEmptyTableUsingDeltaInjection:
    """Tests for USING delta injection in Spark._create_empty_table."""

    def test_simple_ddl_gets_using_delta(self):
        engine = _make_spark_engine()
        ddl = "CREATE TABLE customer (c_custkey BIGINT NOT NULL, c_name VARCHAR(25) NOT NULL)"
        engine._create_empty_table(table_name="customer", ddl=ddl)
        result = engine.executed_statements[0].lower()
        assert "using delta" in result
        assert "customer" in result

    def test_cluster_by_using_delta_before_cluster(self):
        engine = _make_spark_engine()
        ddl = "CREATE TABLE lineitem (l_orderkey BIGINT NOT NULL, l_shipdate DATE NOT NULL) CLUSTER BY (l_shipdate)"
        engine._create_empty_table(table_name="lineitem", ddl=ddl)
        result = engine.executed_statements[0].lower()
        assert "using delta" in result
        assert "cluster by" in result
        using_pos = result.index("using delta")
        cluster_pos = result.index("cluster by")
        assert using_pos < cluster_pos, "USING delta must appear before CLUSTER BY"

    def test_partitioned_by_using_delta_before_partition(self):
        engine = _make_spark_engine()
        ddl = "CREATE TABLE orders (o_orderkey BIGINT NOT NULL, o_orderdate DATE NOT NULL) PARTITIONED BY (o_orderdate)"
        engine._create_empty_table(table_name="orders", ddl=ddl)
        result = engine.executed_statements[0].lower()
        assert "using delta" in result
        assert "partitioned by" in result
        using_pos = result.index("using delta")
        partition_pos = result.index("partitioned by")
        assert using_pos < partition_pos, "USING delta must appear before PARTITIONED BY"

    def test_existing_using_clause_not_duplicated(self):
        engine = _make_spark_engine()
        ddl = "CREATE TABLE t (id INT) USING delta CLUSTER BY (id)"
        engine._create_empty_table(table_name="t", ddl=ddl)
        result = engine.executed_statements[0].lower()
        assert result.count("using") == 1, "Should not add a second USING clause"

    def test_existing_using_parquet_preserved(self):
        engine = _make_spark_engine()
        ddl = "CREATE TABLE t (id INT) USING parquet"
        engine._create_empty_table(table_name="t", ddl=ddl)
        result = engine.executed_statements[0].lower()
        assert "using parquet" in result
        assert "using delta" not in result

    def test_tblproperties_are_injected_after_using_delta(self):
        engine = _make_spark_engine()
        engine.tblproperties = {
            "delta.enableDeletionVectors": "false",
            "delta.targetFileSize": "134217728",
        }

        engine._create_empty_table(table_name="t", ddl="CREATE TABLE t (id INT)")

        result = engine.executed_statements[0]
        assert "USING DELTA" in result
        assert "TBLPROPERTIES" in result
        assert "'delta.enableDeletionVectors'='false'" in result.replace(" ", "")
        assert "'delta.targetFileSize'='134217728'" in result.replace(" ", "")
        assert result.index("USING DELTA") < result.index("TBLPROPERTIES")


class TestSparkAnalyzeTable:
    def test_full_analysis_uses_all_columns(self):
        engine = _make_spark_engine()
        engine.full_catalog_schema_reference = "`catalog`.`schema`"
        engine.spark = type("SparkSessionStub", (), {"sql": engine.executed_statements.append})()

        engine.analyze_table("customer")

        assert engine.executed_statements == [
            "ANALYZE TABLE `catalog`.`schema`.customer COMPUTE STATISTICS FOR ALL COLUMNS"
        ]

    def test_selective_analysis_uses_requested_columns(self):
        engine = _make_spark_engine()
        engine.full_catalog_schema_reference = "`catalog`.`schema`"
        engine.spark = type("SparkSessionStub", (), {"sql": engine.executed_statements.append})()

        engine.analyze_table("customer", columns=["c_custkey", "c_nationkey"])

        assert engine.executed_statements == [
            "ANALYZE TABLE `catalog`.`schema`.customer COMPUTE STATISTICS FOR COLUMNS `c_custkey`, `c_nationkey`"
        ]

    def test_selective_analysis_rejects_empty_columns(self):
        engine = _make_spark_engine()

        with pytest.raises(ValueError, match="At least one column"):
            engine.analyze_table("customer", columns=[])

    def test_selective_analysis_rejects_string_columns(self):
        engine = _make_spark_engine()

        with pytest.raises(TypeError, match="not a string"):
            engine.analyze_table("customer", columns="c_custkey")
