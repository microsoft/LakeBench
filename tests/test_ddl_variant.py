import pytest
from unittest.mock import MagicMock, patch
from lakebench.benchmarks._load_and_query._load_and_query import _LoadAndQuery
from lakebench.engines.base import BaseEngine


class _StubEngine(BaseEngine):
    """Minimal engine stub for testing without optional deps."""
    SQLGLOT_DIALECT = "spark"
    SUPPORTS_SCHEMA_PREP = True
    SUPPORTS_MOUNT_PATH = True

    def __init__(self):
        self.version = "test"
        self.cost_per_vcore_hour = None
        self.cost_per_hour = None
        self.extended_engine_metadata = {}
        self.storage_options = {}
        self.schema_or_working_directory_uri = "/tmp/test"
        self.fs = None
        self.runtime = "local_unknown"
        self.operating_system = self._detect_os()


class _StubBenchmark(_LoadAndQuery):
    """Concrete _LoadAndQuery subclass for testing."""
    BENCHMARK_IMPL_REGISTRY = {
        _StubEngine: None,
    }
    BENCHMARK_NAME = 'StubBench'
    TABLE_REGISTRY = ['table_a', 'table_b']
    QUERY_REGISTRY = ['q1', 'q2']
    DDL_FILE_NAME = 'ddl_test.sql'
    DDL_VARIANT_REGISTRY = {
        'partitioned': 'ddl_test.partitioned.sql',
        'clustered': 'ddl_test.clustered.sql',
    }
    VERSION = '1.0'


def _make_benchmark(**kwargs):
    """Helper to construct a _StubBenchmark with defaults."""
    defaults = dict(
        engine=_StubEngine(),
        scenario_name='test_scenario',
        input_parquet_folder_uri='/tmp/parquet',
    )
    defaults.update(kwargs)
    return _StubBenchmark(**defaults)


class TestDdlVariantValidation:
    def test_default_no_variant(self):
        bench = _make_benchmark()
        assert bench._ddl_variant is None
        assert bench._ddl_override is None
        assert bench.engine.extended_engine_metadata['ddl_variant'] == 'default'

    def test_valid_variant(self):
        bench = _make_benchmark(ddl_variant='partitioned')
        assert bench._ddl_variant == 'partitioned'
        assert bench.engine.extended_engine_metadata['ddl_variant'] == 'partitioned'

    def test_invalid_variant_raises(self):
        with pytest.raises(ValueError, match="Unknown DDL variant 'nonexistent'"):
            _make_benchmark(ddl_variant='nonexistent')

    def test_ddl_override(self):
        ddl = "CREATE TABLE t (id INT);"
        bench = _make_benchmark(ddl_override=ddl)
        assert bench._ddl_override == ddl
        assert bench.engine.extended_engine_metadata['ddl_variant'] == 'custom'

    def test_variant_and_override_mutually_exclusive(self):
        with pytest.raises(ValueError, match="mutually exclusive"):
            _make_benchmark(
                ddl_variant='partitioned',
                ddl_override="CREATE TABLE t (id INT);"
            )

    def test_override_dialect_default(self):
        bench = _make_benchmark(ddl_override="CREATE TABLE t (id INT);")
        assert bench._ddl_override_dialect == 'spark'

    def test_override_dialect_custom(self):
        bench = _make_benchmark(
            ddl_override="CREATE TABLE t (id INT);",
            ddl_override_dialect='duckdb'
        )
        assert bench._ddl_override_dialect == 'duckdb'


class TestDdlVariantRegistry:
    def test_registry_on_subclass(self):
        assert 'partitioned' in _StubBenchmark.DDL_VARIANT_REGISTRY
        assert _StubBenchmark.DDL_VARIANT_REGISTRY['partitioned'] == 'ddl_test.partitioned.sql'

    def test_empty_registry_by_default(self):
        assert _LoadAndQuery.DDL_VARIANT_REGISTRY == {}


class TestPrepareSchemaWithOverride:
    def test_ddl_override_used_directly(self):
        """When ddl_override is set, _prepare_schema should use the raw DDL string."""
        ddl = "CREATE TABLE my_table (id BIGINT NOT NULL)"
        bench = _make_benchmark(ddl_override=ddl)

        bench.engine.create_schema_if_not_exists = MagicMock()
        bench.engine.create_external_location = MagicMock()
        bench.engine._create_empty_table = MagicMock()

        bench._prepare_schema()

        bench.engine._create_empty_table.assert_called_once()
        call_args = bench.engine._create_empty_table.call_args
        assert call_args[1]['table_name'] == 'my_table'

    def test_ddl_override_with_multiple_statements(self):
        """ddl_override with multiple semicolon-separated statements."""
        ddl = "CREATE TABLE t1 (id INT); CREATE TABLE t2 (id INT);"
        bench = _make_benchmark(ddl_override=ddl)

        bench.engine.create_schema_if_not_exists = MagicMock()
        bench.engine.create_external_location = MagicMock()
        bench.engine._create_empty_table = MagicMock()

        bench._prepare_schema()

        assert bench.engine._create_empty_table.call_count == 2


class TestBackwardCompatibility:
    def test_no_variant_params_same_as_before(self):
        """Construction without ddl_variant/ddl_override should work identically."""
        engine = _StubEngine()
        bench = _StubBenchmark(
            engine=engine,
            scenario_name='compat_test',
            input_parquet_folder_uri='/tmp/data',
        )
        assert bench._ddl_variant is None
        assert bench._ddl_override is None
        assert bench.engine.extended_engine_metadata['ddl_variant'] == 'default'

    def test_variant_error_message_lists_available(self):
        """Error for unknown variant should list available options."""
        with pytest.raises(ValueError) as exc_info:
            _make_benchmark(ddl_variant='bad')
        error_msg = str(exc_info.value)
        assert 'partitioned' in error_msg
        assert 'clustered' in error_msg


class TestOptimizeAnalyzeFlags:
    def test_defaults_false(self):
        bench = _make_benchmark()
        assert bench.optimize is False
        assert bench.analyze is False

    def test_optimize_flag_stored(self):
        bench = _make_benchmark(optimize=True)
        assert bench.optimize is True
        assert bench.engine.extended_engine_metadata['optimize'] == 'True'

    def test_analyze_flag_stored(self):
        bench = _make_benchmark(analyze=True)
        assert bench.analyze is True
        assert bench.engine.extended_engine_metadata['analyze'] == 'True'

    def test_both_flags_stored(self):
        bench = _make_benchmark(optimize=True, analyze=True)
        assert bench.optimize is True
        assert bench.analyze is True
        assert bench.engine.extended_engine_metadata['optimize'] == 'True'
        assert bench.engine.extended_engine_metadata['analyze'] == 'True'

    def test_metadata_records_false(self):
        bench = _make_benchmark(optimize=False, analyze=False)
        assert bench.engine.extended_engine_metadata['optimize'] == 'False'
        assert bench.engine.extended_engine_metadata['analyze'] == 'False'
