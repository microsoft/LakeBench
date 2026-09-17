"""Unit coverage for the Fabric Data Warehouse engine.

A live warehouse needs ODBC Driver 18 and a Fabric identity, so these tests drive
the SQL-building and error-classification logic against an uninitialized engine.
"""

import inspect
from pathlib import Path

import pytest
from packaging.requirements import Requirement

from lakebench.benchmarks import TPCDS, TPCH, ClickBench, ELTBench
from lakebench.benchmarks.clickbench.engine_impl import fabric_data_warehouse as clickbench_impl
from lakebench.engines.fabric_data_warehouse import FabricDataWarehouse, FabricDataWarehouseQueryCancelledError
from lakebench.utils.schema_utils import table_schemas_from_ddl
from tests.conftest import _uninitialized_engine


@pytest.fixture
def engine():
    engine = _uninitialized_engine(FabricDataWarehouse)
    engine.schema_name = "dbo"
    return engine


@pytest.fixture
def project_config():
    tomllib = pytest.importorskip("tomllib")
    pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
    return tomllib.loads(pyproject.read_text(encoding="utf-8"))


class _RecordingEngine(FabricDataWarehouse):
    """Captures the SQL an operation would submit instead of executing it."""

    def __init__(self):
        self.executed = []
        self.schema_name = "dbo"
        self.run_analyze_after_load = False
        #: Frame handed back to the single ``return_data=True`` probe an operation makes.
        self.queried_data = None

    def execute_sql_query(self, query, context_decorator=None, return_data=False):
        self.executed.append(query)
        if return_data:
            return self.queried_data
        return {"statement_id": "", "distributed_statement_id": ""}


class _ColumnValues(list):
    def tolist(self):
        return list(self)


class _ColumnFrame:
    """Stands in for the pandas frame the ODBC cursor would produce."""

    def __init__(self, columns):
        self.columns = list(columns)

    def __getitem__(self, key):
        assert key == "COLUMN_NAME"
        return _ColumnValues(self.columns)


def _tpcds_native_columns(table_name):
    benchmark = TPCDS(
        engine=_uninitialized_engine(FabricDataWarehouse),
        scenario_name="native",
        scale_factor=1000,
        input_folder_uri="abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse/Files/tpcds",
        input_format="native",
    )
    return benchmark._native_table_columns()[table_name]


def test_engine_is_registered_for_every_benchmark():
    for benchmark_class in (TPCH, TPCDS, ClickBench, ELTBench):
        assert FabricDataWarehouse in benchmark_class.BENCHMARK_IMPL_REGISTRY, benchmark_class.__name__


def test_the_former_name_is_still_importable_as_an_alias():
    """``FabricWarehouse`` was the pre-rename name; keep it resolving to the same class."""
    from lakebench.engines import FabricWarehouse as exported_alias
    from lakebench.engines.fabric_data_warehouse import (
        FabricWarehouse,
        FabricWarehouseQueryCancelledError,
    )

    assert FabricWarehouse is FabricDataWarehouse
    assert exported_alias is FabricDataWarehouse
    assert FabricWarehouseQueryCancelledError is FabricDataWarehouseQueryCancelledError
    # Registries are keyed by class object, so the alias resolves through them too.
    assert ClickBench.BENCHMARK_IMPL_REGISTRY[FabricWarehouse] is not None


@pytest.mark.parametrize("extra_name", ["fabric_warehouse", "fabricwarehouse"])
def test_the_former_extra_name_still_installs_the_engine(project_config, extra_name):
    extras = project_config["project"]["optional-dependencies"]
    assert extras[extra_name] == ["lakebench[fabric_data_warehouse]"]
    assert any(requirement.startswith("pyodbc") for requirement in extras["fabric_data_warehouse"])


@pytest.mark.parametrize(
    "python_version, available",
    [("3.8", False), ("3.9", False), ("3.10", True), ("3.11", True)],
)
def test_extra_gates_delta_on_supported_python(project_config, python_version, available):
    requirements = {
        requirement.name: requirement
        for requirement in map(Requirement, project_config["project"]["optional-dependencies"]["fabric_data_warehouse"])
    }
    requirement = requirements["deltalake"]
    assert str(requirement.specifier) == "==1.5.1"
    assert requirement.marker is not None
    assert requirement.marker.evaluate({"python_version": python_version}) is available


def test_extra_reuses_the_core_pyarrow_floor(project_config):
    extra = project_config["project"]["optional-dependencies"]["fabric_data_warehouse"]
    assert "pyarrow" not in {requirement.name for requirement in map(Requirement, extra)}
    arrow = next(req for req in map(Requirement, project_config["project"]["dependencies"]) if req.name == "pyarrow")
    assert "14.0.2" in arrow.specifier


def test_extra_requires_pandas_compatible_with_sqlalchemy_2(project_config):
    extra = project_config["project"]["optional-dependencies"]["fabric_data_warehouse"]
    pandas = next(req for req in map(Requirement, extra) if req.name == "pandas")
    assert "1.3.0" not in pandas.specifier
    assert "1.4.0" in pandas.specifier
    assert "2.0.0" in pandas.specifier


@pytest.mark.parametrize("extra_name", ["fabric_data_warehouse", "fabric_warehouse", "fabricwarehouse"])
def test_extra_and_aliases_can_share_daft_delta_dependency(project_config, extra_name):
    conflicts = [{entry["extra"] for entry in conflict} for conflict in project_config["tool"]["uv"]["conflicts"]]
    assert {"daft", extra_name} not in conflicts
    extras = project_config["project"]["optional-dependencies"]
    for extra in ("fabric_data_warehouse", "daft"):
        requirement = next(req for req in map(Requirement, extras[extra]) if req.name == "deltalake")
        assert "1.5.1" in requirement.specifier


@pytest.mark.parametrize("extra_name", ["fabric_data_warehouse", "fabric_warehouse", "fabricwarehouse"])
@pytest.mark.parametrize("other_extra", ["duckdb", "polars", "sail"])
def test_extra_and_aliases_declare_delta_version_conflicts(project_config, extra_name, other_extra):
    conflicts = [{entry["extra"] for entry in conflict} for conflict in project_config["tool"]["uv"]["conflicts"]]
    assert {extra_name, other_extra} in conflicts


def test_engine_declares_its_platform_capabilities():
    assert FabricDataWarehouse.SQLGLOT_DIALECT == "fabric"
    # The warehouse reads from OneLake over https and cannot see notebook mounts.
    assert FabricDataWarehouse.SUPPORTS_ONELAKE is True
    assert FabricDataWarehouse.SUPPORTS_MOUNT_PATH is False
    # COPY INTO for CSV has no AUTO_CREATE_TABLE, so the DDL must run first.
    assert FabricDataWarehouse.SUPPORTS_SCHEMA_PREP is True


def test_job_cost_is_not_reported():
    # Capacity cost does not attribute to an individual query or load, so the
    # engine deliberately publishes no hourly rate and leaves the cost unset.
    assert "cost_per_hour" not in inspect.signature(FabricDataWarehouse.__init__).parameters
    assert not hasattr(FabricDataWarehouse, "_get_cost_per_hour")

    engine = _uninitialized_engine(FabricDataWarehouse)
    assert engine.cost_per_hour is None
    assert engine.cost_per_vcore_hour is None
    assert engine.get_job_cost(3_600_000) is None


def test_return_data_uses_executable_sql_for_older_pandas(engine, monkeypatch):
    pd = pytest.importorskip("pandas")
    sa = pytest.importorskip("sqlalchemy")
    read_sql_query = pd.read_sql_query
    query = "SELECT 1 AS c1, 'test value' AS c2"

    def read_executable_sql(statement, connection):
        assert isinstance(statement, sa.sql.elements.TextClause)
        assert str(statement) == query
        return read_sql_query(statement, connection)

    monkeypatch.setattr(pd, "read_sql_query", read_executable_sql)
    engine._connection_engine = sa.create_engine("sqlite://")
    try:
        result = engine.execute_sql_query(query, return_data=True)
        assert result.to_dict("records") == [{"c1": 1, "c2": "test value"}]
    finally:
        engine._connection_engine.dispose()


@pytest.mark.parametrize(
    "messages",
    [
        [("HY008", "Operation canceled")],
        [("42000", "[Microsoft][ODBC Driver 18] Query was cancelled by user")],
        [("42000", "3617", "canceled")],
        [("42000", "[Microsoft] Error 3617: the operation ended")],
        ["The client has disconnected"],
    ],
)
def test_cancellation_is_detected_from_sql_state_code_or_message(messages):
    assert FabricDataWarehouse._is_cancellation(messages) is True
    with pytest.raises(FabricDataWarehouseQueryCancelledError):
        FabricDataWarehouse._raise_if_cancelled(messages)


@pytest.mark.parametrize(
    "messages",
    [
        [],
        [("22003", "Arithmetic overflow error converting expression to data type int.")],
        [("42S02", "Invalid object name 'dbo.missing'.")],
        # A row count that merely contains the digits of the cancellation code.
        [("01000", "36170 rows affected")],
    ],
)
def test_ordinary_errors_are_not_mistaken_for_cancellation(messages):
    assert FabricDataWarehouse._is_cancellation(messages) is False
    FabricDataWarehouse._raise_if_cancelled(messages)


def test_identifier_quoting_escapes_a_closing_bracket():
    assert FabricDataWarehouse._quote_identifier("normal") == "[normal]"
    assert FabricDataWarehouse._quote_identifier("we[ir]d") == "[we[ir]]d]"


def test_odbc_messages_are_flattened_for_reporting():
    assert FabricDataWarehouse._format_odbc_messages([("HY008", "canceled"), "plain"]) == "HY008 canceled\nplain"


def test_analyze_table_creates_one_full_scan_statistic_per_column():
    engine = _RecordingEngine()
    engine.analyze_table("store_sales", ["ss_item_sk", "ss_ticket_number"])
    assert engine.executed == [
        "CREATE STATISTICS [lakebench_store_sales_ss_item_sk] ON [dbo].[store_sales] ([ss_item_sk]) WITH FULLSCAN",
        "CREATE STATISTICS [lakebench_store_sales_ss_ticket_number] ON [dbo].[store_sales] "
        "([ss_ticket_number]) WITH FULLSCAN",
    ]


def test_analyze_table_without_columns_discovers_them_from_information_schema():
    engine = _RecordingEngine()
    engine.queried_data = _ColumnFrame(["ss_item_sk", "ss_ticket_number"])

    telemetry = engine.analyze_table("store_sales")

    discovery_query = engine.executed[0]
    assert "INFORMATION_SCHEMA.COLUMNS" in discovery_query
    assert "TABLE_SCHEMA = 'dbo'" in discovery_query
    assert "TABLE_NAME = 'store_sales'" in discovery_query
    # Statistics follow the table's declared column order, not a set iteration order.
    assert "ORDER BY ORDINAL_POSITION" in discovery_query
    assert engine.executed[1:] == [
        "CREATE STATISTICS [lakebench_store_sales_ss_item_sk] ON [dbo].[store_sales] ([ss_item_sk]) WITH FULLSCAN",
        "CREATE STATISTICS [lakebench_store_sales_ss_ticket_number] ON [dbo].[store_sales] "
        "([ss_ticket_number]) WITH FULLSCAN",
    ]
    assert telemetry == {"statistics_mode": "full", "statistics_created": "2"}


def test_analyze_table_rejects_a_bare_string_of_columns():
    """A string is iterable, so accepting one would analyze each character."""
    with pytest.raises(TypeError, match="sequence of column names"):
        _RecordingEngine().analyze_table("store_sales", "ss_item_sk")


def test_analyze_table_requires_at_least_one_column():
    with pytest.raises(ValueError, match="No columns"):
        _RecordingEngine().analyze_table("store_sales", [])


def test_optimize_table_is_reported_as_service_managed():
    with pytest.raises(NotImplementedError, match="handled automatically"):
        _RecordingEngine().optimize_table("store_sales")


def test_parquet_load_projects_legacy_column_names_onto_canonical_ones():
    """A legacy-named source is realigned by an explicit projection, not by COPY INTO."""
    engine = _RecordingEngine()
    engine.queried_data = _ColumnFrame(["s_store_sk", "s_tax_precentage"])

    engine.load_parquet_to_delta(
        "abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse/Files/store",
        "store",
        table_is_precreated=True,
        column_name_mapping={"s_tax_precentage": "s_tax_percentage"},
    )

    load_sql = engine.executed[1]
    assert load_sql.startswith("INSERT INTO dbo.store ")
    assert "[s_tax_precentage] AS [s_tax_percentage]" in load_sql
    # Unmapped columns are still projected explicitly to hold the positional order.
    assert "[s_store_sk]" in load_sql
    # COPY INTO maps fields positionally, so a rename has to go through OPENROWSET.
    assert "COPY INTO" not in load_sql


def test_parquet_load_without_a_mapping_uses_copy_into():
    engine = _RecordingEngine()

    engine.load_parquet_to_delta(
        "abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse/Files/store",
        "store",
        table_is_precreated=True,
    )

    assert len(engine.executed) == 1
    assert "COPY INTO dbo.store" in engine.executed[0]
    assert "FILE_TYPE = 'PARQUET'" in engine.executed[0]


def test_native_load_folds_the_trailing_delimiter_into_the_row_terminator():
    engine = _RecordingEngine()
    engine.load_delimited_to_delta(
        folder_uri="abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse/Files/tpcds/store/",
        table_name="store",
        columns=_tpcds_native_columns("store"),
        file_pattern="*.dat",
        table_is_precreated=True,
    )
    (sql,) = engine.executed
    assert "COPY INTO dbo.store" in sql
    assert "FROM 'https://onelake.dfs.fabric.microsoft.com/ws/lh.Lakehouse/Files/tpcds/store/*.dat'" in sql
    assert "FILE_TYPE = 'CSV'" in sql
    assert "FIELDTERMINATOR = '|'" in sql
    # 0x7C is the trailing pipe and 0x0A the line feed; a literal '\n' would be
    # widened to '\r\n' and a single-character terminator would leave an extra field.
    assert "ROWTERMINATOR = '0x7C0A'" in sql
    assert "FIRSTROW = 1" in sql
    assert "ENCODING = 'UTF8'" in sql


def test_native_load_requires_the_table_to_be_created_from_the_ddl():
    engine = _RecordingEngine()
    with pytest.raises(ValueError, match="created from the benchmark DDL"):
        engine.load_delimited_to_delta(
            folder_uri="abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse/Files/tpcds/store/",
            table_name="store",
            columns=_tpcds_native_columns("store"),
            file_pattern="*.dat",
            table_is_precreated=False,
        )
    assert engine.executed == []


def test_native_load_refuses_a_rename_copy_into_cannot_express():
    """COPY INTO maps fields positionally, so a legacy-named source would load silently wrong."""
    engine = _RecordingEngine()
    columns = [
        ("s_tax_precentage" if name == "s_tax_percentage" else name, kind)
        for name, kind in _tpcds_native_columns("store")
    ]
    with pytest.raises(ValueError, match="legacy column names"):
        engine.load_delimited_to_delta(
            folder_uri="abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse/Files/tpcds/store/",
            table_name="store",
            columns=columns,
            file_pattern="*.dat",
            table_is_precreated=True,
            column_name_mapping=TPCDS.COLUMN_NAME_MAPPING_REGISTRY["store"],
        )
    assert engine.executed == []


@pytest.mark.parametrize("benchmark_class", [TPCDS, TPCH])
def test_every_native_table_builds_a_copy_statement(benchmark_class):
    engine = _RecordingEngine()
    benchmark = benchmark_class(
        engine=_uninitialized_engine(FabricDataWarehouse),
        scenario_name="native",
        scale_factor=1000,
        input_folder_uri="abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse/Files/data",
        input_format="native",
    )
    for table_name, columns in benchmark._native_table_columns().items():
        engine.load_delimited_to_delta(
            folder_uri=f"abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse/Files/data/{table_name}/",
            table_name=table_name,
            columns=columns,
            file_pattern=benchmark.NATIVE_FILE_GLOB or f"*.{benchmark.NATIVE_FILE_EXTENSION}",
            table_is_precreated=True,
            column_name_mapping=benchmark.COLUMN_NAME_MAPPING_REGISTRY.get(table_name),
        )
    assert len(engine.executed) == len(benchmark.TABLE_REGISTRY)


def test_clickbench_load_projection_matches_the_engine_ddl_column_order():
    """The projection feeds a positional INSERT, so any drift silently shifts columns."""
    benchmark = ClickBench(
        engine=_uninitialized_engine(FabricDataWarehouse),
        scenario_name="ddl",
        input_parquet_folder_uri="abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse/Files/hits",
    )
    ddl, from_dialect = benchmark._resolve_ddl()
    ddl_columns = [name for name, _ in table_schemas_from_ddl(ddl, dialect=from_dialect)["hits"]]
    projection = [clickbench_impl._projection(column) for column in clickbench_impl._COLUMNS]
    assert [column.split(" AS ")[-1].lower() for column in projection] == ddl_columns
    assert len(ddl_columns) == len(set(ddl_columns)) == 105
    # Every projected column is either passed through or rebased/bounded, never dropped.
    assert sum(1 for column in projection if column.startswith("CAST(")) == len(clickbench_impl._VARCHAR_COLUMNS)
    assert sum(1 for column in projection if column.startswith("DATEADD(")) == len(
        clickbench_impl._SECOND_EPOCH_COLUMNS
    ) + len(clickbench_impl._DAY_EPOCH_COLUMNS)


def test_clickbench_event_time_columns_keep_their_date_component():
    """TIME(0) would silently truncate the epoch offsets these columns are built from."""
    benchmark = ClickBench(
        engine=_uninitialized_engine(FabricDataWarehouse),
        scenario_name="ddl",
        input_parquet_folder_uri="abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse/Files/hits",
    )
    ddl, from_dialect = benchmark._resolve_ddl()
    schema = dict(table_schemas_from_ddl(ddl, dialect=from_dialect)["hits"])
    for column in ("eventtime", "clienteventtime", "localeventtime"):
        assert schema[column].sql(dialect="fabric").upper().startswith("DATETIME2"), column
