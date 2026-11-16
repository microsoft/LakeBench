import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from lakebench.benchmarks import TPCH, ELTBench
from lakebench.engines import Daft, DuckDB, Polars, Sail, Spark
from lakebench.engines.base import BaseEngine
from lakebench.engines.databricks_spark import DatabricksSpark
from lakebench.engines.databricks_sql_warehouse import DatabricksSQLWarehouse
from lakebench.engines.fabric_data_warehouse import FabricDataWarehouse
from tests.conftest import _uninitialized_engine


def _engine(path, consent=True):
    engine = _uninitialized_engine(BaseEngine)
    engine.schema_or_working_directory_uri = path
    engine.allow_destructive_reset = consent
    engine.fs = MagicMock()
    return engine


def test_default_create_preserves_existing_contents():
    engine = _engine("/disposable/lakebench", consent=False)
    engine.create_schema_if_not_exists()
    engine.fs.rm.assert_not_called()
    engine.fs.mkdir.assert_not_called()


def test_missing_directory_is_created_without_reset():
    engine = _engine("/disposable/lakebench", consent=False)
    engine.fs.exists.return_value = False
    engine.create_schema_if_not_exists()
    engine.fs.mkdir.assert_called_once_with("/disposable/lakebench")


def test_reset_requires_explicit_boolean_consent_before_filesystem_access():
    for consent in (False, None, "true", 1):
        engine = _engine("/disposable/lakebench", consent=consent)
        with pytest.raises(ValueError, match="allow_destructive_reset=True"):
            engine.create_schema_if_not_exists(drop_before_create=True)
        assert engine.fs.mock_calls == []


@pytest.mark.parametrize(
    "path",
    [
        None,
        "",
        " ",
        "/",
        ".",
        "..",
        "/tmp",
        "file:///tmp",
        "file:///",
        "file:/",
        "C:/",
        "C:\\",
        "C:relative",
        "//server/share",
        "/mnt/shared",
        "/Volumes/shared",
        "/lakehouse/default/Files",
        "/lakehouse/default/Tables",
        "/home/another-user",
        "s3://bucket",
        "s3://bucket/folder/..",
        "gs://bucket/",
        "abfss://container@account.dfs.core.windows.net/",
        "abfss://container@account.dfs.core.windows.net/../other/bench",
        "abfss://workspace@onelake.dfs.fabric.microsoft.com/lake.Lakehouse",
        "abfss://workspace@onelake.dfs.fabric.microsoft.com/lake.Lakehouse/Files",
        "https://onelake.dfs.fabric.microsoft.com/workspace/lake.Lakehouse/Tables",
        "/disposable/*",
        "s3://bucket/bench?token=secret",
        "unknown://bucket/bench",
        "dbfs:/user/hive/warehouse",
        "dbfs:/mnt/shared",
        "hdfs://cluster/user/hive/warehouse",
    ],
)
def test_broad_or_ambiguous_targets_are_refused_without_removal(path):
    engine = _engine(path)
    with pytest.raises(ValueError):
        engine.create_schema_if_not_exists(drop_before_create=True)
    engine.fs.rm.assert_not_called()


@pytest.mark.parametrize(
    ("target", "protected"),
    [
        ("/disposable/bench", "/disposable/bench/input"),
        ("/disposable/bench", "/disposable/bench"),
        ("/disposable/bench", "/disposable"),
        ("file:///disposable/bench", "/disposable/bench/results"),
        ("C:/bench/run", "c:\\bench\\run\\input"),
        ("s3://bucket/bench", "s3a://bucket/bench/results"),
        ("gs://bucket/bench", "gcs://bucket/bench/../bench/input"),
        (
            "abfss://container@account.dfs.core.windows.net/bench",
            "https://account.blob.core.windows.net/container/bench/results",
        ),
        (
            "wasbs://container@account.blob.core.windows.net/bench",
            "abfss://container@account.dfs.core.windows.net/bench/input",
        ),
        (
            "abfss://ws@onelake.dfs.fabric.microsoft.com/lake.Lakehouse/Files/bench",
            "https://onelake.dfs.fabric.microsoft.com/ws/lake.Lakehouse/Files/%62ench/input",
        ),
    ],
)
def test_overlaps_are_refused_in_both_directions(target, protected):
    engine = _engine(target)
    with pytest.raises(ValueError, match="overlap"):
        engine.create_schema_if_not_exists(drop_before_create=True, protected_uris=[protected])
    engine.fs.rm.assert_not_called()


def test_local_symlink_alias_cannot_hide_overlap(tmp_path):
    target = tmp_path / "bench"
    target.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(target, target_is_directory=True)
    engine = _engine(str(target))
    with pytest.raises(ValueError, match="overlap"):
        engine.create_schema_if_not_exists(drop_before_create=True, protected_uris=[str(alias / "input")])
    engine.fs.rm.assert_not_called()


def test_dedicated_directory_repeat_reset_preserves_siblings(tmp_path):
    from fsspec.implementations.local import LocalFileSystem

    target = tmp_path / "bench"
    target.mkdir()
    (target / "old").write_text("disposable")
    source = tmp_path / "input"
    source.mkdir()
    sentinel = source / "keep"
    sentinel.write_text("source")
    engine = _engine(str(target))
    engine.fs = LocalFileSystem()
    for _ in range(2):
        engine.create_schema_if_not_exists(drop_before_create=True, protected_uris=[str(source)])
        assert list(target.iterdir()) == []
        assert sentinel.read_text() == "source"


def test_file_uri_preserves_absolute_destination(monkeypatch, tmp_path):
    monkeypatch.setattr(BaseEngine, "_detect_runtime", lambda self: "local_unknown")
    engine = BaseEngine((tmp_path / "bench").as_uri(), allow_destructive_reset=True)
    assert engine.schema_or_working_directory_uri == str(tmp_path / "bench")
    engine.create_schema_if_not_exists(drop_before_create=True)
    assert (tmp_path / "bench").is_dir()


SCHEMA_ENGINES = (Spark, DatabricksSpark, DatabricksSQLWarehouse, FabricDataWarehouse)


def _schema_engine(engine_class, name="lakebench_test", consent=True):
    engine = _uninitialized_engine(engine_class)
    engine.allow_destructive_reset = consent
    engine.schema_name = name
    engine.catalog_name = "bench_catalog"
    engine.schema_uri = None
    engine.schema_or_working_directory_uri = None
    engine.full_catalog_schema_reference = f"`bench_catalog`.`{name}`"
    engine.unity_catalog = True
    engine.spark = MagicMock()
    engine.spark.sql.return_value.collect.return_value = []
    engine.execute_sql_statement = MagicMock()
    frame = MagicMock()
    frame.itertuples.return_value = iter([])
    engine.execute_sql_query = MagicMock(return_value=frame)
    engine._connection_engine = MagicMock()
    if engine_class is not FabricDataWarehouse:
        _metadata(engine, [], [("Location", "s3://bucket/bench")])
    return engine


@pytest.mark.parametrize("engine_class", SCHEMA_ENGINES)
@pytest.mark.parametrize("name", ["default", "dbo", "public", "main", "information_schema", "sys", "bad`name"])
def test_shared_and_malformed_schemas_cannot_be_reset(engine_class, name):
    engine = _schema_engine(engine_class, name)
    with pytest.raises(ValueError):
        engine.create_schema_if_not_exists(drop_before_create=True)
    engine.spark.sql.assert_not_called()
    engine.execute_sql_statement.assert_not_called()
    engine.execute_sql_query.assert_not_called()
    engine._connection_engine.connect.assert_not_called()


@pytest.mark.parametrize("engine_class", SCHEMA_ENGINES)
def test_schema_reset_requires_consent_before_any_sql(engine_class):
    engine = _schema_engine(engine_class, consent=False)
    with pytest.raises(ValueError, match="allow_destructive_reset=True"):
        engine.create_schema_if_not_exists(drop_before_create=True)
    engine.spark.sql.assert_not_called()
    engine.execute_sql_statement.assert_not_called()
    engine._connection_engine.connect.assert_not_called()


@pytest.mark.parametrize("engine_class", (Spark, DatabricksSpark, DatabricksSQLWarehouse))
def test_schema_default_create_never_drops(engine_class):
    engine = _schema_engine(engine_class, name="default", consent=False)
    engine.create_schema_if_not_exists()
    calls = engine.spark.sql.call_args_list + engine.execute_sql_statement.call_args_list
    assert any("CREATE SCHEMA IF NOT EXISTS" in call.args[0] for call in calls)
    assert not any("DROP " in call.args[0] for call in calls)


@pytest.mark.parametrize("engine_class", (Spark, DatabricksSpark, DatabricksSQLWarehouse))
def test_dedicated_schema_reset_still_drops_and_recreates(engine_class):
    engine = _schema_engine(engine_class)
    _metadata(engine, [("lakebench_test",)], [("Location", "s3://bucket/bench")])
    engine.create_schema_if_not_exists(drop_before_create=True)
    calls = engine.spark.sql.call_args_list + engine.execute_sql_statement.call_args_list
    sql = [call.args[0] for call in calls]
    assert any(statement.startswith("DROP SCHEMA IF EXISTS `bench_catalog`.`lakebench_test`") for statement in sql)
    assert any(statement.startswith("CREATE SCHEMA ") for statement in sql)
    assert not any("CREATE SCHEMA IF NOT EXISTS" in statement for statement in sql)


def _metadata(engine, schemas, details, tables=(), table_details=()):
    def rows_for(sql):
        if sql.startswith("SHOW SCHEMAS"):
            return schemas
        if sql.startswith("DESCRIBE SCHEMA"):
            return details
        if sql.startswith("SHOW TABLES"):
            return tables
        if sql.startswith("DESCRIBE TABLE"):
            return table_details
        return []

    if isinstance(engine, DatabricksSQLWarehouse):

        def query(sql, **kwargs):
            frame = MagicMock()
            frame.itertuples.side_effect = lambda **kwargs: iter(rows_for(sql))
            return frame

        engine.execute_sql_query.side_effect = query
    else:
        engine.spark.sql.side_effect = lambda sql: SimpleNamespace(collect=lambda: rows_for(sql))


@pytest.mark.parametrize("engine_class", (Spark, DatabricksSpark, DatabricksSQLWarehouse))
@pytest.mark.parametrize(
    ("location", "protected", "message"),
    [
        ("s3://bucket/", "s3://source/input", "root"),
        ("s3://bucket/bench", "s3://bucket/bench/results", "overlap"),
        (None, "s3://source/input", "Cannot verify"),
    ],
)
def test_existing_schema_location_is_verified_before_drop(engine_class, location, protected, message):
    engine = _schema_engine(engine_class)
    _metadata(engine, [("lakebench_test",)], [("Location", location)])
    with pytest.raises(ValueError, match=message):
        engine.create_schema_if_not_exists(drop_before_create=True, protected_uris=[protected])
    calls = engine.spark.sql.call_args_list + engine.execute_sql_statement.call_args_list
    assert not any("DROP " in call.args[0] for call in calls)


@pytest.mark.parametrize("engine_class", (Spark, DatabricksSpark, DatabricksSQLWarehouse))
def test_table_location_outside_schema_cannot_hide_overlap(engine_class):
    engine = _schema_engine(engine_class)
    _metadata(
        engine,
        [("lakebench_test",)],
        [("Location", "s3://bucket/bench")],
        [("lakebench_test", "relocated_table", False)],
        [("Location", "s3://source/input")],
    )
    with pytest.raises(ValueError, match="overlap"):
        engine.create_schema_if_not_exists(drop_before_create=True, protected_uris=["s3://source/input"])
    calls = engine.spark.sql.call_args_list + engine.execute_sql_statement.call_args_list
    assert not any("DROP " in call.args[0] for call in calls)


@pytest.mark.parametrize("engine_class", (Spark, DatabricksSpark, DatabricksSQLWarehouse))
def test_metadata_failure_never_falls_through_to_drop(engine_class):
    engine = _schema_engine(engine_class)
    if isinstance(engine, DatabricksSQLWarehouse):
        engine.execute_sql_query.side_effect = RuntimeError("metadata unavailable")
    else:
        engine.spark.sql.side_effect = RuntimeError("metadata unavailable")
    with pytest.raises(RuntimeError, match="metadata unavailable"):
        engine.create_schema_if_not_exists(drop_before_create=True)
    engine.execute_sql_statement.assert_not_called()
    assert not any("DROP " in call.args[0] for call in engine.spark.sql.call_args_list)


@pytest.mark.parametrize("engine_class", (Spark, DatabricksSpark, DatabricksSQLWarehouse))
def test_new_schema_uses_strict_create_without_an_unverified_drop(engine_class):
    engine = _schema_engine(engine_class)
    engine.create_schema_if_not_exists(drop_before_create=True)
    calls = engine.spark.sql.call_args_list + engine.execute_sql_statement.call_args_list
    sql = [call.args[0] for call in calls]
    assert not any("DROP " in statement for statement in sql)
    assert any(statement.startswith("CREATE SCHEMA `bench_catalog`.`lakebench_test`") for statement in sql)
    assert not any("CREATE SCHEMA IF NOT EXISTS" in statement for statement in sql)


@pytest.mark.parametrize("engine_class", (Spark, DatabricksSpark, DatabricksSQLWarehouse))
def test_new_schema_inherited_location_is_checked_before_table_preparation(engine_class):
    engine = _schema_engine(engine_class)
    _metadata(engine, [], [("Location", "s3://source/input")])
    with pytest.raises(ValueError, match="overlap"):
        engine.create_schema_if_not_exists(drop_before_create=True, protected_uris=["s3://source/input"])
    calls = engine.spark.sql.call_args_list + engine.execute_sql_statement.call_args_list
    sql = [call.args[0] for call in calls]
    assert any(statement.startswith("CREATE SCHEMA ") for statement in sql)
    assert not any(statement.startswith(("DROP ", "USE ", "CREATE TABLE")) for statement in sql)


@pytest.mark.parametrize("engine_class", (Spark, DatabricksSpark, DatabricksSQLWarehouse))
def test_existing_persisted_view_needs_no_physical_location(engine_class):
    engine = _schema_engine(engine_class)
    _metadata(
        engine,
        [("lakebench_test",)],
        [("Location", "s3://bucket/bench")],
        [("lakebench_test", "bench_view", False)],
        [("Type", "VIEW")],
    )
    engine.create_schema_if_not_exists(drop_before_create=True)
    calls = engine.spark.sql.call_args_list + engine.execute_sql_statement.call_args_list
    assert any("DROP SCHEMA" in call.args[0] for call in calls)


def test_fabric_reset_quotes_objects_and_preserves_create_only_mode(monkeypatch):
    monkeypatch.setitem(sys.modules, "sqlalchemy", SimpleNamespace(text=lambda sql: sql))
    engine = _schema_engine(FabricDataWarehouse)
    objects = MagicMock()
    objects.iterrows.return_value = iter(
        [
            (0, {"ObjectType": "USER_TABLE", "ObjectName": "table]name"}),
            (1, {"ObjectType": "VIEW", "ObjectName": "view"}),
        ]
    )
    engine.execute_sql_query.return_value = objects
    connection = engine._connection_engine.connect.return_value.execution_options.return_value.__enter__.return_value
    engine.create_schema_if_not_exists(drop_before_create=True)
    statements = [call.args[0] for call in connection.execute.call_args_list]
    assert statements[:3] == [
        "DROP TABLE IF EXISTS [lakebench_test].[table]]name]",
        "DROP VIEW IF EXISTS [lakebench_test].[view]",
        "DROP SCHEMA IF EXISTS [lakebench_test]",
    ]
    assert "IF SCHEMA_ID" in statements[3]
    connection.execute.reset_mock()
    engine.create_schema_if_not_exists()
    assert connection.execute.call_count == 1
    assert "DROP" not in connection.execute.call_args.args[0]


@pytest.mark.parametrize("engine_class", (DuckDB, Polars, Daft, Sail))
@pytest.mark.parametrize("benchmark_class", (TPCH, ELTBench))
def test_benchmark_preflight_refuses_before_load_or_ddl(engine_class, benchmark_class, tmp_path):
    engine = _uninitialized_engine(engine_class)
    engine.schema_or_working_directory_uri = str(tmp_path / "bench")
    engine.allow_destructive_reset = False
    engine.create_schema_if_not_exists = MagicMock()
    engine.load_parquet_to_delta = MagicMock()
    benchmark = benchmark_class(engine=engine, scenario_name="safe", input_folder_uri="s3://source/input")
    with pytest.raises(ValueError, match="allow_destructive_reset=True"):
        benchmark.run(mode="load" if benchmark_class is TPCH else "light")
    engine.create_schema_if_not_exists.assert_not_called()
    engine.load_parquet_to_delta.assert_not_called()


@pytest.mark.parametrize("engine_class", (DuckDB, Polars, Daft, Sail))
@pytest.mark.parametrize("benchmark_class", (TPCH, ELTBench))
@pytest.mark.parametrize("overlap_field", ("input_folder_uri", "result_table_uri"))
def test_benchmark_preflight_protects_input_and_results_even_without_saving(
    engine_class, benchmark_class, overlap_field, tmp_path
):
    engine = _uninitialized_engine(engine_class)
    engine.schema_or_working_directory_uri = str(tmp_path / "bench")
    engine.create_schema_if_not_exists = MagicMock()
    engine.load_parquet_to_delta = MagicMock()
    kwargs = {"input_folder_uri": "s3://source/input", overlap_field: (tmp_path / "bench" / "data").as_uri()}
    benchmark = benchmark_class(engine=engine, scenario_name="safe", save_results=False, **kwargs)
    with pytest.raises(ValueError, match="overlap"):
        benchmark.run(mode="load" if benchmark_class is TPCH else "light")
    engine.create_schema_if_not_exists.assert_not_called()
    engine.load_parquet_to_delta.assert_not_called()


@pytest.mark.parametrize("mode", ("query", "power_test"))
def test_query_only_runs_do_not_require_reset_consent(mode):
    engine = _uninitialized_engine(DuckDB)
    engine.allow_destructive_reset = False
    engine.validate_storage_reset = MagicMock(side_effect=AssertionError("query must not reset"))
    benchmark = TPCH(engine=engine, scenario_name="query", input_folder_uri="s3://source/input")
    benchmark._run_query_test = MagicMock()
    benchmark.run(mode=mode)
    benchmark._run_query_test.assert_called_once()
    engine.validate_storage_reset.assert_not_called()
