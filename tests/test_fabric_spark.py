import pytest

from lakebench.engines.fabric_spark import FabricSpark


class _SparkConf:
    def __init__(self, values=None):
        self.initial_values = dict(values or {})
        self.values = {}

    def set(self, name, value):
        self.values[name] = value

    def get(self, name, default=None):
        return self.initial_values.get(name, default)


class _Spark:
    def __init__(self, conf_values=None, sql_error=None):
        self.conf = _SparkConf(conf_values)
        self.executed_statements = []
        self.config_values_at_execution = []
        self.sql_error = sql_error

    def sql(self, statement):
        self.executed_statements.append(statement)
        self.config_values_at_execution.append(dict(self.conf.values))
        if self.sql_error is not None:
            raise self.sql_error


def _make_engine(collect_stats_on_write):
    engine = object.__new__(FabricSpark)
    engine.collect_stats_on_write = collect_stats_on_write
    engine.spark_configs = {}
    engine.spark = _Spark()
    return engine


def _make_optimize_engine(fast_optimize_value=None, sql_error=None):
    engine = object.__new__(FabricSpark)
    config_values = (
        {FabricSpark._FAST_OPTIMIZE_CONFIG: fast_optimize_value}
        if fast_optimize_value is not None
        else None
    )
    engine.spark = _Spark(config_values, sql_error)
    engine.full_catalog_schema_reference = "`lakehouse`.`schema`"
    return engine


def test_write_stats_default_is_enabled():
    assert FabricSpark._resolve_collect_stats_on_write(True, None) is True


def test_deprecated_alias_takes_precedence():
    with pytest.warns(DeprecationWarning, match="collect_stats_on_write"):
        enabled = FabricSpark._resolve_collect_stats_on_write(True, False)

    assert enabled is False


@pytest.mark.parametrize(("enabled", "expected"), [(True, "true"), (False, "false")])
def test_write_stats_configs_are_set_and_logged(enabled, expected):
    engine = _make_engine(enabled)

    engine._configure_write_stats_collection()

    assert engine.spark.conf.values == {config_name: expected for config_name in FabricSpark._WRITE_STATS_CONFIGS}
    assert engine.spark_configs == {config_name: expected for config_name in FabricSpark._WRITE_STATS_CONFIGS}


@pytest.mark.parametrize("fast_optimize_value", [None, "false", "FALSE"])
def test_optimize_table_leaves_disabled_fast_optimize_unchanged(fast_optimize_value):
    engine = _make_optimize_engine(fast_optimize_value)

    engine.optimize_table("store_sales")

    assert engine.spark.executed_statements == ["OPTIMIZE `lakehouse`.`schema`.store_sales"]
    assert engine.spark.config_values_at_execution == [{}]
    assert engine.spark.conf.values == {}


def test_optimize_table_temporarily_disables_and_restores_fast_optimize():
    engine = _make_optimize_engine("TRUE")

    engine.optimize_table("store_sales")

    assert engine.spark.executed_statements == ["OPTIMIZE `lakehouse`.`schema`.store_sales"]
    assert engine.spark.config_values_at_execution == [{FabricSpark._FAST_OPTIMIZE_CONFIG: "false"}]
    assert engine.spark.conf.values == {FabricSpark._FAST_OPTIMIZE_CONFIG: "TRUE"}


def test_optimize_table_restores_fast_optimize_when_optimize_fails():
    error = RuntimeError("optimize failed")
    engine = _make_optimize_engine("true", sql_error=error)

    with pytest.raises(RuntimeError, match="optimize failed"):
        engine.optimize_table("store_sales")

    assert engine.spark.config_values_at_execution == [{FabricSpark._FAST_OPTIMIZE_CONFIG: "false"}]
    assert engine.spark.conf.values == {FabricSpark._FAST_OPTIMIZE_CONFIG: "true"}
