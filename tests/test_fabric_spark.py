import pytest

from lakebench.engines.fabric_spark import FabricSpark


class _SparkConf:
    def __init__(self):
        self.values = {}

    def set(self, name, value):
        self.values[name] = value


def _make_engine(collect_stats_on_write):
    engine = object.__new__(FabricSpark)
    engine.collect_stats_on_write = collect_stats_on_write
    engine.spark_configs = {}
    engine.spark = type("SparkStub", (), {"conf": _SparkConf()})()
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

    assert engine.spark.conf.values == {
        config_name: expected for config_name in FabricSpark._WRITE_STATS_CONFIGS
    }
    assert engine.spark_configs == {
        config_name: expected for config_name in FabricSpark._WRITE_STATS_CONFIGS
    }
