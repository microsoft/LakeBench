"""Databricks Spark on serverless and shared (standard) access mode, i.e. without a SparkContext."""

import importlib.machinery
import sys
import types
from datetime import datetime, timedelta
from decimal import Decimal

import pytest

from lakebench.engines.databricks_spark import DatabricksSpark
from lakebench.engines.resources.databricks_pricing import (
    BilledCost,
    DatabricksPricingError,
    backfill_results_cost,
    query_billed_cost,
)
from lakebench.engines.spark import Spark
from lakebench.utils.timer import _to_spark_connect_tag, timer


class _FakeConf:
    def __init__(self, values):
        self.values = dict(values)
        self.set_calls = []

    def get(self, key, default=None):
        return self.values.get(key, default)

    def set(self, key, value):
        raise RuntimeError(f"[CONFIG_NOT_AVAILABLE] {key}")


class _Result:
    def __init__(self, rows):
        self.rows = rows

    def collect(self):
        return self.rows


class _FakeConnectSession:
    """Mimics a Spark Connect session: no sparkContext, no _jconf."""

    version = "3.5.2"

    def __init__(self, conf=None, sql_rows=None):
        self.conf = _FakeConf(conf or {})
        self.sql_calls = []
        self.sql_rows = sql_rows or {}
        self.tags = []
        self.tag_events = []

    @property
    def sparkContext(self):
        raise RuntimeError("[JVM_ATTRIBUTE_NOT_SUPPORTED] sparkContext is not supported in Spark Connect")

    def sql(self, query, args=None):
        self.sql_calls.append((query, args))
        for needle, rows in self.sql_rows.items():
            if needle in query:
                return _Result(rows)
        return _Result([])

    def addTag(self, tag):
        self.tags.append(tag)
        self.tag_events.append(("add", tag))

    def removeTag(self, tag):
        self.tags.remove(tag)
        self.tag_events.append(("remove", tag))


@pytest.fixture
def fake_pyspark(monkeypatch):
    """Install a fake pyspark whose SparkSession.builder returns the configured session."""
    holder = {}

    class _Builder:
        def getOrCreate(self):
            return holder["session"]

    pyspark = types.ModuleType("pyspark")
    pyspark.__spec__ = importlib.machinery.ModuleSpec("pyspark", None)
    pyspark_sql = types.ModuleType("pyspark.sql")
    pyspark_sql.__spec__ = importlib.machinery.ModuleSpec("pyspark.sql", None)
    pyspark_sql.SparkSession = types.SimpleNamespace(builder=_Builder())
    pyspark_functions = types.ModuleType("pyspark.sql.functions")
    pyspark.sql = pyspark_sql
    pyspark_sql.functions = pyspark_functions
    monkeypatch.setitem(sys.modules, "pyspark", pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", pyspark_sql)
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions", pyspark_functions)
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "client.3.1")
    monkeypatch.setattr(DatabricksSpark, "_get_notebook_context_tags", lambda self: holder.get("context_tags", {}))

    def install(session, context_tags=None):
        holder["session"] = session
        holder["context_tags"] = context_tags or {}
        return session

    return install


_METASTORE_ROWS = {"current_metastore()": [{"cloud": "azure", "region": "eastus2"}]}


def _serverless_engine(fake_pyspark, monkeypatch, context_tags=None, **kwargs):
    monkeypatch.setenv("IS_SERVERLESS", "TRUE")
    session = fake_pyspark(_FakeConnectSession(sql_rows=_METASTORE_ROWS), context_tags=context_tags)
    kwargs.setdefault("catalog_name", "main")
    engine = DatabricksSpark(schema_name="tpch", **kwargs)
    return engine, session


# ---------------------------------------------------------------------------
# Serverless detection and validation
# ---------------------------------------------------------------------------
def test_serverless_requires_unity_catalog(fake_pyspark, monkeypatch):
    monkeypatch.setenv("IS_SERVERLESS", "TRUE")
    fake_pyspark(_FakeConnectSession())
    with pytest.raises(ValueError, match="Unity Catalog"):
        DatabricksSpark(schema_name="tpch")


def test_serverless_rejects_cost_per_vcore_hour(fake_pyspark, monkeypatch):
    monkeypatch.setenv("IS_SERVERLESS", "TRUE")
    fake_pyspark(_FakeConnectSession())
    with pytest.raises(ValueError, match="cost_per_vcore_hour"):
        DatabricksSpark(schema_name="tpch", catalog_name="main", cost_per_vcore_hour=0.1)


def test_serverless_rejects_spark_measure(fake_pyspark, monkeypatch):
    monkeypatch.setenv("IS_SERVERLESS", "TRUE")
    fake_pyspark(_FakeConnectSession(sql_rows=_METASTORE_ROWS))
    with pytest.raises(ValueError, match="spark_measure_telemetry"):
        DatabricksSpark(schema_name="tpch", catalog_name="main", spark_measure_telemetry=True)


def test_serverless_metadata_and_compute_size(fake_pyspark, monkeypatch):
    engine, _ = _serverless_engine(fake_pyspark, monkeypatch, context_tags={"jobRunId": "42", "jobId": "7"})

    assert engine.serverless is True
    assert engine.version == "3.5.2 (serverless==client.3.1)"
    assert engine.get_total_cores() == 0
    assert engine.get_compute_size() == "Serverless"
    assert engine.get_job_cost(60_000) is None
    metadata = engine.extended_engine_metadata
    assert metadata["serverless"] == "True"
    assert metadata["photon_enabled"] == "True"
    assert metadata["unity_catalog"] == "True"
    assert metadata["cloud_provider"] == "azure"
    assert metadata["compute_region"] == "eastus2"
    assert metadata["workload_type"] == "serverless-jobs-compute"
    assert metadata["job_run_id"] == "42"
    assert metadata["pricing_source"] == "billing_lookup_pending"


def test_serverless_interactive_workload_type(fake_pyspark, monkeypatch):
    engine, _ = _serverless_engine(fake_pyspark, monkeypatch, context_tags={"notebookId": "123"})
    assert engine.extended_engine_metadata["workload_type"] == "serverless-interactive"
    assert engine.notebook_id == "123"


def test_serverless_explicit_cost_per_hour(fake_pyspark, monkeypatch):
    engine, _ = _serverless_engine(fake_pyspark, monkeypatch, cost_per_hour=36.0)
    assert engine.extended_engine_metadata["pricing_source"] == "explicit_cost_per_hour"
    assert engine.get_job_cost(3_600_000) == Decimal("36.0")


def test_serverless_flag_overrides_environment(fake_pyspark, monkeypatch):
    monkeypatch.delenv("IS_SERVERLESS", raising=False)
    fake_pyspark(_FakeConnectSession(sql_rows=_METASTORE_ROWS))
    engine = DatabricksSpark(schema_name="tpch", catalog_name="main", serverless=True)
    assert engine.get_compute_size() == "Serverless"


def test_vacuum_tolerates_restricted_conf(fake_pyspark, monkeypatch, caplog):
    engine, session = _serverless_engine(fake_pyspark, monkeypatch)
    engine.vacuum_table("lineitem", retain_hours=0, retention_check=False)
    assert any("VACUUM" in query for query, _ in session.sql_calls)
    assert "retention duration check" in caplog.text


# ---------------------------------------------------------------------------
# Shared (standard) access mode on classic compute
# ---------------------------------------------------------------------------
_SHARED_MODE_CONF = {
    "spark.databricks.workspaceUrl": "adb-1.azuredatabricks.net",
    "spark.databricks.clusterUsageTags.clusterId": "0101-abc",
    "spark.databricks.clusterUsageTags.workloadType": "AUTOMATED",
    "spark.databricks.clusterUsageTags.sparkVersion": "16.4.x-photon-scala2.12",
    "spark.databricks.clusterUsageTags.runtimeEngine": "PHOTON",
    "spark.databricks.clusterUsageTags.region": "eastus",
    "spark.databricks.clusterUsageTags.clusterScalingType": "fixed_size",
    "spark.databricks.clusterUsageTags.clusterWorkers": "4",
    "spark.databricks.driverNodeTypeId": "Standard_E8ds_v5",
    "spark.databricks.workerNodeTypeId": "Standard_E8ds_v5",
    "spark.databricks.unityCatalog.enabled": "true",
    "spark.sql.shuffle.partitions": "200",
}


def _shared_mode_engine(fake_pyspark, monkeypatch, conf=None, **kwargs):
    monkeypatch.delenv("IS_SERVERLESS", raising=False)
    fake_pyspark(_FakeConnectSession(conf=conf or _SHARED_MODE_CONF))
    monkeypatch.setattr(DatabricksSpark, "_configure_automatic_pricing", lambda self, pricing_workload_type: None)
    return DatabricksSpark(schema_name="tpch", catalog_name="main", **kwargs)


def test_shared_mode_reads_configs_without_jvm(fake_pyspark, monkeypatch):
    engine = _shared_mode_engine(fake_pyspark, monkeypatch)

    assert engine.serverless is False
    assert engine._has_spark_context is False
    assert engine.version == "3.5.2 (DBR==16.4.x-photon-scala2.12)"
    assert engine.cloud_provider == "azure"
    assert engine.photon_enabled is True
    assert engine.unity_catalog is True
    metadata = engine.extended_engine_metadata
    assert metadata["workload_type"] == "jobs-compute-with-photon"
    assert metadata["spark.sql.shuffle.partitions"] == "200"
    assert metadata["parquet.block.size"] == ""
    assert metadata["spark_history_url"] == "unknown"


def test_shared_mode_compute_size_from_cluster_tags(fake_pyspark, monkeypatch):
    engine = _shared_mode_engine(fake_pyspark, monkeypatch)
    assert engine.get_compute_size() == "4 x Standard_E8ds_v5 Workers"
    assert engine.get_total_cores() == 8 + 4 * 8


def test_shared_mode_autoscaling_and_single_node(fake_pyspark, monkeypatch):
    autoscale_conf = {
        **_SHARED_MODE_CONF,
        "spark.databricks.clusterUsageTags.clusterScalingType": "autoscaling",
        "spark.databricks.clusterUsageTags.clusterMinWorkers": "2",
        "spark.databricks.clusterUsageTags.clusterMaxWorkers": "8",
    }
    engine = _shared_mode_engine(fake_pyspark, monkeypatch, conf=autoscale_conf)
    assert engine.get_compute_size() == "2-8 x Standard_E8ds_v5 Workers"

    single_node_conf = {**_SHARED_MODE_CONF, "spark.databricks.clusterUsageTags.clusterWorkers": "0"}
    engine = _shared_mode_engine(fake_pyspark, monkeypatch, conf=single_node_conf)
    assert engine.get_compute_size() == "Standard_E8ds_v5 SingleNode"
    assert engine.get_total_cores() == 8


def test_shared_mode_unknown_sku_reports_zero_cores(fake_pyspark, monkeypatch):
    conf = {
        **_SHARED_MODE_CONF,
        "spark.databricks.workspaceUrl": "dbc-1.cloud.databricks.com",
        "spark.databricks.driverNodeTypeId": "i3.xlarge",
        "spark.databricks.workerNodeTypeId": "i3.xlarge",
    }
    engine = _shared_mode_engine(fake_pyspark, monkeypatch, conf=conf)
    assert engine.get_total_cores() == 0
    assert engine.get_compute_size() == "4 x i3.xlarge Workers"


def test_shared_mode_uses_azure_auto_pricing_from_tags(fake_pyspark, monkeypatch):
    monkeypatch.delenv("IS_SERVERLESS", raising=False)
    fake_pyspark(_FakeConnectSession(conf=_SHARED_MODE_CONF))
    captured = {}

    def fake_estimate(**kwargs):
        captured.update(kwargs)
        raise DatabricksPricingError("stop")

    monkeypatch.setattr("lakebench.engines.databricks_spark.estimate_azure_spark_cost", fake_estimate)
    engine = DatabricksSpark(schema_name="tpch", catalog_name="main")

    assert captured == {
        "region": "eastus",
        "driver_instance_type": "Standard_E8ds_v5",
        "worker_instance_type": "Standard_E8ds_v5",
        "worker_count": 4,
        "workload_type": "jobs-compute-with-photon",
    }
    assert engine.extended_engine_metadata["pricing_source"] == "not_configured"


# ---------------------------------------------------------------------------
# Timer labeling on Spark Connect
# ---------------------------------------------------------------------------
def test_timer_tags_spark_connect_operations(fake_pyspark, monkeypatch):
    engine, session = _serverless_engine(fake_pyspark, monkeypatch)
    timer.clear_results()

    with timer(phase="Query", test_item="q1, q2", engine=engine):
        assert session.tags == ["Query - q1_ q2 [i:1]"]

    assert session.tags == []
    assert [event for event, _ in session.tag_events] == ["add", "remove"]


def test_timer_removes_tag_when_block_fails(fake_pyspark, monkeypatch):
    engine, session = _serverless_engine(fake_pyspark, monkeypatch)
    timer.clear_results()

    with timer(phase="Load", test_item="lineitem", engine=engine):
        raise RuntimeError("boom")

    assert session.tags == []


def test_spark_connect_tag_sanitizes_commas():
    assert "," not in _to_spark_connect_tag("Load - a,b [i:1]")
    assert _to_spark_connect_tag(",,,") == "_"


# ---------------------------------------------------------------------------
# Billing lookup
# ---------------------------------------------------------------------------
_BILLING_ROW = {"dbus": Decimal("12.5"), "list_cost_usd": Decimal("8.75"), "row_count": 3, "unpriced_row_count": 0}


def test_billed_cost_filters_on_job_run_id():
    session = _FakeConnectSession(sql_rows={"system.billing.usage": [_BILLING_ROW]})
    start = datetime.now() - timedelta(hours=6)
    end = datetime.now() - timedelta(hours=5)

    billed = query_billed_cost(session, start, end, job_run_id="42", notebook_id="9")

    query, args = session.sql_calls[-1]
    assert "u.usage_metadata.job_run_id = :attribution_id" in query
    assert "is_serverless" in query
    assert args["attribution_id"] == "42"
    assert billed.dbus == Decimal("12.5")
    assert billed.list_cost_usd == Decimal("8.75")
    assert billed.attribution == "job_run_id=42"
    assert billed.may_be_incomplete is False
    assert billed.metadata()["pricing_source"] == "databricks_system_billing_usage"


def test_billed_cost_falls_back_to_notebook_and_flags_recent_window():
    session = _FakeConnectSession(sql_rows={"system.billing.usage": [_BILLING_ROW]})

    billed = query_billed_cost(session, datetime.now() - timedelta(minutes=30), notebook_id="9")

    query, args = session.sql_calls[-1]
    assert "u.usage_metadata.notebook_id = :attribution_id" in query
    assert args["attribution_id"] == "9"
    assert billed.may_be_incomplete is True


def test_billed_cost_requires_attribution():
    with pytest.raises(DatabricksPricingError, match="job_run_id or notebook_id"):
        query_billed_cost(_FakeConnectSession(), datetime.now() - timedelta(hours=1))


def test_billed_cost_wraps_permission_errors():
    class _DeniedSession(_FakeConnectSession):
        def sql(self, query, args=None):
            raise RuntimeError("PERMISSION_DENIED")

    with pytest.raises(DatabricksPricingError, match="system.billing.usage"):
        query_billed_cost(_DeniedSession(), datetime.now() - timedelta(hours=1), job_run_id="1")


def test_engine_billed_cost_uses_engine_attribution(fake_pyspark, monkeypatch):
    engine, session = _serverless_engine(fake_pyspark, monkeypatch, context_tags={"jobRunId": "42"})
    session.sql_rows["system.billing.usage"] = [_BILLING_ROW]

    billed = engine.get_billed_cost()

    assert billed.attribution == "job_run_id=42"


def test_engine_billed_cost_is_serverless_only(fake_pyspark, monkeypatch):
    engine = _shared_mode_engine(fake_pyspark, monkeypatch)
    with pytest.raises(DatabricksPricingError, match="serverless"):
        engine.get_billed_cost()


def test_backfill_results_cost_splits_by_duration():
    billed = BilledCost(
        dbus=Decimal("10"),
        list_cost_usd=Decimal("9"),
        row_count=1,
        unpriced_row_count=0,
        attribution="job_run_id=1",
        start_time=datetime.now(),
        end_time=datetime.now(),
        may_be_incomplete=False,
    )
    results = [{"duration_ms": 1000}, {"duration_ms": 2000}]

    updated = backfill_results_cost(results, billed)

    assert [row["estimated_retail_job_cost"] for row in updated] == [Decimal("3"), Decimal("6")]
    assert sum(row["estimated_retail_job_cost"] for row in updated) == billed.list_cost_usd
    assert "estimated_retail_job_cost" not in results[0]


# ---------------------------------------------------------------------------
# Base Spark engine without a SparkContext
# ---------------------------------------------------------------------------
def test_base_spark_defaults_without_spark_context(fake_pyspark):
    fake_pyspark(_FakeConnectSession(conf={"spark.sql.shuffle.partitions": "64"}))
    engine = Spark(schema_name="tpch", catalog_name="main")

    assert engine.version == "3.5.2"
    assert engine.get_total_cores() == 0
    assert engine.get_compute_size() == "unknown"
    assert engine.extended_engine_metadata["spark.sql.shuffle.partitions"] == "64"
