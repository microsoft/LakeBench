from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Optional, Sequence

from .resources.databricks_azure_dbu_map import AZURE_SPARK_DBU_MAP
from .resources.databricks_pricing import (
    BilledCost,
    DatabricksPricingError,
    backfill_results_cost,
    estimate_azure_spark_cost,
    infer_cloud_provider,
    log_if_manual_compute_cost_missing,
    normalize_cloud_provider,
    query_billed_cost,
)
from .spark import Spark

logger = logging.getLogger(__name__)

_TAG = "spark.databricks.clusterUsageTags."

# Azure VM SKU (without the Standard_ prefix) -> vCPUs, from the published DBU map.
_AZURE_SKU_VCPUS = {sku: int(details[2]) for (_, sku), details in AZURE_SPARK_DBU_MAP.items()}


def _detect_serverless() -> bool:
    return os.environ.get("IS_SERVERLESS", "").strip().upper() == "TRUE"


class DatabricksSpark(Spark):
    """Execute LakeBench workloads on Databricks Spark compute.

    Attributes
    ----------
    schema_name : str
        Name of the Databricks schema used for benchmark tables.
    schema_uri : str, optional
        Storage location assigned to the schema.
    catalog_name : str, optional
        Databricks catalog containing the benchmark schema. Serverless compute
        requires a Unity Catalog catalog (not ``hive_metastore``).
    spark_measure_telemetry : bool, optional
        Whether to collect sparkMeasure execution telemetry. Requires
        dedicated access mode on classic compute.
    cost_per_vcore_hour : float, optional
        Retail cost per vCore hour used for estimated job cost. Not supported
        on serverless compute.
    cost_per_hour : float, optional
        Total hourly compute cost, mutually exclusive with
        ``cost_per_vcore_hour``.
    compute_stats_all_cols : bool, optional
        Deprecated compatibility option for post-load column statistics.
    tblproperties : dict, optional
        Delta table properties added to table creation statements.
    serverless : bool, optional
        Override serverless detection. By default serverless compute is
        detected from the ``IS_SERVERLESS`` environment variable.
    job_run_id : str, optional
        Job run ID used to attribute serverless billing. Detected from the
        notebook context when omitted.
    notebook_id : str, optional
        Notebook ID used to attribute interactive serverless billing.
        Detected from the notebook context when omitted.

    Notes
    -----
    Cloud provider, region, workload type, Photon state, and Unity Catalog
    state are detected automatically from Databricks runtime configuration.
    Jobs Compute, All-Purpose Compute (dedicated or standard/shared access
    mode), and serverless compute are supported. Spark UI job descriptions and
    sparkMeasure need a SparkContext, which is only available in dedicated
    access mode; elsewhere timed phases are labeled with Spark Connect tags.

    Azure fixed-size clusters can resolve automatic DBU and infrastructure
    pricing from public Azure APIs. Serverless compute is billed by consumed
    DBUs, so its cost is resolved after the run with :meth:`get_billed_cost`
    unless ``cost_per_hour`` is passed.
    """

    SUPPORTS_MOUNT_PATH = True
    SUPPORTS_ONELAKE = False
    SUPPORTS_SCHEMA_PREP = True
    SPARK_CONFIGS_TO_READ = (
        "spark.databricks.cloudProvider",
        "spark.databricks.workspaceUrl",
        "spark.databricks.sparkContextId",
        f"{_TAG}clusterId",
        f"{_TAG}workloadType",
    )

    def __init__(
        self,
        schema_name: str,
        schema_uri: Optional[str] = None,
        catalog_name: str = "hive_metastore",
        spark_measure_telemetry: bool = False,
        cost_per_vcore_hour: Optional[float] = None,
        cost_per_hour: Optional[float] = None,
        compute_stats_all_cols: bool = False,
        tblproperties: Optional[dict] = None,
        serverless: Optional[bool] = None,
        job_run_id: Optional[str] = None,
        notebook_id: Optional[str] = None,
    ):
        self.serverless = _detect_serverless() if serverless is None else bool(serverless)
        if self.serverless:
            if catalog_name == "hive_metastore":
                raise ValueError(
                    "Databricks serverless compute requires a Unity Catalog catalog. "
                    "Pass catalog_name='<your_catalog>'."
                )
            if cost_per_vcore_hour is not None:
                raise ValueError(
                    "cost_per_vcore_hour is not supported on Databricks serverless compute. "
                    "Pass cost_per_hour, or omit it and call get_billed_cost() after the run."
                )

        super().__init__(
            catalog_name=catalog_name,
            schema_name=schema_name,
            schema_uri=schema_uri,
            spark_measure_telemetry=spark_measure_telemetry,
            compute_stats_all_cols=compute_stats_all_cols,
            tblproperties=tblproperties,
        )
        self._created_at = datetime.now()

        if self.serverless:
            self._init_serverless(job_run_id=job_run_id, notebook_id=notebook_id)
        else:
            self._init_cluster(catalog_name=catalog_name)

        self._configure_pricing(
            cost_per_vcore_hour=cost_per_vcore_hour,
            cost_per_hour=cost_per_hour,
        )

    # ------------------------------------------------------------------
    # Initialization
    # ------------------------------------------------------------------
    def _init_cluster(self, catalog_name: str) -> None:
        runtime_version = self._get_conf(f"{_TAG}sparkVersion", "unknown")
        runtime_engine = self._get_conf(f"{_TAG}runtimeEngine")
        self.version = f"{self.spark.version} (DBR=={runtime_version})"
        self.photon_enabled = runtime_engine == "PHOTON"
        self.unity_catalog = (
            self._get_conf("spark.databricks.unityCatalog.enabled", "false") == "true"
            and catalog_name != "hive_metastore"
        )
        self.cloud_provider = infer_cloud_provider(
            explicit_cloud_provider=None,
            spark_configs=self.spark_configs,
        )
        self.region = self._get_conf(f"{_TAG}region")
        self.workload_type = (
            "jobs-compute"
            if self.spark_configs.get(f"{_TAG}workloadType", "unknown") == "AUTOMATED"
            else "all-purpose-compute"
        )
        self.pricing_workload_type = f"{self.workload_type}-with-photon" if self.photon_enabled else self.workload_type

        workspace_url = self.spark_configs.get("spark.databricks.workspaceUrl")
        cluster_id = self.spark_configs.get(f"{_TAG}clusterId")
        spark_context_id = self.spark_configs.get("spark.databricks.sparkContextId")
        spark_history_url = (
            f"https://{workspace_url}/compute/sparkui/{cluster_id}/driver-{spark_context_id}"
            if workspace_url and cluster_id and spark_context_id
            else "unknown"
        )

        self.extended_engine_metadata.update(
            {
                "cloud_provider": self.cloud_provider,
                "compute_region": self.region or "unknown",
                "photon_enabled": (str(self.photon_enabled) if runtime_engine is not None else "unknown"),
                "unity_catalog": str(self.unity_catalog),
                "serverless": "False",
                f"{_TAG}sparkVersion": runtime_version,
                "spark_history_url": spark_history_url,
                "workload_type": self.pricing_workload_type,
            }
        )

    def _init_serverless(self, job_run_id: Optional[str], notebook_id: Optional[str]) -> None:
        context_tags = self._get_notebook_context_tags()
        self.job_run_id = job_run_id or context_tags.get("jobRunId") or context_tags.get("multitaskParentRunId")
        self.job_id = context_tags.get("jobId")
        self.notebook_id = notebook_id or context_tags.get("notebookId")

        environment_version = os.environ.get("DATABRICKS_RUNTIME_VERSION", "unknown")
        self.version = f"{self.spark.version} (serverless=={environment_version})"
        self.photon_enabled = True
        self.unity_catalog = True
        self.cloud_provider, self.region = self._get_serverless_cloud_and_region()
        self.workload_type = "serverless-jobs-compute" if self.job_run_id else "serverless-interactive"
        self.pricing_workload_type = self.workload_type

        metadata = {
            "cloud_provider": self.cloud_provider or "unknown",
            "compute_region": self.region or "unknown",
            "photon_enabled": "True",
            "unity_catalog": "True",
            "serverless": "True",
            "serverless_environment_version": environment_version,
            "workload_type": self.workload_type,
        }
        for key, value in (("job_run_id", self.job_run_id), ("job_id", self.job_id), ("notebook_id", self.notebook_id)):
            if value:
                metadata[key] = str(value)
        self.extended_engine_metadata.update(metadata)

    def _get_notebook_context_tags(self) -> dict:
        """Read notebook context tags (jobRunId, jobId, notebookId) when available."""
        try:
            dbutils = None
            try:
                from IPython import get_ipython

                ipython = get_ipython()
                if ipython is not None:
                    dbutils = ipython.user_ns.get("dbutils")
            except Exception:
                dbutils = None
            if dbutils is None:
                from databricks.sdk.runtime import dbutils  # type: ignore[no-redef]

            context = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
            tags = context.get("tags") or {}
            return {k: str(v) for k, v in tags.items() if v is not None}
        except Exception as exc:
            logger.debug("Unable to read Databricks notebook context tags: %s", exc)
            return {}

    def _get_serverless_cloud_and_region(self) -> tuple[Optional[str], Optional[str]]:
        try:
            row = self.spark.sql(
                "SELECT split(current_metastore(), ':')[0] AS cloud, split(current_metastore(), ':')[1] AS region"
            ).collect()[0]
            cloud = (row["cloud"] or "").strip()
            region = (row["region"] or "").strip() or None
            if cloud:
                return normalize_cloud_provider(cloud), region
        except Exception as exc:
            logger.debug("Unable to read cloud and region from current_metastore(): %s", exc)
        try:
            return infer_cloud_provider(spark_configs=self.spark_configs), None
        except ValueError:
            return None, None

    # ------------------------------------------------------------------
    # Pricing
    # ------------------------------------------------------------------
    def _configure_pricing(
        self,
        cost_per_vcore_hour: Optional[float],
        cost_per_hour: Optional[float],
    ) -> None:
        if self.serverless:
            self._configure_cost_inputs(cost_per_hour=cost_per_hour)
            if cost_per_hour is None:
                logger.info(
                    "Databricks serverless is billed by consumed DBUs. Call get_billed_cost() after the run "
                    "(usage can take a few hours to appear) or pass cost_per_hour."
                )
                self.extended_engine_metadata["pricing_source"] = "billing_lookup_pending"
            else:
                self.extended_engine_metadata["cost_per_hour"] = str(self.cost_per_hour)
            return

        log_if_manual_compute_cost_missing(
            self.cloud_provider,
            cost_per_vcore_hour,
            cost_per_hour,
        )
        self._configure_cost_inputs(
            cost_per_vcore_hour=cost_per_vcore_hour,
            cost_per_hour=cost_per_hour,
        )
        if cost_per_vcore_hour is not None or cost_per_hour is not None:
            self._materialize_cost_per_hour()
            self.extended_engine_metadata["cost_per_hour"] = str(self.cost_per_hour)
        elif self.cloud_provider == "azure":
            self._configure_automatic_pricing(
                pricing_workload_type=self.pricing_workload_type,
            )
        else:
            self.extended_engine_metadata["pricing_source"] = "not_configured"

    def _configure_automatic_pricing(
        self,
        pricing_workload_type: str,
    ) -> None:
        if self._get_conf(f"{_TAG}clusterScalingType", "") != "fixed_size":
            logger.warning(
                "Estimated job cost will not be reported because automatic Azure "
                "Databricks Spark pricing requires fixed-size compute. Set "
                "cost_per_vcore_hour or cost_per_hour to enable estimated cost reporting."
            )
            self.extended_engine_metadata["pricing_source"] = "not_configured"
            return

        driver_sku = self._get_conf("spark.databricks.driverNodeTypeId")
        worker_sku = self._get_conf("spark.databricks.workerNodeTypeId")
        worker_count = self._get_conf(f"{_TAG}clusterWorkers")
        if not self.region or not driver_sku or not worker_sku or worker_count is None:
            logger.warning(
                "Estimated job cost will not be reported because required Azure "
                "Databricks cluster pricing metadata is unavailable. Set "
                "cost_per_vcore_hour or cost_per_hour to enable estimated cost reporting."
            )
            self.extended_engine_metadata["pricing_source"] = "not_configured"
            return

        try:
            estimate = estimate_azure_spark_cost(
                region=self.region,
                driver_instance_type=driver_sku,
                worker_instance_type=worker_sku,
                worker_count=int(worker_count),
                workload_type=pricing_workload_type,
            )
        except DatabricksPricingError as exc:
            logger.warning("Estimated job cost will not be reported: %s", exc)
            self.extended_engine_metadata["pricing_source"] = "not_configured"
            return

        self.cost_per_hour = float(estimate.cost_per_hour)
        self.extended_engine_metadata.update(estimate.metadata())
        self.extended_engine_metadata.update(
            {
                "driver_sku": driver_sku,
                "worker_sku": worker_sku,
                "worker_count": str(worker_count),
                "dbu_estimation_method": "azure_databricks_vm_sku_mapping",
            }
        )

    def get_billed_cost(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> BilledCost:
        """Look up serverless usage billed to this run in ``system.billing.usage``.

        Parameters
        ----------
        start_time : datetime, optional
            Start of the usage window. Defaults to when this engine was created.
        end_time : datetime, optional
            End of the usage window. Defaults to now.

        Returns
        -------
        BilledCost
            Total DBUs and list cost. ``may_be_incomplete`` is True when the
            window ended within the last few hours, because billing data lags.

        Notes
        -----
        Usage is attributed to the job run (jobs) or the notebook
        (interactive). Interactive attribution includes all serverless usage
        from the notebook in the window, so run LakeBench alone in the notebook
        for accurate figures. Use :func:`backfill_results_cost` to spread the
        total across benchmark result rows.
        """
        if not self.serverless:
            raise DatabricksPricingError("get_billed_cost() is only supported on Databricks serverless compute.")
        return query_billed_cost(
            self.spark,
            start_time=start_time or self._created_at,
            end_time=end_time,
            job_run_id=self.job_run_id,
            notebook_id=self.notebook_id,
        )

    @staticmethod
    def backfill_results_cost(results: Sequence[dict], billed: BilledCost) -> list[dict]:
        """Spread a :class:`BilledCost` across result rows by ``duration_ms``."""
        return backfill_results_cost(results, billed)

    # ------------------------------------------------------------------
    # Compute sizing
    # ------------------------------------------------------------------
    def _cluster_shape(self) -> dict:
        def to_int(value: Optional[str]) -> Optional[int]:
            try:
                return int(value) if value is not None else None
            except ValueError:
                return None

        return {
            "driver_sku": self._get_conf("spark.databricks.driverNodeTypeId"),
            "worker_sku": self._get_conf("spark.databricks.workerNodeTypeId"),
            "workers": to_int(self._get_conf(f"{_TAG}clusterWorkers")),
            "min_workers": to_int(self._get_conf(f"{_TAG}clusterMinWorkers")),
            "max_workers": to_int(self._get_conf(f"{_TAG}clusterMaxWorkers")),
            "autoscaling": self._get_conf(f"{_TAG}clusterScalingType", "") == "autoscaling",
        }

    @staticmethod
    def _sku_vcpus(sku: Optional[str]) -> Optional[int]:
        if not sku:
            return None
        return _AZURE_SKU_VCPUS.get(sku[len("Standard_") :] if sku.startswith("Standard_") else sku)

    def get_total_cores(self) -> int:
        if self.serverless:
            return 0
        if self._has_spark_context:
            return super().get_total_cores()
        shape = self._cluster_shape()
        driver_vcpus = self._sku_vcpus(shape["driver_sku"])
        worker_vcpus = self._sku_vcpus(shape["worker_sku"])
        workers = shape["workers"] or 0
        if driver_vcpus is None or (workers and worker_vcpus is None):
            return 0
        return driver_vcpus + workers * (worker_vcpus or 0)

    def get_compute_size(self) -> str:
        if self.serverless:
            return "Serverless"
        if self._has_spark_context:
            return super().get_compute_size()
        shape = self._cluster_shape()
        worker_sku = shape["worker_sku"] or "unknown"
        if shape["autoscaling"] and shape["min_workers"] is not None and shape["max_workers"] is not None:
            return f"{shape['min_workers']}-{shape['max_workers']} x {worker_sku} Workers"
        workers = shape["workers"]
        if workers is None:
            return "unknown"
        if workers == 0:
            return f"{shape['driver_sku'] or 'unknown'} SingleNode"
        return f"{workers} x {worker_sku} {'Workers' if workers > 1 else 'Worker'}"

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------
    def create_schema_if_not_exists(self, drop_before_create: bool = True):
        location_str = (
            f"{'MANAGED ' if self.unity_catalog else ''}LOCATION '{self.schema_uri}'"
            if self.schema_uri is not None
            else ""
        )
        if drop_before_create:
            self.spark.sql(f"DROP SCHEMA IF EXISTS {self.full_catalog_schema_reference} CASCADE")
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.full_catalog_schema_reference} {location_str}")
        self.spark.sql(f"USE {self.full_catalog_schema_reference}")
