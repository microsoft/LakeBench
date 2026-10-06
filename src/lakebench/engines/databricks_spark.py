from __future__ import annotations

import logging
from typing import Optional

from .databricks_pricing import (
    DatabricksPricingError,
    estimate_azure_spark_cost,
    infer_cloud_provider,
    log_if_manual_compute_cost_missing,
)
from .spark import Spark

logger = logging.getLogger(__name__)


class DatabricksSpark(Spark):
    """Databricks Spark engine."""

    SUPPORTS_MOUNT_PATH = True
    SUPPORTS_ONELAKE = False
    SUPPORTS_SCHEMA_PREP = True
    REQUIRED_MODULES = ("pyspark", "requests")
    INSTALL_EXTRA = "databricks_spark"

    def __init__(
        self,
        schema_name: str,
        schema_uri: Optional[str] = None,
        catalog_name: str = "hive_metastore",
        spark_measure_telemetry: bool = False,
        cost_per_vcore_hour: Optional[float] = None,
        cost_per_hour: Optional[float] = None,
        dbu_rate: Optional[float] = None,
        infrastructure_cost_per_hour: Optional[float] = None,
        workload_type: str = "jobs-compute",
        cloud_provider: Optional[str] = None,
        compute_stats_all_cols: bool = False,
        tblproperties: Optional[dict] = None,
    ):
        super().__init__(
            catalog_name=catalog_name,
            schema_name=schema_name,
            schema_uri=schema_uri,
            spark_measure_telemetry=spark_measure_telemetry,
            compute_stats_all_cols=compute_stats_all_cols,
            tblproperties=tblproperties,
        )

        if workload_type not in {"jobs-compute", "all-purpose-compute"}:
            raise ValueError("workload_type must be 'jobs-compute' or 'all-purpose-compute'.")

        runtime_version = self.spark.conf.get(
            "spark.databricks.clusterUsageTags.sparkVersion",
            "unknown",
        )
        runtime_engine = self.spark.conf.get(
            "spark.databricks.clusterUsageTags.runtimeEngine",
            None,
        )
        self.version = f"{self.spark.sparkContext.version} (DBR=={runtime_version})"
        self.photon_enabled = runtime_engine == "PHOTON"
        self.unity_catalog = (
            self.spark.conf.get("spark.databricks.unityCatalog.enabled", "false") == "true"
            and catalog_name != "hive_metastore"
        )
        self.cloud_provider = infer_cloud_provider(
            explicit_cloud_provider=cloud_provider,
            spark_configs=self.spark_configs,
        )
        self.region = self.spark.conf.get(
            "spark.databricks.clusterUsageTags.region",
            None,
        )
        self.workload_type = workload_type
        pricing_workload_type = f"{workload_type}-with-photon" if self.photon_enabled else workload_type

        workspace_url = self.spark_configs.get("spark.databricks.workspaceUrl")
        cluster_id = self.spark_configs.get("spark.databricks.clusterUsageTags.clusterId")
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
                "spark.databricks.clusterUsageTags.sparkVersion": runtime_version,
                "spark_history_url": spark_history_url,
                "workload_type": pricing_workload_type,
            }
        )

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
                pricing_workload_type=pricing_workload_type,
                dbu_rate=dbu_rate,
                infrastructure_cost_per_hour=infrastructure_cost_per_hour,
            )
        else:
            self.extended_engine_metadata["pricing_source"] = "not_configured"

    def _configure_automatic_pricing(
        self,
        pricing_workload_type: str,
        dbu_rate: Optional[float],
        infrastructure_cost_per_hour: Optional[float],
    ) -> None:
        if (
            self.spark.conf.get(
                "spark.databricks.clusterUsageTags.clusterScalingType",
                "",
            )
            != "fixed_size"
        ):
            logger.warning(
                "Estimated job cost will not be reported because automatic Azure "
                "Databricks Spark pricing requires fixed-size compute. Set "
                "cost_per_vcore_hour or cost_per_hour to enable estimated cost reporting."
            )
            self.extended_engine_metadata["pricing_source"] = "not_configured"
            return

        driver_sku = self.spark.conf.get(
            "spark.databricks.driverNodeTypeId",
            None,
        )
        worker_sku = self.spark.conf.get(
            "spark.databricks.workerNodeTypeId",
            None,
        )
        worker_count = self.spark.conf.get(
            "spark.databricks.clusterUsageTags.clusterWorkers",
            None,
        )
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
                dbu_rate_override=dbu_rate,
                infrastructure_cost_per_hour_override=infrastructure_cost_per_hour,
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
