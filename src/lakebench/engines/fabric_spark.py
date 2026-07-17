import re
import warnings
from decimal import Decimal
from typing import Optional
from urllib.parse import parse_qs, urlparse

from .spark import Spark


class FabricSpark(Spark):
    """
    Fabric Spark Engine
    """

    _WRITE_STATS_CONFIGS = (
        "spark.microsoft.delta.stats.collect.extended",
        "spark.microsoft.delta.stats.injection.enabled",
        "spark.microsoft.delta.stats.collect.extended.property.setAtTableCreation",
    )

    def __init__(
        self,
        lakehouse_name: str,
        lakehouse_schema_name: str,
        spark_measure_telemetry: bool = False,
        cost_per_vcore_hour: Optional[float] = None,
        collect_stats_on_write: bool = True,
        compute_stats_all_cols: Optional[bool] = None,
    ):
        """
        Parameters
        ----------
        lakehouse_name : str
            The name of the lakehouse (catalog) to use within Fabric.
        lakehouse_schema_name : str
            The name of the schema (database) to use within the catalog.
        spark_measure_telemetry : bool, default False
            Whether to enable sparkmeasure telemetry for performance measurement.
        cost_per_vcore_hour : float, optional
            The cost per vCore hour for the Spark cluster. If None, cost calculations are auto calculated
            where possible.
        collect_stats_on_write : bool, default True
            Whether Fabric Delta extended statistics should be collected during write operations.
        compute_stats_all_cols : bool, optional
            Deprecated alias for ``collect_stats_on_write``. When provided, it takes precedence.
        """
        collect_stats_on_write = self._resolve_collect_stats_on_write(
            collect_stats_on_write=collect_stats_on_write,
            compute_stats_all_cols=compute_stats_all_cols,
        )

        super().__init__(
            catalog_name=lakehouse_name,
            schema_name=lakehouse_schema_name,
            spark_measure_telemetry=spark_measure_telemetry,
            cost_per_vcore_hour=cost_per_vcore_hour,
            compute_stats_all_cols=False,
        )

        self.collect_stats_on_write = collect_stats_on_write
        self.compute_stats_all_cols = collect_stats_on_write
        self.run_analyze_after_load = False
        self._configure_write_stats_collection()

        self.version: str = (
            f"{self.spark.sparkContext.version} (vhd_name=={self.spark.conf.get('spark.synapse.vhd.name')})"
        )
        self.cost_per_vcore_hour = cost_per_vcore_hour or getattr(self, "_autocalc_usd_cost_per_vcore_hour", None)
        self.cost_per_hour = self.get_total_cores() * self.cost_per_vcore_hour

        url = self.spark.sparkContext.uiWebUrl
        # Parse webUrl string
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        artifact_id = query.get("artifactId", [None])[0]
        # Regex for GUIDs
        guid_pattern = re.compile(r"[0-9a-fA-F-]{36}")
        guids = guid_pattern.findall(url)
        tenant_id = guids[0]  # after /sparkui/
        activity_id = guids[2]  # after /activities/

        self.extended_engine_metadata.update(
            {
                "spark_history_url": f"https://{self.spark_configs['spark.trident.pbienv'].lower()}.powerbi.com/workloads/de-ds/sparkmonitor/{artifact_id}/{activity_id}?ctid={tenant_id}",
                "cost_per_hour": Decimal(self.cost_per_hour).quantize(Decimal("0.0000")),
                "capacity_id": self.capacity_id,
            }
        )

        spark_configs_to_log = {
            k: v
            for k, v in self.spark_configs.items()
            if k
            in [
                "spark.sql.parquet.vorder.enabled",
                "spark.sql.parquet.vorder.default",
                "spark.microsoft.delta.optimizeWrite.enabled",
                "spark.microsoft.delta.optimizeWrite.binSize",
                "spark.synapse.vegas.useCache",
                "spark.synapse.vegas.cacheSize",
                "spark.native.enabled",
                "spark.gluten.enabled",
                "spark.sql.parquet.native.writer.directWriteEnabled",
                "spark.synapse.vhd.name",
                "spark.synapse.vhd.id",
                "spark.microsoft.delta.stats.collect.extended",
                "spark.microsoft.delta.stats.injection.enabled",
                "spark.microsoft.delta.snapshot.driverMode.enabled",
                "spark.microsoft.delta.stats.collect.extended.property.setAtTableCreation",
                "spark.microsoft.delta.targetFileSize.adaptive.enabled",
                "spark.app.id",
                "spark.cluster.name",
            ]
        }

        self.extended_engine_metadata.update(spark_configs_to_log)
        self.extended_engine_metadata["collect_stats_on_write"] = str(collect_stats_on_write)

    @staticmethod
    def _resolve_collect_stats_on_write(
        collect_stats_on_write: bool,
        compute_stats_all_cols: Optional[bool],
    ) -> bool:
        if compute_stats_all_cols is not None:
            warnings.warn(
                "'compute_stats_all_cols' is deprecated for FabricSpark. Use 'collect_stats_on_write' instead.",
                DeprecationWarning,
                stacklevel=3,
            )
            return compute_stats_all_cols
        return collect_stats_on_write

    def _configure_write_stats_collection(self) -> None:
        config_value = str(self.collect_stats_on_write).lower()
        for config_name in self._WRITE_STATS_CONFIGS:
            self.spark.conf.set(config_name, config_value)
            self.spark_configs[config_name] = config_value
