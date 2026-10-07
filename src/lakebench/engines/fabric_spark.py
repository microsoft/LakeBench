import re
import warnings
from decimal import Decimal
from typing import Optional
from urllib.parse import parse_qs, urlparse

from .spark import Spark


class FabricSpark(Spark):
    """Execute LakeBench workloads in a Microsoft Fabric Spark runtime.

    Attributes
    ----------
    lakehouse_name : str
        Name of the Fabric Lakehouse used as the Spark catalog.
    lakehouse_schema_name : str
        Name of the Lakehouse schema used for benchmark tables.
    spark_measure_telemetry : bool, optional
        Whether to collect sparkMeasure execution telemetry.
    cost_per_vcore_hour : float, optional
        Retail cost per vCore hour used for estimated job cost.
    cost_per_hour : float, optional
        Total hourly compute cost, mutually exclusive with
        ``cost_per_vcore_hour``.
    collect_stats_on_write : bool, optional
        Whether Fabric extended Delta statistics are collected during writes.
    compute_stats_all_cols : bool, optional
        Deprecated alias for ``collect_stats_on_write``.
    tblproperties : dict, optional
        Delta table properties added to table creation statements.

    Notes
    -----
    The engine uses the Fabric-attached Lakehouse as its catalog. Fabric write
    statistics are configured at the Spark session level and replace a
    separate analyze-after-load step.
    """

    _FAST_OPTIMIZE_CONFIG = "spark.microsoft.delta.optimize.fast.enabled"
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
        cost_per_hour: Optional[float] = None,
        collect_stats_on_write: bool = True,
        compute_stats_all_cols: Optional[bool] = None,
        tblproperties: Optional[dict] = None,
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
        cost_per_hour : float, optional
            The total hourly cost for the Spark cluster. Mutually exclusive with ``cost_per_vcore_hour``.
        collect_stats_on_write : bool, default True
            Whether Fabric Delta extended statistics should be collected during write operations.
        compute_stats_all_cols : bool, optional
            Deprecated alias for ``collect_stats_on_write``. When provided, it takes precedence.
        tblproperties : dict, optional
            Delta table properties to inject into CREATE TABLE statements.
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
            cost_per_hour=cost_per_hour,
            compute_stats_all_cols=False,
            tblproperties=tblproperties,
        )

        self.collect_stats_on_write = collect_stats_on_write
        self.compute_stats_all_cols = collect_stats_on_write
        self.run_analyze_after_load = False
        self._configure_write_stats_collection()

        self.version: str = (
            f"{self.spark.sparkContext.version} (vhd_name=={self.spark.conf.get('spark.synapse.vhd.name')})"
        )
        self._configure_cost_inputs(
            cost_per_vcore_hour=cost_per_vcore_hour,
            cost_per_hour=cost_per_hour,
            automatic_cost_per_vcore_hour=getattr(self, "_autocalc_usd_cost_per_vcore_hour", None),
        )
        self._materialize_cost_per_hour()

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
                "cost_per_hour": Decimal(str(self.cost_per_hour)).quantize(Decimal("0.0000")),
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

    def optimize_table(self, table_name: str):
        fast_optimize_value = self.spark.conf.get(self._FAST_OPTIMIZE_CONFIG, None)
        if str(fast_optimize_value).lower() != "true":
            return super().optimize_table(table_name)

        self.spark.conf.set(self._FAST_OPTIMIZE_CONFIG, "false")
        try:
            return super().optimize_table(table_name)
        finally:
            self.spark.conf.set(self._FAST_OPTIMIZE_CONFIG, fast_optimize_value)
