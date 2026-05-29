from decimal import Decimal
from typing import Optional

from .spark import Spark


class SynapseSpark(Spark):
    """
    Synapse Spark Engine
    """

    def __init__(
        self,
        schema_name: str,
        schema_uri: Optional[str] = None,
        spark_measure_telemetry: bool = False,
        cost_per_vcore_hour: Optional[float] = None,
    ):
        """
        Parameters
        ----------
        schema_name : str
            The name of the schema (database) to use within the catalog.
        schema_uri : str, optional
            The URI of the schema (database) to use within the catalog.
        spark_measure_telemetry : bool, default False
            Whether to enable sparkmeasure telemetry for performance measurement.
        cost_per_vcore_hour : float, optional
            The cost per vCore hour for the Spark cluster. If None, cost calculations are auto calculated
            where possible.
        """

        super().__init__(
            catalog_name=None,
            schema_name=schema_name,
            schema_uri=schema_uri,
            spark_measure_telemetry=spark_measure_telemetry,
            cost_per_vcore_hour=cost_per_vcore_hour,
            compute_stats_all_cols=False,
        )

        if self.runtime != "synapse":
            raise RuntimeError("This engine is only supports Synapse Spark Pools.")
        self.version: str = (
            f"{self.spark.sparkContext.version} (vhd_name=={self.spark.conf.get('spark.synapse.vhd.name')})"
        )
        region = self.spark.conf.get("spark.cluster.region")
        self.cost_per_vcore_hour = (
            cost_per_vcore_hour
            if cost_per_vcore_hour is not None
            else self._get_vm_retail_rate(region=region, sku="vCore")
        )
        self.cost_per_hour = self.get_total_cores() * self.cost_per_vcore_hour

        self.extended_engine_metadata.update(
            {
                "spark_history_url": self.spark_configs["spark.tracking.webUrl"],
                "cost_per_hour": Decimal(self.cost_per_hour).quantize(Decimal("0.0000")),
                "compute_region": region,
            }
        )

        spark_configs_to_log = {
            k: v
            for k, v in self.spark_configs.items()
            if k
            in [
                "spark.microsoft.delta.optimizeWrite.enabled",
                "spark.microsoft.delta.optimizeWrite.binSize",
                "spark.synapse.vegas.useCache",
                "spark.synapse.vegas.cacheSize",
                "spark.synapse.vhd.name",
                "spark.synapse.vhd.id",
                "spark.app.id",
                "spark.cluster.name",
            ]
        }

        self.extended_engine_metadata.update(spark_configs_to_log)

    def _get_vm_retail_rate(self, region: str, sku: str, spot: bool = False) -> float:
        import requests

        query = f"armRegionName eq '{region}' and serviceName eq 'Azure Synapse Analytics' and productName eq 'Azure Synapse Analytics Serverless Apache Spark Pool - Memory Optimized'"
        api_url = "https://prices.azure.com/api/retail/prices?"
        return requests.get(api_url, params={"$filter": query}).json()["Items"][0]["retailPrice"]
