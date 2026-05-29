from typing import Optional

from .base import BaseEngine
from .spark import Spark


class SparkConnect(Spark):
    """
    Spark Connect Engine — connects to a remote Spark cluster via Spark Connect protocol.

    Uses the `sc://` URL scheme to establish a remote SparkSession. All Spark-based
    benchmark implementations work automatically since this inherits from Spark.

    Requires: pyspark[connect]

    Parameters
    ----------
    remote : str
        Spark Connect remote URL (e.g., 'sc://localhost:15002').
    schema_name : str
        The name of the schema (database) to use.
    catalog_name : str, optional
        The name of the catalog to use.
    schema_uri : str, optional
        The URI of the schema.
    spark_measure_telemetry : bool, default False
        Whether to enable sparkmeasure telemetry.
    cost_per_vcore_hour : float, optional
        Cost per vCore hour for cost estimation.
    compute_stats_all_cols : bool, default False
        Whether to compute statistics for all columns after loading.
    """

    def __init__(
        self,
        remote: str,
        schema_name: str,
        catalog_name: Optional[str] = None,
        schema_uri: Optional[str] = None,
        spark_measure_telemetry: bool = False,
        cost_per_vcore_hour: Optional[float] = None,
        compute_stats_all_cols: bool = False,
    ):
        import pyspark.sql.functions as sf
        from pyspark.sql import SparkSession

        # Call BaseEngine.__init__ directly (skip Spark's local session creation)
        BaseEngine.__init__(self, schema_or_working_directory_uri=schema_uri)
        self.sf = sf

        # Build session with Spark Connect remote
        self.spark = SparkSession.builder.remote(remote).getOrCreate()

        self.schema_uri = schema_uri
        self._remote_url = remote

        if spark_measure_telemetry:
            try:
                from sparkmeasure import StageMetrics

                self.capture_metrics = StageMetrics(self.spark)
            except ModuleNotFoundError:
                raise ModuleNotFoundError(
                    "`sparkmeasure` is not installed. Install with: `pip install lakebench[sparkmeasure]`."
                )
        self.spark_measure_telemetry = spark_measure_telemetry

        self.version = f"spark-connect ({remote})"

        self.catalog_name = catalog_name
        self.schema_name = schema_name
        self.full_catalog_schema_reference = (
            f"`{self.catalog_name}`.`{self.schema_name}`" if catalog_name else f"`{self.schema_name}`"
        )
        self.cost_per_vcore_hour = cost_per_vcore_hour
        self.compute_stats_all_cols = compute_stats_all_cols
        self.run_analyze_after_load = self.compute_stats_all_cols
        self.spark_configs = {}
        self.extended_engine_metadata.update({"spark_connect_remote": remote})
