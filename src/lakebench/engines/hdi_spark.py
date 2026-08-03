from typing import Optional

from .spark import Spark


class HDISpark(Spark):
    """
    HDInsight Spark Engine
    """

    def __init__(
        self,
        schema_name: str,
        spark_measure_telemetry: bool = False,
        cost_per_vcore_hour: Optional[float] = None,
        tblproperties: Optional[dict] = None,
    ):
        """
        Parameters
        ----------
        schema_name : str
            The name of the schema (database) to use within the catalog.
        spark_measure_telemetry : bool, default False
            Whether to enable sparkmeasure telemetry for performance measurement.
        cost_per_vcore_hour : float, optional
            The cost per vCore hour for the Spark cluster. If None, cost calculations are auto calculated
            where possible.
        tblproperties : dict, optional
            Delta table properties to inject into CREATE TABLE statements.
        """

        super().__init__(
            catalog_name=None,
            schema_name=schema_name,
            spark_measure_telemetry=spark_measure_telemetry,
            cost_per_vcore_hour=cost_per_vcore_hour,
            compute_stats_all_cols=False,
            tblproperties=tblproperties,
        )
