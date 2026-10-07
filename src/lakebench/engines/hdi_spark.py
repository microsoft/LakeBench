from typing import Optional

from .spark import Spark


class HDISpark(Spark):
    """Execute LakeBench workloads on an Azure HDInsight Spark cluster.

    Attributes
    ----------
    schema_name : str
        Name of the Spark database used for benchmark tables.
    spark_measure_telemetry : bool, optional
        Whether sparkMeasure telemetry is collected for timed operations.
    cost_per_vcore_hour : float, optional
        Retail cost per vCore hour used for estimated job cost.
    cost_per_hour : float, optional
        Total hourly compute cost, mutually exclusive with
        ``cost_per_vcore_hour``.
    tblproperties : dict, optional
        Delta table properties added to table creation statements.

    Notes
    -----
    HDInsight uses the generic Spark loading and query implementation without
    a separate catalog. Cost reporting can be configured as either a total
    hourly rate or a per-vCore hourly rate.
    """

    def __init__(
        self,
        schema_name: str,
        spark_measure_telemetry: bool = False,
        cost_per_vcore_hour: Optional[float] = None,
        cost_per_hour: Optional[float] = None,
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
        cost_per_hour : float, optional
            The total hourly cost for the Spark cluster. Mutually exclusive with ``cost_per_vcore_hour``.
        tblproperties : dict, optional
            Delta table properties to inject into CREATE TABLE statements.
        """

        super().__init__(
            catalog_name=None,
            schema_name=schema_name,
            spark_measure_telemetry=spark_measure_telemetry,
            cost_per_vcore_hour=cost_per_vcore_hour,
            cost_per_hour=cost_per_hour,
            compute_stats_all_cols=False,
            tblproperties=tblproperties,
        )
