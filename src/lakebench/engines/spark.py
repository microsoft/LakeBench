import os
import posixpath
from typing import Optional, Sequence

import tenacity

from .base import BaseEngine


class Spark(BaseEngine):
    """
    Generic Spark Engine

    Attributes
    ----------
    SQLGLOT_DIALECT : str, optional
        Specifies the SQL dialect to be used by the engine when SQL transpiling
        is required. Default is None.
    SUPPORTS_ONELAKE : bool
        Indicates if the engine supports OneLake URIs (e.g., abfss://workspace@onelake.dfs.fabric.microsoft.com/...)
    SUPPORTS_SCHEMA_PREP : bool
        Indicates if the engine supports schema preparation (creation of empty table with defined schema)
    SUPPORTS_MOUNT_PATH : bool
        Indicates if the engine supports mount URIs (e.g., /mnt/...)

    Methods
    -------
    get_total_cores()
        Returns the total number of CPU cores available on the system.
    get_compute_size()
        Returns a formatted string with the compute size.
    append_array_to_delta(abfss_path: str, array: list)
        Appends a list of data to a Delta table at the specified path.
    """

    SQLGLOT_DIALECT = "spark"
    SUPPORTS_MOUNT_PATH = True
    SUPPORTS_ONELAKE = True
    SUPPORTS_SCHEMA_PREP = True

    def __init__(
        self,
        schema_name: str,
        catalog_name: Optional[str] = None,
        schema_uri: Optional[str] = None,
        spark_measure_telemetry: bool = False,
        cost_per_vcore_hour: Optional[float] = None,
        compute_stats_all_cols: bool = False,
    ):
        """
        Parameters
        ----------
        schema_name : str
            The name of the schema (database) to use within the catalog.
        catalog_name : str, optional
            The name of the catalog (metastore) to use. If None, the default catalog is used.
        schema_uri : str, optional
            The URI of the schema (database) to use within the catalog.
        spark_measure_telemetry : bool, default False
            Whether to enable sparkmeasure telemetry for performance measurement.
        cost_per_vcore_hour : float, optional
            The cost per vCore hour for the Spark cluster. If None, cost calculations are auto calculated
            where possible.
        compute_stats_all_cols : bool, default False
            .. deprecated::
                Use the ``analyze`` parameter on the benchmark class instead.
                This parameter will be removed in a future release.
            Whether to compute statistics for all columns after each table is loaded.
        """
        super().__init__(schema_or_working_directory_uri=schema_uri)

        if compute_stats_all_cols:
            import warnings

            warnings.warn(
                "The 'compute_stats_all_cols' parameter on Spark is deprecated. "
                "Use the 'analyze' parameter on the benchmark class (e.g., TPCH(..., analyze=True)) instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        import pyspark.sql.functions as sf
        from pyspark.sql import SparkSession

        self.sf = sf

        self.spark = SparkSession.builder
        if self.runtime == "local_unknown":
            warehouse_dir = posixpath.dirname(schema_uri.rstrip("/").rstrip("\\"))
            self.spark = (
                self.spark.master("local[*]")
                .config("spark.sql.warehouse.dir", warehouse_dir)
                .config("spark.driver.host", "localhost")
                .config("spark.driver.bindAddress", "localhost")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
                .config("spark.sql.catalogImplementation", "hive")
            )
            if self.operating_system == "windows":
                # Windows-specific configurations to avoid native IO issues
                self.spark = self.spark.config("spark.hadoop.io.native.lib.available", "false").config(
                    "spark.hadoop.fs.file.impl.disable.cache", "true"
                )

        self.spark = self.spark.getOrCreate()

        self.schema_uri = schema_uri
        if spark_measure_telemetry:
            try:
                from sparkmeasure import StageMetrics

                self.capture_metrics = StageMetrics(self.spark)
            except ModuleNotFoundError:
                raise ModuleNotFoundError(
                    "`sparkmeasure` is not installed, either disable the `spark_measure_telemetry` flag, run `%pip install sparkmeasure==0.24.0`, or install LakeBench with the sparkmeasure option: `%pip install lakebench[sparkmeasure]`."
                )
        self.spark_measure_telemetry = spark_measure_telemetry

        self.version: str = self.spark.sparkContext.version

        self.catalog_name = catalog_name if self.runtime != "local_unknown" else None
        self.schema_name = schema_name
        self.full_catalog_schema_reference: str = (
            f"`{self.catalog_name}`.`{self.schema_name}`" if catalog_name else f"`{self.schema_name}`"
        )
        self.cost_per_vcore_hour = cost_per_vcore_hour
        self.spark_configs = self.__get_spark_session_configs()
        self.extended_engine_metadata.update(
            {
                "parquet.block.size": self.spark.sparkContext._jsc.hadoopConfiguration().get("parquet.block.size")
                or "",
            }
        )
        spark_configs_to_log = {
            k: v
            for k, v in self.spark_configs.items()
            if k
            in [
                "spark.executor.memory",
                "spark.databricks.delta.optimizeWrite.enabled",
                "spark.databricks.delta.optimizeWrite.binSize",
                "spark.sql.autoBroadcastJoinThreshold",
                "spark.sql.sources.parallelPartitionDiscovery.parallelism",
                "spark.sql.cbo.enabled",
                "spark.sql.shuffle.partitions",
                "spark.task.cpus",
                "spark.sql.parquet.compression.codec",
            ]
        }

        self.extended_engine_metadata.update(spark_configs_to_log)

        self.compute_stats_all_cols = compute_stats_all_cols
        self.run_analyze_after_load = self.compute_stats_all_cols

    def __get_spark_session_configs(self) -> dict:
        """
        Extract all Spark session configuration parameters.

        Returns
        -------
        dict
            Dictionary containing all Spark configuration key-value pairs.
        """
        scala_map = self.spark.conf._jconf.getAll()
        spark_conf_dict = {}

        iterator = scala_map.iterator()
        while iterator.hasNext():
            entry = iterator.next()
            key = entry._1()
            value = entry._2()
            spark_conf_dict[key] = value
        return spark_conf_dict

    # Use tenacity to retry on NativeIO error common in spark running on local Windows
    @tenacity.retry(
        retry=tenacity.retry_if_exception(
            lambda e: "java.lang.UnsatisfiedLinkError" in str(e) and "NativeIO$POSIX.stat" in str(e)
        ),
        stop=tenacity.stop_after_attempt(2),
    )
    def create_schema_if_not_exists(self, drop_before_create: bool = True):
        """
        Prepare an empty schema in the lakehouse.

        Parameters
        ----------
        drop_before_create : bool, default True
            Whether to drop the schema before creating it if it already exists.

        Notes
        -----
        Uses tenacity retry decorator to handle NativeIO errors common in Spark
        running on local Windows environments.
        """
        location_str = f"LOCATION '{self.schema_uri}'" if self.schema_uri is not None else ""

        if drop_before_create:
            self.spark.sql(f"DROP SCHEMA IF EXISTS {self.full_catalog_schema_reference} CASCADE")
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.full_catalog_schema_reference} {location_str}")
        self.spark.sql(f"USE {self.full_catalog_schema_reference}")

    def _create_empty_table(self, table_name: Optional[str], ddl: str):
        """
        Create an empty table using the provided DDL statement.

        Parameters
        ----------
        table_name : str, optional
            The name of the table to create (for compatibility, not used).
        ddl : str
            The DDL statement to execute for table creation.

        Notes
        -----
        Automatically adds 'USING delta' clause after the column definitions
        if no storage format is already specified. Handles DDLs that include
        ``PARTITIONED BY``, ``CLUSTER BY``, or ``TBLPROPERTIES`` clauses by
        inserting ``USING delta`` in the correct position.
        """
        if "using " not in ddl.lower():
            import sqlglot

            expression = sqlglot.parse_one(ddl, dialect="spark")
            create_node = expression.find(sqlglot.exp.Create)
            if create_node is not None:
                existing_props = create_node.args.get("properties")
                existing_exprs = existing_props.expressions if existing_props else []
                # Prepend so USING appears before CLUSTER BY / PARTITIONED BY
                create_node.set(
                    "properties",
                    sqlglot.exp.Properties(
                        expressions=[
                            sqlglot.exp.FileFormatProperty(this=sqlglot.exp.Literal.string("delta")),
                            *existing_exprs,
                        ]
                    ),
                )
                ddl = create_node.sql(dialect="spark", pretty=True)

        self.execute_sql_statement(ddl)

    def _convert_generic_to_specific_schema(self, generic_schema: list):
        """
        Convert a generic schema to a specific Spark schema.
        """
        from pyspark.sql.types import (
            BooleanType,
            ByteType,
            DecimalType,
            DoubleType,
            FloatType,
            IntegerType,
            LongType,
            MapType,
            ShortType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        type_mapping = {
            "STRING": StringType(),
            "TIMESTAMP": TimestampType(),
            "TINYINT": ByteType(),
            "SMALLINT": ShortType(),
            "INT": IntegerType(),
            "BIGINT": LongType(),
            "FLOAT": FloatType(),
            "DOUBLE": DoubleType(),
            "DECIMAL(18,10)": DecimalType(18, 10),  # Spark does not have a specific Decimal type, using DoubleType
            "BOOLEAN": BooleanType(),
            "MAP<STRING, STRING>": MapType(StringType(), StringType()),
        }
        return StructType([StructField(name, type_mapping[data_type], True) for name, data_type in generic_schema])

    def _append_results_to_delta(self, table_uri: str, results: list, generic_schema: list):
        """
        Append an array to a Delta table.
        """
        schema = self._convert_generic_to_specific_schema(generic_schema)
        # Use default order of columns in dictionary
        columns = list(results[0].keys())
        df = self.spark.createDataFrame(results, schema=schema).select(*columns)
        df.write.format("delta").option("mergeSchema", "true").option("delta.enableDeletionVectors", "false").option(
            "delta.autoOptimize.autoCompact", "true"
        ).option("delta.autoOptimize.optimizeWrite", "true").mode("append").save(table_uri)

    def get_total_cores(self) -> int:
        """
        Returns the total number of CPU cores available in the Spark cluster.

        Assumes that the driver and workers nodes are all the same VM size.
        """
        cores = int(
            len(
                set(
                    executor.host() for executor in self.spark.sparkContext._jsc.sc().statusTracker().getExecutorInfos()
                )
            )
            * os.cpu_count()
        )
        return cores

    def get_compute_size(self) -> str:
        """
        Returns a formatted string with the compute size.

        Assumes that the driver and workers nodes are all the same VM size.
        """
        sc_conf_dict = {key: value for key, value in self.spark.sparkContext.getConf().getAll()}
        executor_count = self.spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size() - 1
        executor_cores = int(sc_conf_dict.get("spark.executor.cores", os.cpu_count()))
        vm_host_count = len(
            set(executor.host() for executor in self.spark.sparkContext._jsc.sc().statusTracker().getExecutorInfos())
        )
        worker_count = vm_host_count - 1
        worker_cores = os.cpu_count()
        as_min_workers = (
            sc_conf_dict.get("spark.dynamicAllocation.initialExecutors")
            if sc_conf_dict.get("spark.autoscale.executorResourceInfoTag.enabled", "false") == "true"
            else None
        )
        as_max_workers = (
            sc_conf_dict.get("spark.dynamicAllocation.maxExecutors")
            if sc_conf_dict.get("spark.autoscale.executorResourceInfoTag.enabled", "false") == "true"
            else None
        )
        as_enabled = (
            True
            if as_min_workers != as_max_workers
            and sc_conf_dict.get("spark.dynamicAllocation.minExecutors", None)
            != sc_conf_dict.get("spark.dynamicAllocation.maxExecutors", None)
            else False
        )
        type = "SingleNode" if vm_host_count == 1 and not as_enabled else "MultiNode"
        workers_word = (
            "Workers" if worker_count > 1 or (as_max_workers is not None and int(as_max_workers) > 1) else "Worker"
        )
        executors_per_worker = int(executor_count / worker_count) if worker_count > 0 else 1
        executors_word = "Executors" if executors_per_worker > 1 else "Executor"
        executor_str = (
            f"({executors_per_worker} x {executor_cores}vCore {executors_word}{' ea.' if type != 'SingleNode' else ''})"
        )

        if type == "SingleNode":
            cluster_config = f"{worker_cores}vCore {type} {executor_str}"
        elif as_enabled:
            cluster_config = f"{as_min_workers}-{as_max_workers} x {worker_cores}vCore {workers_word} {executor_str}"
        else:
            cluster_config = f"{worker_count} x {worker_cores}vCore {workers_word} {executor_str}"

        return cluster_config

    def load_parquet_to_delta(
        self,
        parquet_folder_uri: str,
        table_name: str,
        table_is_precreated: bool = False,
        context_decorator: Optional[str] = None,
    ):
        df = self.spark.read.parquet(parquet_folder_uri)
        if table_is_precreated:
            df.write.insertInto(table_name, overwrite=False)
        else:
            df.write.format("delta").mode("append").saveAsTable(table_name)

        if self.run_analyze_after_load:
            self.analyze_table(table_name)

    def execute_sql_query(self, query: str, context_decorator: Optional[str] = None):
        execute_sql = self.spark.sql(query).collect()

    def execute_sql_statement(self, statement: str, context_decorator: Optional[str] = None):
        """
        Execute a SQL statement.

        Parameters
        ----------
        statement : str
            The SQL statement to execute.
        context_decorator : Optional[str]
            Not used by Spark, a job description is set instead.

        """
        self.spark.sql(statement)

    def optimize_table(self, table_name: str):
        self.spark.sql(f"OPTIMIZE {self.full_catalog_schema_reference}.{table_name}")

    def analyze_table(self, table_name: str, columns: Optional[Sequence[str]] = None):
        if isinstance(columns, (str, bytes)):
            raise TypeError("'columns' must be a sequence of column names, not a string.")
        if columns is not None and not columns:
            raise ValueError("At least one column is required for selective analysis.")
        column_clause = (
            "ALL COLUMNS"
            if columns is None
            else "COLUMNS " + ", ".join(f"`{column.replace('`', '``')}`" for column in columns)
        )
        self.spark.sql(
            f"ANALYZE TABLE {self.full_catalog_schema_reference}.{table_name} "
            f"COMPUTE STATISTICS FOR {column_clause}"
        )

    def vacuum_table(self, table_name: str, retain_hours: int = 168, retention_check: bool = True):
        self.spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", retention_check)
        self.spark.sql(f"VACUUM {self.full_catalog_schema_reference}.{table_name} RETAIN {retain_hours} HOURS")
