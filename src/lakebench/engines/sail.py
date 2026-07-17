from __future__ import annotations

import os
import posixpath
from importlib.metadata import version
from typing import Any, Optional

from .base import BaseEngine
from .delta_rs import DeltaRs


class Sail(BaseEngine):
    """
    Sail Engine

    File system support: https://docs.lakesail.com/sail/main/guide/storage/
    """

    _SAIL_SERVER = None
    _SPARK = None
    SQLGLOT_DIALECT = "spark"
    SUPPORTS_ONELAKE = True
    SUPPORTS_SCHEMA_PREP = False
    SUPPORTS_MOUNT_PATH = True

    def __init__(
        self,
        schema_or_working_directory_uri: str,
        cost_per_vcore_hour: Optional[float] = None,
        storage_options: Optional[dict[str, Any]] = None,
    ):
        """
        Parameters
        ----------
        schema_or_working_directory_uri : str
            The base URI where tables are stored. This could be an arbitrary directory or
            schema path within a metastore.
        cost_per_vcore_hour : float, optional
            The cost per vCore hour for the compute runtime. If None, cost calculations are auto calculated
            where possible.
        storage_options : dict, optional
            A dictionary of storage options to pass to the engine for filesystem access. Optional as LakeBench
            will attempt to read from environment variables depeneding on the compute runtime.
        """

        super().__init__(schema_or_working_directory_uri, storage_options)
        from pysail.spark import SparkConnectServer
        from pyspark.sql import SparkSession

        self.deltars = DeltaRs()
        self.catalog_name = None
        self.schema_name = None

        # Set Sail specific environment variables
        os.environ["SAIL_OPTIMIZER__ENABLE_JOIN_REORDER"] = "true"

        if Sail._SAIL_SERVER is None:
            # create server
            server = SparkConnectServer(port=50051)
            server.start(background=True)
            Sail._SAIL_SERVER = server
        self.sail_server = Sail._SAIL_SERVER

        if Sail._SPARK is None:
            sail_server_hostname, sail_server_port = self.sail_server.listening_address
            try:
                spark = SparkSession.builder.remote(f"sc://{sail_server_hostname}:{sail_server_port}").getOrCreate()
                spark.conf.set("spark.sql.warehouse.dir", schema_or_working_directory_uri)
                Sail._SPARK = spark
            except ImportError as ex:
                raise RuntimeError(
                    "Python kernel restart is required after package upgrade.\nRun `import sys; sys.exit(0)` in a separate cell before initializing Sail engine."
                ) from ex
        self.spark = Sail._SPARK

        self.version: str = f"""{version("pysail")} (deltalake=={version("deltalake")})"""
        self.cost_per_vcore_hour = cost_per_vcore_hour or getattr(self, "_autocalc_usd_cost_per_vcore_hour", None)

    def load_parquet_to_delta(
        self,
        parquet_folder_uri: str,
        table_name: str,
        table_is_precreated: bool = False,
        context_decorator: Optional[str] = None,
    ):
        self.spark.read.parquet(parquet_folder_uri).write.format("delta").mode("overwrite").save(
            posixpath.join(self.schema_or_working_directory_uri, table_name)
        )

    def register_table(self, table_name: str):
        """
        Register a Delta table as temporary view in Sail.
        """
        self.spark.read.format("delta").load(
            posixpath.join(self.schema_or_working_directory_uri, table_name)
        ).createOrReplaceTempView(table_name)

    def execute_sql_query(self, query: str, context_decorator: Optional[str] = None):
        """
        Execute a SQL query using Sail.
        """
        execute_sql = self.spark.sql(query).toPandas()

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
        fact_table = self.deltars.DeltaTable(
            table_uri=posixpath.join(self.schema_or_working_directory_uri, table_name),
            storage_options=self.storage_options,
        )
        fact_table.optimize.compact()

    def vacuum_table(self, table_name: str, retain_hours: int = 168, retention_check: bool = True):
        fact_table = self.deltars.DeltaTable(
            table_uri=posixpath.join(self.schema_or_working_directory_uri, table_name),
            storage_options=self.storage_options,
        )
        fact_table.vacuum(retain_hours, enforce_retention_duration=retention_check, dry_run=False)
