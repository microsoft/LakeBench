from __future__ import annotations

import os
import posixpath
from importlib.metadata import version
from typing import Any, Optional

from .base import BaseEngine
from .delta_rs import DeltaRs


class DuckDB(BaseEngine):
    """
    DuckDB Engine
    """

    SQLGLOT_DIALECT = "duckdb"
    SUPPORTS_ONELAKE = True
    SUPPORTS_SCHEMA_PREP = True
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
        import duckdb

        self.duckdb = duckdb.connect()
        self.deltars = DeltaRs()
        self.catalog_name = None
        self.schema_name = None
        if self.schema_or_working_directory_uri.startswith("abfss://"):
            self.duckdb.sql(
                f""" CREATE OR REPLACE SECRET onelake ( TYPE AZURE, PROVIDER ACCESS_TOKEN, ACCESS_TOKEN '{os.getenv("AZURE_STORAGE_TOKEN")}') ;"""
            )

        self.version: str = f"{version('duckdb')} (deltalake=={version('deltalake')})"
        self.cost_per_vcore_hour = cost_per_vcore_hour or getattr(self, "_autocalc_usd_cost_per_vcore_hour", None)

    def _create_empty_table(self, table_name: str, ddl: str):
        if not ddl.strip().startswith("CREATE OR REPLACE TABLE"):
            ddl = ddl.replace("CREATE TABLE", "CREATE OR REPLACE TABLE")
        # Create in memory table
        self.duckdb.sql(ddl)

        arrow_df = self.duckdb.sql(f"FROM {table_name}")

        # Write empty in-memory table as Delta
        self.deltars.write_deltalake(
            table_or_uri=posixpath.join(self.schema_or_working_directory_uri, table_name),
            data=arrow_df,
            mode="overwrite",
            storage_options=self.storage_options,
        )
        # Drop the in-memory table
        self.duckdb.sql(f"DROP TABLE IF EXISTS {table_name}")

    def load_parquet_to_delta(
        self,
        parquet_folder_uri: str,
        table_name: str,
        table_is_precreated: bool = False,
        context_decorator: Optional[str] = None,
    ):
        arrow_df = self.duckdb.sql(
            f""" FROM parquet_scan('{posixpath.join(parquet_folder_uri, "*.parquet")}') """
        ).fetch_record_batch()
        self.deltars.write_deltalake(
            table_or_uri=posixpath.join(self.schema_or_working_directory_uri, table_name),
            data=arrow_df,
            mode="overwrite",
            storage_options=self.storage_options,
        )

    def register_table(self, table_name: str):
        """
        Register a Delta table in DuckDB.
        """
        self.duckdb.sql(f"""
            CREATE OR REPLACE VIEW {table_name} 
            AS SELECT * FROM delta_scan('{posixpath.join(self.schema_or_working_directory_uri, table_name)}')
        """)

    def execute_sql_query(self, query: str, context_decorator: Optional[str] = None):
        """
        Execute a SQL query using DuckDB.
        """
        result = self.duckdb.sql(query).arrow()

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
