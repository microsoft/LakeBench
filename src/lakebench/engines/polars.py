from __future__ import annotations

import posixpath
from importlib.metadata import version
from typing import Any, Optional

from .base import BaseEngine
from .delta_rs import DeltaRs


class Polars(BaseEngine):
    """
    Polars Engine
    """

    SQLGLOT_DIALECT = "duckdb"
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
        import polars as pl

        self.pl = pl
        self.deltars = DeltaRs()
        self.catalog_name = None
        self.schema_name = None
        self.sql = pl.SQLContext()
        self.version: str = f"{version('polars')} (deltalake=={version('deltalake')})"
        self.cost_per_vcore_hour = cost_per_vcore_hour or getattr(self, "_autocalc_usd_cost_per_vcore_hour", None)

    def load_parquet_to_delta(
        self,
        parquet_folder_uri: str,
        table_name: str,
        table_is_precreated: bool = False,
        context_decorator: Optional[str] = None,
    ):
        table_df = self.pl.scan_parquet(
            posixpath.join(parquet_folder_uri, "*.parquet"), storage_options=self.storage_options
        )
        # Cast any Decimal columns to Float64 before collecting — TPC-DS datagen can
        # produce values that exceed the column's declared precision at small scale factors,
        # causing a Rust-level panic in Polars strict decimal enforcement.
        decimal_cols = [name for name, dtype in table_df.schema.items() if str(dtype).startswith("Decimal")]
        if decimal_cols:
            table_df = table_df.with_columns([self.pl.col(c).cast(self.pl.Float64, strict=False) for c in decimal_cols])
        table_df.collect(engine="streaming").write_delta(
            posixpath.join(self.schema_or_working_directory_uri, table_name),
            mode="overwrite",
            storage_options=self.storage_options,
        )

    def register_table(self, table_name: str):
        """
        Register a Delta table LazyFrame in Polars.
        """
        df = self.pl.scan_delta(
            posixpath.join(self.schema_or_working_directory_uri, table_name), storage_options=self.storage_options
        )
        self.sql.register(table_name, df)

    def execute_sql_query(self, query: str, context_decorator: Optional[str] = None):
        """
        Execute a SQL query using Polars.
        """
        result = self.sql.execute(query).collect(engine="streaming")

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
