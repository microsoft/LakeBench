import os
import posixpath
from importlib.metadata import version
from typing import Mapping, Optional

from ..utils.path_utils import to_local_path
from .base import BaseEngine
from .delta_rs import DeltaRs


class Daft(BaseEngine):
    """
    Daft Engine
    """

    SQLGLOT_DIALECT = "mysql"
    SUPPORTS_ONELAKE = False
    SUPPORTS_SCHEMA_PREP = False
    SUPPORTS_MOUNT_PATH = False

    def __init__(self, schema_or_working_directory_uri: str, cost_per_vcore_hour: Optional[float] = None):
        """
        Parameters
        ----------
        schema_or_working_directory_uri : str
            The base URI where tables are stored. This could be an arbitrary directory or
            schema path within a metastore.
        cost_per_vcore_hour : float, optional
            The cost per vCore hour for the compute runtime. If None, cost calculations are auto calculated
            where possible.
        """

        super().__init__(schema_or_working_directory_uri)
        import daft
        from daft.io import AzureConfig, IOConfig

        self.daft = daft
        self.deltars = DeltaRs()
        self.catalog_name = None
        self.schema_name = None
        if self.schema_or_working_directory_uri.startswith("abfss://"):
            io_config = IOConfig(azure=AzureConfig(bearer_token=os.getenv("AZURE_STORAGE_TOKEN")))
            self.daft.set_planning_config(default_io_config=io_config)

        if not self.SUPPORTS_ONELAKE:
            if "onelake." in self.schema_or_working_directory_uri:
                raise ValueError("Daft engine does not support OneLake paths. Provide an ADLS Gen2 path instead.")

        self.version: str = f"{version('daft')} (deltalake=={version('deltalake')})"
        self.cost_per_vcore_hour = cost_per_vcore_hour or getattr(self, "_autocalc_usd_cost_per_vcore_hour", None)

    def table_path(self, table_name: str) -> str:
        """Return the Daft-compatible path/URI for *table_name*.

        Daft's object store rejects ``file:///C:/...`` on Windows, so local
        paths are handed over as bare forward-slash paths.
        """
        return to_local_path(posixpath.join(self.schema_or_working_directory_uri, table_name))

    def write_delta(self, df, table_path: str, mode: str = "overwrite"):
        """Write a Daft DataFrame to the Delta table at *table_path*."""
        df.write_deltalake(table=to_local_path(table_path), mode=mode)

    def read_delta(self, table_path: str):
        """Read the Delta table at *table_path* into a Daft DataFrame."""
        return self.daft.read_deltalake(to_local_path(table_path))

    def load_parquet_to_delta(
        self,
        parquet_folder_uri: str,
        table_name: str,
        table_is_precreated: bool = False,
        context_decorator: Optional[str] = None,
        column_name_mapping: Optional[Mapping[str, str]] = None,
    ):
        table_df = self.daft.read_parquet(to_local_path(posixpath.join(parquet_folder_uri)))
        columns = [field.name for field in table_df.schema()]
        resolved_mapping = self._resolve_column_name_mapping(table_name, columns, column_name_mapping)
        if resolved_mapping:
            table_df = table_df.select(
                *[self.daft.col(column).alias(resolved_mapping.get(column, column)) for column in columns]
            )
        self.write_delta(table_df, self.table_path(table_name), mode="overwrite")

    def load_delimited_to_delta(
        self,
        folder_uri: str,
        table_name: str,
        columns,
        file_extension: str,
        table_is_precreated: bool = False,
        context_decorator: Optional[str] = None,
        column_name_mapping: Optional[Mapping[str, str]] = None,
    ):
        import pyarrow

        from ..utils.schema_utils import NATIVE_DELIMITER, TRAILING_DELIMITER_COLUMN, schema_to_pyarrow

        arrow_schema = schema_to_pyarrow(columns)
        target_types = {field.name: self.daft.DataType.from_arrow_type(field.type) for field in arrow_schema}
        # Daft's CSV reader ignores declared decimal precision and mislabels the
        # resulting array, so decimals are read as text and converted explicitly.
        decimal_columns = {field.name for field in arrow_schema if pyarrow.types.is_decimal(field.type)}
        schema = {
            name: self.daft.DataType.string() if name in decimal_columns else data_type
            for name, data_type in target_types.items()
        }
        # The generators terminate every line with a delimiter, producing one more
        # field than the table has columns.
        schema[TRAILING_DELIMITER_COLUMN] = self.daft.DataType.string()
        table_df = self.daft.read_csv(
            to_local_path(posixpath.join(folder_uri, f"*.{file_extension}")),
            infer_schema=False,
            schema=schema,
            delimiter=NATIVE_DELIMITER,
            has_headers=False,
            quote=None,
            escape_char=None,
        ).exclude(TRAILING_DELIMITER_COLUMN)
        if decimal_columns:
            table_df = table_df.select(
                *[
                    self.daft.col(name).cast(target_types[name]) if name in decimal_columns else self.daft.col(name)
                    for name in target_types
                ]
            )
        daft_columns = [field.name for field in table_df.schema()]
        resolved_mapping = self._resolve_column_name_mapping(table_name, daft_columns, column_name_mapping)
        if resolved_mapping:
            table_df = table_df.select(
                *[self.daft.col(column).alias(resolved_mapping.get(column, column)) for column in daft_columns]
            )
        self.write_delta(table_df, self.table_path(table_name), mode="overwrite")

    def register_table(self, table_name: str):
        """
        Register a Delta table DataFrame in Daft.
        """
        globals()[table_name] = self.read_delta(self.table_path(table_name))

    def execute_sql_query(self, query: str, context_decorator: Optional[str] = None):
        """
        Execute a SQL query using Daft.
        """
        result = self.daft.sql(query).collect()

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
