import os
import pathlib
import posixpath
from importlib.metadata import version
from typing import Optional

from ..utils.path_utils import _REMOTE_SCHEMES, to_file_uri
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

    def load_parquet_to_delta(
        self,
        parquet_folder_uri: str,
        table_name: str,
        table_is_precreated: bool = False,
        context_decorator: Optional[str] = None,
    ):
        table_df = self.daft.read_parquet(posixpath.join(parquet_folder_uri))
        raw_path = posixpath.join(self.schema_or_working_directory_uri, table_name)
        is_local = not any(raw_path.startswith(s) for s in _REMOTE_SCHEMES)
        # Daft 0.7.x requires the target directory to exist for local paths
        if is_local:
            pathlib.Path(raw_path).mkdir(parents=True, exist_ok=True)
        table_uri = to_file_uri(raw_path)
        table_df.write_deltalake(
            table=table_uri,
            mode="overwrite",
        )

    def register_table(self, table_name: str):
        """
        Register a Delta table DataFrame in Daft.

        On local paths, Daft 0.7.x has a Windows path-handling bug in its object
        store that corrupts drive-letter paths (``C:/...`` → ``/C:/...``) when
        reading parquet files referenced by the Delta log.  Workaround: use
        delta-rs to resolve the current snapshot's file URIs, then scan via
        ``read_parquet`` which handles Windows paths correctly.
        """
        table_path = posixpath.join(self.schema_or_working_directory_uri, table_name)
        is_local = not any(table_path.startswith(s) for s in _REMOTE_SCHEMES)
        if is_local:
            from deltalake import DeltaTable

            file_uris = DeltaTable(table_path).file_uris()
            globals()[table_name] = self.daft.read_parquet(file_uris)
        else:
            globals()[table_name] = self.daft.read_deltalake(to_file_uri(table_path))

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
