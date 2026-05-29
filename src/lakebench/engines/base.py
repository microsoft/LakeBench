from __future__ import annotations

import os
from abc import ABC
from decimal import Decimal
from importlib.metadata import version
from typing import Any, Optional
from urllib.parse import urlparse

import fsspec


class BaseEngine(ABC):
    """
    Abstract base class for implementing different engine types.

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

    SQLGLOT_DIALECT = None
    SUPPORTS_SCHEMA_PREP = False
    SUPPORTS_MOUNT_PATH = True
    TABLE_FORMAT = "delta"
    # Default per-statement timeout (seconds). None = engine's default
    # behavior (no Lakebench-imposed cap).
    query_timeout_seconds: Optional[int] = None

    def __init__(self, schema_or_working_directory_uri: str = None, storage_options: Optional[dict[str, Any]] = None):
        """
        Parameters
        ----------
        schema_or_working_directory_uri : str, optional
            The base URI where tables are stored. For non-Spark engines,
            tables are stored directly under this path. For Spark engines,
            this serves as the root schema path where tables are created.
        storage_options : dict, optional
            A dictionary of storage options to pass to the engine for filesystem access.
        """
        self.version: str = ""
        self.cost_per_vcore_hour: Optional[float] = None
        self.cost_per_hour: Optional[float] = None
        self.extended_engine_metadata: dict[str, str] = {}
        self.storage_options: dict[str, Any] = storage_options if storage_options is not None else {}
        self.schema_or_working_directory_uri: str = (
            schema_or_working_directory_uri.replace("file:///", "").replace(chr(92), "/")
            if schema_or_working_directory_uri
            else None
        )

        self.runtime = self._detect_runtime() if getattr(self, "runtime", None) is None else self.runtime
        self.operating_system = (
            self._detect_os() if getattr(self, "operating_system", None) is None else self.operating_system
        )

        if self.runtime == "fabric":
            import notebookutils
            import sempy.fabric as fabric

            self._notebookutils = notebookutils
            self._fabric_rest = fabric.FabricRestClient()
            workspace_id = self._notebookutils.runtime.context["currentWorkspaceId"]
            self.region = (
                self._fabric_rest.get(path_or_url=f"/v1/workspaces/{workspace_id}")
                .json()["capacityRegion"]
                .replace(" ", "")
                .lower()
            )
            self.capacity_id = self._fabric_rest.get(path_or_url=f"/v1/workspaces/{workspace_id}").json()["capacityId"]
            self._autocalc_usd_cost_per_vcore_hour = self._get_vm_retail_rate(
                self.region, "Spark Memory Optimized Capacity Usage"
            )
            self.extended_engine_metadata.update({"compute_region": self.region})
            # rust object store (used by delta-rs, polars, sail) parametrization; https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html#variant.Token
            os.environ["AZURE_STORAGE_TOKEN"] = self._notebookutils.credentials.getToken("storage")
        elif self.runtime == "synapse":
            import mssparkutils

            self._notebookutils = mssparkutils

        self.extended_engine_metadata.update({"runtime": self.runtime, "os": self.operating_system})

        if self.schema_or_working_directory_uri is None:
            self.fs = None
        elif self.runtime in ("fabric", "synapse"):
            # workaround: use notebookutils filesystem for abfs due to recursive delete issues in fsspec
            # https://github.com/developmentseed/obstore/issues/556
            self.fs = self._notebookutils.fs
            self.fs.mkdir = self.fs.mkdirs  # notebookutils users mkdirs
            if self.storage_options == {}:
                self._validate_and_set_azure_storage_config()
        elif urlparse(self.schema_or_working_directory_uri).scheme in ("s3", "gs"):
            # TODO: test s3 and gs support
            self.fs = fsspec.filesystem(urlparse(self.schema_or_working_directory_uri).scheme, **self.storage_options)
        else:
            # TODO: use FsspecStore once it's #555 and #556 are fixed
            # workaround: use original fsspec until obstore bugs are fixes:
            # * https://github.com/developmentseed/obstore/issues/555
            self.fs = fsspec.filesystem("file")

    def _detect_runtime(self) -> str:
        """
        Dynamically detect the runtime/environment.
        Returns: str - The detected service name
        """
        import os

        # Check for Microsoft Fabric or Synapse
        try:
            notebookutils = None
            utils_modules = ("notebookutils", "mssparkutils")
            for utils_module in utils_modules:
                try:
                    notebookutils = __import__(utils_module)
                except ImportError:
                    continue
            if notebookutils and hasattr(notebookutils, "runtime"):
                if hasattr(notebookutils.runtime, "context"):
                    context = notebookutils.runtime.context
                    if "productType" in context:
                        product = context["productType"].lower()
                        return product
        except Exception:
            pass

        # Check for Databricks
        try:
            dbutils = None
            if "DATABRICKS_RUNTIME_VERSION" in os.environ:
                return "databricks"
            try:
                dbutils = __import__("dbutils")
                if dbutils is not None:
                    return "databricks"
            except Exception:
                pass
        except Exception:
            pass

        # Check for Google Colab
        try:
            if "COLAB_RELEASE_TAG" in os.environ:
                return "colab"
        except ImportError:
            pass

        # Default fallback
        return "local_unknown"

    def _detect_os(self) -> str:
        """
        Dynamically detect the operating system.
        Returns: str - The detected OS name
        """
        import sys

        os_platform = sys.platform.lower()
        if os_platform.startswith("win"):
            return "windows"
        elif os_platform.startswith("linux"):
            return "linux"
        elif os_platform.startswith("darwin"):
            return "mac"
        else:
            return "unknown"

    def _validate_and_set_azure_storage_config(self) -> None:
        if not os.getenv("AZURE_STORAGE_TOKEN"):
            raise ValueError(
                """Please store bearer token as env variable `AZURE_STORAGE_TOKEN` (via `os.environ["AZURE_STORAGE_TOKEN"] = "..."`)"""
            )
        self.storage_options = {
            "bearer_token": os.getenv("AZURE_STORAGE_TOKEN"),
            "allow_invalid_certificates": "true",  # https://github.com/delta-io/delta-rs/issues/3243#issuecomment-2727206866
        }

    def _get_vm_retail_rate(self, region: str, sku: str, spot: bool = False) -> float:
        import requests

        query = f"armRegionName eq '{region}' and serviceName eq 'Microsoft Fabric' and skuName eq '{sku}'"
        api_url = "https://prices.azure.com/api/retail/prices?"
        return requests.get(api_url, params={"$filter": query}).json()["Items"][0]["retailPrice"] / 2

    def get_total_cores(self) -> int:
        """
        Returns the total number of CPU cores available on the system.
        """
        cores = os.cpu_count()
        return cores

    def get_compute_size(self) -> str:
        """
        Returns a formatted string with the compute size.
        """
        cores = self.get_total_cores()
        return f"{cores}vCore"

    def get_job_cost(self, duration_ms: int) -> Optional[Decimal]:
        """
        Returns the cost per hour for compute as a Decimal.

        If `cost_per_vcore_hour` or `cost_per_hour` is provided, it calculates the job cost.
        Otherwise, it returns None.
        """
        if self.cost_per_hour is None and self.cost_per_vcore_hour is not None:
            self.cost_per_hour = Decimal(self.get_total_cores()) * Decimal(self.cost_per_vcore_hour)
        elif self.cost_per_hour is None:
            return None

        job_cost = Decimal(self.cost_per_hour) * (Decimal(duration_ms) / Decimal(3600000))  # Convert ms to hours
        return job_cost.quantize(Decimal("0.0000000000"))  # Ensure precision matches DECIMAL(18,10)

    def get_table_columns(self, table_name: str) -> list:
        """
        Return column names for a registered/metastore table.

        Override in subclasses that support schema introspection.
        Returns an empty list by default (introspection not supported).
        """
        return []

    def list_databases(self) -> list:
        """
        Return database/schema names visible to the engine's catalog.

        Override in subclasses with a real catalog (Spark family, Livy, DuckDB).
        Engines without a catalog (e.g. Polars, Daft) raise NotImplementedError.
        """
        raise NotImplementedError(f"{type(self).__name__} does not support catalog discovery")

    def list_tables(self, database: str) -> list:
        """
        Return table names in `database` from the engine's catalog.

        Override in subclasses with a real catalog.
        """
        raise NotImplementedError(f"{type(self).__name__} does not support catalog discovery")

    def create_external_location(self, location_uri: str):
        """
        Supports engines that need to create external locations for data access.
        By default, this is a no-op and is only overridden by subclasses as needed.
        """
        pass

    def create_schema_if_not_exists(self, drop_before_create: bool = True):
        if drop_before_create:
            if self.fs.exists(self.schema_or_working_directory_uri):
                self.fs.rm(self.schema_or_working_directory_uri, True)
            self.fs.mkdir(self.schema_or_working_directory_uri)

    def _convert_generic_to_specific_schema(self, generic_schema: list):
        """
        Convert a generic schema to a specific Spark schema.
        """
        import pyarrow as pa

        type_mapping = {
            "STRING": pa.string(),
            "TIMESTAMP": pa.timestamp("us", tz="UTC"),
            "TINYINT": pa.int8(),
            "SMALLINT": pa.int16(),
            "INT": pa.int32(),
            "BIGINT": pa.int64(),
            "FLOAT": pa.float32(),
            "DOUBLE": pa.float64(),
            "DECIMAL(18,10)": pa.decimal128(18, 10),
            "BOOLEAN": pa.bool_(),
            "MAP<STRING, STRING>": pa.map_(pa.string(), pa.string()),
        }
        return pa.schema([(name, type_mapping[data_type]) for name, data_type in generic_schema])

    def _append_results_to_delta(self, table_uri: str, results: list, generic_schema: list):
        """
        Appends a list of result records to an existing Delta table.

        Parameters
        ----------
        table_uri : str
            The URI of where the table resides.
        results : list of dict
            A list of row dictionaries to append. Each dictionary may include an
            'engine_metadata' key, whose contents will be stored as a MAP<STRING,STRING>.
        generic_schema : list of tuple
            A list of (field_name, field_type) tuples defining the generic schema
            for the rows in `results`.

        Notes
        -----
        - Converts `generic_schema` into a PyArrow schema.
        - Extracts 'engine_metadata' from each row and appends it as a
          MAP<STRING,STRING> column.
        - Uses DeltaRs to write the data in "append" mode.
        - If the installed `deltalake` version is 0.x, forces the Rust engine.
        """
        import pyarrow as pa

        from ..engines.delta_rs import DeltaRs

        schema = self._convert_generic_to_specific_schema(generic_schema=generic_schema)

        # Extract engine_metadata and convert to map format, otherwise is interpreted as a Struct
        index = schema.get_field_index("engine_properties")
        schema = schema.remove(index)
        index = schema.get_field_index("execution_telemetry")
        schema = schema.remove(index)

        engine_map_data = []
        execution_map_data = []
        for result in results:
            engine_properties = result.pop("engine_properties", {})
            if engine_properties:
                map_items = [(str(k), str(v)) for k, v in engine_properties.items()]
            else:
                map_items = []

            engine_map_data.append(map_items)

            execution_telemetry = result.pop("execution_telemetry", {})
            if execution_telemetry:
                execution_map_items = [(str(k), str(v)) for k, v in execution_telemetry.items()]
            else:
                execution_map_items = []

            execution_map_data.append(execution_map_items)

        table = pa.Table.from_pylist(results, schema)
        engine_map_array = pa.array(engine_map_data, type=pa.map_(pa.string(), pa.string()))
        execution_map_array = pa.array(execution_map_data, type=pa.map_(pa.string(), pa.string()))
        table = table.append_column("engine_properties", engine_map_array)
        table = table.append_column("execution_telemetry", execution_map_array)

        if version("deltalake").startswith("0."):
            DeltaRs().write_deltalake(table_uri, table, mode="append", schema_mode="merge", engine="rust")
        else:
            DeltaRs().write_deltalake(
                table_or_uri=table_uri,
                data=table,
                mode="append",
                schema_mode="merge",
                storage_options=self.storage_options,
            )
