import logging
import posixpath
import re
import struct
from contextlib import closing
from typing import Iterable, Mapping, Optional, Sequence

from ..utils.path_utils import abfss_to_https
from .base import BaseEngine

logger = logging.getLogger(__name__)


class FabricDataWarehouseQueryCancelledError(RuntimeError):
    """Raised when Fabric or ODBC reports that a query was cancelled."""


class FabricDataWarehouse(BaseEngine):
    """
    Fabric Data Warehouse Engine

    Attributes
    ----------
    SQLGLOT_DIALECT : str
        Specifies the SQL dialect to be used by the engine when SQL transpiling
        is required.
    SUPPORTS_ONELAKE : bool
        Indicates if the engine supports OneLake URIs
        (e.g., abfss://workspace@onelake.dfs.fabric.microsoft.com/...)
    SUPPORTS_SCHEMA_PREP : bool
        Indicates if the engine supports schema preparation (creation of empty table with defined schema)
    SUPPORTS_MOUNT_PATH : bool
        Indicates if the engine supports mount URIs (e.g., /mnt/...)

    Notes
    -----
    Job cost is not reported. Capacity cost does not reliably attribute to an
    individual query or load, so `estimated_retail_job_cost` is left unset
    rather than populated with a misleading figure.
    """

    SQLGLOT_DIALECT = "fabric"
    SUPPORTS_MOUNT_PATH = False
    SUPPORTS_ONELAKE = True
    SUPPORTS_SCHEMA_PREP = True
    #: Every line the TPC generators emit ends with the field delimiter followed by a
    #: line feed, so folding the trailing pipe into a multi-character row terminator
    #: consumes it without declaring a throwaway column. COPY INTO prefixes a carriage
    #: return to a literal ``\n``, so the terminator must be given in hex, and hex
    #: terminators are written as one unbroken string (``|`` = 0x7C, ``\n`` = 0x0A).
    _NATIVE_ROW_TERMINATOR = "0x7C0A"
    _SQL_COPT_SS_ACCESS_TOKEN = 1256
    _POOL_RECYCLE_SECONDS = 45 * 60
    _CANCELLATION_ERROR_CODE = 3617
    _CANCELLATION_PATTERNS = (
        "operation canceled",
        "operation cancelled",
        "query was canceled",
        "query was cancelled",
        "query canceled",
        "query cancelled",
        "request was canceled",
        "request was cancelled",
        "client disconnected",
        "client has disconnected",
        "client closed the connection",
    )

    def __init__(
        self,
        warehouse_name: str,
        warehouse_server: str,
        schema_name: str,
    ):
        """
        Parameters
        ----------
        warehouse_name : str
            The name of the Fabric Data Warehouse.
        warehouse_server : str
            The SQL connection string of the Fabric Data Warehouse.
        schema_name : str
            The name of the schema to use within the warehouse.
        """
        super().__init__()

        if " " in warehouse_name:
            raise ValueError("`warehouse_name` attribute should not contain spaces.")
        if " " in schema_name:
            raise ValueError("`schema_name` attribute should not contain spaces.")
        self.warehouse_name = warehouse_name
        self.warehouse_server = warehouse_server
        self.schema_name = schema_name
        self._create_connection()

        self.fabric_sku = self._get_fabric_sku()

        self.version, version_str = self._get_sql_version()

        is_vorder_enabled = self.execute_sql_query(
            f"SELECT is_vorder_enabled FROM sys.databases WHERE name = '{warehouse_name}'",
            return_data=True,
        ).iloc[0, 0]

        self.extended_engine_metadata.update(
            {
                "warehouse_server": warehouse_server,
                "warehouse_name": warehouse_name,
                "fabric_sku": self.fabric_sku,
                "version": version_str,
                "is_vorder_enabled": is_vorder_enabled,
            }
        )

    def _get_sql_version(self):
        version_str = self.execute_sql_query("SELECT @@VERSION", return_data=True).iloc[0, 0]
        match = re.search(r"\d+\.\d+\.\d+\.\d+", version_str)
        version = match.group(0) if match else "Unknown"
        return version, version_str

    def _get_fabric_sku(self) -> str:
        capacities_response = self._fabric_rest.get("/v1/capacities")
        if not capacities_response.ok:
            logger.warning(
                "Unable to retrieve Fabric capacity metadata; the SKU is unavailable (HTTP %s).",
                capacities_response.status_code,
            )
            return "Unknown"

        capacities = capacities_response.json().get("value", [])
        capacity = next(
            (capacity for capacity in capacities if capacity.get("id") == self.capacity_id),
            None,
        )
        if capacity is None:
            logger.warning("The workspace capacity is not visible to the current identity; the SKU is unavailable.")
            return "Unknown"

        fabric_sku = capacity.get("sku")
        if not fabric_sku:
            logger.warning("Fabric capacity metadata did not include a SKU.")
            return "Unknown"

        return fabric_sku

    def _create_connection(self):
        import sqlalchemy as sa

        connection_string = (
            "Driver={ODBC Driver 18 for SQL Server};"
            f"Server={self.warehouse_server};"
            f"Database={self.warehouse_name};"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
            "Connection Timeout=30"
        )

        self._connection_engine = sa.create_engine(
            sa.engine.URL.create(
                "mssql+pyodbc",
                query={"odbc_connect": connection_string},
            ),
            pool_recycle=self._POOL_RECYCLE_SECONDS,
        )

        @sa.event.listens_for(self._connection_engine, "do_connect")
        def provide_access_token(dialect, conn_rec, cargs, cparams):
            attrs_before = dict(cparams.get("attrs_before", {}))
            attrs_before[self._SQL_COPT_SS_ACCESS_TOKEN] = self._get_access_token_struct()
            cparams["attrs_before"] = attrs_before

        self.execute_sql_query("SELECT 1 AS c1", return_data=True)

    def _get_access_token_struct(self) -> bytes:
        token = self._notebookutils.credentials.getToken("https://analysis.windows.net/powerbi/api").encode("UTF-16-LE")
        return struct.pack(f"<I{len(token)}s", len(token), token)

    @staticmethod
    def _format_odbc_messages(messages: Iterable) -> str:
        formatted_messages = []
        for message in messages:
            if isinstance(message, (tuple, list)):
                formatted_messages.append(" ".join(str(part) for part in message))
            else:
                formatted_messages.append(str(message))
        return "\n".join(formatted_messages)

    @classmethod
    def _is_cancellation(cls, messages: Iterable) -> bool:
        messages = list(messages)
        for message in messages:
            if isinstance(message, (tuple, list)) and message:
                if str(message[0]).upper() == "HY008":
                    return True
                if any(str(part).strip() == str(cls._CANCELLATION_ERROR_CODE) for part in message):
                    return True

        message_text = cls._format_odbc_messages(messages).lower()
        if re.search(
            rf"(?:\berror\s+{cls._CANCELLATION_ERROR_CODE}\b|"
            rf"\({cls._CANCELLATION_ERROR_CODE}\)|"
            rf"\[{cls._CANCELLATION_ERROR_CODE}\])",
            message_text,
        ):
            return True
        return any(pattern in message_text for pattern in cls._CANCELLATION_PATTERNS)

    @classmethod
    def _raise_if_cancelled(cls, messages: Iterable) -> None:
        messages = list(messages)
        if cls._is_cancellation(messages):
            message_text = cls._format_odbc_messages(messages)
            raise FabricDataWarehouseQueryCancelledError(f"Fabric Data Warehouse query was cancelled: {message_text}")

    def create_schema_if_not_exists(self, drop_before_create: bool = True):
        from sqlalchemy import text

        with self._connection_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
            if drop_before_create:
                objects_df = self.execute_sql_query(
                    f"""
                    SELECT
                        o.name AS ObjectName,
                        o.type_desc AS ObjectType
                    FROM sys.objects o
                    JOIN sys.schemas s ON o.schema_id = s.schema_id
                    WHERE o.type IN ('U', 'V')
                    AND s.name = '{self.schema_name}'
                    """,
                    return_data=True,
                )

                for _, row in objects_df.iterrows():
                    if row["ObjectType"] == "USER_TABLE":
                        connection.execute(text(f"DROP TABLE IF EXISTS {self.schema_name}.{row['ObjectName']}"))
                    elif row["ObjectType"] == "VIEW":
                        connection.execute(text(f"DROP VIEW IF EXISTS {self.schema_name}.{row['ObjectName']}"))

                connection.execute(text(f"DROP SCHEMA IF EXISTS {self.schema_name}"))

            connection.execute(text(f"CREATE SCHEMA {self.schema_name}"))

    def _create_empty_table(self, table_name: Optional[str], ddl: str):
        self.execute_sql_query(ddl)

    def get_total_cores(self) -> Optional[int]:
        return None

    def get_compute_size(self) -> str:
        return self.fabric_sku

    def load_parquet_to_delta(
        self,
        parquet_folder_uri: str,
        table_name: str,
        table_is_precreated: bool = False,
        context_decorator: Optional[str] = None,
        column_name_mapping: Optional[Mapping[str, str]] = None,
    ):
        https_parquet_folder_path = abfss_to_https(parquet_folder_uri)
        parquet_glob = posixpath.join(https_parquet_folder_path, "*.parquet")

        if column_name_mapping:
            source_query = f"SELECT TOP 0 * FROM OPENROWSET(BULK '{parquet_glob}', FORMAT = 'PARQUET') AS source"
            source_columns = list(self.execute_sql_query(source_query, return_data=True).columns)
            resolved_mapping = self._resolve_column_name_mapping(table_name, source_columns, column_name_mapping)
            projection = ", ".join(
                f"[{column}] AS [{resolved_mapping[column]}]" if column in resolved_mapping else f"[{column}]"
                for column in source_columns
            )
            select_query = f"SELECT {projection} FROM OPENROWSET(BULK '{parquet_glob}', FORMAT = 'PARQUET') AS source"
            if table_is_precreated:
                sql = f"INSERT INTO {self.schema_name}.{table_name} {select_query}"
            else:
                sql = f"CREATE TABLE {self.schema_name}.{table_name} AS {select_query}"
        elif table_is_precreated:
            sql = f"""
            COPY INTO {self.schema_name}.{table_name}
            FROM '{parquet_glob}'
            WITH (
                FILE_TYPE = 'PARQUET'
            )
            """
        else:
            sql = f"""
            CREATE TABLE {self.schema_name}.{table_name}
            AS SELECT * FROM OPENROWSET(BULK '{parquet_glob}')
            """

        return self.execute_sql_query(sql, context_decorator=context_decorator, return_data=False)

    def load_delimited_to_delta(
        self,
        folder_uri: str,
        table_name: str,
        columns: Sequence[tuple],
        file_pattern: str,
        table_is_precreated: bool = False,
        context_decorator: Optional[str] = None,
        column_name_mapping: Optional[Mapping[str, str]] = None,
    ):
        from ..utils.schema_utils import NATIVE_DELIMITER

        if not table_is_precreated:
            # COPY INTO has no AUTO_CREATE_TABLE for CSV, and the native format carries
            # no header or types, so the target must come from the benchmark's DDL.
            raise ValueError(
                f"{type(self).__name__} requires the target table to be created from the benchmark DDL "
                "before loading the native delimited format."
            )

        # The native columns are derived from the same DDL that created the target table,
        # so COPY INTO's positional field mapping already lines up. A non-empty mapping
        # would mean the two disagree, which would silently load columns out of order.
        resolved_mapping = self._resolve_column_name_mapping(
            table_name, [name for name, _ in columns], column_name_mapping
        )
        if resolved_mapping:
            raise ValueError(
                f"Cannot load table '{table_name}' from the native delimited format: the DDL uses legacy "
                f"column names {sorted(resolved_mapping)} that COPY INTO cannot remap positionally."
            )

        glob_path = posixpath.join(abfss_to_https(folder_uri), file_pattern)
        sql = f"""
        COPY INTO {self.schema_name}.{table_name}
        FROM '{glob_path}'
        WITH (
            FILE_TYPE = 'CSV',
            FIELDTERMINATOR = '{NATIVE_DELIMITER}',
            ROWTERMINATOR = '{self._NATIVE_ROW_TERMINATOR}',
            FIRSTROW = 1,
            ENCODING = 'UTF8'
        )
        """
        return self.execute_sql_query(sql, context_decorator=context_decorator, return_data=False)

    def execute_sql_query(self, query: str, context_decorator: Optional[str] = None, return_data: bool = False):
        if context_decorator:
            query = f"{query}\nOPTION (LABEL = '{context_decorator}')"

        if return_data:
            import pandas as pd
            import sqlalchemy as sa

            with self._connection_engine.connect() as connection:
                try:
                    return pd.read_sql_query(query, connection)
                except sa.exc.DBAPIError as exc:
                    if self._is_cancellation(exc.orig.args):
                        connection.invalidate()
                        raise FabricDataWarehouseQueryCancelledError(
                            f"Fabric Data Warehouse query was cancelled: {exc.orig}"
                        ) from exc
                    raise

        import pyodbc

        with closing(self._connection_engine.raw_connection()) as raw_conn:
            messages = []
            try:
                with closing(raw_conn.cursor()) as cursor:
                    cursor.execute(query)

                    while True:
                        messages.extend(cursor.messages or [])
                        self._raise_if_cancelled(messages)

                        has_next_result = cursor.nextset()
                        messages.extend(cursor.messages or [])
                        self._raise_if_cancelled(messages)
                        if not has_next_result:
                            break

                    raw_conn.commit()
            except FabricDataWarehouseQueryCancelledError:
                raw_conn.invalidate()
                raise
            except pyodbc.Error as exc:
                error_messages = [*messages, *exc.args]
                if self._is_cancellation(error_messages):
                    raw_conn.invalidate()
                    raise FabricDataWarehouseQueryCancelledError(
                        f"Fabric Data Warehouse query was cancelled: {self._format_odbc_messages(error_messages)}"
                    ) from exc
                raise

        message_text = self._format_odbc_messages(messages)
        guids = list(dict.fromkeys(re.findall(r"\{([^}]*)\}", message_text)))

        return {
            "statement_id": guids[0] if guids else "",
            "distributed_statement_id": guids[1] if len(guids) > 1 else "",
        }

    def execute_sql_statement(self, sql: str, context_decorator: Optional[str] = None):
        return self.execute_sql_query(sql, context_decorator=context_decorator, return_data=False)

    def optimize_table(self, table_name: str):
        raise NotImplementedError(
            "Optimize table is not a user callable operation in Fabric Data Warehouse. "
            "Table compaction is handled automatically by the service."
        )

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        return f"[{identifier.replace(']', ']]')}]"

    def analyze_table(self, table_name: str, columns: Optional[Sequence[str]] = None):
        if isinstance(columns, (str, bytes)):
            raise TypeError("'columns' must be a sequence of column names, not a string.")

        analysis_mode = "selective"
        if columns is None:
            analysis_mode = "full"
            schema_name = self.schema_name.replace("'", "''")
            escaped_table_name = table_name.replace("'", "''")
            column_df = self.execute_sql_query(
                f"""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{schema_name}'
                  AND TABLE_NAME = '{escaped_table_name}'
                ORDER BY ORDINAL_POSITION
                """,
                return_data=True,
            )
            columns = column_df["COLUMN_NAME"].tolist()

        if not columns:
            raise ValueError(f"No columns were provided or found for {self.schema_name}.{table_name}.")

        quoted_schema = self._quote_identifier(self.schema_name)
        quoted_table = self._quote_identifier(table_name)
        for column in columns:
            statistic_name = self._quote_identifier(f"lakebench_{table_name}_{column}")
            quoted_column = self._quote_identifier(column)
            self.execute_sql_statement(
                f"CREATE STATISTICS {statistic_name} ON {quoted_schema}.{quoted_table} ({quoted_column}) WITH FULLSCAN"
            )

        return {
            "statistics_mode": analysis_mode,
            "statistics_created": str(len(columns)),
        }

    def vacuum_table(self, table_name: str, retain_hours: int = 168, retention_check: bool = True):
        raise NotImplementedError(
            "Vacuum table is not a user callable operation in Fabric Data Warehouse. "
            "File cleanup is handled automatically by the service."
        )


#: Backwards-compatible aliases for the engine's former name.
FabricWarehouse = FabricDataWarehouse
FabricWarehouseQueryCancelledError = FabricDataWarehouseQueryCancelledError
