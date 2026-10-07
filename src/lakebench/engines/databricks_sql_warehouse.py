from __future__ import annotations

import logging
import posixpath
import warnings
from datetime import datetime
from decimal import Decimal
from typing import Mapping, Optional, Sequence

from .base import BaseEngine
from .databricks_pricing import (
    DatabricksPricingError,
    estimate_sql_warehouse_cost,
    normalize_cloud_provider,
)

logger = logging.getLogger(__name__)

_AWS_SERVERLESS_SQL_REGION_SUFFIXES = {
    "ap-northeast-1": "AP_TOKYO",
    "ap-northeast-2": "AP_SEOUL",
    "ap-south-1": "AP_MUMBAI",
    "ap-southeast-1": "AP_SINGAPORE",
    "ap-southeast-2": "AP_SYDNEY",
    "ap-southeast-3": "AP_JAKARTA",
    "ca-central-1": "CANADA",
    "eu-central-1": "EUROPE_FRANKFURT",
    "eu-west-1": "EUROPE_IRELAND",
    "eu-west-2": "EUROPE_LONDON",
    "eu-west-3": "EUROPE_FRANCE",
    "sa-east-1": "SA_BRAZIL",
    "us-east-1": "US_EAST_N_VIRGINIA",
    "us-east-2": "US_EAST_OHIO",
    "us-west-1": "US_WEST_CALIFORNIA",
    "us-west-2": "US_WEST_OREGON",
}


class DatabricksSQLWarehouse(BaseEngine):
    """Databricks SQL Warehouse engine."""

    SQLGLOT_DIALECT = "spark"
    SUPPORTS_MOUNT_PATH = False
    SUPPORTS_ONELAKE = False
    SUPPORTS_SCHEMA_PREP = True
    REQUIRED_MODULES = ("databricks.sql", "pandas", "requests")
    INSTALL_EXTRA = "databricks_sql_warehouse"

    def __init__(
        self,
        server_hostname: str,
        warehouse_name: str,
        catalog_name: str,
        schema_name: str,
        access_token: str,
        schema_uri: Optional[str] = None,
        enable_result_caching: bool = False,
        cost_per_hour: Optional[float] = None,
        dbu_rate: Optional[float] = None,
        compute_stats_all_cols: bool = False,
    ):
        super().__init__(schema_or_working_directory_uri=schema_uri)

        if compute_stats_all_cols:
            warnings.warn(
                "The 'compute_stats_all_cols' parameter on DatabricksSQLWarehouse is deprecated. "
                "Use the benchmark's 'analyze' parameter instead.",
                DeprecationWarning,
                stacklevel=2,
            )

        self.server_hostname = server_hostname
        self.warehouse_name = warehouse_name
        self.catalog_name = catalog_name
        self.schema_name = schema_name
        self.schema_uri = schema_uri
        self.access_token = access_token
        self.full_catalog_schema_reference = f"`{catalog_name}`.`{schema_name}`"
        self.compute_stats_all_cols = compute_stats_all_cols
        self.run_analyze_after_load = compute_stats_all_cols

        warehouse = self._get_warehouse()
        self.warehouse_id = warehouse["id"]
        self.warehouse_path = warehouse["odbc_params"]["path"]
        self.warehouse_size = warehouse["cluster_size"]
        self.warehouse_type = str(warehouse.get("warehouse_type", "UNKNOWN"))
        self.serverless = bool(
            warehouse.get("enable_serverless_compute", False) or "SERVERLESS" in self.warehouse_type.upper()
        )
        self.connection = self._create_connection()
        metastore_cloud, metastore_region, self.version = self._get_runtime_metadata()
        self.cloud_provider = normalize_cloud_provider(metastore_cloud)
        self.region = metastore_region

        if not enable_result_caching:
            self.execute_sql_statement("SET use_cached_result = False")

        workload_type = (
            "serverless-sql-warehouse" if self.serverless else f"{self.warehouse_type.lower()}-sql-warehouse"
        )
        self.extended_engine_metadata.update(
            {
                "cloud_provider": self.cloud_provider,
                "compute_region": self.region,
                "workload_type": workload_type,
            }
        )
        if cost_per_hour is not None:
            self._configure_cost_inputs(cost_per_hour=cost_per_hour)
        elif self.serverless:
            try:
                estimate = estimate_sql_warehouse_cost(
                    cloud_provider=self.cloud_provider,
                    region=self.region,
                    warehouse_size=self.warehouse_size,
                    query_list_price=self._query_list_price,
                    dbu_rate_override=dbu_rate,
                )
            except DatabricksPricingError as exc:
                logger.warning("Estimated job cost will not be reported: %s", exc)
                self.extended_engine_metadata["pricing_source"] = "not_configured"
            else:
                self.cost_per_hour = float(estimate.cost_per_hour)
                self.extended_engine_metadata.update(estimate.metadata())
        else:
            logger.warning(
                "Estimated job cost will not be reported for this Databricks SQL "
                "Warehouse. Set cost_per_hour for classic or pro warehouses."
            )
            self.extended_engine_metadata["pricing_source"] = "not_configured"

        self.extended_engine_metadata.update(
            {
                "warehouse_name": warehouse_name,
                "warehouse_size": self.warehouse_size,
                "warehouse_type": self.warehouse_type,
                "photon_enabled": (str(warehouse["enable_photon"]) if "enable_photon" in warehouse else "unknown"),
            }
        )

    @property
    def _auth_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.access_token}"}

    def _get_warehouse(self) -> dict:
        import requests

        response = requests.get(
            f"https://{self.server_hostname}/api/2.0/sql/warehouses",
            headers=self._auth_headers,
            timeout=30,
        )
        response.raise_for_status()
        warehouse = next(
            (item for item in response.json().get("warehouses", []) if item.get("name") == self.warehouse_name),
            None,
        )
        if warehouse is None:
            raise ValueError(f"Databricks SQL Warehouse '{self.warehouse_name}' does not exist.")
        return warehouse

    def _create_connection(self):
        from databricks import sql

        return sql.connect(
            server_hostname=self.server_hostname,
            http_path=self.warehouse_path,
            access_token=self.access_token,
        )

    def _get_runtime_metadata(self) -> tuple[str, str, str]:
        result = self.execute_sql_query(
            "SELECT "
            "split(current_metastore(), ':')[0] AS cloud, "
            "split(current_metastore(), ':')[1] AS region, "
            "current_version().dbsql_version AS dbsql_version",
            return_data=True,
        )
        row = result.iloc[0]
        cloud = str(row["cloud"]).strip() if row["cloud"] is not None else ""
        region = str(row["region"]).strip() if row["region"] is not None else ""
        if not cloud or not region:
            raise ValueError("current_metastore() did not return the Databricks cloud and region.")
        return cloud, region, str(row["dbsql_version"])

    def _query_list_price(self) -> Optional[Decimal]:
        cloud = self.cloud_provider.upper()
        sku_name = self._query_recent_billing_sku()
        if sku_name is not None:
            escaped_sku_name = sku_name.replace("'", "''")
            sku_filter = f"AND sku_name = '{escaped_sku_name}' "
        else:
            region_suffix = (
                _AWS_SERVERLESS_SQL_REGION_SUFFIXES.get(self.region.lower()) if self.cloud_provider == "aws" else None
            )
            sku_filter = f"AND endswith(sku_name, '_{region_suffix}') " if region_suffix is not None else ""
        try:
            result = self.execute_sql_query(
                "SELECT sku_name, pricing.default AS price "
                "FROM system.billing.list_prices "
                f"WHERE cloud = '{cloud}' "
                "AND currency_code = 'USD' "
                "AND usage_unit = 'DBU' "
                "AND ("
                "sku_name LIKE '%SQL%SERVERLESS%COMPUTE%' "
                "OR sku_name LIKE '%SERVERLESS%SQL%COMPUTE%'"
                ") "
                "AND sku_name NOT LIKE '%FREE_TRIAL%' "
                "AND sku_name NOT LIKE '%NON_BILLABLE%' "
                "AND sku_name NOT LIKE '%POC%' "
                f"{sku_filter}"
                "AND price_start_time <= current_timestamp() "
                "AND (price_end_time IS NULL OR price_end_time > current_timestamp()) "
                "ORDER BY price_start_time DESC, sku_name",
                return_data=True,
            )
        except Exception as exc:
            logger.warning(
                "Unable to query system.billing.list_prices for the current "
                "%s Databricks serverless SQL list price: %s",
                cloud,
                exc,
            )
            return None
        if result.empty:
            return None
        prices = set()
        selected_skus = []
        for selected_sku, price in zip(result["sku_name"], result["price"]):
            if isinstance(price, dict):
                price = price.get("USD")
            if price is not None:
                selected_skus.append(str(selected_sku))
                prices.add(Decimal(str(price)))
        if len(prices) != 1:
            logger.warning(
                "Unable to select one %s Databricks serverless SQL list price "
                "from system.billing.list_prices; found %s distinct rates.",
                cloud,
                len(prices),
            )
            return None
        self.extended_engine_metadata["pricing_sku_names"] = ",".join(selected_skus)
        return prices.pop()

    def _query_recent_billing_sku(self) -> Optional[str]:
        cloud = self.cloud_provider.upper()
        warehouse_id = self.warehouse_id.replace("'", "''")
        try:
            result = self.execute_sql_query(
                "SELECT sku_name "
                "FROM system.billing.usage "
                f"WHERE cloud = '{cloud}' "
                f"AND usage_metadata.warehouse_id = '{warehouse_id}' "
                "AND ("
                "sku_name LIKE '%SQL%SERVERLESS%COMPUTE%' "
                "OR sku_name LIKE '%SERVERLESS%SQL%COMPUTE%'"
                ") "
                "ORDER BY usage_end_time DESC "
                "LIMIT 1",
                return_data=True,
            )
        except Exception as exc:
            logger.warning(
                "Unable to resolve the SQL warehouse's exact billed SKU from "
                "system.billing.usage; falling back to region matching: %s",
                exc,
            )
            return None
        if result.empty:
            return None
        return str(result["sku_name"].iloc[0])

    def create_schema_if_not_exists(self, drop_before_create: bool = True):
        location = f"LOCATION '{self.schema_uri}'" if self.schema_uri is not None else ""
        if drop_before_create:
            self.execute_sql_statement(f"DROP SCHEMA IF EXISTS {self.full_catalog_schema_reference} CASCADE")
        self.execute_sql_statement(f"CREATE SCHEMA IF NOT EXISTS {self.full_catalog_schema_reference} {location}")
        self.execute_sql_statement(f"USE {self.full_catalog_schema_reference}")

    def _create_empty_table(self, table_name: Optional[str], ddl: str):
        if "using " not in ddl.lower():
            import sqlglot

            expression = sqlglot.parse_one(ddl, dialect="spark")
            create_node = expression.find(sqlglot.exp.Create)
            if create_node is not None:
                existing = create_node.args.get("properties")
                create_node.set(
                    "properties",
                    sqlglot.exp.Properties(
                        expressions=[
                            sqlglot.exp.FileFormatProperty(this=sqlglot.exp.Literal.string("delta")),
                            *(existing.expressions if existing else []),
                        ]
                    ),
                )
                ddl = create_node.sql(dialect="spark", pretty=True)
        self.execute_sql_statement(ddl)

    def _append_results_to_delta(self, table_uri: str, results: list, generic_schema: list):
        self.execute_sql_statement(
            f"CREATE TABLE IF NOT EXISTS delta.`{table_uri}` "
            f"({', '.join(f'{column} {data_type}' for column, data_type in generic_schema)}) "
            "TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')"
        )

        def format_value(value):
            if value is None:
                return "NULL"
            if isinstance(value, str):
                return f"'{value.replace(chr(39), chr(39) * 2)}'"
            if isinstance(value, datetime):
                return f"'{value}'"
            if isinstance(value, dict):
                items = [
                    f"'{str(key).replace(chr(39), chr(39) * 2)}', '{str(item).replace(chr(39), chr(39) * 2)}'"
                    for key, item in value.items()
                ]
                return f"map({', '.join(items)})" if items else "map()"
            return str(value)

        columns = list(results[0].keys())
        values = ", ".join("(" + ", ".join(format_value(value) for value in row.values()) + ")" for row in results)
        self.execute_sql_statement(f"INSERT INTO delta.`{table_uri}` ({', '.join(columns)}) VALUES {values}")

    def get_total_cores(self) -> int:
        size_map = {
            "2X-Small": (8, 1),
            "X-Small": (8, 2),
            "Small": (16, 4),
            "Medium": (32, 8),
            "Large": (32, 16),
            "X-Large": (64, 32),
            "2X-Large": (64, 64),
            "3X-Large": (64, 128),
            "4X-Large": (64, 256),
            "5X-Large": (64, 512),
        }
        try:
            driver_cores, workers = size_map[self.warehouse_size]
        except KeyError as exc:
            raise ValueError(f"Unknown warehouse size: {self.warehouse_size}") from exc
        return driver_cores + (workers * 8)

    def get_compute_size(self) -> str:
        return self.warehouse_size

    def load_parquet_to_delta(
        self,
        parquet_folder_uri: str,
        table_name: str,
        table_is_precreated: bool = False,
        context_decorator: Optional[str] = None,
        column_name_mapping: Optional[Mapping[str, str]] = None,
    ):
        source = f"SELECT * FROM parquet.`{parquet_folder_uri}`"
        source_columns = list(self.execute_sql_query(f"{source} LIMIT 0", return_data=True).columns)
        mapping = self._resolve_column_name_mapping(table_name, source_columns, column_name_mapping)
        projection = ", ".join(
            f"`{column}` AS `{mapping[column]}`" if column in mapping else f"`{column}`" for column in source_columns
        )
        if table_is_precreated:
            self.execute_sql_statement(
                f"INSERT INTO {table_name} SELECT {projection} FROM parquet.`{parquet_folder_uri}`"
            )
        else:
            self.execute_sql_statement(
                f"CREATE TABLE {table_name} USING delta AS SELECT {projection} FROM parquet.`{parquet_folder_uri}`"
            )
        if self.run_analyze_after_load:
            self.analyze_table(table_name)

    def load_delimited_to_delta(
        self,
        folder_uri: str,
        table_name: str,
        columns,
        file_pattern: str,
        table_is_precreated: bool = False,
        context_decorator: Optional[str] = None,
        column_name_mapping: Optional[Mapping[str, str]] = None,
    ):
        from ..utils.schema_utils import NATIVE_DELIMITER, TRAILING_DELIMITER_COLUMN, schema_to_sql_types

        sql_types = schema_to_sql_types(columns, self.SQLGLOT_DIALECT)
        sql_types[TRAILING_DELIMITER_COLUMN] = "STRING"
        schema = ", ".join(f"`{name}` {data_type}" for name, data_type in sql_types.items())
        source_columns = [name for name in sql_types if name != TRAILING_DELIMITER_COLUMN]
        mapping = self._resolve_column_name_mapping(table_name, source_columns, column_name_mapping)
        projection = ", ".join(
            f"`{column}` AS `{mapping[column]}`" if column in mapping else f"`{column}`" for column in source_columns
        )
        source_path = posixpath.join(folder_uri, file_pattern)
        source = (
            f"SELECT {projection} FROM read_files("
            f"'{source_path}', format => 'csv', sep => '{NATIVE_DELIMITER}', "
            f"header => false, schema => '{schema}')"
        )
        if table_is_precreated:
            self.execute_sql_statement(f"INSERT INTO {table_name} {source}")
        else:
            self.execute_sql_statement(f"CREATE TABLE {table_name} USING delta AS {source}")
        if self.run_analyze_after_load:
            self.analyze_table(table_name)

    def execute_sql_query(
        self,
        query: str,
        return_data: bool = False,
        context_decorator: Optional[str] = None,
    ):
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            if not return_data:
                return None

            import pandas as pd

            rows = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            return pd.DataFrame(rows, columns=columns)

    def execute_sql_statement(self, statement: str, context_decorator: Optional[str] = None):
        return self.execute_sql_query(statement)

    def optimize_table(self, table_name: str):
        self.execute_sql_statement(f"OPTIMIZE {self.full_catalog_schema_reference}.{table_name}")

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
        self.execute_sql_statement(
            f"ANALYZE TABLE {self.full_catalog_schema_reference}.{table_name} COMPUTE STATISTICS FOR {column_clause}"
        )

    def vacuum_table(self, table_name: str, retain_hours: int = 168, retention_check: bool = True):
        self.execute_sql_statement(
            f"SET spark.databricks.delta.retentionDurationCheck.enabled = {str(retention_check).lower()}"
        )
        self.execute_sql_statement(
            f"VACUUM {self.full_catalog_schema_reference}.{table_name} RETAIN {retain_hours} HOURS"
        )
