import inspect
import logging
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from sqlglot import exp

from lakebench.engines.databricks_azure_dbu_map import AZURE_SPARK_DBU_MAP
from lakebench.engines.databricks_pricing import (
    DatabricksPricingError,
    estimate_azure_spark_cost,
    estimate_sql_warehouse_cost,
    get_azure_dbu_hourly_rate,
    get_azure_serverless_sql_dbu_hourly_rate,
    infer_cloud_provider,
    log_if_manual_compute_cost_missing,
    sql_warehouse_dbus_per_hour,
)
from lakebench.engines.databricks_spark import DatabricksSpark
from lakebench.engines.databricks_sql_warehouse import DatabricksSQLWarehouse


@pytest.mark.parametrize(
    ("hostname", "expected"),
    [
        ("adb-123.azuredatabricks.net", "azure"),
        ("dbc-123.gcp.databricks.com", "gcp"),
        ("dbc-123.cloud.databricks.com", "aws"),
    ],
)
def test_infer_cloud_provider_from_workspace_hostname(hostname, expected):
    assert infer_cloud_provider(workspace_hostname=hostname) == expected


def test_explicit_cloud_provider_takes_precedence():
    assert infer_cloud_provider("gcp", "adb-123.azuredatabricks.net") == "gcp"


@pytest.mark.parametrize(
    ("configured_value", "expected"),
    [
        ("AWS", "aws"),
        ("Azure", "azure"),
        ("GCP", "gcp"),
    ],
)
def test_infer_cloud_provider_from_databricks_spark_config(configured_value, expected):
    assert (
        infer_cloud_provider(
            workspace_hostname="custom.example.com",
            spark_configs={"spark.databricks.cloudProvider": configured_value},
        )
        == expected
    )


def test_unknown_cloud_provider_requires_explicit_configuration():
    with pytest.raises(ValueError, match="Unable to detect"):
        infer_cloud_provider(workspace_hostname="custom.example.com")


def test_sql_warehouse_dbu_sizes_use_serverless_consumption_table():
    assert sql_warehouse_dbus_per_hour("2X-Small") == Decimal("4")
    assert sql_warehouse_dbus_per_hour("4X-Large") == Decimal("528")


@pytest.mark.parametrize("cloud_provider", ["aws", "azure", "gcp"])
def test_serverless_sql_warehouse_estimate_uses_account_list_price(cloud_provider):
    estimate = estimate_sql_warehouse_cost(
        cloud_provider=cloud_provider,
        region="eastus",
        warehouse_size="Small",
        query_list_price=lambda: Decimal("0.42"),
    )

    assert estimate.dbus_per_hour == Decimal("12")
    assert estimate.dbu_rate == Decimal("0.42")
    assert estimate.cost_per_hour == Decimal("5.04")
    assert estimate.pricing_source == "databricks_system_billing_list_prices"


@patch("lakebench.engines.databricks_pricing.get_azure_serverless_sql_dbu_hourly_rate")
def test_azure_serverless_sql_warehouse_falls_back_to_retail_api(mock_retail_rate):
    mock_retail_rate.return_value = Decimal("0.70")

    estimate = estimate_sql_warehouse_cost(
        cloud_provider="azure",
        region="eastus",
        warehouse_size="Small",
        query_list_price=lambda: None,
    )

    assert estimate.dbus_per_hour == Decimal("12")
    assert estimate.dbu_rate == Decimal("0.70")
    assert estimate.cost_per_hour == Decimal("8.40")
    assert estimate.pricing_source == "azure_retail_catalog_serverless_sql_dbu"
    mock_retail_rate.assert_called_once_with("eastus")


@patch("lakebench.engines.databricks_pricing._get_azure_retail_price")
def test_azure_serverless_sql_rate_uses_exact_regional_meter(
    mock_retail_price,
):
    mock_retail_price.return_value = Decimal("0.70")

    assert get_azure_serverless_sql_dbu_hourly_rate("eastus") == Decimal("0.70")

    query = mock_retail_price.call_args.args[0]
    assert "serviceName eq 'Azure Databricks'" in query
    assert "productName eq 'Azure Databricks Regional'" in query
    assert "skuName eq 'Premium Serverless SQL'" in query
    assert "armSkuName eq 'Premium'" in query
    assert "armRegionName eq 'eastus'" in query
    assert "meterName eq 'Premium Serverless SQL DBU'" in query
    assert "type eq 'Consumption'" in query


@patch("lakebench.engines.databricks_pricing.get_azure_dbu_hourly_rate")
@patch("lakebench.engines.databricks_pricing.get_azure_vm_hourly_rate")
def test_azure_cluster_estimate_combines_mapped_dbus_and_infrastructure_cost(
    mock_vm_rate,
    mock_dbu_rate,
):
    mock_vm_rate.side_effect = [Decimal("1.00"), Decimal("2.00")]
    mock_dbu_rate.return_value = Decimal("0.30")

    estimate = estimate_azure_spark_cost(
        region="eastus",
        driver_instance_type="Standard_D8ds_v5",
        worker_instance_type="Standard_D8ds_v5",
        worker_count=2,
        workload_type="jobs-compute",
    )

    assert estimate.dbus_per_hour == Decimal("6.00")
    assert estimate.dbu_cost_per_hour == Decimal("1.8000")
    assert estimate.infrastructure_cost_per_hour == Decimal("5.00")
    assert estimate.cost_per_hour == Decimal("6.8000")


def test_azure_dbu_mapping_distinguishes_workload_and_photon():
    assert AZURE_SPARK_DBU_MAP[("jobs-compute", "D16ds_v5")][4] == 4.0
    assert AZURE_SPARK_DBU_MAP[("jobs-compute-with-photon", "D16ds_v5")][4] == 10.0
    assert AZURE_SPARK_DBU_MAP[("all-purpose-compute", "D16ds_v5")][4] == 4.0
    assert AZURE_SPARK_DBU_MAP[("all-purpose-compute-with-photon", "D16ds_v5")][4] == 8.0


@pytest.mark.parametrize(
    ("workload_type", "meter_name"),
    [
        ("jobs-compute", "Premium Jobs Compute DBU"),
        ("jobs-compute-with-photon", "Premium Jobs Compute Photon DBU"),
        ("all-purpose-compute", "Premium All-purpose Compute DBU"),
        ("all-purpose-compute-with-photon", "Premium All-purpose Photon DBU"),
    ],
)
@patch("lakebench.engines.databricks_pricing._get_azure_retail_price")
def test_azure_dbu_rate_uses_exact_workload_meter(
    mock_retail_price,
    workload_type,
    meter_name,
):
    mock_retail_price.return_value = Decimal("0.30")

    assert get_azure_dbu_hourly_rate("eastus", workload_type) == Decimal("0.30")
    assert f"meterName eq '{meter_name}'" in mock_retail_price.call_args.args[0]


@pytest.mark.parametrize("cloud_provider", ["aws", "gcp"])
def test_aws_and_gcp_log_when_manual_compute_cost_is_missing(cloud_provider, caplog):
    with caplog.at_level(logging.WARNING, logger="lakebench.engines.databricks_pricing"):
        log_if_manual_compute_cost_missing(cloud_provider, None, None)

    assert "Estimated job cost will not be reported" in caplog.text


@pytest.mark.parametrize(
    ("cloud_provider", "cost_per_vcore_hour", "cost_per_hour"),
    [
        ("aws", 0.75, None),
        ("gcp", None, 6.0),
        ("azure", None, None),
    ],
)
def test_manual_compute_cost_policy_accepts_supported_inputs(
    cloud_provider,
    cost_per_vcore_hour,
    cost_per_hour,
):
    log_if_manual_compute_cost_missing(cloud_provider, cost_per_vcore_hour, cost_per_hour)


def test_sql_warehouse_only_exposes_total_hourly_manual_cost():
    parameters = inspect.signature(DatabricksSQLWarehouse.__init__).parameters
    assert "cost_per_hour" in parameters
    assert "cost_per_vcore_hour" not in parameters
    assert "cloud_provider" not in parameters
    assert "region" not in parameters


def test_databricks_spark_exposes_both_manual_cost_inputs():
    parameters = inspect.signature(DatabricksSpark.__init__).parameters
    assert "cost_per_vcore_hour" in parameters
    assert "cost_per_hour" in parameters


def test_azure_cluster_estimate_explains_how_to_refresh_missing_sku():
    with pytest.raises(DatabricksPricingError, match="refresh_databricks_azure_dbu_map"):
        estimate_azure_spark_cost(
            region="eastus",
            driver_instance_type="Standard_NOT_REAL",
            worker_instance_type="Standard_D8ds_v5",
            worker_count=1,
            workload_type="jobs-compute",
        )



def _make_sql_warehouse():
    warehouse = DatabricksSQLWarehouse.__new__(DatabricksSQLWarehouse)
    warehouse.server_hostname = "workspace.cloud.databricks.com"
    warehouse.warehouse_id = "warehouse-id"
    warehouse.access_token = "access-token"
    warehouse.region = "us-east-1"
    warehouse.extended_engine_metadata = {}
    return warehouse


def test_sql_warehouse_runtime_metadata_uses_current_metastore():
    result = MagicMock()
    result.iloc.__getitem__.return_value = {
        "cloud": "aws",
        "region": "us-west-2",
        "dbsql_version": "2026.35",
    }
    warehouse = _make_sql_warehouse()
    warehouse.execute_sql_query = MagicMock(return_value=result)

    assert warehouse._get_runtime_metadata() == ("aws", "us-west-2", "2026.35")

    statement = warehouse.execute_sql_query.call_args.args[0]
    assert "split(current_metastore(), ':')[0] AS cloud" in statement
    assert "split(current_metastore(), ':')[1] AS region" in statement
    assert "current_version().dbsql_version AS dbsql_version" in statement


def test_sql_warehouse_pricing_permission_failure_logs_without_crashing(caplog):
    warehouse_definition = {
        "id": "warehouse-id",
        "odbc_params": {"path": "/sql/1.0/warehouses/warehouse-id"},
        "cluster_size": "Small",
        "warehouse_type": "SERVERLESS",
        "enable_serverless_compute": True,
    }
    with (
        patch.object(DatabricksSQLWarehouse, "verify_dependencies"),
        patch.object(DatabricksSQLWarehouse, "_detect_runtime", return_value="local_unknown"),
        patch.object(DatabricksSQLWarehouse, "_detect_os", return_value="Linux"),
        patch.object(DatabricksSQLWarehouse, "_get_warehouse", return_value=warehouse_definition),
        patch.object(DatabricksSQLWarehouse, "_create_connection", return_value=MagicMock()),
        patch.object(
            DatabricksSQLWarehouse,
            "_get_runtime_metadata",
            return_value=("azure", "eastus", "2026.35"),
        ),
        patch.object(DatabricksSQLWarehouse, "execute_sql_statement"),
        patch.object(DatabricksSQLWarehouse, "_query_list_price", return_value=None),
        patch(
            "lakebench.engines.databricks_pricing.get_azure_serverless_sql_dbu_hourly_rate",
            return_value=Decimal("0.70"),
        ),
    ):
        with caplog.at_level(
            logging.WARNING,
            logger="lakebench.engines.databricks_sql_warehouse",
        ):
            warehouse = DatabricksSQLWarehouse(
                server_hostname="adb-123.azuredatabricks.net",
                warehouse_name="Serverless Warehouse",
                catalog_name="main",
                schema_name="lakebench",
                access_token="token",
            )

    assert warehouse.cost_per_hour == 8.4
    assert (
        warehouse.extended_engine_metadata["pricing_source"]
        == "azure_retail_catalog_serverless_sql_dbu"
    )
    assert "Estimated job cost will not be reported" not in caplog.text


@pytest.mark.parametrize(
    ("cloud_provider", "expected_cloud", "sku_name"),
    [
        ("aws", "AWS", "PREMIUM_SERVERLESS_SQL_COMPUTE_US_EAST_N_VIRGINIA"),
        ("azure", "AZURE", "PREMIUM_SERVERLESS_SQL_COMPUTE"),
        ("gcp", "GCP", "PREMIUM_SERVERLESS_SQL_COMPUTE"),
    ],
)
def test_sql_warehouse_list_price_query_uses_recent_billed_sku(
    cloud_provider,
    expected_cloud,
    sku_name,
):
    usage_result = MagicMock()
    usage_result.empty = False
    usage_result.__getitem__.return_value.iloc.__getitem__.return_value = sku_name
    price_result = MagicMock()
    price_result.empty = False
    price_result.__getitem__.side_effect = lambda key: {
        "sku_name": [sku_name],
        "price": [{"USD": "0.70"}],
    }[key]
    warehouse = _make_sql_warehouse()
    warehouse.cloud_provider = cloud_provider
    warehouse.execute_sql_query = MagicMock(side_effect=[usage_result, price_result])

    assert warehouse._query_list_price() == Decimal("0.70")

    usage_statement = warehouse.execute_sql_query.call_args_list[0].args[0]
    statement = warehouse.execute_sql_query.call_args_list[1].args[0]
    assert "system.billing.usage" in usage_statement
    assert "usage_metadata.warehouse_id = 'warehouse-id'" in usage_statement
    assert f"cloud = '{expected_cloud}'" in statement
    assert f"sku_name = '{sku_name}'" in statement
    assert "currency_code = 'USD'" in statement
    assert "usage_unit = 'DBU'" in statement
    assert "%SQL%SERVERLESS%COMPUTE%" in statement
    assert warehouse.extended_engine_metadata["pricing_sku_names"] == sku_name


def test_aws_sql_warehouse_list_price_falls_back_to_region_sku():
    usage_result = MagicMock()
    usage_result.empty = True
    price_result = MagicMock()
    price_result.empty = False
    price_result.__getitem__.side_effect = lambda key: {
        "sku_name": [
            "ENTERPRISE_SERVERLESS_SQL_COMPUTE_US_EAST_N_VIRGINIA",
            "PREMIUM_SERVERLESS_SQL_COMPUTE_US_EAST_N_VIRGINIA",
        ],
        "price": [Decimal("0.70"), Decimal("0.70")],
    }[key]
    warehouse = _make_sql_warehouse()
    warehouse.cloud_provider = "aws"
    warehouse.execute_sql_query = MagicMock(side_effect=[usage_result, price_result])

    assert warehouse._query_list_price() == Decimal("0.70")

    statement = warehouse.execute_sql_query.call_args_list[1].args[0]
    assert "endswith(sku_name, '_US_EAST_N_VIRGINIA')" in statement


def test_sql_warehouse_list_price_rejects_distinct_tier_rates(caplog):
    usage_result = MagicMock()
    usage_result.empty = True
    price_result = MagicMock()
    price_result.empty = False
    price_result.__getitem__.side_effect = lambda key: {
        "sku_name": [
            "ENTERPRISE_SERVERLESS_SQL_COMPUTE_US_EAST_N_VIRGINIA",
            "PREMIUM_SERVERLESS_SQL_COMPUTE_US_EAST_N_VIRGINIA",
        ],
        "price": [Decimal("0.70"), Decimal("0.75")],
    }[key]
    warehouse = _make_sql_warehouse()
    warehouse.cloud_provider = "aws"
    warehouse.execute_sql_query = MagicMock(side_effect=[usage_result, price_result])

    with caplog.at_level(
        logging.WARNING,
        logger="lakebench.engines.databricks_sql_warehouse",
    ):
        assert warehouse._query_list_price() is None
    assert "found 2 distinct rates" in caplog.text


def test_sql_warehouse_billing_permissions_are_logged_at_warning(caplog):
    warehouse = _make_sql_warehouse()
    warehouse.cloud_provider = "azure"
    warehouse.region = "eastus"
    warehouse.execute_sql_query = MagicMock(
        side_effect=[
            PermissionError("USE SCHEMA denied for system.billing"),
            PermissionError("USE SCHEMA denied for system.billing"),
        ]
    )

    with caplog.at_level(
        logging.WARNING,
        logger="lakebench.engines.databricks_sql_warehouse",
    ):
        assert warehouse._query_list_price() is None

    assert "exact billed SKU" in caplog.text
    assert "list_prices" in caplog.text
    assert "USE SCHEMA denied" in caplog.text


def test_sql_warehouse_appends_results_through_remote_sql_from_local_runtime():
    warehouse = _make_sql_warehouse()
    warehouse.runtime = "local_unknown"
    warehouse.execute_sql_statement = MagicMock()

    warehouse._append_results_to_delta(
        table_uri="abfss://container@account/results",
        results=[{"run_id": "run-1"}],
        generic_schema=[("run_id", "STRING")],
    )

    statements = [
        call.args[0] for call in warehouse.execute_sql_statement.call_args_list
    ]
    assert statements[0].startswith("CREATE TABLE IF NOT EXISTS delta.")
    assert statements[1].startswith("INSERT INTO delta.")


def test_sql_warehouse_native_loader_uses_read_files_and_drops_trailing_field():
    warehouse = DatabricksSQLWarehouse.__new__(DatabricksSQLWarehouse)
    warehouse.SQLGLOT_DIALECT = "spark"
    warehouse.run_analyze_after_load = False
    warehouse.execute_sql_statement = MagicMock()

    warehouse.load_delimited_to_delta(
        folder_uri="s3://bucket/tpch/customer",
        table_name="customer",
        columns=[
            ("c_custkey", exp.DataType.build("BIGINT")),
            ("c_name", exp.DataType.build("VARCHAR(25)")),
        ],
        file_pattern="*.tbl*",
        table_is_precreated=True,
    )

    statement = warehouse.execute_sql_statement.call_args.args[0]
    assert statement.startswith("INSERT INTO customer SELECT `c_custkey`, `c_name` FROM read_files(")
    assert "s3://bucket/tpch/customer/*.tbl*" in statement
    assert "`_lakebench_trailing_delimiter` STRING" in statement
    assert "SELECT `c_custkey`, `c_name`" in statement
