from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable, Literal, Optional

from .databricks_azure_dbu_map import (
    AZURE_SPARK_DBU_MAP,
    SERVERLESS_SQL_DBUS_PER_HOUR,
)

logger = logging.getLogger(__name__)

CloudProvider = Literal["aws", "azure", "gcp"]

_CLOUD_ALIASES = {
    "amazon": "aws",
    "amazon web services": "aws",
    "aws": "aws",
    "azure": "azure",
    "gcp": "gcp",
    "google": "gcp",
    "google cloud": "gcp",
}


class DatabricksPricingError(RuntimeError):
    """Raised when an automatic Databricks price cannot be resolved."""


@dataclass(frozen=True)
class DatabricksPriceEstimate:
    cloud_provider: CloudProvider
    region: str
    workload_type: str
    cost_per_hour: Decimal
    dbus_per_hour: Decimal
    dbu_rate: Decimal
    dbu_cost_per_hour: Decimal
    infrastructure_cost_per_hour: Decimal
    pricing_source: str

    def metadata(self) -> dict[str, str]:
        return {
            "cloud_provider": self.cloud_provider,
            "compute_region": self.region,
            "workload_type": self.workload_type,
            "cost_per_hour": str(self.cost_per_hour.quantize(Decimal("0.0000"))),
            "dbus_per_hour": str(self.dbus_per_hour.quantize(Decimal("0.0000"))),
            "dbu_rate": str(self.dbu_rate.quantize(Decimal("0.0000"))),
            "dbu_cost_per_hour": str(self.dbu_cost_per_hour.quantize(Decimal("0.0000"))),
            "infrastructure_cost_per_hour": str(self.infrastructure_cost_per_hour.quantize(Decimal("0.0000"))),
            "pricing_source": self.pricing_source,
        }


def normalize_cloud_provider(value: str) -> CloudProvider:
    normalized = _CLOUD_ALIASES.get(value.strip().lower())
    if normalized is None:
        raise ValueError(f"Unsupported Databricks cloud provider '{value}'. Expected one of: aws, azure, gcp.")
    return normalized  # type: ignore[return-value]


def infer_cloud_provider(
    explicit_cloud_provider: Optional[str] = None,
    workspace_hostname: Optional[str] = None,
    spark_configs: Optional[dict[str, Any]] = None,
) -> CloudProvider:
    if explicit_cloud_provider:
        return normalize_cloud_provider(explicit_cloud_provider)

    configs = spark_configs or {}
    value = configs.get("spark.databricks.cloudProvider")
    if value:
        return normalize_cloud_provider(str(value))

    hostname = (workspace_hostname or configs.get("spark.databricks.workspaceUrl") or "").lower()
    if "azuredatabricks.net" in hostname:
        return "azure"
    if "gcp.databricks.com" in hostname:
        return "gcp"
    if hostname.endswith(".cloud.databricks.com") or hostname == "cloud.databricks.com":
        return "aws"

    raise ValueError("Unable to detect the Databricks cloud provider. Pass cloud_provider='aws', 'azure', or 'gcp'.")


def sql_warehouse_dbus_per_hour(warehouse_size: str) -> Decimal:
    try:
        return Decimal(str(SERVERLESS_SQL_DBUS_PER_HOUR[warehouse_size]))
    except KeyError as exc:
        raise ValueError(f"Unknown Databricks SQL warehouse size: {warehouse_size}") from exc


def log_if_manual_compute_cost_missing(
    cloud_provider: CloudProvider,
    cost_per_vcore_hour: Optional[float],
    cost_per_hour: Optional[float],
) -> None:
    if cloud_provider in ("aws", "gcp") and cost_per_vcore_hour is None and cost_per_hour is None:
        logger.warning(
            f"Estimated job cost will not be reported for Databricks on {cloud_provider.upper()}. "
            "Set cost_per_vcore_hour or cost_per_hour to enable estimated cost reporting."
        )


def resolve_dbu_rate(
    query_list_price: Optional[Callable[[], Optional[Decimal]]] = None,
    override: Optional[float] = None,
    fallback_rate: Optional[Callable[[], Decimal]] = None,
    fallback_source: Optional[str] = None,
) -> tuple[Decimal, str]:
    if override is not None:
        return Decimal(str(override)), "explicit_dbu_rate"

    if query_list_price is not None:
        rate = query_list_price()
        if rate is not None:
            return rate, "databricks_system_billing_list_prices"

    if fallback_rate is not None and fallback_source is not None:
        return fallback_rate(), fallback_source

    raise DatabricksPricingError(
        "Unable to resolve a current Databricks serverless SQL DBU list price. "
        "Pass dbu_rate or cost_per_hour to enable estimated cost reporting."
    )


def _get_azure_retail_price(query: str, description: str) -> Decimal:
    import requests

    response = requests.get(
        "https://prices.azure.com/api/retail/prices",
        params={"$filter": query},
        timeout=30,
    )
    response.raise_for_status()
    items = response.json().get("Items", [])
    if not items:
        raise DatabricksPricingError(f"Azure Retail Prices returned no rate for {description}.")
    rates = {Decimal(str(item["retailPrice"])) for item in items}
    if len(rates) != 1:
        raise DatabricksPricingError(
            f"Azure Retail Prices returned multiple rates for {description}: "
            f"{', '.join(str(rate) for rate in sorted(rates))}."
        )
    return rates.pop()


def get_azure_vm_hourly_rate(region: str, instance_type: str) -> Decimal:
    meter_name = (
        instance_type[len("Standard_") :] if instance_type.startswith("Standard_") else instance_type
    ).replace("_", " ")
    query = (
        "serviceName eq 'Virtual Machines' "
        f"and armSkuName eq '{instance_type}' "
        f"and armRegionName eq '{region}' "
        f"and meterName eq '{meter_name}' "
        "and type eq 'Consumption' "
        "and endswith(productName, 'Series')"
    )
    return _get_azure_retail_price(query, f"{instance_type} in {region}")


def get_azure_dbu_hourly_rate(region: str, workload_type: str) -> Decimal:
    meter_names = {
        "all-purpose-compute": "Premium All-purpose Compute DBU",
        "all-purpose-compute-with-photon": "Premium All-purpose Photon DBU",
        "jobs-compute": "Premium Jobs Compute DBU",
        "jobs-compute-with-photon": "Premium Jobs Compute Photon DBU",
    }
    try:
        meter_name = meter_names[workload_type]
    except KeyError as exc:
        raise ValueError(f"Unsupported Azure Databricks workload type: {workload_type}") from exc
    query = (
        "serviceName eq 'Azure Databricks' "
        f"and armRegionName eq '{region}' "
        f"and meterName eq '{meter_name}' "
        "and type eq 'Consumption'"
    )
    return _get_azure_retail_price(query, f"{meter_name} in {region}")


def get_azure_serverless_sql_dbu_hourly_rate(region: str) -> Decimal:
    meter_name = "Premium Serverless SQL DBU"
    query = (
        "serviceName eq 'Azure Databricks' "
        "and productName eq 'Azure Databricks Regional' "
        "and skuName eq 'Premium Serverless SQL' "
        "and armSkuName eq 'Premium' "
        f"and armRegionName eq '{region}' "
        f"and meterName eq '{meter_name}' "
        "and type eq 'Consumption'"
    )
    return _get_azure_retail_price(query, f"{meter_name} in {region}")


def estimate_azure_spark_cost(
    region: str,
    driver_instance_type: str,
    worker_instance_type: str,
    worker_count: int,
    workload_type: str,
) -> DatabricksPriceEstimate:
    driver_sku = (
        driver_instance_type[len("Standard_") :]
        if driver_instance_type.startswith("Standard_")
        else driver_instance_type
    )
    worker_sku = (
        worker_instance_type[len("Standard_") :]
        if worker_instance_type.startswith("Standard_")
        else worker_instance_type
    )
    try:
        driver_dbus = Decimal(str(AZURE_SPARK_DBU_MAP[(workload_type, driver_sku)][4]))
        worker_dbus = Decimal(str(AZURE_SPARK_DBU_MAP[(workload_type, worker_sku)][4]))
    except KeyError as exc:
        missing_sku = exc.args[0][1]
        raise DatabricksPricingError(
            f"No Azure Databricks DBU mapping exists for {workload_type} on {missing_sku}. "
            "Refresh the mapping with "
            "'uv run python scripts/refresh_databricks_azure_dbu_map.py' or pass cost_per_hour."
        ) from exc

    dbus_per_hour = driver_dbus + (worker_dbus * worker_count)
    dbu_rate = get_azure_dbu_hourly_rate(region, workload_type)
    driver_rate = get_azure_vm_hourly_rate(region, driver_instance_type)
    worker_rate = get_azure_vm_hourly_rate(region, worker_instance_type)
    infrastructure_cost = driver_rate + (worker_rate * worker_count)

    dbu_cost = dbus_per_hour * dbu_rate
    return DatabricksPriceEstimate(
        cloud_provider="azure",
        region=region,
        workload_type=workload_type,
        cost_per_hour=dbu_cost + infrastructure_cost,
        dbus_per_hour=dbus_per_hour,
        dbu_rate=dbu_rate,
        dbu_cost_per_hour=dbu_cost,
        infrastructure_cost_per_hour=infrastructure_cost,
        pricing_source="azure_retail_catalog_dbu+azure_retail_catalog_vm",
    )


def estimate_sql_warehouse_cost(
    cloud_provider: CloudProvider,
    region: str,
    warehouse_size: str,
    query_list_price: Optional[Callable[[], Optional[Decimal]]] = None,
    dbu_rate_override: Optional[float] = None,
) -> DatabricksPriceEstimate:
    try:
        dbus_per_hour = sql_warehouse_dbus_per_hour(warehouse_size)
    except ValueError as exc:
        raise DatabricksPricingError(
            f"No published Azure serverless SQL DBU mapping exists for warehouse "
            f"size {warehouse_size}. Refresh the mapping with "
            "'uv run python scripts/refresh_databricks_azure_dbu_map.py' or pass "
            "cost_per_hour."
        ) from exc
    azure_fallback = (lambda: get_azure_serverless_sql_dbu_hourly_rate(region)) if cloud_provider == "azure" else None
    dbu_rate, pricing_source = resolve_dbu_rate(
        query_list_price=query_list_price,
        override=dbu_rate_override,
        fallback_rate=azure_fallback,
        fallback_source=("azure_retail_catalog_serverless_sql_dbu" if azure_fallback is not None else None),
    )
    cost_per_hour = dbus_per_hour * dbu_rate
    return DatabricksPriceEstimate(
        cloud_provider=cloud_provider,
        region=region,
        workload_type="serverless-sql-warehouse",
        cost_per_hour=cost_per_hour,
        dbus_per_hour=dbus_per_hour,
        dbu_rate=dbu_rate,
        dbu_cost_per_hour=cost_per_hour,
        infrastructure_cost_per_hour=Decimal("0"),
        pricing_source=pricing_source,
    )
