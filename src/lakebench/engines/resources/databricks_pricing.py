from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Callable, Literal, Optional

from ...billing import MIXED_PERFORMANCE_MODE, BilledCost, summarize_performance_modes
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


#: Databricks documents that billable usage can take a few hours to land in
#: ``system.billing.usage``; lookups ending more recently than this are flagged.
BILLING_LAG = timedelta(hours=3)

_BILLED_COST_QUERY = """
SELECT
    COALESCE(SUM(u.usage_quantity), 0) AS billed_units,
    COALESCE(SUM(u.usage_quantity * p.pricing.effective_list.default), 0) AS list_cost,
    COUNT(*) AS usage_record_count,
    COUNT_IF(u.usage_quantity <> 0 AND p.sku_name IS NULL) AS unpriced_record_count,
    MAX(u.usage_end_time) AS latest_usage_end,
    COLLECT_SET(u.product_features.performance_target) AS performance_targets,
    COLLECT_SET(u.sku_name) AS sku_names,
    COLLECT_SET(p.currency_code) AS currency_codes,
    COLLECT_SET(u.usage_metadata.job_id) AS job_ids,
    COLLECT_SET(u.cloud) AS clouds
FROM system.billing.usage AS u
LEFT JOIN system.billing.list_prices AS p
    ON u.sku_name = p.sku_name
    AND u.cloud = p.cloud
    AND u.usage_unit = p.usage_unit
    AND u.usage_end_time >= p.price_start_time
    AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time)
WHERE u.usage_unit = 'DBU'
    AND u.product_features.is_serverless = true
    AND u.usage_date BETWEEN to_date(:start_time) AND to_date(:end_time)
    AND u.usage_end_time > :start_time
    AND u.usage_start_time < :end_time
    AND {attribution_filter}
"""


def _to_utc(value: datetime) -> datetime:
    # Naive datetimes are treated as local time, matching ``datetime.now()``
    # used for ``run_datetime`` / ``start_datetime`` in benchmark results.
    return value.astimezone(timezone.utc) if value.tzinfo is not None else value.astimezone().astimezone(timezone.utc)


def query_billed_cost(
    spark: Any,
    start_time: datetime,
    end_time: Optional[datetime] = None,
    job_run_id: Optional[str] = None,
    notebook_id: Optional[str] = None,
    run_id: Optional[str] = None,
    cloud_provider: Optional[str] = None,
    compute_region: Optional[str] = None,
    compute_type: Optional[str] = None,
) -> BilledCost:
    """Sum serverless DBUs and list cost billed to a job run or notebook.

    Parameters
    ----------
    spark : SparkSession
        Session with ``SELECT`` access to ``system.billing.usage`` and
        ``system.billing.list_prices``.
    start_time, end_time : datetime
        Window to sum usage over. ``end_time`` defaults to now. Usage records
        cover time buckets, so any record overlapping the window is included.
    job_run_id, notebook_id : str, optional
        Attribution identifier; ``job_run_id`` takes precedence. Interactive
        notebook attribution includes all serverless usage from that notebook
        in the window, not only LakeBench work.
    run_id, cloud_provider, compute_region, compute_type : str, optional
        Context copied onto the returned :class:`BilledCost`.

    Returns
    -------
    BilledCost
        Platform-neutral record. ``performance_mode`` is the serverless
        ``performance_target`` billed (``STANDARD``, ``PERFORMANCE_OPTIMIZED``,
        or ``MIXED``). Only DBU usage is included; other usage types such as
        networking are excluded.
    """
    if job_run_id:
        attribution_type, attribution_filter = "job_run_id", "u.usage_metadata.job_run_id = :attribution_id"
        attribution_id = str(job_run_id)
    elif notebook_id:
        attribution_type, attribution_filter = "notebook_id", "u.usage_metadata.notebook_id = :attribution_id"
        attribution_id = str(notebook_id)
    else:
        raise DatabricksPricingError(
            "Unable to look up billed serverless cost without a job_run_id or notebook_id. "
            "Pass job_run_id or notebook_id explicitly."
        )

    start_utc = _to_utc(start_time)
    end_utc = _to_utc(end_time) if end_time is not None else datetime.now(timezone.utc)
    if end_utc <= start_utc:
        raise ValueError("end_time must be after start_time.")

    query = _BILLED_COST_QUERY.format(attribution_filter=attribution_filter)
    args = {
        "start_time": start_utc.replace(tzinfo=None),
        "end_time": end_utc.replace(tzinfo=None),
        "attribution_id": attribution_id,
    }
    try:
        row = spark.sql(query, args=args).collect()[0]
    except Exception as exc:
        raise DatabricksPricingError(
            "Unable to query system.billing.usage for billed serverless cost. Ensure the current "
            f"principal can SELECT from system.billing.usage and system.billing.list_prices: {exc}"
        ) from exc

    unpriced = int(row["unpriced_record_count"] or 0)
    if unpriced:
        logger.warning(
            "%s billed usage record(s) had no matching list price and are excluded from list_cost.",
            unpriced,
        )

    performance_targets = sorted(str(mode) for mode in (row["performance_targets"] or []) if mode)
    performance_mode = summarize_performance_modes(performance_targets)
    if performance_mode == MIXED_PERFORMANCE_MODE:
        logger.warning(
            "Billed usage for %s=%s spans multiple serverless performance modes: %s.",
            attribution_type,
            attribution_id,
            ", ".join(performance_targets),
        )

    currencies = sorted(str(code) for code in (row["currency_codes"] or []) if code)
    if len(currencies) > 1:
        logger.warning("Billed usage is priced in multiple currencies: %s.", ", ".join(currencies))
    clouds = sorted(str(cloud) for cloud in (row["clouds"] or []) if cloud)

    details = {}
    for key, values in (
        ("sku_names", row["sku_names"]),
        ("job_ids", row["job_ids"]),
        ("performance_targets", performance_targets),
        ("currency_codes", currencies),
    ):
        joined = ",".join(sorted(str(value) for value in (values or []) if value))
        if joined:
            details[key] = joined

    latest_usage_end = row["latest_usage_end"]
    if isinstance(latest_usage_end, datetime) and latest_usage_end.tzinfo is None:
        latest_usage_end = latest_usage_end.replace(tzinfo=timezone.utc)

    billed = BilledCost(
        platform="databricks",
        billing_unit="DBU",
        billed_units=Decimal(str(row["billed_units"] or 0)),
        list_cost=Decimal(str(row["list_cost"] or 0)),
        currency=currencies[0] if len(currencies) == 1 else ("MIXED" if currencies else "USD"),
        attribution_type=attribution_type,
        attribution_id=attribution_id,
        window_start=start_utc,
        window_end=end_utc,
        latest_usage_end=latest_usage_end,
        usage_record_count=int(row["usage_record_count"] or 0),
        unpriced_record_count=unpriced,
        may_be_incomplete=datetime.now(timezone.utc) - end_utc < BILLING_LAG,
        pricing_source="databricks_system_billing_usage",
        performance_mode=performance_mode,
        cloud_provider=cloud_provider or (normalize_cloud_provider(clouds[0]) if len(clouds) == 1 else None),
        compute_region=compute_region,
        compute_type=compute_type,
        run_id=run_id,
        details=details,
    )
    logger.info(
        "Billed cost for %s: %s %s (%s %s, performance_mode=%s, records=%s%s).",
        billed.attribution,
        billed.list_cost.quantize(Decimal("0.0001")),
        billed.currency,
        billed.billed_units.quantize(Decimal("0.0001")),
        billed.billing_unit,
        billed.performance_mode or "unknown",
        billed.usage_record_count,
        ", may be incomplete" if billed.may_be_incomplete else "",
    )
    return billed
