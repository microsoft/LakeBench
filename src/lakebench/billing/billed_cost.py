"""Platform-neutral record of the actual billed cost of a benchmark run.

See ``docs/cost/README.md`` for how cost attribution works across engines.

Platforms bill in different units (Databricks DBUs, Fabric capacity units,
Snowflake credits, ...) and expose usage differently, but a billed-cost lookup
always answers the same question: what did this run cost, how sure are we, and
where did the number come from. :class:`BilledCost` captures that in a fixed
shape so lookups from any platform can be written to one table
(``BILLED_COST_SCHEMA``) and joined to benchmark results on ``run_id``.

Platform-specific detail (SKUs, job IDs, warehouse IDs, ...) goes in
``details`` rather than new columns, so the schema stays stable.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Optional, Sequence

#: Column names and generic types, in the same format as
#: ``BaseBenchmark.RESULT_SCHEMA`` so engines can write rows with
#: ``engine._append_results_to_delta(table_uri, [cost.to_row()], BILLED_COST_SCHEMA)``.
BILLED_COST_SCHEMA = [
    ("run_id", "STRING"),  # LakeBench run_id; join key to benchmark results
    ("platform", "STRING"),  # e.g. databricks, fabric, snowflake
    ("cloud_provider", "STRING"),
    ("compute_region", "STRING"),
    ("compute_type", "STRING"),  # e.g. serverless-jobs-compute
    ("performance_mode", "STRING"),  # platform mode, e.g. STANDARD / PERFORMANCE_OPTIMIZED; MIXED if several
    ("billing_unit", "STRING"),  # e.g. DBU, CU, CREDIT
    ("billed_units", "DECIMAL(18,10)"),
    ("list_cost", "DECIMAL(18,10)"),
    ("currency", "STRING"),
    ("attribution_type", "STRING"),  # how usage was matched to the run, e.g. job_run_id
    ("attribution_id", "STRING"),
    ("window_start", "TIMESTAMP"),  # UTC
    ("window_end", "TIMESTAMP"),  # UTC
    ("latest_usage_end", "TIMESTAMP"),  # UTC end of the newest matched usage record
    ("usage_record_count", "BIGINT"),
    ("unpriced_record_count", "BIGINT"),
    ("may_be_incomplete", "BOOLEAN"),
    ("pricing_source", "STRING"),
    ("looked_up_at", "TIMESTAMP"),  # UTC
    ("details", "MAP<STRING, STRING>"),  # platform-specific extras
]


class CostAttribution:
    """How an engine attributes cost to a benchmark run.

    ``ITEM_ESTIMATE``
        Each timed item gets ``estimated_retail_job_cost`` at execution time
        from an hourly rate (manual or automatic). Requires compute billed by
        provisioned time, such as a fixed-size cluster or warehouse.
    ``RUN_BILLED``
        The platform bills by consumption that can't be tied to a statement at
        execution time (e.g. Databricks serverless DBUs). Item cost is left
        unset; the actual cost of the whole run is looked up after the fact
        with ``engine.get_billed_cost()`` or the engine's lookup function.
    ``NOT_CONFIGURED``
        No rate and no lookup; cost is not reported.
    """

    ITEM_ESTIMATE = "item_estimate"
    RUN_BILLED = "run_billed"
    NOT_CONFIGURED = "not_configured"


#: Scale of the DECIMAL(18,10) columns; billing sources may return more digits.
_SCHEMA_DECIMAL_SCALE = Decimal("0.0000000001")

#: ``performance_mode`` value when matched usage spans more than one mode.
MIXED_PERFORMANCE_MODE = "MIXED"


def summarize_performance_modes(modes: Any) -> Optional[str]:
    """Collapse the distinct modes seen in usage into one value (``None``, the mode, or ``MIXED``)."""
    distinct = sorted({str(mode) for mode in (modes or []) if mode is not None})
    if not distinct:
        return None
    return distinct[0] if len(distinct) == 1 else MIXED_PERFORMANCE_MODE


@dataclass(frozen=True)
class BilledCost:
    """Actual billed cost of a benchmark run, independent of platform."""

    platform: str
    billing_unit: str
    billed_units: Decimal
    list_cost: Decimal
    currency: str
    attribution_type: str
    attribution_id: str
    window_start: datetime
    window_end: datetime
    usage_record_count: int
    may_be_incomplete: bool
    pricing_source: str
    unpriced_record_count: int = 0
    latest_usage_end: Optional[datetime] = None
    performance_mode: Optional[str] = None
    cloud_provider: Optional[str] = None
    compute_region: Optional[str] = None
    compute_type: Optional[str] = None
    run_id: Optional[str] = None
    looked_up_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    details: dict[str, str] = field(default_factory=dict)

    @property
    def attribution(self) -> str:
        return f"{self.attribution_type}={self.attribution_id}"

    def to_row(self, run_id: Optional[str] = None) -> dict[str, Any]:
        """Return a dict matching ``BILLED_COST_SCHEMA``; ``run_id`` overrides the stored value."""
        return {
            "run_id": run_id if run_id is not None else self.run_id,
            "platform": self.platform,
            "cloud_provider": self.cloud_provider,
            "compute_region": self.compute_region,
            "compute_type": self.compute_type,
            "performance_mode": self.performance_mode,
            "billing_unit": self.billing_unit,
            "billed_units": self.billed_units.quantize(_SCHEMA_DECIMAL_SCALE),
            "list_cost": self.list_cost.quantize(_SCHEMA_DECIMAL_SCALE),
            "currency": self.currency,
            "attribution_type": self.attribution_type,
            "attribution_id": self.attribution_id,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "latest_usage_end": self.latest_usage_end,
            "usage_record_count": self.usage_record_count,
            "unpriced_record_count": self.unpriced_record_count,
            "may_be_incomplete": self.may_be_incomplete,
            "pricing_source": self.pricing_source,
            "looked_up_at": self.looked_up_at,
            "details": dict(self.details),
        }

    def metadata(self) -> dict[str, str]:
        """Flat string form for ``extended_engine_metadata`` / logging."""
        return {
            "billed_units": str(self.billed_units.quantize(Decimal("0.0000"))),
            "billing_unit": self.billing_unit,
            "billed_list_cost": str(self.list_cost.quantize(Decimal("0.0000"))),
            "billed_currency": self.currency,
            "billed_performance_mode": self.performance_mode or "unknown",
            "billed_usage_records": str(self.usage_record_count),
            "billed_attribution": self.attribution,
            "billed_may_be_incomplete": str(self.may_be_incomplete),
            "pricing_source": self.pricing_source,
        }


def run_window_from_results(results: Sequence[dict]) -> tuple[datetime, datetime]:
    """Return the ``(start, end)`` covered by benchmark result rows.

    ``start`` is the earliest ``start_datetime`` and ``end`` the latest
    ``start_datetime + duration_ms``. Use it to bound a post-run billing
    lookup from saved results.
    """
    rows = [row for row in results if row.get("start_datetime") is not None]
    if not rows:
        raise ValueError("Results contain no start_datetime values.")
    start = min(row["start_datetime"] for row in rows)
    end = max(row["start_datetime"] + timedelta(milliseconds=int(row.get("duration_ms") or 0)) for row in rows)
    return start, end


def backfill_results_cost(results: Sequence[dict], billed: BilledCost) -> list[dict]:
    """Apportion a run's billed total across result rows by ``duration_ms``.

    This is an estimate, not billed item cost: platforms that only bill per
    run (``CostAttribution.RUN_BILLED``) don't expose cost per statement. Each
    row's ``estimated_retail_job_cost`` is the billed list cost weighted by its
    share of total timed duration. Returns new row dicts.
    """
    total_ms = sum(int(row.get("duration_ms") or 0) for row in results)
    if total_ms <= 0:
        return [dict(row) for row in results]
    updated = []
    for row in results:
        share = Decimal(int(row.get("duration_ms") or 0)) / Decimal(total_ms)
        new_row = dict(row)
        new_row["estimated_retail_job_cost"] = (billed.list_cost * share).quantize(_SCHEMA_DECIMAL_SCALE)
        updated.append(new_row)
    return updated
