"""Refresh Azure Databricks DBU consumption mappings from the public pricing page."""

from __future__ import annotations

import argparse
import html
import re
import urllib.request
from pathlib import Path

SOURCE_URL = "https://azure.microsoft.com/en-us/pricing/details/databricks/"
DEFAULT_OUTPUT = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "lakebench"
    / "engines"
    / "databricks_azure_dbu_map.py"
)

SPARK_TABLES = {
    "all-purpose-compute-premium": "all-purpose-compute",
    "all-purpose-compute-with-photon-premium": "all-purpose-compute-with-photon",
    "jobs-compute-premium": "jobs-compute",
    "jobs-compute-with-photon-premium": "jobs-compute-with-photon",
}
SQL_TABLE = "serverless-sql-premium"


def _plain_text(fragment: str) -> str:
    return " ".join(
        html.unescape(re.sub(r"<[^>]+>", " ", fragment)).split()
    )


def _table_section(page: str, table_filter: str) -> str:
    marker = f'class="databricks-table" data-filter="{table_filter}"'
    start = page.find(marker)
    if start < 0:
        raise ValueError(f"Azure pricing page did not contain table '{table_filter}'.")
    end = page.find('class="databricks-table" data-filter="', start + len(marker))
    return page[start:] if end < 0 else page[start:end]


def parse_spark_mappings(page: str) -> dict[tuple[str, str], tuple[str, str, float, str, float]]:
    mappings: dict[tuple[str, str], tuple[str, str, float, str, float]] = {}
    for table_filter, workload in SPARK_TABLES.items():
        section = _table_section(page, table_filter)
        series_matches = list(re.finditer(r"<h4[^>]*>(.*?)</h4>", section, re.DOTALL))
        for index, series_match in enumerate(series_matches):
            series = _plain_text(series_match.group(1))
            series_end = (
                series_matches[index + 1].start()
                if index + 1 < len(series_matches)
                else len(section)
            )
            series_section = section[series_match.end() : series_end]
            for row in re.findall(r"<tr[^>]*>(.*?)</tr>", series_section, re.DOTALL):
                cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL)
                if len(cells) < 4:
                    continue
                instance, vcpus, ram, dbus = (_plain_text(cell) for cell in cells[:4])
                if not instance or not re.fullmatch(r"\d+(?:\.\d+)?", dbus):
                    continue
                sku = instance.replace(" ", "_")
                mappings[(workload, sku)] = (
                    instance,
                    series,
                    float(vcpus),
                    ram,
                    float(dbus),
                )
    if not mappings:
        raise ValueError("No Azure Databricks Spark DBU mappings were parsed.")
    return mappings


def parse_sql_warehouse_mappings(page: str) -> dict[str, float]:
    section = _table_section(page, SQL_TABLE)
    mappings: dict[str, float] = {}
    for row in re.findall(r"<tr[^>]*>(.*?)</tr>", section, re.DOTALL):
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL)
        if len(cells) < 2:
            continue
        size, dbus = (_plain_text(cell) for cell in cells[:2])
        if size and re.fullmatch(r"\d+(?:\.\d+)?", dbus):
            mappings[size] = float(dbus)
    if not mappings:
        raise ValueError("No Azure Databricks serverless SQL DBU mappings were parsed.")
    return mappings


def render_module(
    spark_mappings: dict[tuple[str, str], tuple[str, str, float, str, float]],
    sql_mappings: dict[str, float],
) -> str:
    lines = [
        '"""Generated Azure Databricks DBU consumption mappings.',
        "",
        f"Source: {SOURCE_URL}",
        "Refresh with: uv run python scripts/refresh_databricks_azure_dbu_map.py",
        '"""',
        "",
        "AZURE_DATABRICKS_PRICING_URL = " + repr(SOURCE_URL),
        "",
        "# (workload, Azure VM SKU without the Standard_ prefix) ->",
        "# (display name, series, vCPUs, RAM, DBUs per hour)",
        "AZURE_SPARK_DBU_MAP = {",
    ]
    for key in sorted(spark_mappings):
        lines.append(f"    {key!r}: {spark_mappings[key]!r},")
    lines.extend(
        [
            "}",
            "",
            "# Serverless SQL warehouse-size consumption is expressed in DBUs per hour.",
            "SERVERLESS_SQL_DBUS_PER_HOUR = {",
        ]
    )
    for size in sorted(sql_mappings):
        lines.append(f"    {size!r}: {sql_mappings[size]!r},")
    lines.extend(["}", ""])
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-url", default=SOURCE_URL)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    with urllib.request.urlopen(args.source_url, timeout=60) as response:
        page = response.read().decode("utf-8")

    output = render_module(
        parse_spark_mappings(page),
        parse_sql_warehouse_mappings(page),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="\n") as output_file:
        output_file.write(output)


if __name__ == "__main__":
    main()
