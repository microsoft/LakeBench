"""
LakeBench Reporting — generate text-based reports from benchmark results.

All output is plain text tables (no external dependencies).
"""

import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from .results import ResultsManager


def _format_duration(ms: int) -> str:
    """Format milliseconds as human-readable duration."""
    if ms < 1000:
        return f"{ms}ms"
    elif ms < 60000:
        return f"{ms / 1000:.1f}s"
    elif ms < 3600000:
        return f"{ms / 60000:.1f}m"
    else:
        return f"{ms / 3600000:.1f}h"


def _format_table(headers: List[str], rows: List[List[str]], alignments: Optional[List[str]] = None) -> str:
    """
    Format a list of rows into an aligned text table.

    Parameters
    ----------
    headers : list of str
    rows : list of list of str
    alignments : list of 'l' or 'r' (left/right align per column)
    """
    if not rows:
        return "(no data)"

    all_rows = [headers] + rows
    widths = [max(len(str(cell)) for cell in col) for col in zip(*all_rows)]

    if alignments is None:
        alignments = ["l"] * len(headers)

    def fmt_row(row):
        cells = []
        for i, cell in enumerate(row):
            w = widths[i]
            if i < len(alignments) and alignments[i] == "r":
                cells.append(str(cell).rjust(w))
            else:
                cells.append(str(cell).ljust(w))
        return "  ".join(cells)

    lines = [fmt_row(headers)]
    lines.append("  ".join("-" * w for w in widths))
    for row in rows:
        lines.append(fmt_row(row))
    return "\n".join(lines)


def report_summary(rm: ResultsManager, run_id: Optional[str] = None) -> str:
    """
    Generate a summary report for the latest or a specific run.

    Shows: run metadata, per-phase summary, and per-item timing table.
    """
    if run_id:
        run_data = rm.get_run(run_id)
        if not run_data:
            return f"Run '{run_id}' not found."
    else:
        runs = rm.list_runs(limit=1)
        if not runs:
            return "No runs found."
        run_id = runs[0]["run_id"]
        run_data = rm.get_run(run_id)
        if not run_data:
            return f"Run '{run_id}' not found."

    meta = run_data.get("metadata", {})
    results = run_data.get("results", {})

    # Header
    lines = []
    lines.append(f"{'=' * 70}")
    lines.append(f"LakeBench Run Summary")
    lines.append(f"{'=' * 70}")
    lines.append(f"  Run ID:     {meta.get('run_id', run_id)}")
    lines.append(f"  Date:       {meta.get('run_datetime', 'N/A')}")
    lines.append(f"  Benchmark:  {meta.get('benchmark', 'N/A')}")
    lines.append(f"  Engine:     {meta.get('engine', 'N/A')} ({meta.get('engine_version', '')})")
    lines.append(f"  Scenario:   {meta.get('scenario', 'N/A')} (SF={meta.get('scale_factor', 'N/A')})")
    lines.append(f"  Profile:    {meta.get('profile', 'N/A')}")
    plat = meta.get("platform", {})
    lines.append(f"  Platform:   {plat.get('os', '')} / {plat.get('cpu_model', '')}")
    lines.append(f"  Cores:      {plat.get('total_cores', 'N/A')} / Memory: {plat.get('total_memory_gb', 'N/A')} GB")
    lines.append("")

    # Phase summary
    summary = meta.get("summary", {})
    phases = summary.get("phases", {})
    if phases:
        lines.append("Phase Summary:")
        phase_headers = ["Phase", "Items", "Passed", "Failed", "Total Time", "Avg Time"]
        phase_rows = []
        for phase, stats in phases.items():
            count = stats.get("count", 0)
            total_ms = stats.get("total_ms", 0)
            avg_ms = total_ms // count if count > 0 else 0
            phase_rows.append([
                phase,
                str(count),
                str(stats.get("success", 0)),
                str(stats.get("failed", 0)),
                _format_duration(total_ms),
                _format_duration(avg_ms),
            ])
        lines.append(_format_table(phase_headers, phase_rows, ["l", "r", "r", "r", "r", "r"]))
        total_ms = summary.get("total_duration_ms", 0)
        lines.append(f"\n  Total Duration: {_format_duration(total_ms)}")
    lines.append("")

    # Per-item table
    test_items = results.get("test_item", [])
    if test_items:
        n = len(test_items)
        item_headers = ["Phase", "Item", "Duration", "Status"]
        item_rows = []
        for i in range(n):
            phase = results.get("phase", [""])[i]
            item = test_items[i]
            dur = results.get("duration_ms", [0])[i]
            success = results.get("success", [True])[i]
            status = "PASS" if success else "FAIL"
            item_rows.append([phase, item, _format_duration(dur), status])
        lines.append("Detail:")
        lines.append(_format_table(item_headers, item_rows, ["l", "l", "r", "l"]))

    return "\n".join(lines)


def report_compare(
    rm: ResultsManager,
    benchmark: Optional[str] = None,
    scenario: Optional[str] = None,
    engines: Optional[List[str]] = None,
    run_ids: Optional[List[str]] = None,
) -> str:
    """
    Generate a cross-engine comparison report.

    Compares the latest run per engine for a given benchmark/scenario,
    or compares specific run_ids.
    """
    import pyarrow.parquet as pq
    import pyarrow.compute as pc

    all_results = rm.get_all_results(benchmark=benchmark, scenario=scenario)
    if all_results is None or all_results.num_rows == 0:
        return "No results found for comparison."

    # Filter by engines if specified
    if engines:
        masks = [pc.equal(all_results.column("engine"), e) for e in engines]
        combined_mask = masks[0]
        for m in masks[1:]:
            combined_mask = pc.or_(combined_mask, m)
        all_results = all_results.filter(combined_mask)

    # Filter by run_ids if specified
    if run_ids:
        masks = [pc.equal(all_results.column("run_id"), rid) for rid in run_ids]
        combined_mask = masks[0]
        for m in masks[1:]:
            combined_mask = pc.or_(combined_mask, m)
        all_results = all_results.filter(combined_mask)

    if all_results.num_rows == 0:
        return "No matching results found."

    # Get unique run_ids grouped by engine (latest per engine if no run_ids specified)
    data = all_results.to_pydict()
    n = len(data["run_id"])

    # Group by engine -> latest run_id
    engine_runs = {}
    for i in range(n):
        eng = data["engine"][i]
        rid = data["run_id"][i]
        rdt = data["run_datetime"][i]
        if eng not in engine_runs or rdt > engine_runs[eng][1]:
            engine_runs[eng] = (rid, rdt)

    # Collect per-query timing per engine
    engine_timings = {}  # engine -> {test_item -> duration_ms}
    engine_meta = {}  # engine -> {version, total_ms}
    for i in range(n):
        eng = data["engine"][i]
        rid = data["run_id"][i]
        if rid != engine_runs[eng][0]:
            continue
        phase = data["phase"][i]
        item = data["test_item"][i]
        dur = data["duration_ms"][i]
        if eng not in engine_timings:
            engine_timings[eng] = {}
            engine_meta[eng] = {"version": data["engine_version"][i], "total_ms": 0}
        if phase == "Query":
            engine_timings[eng][item] = dur
            engine_meta[eng]["total_ms"] += dur

    if not engine_timings:
        return "No query results found for comparison."

    engine_names = sorted(engine_timings.keys())
    all_queries = sorted(
        set(q for timings in engine_timings.values() for q in timings),
        key=lambda q: (q.replace("q", "").replace("a", ".1").replace("b", ".2"))
    )

    lines = []
    lines.append(f"{'=' * 70}")
    lines.append(f"Cross-Engine Comparison — {benchmark or 'All'} {scenario or ''}")
    lines.append(f"{'=' * 70}")
    for eng in engine_names:
        meta = engine_meta[eng]
        lines.append(f"  {eng}: {meta['version']} (total query time: {_format_duration(meta['total_ms'])})")
    lines.append("")

    # Build comparison table
    headers = ["Query"] + engine_names + (["Fastest"] if len(engine_names) > 1 else [])
    alignments = ["l"] + ["r"] * len(engine_names) + (["l"] if len(engine_names) > 1 else [])
    rows = []
    wins = {eng: 0 for eng in engine_names}

    for q in all_queries:
        row = [q]
        times = {}
        for eng in engine_names:
            dur = engine_timings[eng].get(q)
            if dur is not None:
                row.append(_format_duration(dur))
                times[eng] = dur
            else:
                row.append("-")
        if len(engine_names) > 1 and times:
            fastest = min(times, key=times.get)
            wins[fastest] += 1
            row.append(fastest)
        rows.append(row)

    # Totals row
    total_row = ["TOTAL"]
    for eng in engine_names:
        total_row.append(_format_duration(engine_meta[eng]["total_ms"]))
    if len(engine_names) > 1:
        total_row.append("")
    rows.append(total_row)

    lines.append(_format_table(headers, rows, alignments))

    if len(engine_names) > 1:
        lines.append("")
        lines.append("Wins:")
        for eng in engine_names:
            lines.append(f"  {eng}: {wins[eng]}/{len(all_queries)} queries")

    return "\n".join(lines)


def report_history(
    rm: ResultsManager,
    benchmark: Optional[str] = None,
    engine: Optional[str] = None,
    scenario: Optional[str] = None,
    limit: int = 20,
) -> str:
    """Generate a historical runs table."""
    runs = rm.list_runs(benchmark=benchmark, engine=engine, scenario=scenario, limit=limit)
    if not runs:
        return "No runs found."

    lines = []
    lines.append(f"{'=' * 70}")
    lines.append(f"Run History")
    lines.append(f"{'=' * 70}")

    headers = ["Date", "Benchmark", "Engine", "Scenario", "Items", "Pass", "Fail", "Duration", "Profile"]
    alignments = ["l", "l", "l", "l", "r", "r", "r", "r", "l"]
    rows = []
    for r in runs:
        dt = r.get("run_datetime", "")
        if isinstance(dt, datetime):
            dt = dt.strftime("%Y-%m-%d %H:%M")
        else:
            dt = str(dt)[:16]
        rows.append([
            dt,
            r.get("benchmark", ""),
            r.get("engine", ""),
            r.get("scenario", ""),
            str(r.get("total_items", 0)),
            str(r.get("success_count", 0)),
            str(r.get("failed_count", 0)),
            _format_duration(r.get("total_duration_ms", 0)),
            r.get("profile", "") or "",
        ])

    lines.append(_format_table(headers, rows, alignments))
    return "\n".join(lines)


def export_results(
    rm: ResultsManager,
    run_id: Optional[str] = None,
    fmt: str = "csv",
    output_path: Optional[str] = None,
) -> str:
    """
    Export results as CSV, JSON, or markdown.

    Returns the output path or content string.
    """
    import pyarrow.parquet as pq

    if run_id:
        run_data = rm.get_run(run_id)
        if not run_data:
            return f"Run '{run_id}' not found."
        results_dict = run_data.get("results", {})
        n = len(results_dict.get("run_id", []))
        rows = [{k: v[i] for k, v in results_dict.items()} for i in range(n)]
    else:
        table = rm.get_all_results()
        if table is None or table.num_rows == 0:
            return "No results to export."
        results_dict = table.to_pydict()
        n = table.num_rows
        rows = [{k: v[i] for k, v in results_dict.items()} for i in range(n)]

    # Simplify MAP columns to JSON strings
    for row in rows:
        for key in ("engine_properties", "execution_telemetry"):
            val = row.get(key)
            if val and not isinstance(val, str):
                if isinstance(val, list):
                    row[key] = json.dumps(dict(val))
                elif isinstance(val, dict):
                    row[key] = json.dumps(val)
        # Convert datetimes
        for key in ("run_datetime", "start_datetime"):
            if key in row and row[key] is not None:
                row[key] = str(row[key])

    if fmt == "csv":
        import csv
        import io
        if not rows:
            return "No data."
        fieldnames = list(rows[0].keys())
        if output_path:
            with open(output_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            return f"Exported {len(rows)} rows to {output_path}"
        else:
            buf = io.StringIO()
            writer = csv.DictWriter(buf, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
            return buf.getvalue()

    elif fmt == "json":
        content = json.dumps(rows, indent=2, default=str)
        if output_path:
            with open(output_path, "w") as f:
                f.write(content)
            return f"Exported {len(rows)} rows to {output_path}"
        return content

    elif fmt == "md":
        if not rows:
            return "No data."
        # Subset of columns for readability
        md_cols = ["benchmark", "engine", "scenario", "phase", "test_item", "duration_ms", "success"]
        headers = md_cols
        md_rows = []
        for r in rows:
            md_rows.append([str(r.get(c, "")) for c in md_cols])

        lines = ["| " + " | ".join(headers) + " |"]
        lines.append("| " + " | ".join("---" for _ in headers) + " |")
        for row in md_rows:
            lines.append("| " + " | ".join(row) + " |")
        content = "\n".join(lines)

        if output_path:
            with open(output_path, "w") as f:
                f.write(content)
            return f"Exported {len(rows)} rows to {output_path}"
        return content

    else:
        return f"Unknown format: {fmt}. Use csv, json, or md."
