"""Record-list formatting helpers for the CLI (table / json / csv / yaml)."""

from __future__ import annotations

import json
from typing import Iterable, Mapping


def format_records(records: Iterable[Mapping], fmt: str = "table") -> str:
    """Render a list of dict records in the requested format."""
    records = list(records)
    if not records:
        return "(no rows)"
    if fmt == "json":
        return json.dumps(records, indent=2, default=str)
    if fmt == "csv":
        import csv
        import io

        buf = io.StringIO()
        cols = list(records[0].keys())
        w = csv.DictWriter(buf, fieldnames=cols)
        w.writeheader()
        for r in records:
            w.writerow({k: r.get(k, "") for k in cols})
        return buf.getvalue().rstrip("\n")
    if fmt == "yaml":
        # Minimal YAML emitter — avoids a PyYAML dependency
        out = []
        for r in records:
            out.append("- " + "\n  ".join(f"{k}: {v}" for k, v in r.items()))
        return "\n".join(out)
    # default: table
    cols = list(records[0].keys())
    widths = {c: max(len(str(c)), max(len(str(r.get(c, ""))) for r in records)) for c in cols}
    header = "  ".join(f"{c:<{widths[c]}}" for c in cols)
    sep = "  ".join("-" * widths[c] for c in cols)
    rows = ["  ".join(f"{str(r.get(c, '')):<{widths[c]}}" for c in cols) for r in records]
    return "\n".join([header, sep, *rows])
