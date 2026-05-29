"""
TPC-DI FINWIRE fixed-width parser — engine-agnostic helper.

The FINWIRE files are fixed-width text records with three record types
(CMP / SEC / FIN). Parsing is pure Python and identical across the
DuckDB / Polars / Daft engine implementations, which previously each
held a copy of this code (see git history).

Returns three lists of dicts; callers wrap them in their preferred
DataFrame / Arrow representation and write to Delta.

Field widths are taken from the official TPC-DI v1.1.0 spec.
"""

from __future__ import annotations

import os
from typing import Dict, List, Optional, Tuple


def _maybe_int(s: str) -> Optional[int]:
    s = s.strip()
    return int(s) if s else None


def _maybe_str(s: str) -> Optional[str]:
    s = s.strip()
    return s or None


def _list_finwire_files(batch_uri: str) -> List[str]:
    """Return sorted FINWIRE files in `batch_uri` (or `[batch_uri]` if it's a file)."""
    if os.path.isdir(batch_uri):
        return sorted(
            os.path.join(batch_uri, f)
            for f in os.listdir(batch_uri)
            if f.startswith("FINWIRE") and not f.endswith(".csv")
        )
    return [batch_uri]


def parse_finwire_records(
    batch_uri: str,
) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """
    Parse FINWIRE fixed-width files into three lists of records:
    (cmp_records, sec_records, fin_records).

    Each list element is a dict with the full TPC-DI v1.1.0 column set for
    that record type, suitable for `pyarrow.Table.from_pylist`.
    """
    cmp_records: List[Dict] = []
    sec_records: List[Dict] = []
    fin_records: List[Dict] = []

    for filepath in _list_finwire_files(batch_uri):
        with open(filepath, "r") as f:
            for line in f:
                if len(line) < 18:
                    continue
                pts = line[0:15].strip()
                rec_type = line[15:18].strip()

                if rec_type == "CMP":
                    cmp_records.append(
                        {
                            "pts": pts,
                            "rec_type": rec_type,
                            "company_name": line[18:78].strip(),
                            "cik": _maybe_int(line[78:88]),
                            "status": line[88:92].strip(),
                            "industry_id": line[92:94].strip(),
                            "sp_rating": line[94:98].strip(),
                            "founding_date": _maybe_str(line[98:106]),
                            "addr_line1": line[106:186].strip(),
                            "addr_line2": line[186:266].strip(),
                            "postal_code": line[266:278].strip(),
                            "city": line[278:303].strip(),
                            "state_province": line[303:323].strip(),
                            "country": line[323:347].strip(),
                            "ceo_name": line[347:393].strip(),
                            "description": line[393:].strip(),
                        }
                    )
                elif rec_type == "SEC":
                    sec_records.append(
                        {
                            "pts": pts,
                            "rec_type": rec_type,
                            "symbol": line[18:33].strip(),
                            "issue_type": line[33:39].strip(),
                            "status": line[39:43].strip(),
                            "name": line[43:113].strip(),
                            "ex_id": line[113:119].strip(),
                            "sh_out": _maybe_int(line[119:132]),
                            "first_trade_date": _maybe_str(line[132:140]),
                            "first_trade_exchange": _maybe_str(line[140:148]),
                            "dividend": _maybe_str(line[148:160]),
                            "co_name_or_cik": line[160:].strip(),
                        }
                    )
                elif rec_type == "FIN":
                    fin_records.append(
                        {
                            "pts": pts,
                            "rec_type": rec_type,
                            "year": _maybe_int(line[18:22]),
                            "quarter": _maybe_int(line[22:23]),
                            "qtr_start_date": _maybe_str(line[23:31]),
                            "posting_date": _maybe_str(line[31:39]),
                            "revenue": _maybe_str(line[39:56]),
                            "earnings": _maybe_str(line[56:73]),
                            "eps": _maybe_str(line[73:85]),
                            "diluted_eps": _maybe_str(line[85:97]),
                            "margin": _maybe_str(line[97:109]),
                            "inventory": _maybe_str(line[109:126]),
                            "assets": _maybe_str(line[126:143]),
                            "liabilities": _maybe_str(line[143:160]),
                            "sh_out": _maybe_int(line[160:173]),
                            "diluted_sh_out": _maybe_int(line[173:186]),
                            "co_name_or_cik": line[186:].strip(),
                        }
                    )

    return cmp_records, sec_records, fin_records


# Public table-name → record-list mapping for the three FINWIRE staging tables.
FINWIRE_STAGING_TABLES = (
    "staging_finwire_cmp",
    "staging_finwire_sec",
    "staging_finwire_fin",
)
