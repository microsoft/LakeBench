"""Unit tests for the engine-agnostic FINWIRE parser."""

from __future__ import annotations

import textwrap

import pytest

from lakebench.benchmarks.tpcdi.finwire import (
    FINWIRE_STAGING_TABLES,
    parse_finwire_records,
)


def _write(tmp_path, name, content):
    p = tmp_path / name
    p.write_text(content)
    return p


def test_finwire_staging_table_names():
    assert FINWIRE_STAGING_TABLES == (
        "staging_finwire_cmp",
        "staging_finwire_sec",
        "staging_finwire_fin",
    )


def test_parse_cmp_record(tmp_path):
    # Build a CMP record by laying out the expected slices precisely.
    pts = "20200101-120000"  # 15 chars
    rec_type = "CMP"  # 3 chars at [15:18]
    company_name = "ACME CORP".ljust(60)
    cik = "0000123456"  # 10 chars
    status = "ACTV"  # 4
    industry_id = "TC"  # 2
    sp_rating = "AA  "  # 4
    founding_date = "19991231"  # 8
    addr1 = "100 MAIN ST".ljust(80)
    addr2 = "STE 200".ljust(80)
    postal = "94105".ljust(12)
    city = "SAN FRANCISCO".ljust(25)
    state = "CALIFORNIA".ljust(20)
    country = "USA".ljust(24)
    ceo = "JANE DOE".ljust(46)
    description = "A test company"
    line = (
        pts
        + rec_type
        + company_name
        + cik
        + status
        + industry_id
        + sp_rating
        + founding_date
        + addr1
        + addr2
        + postal
        + city
        + state
        + country
        + ceo
        + description
        + "\n"
    )

    f = _write(tmp_path, "FINWIRE2020Q1", line)
    cmp, sec, fin = parse_finwire_records(str(f))

    assert len(cmp) == 1 and not sec and not fin
    rec = cmp[0]
    assert rec["pts"] == "20200101-120000"
    assert rec["rec_type"] == "CMP"
    assert rec["company_name"] == "ACME CORP"
    assert rec["cik"] == 123456
    assert rec["status"] == "ACTV"
    assert rec["industry_id"] == "TC"
    assert rec["sp_rating"] == "AA"
    assert rec["founding_date"] == "19991231"
    assert rec["city"] == "SAN FRANCISCO"
    assert rec["country"] == "USA"
    assert rec["ceo_name"] == "JANE DOE"
    assert rec["description"] == "A test company"


def test_parse_skips_short_lines_and_unknown_types(tmp_path):
    f = _write(tmp_path, "FINWIRE2020Q1", "short\n" + ("x" * 18) + "UNK rest\n")
    cmp, sec, fin = parse_finwire_records(str(f))
    assert cmp == [] and sec == [] and fin == []


def test_parse_directory_glob(tmp_path):
    # Two FINWIRE files + one non-FINWIRE file → only the two are read.
    pts = "20200101-120000"
    sec_line = (
        pts
        + "SEC"
        + "AAPL".ljust(15)
        + "COMMON".ljust(6)
        + "ACTV"
        + "APPLE INC".ljust(70)
        + "NASDAQ"
        + "1000000000000"
        + "19801212"
        + "        "
        + "            "
        + "APPLE\n"
    )
    _write(tmp_path, "FINWIRE2020Q1", sec_line)
    _write(tmp_path, "FINWIRE2020Q2", sec_line)
    _write(tmp_path, "OTHER.csv", sec_line)  # excluded by .csv suffix
    _write(tmp_path, "README.txt", "ignored")  # excluded: not FINWIRE prefix

    cmp, sec, fin = parse_finwire_records(str(tmp_path))
    assert len(sec) == 2
    assert sec[0]["symbol"] == "AAPL"
    assert sec[0]["name"] == "APPLE INC"
    assert sec[0]["sh_out"] == 1_000_000_000_000


def test_parse_fin_handles_blank_numerics(tmp_path):
    pts = "20200101-120000"
    # Blank year/quarter/sh_out should become None, not raise.
    line = pts + "FIN" + (" " * 200) + "\n"
    f = _write(tmp_path, "FINWIRE2020Q1", line)
    _, _, fin = parse_finwire_records(str(f))
    assert len(fin) == 1
    assert fin[0]["year"] is None
    assert fin[0]["quarter"] is None
    assert fin[0]["sh_out"] is None
    assert fin[0]["revenue"] is None
