"""
Catalog discovery: fingerprint database/schema contents against known
benchmark table sets.

Used by the `lakebench discover` CLI subcommand. Pure logic — no engine
imports beyond benchmark TABLE_REGISTRY constants.
"""

from __future__ import annotations

from typing import Dict, Iterable, List, Set, Tuple

from lakebench.benchmarks.clickbench.clickbench import ClickBench
from lakebench.benchmarks.elt_bench.elt_bench import ELTBench
from lakebench.benchmarks.tpcds.tpcds import TPCDS
from lakebench.benchmarks.tpch.tpch import TPCH


def _norm(names: Iterable[str]) -> Set[str]:
    return {str(n).strip().lower() for n in names if n}


BENCHMARK_TABLES: Dict[str, Set[str]] = {
    "tpch": _norm(TPCH.TABLE_REGISTRY),
    "tpcds": _norm(TPCDS.TABLE_REGISTRY),
    "clickbench": _norm(ClickBench.TABLE_REGISTRY),
    "eltbench": _norm(ELTBench.TABLE_REGISTRY),
}


def fingerprint_schema(table_names: Iterable[str]) -> List[Tuple[str, int, int]]:
    """
    Return a list of (benchmark_name, matched_count, expected_count) tuples,
    sorted descending by match ratio. Only benchmarks with at least one
    matched table are returned.
    """
    have = _norm(table_names)
    out: List[Tuple[str, int, int]] = []
    for bench, expected in BENCHMARK_TABLES.items():
        matched = len(have & expected)
        if matched:
            out.append((bench, matched, len(expected)))
    return sorted(out, key=lambda x: (x[1] / x[2], x[1]), reverse=True)


def best_match(table_names: Iterable[str]) -> Tuple[str, int, int] | None:
    """
    Return the single best (benchmark, matched, expected) tuple, or None
    if no benchmark matches at all. ELTBench/TPCDS ties resolve to the
    first listed in BENCHMARK_TABLES (i.e. tpcds wins on equal ratio
    because of dict-insertion order in Python 3.7+).
    """
    candidates = fingerprint_schema(table_names)
    return candidates[0] if candidates else None


def all_equal_top_matches(table_names: Iterable[str]) -> List[Tuple[str, int, int]]:
    """
    Return all candidates tied at the top match ratio (handles the
    expected TPC-DS / ELTBench collision: same table set, same ratio).
    """
    candidates = fingerprint_schema(table_names)
    if not candidates:
        return []
    top_ratio = candidates[0][1] / candidates[0][2]
    return [c for c in candidates if c[1] / c[2] == top_ratio]
