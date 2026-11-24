"""
Shared fixtures and helpers for all integration tests.

Data fixtures are session-scoped so TPC-H / TPC-DS parquet is generated once
per pytest session regardless of how many engine tests consume it.

Helpers
-------
expected_workload(BenchmarkCls, run_mode)
    Declares the complete default workload for the requested benchmark mode.

report_and_assert(results, benchmark_name, engine_label, ..., expected_results=...)
    Unified pass/fail reporting used by every engine test file.

run_benchmark(engine, BenchmarkCls, input_dir, run_mode, **kwargs)
    Instantiates a benchmark, runs it (catching exceptions), and returns
    (results, run_exception) for callers to pass to report_and_assert.

Report generation
-----------------
After a pytest session, per-engine markdown reports are written to
reports/coverage/<engine>.md  whenever report_and_assert is called at least
once.  Run any integration test to refresh the reports.
"""

from __future__ import annotations

import datetime
import pathlib
import warnings
from collections import Counter

import pytest

pytest.importorskip("pyarrow", reason="integration tests require LakeBench's core pyarrow dependency")

# ---------------------------------------------------------------------------
# Session-level result collector (populated by report_and_assert)
# ---------------------------------------------------------------------------
_RESULTS: list[dict] = []


# ---------------------------------------------------------------------------
# Shared reporting helper
# ---------------------------------------------------------------------------


def expected_workload(BenchmarkCls, run_mode: str):
    """Return expected (phase, sub_phase, test_item, iteration) keys.

    This describes the default full query/table selection, without optional
    load optimization or analysis. Custom runs must declare their own keys.
    """
    steps = []
    if BenchmarkCls.__name__ == "ELTBench":
        if run_mode != "light":
            raise ValueError(f"No ELTBench integration workload declared for mode {run_mode!r}.")
        steps.extend(
            ("Read parquet, write delta (x5)", None, table)
            for table in ("store_sales", "date_dim", "store", "item", "customer")
        )
        steps.append(("Create fact table", None, "total_sales_fact"))
        steps.extend([("Merge 0.1% into fact table (3x)", None, "total_sales_fact")] * 3)
        steps.extend(
            (phase, None, "total_sales_fact")
            for phase in ("OPTIMIZE", "VACUUM", "Ad-hoc query (small result aggregation)")
        )
    else:
        if run_mode not in ("load", "query", "power_test", "load_and_query"):
            raise ValueError(f"No integration workload declared for mode {run_mode!r}.")
        if run_mode in ("load", "load_and_query"):
            steps.extend(("Load", "load", table) for table in BenchmarkCls.TABLE_REGISTRY)
        if run_mode in ("query", "power_test", "load_and_query"):
            steps.extend(("Query", None, query) for query in BenchmarkCls.QUERY_REGISTRY)

    occurrences = Counter()
    expected = []
    for step in steps:
        occurrences[step] += 1
        expected.append((*step, occurrences[step]))
    return expected


def report_and_assert(
    results,
    benchmark_name: str,
    engine_label: str,
    run_exception=None,
    *,
    expected_results,
    min_pass_rate: float = 0.0,
):
    """Require a complete, uninterrupted workload before checking pass rate.

    min_pass_rate=0.0 (default) — at least one step must succeed (⚠️ engines).
    min_pass_rate=1.0           — every step must succeed        (✅ engines).

    All Load-phase results must succeed, independently of the query/task threshold.
    Expected keys include phase, sub_phase, test_item, and iteration so repeated
    tasks are distinguished from duplicate results.
    """
    expected = Counter(expected_results)
    if not expected or any(count != 1 for count in expected.values()):
        raise ValueError("expected_results must contain nonempty, unique workload keys.")
    actual = Counter((r["phase"], r["sub_phase"], r["test_item"], r["iteration"]) for r in results)
    workload_errors = []
    missing = list((expected - actual).elements())
    extra = list((actual - expected).elements())
    if missing:
        workload_errors.append(f"missing results: {missing}")
    if extra:
        workload_errors.append(f"unexpected or duplicate results: {extra}")

    load_results = [r for r in results if r["phase"] == "Load"]
    query_results = [r for r in results if r["phase"] == "Query"]

    def _assert_complete():
        if run_exception is not None:
            pytest.fail(
                f"{benchmark_name} [{engine_label}]: run aborted: "
                f"{type(run_exception).__name__}: {str(run_exception)[:200]}"
            )
        if workload_errors:
            pytest.fail(f"{benchmark_name} [{engine_label}]: incomplete workload: {'; '.join(workload_errors)}")

    def _assert_rate(passed, total, unit):
        if total == 0:
            return
        rate = len(passed) / total
        if min_pass_rate > 0.0:
            assert rate >= min_pass_rate, (
                f"{benchmark_name} [{engine_label}]: pass rate "
                f"{rate:.1%} ({len(passed)}/{total} {unit}) "
                f"is below required {min_pass_rate:.0%}."
            )
        else:
            assert len(passed) > 0, f"{benchmark_name} [{engine_label}]: ALL {total} {unit} failed."

    # ELTBench uses task-specific phase names instead of Load/Query.
    if benchmark_name == "ELTBench":
        task_results = results
        passed = [r for r in task_results if r["success"]]
        failed = [r for r in task_results if not r["success"]]

        print(f"\n{'=' * 60}")
        print(f"{benchmark_name} [{engine_label}]")
        print(f"  Tasks : {len(passed)}/{len(task_results)} passed, {len(failed)} failed")
        for r in failed:
            print(f"    x {r['test_item']} ({r['phase']}): {r['error_message'][:120]}")
        if run_exception is not None:
            print(f"  [WARN] raised before completion: {type(run_exception).__name__}: {str(run_exception)[:200]}")
        print(f"{'=' * 60}")

        _RESULTS.append(
            {
                "benchmark": benchmark_name,
                "engine": engine_label,
                "unit": "tasks",
                "passed": len(passed),
                "total": len(task_results),
                "failed": [{"name": r["test_item"], "phase": r["phase"], "error": r["error_message"]} for r in failed],
                "run_exception": f"{type(run_exception).__name__}: {run_exception}"
                if run_exception is not None
                else None,
                "workload_errors": workload_errors,
                "timestamp": datetime.datetime.utcnow().isoformat(),
            }
        )

        _assert_complete()

        if failed:
            warnings.warn(
                f"{benchmark_name} [{engine_label}]: {len(failed)} of {len(task_results)} "
                f"tasks failed: {[r['test_item'] for r in failed]}",
                UserWarning,
                stacklevel=2,
            )
        _assert_rate(passed, len(task_results), "tasks")
        return

    # Load-and-query benchmarks (TPC-H, TPC-DS, ClickBench)
    passed = [r for r in query_results if r["success"]]
    failed = [r for r in query_results if not r["success"]]
    lf = [r for r in load_results if not r["success"]]

    print(f"\n{'=' * 60}")
    print(f"{benchmark_name} [{engine_label}]")
    print(
        f"  Load  : {len(load_results) - len(lf)}/{len(load_results)} tables loaded OK"
        + (f"  [WARN] failed: {[r['test_item'] for r in lf]}" if lf else "")
    )
    print(f"  Query : {len(passed)}/{len(query_results)} passed, {len(failed)} failed")
    for r in failed:
        print(f"    x {r['test_item']}: {r['error_message'][:120]}")
    if run_exception is not None:
        print(f"  [WARN] raised before completion: {type(run_exception).__name__}: {str(run_exception)[:200]}")
    print(f"{'=' * 60}")

    _RESULTS.append(
        {
            "benchmark": benchmark_name,
            "engine": engine_label,
            "unit": "queries",
            "passed": len(passed),
            "total": len(query_results),
            "failed": [{"name": r["test_item"], "phase": "Query", "error": r["error_message"]} for r in failed],
            "load_failed": [{"name": r["test_item"], "error": r["error_message"]} for r in lf],
            "run_exception": f"{type(run_exception).__name__}: {run_exception}" if run_exception is not None else None,
            "workload_errors": workload_errors,
            "timestamp": datetime.datetime.utcnow().isoformat(),
        }
    )

    _assert_complete()

    if lf:
        pytest.fail(
            f"{benchmark_name} [{engine_label}]: {len(lf)} of {len(load_results)} tables failed to load. "
            f"First error: {lf[0]['error_message'][:200]}"
        )

    if failed:
        warnings.warn(
            f"{benchmark_name} [{engine_label}]: {len(failed)} of {len(query_results)} "
            f"queries failed: {[r['test_item'] for r in failed]}",
            UserWarning,
            stacklevel=2,
        )
    _assert_rate(passed, len(query_results), "queries")


# ---------------------------------------------------------------------------
# Shared benchmark runner
# ---------------------------------------------------------------------------


def run_benchmark(engine, BenchmarkCls, input_dir: str, run_mode: str, **kwargs):
    """Instantiate *BenchmarkCls*, run it, and return (results, exception).

    The exception is None on a clean run.  Callers pass both values straight
    to report_and_assert().
    """
    benchmark = BenchmarkCls(
        engine=engine,
        scenario_name="sf1",
        input_parquet_folder_uri=input_dir,
        **kwargs,
    )
    exc = None
    try:
        benchmark.run(mode=run_mode)
    except Exception as e:
        exc = e
    return benchmark.results, exc


# ---------------------------------------------------------------------------
# Data fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def tpch_parquet_dir(tmp_path_factory):
    """Generate TPC-H SF0.1 parquet data once per session."""
    from lakebench.datagen import TPCHDataGenerator

    data_dir = tmp_path_factory.mktemp("tpch_sf0.1")
    print(f"\n[datagen] Generating TPC-H SF0.1 -> {data_dir}")
    TPCHDataGenerator(scale_factor=0.1, target_folder_uri=str(data_dir)).run()
    return str(data_dir)


@pytest.fixture(scope="session")
def tpcds_parquet_dir(tmp_path_factory):
    """Generate TPC-DS SF0.1 parquet data once per session."""
    from lakebench.datagen import TPCDSDataGenerator

    data_dir = tmp_path_factory.mktemp("tpcds_sf0.1")
    print(f"\n[datagen] Generating TPC-DS SF0.1 -> {data_dir}")
    TPCDSDataGenerator(scale_factor=0.1, target_folder_uri=str(data_dir)).run()
    return str(data_dir)


@pytest.fixture(scope="session")
def clickbench_parquet_dir():
    """Return the directory containing the committed ClickBench 100-row sample."""
    data_dir = pathlib.Path(__file__).parent / "data"
    assert (data_dir / "clickbench_sample.parquet").exists(), (
        "ClickBench sample parquet not found. Run: python tests/integration/data/generate_clickbench_sample.py"
    )
    return str(data_dir)


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

_BENCHMARK_ORDER = ["TPC-H", "TPC-DS", "ClickBench", "ELTBench"]
_DOCS_DIR = pathlib.Path(__file__).parent.parent.parent / "reports" / "coverage"


def _engine_slug(label: str) -> str:
    """'DuckDB' → 'duckdb',  'Polars 1.x ...' → 'polars'"""
    return label.split()[0].lower()


def _render_engine_report(engine_label: str, records: list) -> str:
    ordered = sorted(
        records, key=lambda r: _BENCHMARK_ORDER.index(r["benchmark"]) if r["benchmark"] in _BENCHMARK_ORDER else 99
    )
    ts = max(r["timestamp"] for r in records)
    lines = [
        f"# {engine_label} Benchmark Report",
        "",
        "_Auto-generated by the LakeBench integration test suite._  ",
        f"_Last updated: {ts[:19].replace('T', ' ')} UTC_",
        "",
        "---",
        "",
    ]
    has_utf8view_failure = any(
        "Utf8View" in item.get("error", "")
        for record in records
        for key in ("failed", "load_failed")
        for item in record.get(key, [])
    )
    if _engine_slug(engine_label) == "daft" and has_utf8view_failure:
        lines += [
            "> **Compatibility note:** Daft is unsupported as of LakeBench 2.0.0. Daft 0.7.21, used by this "
            "LakeBench test lane, cannot read Parquet files whose embedded Arrow schema uses `Utf8View`. The "
            "latest PyPI release tested, Daft 0.7.24, fails with the same unsupported-type error. The bundled "
            "TPC generator intentionally retains this Arrow type, so affected TPC and ELTBench loads are "
            "reported as unsupported rather than rewritten during generation. The Daft integration tests are "
            "excluded from CI and skipped unless `LAKEBENCH_RUN_DAFT_INTEGRATION=1` is set.",
            "",
        ]
    for r in ordered:
        bm = r["benchmark"]
        passed = r["passed"]
        total = r["total"]
        unit = r["unit"]
        failed = r.get("failed", [])
        lf = r.get("load_failed", [])
        exc_str = r.get("run_exception")
        workload_errors = r.get("workload_errors", [])

        rate = passed / total if total > 0 else 0.0
        icon = "✅" if rate == 1.0 else ("⚠️" if rate > 0 else "❌")
        if exc_str or workload_errors or lf:
            icon = "❌"

        lines += [
            f"## {bm}",
            "",
            f"{icon} **{passed}/{total} {unit} passed ({rate:.1%})**",
            "",
        ]

        if lf:
            lines += [
                "### Load failures",
                "",
                "| Table | Error |",
                "|-------|-------|",
            ]
            for item in lf:
                err = item["error"][:200].replace("\n", " ").replace("|", "\\|")
                lines.append(f"| `{item['name']}` | {err} |")
            lines.append("")

        if failed:
            cap = unit.rstrip("s").title()
            lines += [
                f"### Failed {unit}",
                "",
                f"| {cap} | Error |",
                "|---|---|",
            ]
            for item in failed:
                err = item["error"][:300].replace("\n", " ").replace("|", "\\|")
                lines.append(f"| `{item['name']}` | {err} |")
            lines.append("")

        if exc_str:
            lines += [
                f"> ⚠️ Run terminated early: `{exc_str[:300]}`",
                "",
            ]

        if workload_errors:
            lines += [
                "### Incomplete workload",
                "",
                *[f"- {error}" for error in workload_errors],
                "",
            ]

        lines.append("---")
        lines.append("")

    return "\n".join(lines)


def pytest_sessionfinish(session, exitstatus):
    """Write per-engine markdown reports after the test session."""
    if not _RESULTS:
        return

    from collections import defaultdict

    by_engine: dict[str, list] = defaultdict(list)
    for r in _RESULTS:
        by_engine[r["engine"]].append(r)

    _DOCS_DIR.mkdir(parents=True, exist_ok=True)
    for engine_label, records in by_engine.items():
        slug = _engine_slug(engine_label)
        out = _DOCS_DIR / f"{slug}.md"
        # Merge with existing records for other benchmarks not run this session
        existing = _load_existing_records(out)
        merged = _merge_records(existing, records)
        out.write_text(_render_engine_report(engine_label, merged), encoding="utf-8")
        print(f"\n[report] {out}")


def _load_existing_records(path: pathlib.Path) -> list:
    """No-op: we don't persist JSON, so existing records are always empty.
    Override this if you add a JSON sidecar in the future."""
    return []


def _merge_records(existing: list, fresh: list) -> list:
    """Fresh results win; keep existing records for benchmarks not in fresh."""
    fresh_keys = {(r["benchmark"], r["engine"]) for r in fresh}
    kept = [r for r in existing if (r["benchmark"], r["engine"]) not in fresh_keys]
    return kept + fresh
