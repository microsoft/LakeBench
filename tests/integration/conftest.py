"""
Shared fixtures and helpers for all integration tests.

Data fixtures are session-scoped so TPC-H / TPC-DS parquet is generated once
per pytest session regardless of how many engine tests consume it.

Helpers
-------
report_and_assert(results, benchmark_name, engine_label, run_exception=None)
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

import datetime
import pathlib
import warnings

import pytest

pytest.importorskip("pyarrow", reason="integration tests require LakeBench's core pyarrow dependency")

# ---------------------------------------------------------------------------
# Session-level result collector (populated by report_and_assert)
# ---------------------------------------------------------------------------
_RESULTS: list[dict] = []


# ---------------------------------------------------------------------------
# Shared reporting helper
# ---------------------------------------------------------------------------


def report_and_assert(results, benchmark_name: str, engine_label: str, run_exception=None, min_pass_rate: float = 0.0):
    """Print a run summary, emit warnings on partial failures, and assert
    pass rate meets *min_pass_rate*.

    min_pass_rate=0.0 (default) — at least one step must succeed (⚠️ engines).
    min_pass_rate=1.0           — every step must succeed        (✅ engines).

    Works for both load-and-query benchmarks (TPC-H, TPC-DS, ClickBench) and
    task-based benchmarks (ELTBench).
    """
    load_results = [r for r in results if r["phase"] == "Load"]
    query_results = [r for r in results if r["phase"] == "Query"]

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

    # ELTBench: no Load/Query phases — treat every result as a "task"
    if not load_results and not query_results:
        task_results = results
        passed = [r for r in task_results if r["success"]]
        failed = [r for r in task_results if not r["success"]]

        print(f"\n{'=' * 60}")
        print(f"{benchmark_name} [{engine_label}]")
        print(f"  Tasks : {len(passed)}/{len(task_results)} passed, {len(failed)} failed")
        for r in failed:
            print(f"    x {r['test_item']} ({r['phase']}): {r['error_message'][:120]}")
        if run_exception:
            print(f"  [WARN] raised before completion: {type(run_exception).__name__}: {str(run_exception)[:200]}")
        print(f"{'=' * 60}")

        if len(task_results) == 0 and run_exception is not None:
            warnings.warn(
                f"{benchmark_name} [{engine_label}]: engine crashed before any tasks ran: "
                f"{type(run_exception).__name__}: {str(run_exception)[:200]}",
                UserWarning,
                stacklevel=2,
            )
            return

        if failed:
            warnings.warn(
                f"{benchmark_name} [{engine_label}]: {len(failed)} of {len(task_results)} "
                f"tasks failed: {[r['test_item'] for r in failed]}",
                UserWarning,
                stacklevel=2,
            )
        _assert_rate(passed, len(task_results), "tasks")
        _RESULTS.append(
            {
                "benchmark": benchmark_name,
                "engine": engine_label,
                "unit": "tasks",
                "passed": len(passed),
                "total": len(task_results),
                "failed": [{"name": r["test_item"], "phase": r["phase"], "error": r["error_message"]} for r in failed],
                "run_exception": str(run_exception) if run_exception else None,
                "timestamp": datetime.datetime.utcnow().isoformat(),
            }
        )
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
    if run_exception:
        print(f"  [WARN] raised before completion: {type(run_exception).__name__}: {str(run_exception)[:200]}")
    print(f"{'=' * 60}")

    if lf and len(lf) == len(load_results) and len(load_results) > 0:
        pytest.fail(
            f"{benchmark_name} [{engine_label}]: ALL {len(load_results)} tables failed to load. "
            f"First error: {lf[0]['error_message'][:200]}"
        )

    if len(query_results) == 0 and run_exception is not None:
        warnings.warn(
            f"{benchmark_name} [{engine_label}]: engine crashed before any queries ran: "
            f"{type(run_exception).__name__}: {str(run_exception)[:200]}",
            UserWarning,
            stacklevel=2,
        )
        return

    if failed:
        warnings.warn(
            f"{benchmark_name} [{engine_label}]: {len(failed)} of {len(query_results)} "
            f"queries failed: {[r['test_item'] for r in failed]}",
            UserWarning,
            stacklevel=2,
        )
    _assert_rate(passed, len(query_results), "queries")
    _RESULTS.append(
        {
            "benchmark": benchmark_name,
            "engine": engine_label,
            "unit": "queries",
            "passed": len(passed),
            "total": len(query_results),
            "failed": [{"name": r["test_item"], "phase": "Query", "error": r["error_message"]} for r in failed],
            "load_failed": [{"name": r["test_item"], "error": r["error_message"]} for r in lf],
            "run_exception": str(run_exception) if run_exception else None,
            "timestamp": datetime.datetime.utcnow().isoformat(),
        }
    )


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
    pytest.importorskip("duckdb", reason="source integration tests use the legacy DuckDB fallback")
    TPCDSDataGenerator(scale_factor=0.1, target_folder_uri=str(data_dir), backend="duckdb").run()
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
    for r in ordered:
        bm = r["benchmark"]
        passed = r["passed"]
        total = r["total"]
        unit = r["unit"]
        failed = r.get("failed", [])
        lf = r.get("load_failed", [])
        exc_str = r.get("run_exception")

        rate = passed / total if total > 0 else 0.0
        icon = "✅" if rate == 1.0 else ("⚠️" if rate > 0 else "❌")

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
