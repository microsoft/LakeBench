"""
Integration tests: all benchmarks with the DuckDB engine.

Run with:
    uv sync --group dev --extra duckdb --extra tpcds_datagen --extra tpch_datagen
    uv run pytest tests/integration/test_tpc_duckdb.py -v -s
"""

import pytest

from tests.integration.conftest import report_and_assert, run_benchmark

pytest.importorskip("duckdb", reason="requires lakebench[duckdb] extra")
pytest.importorskip("deltalake", reason="requires lakebench[duckdb] extra")


def _engine(tmp_path, name):
    from lakebench.engines import DuckDB

    return DuckDB(schema_or_working_directory_uri=str(tmp_path / name))


@pytest.mark.integration
def test_tpch_duckdb(tpch_parquet_dir, tmp_path):
    from lakebench.benchmarks import TPCH

    results, exc = run_benchmark(_engine(tmp_path, "tpch"), TPCH, tpch_parquet_dir, "power_test", scale_factor=0.1)
    report_and_assert(results, "TPC-H", "DuckDB", exc, min_pass_rate=1.0)


@pytest.mark.integration
def test_tpcds_duckdb(tpcds_parquet_dir, tmp_path):
    from lakebench.benchmarks import TPCDS

    results, exc = run_benchmark(_engine(tmp_path, "tpcds"), TPCDS, tpcds_parquet_dir, "power_test", scale_factor=0.1)
    report_and_assert(results, "TPC-DS", "DuckDB", exc, min_pass_rate=1.0)


@pytest.mark.integration
def test_clickbench_duckdb(clickbench_parquet_dir, tmp_path):
    from lakebench.benchmarks import ClickBench

    results, exc = run_benchmark(_engine(tmp_path, "clickbench"), ClickBench, clickbench_parquet_dir, "power_test")
    report_and_assert(results, "ClickBench", "DuckDB", exc, min_pass_rate=1.0)


@pytest.mark.integration
def test_eltbench_duckdb(tpcds_parquet_dir, tmp_path):
    from lakebench.benchmarks import ELTBench

    results, exc = run_benchmark(_engine(tmp_path, "eltbench"), ELTBench, tpcds_parquet_dir, "light", scale_factor=0.1)
    report_and_assert(results, "ELTBench", "DuckDB", exc, min_pass_rate=1.0)
