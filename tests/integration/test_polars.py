"""
Integration tests: all benchmarks with the Polars engine.

Run with:
    uv sync --group dev --extra polars --extra tpcds_duckdb_datagen --extra tpch_datagen
    uv run pytest tests/integration/test_tpc_polars.py -v -s
"""

import pytest

from tests.integration.conftest import report_and_assert, run_benchmark

pytest.importorskip("polars", reason="requires lakebench[polars] extra")
pytest.importorskip("deltalake", reason="requires lakebench[polars] extra")


def _engine(tmp_path, name):
    from lakebench.engines import Polars

    return Polars(schema_or_working_directory_uri=str(tmp_path / name))


@pytest.mark.integration
def test_tpch_polars(tpch_parquet_dir, tmp_path):
    from lakebench.benchmarks import TPCH

    results, exc = run_benchmark(_engine(tmp_path, "tpch"), TPCH, tpch_parquet_dir, "power_test", scale_factor=0.1)
    report_and_assert(results, "TPC-H", "Polars", exc)


@pytest.mark.integration
def test_tpcds_polars(tpcds_parquet_dir, tmp_path):
    from lakebench.benchmarks import TPCDS

    results, exc = run_benchmark(_engine(tmp_path, "tpcds"), TPCDS, tpcds_parquet_dir, "power_test", scale_factor=0.1)
    report_and_assert(results, "TPC-DS", "Polars", exc)


@pytest.mark.integration
def test_clickbench_polars(clickbench_parquet_dir, tmp_path):
    from lakebench.benchmarks import ClickBench

    results, exc = run_benchmark(_engine(tmp_path, "clickbench"), ClickBench, clickbench_parquet_dir, "power_test")
    report_and_assert(results, "ClickBench", "Polars", exc)


@pytest.mark.integration
def test_eltbench_polars(tpcds_parquet_dir, tmp_path):
    from lakebench.benchmarks import ELTBench

    results, exc = run_benchmark(_engine(tmp_path, "eltbench"), ELTBench, tpcds_parquet_dir, "light", scale_factor=0.1)
    report_and_assert(results, "ELTBench", "Polars", exc)
