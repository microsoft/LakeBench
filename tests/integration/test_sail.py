"""
Integration tests: all benchmarks with the Sail engine.

LakeBench manages the Sail session internally; no module-level fixture needed.

Run with:
    uv sync --group dev --extra sail --extra tpcds_datagen --extra tpch_datagen
    uv run pytest tests/integration/test_tpc_sail.py -v -s
"""

import pytest

from tests.integration.conftest import report_and_assert, run_benchmark

pytest.importorskip("pysail", reason="requires lakebench[sail] extra")
pytest.importorskip("pyspark", reason="requires lakebench[sail] extra")


def _engine(tmp_path, name):
    from lakebench.engines import Sail

    return Sail(schema_or_working_directory_uri=str(tmp_path / name).replace("\\", "/") + "/")


@pytest.mark.integration
def test_tpch_sail(tpch_parquet_dir, tmp_path):
    from lakebench.benchmarks import TPCH

    results, exc = run_benchmark(_engine(tmp_path, "tpch"), TPCH, tpch_parquet_dir, "power_test", scale_factor=0.1)
    report_and_assert(results, "TPC-H", "Sail", exc, min_pass_rate=1.0)


@pytest.mark.integration
def test_tpcds_sail(tpcds_parquet_dir, tmp_path):
    from lakebench.benchmarks import TPCDS

    results, exc = run_benchmark(_engine(tmp_path, "tpcds"), TPCDS, tpcds_parquet_dir, "power_test", scale_factor=0.1)
    report_and_assert(results, "TPC-DS", "Sail", exc, min_pass_rate=1.0)


@pytest.mark.integration
def test_clickbench_sail(clickbench_parquet_dir, tmp_path):
    from lakebench.benchmarks import ClickBench

    results, exc = run_benchmark(_engine(tmp_path, "clickbench"), ClickBench, clickbench_parquet_dir, "power_test")
    report_and_assert(results, "ClickBench", "Sail", exc, min_pass_rate=1.0)


@pytest.mark.integration
def test_eltbench_sail(tpcds_parquet_dir, tmp_path):
    from lakebench.benchmarks import ELTBench

    results, exc = run_benchmark(_engine(tmp_path, "eltbench"), ELTBench, tpcds_parquet_dir, "light", scale_factor=0.1)
    report_and_assert(results, "ELTBench", "Sail", exc, min_pass_rate=1.0)
