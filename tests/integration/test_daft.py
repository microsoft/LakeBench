"""
Integration tests: all benchmarks with the Daft engine.

Run with:
    uv sync --group dev --extra daft --extra tpcds_duckdb_datagen --extra tpch_datagen
    uv run pytest tests/integration/test_tpc_daft.py -v -s
"""

import pytest

from lakebench.utils.path_utils import to_file_uri
from tests.integration.conftest import report_and_assert, run_benchmark

pytest.importorskip("daft", reason="requires lakebench[daft] extra")
pytest.importorskip("deltalake", reason="requires lakebench[daft] extra")


def _engine(tmp_path, name):
    from lakebench.engines import Daft

    return Daft(schema_or_working_directory_uri=str(tmp_path / name))


@pytest.mark.integration
def test_tpch_daft(tpch_parquet_dir, tmp_path):
    from lakebench.benchmarks import TPCH

    results, exc = run_benchmark(
        _engine(tmp_path, "tpch"), TPCH, to_file_uri(tpch_parquet_dir), "power_test", scale_factor=0.1
    )
    report_and_assert(results, "TPC-H", "Daft", exc)


@pytest.mark.integration
def test_tpcds_daft(tpcds_parquet_dir, tmp_path):
    from lakebench.benchmarks import TPCDS

    results, exc = run_benchmark(
        _engine(tmp_path, "tpcds"), TPCDS, to_file_uri(tpcds_parquet_dir), "power_test", scale_factor=0.1
    )
    report_and_assert(results, "TPC-DS", "Daft", exc)


@pytest.mark.integration
def test_clickbench_daft(clickbench_parquet_dir, tmp_path):
    from lakebench.benchmarks import ClickBench

    results, exc = run_benchmark(
        _engine(tmp_path, "clickbench"), ClickBench, to_file_uri(clickbench_parquet_dir), "power_test"
    )
    report_and_assert(results, "ClickBench", "Daft", exc)


@pytest.mark.integration
def test_eltbench_daft(tpcds_parquet_dir, tmp_path):
    from lakebench.benchmarks import ELTBench

    results, exc = run_benchmark(
        _engine(tmp_path, "eltbench"), ELTBench, to_file_uri(tpcds_parquet_dir), "light", scale_factor=0.1
    )
    report_and_assert(results, "ELTBench", "Daft", exc, min_pass_rate=1.0)
