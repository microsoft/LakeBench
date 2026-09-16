"""
Integration tests: all benchmarks with the Daft engine.

Daft is unsupported as of LakeBench 2.0.0, so these are skipped by default. Daft
cannot read Parquet whose embedded Arrow schema uses `Utf8View`, which the bundled
TPC generators emit, so every TPC-H, TPC-DS, and ELTBench table fails to load.
The tests are kept so support can be re-verified in one command once Daft handles
the type; see reports/coverage/daft.md for the recorded failures.

Run with:
    uv sync --group dev --extra daft --extra tpcds_datagen --extra tpch_datagen
    LAKEBENCH_RUN_DAFT_INTEGRATION=1 uv run pytest tests/integration/test_daft.py -v -s
"""

import os

import pytest

from lakebench.utils.path_utils import to_file_uri
from tests.integration.conftest import report_and_assert, run_benchmark

pytest.importorskip("daft", reason="requires lakebench[daft] extra")
pytest.importorskip("deltalake", reason="requires lakebench[daft] extra")

pytestmark = pytest.mark.skipif(
    os.environ.get("LAKEBENCH_RUN_DAFT_INTEGRATION") != "1",
    reason=(
        "Daft is unsupported as of LakeBench 2.0.0: it cannot read the Arrow Utf8View the TPC "
        "generators emit. Set LAKEBENCH_RUN_DAFT_INTEGRATION=1 to re-check."
    ),
)


def _engine(tmp_path, name):
    from lakebench.engines import Daft

    return Daft(schema_or_working_directory_uri=str(tmp_path / name))


@pytest.mark.integration
def test_tpch_daft(tpch_parquet_dir, tmp_path):
    from lakebench.benchmarks import TPCH

    results, exc = run_benchmark(
        _engine(tmp_path, "tpch"), TPCH, to_file_uri(tpch_parquet_dir), "load_and_query", scale_factor=0.1
    )
    report_and_assert(results, "TPC-H", "Daft", exc)


@pytest.mark.integration
def test_tpcds_daft(tpcds_parquet_dir, tmp_path):
    from lakebench.benchmarks import TPCDS

    results, exc = run_benchmark(
        _engine(tmp_path, "tpcds"), TPCDS, to_file_uri(tpcds_parquet_dir), "load_and_query", scale_factor=0.1
    )
    report_and_assert(results, "TPC-DS", "Daft", exc)


@pytest.mark.integration
def test_clickbench_daft(clickbench_parquet_dir, tmp_path):
    from lakebench.benchmarks import ClickBench

    results, exc = run_benchmark(
        _engine(tmp_path, "clickbench"), ClickBench, to_file_uri(clickbench_parquet_dir), "load_and_query"
    )
    report_and_assert(results, "ClickBench", "Daft", exc)


@pytest.mark.integration
def test_eltbench_daft(tpcds_parquet_dir, tmp_path):
    from lakebench.benchmarks import ELTBench

    results, exc = run_benchmark(
        _engine(tmp_path, "eltbench"), ELTBench, to_file_uri(tpcds_parquet_dir), "light", scale_factor=0.1
    )
    report_and_assert(results, "ELTBench", "Daft", exc, min_pass_rate=0.0)
