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

    return DuckDB(schema_or_working_directory_uri=str(tmp_path / name), allow_destructive_reset=True)


@pytest.mark.integration
def test_tpch_duckdb(tpch_parquet_dir, tmp_path):
    from lakebench.benchmarks import TPCH

    results, exc = run_benchmark(_engine(tmp_path, "tpch"), TPCH, tpch_parquet_dir, "load_and_query", scale_factor=0.1)
    report_and_assert(results, "TPC-H", "DuckDB", exc, min_pass_rate=1.0)


@pytest.mark.integration
def test_tpcds_duckdb(tpcds_parquet_dir, tmp_path):
    from lakebench.benchmarks import TPCDS

    results, exc = run_benchmark(
        _engine(tmp_path, "tpcds"), TPCDS, tpcds_parquet_dir, "load_and_query", scale_factor=0.1
    )
    report_and_assert(results, "TPC-DS", "DuckDB", exc, min_pass_rate=1.0)


@pytest.mark.integration
def test_clickbench_duckdb(clickbench_parquet_dir, tmp_path):
    from lakebench.benchmarks import ClickBench

    results, exc = run_benchmark(_engine(tmp_path, "clickbench"), ClickBench, clickbench_parquet_dir, "load_and_query")
    report_and_assert(results, "ClickBench", "DuckDB", exc, min_pass_rate=1.0)


@pytest.mark.integration
def test_clickbench_duckdb_repeat_reset_preserves_input_and_results(clickbench_parquet_dir, tmp_path):
    from lakebench.benchmarks import ClickBench

    output = tmp_path / "output"
    result_uri = str(tmp_path / "results")
    engine = _engine(tmp_path, "output")
    benchmark = ClickBench(
        engine=engine,
        scenario_name="repeat-reset",
        query_list=["q1"],
        input_folder_uri=clickbench_parquet_dir,
        result_table_uri=result_uri,
        save_results=True,
    )
    for _ in range(2):
        benchmark.run(mode="load_and_query")
        assert benchmark.results and all(row["success"] for row in benchmark.results)
        assert not (output / "disposable-sentinel").exists()
        (output / "disposable-sentinel").write_text("old output")
    assert engine.duckdb.sql("SELECT count(*) FROM hits").fetchone()[0] == 100
    from deltalake import DeltaTable

    assert DeltaTable(result_uri).to_pyarrow_table().num_rows == len(benchmark.results)
    engine.allow_destructive_reset = False
    benchmark.run(mode="query")
    assert benchmark.results[-1]["success"]
    assert (output / "disposable-sentinel").read_text() == "old output"


@pytest.mark.integration
def test_eltbench_duckdb(tpcds_parquet_dir, tmp_path):
    from lakebench.benchmarks import ELTBench

    results, exc = run_benchmark(_engine(tmp_path, "eltbench"), ELTBench, tpcds_parquet_dir, "light", scale_factor=0.1)
    report_and_assert(results, "ELTBench", "DuckDB", exc, min_pass_rate=1.0)
