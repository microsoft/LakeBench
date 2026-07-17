"""
Integration tests: all benchmarks with the Spark engine (local mode + Delta Lake).

Query failures are warnings; the test fails only if ALL loads or ALL queries fail.
JVM crashes (Windows) are caught and reported as warnings.

Run with:
    uv sync --group dev --extra spark --extra tpcds_datagen --extra tpch_datagen
    uv run pytest tests/integration/test_tpc_spark.py -v -s
"""

import warnings

import pytest

from tests.integration.conftest import report_and_assert, run_benchmark

pytest.importorskip("pyspark", reason="requires lakebench[spark] extra")


# ---------------------------------------------------------------------------
# JVM lifecycle — keeps the SparkSession alive across all tests in this module.
# PySpark's SparkContext.__del__ stops the JVM when the last Python reference
# is GC'd, so without this fixture the JVM dies between tests.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module", autouse=True)
def _spark_session_lifecycle(tmp_path_factory):
    import platform

    from pyspark.sql import SparkSession

    warehouse = str(tmp_path_factory.mktemp("spark_warehouse")).replace("\\", "/") + "/"
    builder = (
        SparkSession.builder.master("local[*]")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.driver.host", "localhost")
        .config("spark.driver.bindAddress", "localhost")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.catalogImplementation", "hive")
    )
    if platform.system() == "Windows":
        builder = builder.config("spark.hadoop.io.native.lib.available", "false").config(
            "spark.hadoop.fs.file.impl.disable.cache", "true"
        )
    spark = builder.getOrCreate()
    yield spark
    try:
        spark.stop()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Engine factory — Spark takes schema_name + schema_uri separately
# ---------------------------------------------------------------------------


def _engine(tmp_path, name):
    from lakebench.engines import Spark

    schema_uri = str(tmp_path / name).replace("\\", "/") + "/"
    try:
        return Spark(schema_name=name, schema_uri=schema_uri)
    except Exception as e:
        return e  # caller checks isinstance(engine, Exception)


def _run(engine_or_exc, BenchmarkCls, input_dir, run_mode, benchmark_name, **kwargs):
    """Wrap _engine result: if JVM is dead return empty results with the exc."""
    if isinstance(engine_or_exc, Exception):
        warnings.warn(
            f"{benchmark_name} [Spark]: JVM unavailable at test start: {engine_or_exc}",
            UserWarning,
            stacklevel=2,
        )
        return [], None
    return run_benchmark(engine_or_exc, BenchmarkCls, input_dir, run_mode, **kwargs)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_tpch_spark(tpch_parquet_dir, tmp_path):
    from lakebench.benchmarks import TPCH

    engine = _engine(tmp_path, "tpch")
    results, exc = _run(engine, TPCH, tpch_parquet_dir, "power_test", "TPC-H", scale_factor=0.1)
    if results is not None:
        report_and_assert(results, "TPC-H", "Spark", exc, min_pass_rate=1.0)


@pytest.mark.integration
def test_tpcds_spark(tpcds_parquet_dir, tmp_path):
    from lakebench.benchmarks import TPCDS

    engine = _engine(tmp_path, "tpcds")
    results, exc = _run(engine, TPCDS, tpcds_parquet_dir, "power_test", "TPC-DS", scale_factor=0.1)
    if results is not None:
        report_and_assert(results, "TPC-DS", "Spark", exc, min_pass_rate=1.0)


@pytest.mark.integration
def test_clickbench_spark(clickbench_parquet_dir, tmp_path):
    from lakebench.benchmarks import ClickBench

    engine = _engine(tmp_path, "clickbench")
    results, exc = _run(engine, ClickBench, clickbench_parquet_dir, "power_test", "ClickBench")
    if results is not None:
        report_and_assert(results, "ClickBench", "Spark", exc, min_pass_rate=1.0)


@pytest.mark.integration
def test_eltbench_spark(tpcds_parquet_dir, tmp_path):
    from lakebench.benchmarks import ELTBench

    engine = _engine(tmp_path, "eltbench")
    results, exc = _run(engine, ELTBench, tpcds_parquet_dir, "light", "ELTBench", scale_factor=0.1)
    if results is not None:
        report_and_assert(results, "ELTBench", "Spark", exc, min_pass_rate=1.0)
