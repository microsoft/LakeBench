import subprocess
from pathlib import Path
from unittest.mock import Mock

import pytest

from lakebench.benchmarks.tpcds import TPCDS
from lakebench.benchmarks.tpch import TPCH
from lakebench.datagen.tpcds import TPCDSDataGenerator
from lakebench.datagen.tpch import TPCHDataGenerator
from lakebench.engines.base import BaseEngine
from lakebench.engines.duckdb import DuckDB
from lakebench.engines.polars import Polars
from lakebench.utils.schema_utils import (
    NATIVE_DELIMITER,
    TRAILING_DELIMITER_COLUMN,
    schema_to_pyarrow,
    schema_to_sql_types,
    table_schemas_from_ddl,
)

from .conftest import _uninitialized_engine

TPCDS_DDL = Path("src/lakebench/benchmarks/tpcds/resources/ddl/canonical/ddl_v4.0.0.simple.sql")
TPCH_DDL = Path("src/lakebench/benchmarks/tpch/resources/ddl/canonical/ddl_v3.0.1.simple.sql")


def test_ddl_schema_derivation_covers_every_table():
    tpcds_schemas = table_schemas_from_ddl(TPCDS_DDL.read_text())
    tpch_schemas = table_schemas_from_ddl(TPCH_DDL.read_text())

    assert set(tpcds_schemas) == set(TPCDS.TABLE_REGISTRY)
    assert set(tpch_schemas) == set(TPCH.TABLE_REGISTRY)
    assert [name for name, _ in tpch_schemas["region"]] == ["r_regionkey", "r_name", "r_comment"]


def test_ddl_schema_maps_to_pyarrow_and_sql_types():
    columns = table_schemas_from_ddl(TPCH_DDL.read_text())["lineitem"]
    arrow_schema = schema_to_pyarrow(columns)

    assert arrow_schema.field("l_orderkey").type == __import__("pyarrow").int64()
    assert arrow_schema.field("l_shipdate").type == __import__("pyarrow").date32()
    assert str(arrow_schema.field("l_quantity").type) == "decimal128(15, 2)"
    assert schema_to_sql_types(columns, "spark")["l_shipdate"] == "DATE"


def test_every_canonical_ddl_variant_derives_a_usable_schema():
    for ddl_path in Path("src/lakebench").rglob("resources/ddl/canonical/*.sql"):
        schemas = table_schemas_from_ddl(ddl_path.read_text())
        assert schemas, ddl_path
        for columns in schemas.values():
            schema_to_pyarrow(columns)
            schema_to_sql_types(columns, "duckdb")


def test_benchmarks_declare_native_extensions_matching_generators():
    assert TPCDS.NATIVE_FILE_EXTENSION == TPCDSDataGenerator.NATIVE_FORMAT == "dat"
    assert TPCH.NATIVE_FILE_EXTENSION == TPCHDataGenerator.NATIVE_FORMAT == "tbl"
    assert TPCDS.NATIVE_FILE_GLOB == "*.dat"
    assert TPCH.NATIVE_FILE_GLOB == "*.tbl*"


def test_trailing_delimiter_column_is_distinct_from_every_benchmark_column():
    for ddl_path in (TPCDS_DDL, TPCH_DDL):
        for columns in table_schemas_from_ddl(ddl_path.read_text()).values():
            assert TRAILING_DELIMITER_COLUMN not in {name for name, _ in columns}
    assert NATIVE_DELIMITER == "|"


def _benchmark(benchmark_class, engine_class, **kwargs):
    return benchmark_class(
        engine=_uninitialized_engine(engine_class),
        scenario_name="test",
        scale_factor=1000,
        input_parquet_folder_uri="file:///tmp/in",
        **kwargs,
    )


@pytest.mark.parametrize("benchmark_class", [TPCDS, TPCH])
def test_benchmark_accepts_native_input_format(benchmark_class):
    benchmark = _benchmark(benchmark_class, DuckDB, input_format="native")

    assert benchmark.input_format == "native"
    assert benchmark.engine.extended_engine_metadata["input_format"] == "native"
    assert set(benchmark._native_table_columns()) == set(benchmark_class.TABLE_REGISTRY)


@pytest.mark.parametrize("benchmark_class", [TPCDS, TPCH])
def test_benchmark_defaults_to_parquet_input_format(benchmark_class):
    benchmark = _benchmark(benchmark_class, DuckDB)

    assert benchmark.input_format == "parquet"
    assert benchmark.engine.extended_engine_metadata["input_format"] == "parquet"


def test_benchmark_rejects_unknown_input_format():
    with pytest.raises(ValueError, match="'input_format' must be one of"):
        _benchmark(TPCH, DuckDB, input_format="orc")


def test_native_input_format_is_rejected_without_a_native_extension(monkeypatch):
    monkeypatch.setattr(TPCH, "NATIVE_FILE_EXTENSION", None)
    with pytest.raises(ValueError, match="does not support input_format='native'"):
        _benchmark(TPCH, DuckDB, input_format="native")


def test_unsupported_engine_raises_rather_than_silently_loading_parquet():
    engine = _uninitialized_engine(Polars)
    with pytest.raises(NotImplementedError, match="native delimited format"):
        BaseEngine.load_delimited_to_delta(
            engine,
            folder_uri="file:///tmp/in/nation",
            table_name="nation",
            columns=[],
            file_pattern="*.tbl*",
        )


@pytest.mark.parametrize("benchmark_class", [TPCDS, TPCH])
def test_every_registered_engine_implements_the_native_loader(benchmark_class):
    """Sail subclasses BaseEngine rather than Spark, so inheritance cannot be assumed."""
    for engine_class in benchmark_class.BENCHMARK_IMPL_REGISTRY:
        if benchmark_class.BENCHMARK_IMPL_REGISTRY[engine_class] is not None:
            # Engine-specific implementations load Parquet directly.
            continue
        assert engine_class.load_delimited_to_delta is not BaseEngine.load_delimited_to_delta, (
            f"{engine_class.__name__} is registered for {benchmark_class.__name__} without a native delimited loader."
        )
