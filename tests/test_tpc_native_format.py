import subprocess
from pathlib import Path
from unittest.mock import Mock

import pytest

from lakebench.benchmarks.tpcds import TPCDS
from lakebench.benchmarks.tpch import TPCH
from lakebench.datagen._tpcds_rs import _TPCDSRsDataGenerator
from lakebench.datagen._tpch_rs import _TPCHRsDataGenerator
from lakebench.datagen.tpcds import TPCDSDataGenerator
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
from .test_tpcds_datagen import fake_executable  # noqa: F401

TPCDS_DDL = Path("src/lakebench/benchmarks/tpcds/resources/ddl/canonical/ddl_v4.0.0.simple.sql")
TPCH_DDL = Path("src/lakebench/benchmarks/tpch/resources/ddl/canonical/ddl_v3.0.1.simple.sql")


def _mock_generation(generator, extension):
    def create_outputs(args):
        output_path = Path(args[args.index("--output-dir") + 1])
        parts = int(args[args.index("--parts") + 1])
        for table_name in args[args.index("--tables") + 1].split(","):
            table_dir = output_path / table_name
            table_dir.mkdir(parents=True, exist_ok=True)
            for part_number in range(1, parts + 1):
                (table_dir / f"{table_name}.{part_number}.{extension}").write_bytes(b"row|")
        return subprocess.CompletedProcess(args, 0, "", "")

    generator.cli.run = Mock(side_effect=create_outputs)
    return generator


def test_tpcds_native_uses_dat_subcommand_and_omits_parquet_arguments(tmp_path, fake_executable):  # noqa: F811
    output_dir = tmp_path / "tpcds"
    generator = _mock_generation(
        _TPCDSRsDataGenerator(
            scale_factor=1,
            target_folder_uri=str(output_dir),
            table_list=["store_sales"],
            output_format="native",
        ),
        "dat",
    )
    generator.run()

    args = generator.cli.run.call_args.args[0]
    assert args[:2] == ["tpcds", "dat"]
    assert args[args.index("--compat") + 1] == "c"
    # `tpcgen-cli tpcds dat` rejects these Parquet-only arguments.
    assert "--compression" not in args
    assert "--row-group-bytes" not in args
    assert "--num-threads" not in args


def test_tpch_native_uses_tbl_subcommand_and_keeps_num_threads(tmp_path, fake_executable):  # noqa: F811
    output_dir = tmp_path / "tpch"
    generator = _mock_generation(
        _TPCHRsDataGenerator(
            scale_factor=1,
            target_folder_uri=str(output_dir),
            table_list=["lineitem"],
            num_threads=4,
            output_format="native",
        ),
        "tbl",
    )
    generator.run()

    args = generator.cli.run.call_args.args[0]
    assert args[:2] == ["tpch", "tbl"]
    assert args[args.index("--num-threads") + 1] == "4"
    assert "--compression" not in args
    assert "--row-group-bytes" not in args


def test_native_outputs_use_official_generator_file_names(tmp_path, fake_executable):  # noqa: F811
    output_dir = tmp_path / "tpch"
    generator = _mock_generation(
        _TPCHRsDataGenerator(
            scale_factor=1,
            target_folder_uri=str(output_dir),
            table_list=["nation"],
            output_format="native",
        ),
        "tbl",
    )
    generator.run()

    assert [path.name for path in (output_dir / "nation").glob("*")] == ["nation.tbl"]


def test_native_part_names_match_official_parallel_generators():
    tpch = _TPCHRsDataGenerator(scale_factor=1, target_folder_uri="/tmp/x", table_list=["orders"])
    tpcds = _TPCDSRsDataGenerator(scale_factor=1, target_folder_uri="/tmp/x", table_list=["store_sales"])

    # Serial dbgen/dsdgen write a single unsuffixed file per table.
    assert tpch._native_output_file_name("orders", 1, 1) == "orders.tbl"
    assert tpcds._native_output_file_name("store_sales", 1, 1) == "store_sales.dat"

    # Parallel dbgen writes <table>.tbl.<step>; dsdgen writes <table>_<child>_<parallel>.dat.
    assert tpch._native_output_file_name("orders", 3, 4) == "orders.tbl.3"
    assert tpcds._native_output_file_name("store_sales", 3, 4) == "store_sales_3_4.dat"


def test_native_globs_match_every_official_part_name():
    from fnmatch import fnmatch

    for benchmark, generator, table in (
        (TPCH, _TPCHRsDataGenerator, "orders"),
        (TPCDS, _TPCDSRsDataGenerator, "store_sales"),
    ):
        instance = generator(scale_factor=1, target_folder_uri="/tmp/x", table_list=[table])
        for part_count in (1, 4):
            for part_number in range(1, part_count + 1):
                name = instance._native_output_file_name(table, part_number, part_count)
                assert fnmatch(name, benchmark.NATIVE_FILE_GLOB), (benchmark.__name__, name)


def test_native_sizing_applies_measured_expansion_factor(tmp_path, fake_executable):  # noqa: F811
    shared = dict(scale_factor=1000, target_folder_uri=str(tmp_path / "t"), table_list=["lineitem"])
    parquet_generator = _TPCHRsDataGenerator(**shared)
    native_generator = _TPCHRsDataGenerator(**shared, output_format="native")

    uncompressed_gib = (
        _TPCHRsDataGenerator.SF1000_SIZE_GB_DICT["lineitem"]
        * _TPCHRsDataGenerator.ZSTD1_COMPRESSION_FACTOR_DICT["lineitem"]
    )
    expected = uncompressed_gib * _TPCHRsDataGenerator.NATIVE_SIZE_FACTOR_DICT["lineitem"]

    assert native_generator._estimated_table_size_gib("lineitem") == pytest.approx(expected)
    # Native text is larger than compressed Parquet, so it must be split more.
    assert native_generator.parts_by_table["lineitem"] > parquet_generator.parts_by_table["lineitem"]


@pytest.mark.parametrize("option", ["target_row_group_size_mb", "compression", "compression_factor"])
def test_native_rejects_parquet_only_options(tmp_path, fake_executable, option):  # noqa: F811
    values = {"target_row_group_size_mb": 256, "compression": "SNAPPY", "compression_factor": 2.0}
    with pytest.raises(ValueError, match="apply only to output_format='parquet'"):
        _TPCHRsDataGenerator(
            scale_factor=1,
            target_folder_uri=str(tmp_path / "t"),
            output_format="native",
            **{option: values[option]},
        )


def test_native_output_format_is_rejected_for_duckdb_backend(tmp_path):
    with pytest.raises(ValueError, match="supported only by backend='rust'"):
        TPCDSDataGenerator(
            scale_factor=1,
            target_folder_uri=str(tmp_path / "t"),
            backend="duckdb",
            output_format="native",
        )


def test_unknown_output_format_is_rejected(tmp_path, fake_executable):  # noqa: F811
    with pytest.raises(ValueError, match="output_format must be one of"):
        _TPCHRsDataGenerator(scale_factor=1, target_folder_uri=str(tmp_path / "t"), output_format="csv")


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


def test_native_size_factors_cover_every_generated_table():
    for generator in (_TPCHRsDataGenerator, _TPCDSRsDataGenerator):
        assert set(generator.NATIVE_SIZE_FACTOR_DICT) == set(generator.GEN_TABLE_REGISTRY)


def test_benchmarks_declare_native_extensions_matching_generators():
    assert TPCDS.NATIVE_FILE_EXTENSION == _TPCDSRsDataGenerator.NATIVE_FILE_EXTENSION == "dat"
    assert TPCH.NATIVE_FILE_EXTENSION == _TPCHRsDataGenerator.NATIVE_FILE_EXTENSION == "tbl"
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
