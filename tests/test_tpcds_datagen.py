import hashlib
import inspect
import json
import subprocess
from pathlib import Path
from unittest.mock import Mock

import pytest

from lakebench.datagen._tpcds_rs import _TPCDSRsDataGenerator
from lakebench.datagen._tpcgen_cli import TpcgenCli
from lakebench.datagen.tpcds import TPCDSDataGenerator
from lakebench.datagen.tpch import TPCHDataGenerator


@pytest.fixture
def fake_executable(tmp_path):
    executable = tmp_path / "tpcgen-cli"
    executable.write_bytes(b"fake")
    return str(executable)


def test_rust_generator_builds_automatic_multipart_command_and_outputs(tmp_path, fake_executable):
    output_dir = tmp_path / "tpcds"
    generator = _TPCDSRsDataGenerator(
        scale_factor=10,
        target_folder_uri=str(output_dir),
        table_list=["store_sales"],
        multithreading=False,
        executable=fake_executable,
    )

    def create_outputs(args):
        output_path = Path(args[args.index("--output-dir") + 1])
        table_dir = output_path / "store_sales"
        table_dir.mkdir()
        parts = int(args[args.index("--parts") + 1])
        for part_number in range(1, parts + 1):
            (table_dir / f"store_sales.{part_number}.parquet").write_bytes(b"parquet")
        return subprocess.CompletedProcess(args, 0, "", "")

    generator.cli.run = Mock(side_effect=create_outputs)
    generator.run()

    args = generator.cli.run.call_args.args[0]
    assert args[:2] == ["tpcds", "parquet"]
    assert args[args.index("--tables") + 1] == "store_sales"
    assert args[args.index("--parts") + 1] == "8"
    assert args[args.index("--compat") + 1] == "c"
    assert args[args.index("--num-threads") + 1] == "1"
    assert Path(args[args.index("--output-dir") + 1]) == output_dir.resolve()
    assert "--no-progress" in args
    assert len(list((output_dir / "store_sales").glob("*.parquet"))) == 8
    assert not list(output_dir.rglob("*.json"))


def test_public_generator_mirrors_tpch_generation_parameters():
    tpcds_parameters = list(inspect.signature(TPCDSDataGenerator).parameters)
    tpch_parameters = list(inspect.signature(TPCHDataGenerator).parameters)

    assert tpcds_parameters[:6] == tpch_parameters[:6]


def test_automatic_parts_group_tables_by_target_size(monkeypatch, tmp_path, fake_executable):
    output_dir = tmp_path / "tpcds"
    generator = _TPCDSRsDataGenerator(
        scale_factor=10,
        target_folder_uri=str(output_dir),
        table_list=["reason", "store_sales"],
        compression_factor=2.0,
        executable=fake_executable,
    )

    def create_outputs(args):
        staging_dir = Path(args[args.index("--output-dir") + 1])
        tables = args[args.index("--tables") + 1].split(",")
        parts = int(args[args.index("--parts") + 1])
        for table in tables:
            if parts == 1:
                (staging_dir / f"{table}.parquet").write_bytes(b"parquet")
            else:
                table_dir = staging_dir / table
                table_dir.mkdir()
                for part_number in range(1, parts + 1):
                    (table_dir / f"{table}.{part_number}.parquet").write_bytes(b"parquet")
        return subprocess.CompletedProcess(args, 0, "", "")

    generator.cli.run = Mock(side_effect=create_outputs)
    generator.run()

    assert generator.parts_by_table == {"reason": 1, "store_sales": 21}
    assert generator.cli.run.call_count == 2
    commands = [call.args[0] for call in generator.cli.run.call_args_list]
    assert {
        (command[command.index("--tables") + 1], command[command.index("--parts") + 1]) for command in commands
    } == {("reason", "1"), ("store_sales", "21")}


def test_default_zstd_row_group_target_uses_per_table_compression(tmp_path, fake_executable):
    generator = _TPCDSRsDataGenerator(
        scale_factor=1,
        target_folder_uri=str(tmp_path),
        target_row_group_size_mb=64,
        table_list=["inventory", "store_sales"],
        executable=fake_executable,
    )

    grouped = generator._group_tables_by_generation_settings()

    assert set(grouped) == {(1, 1.019), (1, 5.447)}
    inventory_command = generator._build_command(tmp_path, ["inventory"], 1)
    store_sales_command = generator._build_command(tmp_path, ["store_sales"], 1)
    assert int(inventory_command[inventory_command.index("--row-group-bytes") + 1]) == round(
        64 * 1.019 * 1.05 * 1024 * 1024
    )
    assert int(store_sales_command[store_sales_command.index("--row-group-bytes") + 1]) == round(
        64 * 5.447 * 1.05 * 1024 * 1024
    )


def test_snappy_uses_measured_compression_for_row_groups_and_parts(tmp_path, fake_executable):
    generator = _TPCDSRsDataGenerator(
        scale_factor=10,
        target_folder_uri=str(tmp_path),
        target_row_group_size_mb=64,
        compression="SNAPPY",
        table_list=["store_sales"],
        executable=fake_executable,
    )

    command = generator._build_command(tmp_path, ["store_sales"], generator.parts_by_table["store_sales"])

    assert generator.compression_factors_by_table["store_sales"] == 3.005
    assert generator._estimated_table_size_gib("store_sales") == pytest.approx(1.754, rel=0.002)
    assert generator.parts_by_table["store_sales"] == 14
    assert int(command[command.index("--row-group-bytes") + 1]) == round(64 * 3.005 * 1.05 * 1024 * 1024)


@pytest.mark.parametrize(
    ("scaled_size_gib", "expected_target_mib"),
    [
        (9.999, 128),
        (10, 256),
        (1023.999, 256),
        (1024, 512),
        (5119.999, 512),
        (5120, 1024),
    ],
)
def test_target_file_size_thresholds(tmp_path, fake_executable, scaled_size_gib, expected_target_mib):
    generator = _TPCDSRsDataGenerator(
        scale_factor=1,
        target_folder_uri=str(tmp_path),
        table_list=["reason"],
        executable=fake_executable,
    )

    assert generator._target_file_size_mb(scaled_size_gib) == expected_target_mib


@pytest.mark.parametrize(
    ("table_name", "scale_factor", "expected_parts"),
    [
        ("catalog_sales", 3, 2),
        ("catalog_sales", 10, 6),
        ("store_sales", 3, 2),
        ("store_sales", 10, 8),
        ("web_sales", 10, 3),
        ("web_sales", 20, 6),
        ("item", 1000, 5),
        ("customer_demographics", 1000, 2),
    ],
)
def test_automatic_parts_scale_directly_from_sf1000(
    tmp_path,
    fake_executable,
    table_name,
    scale_factor,
    expected_parts,
):
    generator = _TPCDSRsDataGenerator(
        scale_factor=scale_factor,
        target_folder_uri=str(tmp_path),
        table_list=[table_name],
        executable=fake_executable,
    )

    assert generator.parts_by_table[table_name] == expected_parts


@pytest.mark.parametrize(
    ("table_name", "scale_factor", "expected_size_gib"),
    [
        ("inventory", 10, 0.412427),
        ("inventory", 1000, 41.242681),
        ("customer", 1000, 1.855254),
        ("customer_demographics", 1000, 0.213324),
    ],
)
def test_table_size_scales_directly_from_sf1000(
    tmp_path,
    fake_executable,
    table_name,
    scale_factor,
    expected_size_gib,
):
    generator = _TPCDSRsDataGenerator(
        scale_factor=scale_factor,
        target_folder_uri=str(tmp_path),
        table_list=[table_name],
        executable=fake_executable,
    )

    assert generator._estimated_table_size_gib(table_name) == pytest.approx(expected_size_gib, rel=1e-5)


def test_row_group_target_is_adjusted_for_compression(tmp_path, fake_executable):
    generator = _TPCDSRsDataGenerator(
        scale_factor=1,
        target_folder_uri=str(tmp_path),
        target_row_group_size_mb=64,
        table_list=["reason"],
        compression_factor=3.25,
        executable=fake_executable,
    )

    command = generator._build_command(tmp_path, ["reason"], 1)

    assert int(command[command.index("--row-group-bytes") + 1]) == round(64 * 3.25 * 1.05 * 1024 * 1024)


def test_non_default_compression_requires_factor(tmp_path, fake_executable):
    with pytest.raises(ValueError, match="compression_factor is required"):
        _TPCDSRsDataGenerator(
            scale_factor=1,
            target_folder_uri=str(tmp_path),
            compression="GZIP(1)",
            executable=fake_executable,
        )


def test_unpartitioned_output_is_normalized_to_table_directory(tmp_path, fake_executable):
    output_dir = tmp_path / "tpcds"
    generator = _TPCDSRsDataGenerator(
        scale_factor=0.1,
        target_folder_uri=str(output_dir),
        table_list=["reason"],
        executable=fake_executable,
    )

    def create_output(args):
        staging_dir = Path(args[args.index("--output-dir") + 1])
        (staging_dir / "reason.parquet").write_bytes(b"parquet")
        return subprocess.CompletedProcess(args, 0, "", "")

    generator.cli.run = Mock(side_effect=create_output)
    generator.run()

    assert (output_dir / "reason" / "reason.1.parquet").read_bytes() == b"parquet"


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"table_list": ["unknown"]}, "Unsupported TPC-DS tables"),
    ],
)
def test_rust_generator_validates_options(tmp_path, fake_executable, kwargs, message):
    with pytest.raises(ValueError, match=message):
        _TPCDSRsDataGenerator(
            scale_factor=1,
            target_folder_uri=str(tmp_path / "output"),
            executable=fake_executable,
            **kwargs,
        )


def test_public_generator_dispatches_to_duckdb(monkeypatch, tmp_path):
    duckdb_generator = Mock()
    constructor = Mock(return_value=duckdb_generator)
    monkeypatch.setattr("lakebench.datagen.tpcds._TPCDSDuckDBDataGenerator", constructor)

    generator = TPCDSDataGenerator(1, str(tmp_path), backend="duckdb")
    generator.run()

    constructor.assert_called_once_with(
        scale_factor=1,
        target_folder_uri=str(tmp_path),
        target_row_group_size_mb=128,
    )
    duckdb_generator.run.assert_called_once_with()
    assert generator.scale_factor == 1
    assert generator.target_folder_uri == str(tmp_path)
    assert generator.backend == "duckdb"


def test_public_generator_rejects_unknown_backend(tmp_path):
    with pytest.raises(ValueError, match="backend"):
        TPCDSDataGenerator(1, str(tmp_path), backend="unknown")


def test_duckdb_backend_rejects_rust_only_options(tmp_path):
    with pytest.raises(ValueError, match="compression, multithreading"):
        TPCDSDataGenerator(
            1,
            str(tmp_path),
            backend="duckdb",
            compression="SNAPPY",
            multithreading=False,
        )


def test_packaged_binary_checksum_validation(tmp_path):
    binary = tmp_path / "tpcgen-cli"
    binary.write_bytes(b"binary")
    checksum = hashlib.sha256(b"binary").hexdigest()
    (tmp_path / "provenance.json").write_text(
        json.dumps({"binary_sha256": checksum}),
        encoding="utf-8",
    )

    TpcgenCli._validate_packaged_binary(binary)

    (tmp_path / "provenance.json").write_text(
        json.dumps({"binary_sha256": "0" * 64}),
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError, match="checksum mismatch"):
        TpcgenCli._validate_packaged_binary(binary)


def test_vendored_binary_checksums_match_provenance():
    bundle_root = Path(__file__).parents[1] / "native" / "tpcgen"
    for platform_directory, binary_name in (
        ("windows-x86_64", "tpcgen-cli.exe"),
        ("linux-x86_64", "tpcgen-cli"),
    ):
        binary_path = bundle_root / platform_directory / binary_name
        manifest = TpcgenCli._validate_packaged_binary(binary_path)

        assert manifest["commit"] == "a0e30d55358bd5a21f2981e92b5978cdcb669d95"
        assert manifest["binary_name"] == binary_name


def test_cli_surfaces_subprocess_output(monkeypatch, fake_executable):
    error = subprocess.CalledProcessError(
        2,
        [fake_executable],
        output="partial output",
        stderr="bad option",
    )
    monkeypatch.setattr(subprocess, "run", Mock(side_effect=error))

    with pytest.raises(RuntimeError, match="partial output") as exc_info:
        TpcgenCli(fake_executable).run(["tpcds", "parquet"])
    assert "bad option" in str(exc_info.value)


def test_shared_cli_runner_accepts_future_tpch_command(monkeypatch, fake_executable):
    run = Mock(return_value=subprocess.CompletedProcess([], 0, "", ""))
    monkeypatch.setattr(subprocess, "run", run)

    TpcgenCli(fake_executable).run(["tpch", "parquet", "--scale-factor", "1"])

    run.assert_called_once_with(
        [fake_executable, "tpch", "parquet", "--scale-factor", "1"],
        capture_output=True,
        text=True,
        check=True,
    )
