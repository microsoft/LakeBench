import math
import os
import subprocess
from pathlib import Path
from unittest.mock import Mock

import pytest

from lakebench.datagen import TPCDSDataGenerator, TPCHDataGenerator, _tpcgen


@pytest.fixture
def fake_executable(monkeypatch):
    monkeypatch.setattr(_tpcgen, "_find_executable", lambda: "tpcgen-cli")
    return "tpcgen-cli"


@pytest.fixture
def commands(monkeypatch, fake_executable):
    run = Mock()
    monkeypatch.setattr(_tpcgen, "_run", run)
    return lambda: [call.args[0] for call in run.call_args_list]


def _arg(command, flag):
    return command[command.index(flag) + 1]


def test_tpch_parquet_command(tmp_path, commands):
    TPCHDataGenerator(scale_factor=10, target_folder_uri=str(tmp_path), table_list=["lineitem"]).run()

    (command,) = commands()
    assert command[:3] == ["tpcgen-cli", "tpch", "parquet"]
    assert _arg(command, "--scale-factor") == "10"
    assert _arg(command, "--output-dir") == str(tmp_path.resolve())
    assert _arg(command, "--tables") == "lineitem"
    assert _arg(command, "--parts") == "12"
    assert _arg(command, "--compression") == "ZSTD(1)"
    assert _arg(command, "--row-group-bytes") == "320MiB"
    assert "--num-threads" not in command


def test_tpcds_native_command(tmp_path, commands):
    TPCDSDataGenerator(
        scale_factor=1,
        target_folder_uri=str(tmp_path),
        table_list=["store_sales"],
        num_threads=4,
        output_format="native",
    ).run()

    (command,) = commands()
    assert command[1:3] == ["tpcds", "dat"]
    assert _arg(command, "--num-threads") == "4"
    assert "--compression" not in command
    assert "--row-group-bytes" not in command


def test_parquet_options_are_passed_through(tmp_path, commands):
    TPCHDataGenerator(
        scale_factor=1,
        target_folder_uri=str(tmp_path),
        table_list=["region"],
        compression="SNAPPY",
        target_row_group_size_mb=64,
    ).run()

    (command,) = commands()
    assert _arg(command, "--compression") == "SNAPPY"
    assert _arg(command, "--row-group-bytes") == "64MiB"


def test_parts_scale_with_table_size(fake_executable, tmp_path):
    generator = TPCHDataGenerator(scale_factor=1000, target_folder_uri=str(tmp_path))

    # 1501.7 MiB of lineitem at SF10, grown 100x (~147 GiB), in 256 MiB files.
    assert generator.parts_by_table["lineitem"] == 587
    assert generator.parts_by_table["nation"] == 1


@pytest.mark.parametrize(
    ("scale_factor", "output_format", "file_size_mib"),
    [(10, "parquet", 128), (1000, "parquet", 256), (100000, "parquet", 1024), (10, "native", 128)],
)
def test_file_size_steps_with_table_size(fake_executable, tmp_path, scale_factor, output_format, file_size_mib):
    generator = TPCHDataGenerator(
        scale_factor=scale_factor, target_folder_uri=str(tmp_path), table_list=["lineitem"], output_format=output_format
    )
    size_mib = generator.SF10_SIZE_MIB["lineitem"][output_format == "native"] * scale_factor / 10

    assert generator.parts_by_table["lineitem"] == math.ceil(size_mib / file_size_mib)


def test_native_parts_use_native_sizes(fake_executable, tmp_path):
    kwargs = {"scale_factor": 1000, "target_folder_uri": str(tmp_path), "table_list": ["store_sales"]}

    # Native text is ~4x the ZSTD(1) Parquet size, so it is split into ~4x the files.
    assert TPCDSDataGenerator(**kwargs).parts_by_table["store_sales"] == 376
    assert TPCDSDataGenerator(**kwargs, output_format="native").parts_by_table["store_sales"] == 1490


def test_row_group_default_is_per_benchmark(fake_executable, tmp_path):
    assert TPCHDataGenerator(1, str(tmp_path)).target_row_group_size_mb == 320
    assert TPCDSDataGenerator(1, str(tmp_path)).target_row_group_size_mb == 176


@pytest.mark.parametrize(
    ("scale_factor", "table", "expected_parts"),
    [
        (1000, "store_sales", 376),
        # Grows with the TPC-DS row-count model, not linearly: 783M rows at SF1000 vs 133M at SF10.
        (1000, "inventory", 20),
        (1000, "customer", 4),
        (10000, "customer", 21),
        (100000, "customer_demographics", 1),
        # Uses the nearest defined scale factor at or below: SF100.
        (200, "inventory", 10),
    ],
)
def test_tpcds_parts_follow_spec_row_counts(fake_executable, tmp_path, scale_factor, table, expected_parts):
    generator = TPCDSDataGenerator(scale_factor=scale_factor, target_folder_uri=str(tmp_path), table_list=[table])

    assert generator.parts_by_table[table] == expected_parts


def test_tables_with_equal_part_counts_share_one_command(tmp_path, commands):
    TPCHDataGenerator(scale_factor=1, target_folder_uri=str(tmp_path)).run()

    tables = sorted(table for command in commands() for table in _arg(command, "--tables").split(","))
    assert tables == sorted(TPCHDataGenerator.SF10_SIZE_MIB)
    assert len(commands()) == len(set(TPCHDataGenerator(1, str(tmp_path)).parts_by_table.values()))


def test_run_clears_previous_table_output(tmp_path, commands):
    stale = tmp_path / "region" / "region.2.parquet"
    stale.parent.mkdir()
    stale.write_bytes(b"old")

    TPCHDataGenerator(scale_factor=1, target_folder_uri=str(tmp_path), table_list=["region"]).run()

    assert not stale.exists()


def test_cleanup_failure_stops_generation(monkeypatch, tmp_path, commands):
    (tmp_path / "region").mkdir()
    monkeypatch.setattr(_tpcgen.shutil, "rmtree", Mock(side_effect=PermissionError("locked")))

    with pytest.raises(PermissionError):
        TPCHDataGenerator(scale_factor=1, target_folder_uri=str(tmp_path), table_list=["region"]).run()
    assert commands() == []


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"output_format": "csv"}, "output_format"),
        ({"table_list": []}, "at least one table"),
        ({"output_format": "native", "compression": "SNAPPY"}, "apply only to output_format='parquet'"),
        ({"output_format": "native", "target_row_group_size_mb": 64}, "apply only to output_format='parquet'"),
        ({"table_list": ["unknown"]}, "Unsupported tpch tables: unknown"),
        ({"target_folder_uri": "abfss://ws@onelake/files"}, "local or mounted paths"),
    ],
)
def test_invalid_options_are_rejected(tmp_path, fake_executable, kwargs, message):
    kwargs = {"scale_factor": 1, "target_folder_uri": str(tmp_path), **kwargs}
    with pytest.raises(ValueError, match=message):
        TPCHDataGenerator(**kwargs)


def test_file_uri_target(tmp_path, fake_executable):
    assert TPCHDataGenerator(1, tmp_path.as_uri()).target_folder == tmp_path.resolve()


def test_executable_is_found_in_interpreter_scripts_directory(monkeypatch, tmp_path):
    executable = tmp_path / ("tpcgen-cli.exe" if os.name == "nt" else "tpcgen-cli")
    executable.write_bytes(b"binary")
    executable.chmod(0o755)
    monkeypatch.setattr(_tpcgen.sysconfig, "get_path", lambda name: str(tmp_path))
    monkeypatch.setenv("PATH", "")

    assert Path(_tpcgen._find_executable()) == executable


def test_missing_executable_explains_how_to_install(monkeypatch, tmp_path):
    monkeypatch.setattr(_tpcgen.sysconfig, "get_path", lambda name: str(tmp_path))
    monkeypatch.setenv("PATH", "")

    with pytest.raises(ImportError, match=r"lakebench\[tpch_datagen\]"):
        _tpcgen._find_executable()


def test_cli_failure_surfaces_output(monkeypatch):
    error = subprocess.CalledProcessError(2, ["tpcgen-cli"], output="partial output", stderr="bad option")
    monkeypatch.setattr(subprocess, "run", Mock(side_effect=error))

    with pytest.raises(RuntimeError, match="partial output") as exc_info:
        _tpcgen._run(["tpcgen-cli"])
    assert "bad option" in str(exc_info.value)


def test_cli_rename_failure_suggests_fewer_threads(monkeypatch):
    error = subprocess.CalledProcessError(1, ["tpcgen-cli"], stderr='Failed to rename "a.inprogress" to "a"')
    monkeypatch.setattr(subprocess, "run", Mock(side_effect=error))

    with pytest.raises(RuntimeError, match="num_threads=8"):
        _tpcgen._run(["tpcgen-cli"])
