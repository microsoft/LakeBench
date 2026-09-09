import hashlib
import json
import os
import platform
import stat
import subprocess
from pathlib import Path
from typing import List, Optional, Tuple


class TpcgenCli:
    """Resolve and invoke the unified tpcgen-cli executable."""

    def __init__(self) -> None:
        self.executable, self.provenance = self._resolve_executable()
        self.binary_sha256 = self.provenance["binary_sha256"]

    @staticmethod
    def _resolve_executable() -> Tuple[str, dict]:
        binary_name = "tpcgen-cli.exe" if os.name == "nt" else "tpcgen-cli"
        packaged_path = Path(__file__).parent / "_bin" / binary_name
        if packaged_path.is_file():
            manifest = TpcgenCli._validate_packaged_binary(packaged_path)
            TpcgenCli._ensure_executable(packaged_path)
            return str(packaged_path), manifest

        source_tree_path = TpcgenCli._source_tree_binary(binary_name)
        if source_tree_path is not None:
            manifest = TpcgenCli._validate_packaged_binary(source_tree_path)
            TpcgenCli._ensure_executable(source_tree_path)
            return str(source_tree_path), manifest

        machine = platform.machine().lower()
        raise ImportError(
            "The bundled tpcgen-cli executable is unavailable for "
            f"{platform.system()} {machine}. Install a supported LakeBench wheel "
            "(Windows x86_64 or Linux x86_64). TPC-DS can alternatively use "
            "backend='duckdb' with lakebench[tpcds_duckdb_datagen] on Python 3.10 or newer."
        )

    @staticmethod
    def _source_tree_binary(binary_name: str) -> Optional[Path]:
        platform_key = (platform.system().lower(), platform.machine().lower())
        platform_directories = {
            ("windows", "amd64"): "windows-x86_64",
            ("windows", "x86_64"): "windows-x86_64",
            ("linux", "amd64"): "linux-x86_64",
            ("linux", "x86_64"): "linux-x86_64",
        }
        platform_directory = platform_directories.get(platform_key)
        if platform_directory is None:
            return None

        repository_root = Path(__file__).parents[3]
        binary_path = repository_root / "native" / "tpcgen" / platform_directory / binary_name
        return binary_path if binary_path.is_file() else None

    @staticmethod
    def _validate_packaged_binary(binary_path: Path) -> dict:
        manifest_path = binary_path.parent / "provenance.json"
        if not manifest_path.is_file():
            raise RuntimeError(f"Bundled tpcgen-cli provenance is missing: {manifest_path}")

        with manifest_path.open(encoding="utf-8") as manifest_file:
            manifest = json.load(manifest_file)

        expected_hash = manifest.get("binary_sha256")
        if not expected_hash:
            raise RuntimeError("Bundled tpcgen-cli provenance does not contain a SHA-256 checksum.")

        actual_hash = TpcgenCli._sha256(binary_path)
        if actual_hash != expected_hash:
            raise RuntimeError(f"Bundled tpcgen-cli checksum mismatch: expected {expected_hash}, got {actual_hash}.")
        return manifest

    @staticmethod
    def _sha256(binary_path: Path) -> str:
        digest = hashlib.sha256()
        with binary_path.open("rb") as binary_file:
            for chunk in iter(lambda: binary_file.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    @staticmethod
    def _ensure_executable(binary_path: Path) -> None:
        binary_path.chmod(binary_path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    def run(self, args: List[str]) -> subprocess.CompletedProcess:
        command = [self.executable] + args
        try:
            return subprocess.run(command, capture_output=True, text=True, check=True)
        except subprocess.CalledProcessError as exc:
            details = [f"tpcgen-cli failed with exit code {exc.returncode}: {' '.join(command)}"]
            if exc.stdout:
                details.append(f"stdout:\n{exc.stdout.rstrip()}")
            if exc.stderr:
                details.append(f"stderr:\n{exc.stderr.rstrip()}")
                if "Failed to rename" in exc.stderr and "os error 5" in exc.stderr:
                    details.append(
                        "The target mounted filesystem returned an I/O error while finalizing a generated file. "
                        "Remove the incomplete table directory and retry with a lower num_threads value, such as "
                        "num_threads=8 or num_threads=16, to reduce concurrent file creation and rename pressure."
                    )
            raise RuntimeError("\n".join(details)) from exc
