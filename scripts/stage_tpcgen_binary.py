import argparse
import hashlib
import json
import os
import stat
import subprocess
import zipfile
from pathlib import Path

UPSTREAM_REPOSITORY = "https://github.com/datafusion-contrib/tpcgen-rs"
UPSTREAM_COMMIT = "a0e30d55358bd5a21f2981e92b5978cdcb669d95"
UPSTREAM_LOCK_COMMIT = "1ec501fc4f23ee527b5da5dce646fb3c5dba2385"


def find_upstream_wheel(wheel_directory: Path) -> Path:
    wheels = list(wheel_directory.glob("tpcgen_cli-*.whl"))
    if len(wheels) != 1:
        raise RuntimeError(f"Expected one tpcgen-cli wheel in {wheel_directory}, found {len(wheels)}.")
    return wheels[0]


def extract_binary(wheel_path: Path, output_directory: Path, platform_name: str) -> Path:
    binary_name = "tpcgen-cli.exe" if platform_name == "windows" else "tpcgen-cli"
    with zipfile.ZipFile(wheel_path) as wheel:
        candidates = [name for name in wheel.namelist() if name.endswith(f"/scripts/{binary_name}")]
        if len(candidates) != 1:
            raise RuntimeError(f"Expected one {binary_name} script in {wheel_path}, found {len(candidates)}.")
        binary_path = output_directory / binary_name
        binary_path.write_bytes(wheel.read(candidates[0]))

    binary_path.chmod(binary_path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return binary_path


def run_smoke_test(binary_path: Path) -> str:
    version = subprocess.run(
        [str(binary_path), "--version"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    subprocess.run(
        [str(binary_path), "tpcds", "--help"],
        capture_output=True,
        text=True,
        check=True,
    )
    return version


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_handle:
        for chunk in iter(lambda: file_handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--wheel-directory", type=Path, required=True)
    parser.add_argument("--upstream-root", type=Path, required=True)
    parser.add_argument("--output-directory", type=Path, required=True)
    parser.add_argument("--platform", choices=("linux", "windows"), required=True)
    args = parser.parse_args()

    args.output_directory.mkdir(parents=True, exist_ok=True)
    wheel_path = find_upstream_wheel(args.wheel_directory)
    binary_path = extract_binary(wheel_path, args.output_directory, args.platform)
    version = run_smoke_test(binary_path)

    license_source = args.upstream_root / "LICENSE"
    license_target = args.output_directory / "LICENSE.tpcgen-rs"
    license_target.write_bytes(license_source.read_bytes())

    provenance = {
        "repository": UPSTREAM_REPOSITORY,
        "commit": UPSTREAM_COMMIT,
        "cargo_lock_commit": UPSTREAM_LOCK_COMMIT,
        "source_branch": "cl/feat/support-specific-part-generation",
        "pull_request": "https://github.com/datafusion-contrib/tpcgen-rs/pull/406",
        "cli_version": version,
        "platform": args.platform,
        "binary_name": binary_path.name,
        "binary_sha256": sha256(binary_path),
        "upstream_wheel": wheel_path.name,
    }
    provenance_path = args.output_directory / "provenance.json"
    provenance_path.write_text(json.dumps(provenance, indent=2) + os.linesep, encoding="utf-8")

    print(f"binary={binary_path.resolve()}")
    print(f"provenance={provenance_path.resolve()}")
    print(f"license={license_target.resolve()}")


if __name__ == "__main__":
    main()
