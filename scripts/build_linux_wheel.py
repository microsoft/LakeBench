# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "cibuildwheel==4.2.0",
# ]
# ///

import argparse
import hashlib
import os
import shutil
import subprocess
import sys
from pathlib import Path

import tomllib

BUILD_IDENTIFIER = "cp311-manylinux_x86_64"


def _project_configuration(project_root: Path) -> dict:
    with (project_root / "pyproject.toml").open("rb") as file:
        return tomllib.load(file)


def _require_docker() -> None:
    if shutil.which("docker") is None:
        raise RuntimeError(
            "Docker is required to build a Linux wheel. Install and start Docker Desktop, then rerun this command."
        )

    result = subprocess.run(
        ["docker", "info"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        details = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(
            f"Docker is installed but unavailable. Start Docker Desktop and rerun this command.\n{details}"
        )


def _require_uv() -> str:
    uv = shutil.which("uv")
    if uv is None:
        raise RuntimeError("uv is required to prepare the offline Linux build environment.")
    return uv


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the LakeBench Linux x86_64 wheel using Docker.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("dist"),
        help="Wheel output directory relative to the repository root (default: dist).",
    )
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[1]
    output_dir = args.output_dir if args.output_dir.is_absolute() else project_root / args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    _require_docker()
    uv = _require_uv()

    configuration = _project_configuration(project_root)
    version = configuration["project"]["version"]
    wheel_pattern = f"lakebench-{version}-py3-none-manylinux_2_17_x86_64.whl"
    current_linux_wheels = f"lakebench-{version}-py3-none-*manylinux*x86_64.whl"
    for existing_wheel in output_dir.glob(current_linux_wheels):
        existing_wheel.unlink()

    wheelhouse = project_root / ".cibuildwheel-build-requirements"
    shutil.rmtree(wheelhouse, ignore_errors=True)
    wheelhouse.mkdir()
    try:
        subprocess.run(
            [
                uv,
                "pip",
                "install",
                "--target",
                str(wheelhouse),
                "--no-compile",
                *configuration["build-system"]["requires"],
            ],
            cwd=project_root,
            check=True,
        )

        environment = os.environ.copy()
        environment["CIBW_ENVIRONMENT"] = "PYTHONPATH=/project/.cibuildwheel-build-requirements"
        environment["CIBW_BUILD_FRONTEND"] = "build; args: --no-isolation"
        environment["CIBW_REPAIR_WHEEL_COMMAND_LINUX"] = ""
        subprocess.run(
            [
                sys.executable,
                "-m",
                "cibuildwheel",
                "--only",
                BUILD_IDENTIFIER,
                "--output-dir",
                str(output_dir),
                str(project_root),
            ],
            cwd=project_root,
            env=environment,
            check=True,
        )
    finally:
        shutil.rmtree(wheelhouse, ignore_errors=True)

    wheels = sorted(output_dir.glob(wheel_pattern))
    if len(wheels) != 1:
        raise RuntimeError(f"Expected one {wheel_pattern} artifact, found {len(wheels)}.")

    wheel = wheels[0]
    print(f"Built {wheel}")
    print(f"SHA-256: {_sha256(wheel)}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1) from error
