from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Tuple

QUERY_NUMBERS = tuple(range(1, 23))


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def windows_to_wsl(path: Path) -> str:
    resolved = path.resolve()
    drive = resolved.drive.rstrip(":").lower()
    if not drive:
        raise ValueError(f"Cannot convert path to WSL: {resolved}")
    relative = resolved.as_posix().split(":", 1)[1]
    return f"/mnt/{drive}{relative}"


def run_command(
    command: List[str],
    *,
    environment: Optional[Dict[str, str]] = None,
) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(
            command,
            capture_output=True,
            check=True,
            env=environment,
        )
    except subprocess.CalledProcessError as error:
        details = [
            f"Command failed with exit code {error.returncode}: {' '.join(command)}"
        ]
        if error.stdout:
            details.append(f"stdout:\n{error.stdout.decode(errors='replace').rstrip()}")
        if error.stderr:
            details.append(f"stderr:\n{error.stderr.decode(errors='replace').rstrip()}")
        raise RuntimeError("\n".join(details)) from error


def qgen_command(
    qgen: Path,
    dbgen_directory: Path,
    scale_factor: int,
    rng_seed: int,
    use_wsl: bool,
    query_number: Optional[int] = None,
) -> Tuple[List[str], Optional[Dict[str, str]]]:
    arguments = [
        "-a",
        "-s",
        str(scale_factor),
        "-r",
        str(rng_seed),
    ]
    if query_number is None:
        arguments.extend(["-p", "0"])
    else:
        arguments.append(str(query_number))

    if use_wsl:
        return (
            [
                "wsl.exe",
                "env",
                f"DSS_QUERY={windows_to_wsl(dbgen_directory / 'queries')}",
                f"DSS_CONFIG={windows_to_wsl(dbgen_directory)}",
                "DSS_DIST=dists.dss",
                windows_to_wsl(qgen),
                *arguments,
            ],
            None,
        )

    environment = os.environ.copy()
    environment.update(
        {
            "DSS_QUERY": str(dbgen_directory / "queries"),
            "DSS_CONFIG": str(dbgen_directory),
            "DSS_DIST": "dists.dss",
        }
    )
    return [str(qgen), *arguments], environment


def run_qgen(
    qgen: Path,
    dbgen_directory: Path,
    scale_factor: int,
    rng_seed: int,
    use_wsl: bool,
    query_number: Optional[int] = None,
) -> bytes:
    command, environment = qgen_command(
        qgen,
        dbgen_directory,
        scale_factor,
        rng_seed,
        use_wsl,
        query_number,
    )
    return run_command(command, environment=environment).stdout


def verify_version(qgen: Path, use_wsl: bool) -> str:
    executable = windows_to_wsl(qgen) if use_wsl else str(qgen)
    command = ["wsl.exe", executable, "-h"] if use_wsl else [executable, "-h"]
    result = run_command(command)
    output = (result.stdout + result.stderr).decode(errors="replace")
    if "v. 3.0.0" not in output:
        raise RuntimeError(f"Expected qgen 3.0.0, received:\n{output.strip()}")
    return output.splitlines()[0]


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate exact ANSI TPC-H qgen queries for LakeBench."
    )
    parser.add_argument("--kit-path", type=Path, required=True)
    parser.add_argument("--qgen", type=Path, required=True)
    parser.add_argument("--scale-factor", type=int, required=True)
    parser.add_argument("--rng-seed", type=int, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--wsl",
        action="store_true",
        help="Invoke a Linux qgen executable through WSL.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    kit_path = args.kit_path.resolve()
    dbgen_directory = kit_path / "dbgen"
    qgen = args.qgen.resolve()
    output_directory = args.output_dir.resolve()
    query_directory = dbgen_directory / "queries"
    distributions = dbgen_directory / "dists.dss"

    if output_directory.exists():
        raise FileExistsError(f"Output directory already exists: {output_directory}")
    if not qgen.is_file():
        raise FileNotFoundError(f"qgen executable not found: {qgen}")
    if not query_directory.is_dir():
        raise FileNotFoundError(f"TPC-H query directory not found: {query_directory}")
    if not distributions.is_file():
        raise FileNotFoundError(f"TPC-H distributions not found: {distributions}")

    release = verify_version(qgen, args.wsl)
    output_directory.mkdir(parents=True)
    ansi_directory = output_directory / "ansi"
    ansi_directory.mkdir()

    stream_path = output_directory / "query_0.sql"
    stream_path.write_bytes(
        run_qgen(
            qgen,
            dbgen_directory,
            args.scale_factor,
            args.rng_seed,
            args.wsl,
        )
    )

    queries = []
    for query_number in QUERY_NUMBERS:
        query_name = f"q{query_number}"
        query_path = ansi_directory / f"{query_name}.sql"
        query_path.write_bytes(
            run_qgen(
                qgen,
                dbgen_directory,
                args.scale_factor,
                args.rng_seed,
                args.wsl,
                query_number,
            )
        )
        template_path = query_directory / f"{query_number}.sql"
        queries.append(
            {
                "query": query_name,
                "ansi_sha256": sha256(query_path),
                "template_sha256": sha256(template_path),
            }
        )

    manifest = {
        "tpch_specification_version": "3.0.1",
        "qgen_version": "3.0.0",
        "release_output": release,
        "scale_factor": args.scale_factor,
        "rng_seed": args.rng_seed,
        "stream": 0,
        "ansi_mode": True,
        "qgen_sha256": sha256(qgen),
        "distributions_sha256": sha256(distributions),
        "query_stream_sha256": sha256(stream_path),
        "query_count": len(queries),
        "queries": queries,
    }
    (output_directory / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    print(f"Generated 22 ANSI queries in {output_directory}")


if __name__ == "__main__":
    main()
