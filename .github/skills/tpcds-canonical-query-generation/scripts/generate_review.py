from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import List, Optional

import sqlglot

VARIANT_QUERIES = {14, 23, 24, 39}
BEGIN_END_DEFINITIONS = """
define _BEGIN = "-- start query " + [_QUERY] + " in stream " + [_STREAM] + " using template " + [_TEMPLATE];
define _END = "-- end query " + [_QUERY] + " in stream " + [_STREAM] + " using template " + [_TEMPLATE];
""".strip()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def split_sql_statements(sql: str) -> List[str]:
    statements = []
    start = 0
    index = 0
    quote = None
    line_comment = False
    block_comment = False

    while index < len(sql):
        char = sql[index]
        next_char = sql[index + 1] if index + 1 < len(sql) else ""

        if line_comment:
            if char == "\n":
                line_comment = False
        elif block_comment:
            if char == "*" and next_char == "/":
                block_comment = False
                index += 1
        elif quote:
            if char == quote:
                if next_char == quote:
                    index += 1
                else:
                    quote = None
        elif char in {"'", '"'}:
            quote = char
        elif char == "-" and next_char == "-":
            line_comment = True
            index += 1
        elif char == "/" and next_char == "*":
            block_comment = True
            index += 1
        elif char == ";":
            statement = sql[start : index + 1].strip()
            if statement:
                statements.append(statement)
            start = index + 1

        index += 1

    remainder = sql[start:].strip()
    if remainder:
        statements.append(remainder)
    return statements


def normalize_for_transpilation(sql: str) -> str:
    sql = re.sub(
        r"(?is)\bselect\s+top\s+(\d+)\s+distinct\s*\(([^()]+)\)",
        r"select distinct top \1 \2",
        sql,
    )
    return re.sub(
        r"(?i)([+-])\s*(\d+)\s+days\b",
        r"\1 INTERVAL '\2' DAY",
        sql,
    )


def patch_ansi_dialect(path: Path) -> None:
    content = path.read_text(encoding="utf-8")
    if re.search(r"(?im)^\s*define\s+_END\s*=", content):
        return
    path.write_text(content.rstrip() + "\n" + BEGIN_END_DEFINITIONS + "\n", encoding="utf-8", newline="\n")


def windows_to_wsl(path: Path) -> str:
    resolved = path.resolve()
    drive = resolved.drive.rstrip(":").lower()
    if not drive:
        raise ValueError(f"Cannot convert path to WSL: {resolved}")
    relative = resolved.as_posix().split(":", 1)[1]
    return f"/mnt/{drive}{relative}"


def run_command(command: List[str], *, cwd: Optional[Path] = None) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(command, cwd=cwd, capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as error:
        details = [f"Command failed with exit code {error.returncode}: {' '.join(command)}"]
        if error.stdout:
            details.append(f"stdout:\n{error.stdout.rstrip()}")
        if error.stderr:
            details.append(f"stderr:\n{error.stderr.rstrip()}")
        raise RuntimeError("\n".join(details)) from error


def verify_version(dsqgen: Path, use_wsl: bool) -> str:
    executable = windows_to_wsl(dsqgen) if use_wsl else str(dsqgen)
    command = ["wsl.exe", executable, "-RELEASE", "Y"] if use_wsl else [executable, "-RELEASE", "Y"]
    result = run_command(command)
    output = result.stdout + result.stderr
    if "4.0.0" not in output:
        raise RuntimeError(f"Expected dsqgen 4.0.0, received:\n{output.strip()}")
    return output.strip()


def generate_stream(
    dsqgen: Path,
    distributions: Path,
    template_directory: Path,
    scale_factor: int,
    rng_seed: int,
    use_wsl: bool,
) -> bytes:
    if use_wsl:
        working_directory = f"/tmp/lakebench-tpcds-qgen-{os.getpid()}"
        output_directory = f"{working_directory}/output"
        executable = windows_to_wsl(dsqgen)
        source_distribution_index = windows_to_wsl(distributions)
        source_templates = windows_to_wsl(template_directory)
        distribution_index = f"{working_directory}/tpcds.idx"
        templates = f"{working_directory}/query_templates"
        command = [
            "wsl.exe",
            executable,
            "-DIRECTORY",
            templates,
            "-INPUT",
            f"{templates}/templates.lst",
            "-DISTRIBUTIONS",
            distribution_index,
            "-SCALE",
            str(scale_factor),
            "-RNGSEED",
            str(rng_seed),
            "-DIALECT",
            "ansi",
            "-QUALIFY",
            "Y",
            "-OUTPUT_DIR",
            output_directory,
            "-QUIET",
            "Y",
        ]
        try:
            run_command(["wsl.exe", "rm", "-rf", working_directory])
            run_command(["wsl.exe", "mkdir", "-p", output_directory])
            run_command(["wsl.exe", "cp", "-r", source_templates, templates])
            run_command(["wsl.exe", "cp", source_distribution_index, distribution_index])
            run_command(command)
            return subprocess.run(
                ["wsl.exe", "cat", f"{output_directory}/query_0.sql"],
                capture_output=True,
                check=True,
            ).stdout
        finally:
            subprocess.run(["wsl.exe", "rm", "-rf", working_directory], check=False)

    with tempfile.TemporaryDirectory(prefix="lakebench-tpcds-qgen-") as output_directory:
        command = [
            str(dsqgen),
            "-DIRECTORY",
            str(template_directory),
            "-INPUT",
            str(template_directory / "templates.lst"),
            "-DISTRIBUTIONS",
            str(distributions),
            "-SCALE",
            str(scale_factor),
            "-RNGSEED",
            str(rng_seed),
            "-DIALECT",
            "ansi",
            "-QUALIFY",
            "Y",
            "-OUTPUT_DIR",
            output_directory,
            "-QUIET",
            "Y",
        ]
        run_command(command)
        return (Path(output_directory) / "query_0.sql").read_bytes()


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a reviewable TPC-DS 4.0.0 query set.")
    parser.add_argument("--kit-path", type=Path, required=True)
    parser.add_argument("--dsqgen", type=Path, required=True)
    parser.add_argument("--scale-factor", type=int, default=1000)
    parser.add_argument("--rng-seed", type=int, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--wsl", action="store_true", help="Invoke a Linux dsqgen executable through WSL.")
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    kit_path = args.kit_path.resolve()
    dsqgen = args.dsqgen.resolve()
    output_directory = args.output_dir.resolve()
    source_templates = kit_path / "query_templates"
    distributions = dsqgen.parent / "tpcds.idx"

    if output_directory.exists():
        raise FileExistsError(f"Output directory already exists: {output_directory}")
    if not source_templates.is_dir():
        raise FileNotFoundError(f"TPC-DS query_templates directory not found: {source_templates}")
    if not (source_templates / "templates.lst").is_file():
        raise FileNotFoundError(f"TPC-DS templates.lst not found under: {source_templates}")
    if not dsqgen.is_file():
        raise FileNotFoundError(f"dsqgen executable not found: {dsqgen}")
    if not distributions.is_file():
        raise FileNotFoundError(
            f"Compiled distribution index not found beside dsqgen: {distributions}. "
            "Build the dsqgen target before running the helper."
        )

    release = verify_version(dsqgen, args.wsl)

    with tempfile.TemporaryDirectory(prefix="lakebench-tpcds-templates-") as temporary_directory:
        templates = Path(temporary_directory) / "query_templates"
        shutil.copytree(source_templates, templates)
        patch_ansi_dialect(templates / "ansi.tpl")
        stream_bytes = generate_stream(
            dsqgen=dsqgen,
            distributions=distributions,
            template_directory=templates,
            scale_factor=args.scale_factor,
            rng_seed=args.rng_seed,
            use_wsl=args.wsl,
        )

    output_directory.mkdir(parents=True)
    ansi_directory = output_directory / "ansi"
    spark_directory = output_directory / "spark-transpiled"
    ansi_directory.mkdir()
    spark_directory.mkdir()
    stream_path = output_directory / "query_0.sql"
    stream_path.write_bytes(stream_bytes)
    stream = stream_bytes.decode("utf-8")

    block_pattern = re.compile(
        r"^-- start query (?P<number>\d+) in stream 0 using template query\d+\.tpl\r?\n"
        r"(?P<body>.*?)"
        r"^-- end query (?P=number) in stream 0 using template query\d+\.tpl\s*$",
        re.MULTILINE | re.DOTALL,
    )
    blocks = list(block_pattern.finditer(stream))
    if len(blocks) != 99:
        raise RuntimeError(f"Expected 99 query blocks, found {len(blocks)}")

    queries = []
    for block in blocks:
        query_number = int(block.group("number"))
        statements = split_sql_statements(block.group("body"))
        expected_count = 2 if query_number in VARIANT_QUERIES else 1
        if len(statements) != expected_count:
            raise RuntimeError(
                f"Expected {expected_count} statement(s) for query {query_number}, found {len(statements)}"
            )

        for index, statement in enumerate(statements):
            suffix = chr(ord("a") + index) if expected_count == 2 else ""
            query_name = f"q{query_number}{suffix}"
            ansi_sql = statement.rstrip() + "\n"
            ansi_path = ansi_directory / f"{query_name}.sql"
            ansi_path.write_text(ansi_sql, encoding="utf-8", newline="\n")

            expression = sqlglot.parse_one(normalize_for_transpilation(ansi_sql), read="tsql")
            spark_sql = expression.sql(dialect="spark", pretty=True, normalize=False).rstrip() + "\n"
            sqlglot.parse_one(spark_sql, read="spark")
            spark_path = spark_directory / f"{query_name}.sql"
            spark_path.write_text(spark_sql, encoding="utf-8", newline="\n")

            queries.append(
                {
                    "query": query_name,
                    "ansi_sha256": sha256(ansi_path),
                    "spark_transpiled_sha256": sha256(spark_path),
                }
            )

    manifest = {
        "tpcds_tools_version": "4.0.0",
        "release_output": release,
        "scale_factor": args.scale_factor,
        "rng_seed": args.rng_seed,
        "qualification_mode": True,
        "dialect": "ansi",
        "dsqgen_sha256": sha256(dsqgen),
        "templates_list_sha256": sha256(source_templates / "templates.lst"),
        "query_stream_sha256": sha256(stream_path),
        "query_count": len(queries),
        "variant_queries": sorted(VARIANT_QUERIES),
        "queries": queries,
    }
    if len(queries) != 103:
        raise RuntimeError(f"Expected 103 query files, generated {len(queries)}")
    (output_directory / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    print(f"Generated 103 ANSI and 103 Spark-transpiled queries in {output_directory}")


if __name__ == "__main__":
    main()
