import argparse
import json
import subprocess
from pathlib import Path

import pyarrow.parquet as pq

TPCDS_TABLES = [
    "call_center",
    "catalog_page",
    "catalog_returns",
    "catalog_sales",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "inventory",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "store_returns",
    "store_sales",
    "time_dim",
    "warehouse",
    "web_page",
    "web_returns",
    "web_sales",
    "web_site",
]


def parquet_files(output_dir: Path, table_name: str):
    root_file = output_dir / f"{table_name}.parquet"
    if root_file.is_file():
        return [root_file]
    return sorted((output_dir / table_name).glob("*.parquet"))


def profile_table(output_dir: Path, table_name: str, scale_factor: float) -> dict:
    files = parquet_files(output_dir, table_name)
    if not files:
        raise RuntimeError(f"No Parquet files found for {table_name}.")

    disk_bytes = 0
    parquet_compressed_bytes = 0
    parquet_uncompressed_bytes = 0
    rows = 0
    row_groups = 0
    for file_path in files:
        disk_bytes += file_path.stat().st_size
        metadata = pq.ParquetFile(file_path).metadata
        rows += metadata.num_rows
        row_groups += metadata.num_row_groups
        for row_group_index in range(metadata.num_row_groups):
            row_group = metadata.row_group(row_group_index)
            for column_index in range(row_group.num_columns):
                column = row_group.column(column_index)
                parquet_compressed_bytes += column.total_compressed_size
                parquet_uncompressed_bytes += column.total_uncompressed_size

    compression_factor = parquet_uncompressed_bytes / parquet_compressed_bytes if parquet_compressed_bytes else 1.0
    return {
        "files": len(files),
        "rows": rows,
        "row_groups": row_groups,
        "disk_bytes": disk_bytes,
        "disk_gib": disk_bytes / (1024**3),
        "sf1000_gib": disk_bytes / (1024**3) * (1000 / scale_factor),
        "parquet_compressed_bytes": parquet_compressed_bytes,
        "parquet_uncompressed_bytes": parquet_uncompressed_bytes,
        "compression_factor": compression_factor,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--executable", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--scale-factor", type=float, default=10)
    parser.add_argument("--row-group-size-mb", type=int, default=128)
    parser.add_argument("--num-threads", type=int, default=0)
    parser.add_argument("--compression", default="ZSTD(1)")
    parser.add_argument("--report", type=Path)
    parser.add_argument("--generate", action="store_true")
    args = parser.parse_args()
    if args.scale_factor != 10:
        raise ValueError("The normalization profile must be generated at SF10.")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    if args.generate:
        command = [
            str(args.executable.resolve()),
            "tpcds",
            "parquet",
            "--scale-factor",
            str(args.scale_factor),
            "--output-dir",
            str(args.output_dir.resolve()),
            "--tables",
            ",".join(TPCDS_TABLES),
            "--compat",
            "c",
            "--compression",
            args.compression,
            "--row-group-bytes",
            str(args.row_group_size_mb * 1024 * 1024),
            "--parts",
            "1",
            "--no-progress",
        ]
        if args.num_threads:
            command.extend(["--num-threads", str(args.num_threads)])
        subprocess.run(command, check=True)

    tables = {table_name: profile_table(args.output_dir, table_name, args.scale_factor) for table_name in TPCDS_TABLES}
    total_compressed = sum(table["parquet_compressed_bytes"] for table in tables.values())
    total_uncompressed = sum(table["parquet_uncompressed_bytes"] for table in tables.values())
    report = {
        "scale_factor": args.scale_factor,
        "compression": args.compression,
        "row_group_size_mb_uncompressed": args.row_group_size_mb,
        "weighted_compression_factor": total_uncompressed / total_compressed,
        "tables": tables,
        "sf1000_size_gib_dict": {table_name: round(table["sf1000_gib"], 3) for table_name, table in tables.items()},
        "compression_factor_dict": {
            table_name: round(table["compression_factor"], 3) for table_name, table in tables.items()
        },
    }
    output = json.dumps(report, indent=2)
    print(output)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(output + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
