"""Measure the SF10 table sizes in lakebench.datagen (SF10_SIZE_MIB).

Usage: python scripts/measure_tpcgen_sizes.py <empty work directory>
"""

import shutil
import subprocess
import sys
from pathlib import Path

from lakebench.datagen import TPCDSDataGenerator, TPCHDataGenerator


def main() -> None:
    work = Path(sys.argv[1])
    executable = shutil.which("tpcgen-cli")
    for generator in (TPCHDataGenerator, TPCDSDataGenerator):
        benchmark = generator.BENCHMARK
        sizes = {}
        for output in ("parquet", generator.NATIVE_FORMAT):
            out = work / benchmark / output
            command = [executable, benchmark, output, "--scale-factor", "10", "--output-dir", str(out), "--quiet"]
            if output == "parquet":
                command += ["--compression", "ZSTD(1)", "--row-group-bytes", f"{generator.ROW_GROUP_MIB}MiB"]
            subprocess.run(command, check=True)
            for file in out.iterdir():
                sizes.setdefault(file.stem, []).append(round(file.stat().st_size / 2**20, 1))
        print(f"{benchmark} SF10_SIZE_MIB = {{")
        for table, (parquet_mib, native_mib) in sorted(sizes.items()):
            print(f'    "{table}": ({parquet_mib}, {native_mib}),')
        print("}")


if __name__ == "__main__":
    main()
