import logging
import posixpath
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

import fsspec
from fsspec import AbstractFileSystem

from lakebench.utils.path_utils import to_unix_path

logger = logging.getLogger(__name__)


class _TPCRsDataGenerator:
    """
    Base class for TPC Rust based data generation. PLEASE DO NOT INSTANTIATE THIS CLASS DIRECTLY. Use the TPCHDataGenerator and TPCDSDataGenerator
    subclasses instead.
    """

    GEN_UTIL = ""
    GEN_TYPE = "tpch"
    GEN_TABLE_REGISTRY = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
    TARGET_FILE_SIZE_MAP = [
        (10, 128),  # up to 10GB -> 128MB files
        (1024, 256),  # up to 1TB -> 256MB files
        (5120, 512),  # up to 5TB -> 512MB files
        (10240, 1024),  # up to 10TB and larger -> 1GB files
    ]
    SF1000_SIZE_GB_DICT = {
        "lineitem": 152,
        "orders": 38,
        "partsupp": 26.7,
        "part": 4,
        "customer": 7.6,
        "supplier": 0.48,
        "region": 0.00,
        "nation": 0.00,
    }

    # Class-level lock for thread-safe printing
    _print_lock = threading.Lock()

    def __init__(
        self,
        scale_factor: int,
        target_folder_uri: str,
        target_row_group_size_mb: int = 128,
        compression: str = "ZSTD(1)",
        table_list: list = None,
        multithreading: bool = True,
    ) -> None:
        """
        Initialize the TPC data generator with a scale factor.

        Parameters
        ----------
        scale_factor: int
            The scale factor for the data generation.
        target_folder_uri: str
            Test data will be written to this location where tables are represented as folders containing parquet files.
        target_row_group_size_mb: int, default=128
            Desired row group size for the generated parquet files.
        compression: str, default="ZSTD(1)"
            Compression codec to use for the generated parquet files.
            Supports codecs: "UNCOMPRESSED", "SNAPPY", "GZIP(compression_level)", "BROTLI(compression_level)", "LZ4", "LZ4_RAW", "LZO", "ZSTD(compression_level)"
        """
        self.scale_factor = scale_factor
        uri_scheme = urlparse(target_folder_uri).scheme

        # Allow local file systems: no scheme, file://, or Windows drive letters
        cloud_schemes = {"s3", "gs", "gcs", "abfs", "abfss", "adl", "wasb", "wasbs"}

        if uri_scheme in cloud_schemes:
            raise ValueError(
                f"{uri_scheme} protocol is not currently supported for TPC-RS data generation. Please use a local file system path or mount the storage location."
            )

        if compression.split("(")[0] not in [
            "UNCOMPRESSED",
            "SNAPPY",
            "GZIP",
            "BROTLI",
            "LZ4",
            "LZ4_RAW",
            "LZO",
            "ZSTD",
        ]:
            raise ValueError(f"Unsupported compression codec: {compression}")

        self.fs: AbstractFileSystem = fsspec.filesystem("file")
        self.target_folder_uri = to_unix_path(target_folder_uri)
        self.target_row_group_size_mb = int(
            target_row_group_size_mb * 2.6
        )  # 2.6 for uncompressed-> ZSTD(1) compression ratio
        self.compression = compression
        self.table_list = table_list
        self.multithreading = multithreading

        def get_tpcgen_path():
            import shutil

            # Try shutil.which first (most reliable)
            path = shutil.which(f"{self.GEN_TYPE}gen-cli")
            if path:
                return path

            # Fallback to user Scripts directory
            import sys
            from pathlib import Path

            user_scripts = (
                Path.home()
                / "AppData"
                / "Roaming"
                / "Python"
                / f"Python{sys.version_info.major}{sys.version_info.minor}"
                / "Scripts"
                / "tpchgen-cli.exe"
            )
            if user_scripts.exists():
                return str(user_scripts)

            raise ImportError(
                f"{self.GEN_TYPE}gen-cli is used for data generation but is not installed. Install using `%pip install {self.GEN_TYPE}gen-cli`"
            )

        self.tpcgen_exe = get_tpcgen_path()

    def run(self) -> None:
        """
        This method uses multithreading to generate individual tables in parallel using
        a rust-based TPC data generation utility. Each table is generated with an optimal
        number of parts (based on the GEN_SF1000_FILE_COUNT_MAP) to target having files around 1GB.
        """

        # cleanup target directory
        def clean_dir(path: str) -> None:
            if self.fs.exists(path):
                self.fs.rm(path, recursive=True)
            self.fs.mkdirs(path, exist_ok=True)

        if self.table_list is None:
            clean_dir(self.target_folder_uri)
        else:
            for table_name in self.table_list:
                table_path = posixpath.join(self.target_folder_uri, table_name)
                clean_dir(table_path)

        if self.table_list is None:
            tables = self.GEN_TABLE_REGISTRY
        else:
            tables = [table for table in self.GEN_TABLE_REGISTRY if table in self.table_list]

        logger.info("🚀 Starting parallel generation of %d tables with multithreading...", len(tables))
        logger.info("📊 Scale Factor: %s", self.scale_factor)
        logger.info("📁 Output Directory: %s", self.target_folder_uri)

        completed_tables = []
        failed_tables = []

        if self.multithreading:
            with ThreadPoolExecutor() as executor:
                future_to_table = {
                    executor.submit(self._generate_table, table_name): table_name for table_name in tables
                }

                for future in as_completed(future_to_table):
                    table_name = future_to_table[future]
                    try:
                        result = future.result()
                        if result:
                            completed_tables.append(table_name)
                            logger.info("✅ %s - Generation completed successfully", table_name)
                        else:
                            failed_tables.append(table_name)
                            logger.error("❌ %s - Generation failed", table_name)
                    except Exception as exc:
                        failed_tables.append(table_name)
                        logger.error("❌ %s - Generation failed with exception: %s", table_name, exc)
        else:
            for table_name in tables:
                result = self._generate_table(table_name)
                if result:
                    completed_tables.append(table_name)
                    logger.info("✅ %s - Generation completed successfully", table_name)
                else:
                    failed_tables.append(table_name)
                    logger.error("❌ %s - Generation failed", table_name)

        logger.info("📋 Generation Summary:")
        logger.info("   ✅ Successfully generated: %d tables", len(completed_tables))
        if completed_tables:
            logger.info("      Tables: %s", ", ".join(completed_tables))

        if failed_tables:
            logger.error("   ❌ Failed to generate: %d tables", len(failed_tables))
            logger.error("      Tables: %s", ", ".join(failed_tables))
            raise RuntimeError(f"Failed to generate {len(failed_tables)} tables: {', '.join(failed_tables)}")
        else:
            logger.info("🎉 All %d tables generated successfully!", len(tables))

    def _generate_table(self, table_name: str) -> bool:
        """
        Generate a single table using the optimal number of parts.

        Parameters
        ----------
        table_name: str
            Name of the table to generate

        Returns
        -------
        bool
            True if generation was successful, False otherwise
        """

        def find_target_size(size: float) -> int:
            for threshold_gb, target_mb in self.TARGET_FILE_SIZE_MAP:
                if size < threshold_gb:
                    return target_mb
            return 1024

        # Scale the parts based on the scale factor and size of the table
        sf1000_size_gb = self.SF1000_SIZE_GB_DICT.get(table_name, 0)
        scale_adj_size_gb = sf1000_size_gb * (self.scale_factor / 1000.0)
        target_size_mb = find_target_size(scale_adj_size_gb)
        optimal_parts = max(round(scale_adj_size_gb * 1024 / target_size_mb), 1)

        logger.info("🔧 %s - Using %d parts (target file size: %dmb)", table_name, optimal_parts, target_size_mb)

        # ensure that 128mb target files have a single row group
        adj_row_group_target_mb = 1024 if target_size_mb == 128 else self.target_row_group_size_mb
        # Build command for individual table generation
        cmd = [
            self.tpcgen_exe,
            "--scale-factor",
            str(self.scale_factor),
            "--output-dir",
            self.target_folder_uri,
            "--parts",
            str(optimal_parts),
            "--format",
            "parquet",
            "--parquet-row-group-bytes",
            str(adj_row_group_target_mb * 1024 * 1024),
            "--parquet-compression",
            self.compression,
            "--tables",
            table_name,
        ]

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            if result.stdout:
                with self._print_lock:
                    logger.info("📝 %s output:", table_name)
                    for line in result.stdout.strip().split("\n"):
                        if line.strip():
                            logger.info("   %s", line)
            return True

        except subprocess.CalledProcessError as e:
            with self._print_lock:
                logger.error("❌ %s failed:", table_name)
                if e.stdout:
                    logger.error("   stdout: %s", e.stdout)
                if e.stderr:
                    logger.error("   stderr: %s", e.stderr)
            return False
        except Exception as e:
            with self._print_lock:
                logger.error("❌ %s failed with exception: %s", table_name, e)
            return False
