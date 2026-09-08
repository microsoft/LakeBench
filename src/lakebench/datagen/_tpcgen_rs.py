import logging
import os
import re
import shutil
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse
from urllib.request import url2pathname

from ._tpcgen_cli import TpcgenCli

logger = logging.getLogger(__name__)


class _TpcgenRsDataGenerator:
    BENCHMARK_NAME = ""
    BENCHMARK_LABEL = ""
    COMMAND_ARGUMENTS: Tuple[str, ...] = ()
    GEN_TABLE_REGISTRY: List[str] = []
    SF1000_SIZE_GB_DICT: Dict[str, float] = {}
    ZSTD1_COMPRESSION_FACTOR_DICT: Dict[str, float] = {}
    SNAPPY_COMPRESSION_FACTOR_DICT: Dict[str, float] = {}
    TARGET_FILE_SIZE_MAP = [
        (10, 128),
        (1024, 256),
        (5120, 512),
        (10240, 1024),
    ]
    ROW_GROUP_ESTIMATION_HEADROOM = 1.05
    CLOUD_SCHEMES = {"s3", "gs", "gcs", "abfs", "abfss", "adl", "wasb", "wasbs"}
    COMPRESSION_PATTERN = re.compile(
        r"^(UNCOMPRESSED|SNAPPY|LZO|LZ4|LZ4_RAW|GZIP(?:\(\d+\))?|BROTLI(?:\(\d+\))?|ZSTD\(\d+\))$"
    )

    def __init__(
        self,
        scale_factor: float,
        target_folder_uri: str,
        target_row_group_size_mb: int = 128,
        compression: str = "ZSTD(1)",
        table_list: Optional[List[str]] = None,
        multithreading: bool = True,
        compression_factor: Optional[float] = None,
    ) -> None:
        if not self.BENCHMARK_NAME or not self.BENCHMARK_LABEL:
            raise TypeError("_TpcgenRsDataGenerator must be subclassed with benchmark metadata.")

        parsed_uri = urlparse(target_folder_uri)
        uri_scheme = parsed_uri.scheme.lower()
        if uri_scheme in self.CLOUD_SCHEMES:
            raise ValueError(
                f"{uri_scheme} protocol is not supported for {self.BENCHMARK_LABEL} Rust data generation. "
                "Use a local filesystem path or a mounted storage path."
            )
        if scale_factor <= 0:
            raise ValueError("scale_factor must be greater than zero.")
        if target_row_group_size_mb <= 0:
            raise ValueError("target_row_group_size_mb must be greater than zero.")

        compression = compression.upper()
        if not self.COMPRESSION_PATTERN.fullmatch(compression):
            raise ValueError(f"Unsupported compression codec: {compression}")
        if compression_factor is not None and compression_factor < 1:
            raise ValueError("compression_factor must be greater than or equal to one.")

        requested_tables = table_list if table_list is not None else self.GEN_TABLE_REGISTRY
        unknown_tables = sorted(set(requested_tables) - set(self.GEN_TABLE_REGISTRY))
        if unknown_tables:
            raise ValueError(f"Unsupported {self.BENCHMARK_LABEL} tables: {', '.join(unknown_tables)}")
        if not requested_tables:
            raise ValueError("table_list must contain at least one table.")

        target_path = (
            Path(url2pathname(f"//{parsed_uri.netloc}{parsed_uri.path}"))
            if uri_scheme == "file"
            else Path(target_folder_uri)
        )
        self.scale_factor = scale_factor
        self.target_folder = target_path.expanduser().resolve()
        self.target_row_group_size_mb = target_row_group_size_mb
        self.compression = compression
        self.table_list = list(dict.fromkeys(requested_tables))
        self.compression_factor = compression_factor
        self.compression_factors_by_table = {
            table_name: self._resolve_compression_factor(table_name, compression_factor)
            for table_name in self.table_list
        }
        self.num_threads = (os.cpu_count() or 1) if multithreading else 1
        self.cli = TpcgenCli()
        self.parts_by_table = {table_name: self._calculate_optimal_parts(table_name) for table_name in self.table_list}

    def run(self) -> None:
        self._prepare_target()
        for (part_count, compression_factor), table_names in self._group_tables_by_generation_settings().items():
            logger.info(
                "Generating %s tables %s with %d part(s), %.2f MB on-disk row groups, and a %.3f compression factor.",
                self.BENCHMARK_LABEL,
                ", ".join(table_names),
                part_count,
                self.target_row_group_size_mb,
                compression_factor,
            )
            result = self.cli.run(self._build_command(self.target_folder, table_names, part_count, compression_factor))
            if result.stdout:
                logger.info(result.stdout.rstrip())
            if result.stderr:
                logger.info(result.stderr.rstrip())

        self._normalize_outputs()
        missing_outputs = [str(path) for path in self._expected_outputs() if not path.is_file()]
        if missing_outputs:
            raise RuntimeError("tpcgen-cli did not create expected files: " + ", ".join(missing_outputs))

    def _prepare_target(self) -> None:
        self.target_folder.mkdir(parents=True, exist_ok=True)
        for table_name in self.table_list:
            root_file = self.target_folder / f"{table_name}.parquet"
            if root_file.exists():
                root_file.unlink()
            table_folder = self.target_folder / table_name
            if table_folder.exists():
                shutil.rmtree(table_folder)

    def _normalize_outputs(self) -> None:
        for table_name in self.table_list:
            part_count = self.parts_by_table[table_name]
            table_folder = self.target_folder / table_name
            table_folder.mkdir(parents=True, exist_ok=True)
            for part_number in range(1, part_count + 1):
                source_candidates = [table_folder / f"{table_name}.{part_number}.parquet"]
                if part_count == 1:
                    source_candidates.insert(0, self.target_folder / f"{table_name}.parquet")
                source_file = next((path for path in source_candidates if path.is_file()), source_candidates[0])
                if source_file.is_file():
                    shutil.move(
                        str(source_file),
                        str(table_folder / self._output_file_name(table_name, part_number)),
                    )

    def _expected_outputs(self) -> List[Path]:
        outputs = []
        for table_name in self.table_list:
            part_count = self.parts_by_table[table_name]
            table_folder = self.target_folder / table_name
            outputs.extend(
                table_folder / self._output_file_name(table_name, part_number)
                for part_number in range(1, part_count + 1)
            )
        return outputs

    def _output_file_name(self, table_name: str, part_number: int) -> str:
        compression_name = self.compression.partition("(")[0].lower()
        return f"{table_name}-{part_number:05d}.{compression_name}.parquet"

    def _group_tables_by_generation_settings(self) -> Dict[Tuple[int, float], List[str]]:
        grouped_tables = defaultdict(list)
        for table_name in self.table_list:
            settings = (
                self.parts_by_table[table_name],
                self.compression_factors_by_table[table_name],
            )
            grouped_tables[settings].append(table_name)
        return dict(grouped_tables)

    def _build_command(
        self,
        output_folder: Path,
        table_names: List[str],
        part_count: int,
        compression_factor: Optional[float] = None,
    ) -> List[str]:
        if compression_factor is None:
            factors = {self.compression_factors_by_table[table_name] for table_name in table_names}
            if len(factors) != 1:
                raise ValueError("Tables with different compression factors must be generated separately.")
            compression_factor = factors.pop()

        return [
            self.BENCHMARK_NAME,
            "parquet",
            "--scale-factor",
            str(self.scale_factor),
            "--output-dir",
            str(output_folder),
            "--tables",
            ",".join(table_names),
            *self.COMMAND_ARGUMENTS,
            "--compression",
            self.compression,
            "--row-group-bytes",
            str(
                round(
                    self.target_row_group_size_mb
                    * compression_factor
                    * self.ROW_GROUP_ESTIMATION_HEADROOM
                    * 1024
                    * 1024
                )
            ),
            "--num-threads",
            str(self.num_threads),
            "--parts",
            str(part_count),
            "--no-progress",
        ]

    def _calculate_optimal_parts(self, table_name: str) -> int:
        scaled_size_gib = self._estimated_table_size_gib(table_name)
        target_file_size_mb = self._target_file_size_mb(scaled_size_gib)
        return max(round(scaled_size_gib * 1024 / target_file_size_mb), 1)

    def _estimated_table_size_gib(self, table_name: str) -> float:
        zstd_size_gib = self.SF1000_SIZE_GB_DICT[table_name] * (self.scale_factor / 1000)
        return (
            zstd_size_gib
            * self.ZSTD1_COMPRESSION_FACTOR_DICT[table_name]
            / self.compression_factors_by_table[table_name]
        )

    def _target_file_size_mb(self, scaled_size_gib: float) -> int:
        for threshold_gib, target_size_mb in self.TARGET_FILE_SIZE_MAP:
            if scaled_size_gib < threshold_gib:
                return target_size_mb
        return 1024

    def _resolve_compression_factor(self, table_name: str, compression_factor: Optional[float]) -> float:
        if compression_factor is not None:
            return compression_factor
        if self.compression == "UNCOMPRESSED":
            return 1.0
        if self.compression.startswith("ZSTD("):
            return self.ZSTD1_COMPRESSION_FACTOR_DICT[table_name]
        if self.compression == "SNAPPY":
            return self.SNAPPY_COMPRESSION_FACTOR_DICT[table_name]
        raise ValueError(
            f"compression_factor is required for {self.compression}; "
            "built-in measured defaults are available only for ZSTD(N) and SNAPPY."
        )
