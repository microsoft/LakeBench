from typing import List, Optional

from ._tpc import _TPCDataGenerator
from ._tpcds_rs import _TPCDSRsDataGenerator


class _TPCDSDuckDBDataGenerator(_TPCDataGenerator):
    GEN_UTIL = "dsdgen"
    GEN_TYPE = "tpcds"


class TPCDSDataGenerator:
    """
    Generate TPC-DS data with the bundled Rust tpcgen-cli executable or the
    legacy DuckDB generator.

    Parameters
    ----------
    scale_factor : float
        The scale factor for the data generation, which determines the size of the generated dataset.
    target_folder_uri : str
        The folder path where the generated Parquet data will be stored. A folder for each table will be created.
    target_row_group_size_mb : int, default=128
        Target on-disk size of Parquet row groups in megabytes. The Rust
        generator scales the upstream uncompressed-byte target by the
        compression factor.
    compression : str, default="ZSTD(1)"
        Parquet compression used by the Rust backend.
    table_list : list of str, optional
        TPC-DS tables to generate. The Rust backend generates all 24 tables by default.
    multithreading : bool, default=True
        Whether the Rust generator should use all available CPU cores. When
        false, generation uses one thread.
    backend : {"rust", "duckdb"}, default="rust"
        Data generator backend. DuckDB is retained as an explicit legacy fallback.
    executable : str, optional
        Development override for the bundled ``tpcgen-cli`` executable.
    compression_factor : float, optional
        Ratio of uncompressed to on-disk Parquet bytes used to translate the
        requested row-group target and estimate physical table size. Measured
        per-table defaults are used for ``ZSTD(1)`` and ``SNAPPY``; other
        compressed codecs require an explicit value.

    Methods
    -------
    run()
        Generates TPC-DS data in Parquet format based on the input scale factor and writes it to the target folder.
    """

    def __init__(
        self,
        scale_factor: float,
        target_folder_uri: str,
        target_row_group_size_mb: int = 128,
        compression: str = "ZSTD(1)",
        table_list: Optional[List[str]] = None,
        multithreading: bool = True,
        backend: str = "rust",
        executable: Optional[str] = None,
        compression_factor: Optional[float] = None,
    ) -> None:
        self.scale_factor = scale_factor
        self.target_folder_uri = target_folder_uri
        self.target_row_group_size_mb = target_row_group_size_mb
        self.backend = backend

        if backend == "rust":
            self._generator = _TPCDSRsDataGenerator(
                scale_factor=scale_factor,
                target_folder_uri=target_folder_uri,
                target_row_group_size_mb=target_row_group_size_mb,
                compression=compression,
                table_list=table_list,
                multithreading=multithreading,
                executable=executable,
                compression_factor=compression_factor,
            )
        elif backend == "duckdb":
            rust_options = {
                "compression": compression if compression != "ZSTD(1)" else None,
                "table_list": table_list,
                "multithreading": False if not multithreading else None,
                "executable": executable,
                "compression_factor": compression_factor,
            }
            specified_options = [name for name, value in rust_options.items() if value is not None]
            if specified_options:
                raise ValueError(
                    "The following options are supported only by backend='rust': " + ", ".join(specified_options)
                )
            self._generator = _TPCDSDuckDBDataGenerator(
                scale_factor=scale_factor,
                target_folder_uri=target_folder_uri,
                target_row_group_size_mb=target_row_group_size_mb,
            )
        else:
            raise ValueError("backend must be either 'rust' or 'duckdb'.")

    def run(self) -> None:
        self._generator.run()
