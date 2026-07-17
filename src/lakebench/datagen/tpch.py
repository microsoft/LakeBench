from ._tpc_rs import _TPCRsDataGenerator


class TPCHDataGenerator(_TPCRsDataGenerator):
    """
    This class is a multithreading wrapper of the rust-based TPC-H data generator, `tpchgen-rs`. It generates TPC-H data in Parquet format
    based on the specified scale factor, target row group size in MB, and compression codec. Each table is partitioned into multiple parts to
    target generating 1GB sized files.

    Attributes
    ----------
    scale_factor : int
        The scale factor for the data generation, which determines the size of the generated dataset.
    target_folder_uri : str, optional
        The folder path where the generated Parquet data will be stored. A folder for each table will be created.
    target_row_group_size_mb : int
        The target size of row groups in megabytes for the generated Parquet files.
    compression: str, default="ZSTD"
        Compression codec to use for the generated parquet files.
        Supports codecs: "UNCOMPRESSED", "SNAPPY", "GZIP(compression_level)", "BROTLI(compression_level)", "LZ4", "LZ4_RAW", "LZO", "ZSTD(compression_level)"

    Methods
    -------
    run()
        Generates TPC-H data in Parquet format based on the input scale factor and writes it to the target folder.
    """

    GEN_UTIL = "dbgen"
    GEN_TYPE = "tpch"
    GEN_SF1000_FILE_COUNT_MAP = {"lineitem": 150, "orders": 40, "partsupp": 26, "part": 4, "customer": 8}
    GEN_TABLE_REGISTRY = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
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
