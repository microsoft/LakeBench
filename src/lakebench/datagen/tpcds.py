from ._tpc import _TPCDataGenerator


class TPCDSDataGenerator(_TPCDataGenerator):
    """
    This class is a wrapper for the DuckDB TPC-DS data generation utility. It generates TPC-DS data in Parquet format
    based on the specified scale factor and target row group size in MB.

    Attributes
    ----------
    scale_factor : int
        The scale factor for the data generation, which determines the size of the generated dataset.
    target_folder_uri : str, optional
        The folder path where the generated Parquet data will be stored. A folder for each table will be created.
    target_row_group_size_mb : int
        The target size of row groups in megabytes for the generated Parquet files.

    Methods
    -------
    run()
        Generates TPC-DS data in Parquet format based on the input scale factor and writes it to the target folder.
    """

    GEN_UTIL = "dsdgen"
    GEN_TYPE = "tpds"
