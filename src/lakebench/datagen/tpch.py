from ._tpcgen import TpcgenDataGenerator


class TPCHDataGenerator(TpcgenDataGenerator):
    """Generate TPC-H data with ``tpcgen-cli`` (``pip install lakebench[tpch_datagen]``)."""

    BENCHMARK = "tpch"
    NATIVE_FORMAT = "tbl"
    ROW_GROUP_MIB = 320
    # Measured with scripts/measure_tpcgen_sizes.py.
    SF10_SIZE_MIB = {
        "customer": (79.4, 233.5),
        "lineitem": (1501.7, 7415.5),
        "nation": (0.0, 0.0),
        "orders": (379.0, 1668.2),
        "part": (44.0, 232.1),
        "partsupp": (286.3, 1149.0),
        "region": (0.0, 0.0),
        "supplier": (5.6, 13.5),
    }
