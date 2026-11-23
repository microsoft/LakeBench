from bisect import bisect_right

from ._tpcgen import TpcgenDataGenerator


class TPCDSDataGenerator(TpcgenDataGenerator):
    """Generate TPC-DS data with ``tpcgen-cli`` (``pip install lakebench[tpcds_datagen]``)."""

    BENCHMARK = "tpcds"
    NATIVE_FORMAT = "dat"
    ROW_GROUP_MIB = 176
    # Measured with scripts/measure_tpcgen_sizes.py.
    SF10_SIZE_MIB = {
        "call_center": (0.0, 0.0),
        "catalog_page": (0.5, 1.6),
        "catalog_returns": (76.8, 211.4),
        "catalog_sales": (786.6, 2897.2),
        "customer": (20.0, 63.8),
        "customer_address": (3.8, 26.4),
        "customer_demographics": (5.5, 76.9),
        "date_dim": (1.6, 9.8),
        "household_demographics": (0.0, 0.1),
        "income_band": (0.0, 0.0),
        "inventory": (418.6, 2635.0),
        "item": (6.5, 27.5),
        "promotion": (0.0, 0.1),
        "reason": (0.0, 0.0),
        "ship_mode": (0.0, 0.0),
        "store": (0.0, 0.0),
        "store_returns": (111.7, 322.9),
        "store_sales": (962.5, 3813.7),
        "time_dim": (0.9, 4.9),
        "warehouse": (0.0, 0.0),
        "web_page": (0.0, 0.0),
        "web_returns": (40.7, 97.4),
        "web_sales": (358.2, 1441.4),
        "web_site": (0.0, 0.0),
    }
    #: Row counts, in millions, at the TPC-DS defined scale factors for the tables that grow
    #: sublinearly and can exceed one million rows. Other tables grow linearly, or stay under
    #: one million rows, which tpcgen-cli never splits.
    DEFINED_SCALE_FACTORS = (1, 10, 100, 300, 1000, 3000, 10000, 30000, 100000)
    SUBLINEAR_ROWS_M = {
        "customer": (0.1, 0.5, 2, 5, 12, 30, 65, 80, 100),
        "customer_address": (0.05, 0.25, 1, 2.5, 6, 15, 32.5, 40, 50),
        "customer_demographics": (1.9208,) * 9,
        "inventory": (11.745, 133.11, 399.33, 585.684, 783, 1033.56, 1311.525, 1627.857, 1965.338),
    }

    def _growth(self, table: str) -> float:
        rows = self.SUBLINEAR_ROWS_M.get(table)
        if rows is None:
            return super()._growth(table)
        # Use the nearest defined scale factor at or below this one.
        return rows[max(bisect_right(self.DEFINED_SCALE_FACTORS, self.scale_factor) - 1, 0)] / rows[1]
