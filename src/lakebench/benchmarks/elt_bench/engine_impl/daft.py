from ....engines.daft import Daft
from ....engines.delta_rs import DeltaRs


class DaftELTBench:
    def __init__(self, engine: Daft):
        self.engine = engine

        import numpy as np

        self.np = np
        self.delta_rs = DeltaRs()
        self.DeltaTable = self.delta_rs.DeltaTable

    def _table_path(self, table_name: str) -> str:
        """Daft-compatible path (or remote URI) for *table_name*."""
        return self.engine.table_path(table_name)

    def _read_delta(self, table_name: str):
        """Read a Delta table via Daft."""
        return self.engine.read_delta(self._table_path(table_name))

    def _write_delta(self, df, table_name: str, mode: str = "overwrite"):
        """Write *df* as a Delta table."""
        self.engine.write_delta(df, self._table_path(table_name), mode=mode)

    # ------------------------------------------------------------------

    def create_total_sales_fact(self):
        fact_table_df = (
            self._read_delta("store_sales")
            .join(self._read_delta("date_dim"), left_on="ss_sold_date_sk", right_on="d_date_sk")
            .join(self._read_delta("store"), left_on="ss_store_sk", right_on="s_store_sk")
            .join(self._read_delta("item"), left_on="ss_item_sk", right_on="i_item_sk")
            .join(self._read_delta("customer"), left_on="ss_customer_sk", right_on="c_customer_sk")
            .with_columns({"sale_date": self.engine.daft.col("d_date")})
            .where(self.engine.daft.col("d_year") == 2001)
            .groupby(["s_store_id", "i_item_id", "c_customer_id", "sale_date"])
            .agg(
                [
                    self.engine.daft.col("ss_quantity").sum().alias("total_quantity"),
                    self.engine.daft.col("ss_net_paid")
                    .sum()
                    .cast(self.engine.daft.DataType.decimal128(38, 2))
                    .alias("total_net_paid"),
                    self.engine.daft.col("ss_net_profit")
                    .sum()
                    .cast(self.engine.daft.DataType.decimal128(38, 2))
                    .alias("total_net_profit"),
                ]
            )
            .sort(["s_store_id", "sale_date"])
        )
        self._write_delta(fact_table_df, "total_sales_fact")

    def merge_percent_into_total_sales_fact(self, percent: float):
        seed = self.np.random.randint(1, high=1000, size=None, dtype=int)
        modulo = int(1 / percent)

        daft = self.engine.daft

        sampled_fact_data = (
            self._read_delta("store_sales")
            .join(self._read_delta("date_dim"), left_on="ss_sold_date_sk", right_on="d_date_sk")
            .join(self._read_delta("store"), left_on="ss_store_sk", right_on="s_store_sk")
            .join(self._read_delta("item"), left_on="ss_item_sk", right_on="i_item_sk")
            .join(self._read_delta("customer"), left_on="ss_customer_sk", right_on="c_customer_sk")
            .with_columns(
                {
                    "new_uid_val": (daft.col("ss_customer_sk") + daft.col("ss_sold_date_sk") + seed),
                    "s_store_id": daft.col("s_store_id"),
                    "i_item_id": daft.col("i_item_id"),
                    "sale_date": daft.col("d_date"),
                }
            )
            .filter((daft.col("new_uid_val") % modulo) == 0)
            .with_columns(
                {
                    "c_customer_id": daft.functions.when(
                        daft.col("new_uid_val") % 2 == 0, daft.col("c_customer_id")
                    ).otherwise(daft.lit("NEW_") + daft.col("new_uid_val").cast(daft.DataType.string())),
                    "total_quantity": daft.col("ss_quantity") + (daft.col("new_uid_val") % 5 + 1),
                    "total_net_paid": (daft.col("ss_net_paid") + ((daft.col("new_uid_val") % 5000) / 100.0 + 5)).cast(
                        daft.DataType.decimal128(38, 2)
                    ),
                    "total_net_profit": (
                        daft.col("ss_net_profit") + ((daft.col("new_uid_val") % 2000) / 100.0 + 1)
                    ).cast(daft.DataType.decimal128(38, 2)),
                }
            )
            .select(
                "s_store_id",
                "i_item_id",
                "c_customer_id",
                "sale_date",
                "total_quantity",
                "total_net_paid",
                "total_net_profit",
            )
            .to_arrow()
        )

        fact_table = self.DeltaTable(
            table_uri=self._table_path("total_sales_fact"),
            storage_options=self.engine.storage_options,
        )
        fact_table.merge(
            source=sampled_fact_data,
            predicate="""
                target.s_store_id    = source.s_store_id    AND
                target.i_item_id     = source.i_item_id     AND
                target.c_customer_id = source.c_customer_id AND
                target.sale_date     = source.sale_date
            """,
            source_alias="source",
            target_alias="target",
        ).when_matched_update(
            {
                "total_quantity": "target.total_quantity   + source.total_quantity",
                "total_net_paid": "target.total_net_paid   + source.total_net_paid",
                "total_net_profit": "target.total_net_profit + source.total_net_profit",
            }
        ).when_not_matched_insert(
            {
                "s_store_id": "source.s_store_id",
                "i_item_id": "source.i_item_id",
                "c_customer_id": "source.c_customer_id",
                "sale_date": "source.sale_date",
                "total_quantity": "source.total_quantity",
                "total_net_paid": "source.total_net_paid",
                "total_net_profit": "source.total_net_profit",
            }
        ).execute()

    def query_total_sales_fact(self):
        (
            self._read_delta("total_sales_fact")
            .groupby(self.engine.daft.col("sale_date").year())
            .agg(self.engine.daft.col("total_net_profit").sum().alias("sum_net_profit"))
            .collect()
        )
