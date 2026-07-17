import posixpath

from ....engines.delta_rs import DeltaRs
from ....engines.polars import Polars


class PolarsELTBench:
    def __init__(self, engine: Polars):

        import numpy as np

        self.np = np
        self.delta_rs = DeltaRs()
        self.write_deltalake = self.delta_rs.write_deltalake
        self.DeltaTable = self.delta_rs.DeltaTable
        self.storage_options = engine.storage_options
        self.engine = engine

    def create_total_sales_fact(self):
        fact_table_df = (
            self.engine.pl.scan_delta(
                posixpath.join(self.engine.schema_or_working_directory_uri, "store_sales"),
                storage_options=self.storage_options,
            )
            .join(
                self.engine.pl.scan_delta(
                    posixpath.join(self.engine.schema_or_working_directory_uri, "date_dim"),
                    storage_options=self.storage_options,
                ),
                left_on="ss_sold_date_sk",
                right_on="d_date_sk",
            )
            .join(
                self.engine.pl.scan_delta(
                    posixpath.join(self.engine.schema_or_working_directory_uri, "store"),
                    storage_options=self.storage_options,
                ),
                left_on="ss_store_sk",
                right_on="s_store_sk",
            )
            .join(
                self.engine.pl.scan_delta(
                    posixpath.join(self.engine.schema_or_working_directory_uri, "item"),
                    storage_options=self.storage_options,
                ),
                left_on="ss_item_sk",
                right_on="i_item_sk",
            )
            .join(
                self.engine.pl.scan_delta(
                    posixpath.join(self.engine.schema_or_working_directory_uri, "customer"),
                    storage_options=self.storage_options,
                ),
                left_on="ss_customer_sk",
                right_on="c_customer_sk",
            )
            .with_columns(self.engine.pl.col("d_date").alias("sale_date"))
            .filter(self.engine.pl.col("d_year") == 2001)
            .group_by(["s_store_id", "i_item_id", "c_customer_id", "sale_date"])
            .agg(
                [
                    self.engine.pl.sum("ss_quantity").alias("total_quantity"),
                    self.engine.pl.sum("ss_net_paid").alias("total_net_paid"),
                    self.engine.pl.sum("ss_net_profit").alias("total_net_profit"),
                ]
            )
            .sort(["s_store_id", "sale_date"])
        )

        fact_table_df.collect(engine="streaming").write_delta(
            posixpath.join(self.engine.schema_or_working_directory_uri, "total_sales_fact"),
            mode="overwrite",
            storage_options=self.storage_options,
        )

    def merge_percent_into_total_sales_fact(self, percent: float):
        seed = self.np.random.randint(1, high=1000, size=None, dtype=int)
        modulo = int(1 / percent)
        sampled_fact_data = (
            self.engine.pl.scan_delta(
                posixpath.join(self.engine.schema_or_working_directory_uri, "store_sales"),
                storage_options=self.storage_options,
            )
            .filter(
                (
                    (self.engine.pl.col("ss_item_sk") * 1000000 + self.engine.pl.col("ss_ticket_number") + seed).hash()
                    % modulo
                )
                == 0
            )
            .join(
                self.engine.pl.scan_delta(
                    posixpath.join(self.engine.schema_or_working_directory_uri, "date_dim"),
                    storage_options=self.storage_options,
                ),
                left_on="ss_sold_date_sk",
                right_on="d_date_sk",
            )
            .join(
                self.engine.pl.scan_delta(
                    posixpath.join(self.engine.schema_or_working_directory_uri, "store"),
                    storage_options=self.storage_options,
                ),
                left_on="ss_store_sk",
                right_on="s_store_sk",
            )
            .join(
                self.engine.pl.scan_delta(
                    posixpath.join(self.engine.schema_or_working_directory_uri, "item"),
                    storage_options=self.storage_options,
                ),
                left_on="ss_item_sk",
                right_on="i_item_sk",
            )
            .join(
                self.engine.pl.scan_delta(
                    posixpath.join(self.engine.schema_or_working_directory_uri, "customer"),
                    storage_options=self.storage_options,
                ),
                left_on="ss_customer_sk",
                right_on="c_customer_sk",
            )
            .with_columns(
                [
                    # Create hash-based pseudo-random values for each row
                    (self.engine.pl.col("ss_customer_sk") + self.engine.pl.col("ss_sold_date_sk") + seed).alias(
                        "new_uid_val"
                    )
                ]
            )
            .filter((self.engine.pl.col("new_uid_val") % modulo) == 0)
            .with_columns(
                [
                    self.engine.pl.col("s_store_id"),
                    self.engine.pl.col("i_item_id"),
                    self.engine.pl.when(self.engine.pl.col("new_uid_val") % 2 == 0)
                    .then(self.engine.pl.col("c_customer_id"))
                    .otherwise(
                        self.engine.pl.concat_str(
                            [self.engine.pl.lit("NEW_"), self.engine.pl.col("new_uid_val")], separator=""
                        )
                    )
                    .alias("c_customer_id"),
                    self.engine.pl.col("d_date").alias("sale_date"),
                    (self.engine.pl.col("ss_quantity") + (self.engine.pl.col("new_uid_val") % 5) + 1).alias(
                        "total_quantity"
                    ),
                    (
                        self.engine.pl.col("ss_net_paid") + ((self.engine.pl.col("new_uid_val") % 5000) / 100.0) + 5
                    ).alias("total_net_paid"),
                    (
                        self.engine.pl.col("ss_net_profit") + ((self.engine.pl.col("new_uid_val") % 2000) / 100.0) + 1
                    ).alias("total_net_profit"),
                ]
            )
            .select(
                [
                    "s_store_id",
                    "i_item_id",
                    "c_customer_id",
                    "sale_date",
                    "total_quantity",
                    "total_net_paid",
                    "total_net_profit",
                ]
            )
        )

        sampled_fact_data.collect(engine="streaming").write_delta(
            posixpath.join(self.engine.schema_or_working_directory_uri, "total_sales_fact"),
            mode="merge",
            delta_merge_options={
                "predicate": """
                    target.s_store_id = source.s_store_id AND 
                    target.i_item_id = source.i_item_id AND 
                    target.c_customer_id = source.c_customer_id AND 
                    target.sale_date = source.sale_date
                    """,
                "source_alias": "source",
                "target_alias": "target",
            },
            storage_options=self.storage_options,
        ).when_matched_update(
            {
                "total_quantity": "target.total_quantity + source.total_quantity",
                "total_net_paid": "target.total_net_paid + source.total_net_paid",
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
        query_df = (
            self.engine.pl.scan_delta(
                posixpath.join(self.engine.schema_or_working_directory_uri, "total_sales_fact"),
                storage_options=self.storage_options,
            )
            .group_by(self.engine.pl.col("sale_date").dt.year())
            .agg(self.engine.pl.sum("total_net_profit").alias("sum_net_profit"))
            .collect()
        )
