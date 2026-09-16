from ....engines.fabric_data_warehouse import FabricDataWarehouse


class FabricDataWarehouseELTBench:
    def __init__(self, engine: FabricDataWarehouse):
        self.engine = engine

    def create_total_sales_fact(self):
        return self.engine.execute_sql_statement(f"""
            CREATE TABLE {self.engine.schema_name}.total_sales_fact AS
            SELECT
                s.s_store_id,
                i.i_item_id,
                c.c_customer_id,
                d.d_date AS sale_date,
                SUM(ss.ss_quantity) AS total_quantity,
                SUM(ss.ss_net_paid) AS total_net_paid,
                SUM(ss.ss_net_profit) AS total_net_profit
            FROM {self.engine.schema_name}.store_sales ss
            JOIN {self.engine.schema_name}.date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
            JOIN {self.engine.schema_name}.store s ON ss.ss_store_sk = s.s_store_sk
            JOIN {self.engine.schema_name}.item i ON ss.ss_item_sk = i.i_item_sk
            JOIN {self.engine.schema_name}.customer c ON ss.ss_customer_sk = c.c_customer_sk
            WHERE d.d_year = 2001
            GROUP BY s.s_store_id, i.i_item_id, c.c_customer_id, d.d_date
            ORDER BY s.s_store_id, d.d_date
        """)

    def merge_percent_into_total_sales_fact(self, percent: float):
        import numpy as np

        seed = np.random.randint(1, high=1000, size=None, dtype=int)
        modulo = int(1 / percent)
        # Fabric Data Warehouse has no RAND(), so a hashed NEWID() supplies the
        # per-row jitter that the other engines get from RAND().
        random_fraction = "ABS(CHECKSUM(NEWID())) % 10000000 / 10000000.0"

        return self.engine.execute_sql_statement(f"""
            WITH sampled_fact_data AS (
                SELECT
                    s.s_store_id,
                    i.i_item_id,
                    CASE
                        WHEN {random_fraction} > 0.5 THEN CONCAT('NEW_', new_uid_val)
                        ELSE c.c_customer_id
                    END AS c_customer_id,
                    d.d_date AS sale_date,
                    ss.ss_quantity + FLOOR({random_fraction} * 5 + 1) AS total_quantity,
                    ss.ss_net_paid + {random_fraction} * 50 + 5 AS total_net_paid,
                    ss.ss_net_profit + {random_fraction} * 20 + 1 AS total_net_profit
                FROM (
                    SELECT *,
                        ss_customer_sk + ss_sold_date_sk + {seed} AS new_uid_val
                    FROM {self.engine.schema_name}.store_sales
                    WHERE (ss_customer_sk + ss_sold_date_sk + {seed}) % {modulo} = 0
                ) ss
                JOIN {self.engine.schema_name}.date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
                JOIN {self.engine.schema_name}.store s ON ss.ss_store_sk = s.s_store_sk
                JOIN {self.engine.schema_name}.item i ON ss.ss_item_sk = i.i_item_sk
                JOIN {self.engine.schema_name}.customer c ON ss.ss_customer_sk = c.c_customer_sk
            )
            MERGE INTO {self.engine.schema_name}.total_sales_fact AS target
            USING (
                SELECT
                    s_store_id,
                    i_item_id,
                    c_customer_id,
                    sale_date,
                    total_quantity,
                    total_net_paid,
                    total_net_profit
                FROM sampled_fact_data
            ) AS source
            ON
                target.s_store_id = source.s_store_id AND
                target.i_item_id = source.i_item_id AND
                target.c_customer_id = source.c_customer_id AND
                target.sale_date = source.sale_date
            WHEN MATCHED THEN
                UPDATE SET
                    target.total_quantity = target.total_quantity + source.total_quantity,
                    target.total_net_paid = target.total_net_paid + source.total_net_paid,
                    target.total_net_profit = target.total_net_profit + source.total_net_profit
            WHEN NOT MATCHED THEN
                INSERT (s_store_id, i_item_id, c_customer_id, sale_date, total_quantity, total_net_paid, total_net_profit)
                VALUES (source.s_store_id, source.i_item_id, source.c_customer_id, source.sale_date,
                        source.total_quantity, source.total_net_paid, source.total_net_profit);
        """)

    def query_total_sales_fact(self):
        return self.engine.execute_sql_query(
            f"""
            SELECT SUM(total_net_profit), YEAR(sale_date)
            FROM {self.engine.schema_name}.total_sales_fact
            GROUP BY YEAR(sale_date)
            """,
            return_data=True,
        )
