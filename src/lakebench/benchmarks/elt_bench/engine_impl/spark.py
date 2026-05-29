from ....engines.spark import Spark


class SparkELTBench:
    def __init__(self, engine: Spark):

        import numpy as np

        self.np = np
        self.engine = engine

    def create_total_sales_fact(self):
        self.engine.spark.sql("""
            CREATE TABLE total_sales_fact USING DELTA AS
            SELECT 
                s.s_store_id,
                i.i_item_id,
                c.c_customer_id,
                d.d_date AS sale_date,
                SUM(ss.ss_quantity) AS total_quantity,
                SUM(ss.ss_net_paid) AS total_net_paid,
                SUM(ss.ss_net_profit) AS total_net_profit
            FROM 
                store_sales ss
            JOIN 
                date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
            JOIN 
                store s ON ss.ss_store_sk = s.s_store_sk
            JOIN 
                item i ON ss.ss_item_sk = i.i_item_sk
            JOIN 
                customer c ON ss.ss_customer_sk = c.c_customer_sk
            WHERE 
                d.d_year = 2001
            GROUP BY 
                s.s_store_id, i.i_item_id, c.c_customer_id, d.d_date
            ORDER BY 
                s.s_store_id, d.d_date;
        """)

    def merge_percent_into_total_sales_fact(self, percent: float):
        seed = self.np.random.randint(1, high=1000, size=None, dtype=int)
        modulo = int(1 / percent)

        sampled_fact_data = self.engine.spark.sql(f"""
            SELECT 
                s.s_store_id, 
                i.i_item_id, 
                CASE 
                    WHEN rand() > 0.5 THEN 
                        CONCAT('NEW_', CAST(new_uid_val AS STRING))
                    ELSE 
                        c.c_customer_id
                END AS c_customer_id,
                d.d_date AS sale_date,
                ss.ss_quantity + FLOOR(rand() * 5 + 1) AS total_quantity,
                ss.ss_net_paid + rand() * 50 + 5 AS total_net_paid,
                ss.ss_net_profit + rand() * 20 + 1 AS total_net_profit
            FROM (
                SELECT *, 
                       ss_customer_sk + ss_sold_date_sk + {seed} AS new_uid_val
                FROM store_sales
                WHERE MOD(ss_customer_sk + ss_sold_date_sk + {seed}, {modulo}) = 0
            ) ss
            JOIN date_dim d 
              ON ss.ss_sold_date_sk = d.d_date_sk
            JOIN store s 
              ON ss.ss_store_sk = s.s_store_sk
            JOIN item i 
              ON ss.ss_item_sk = i.i_item_sk
            JOIN customer c 
              ON ss.ss_customer_sk = c.c_customer_sk
            """)

        # Use the Delta Python API instead of SQL MERGEto work around a local delta-spark
        # bug (DELTA_MERGE_RESOLVED_ATTRIBUTE_MISSING_FROM_INPUT) where the SQL planner
        # fails to resolve target table attributes when source and target share column names.
        # Cloud runtimes (Databricks, Fabric, Synapse) use return this error.
        from delta.tables import DeltaTable

        delta_table = DeltaTable.forName(self.engine.spark, "total_sales_fact")
        delta_table.alias("target").merge(
            sampled_fact_data.alias("source"),
            "target.s_store_id = source.s_store_id AND "
            "target.i_item_id = source.i_item_id AND "
            "target.c_customer_id = source.c_customer_id AND "
            "target.sale_date = source.sale_date",
        ).whenMatchedUpdate(
            set={
                "total_quantity": "target.total_quantity + source.total_quantity",
                "total_net_paid": "target.total_net_paid + source.total_net_paid",
                "total_net_profit": "target.total_net_profit + source.total_net_profit",
            }
        ).whenNotMatchedInsertAll().execute()

    def query_total_sales_fact(self):
        df = self.engine.spark.sql("""
                            select sum(total_net_profit), year(sale_date) 
                            from total_sales_fact group by year(sale_date)
                            """)
        result = df.collect()
