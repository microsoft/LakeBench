import posixpath

from ....engines.sail import Sail


class SailELTBench:
    def __init__(self, engine: Sail):

        import numpy as np

        self.np = np
        self.engine = engine

    def create_total_sales_fact(self):
        for table in ["store_sales", "date_dim", "store", "item", "customer"]:
            self.engine.register_table(table)

        df = self.engine.spark.sql("""
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

        df.write.format("delta").mode("overwrite").save(
            posixpath.join(self.engine.schema_or_working_directory_uri, "total_sales_fact")
        )

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
            """).toArrow()

        fact_table = self.engine.deltars.DeltaTable(
            table_uri=posixpath.join(self.engine.schema_or_working_directory_uri, "total_sales_fact"),
            storage_options=self.engine.storage_options,
        )

        fact_table.merge(
            source=sampled_fact_data,
            predicate="""
                target.s_store_id = source.s_store_id AND 
                target.i_item_id = source.i_item_id AND 
                target.c_customer_id = source.c_customer_id AND 
                target.sale_date = source.sale_date
                """,
            source_alias="source",
            target_alias="target",
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
        self.engine.register_table("total_sales_fact")
        df = self.engine.spark.sql("""
                            select sum(total_net_profit), year(sale_date) 
                            from total_sales_fact group by year(sale_date)
                            """)
        result = df.collect()
