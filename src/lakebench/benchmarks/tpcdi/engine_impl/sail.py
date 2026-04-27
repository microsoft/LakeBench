from ....engines.sail import Sail

import posixpath


class SailTPCDI:
    """Sail engine implementation for the TPC-DI benchmark.

    Sail uses a Spark-compatible API, so this implementation mirrors the SparkTPCDI
    approach with minor adjustments for Sail-specific patterns (register_table, delta-rs for merge).
    """

    def __init__(self, engine: Sail):
        self.engine = engine

    def load_source_file(self, file_uri, file_format, delimiter, table_name, context_decorator=None):
        if file_format in ('delimited', 'csv'):
            header = "false" if file_format == 'delimited' else "true"
            df = self.engine.spark.read.option("header", header).option("delimiter", delimiter).option("inferSchema", "true").csv(file_uri)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
        staging_cols = self.engine.spark.table(table_name).columns
        for i, col_name in enumerate(staging_cols):
            if i < len(df.columns):
                df = df.withColumnRenamed(df.columns[i], col_name)
        df.write.format("delta").mode("append").saveAsTable(table_name)
        return {'rows_loaded': str(df.count())}

    def load_dim_date(self, file_uri, context_decorator=None):
        df = self.engine.spark.read.option("header", "false").option("delimiter", "|").option("inferSchema", "true").csv(file_uri)
        staging_cols = self.engine.spark.table('dim_date').columns
        for i, col_name in enumerate(staging_cols):
            if i < len(df.columns):
                df = df.withColumnRenamed(df.columns[i], col_name)
        df.write.format("delta").mode("overwrite").save(posixpath.join(self.engine.schema_or_working_directory_uri, 'dim_date'))
        return {'table': 'dim_date'}

    def load_dim_time(self, file_uri, context_decorator=None):
        df = self.engine.spark.read.option("header", "false").option("delimiter", "|").option("inferSchema", "true").csv(file_uri)
        staging_cols = self.engine.spark.table('dim_time').columns
        for i, col_name in enumerate(staging_cols):
            if i < len(df.columns):
                df = df.withColumnRenamed(df.columns[i], col_name)
        df.write.format("delta").mode("overwrite").save(posixpath.join(self.engine.schema_or_working_directory_uri, 'dim_time'))
        return {'table': 'dim_time'}

    def parse_customer_mgmt_xml(self, file_uri, context_decorator=None):
        """Parse CustomerMgmt.xml using spark-xml."""
        df = self.engine.spark.read.format("xml").option("rowTag", "TPCDI:Action").option("rootTag", "TPCDI:Actions").load(file_uri)
        df.createOrReplaceTempView("customer_mgmt_raw")

        customer_df = self.engine.spark.sql("""
            SELECT ActionType AS cdc_flag, monotonically_increasing_id() AS cdc_dsn,
                Customer._C_ID AS c_id, Customer._C_TAX_ID AS c_tax_id,
                CAST(NULL AS STRING) AS c_st_id,
                Customer.Name.C_L_NAME AS c_l_name, Customer.Name.C_F_NAME AS c_f_name,
                Customer.Name.C_M_NAME AS c_m_name, Customer._C_GNDR AS c_gndr,
                CAST(Customer._C_TIER AS SMALLINT) AS c_tier, CAST(Customer._C_DOB AS DATE) AS c_dob,
                CAST(NULL AS STRING) AS c_adline1, CAST(NULL AS STRING) AS c_adline2,
                CAST(NULL AS STRING) AS c_zipcode, CAST(NULL AS STRING) AS c_city,
                CAST(NULL AS STRING) AS c_state_prov, CAST(NULL AS STRING) AS c_ctry,
                CAST(NULL AS STRING) AS c_ctry_1, CAST(NULL AS STRING) AS c_area_1,
                CAST(NULL AS STRING) AS c_local_1, CAST(NULL AS STRING) AS c_ext_1,
                CAST(NULL AS STRING) AS c_ctry_2, CAST(NULL AS STRING) AS c_area_2,
                CAST(NULL AS STRING) AS c_local_2, CAST(NULL AS STRING) AS c_ext_2,
                CAST(NULL AS STRING) AS c_ctry_3, CAST(NULL AS STRING) AS c_area_3,
                CAST(NULL AS STRING) AS c_local_3, CAST(NULL AS STRING) AS c_ext_3,
                CAST(NULL AS STRING) AS c_email_1, CAST(NULL AS STRING) AS c_email_2,
                CAST(NULL AS STRING) AS c_lcl_tx_id, CAST(NULL AS STRING) AS c_nat_tx_id
            FROM customer_mgmt_raw WHERE Customer IS NOT NULL
        """)
        customer_df.write.format("delta").mode("append").save(posixpath.join(self.engine.schema_or_working_directory_uri, 'staging_customer'))

        account_df = self.engine.spark.sql("""
            SELECT ActionType AS cdc_flag, monotonically_increasing_id() AS cdc_dsn,
                Customer.Account._CA_ID AS ca_id, Customer.Account._CA_B_ID AS ca_b_id,
                Customer._C_ID AS ca_c_id, Customer.Account.CA_NAME AS ca_name,
                CAST(Customer.Account._CA_TAX_ST AS SMALLINT) AS ca_tax_st,
                Customer.Account._CA_ST_ID AS ca_st_id
            FROM customer_mgmt_raw WHERE Customer.Account IS NOT NULL
        """)
        account_df.write.format("delta").mode("append").save(posixpath.join(self.engine.schema_or_working_directory_uri, 'staging_account'))

        return {'customer_rows': str(customer_df.count()), 'account_rows': str(account_df.count())}

    def parse_finwire(self, batch_uri, context_decorator=None):
        """Parse FINWIRE fixed-width files."""
        from pyspark.sql.functions import substring, trim, col, to_timestamp, to_date

        raw_df = self.engine.spark.read.text(posixpath.join(batch_uri, 'FINWIRE*'))
        raw_df = raw_df.withColumn("rec_type", trim(substring("value", 16, 3)))
        raw_df = raw_df.withColumn("pts", to_timestamp(substring("value", 1, 15), "yyyyMMdd-HHmmss"))

        cmp_df = raw_df.filter(col("rec_type") == "CMP").select(
            col("pts"), col("rec_type"),
            trim(substring("value", 19, 60)).alias("company_name"),
            substring("value", 79, 10).cast("bigint").alias("cik"),
            trim(substring("value", 89, 4)).alias("status"),
            trim(substring("value", 93, 2)).alias("industry_id"),
            trim(substring("value", 95, 4)).alias("sp_rating"),
            to_date(substring("value", 99, 8), "yyyyMMdd").alias("founding_date"),
            trim(substring("value", 107, 80)).alias("addr_line1"),
            trim(substring("value", 187, 80)).alias("addr_line2"),
            trim(substring("value", 267, 12)).alias("postal_code"),
            trim(substring("value", 279, 25)).alias("city"),
            trim(substring("value", 304, 20)).alias("state_province"),
            trim(substring("value", 324, 24)).alias("country"),
            trim(substring("value", 348, 46)).alias("ceo_name"),
            trim(substring("value", 394, 150)).alias("description"),
        )
        cmp_df.write.format("delta").mode("append").save(posixpath.join(self.engine.schema_or_working_directory_uri, 'staging_finwire_cmp'))

        sec_df = raw_df.filter(col("rec_type") == "SEC").select(
            col("pts"), col("rec_type"),
            trim(substring("value", 19, 15)).alias("symbol"),
            trim(substring("value", 34, 6)).alias("issue_type"),
            trim(substring("value", 40, 4)).alias("status"),
            trim(substring("value", 44, 70)).alias("name"),
            trim(substring("value", 114, 6)).alias("ex_id"),
            substring("value", 120, 13).cast("bigint").alias("sh_out"),
            to_date(substring("value", 133, 8), "yyyyMMdd").alias("first_trade_date"),
            to_date(substring("value", 141, 8), "yyyyMMdd").alias("first_trade_exchange"),
            substring("value", 149, 12).cast("decimal(10,2)").alias("dividend"),
            trim(substring("value", 161, 60)).alias("co_name_or_cik"),
        )
        sec_df.write.format("delta").mode("append").save(posixpath.join(self.engine.schema_or_working_directory_uri, 'staging_finwire_sec'))

        fin_df = raw_df.filter(col("rec_type") == "FIN").select(
            col("pts"), col("rec_type"),
            substring("value", 19, 4).cast("int").alias("year"),
            substring("value", 23, 1).cast("smallint").alias("quarter"),
            to_date(substring("value", 24, 8), "yyyyMMdd").alias("qtr_start_date"),
            to_date(substring("value", 32, 8), "yyyyMMdd").alias("posting_date"),
            substring("value", 40, 17).cast("decimal(15,2)").alias("revenue"),
            substring("value", 57, 17).cast("decimal(15,2)").alias("earnings"),
            substring("value", 74, 12).cast("decimal(10,2)").alias("eps"),
            substring("value", 86, 12).cast("decimal(10,2)").alias("diluted_eps"),
            substring("value", 98, 12).cast("decimal(10,2)").alias("margin"),
            substring("value", 110, 17).cast("decimal(15,2)").alias("inventory"),
            substring("value", 127, 17).cast("decimal(15,2)").alias("assets"),
            substring("value", 144, 17).cast("decimal(15,2)").alias("liabilities"),
            substring("value", 161, 13).cast("bigint").alias("sh_out"),
            substring("value", 174, 13).cast("bigint").alias("diluted_sh_out"),
            trim(substring("value", 187, 60)).alias("co_name_or_cik"),
        )
        fin_df.write.format("delta").mode("append").save(posixpath.join(self.engine.schema_or_working_directory_uri, 'staging_finwire_fin'))

        return {'cmp_rows': str(cmp_df.count()), 'sec_rows': str(sec_df.count()), 'fin_rows': str(fin_df.count())}

    def load_batch_date(self, file_uri, batch_id, context_decorator=None):
        return {'batch_id': str(batch_id)}

    def build_lookup_dimension(self, dim_table, batch_id, context_decorator=None):
        staging_map = {'dim_status_type': 'staging_status_type', 'dim_tax_rate': 'staging_tax_rate', 'dim_trade_type': 'staging_trade_type'}
        staging_table = staging_map[dim_table]
        self.engine.register_table(staging_table)
        df = self.engine.spark.sql(f"SELECT * FROM {staging_table}")
        df.write.format("delta").mode("overwrite").save(posixpath.join(self.engine.schema_or_working_directory_uri, dim_table))
        return {'table': dim_table}

    def build_dim_broker(self, batch_id, context_decorator=None):
        self.engine.register_table('staging_hr')
        df = self.engine.spark.sql(f"""
            SELECT monotonically_increasing_id() AS sk_broker_id,
                employee_id AS broker_id, manager_id,
                employee_first_name AS first_name, employee_last_name AS last_name,
                employee_mi AS middle_initial, employee_branch AS branch,
                employee_office AS office, employee_phone AS phone,
                true AS is_current, {batch_id} AS batch_id,
                CURRENT_DATE() AS effective_date, CAST('9999-12-31' AS DATE) AS end_date
            FROM staging_hr WHERE employee_job_code = 314
        """)
        df.write.format("delta").mode("overwrite").save(posixpath.join(self.engine.schema_or_working_directory_uri, 'dim_broker'))
        return {'table': 'dim_broker'}

    def build_dim_company(self, batch_id, context_decorator=None):
        self.engine.register_table('staging_finwire_cmp')
        df = self.engine.spark.sql(f"""
            SELECT monotonically_increasing_id() AS sk_company_id,
                cik AS company_id, status, company_name AS name,
                industry_id AS industry, sp_rating,
                CASE WHEN sp_rating LIKE 'A%' OR sp_rating LIKE 'BBB%' THEN false ELSE true END AS is_low_grade,
                ceo_name AS ceo, addr_line1 AS address_line1, addr_line2 AS address_line2,
                postal_code, city, state_province, country, description, founding_date,
                true AS is_current, {batch_id} AS batch_id,
                CAST(pts AS DATE) AS effective_date, CAST('9999-12-31' AS DATE) AS end_date
            FROM staging_finwire_cmp
        """)
        df.write.format("delta").mode("overwrite").save(posixpath.join(self.engine.schema_or_working_directory_uri, 'dim_company'))
        return {'table': 'dim_company'}

    def build_dim_security(self, batch_id, context_decorator=None):
        self.engine.register_table('staging_finwire_sec')
        self.engine.register_table('dim_company')
        df = self.engine.spark.sql(f"""
            SELECT monotonically_increasing_id() AS sk_security_id,
                s.symbol, s.issue_type, s.status, s.name,
                s.ex_id AS exchange_id, c.sk_company_id,
                s.sh_out AS shares_outstanding, s.first_trade_date AS first_trade,
                s.first_trade_exchange AS first_trade_on_exchange, s.dividend,
                true AS is_current, {batch_id} AS batch_id,
                CAST(s.pts AS DATE) AS effective_date, CAST('9999-12-31' AS DATE) AS end_date
            FROM staging_finwire_sec s
            LEFT JOIN dim_company c ON (s.co_name_or_cik = CAST(c.company_id AS STRING) OR s.co_name_or_cik = c.name) AND c.is_current = true
        """)
        df.write.format("delta").mode("overwrite").save(posixpath.join(self.engine.schema_or_working_directory_uri, 'dim_security'))
        return {'table': 'dim_security'}

    def build_dim_customer(self, batch_id, context_decorator=None):
        self.engine.register_table('staging_customer')
        self.engine.register_table('dim_tax_rate')
        df = self.engine.spark.sql(f"""
            SELECT monotonically_increasing_id() AS sk_customer_id,
                c.c_id AS customer_id, c.c_tax_id AS tax_id,
                COALESCE(c.c_st_id, 'ACTIVE') AS status,
                c.c_l_name AS last_name, c.c_f_name AS first_name,
                c.c_m_name AS middle_name, c.c_gndr AS gender,
                c.c_tier AS tier, c.c_dob AS dob,
                c.c_adline1 AS address_line1, c.c_adline2 AS address_line2,
                c.c_zipcode AS postal_code, c.c_city AS city,
                c.c_state_prov AS state_province, c.c_ctry AS country,
                CAST(NULL AS STRING) AS phone1, CAST(NULL AS STRING) AS phone2,
                CAST(NULL AS STRING) AS phone3,
                c.c_email_1 AS email1, c.c_email_2 AS email2,
                c.c_nat_tx_id AS national_tx_id,
                nt.tx_name AS national_tx_desc, nt.tx_rate AS national_tx_rate,
                c.c_lcl_tx_id AS local_tx_id,
                lt.tx_name AS local_tx_desc, lt.tx_rate AS local_tx_rate,
                CAST(NULL AS STRING) AS agency_id,
                CAST(NULL AS INT) AS credit_rating, CAST(NULL AS INT) AS net_worth,
                CAST(NULL AS STRING) AS marketing_nameplate,
                true AS is_current, {batch_id} AS batch_id,
                CURRENT_DATE() AS effective_date, CAST('9999-12-31' AS DATE) AS end_date
            FROM staging_customer c
            LEFT JOIN dim_tax_rate nt ON c.c_nat_tx_id = nt.tx_id
            LEFT JOIN dim_tax_rate lt ON c.c_lcl_tx_id = lt.tx_id
            WHERE c.cdc_flag IN ('I', 'NEW')
        """)
        df.write.format("delta").mode("overwrite").save(posixpath.join(self.engine.schema_or_working_directory_uri, 'dim_customer'))
        return {'table': 'dim_customer'}

    def build_dim_account(self, batch_id, context_decorator=None):
        self.engine.register_table('staging_account')
        self.engine.register_table('dim_broker')
        self.engine.register_table('dim_customer')
        df = self.engine.spark.sql(f"""
            SELECT monotonically_increasing_id() AS sk_account_id,
                a.ca_id AS account_id, b.sk_broker_id, c.sk_customer_id,
                a.ca_name AS account_desc, a.ca_tax_st AS tax_status,
                COALESCE(a.ca_st_id, 'ACTIVE') AS status,
                true AS is_current, {batch_id} AS batch_id,
                CURRENT_DATE() AS effective_date, CAST('9999-12-31' AS DATE) AS end_date
            FROM staging_account a
            LEFT JOIN dim_broker b ON a.ca_b_id = b.broker_id AND b.is_current = true
            LEFT JOIN dim_customer c ON a.ca_c_id = c.customer_id AND c.is_current = true
            WHERE a.cdc_flag IN ('I', 'NEW')
        """)
        df.write.format("delta").mode("overwrite").save(posixpath.join(self.engine.schema_or_working_directory_uri, 'dim_account'))
        return {'table': 'dim_account'}

    def build_dim_trade(self, batch_id, context_decorator=None):
        self.engine.register_table('staging_trade')
        self.engine.register_table('dim_security')
        self.engine.register_table('dim_account')
        self.engine.register_table('dim_date')
        df = self.engine.spark.sql(f"""
            SELECT monotonically_increasing_id() AS sk_trade_id,
                t.t_id AS trade_id, CAST(NULL AS BIGINT) AS sk_broker_id,
                dd.sk_date_id AS sk_create_date_id,
                CAST(NULL AS BIGINT) AS sk_create_time_id,
                CAST(NULL AS BIGINT) AS sk_close_date_id,
                CAST(NULL AS BIGINT) AS sk_close_time_id,
                t.t_st_id AS status, t.t_tt_id AS type,
                CASE WHEN t.t_is_cash = 1 THEN true ELSE false END AS is_cash,
                sec.sk_security_id, sec.sk_company_id,
                t.t_qty AS quantity, t.t_bid_price AS bid_price,
                ca.sk_customer_id, ca.sk_account_id,
                t.t_exec_name AS executed_by, t.t_trade_price AS trade_price,
                t.t_chrg AS fee, t.t_comm AS commission, t.t_tax AS tax,
                {batch_id} AS batch_id
            FROM staging_trade t
            LEFT JOIN dim_security sec ON t.t_s_symb = sec.symbol AND sec.is_current = true
            LEFT JOIN dim_account ca ON t.t_ca_id = ca.account_id AND ca.is_current = true
            LEFT JOIN dim_date dd ON CAST(t.t_dts AS DATE) = dd.date_value
        """)
        df.write.format("delta").mode("append").save(posixpath.join(self.engine.schema_or_working_directory_uri, 'dim_trade'))
        return {'table': 'dim_trade'}

    def build_fact_market_history(self, batch_id, context_decorator=None):
        self.engine.register_table('staging_daily_market')
        self.engine.register_table('dim_security')
        self.engine.register_table('dim_date')
        df = self.engine.spark.sql(f"""
            SELECT sec.sk_security_id, sec.sk_company_id, dd.sk_date_id,
                CAST(NULL AS DECIMAL(10,2)) AS peratio,
                CAST(NULL AS DECIMAL(5,2)) AS yield_val,
                dm.dm_high AS fifty_two_week_high, dd.sk_date_id AS sk_fifty_two_week_high_date,
                dm.dm_low AS fifty_two_week_low, dd.sk_date_id AS sk_fifty_two_week_low_date,
                dm.dm_close AS close_price, dm.dm_high AS day_high, dm.dm_low AS day_low,
                dm.dm_vol AS volume, {batch_id} AS batch_id
            FROM staging_daily_market dm
            JOIN dim_security sec ON dm.dm_s_symb = sec.symbol AND sec.is_current = true
            JOIN dim_date dd ON dm.dm_date = dd.date_value
        """)
        df.write.format("delta").mode("append").save(posixpath.join(self.engine.schema_or_working_directory_uri, 'fact_market_history'))
        return {'table': 'fact_market_history'}

    def build_fact_watches(self, batch_id, context_decorator=None):
        self.engine.register_table('staging_watch_history')
        self.engine.register_table('dim_customer')
        self.engine.register_table('dim_security')
        self.engine.register_table('dim_date')
        df = self.engine.spark.sql(f"""
            SELECT c.sk_customer_id, sec.sk_security_id,
                dd.sk_date_id AS sk_date_id_date_placed,
                CASE WHEN w.w_action = 'CNCL' THEN dd.sk_date_id ELSE NULL END AS sk_date_id_date_removed,
                {batch_id} AS batch_id
            FROM staging_watch_history w
            JOIN dim_customer c ON w.w_c_id = c.customer_id AND c.is_current = true
            JOIN dim_security sec ON w.w_s_symb = sec.symbol AND sec.is_current = true
            JOIN dim_date dd ON CAST(w.w_dts AS DATE) = dd.date_value
        """)
        df.write.format("delta").mode("append").save(posixpath.join(self.engine.schema_or_working_directory_uri, 'fact_watches'))
        return {'table': 'fact_watches'}

    def build_fact_cash_balances(self, batch_id, context_decorator=None):
        self.engine.register_table('staging_cash_transaction')
        self.engine.register_table('dim_account')
        self.engine.register_table('dim_date')
        df = self.engine.spark.sql(f"""
            SELECT ca.sk_customer_id, ca.sk_account_id, dd.sk_date_id,
                SUM(ct.ct_amt) AS cash, {batch_id} AS batch_id
            FROM staging_cash_transaction ct
            JOIN dim_account ca ON ct.ct_ca_id = ca.account_id AND ca.is_current = true
            JOIN dim_date dd ON CAST(ct.ct_dts AS DATE) = dd.date_value
            GROUP BY ca.sk_customer_id, ca.sk_account_id, dd.sk_date_id
        """)
        df.write.format("delta").mode("append").save(posixpath.join(self.engine.schema_or_working_directory_uri, 'fact_cash_balances'))
        return {'table': 'fact_cash_balances'}

    def build_fact_holdings(self, batch_id, context_decorator=None):
        self.engine.register_table('dim_trade')
        df = self.engine.spark.sql(f"""
            SELECT trade_id, trade_id AS current_trade_id,
                sk_customer_id, sk_account_id, sk_security_id, sk_company_id,
                sk_create_date_id AS sk_date_id, sk_create_time_id AS sk_time_id,
                trade_price AS current_price, quantity AS current_holding,
                {batch_id} AS batch_id
            FROM dim_trade WHERE batch_id = {batch_id} AND is_cash = true
        """)
        df.write.format("delta").mode("append").save(posixpath.join(self.engine.schema_or_working_directory_uri, 'fact_holdings'))
        return {'table': 'fact_holdings'}

    def build_financial(self, batch_id, context_decorator=None):
        self.engine.register_table('staging_finwire_fin')
        self.engine.register_table('dim_company')
        df = self.engine.spark.sql("""
            SELECT c.sk_company_id, f.year AS fi_year, f.quarter AS fi_qtr,
                f.qtr_start_date AS fi_qtr_start_date, f.revenue AS fi_revenue,
                f.earnings AS fi_net_earn, f.eps AS fi_basic_eps, f.diluted_eps AS fi_dilut_eps,
                f.margin AS fi_margin, f.inventory AS fi_inventory, f.assets AS fi_assets,
                f.liabilities AS fi_liability, f.sh_out AS fi_out_basic, f.diluted_sh_out AS fi_out_dilut
            FROM staging_finwire_fin f
            LEFT JOIN dim_company c ON (f.co_name_or_cik = CAST(c.company_id AS STRING) OR f.co_name_or_cik = c.name) AND c.is_current = true
        """)
        df.write.format("delta").mode("overwrite").save(posixpath.join(self.engine.schema_or_working_directory_uri, 'financial'))
        return {'table': 'financial'}

    def build_prospect(self, batch_id, context_decorator=None):
        self.engine.register_table('staging_prospect')
        self.engine.register_table('dim_customer')
        df = self.engine.spark.sql(f"""
            SELECT p.agency_id,
                CAST(NULL AS BIGINT) AS sk_record_date_id,
                CAST(NULL AS BIGINT) AS sk_update_date_id,
                {batch_id} AS batch_id,
                CASE WHEN c.sk_customer_id IS NOT NULL THEN true ELSE false END AS is_customer,
                p.last_name, p.first_name, p.middle_initial, p.gender,
                p.address_line1, p.address_line2, p.postal_code,
                p.city, p.state, p.country, p.phone,
                p.income, p.number_cars, p.number_children,
                p.marital_status, p.age, p.credit_rating,
                p.own_or_rent_flag, p.employer, p.number_credit_cards, p.net_worth,
                CAST(NULL AS STRING) AS marketing_nameplate
            FROM staging_prospect p
            LEFT JOIN dim_customer c ON UPPER(p.last_name) = UPPER(c.last_name)
                AND UPPER(p.first_name) = UPPER(c.first_name) AND p.address_line1 = c.address_line1
                AND p.postal_code = c.postal_code AND c.is_current = true
        """)
        df.write.format("delta").mode("append").save(posixpath.join(self.engine.schema_or_working_directory_uri, 'prospect'))
        return {'table': 'prospect'}

    def merge_incremental_scd2(self, table_name, batch_id, context_decorator=None):
        """Apply SCD Type 2 merge using delta-rs (Sail doesn't support SQL MERGE directly)."""
        if table_name == 'dim_customer':
            self.engine.register_table('staging_customer')
            updated = self.engine.spark.sql("""
                SELECT DISTINCT c_id AS customer_id FROM staging_customer WHERE cdc_flag IN ('U', 'UPDCUST')
            """).toArrow()
            if updated.num_rows > 0:
                table = self.engine.deltars.DeltaTable(
                    table_uri=posixpath.join(self.engine.schema_or_working_directory_uri, 'dim_customer'),
                    storage_options=self.engine.storage_options,
                )
                table.merge(source=updated, predicate="target.customer_id = source.customer_id AND target.is_current = true",
                    source_alias="source", target_alias="target"
                ).when_matched_update({"is_current": "false"}).execute()
            self.build_dim_customer(batch_id=batch_id)

        elif table_name == 'dim_account':
            self.engine.register_table('staging_account')
            updated = self.engine.spark.sql("""
                SELECT DISTINCT ca_id AS account_id FROM staging_account WHERE cdc_flag IN ('U', 'UPDACCT')
            """).toArrow()
            if updated.num_rows > 0:
                table = self.engine.deltars.DeltaTable(
                    table_uri=posixpath.join(self.engine.schema_or_working_directory_uri, 'dim_account'),
                    storage_options=self.engine.storage_options,
                )
                table.merge(source=updated, predicate="target.account_id = source.account_id AND target.is_current = true",
                    source_alias="source", target_alias="target"
                ).when_matched_update({"is_current": "false"}).execute()
            self.build_dim_account(batch_id=batch_id)

        return {'table': table_name, 'batch_id': str(batch_id)}

    def validate_audit(self, audit_file_uri, batch_id, context_decorator=None):
        validation_results = {}
        target_tables = ['dim_customer', 'dim_account', 'dim_broker', 'dim_company', 'dim_security', 'dim_trade',
                         'fact_market_history', 'fact_watches', 'fact_cash_balances', 'fact_holdings', 'financial', 'prospect']
        for table in target_tables:
            try:
                self.engine.register_table(table)
                count = self.engine.spark.table(table).count()
                validation_results[f'{table}_count'] = str(count)
            except Exception:
                validation_results[f'{table}_count'] = 'ERROR'
        return validation_results
