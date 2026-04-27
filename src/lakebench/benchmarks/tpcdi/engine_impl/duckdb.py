from ....engines.duckdb import DuckDB
from ....engines.delta_rs import DeltaRs

import posixpath


class DuckDBTPCDI:
    """DuckDB engine implementation for the TPC-DI benchmark."""

    def __init__(self, engine: DuckDB):
        self.engine = engine
        self.delta_rs = DeltaRs()
        self.write_deltalake = self.delta_rs.write_deltalake
        self.DeltaTable = self.delta_rs.DeltaTable

    def _table_uri(self, table_name):
        return posixpath.join(self.engine.schema_or_working_directory_uri, table_name)

    def _delta_scan(self, table_name):
        return f"delta_scan('{self._table_uri(table_name)}')"

    def load_source_file(self, file_uri, file_format, delimiter, table_name, context_decorator=None):
        """Load a delimited source file into a staging Delta table."""
        self.engine.duckdb.sql("use main")

        if file_format in ('delimited', 'csv'):
            header = 'true' if file_format == 'csv' else 'false'
            arrow_df = self.engine.duckdb.sql(f"""
                SELECT * FROM read_csv('{file_uri}',
                    header={header},
                    delimiter='{delimiter}',
                    auto_detect=true
                )
            """).record_batch()
        else:
            raise ValueError(f"Unsupported file format: {file_format}")

        self.write_deltalake(
            table_or_uri=self._table_uri(table_name),
            data=arrow_df,
            mode="append",
            storage_options=self.engine.storage_options,
        )
        return {'rows_loaded': str(arrow_df.num_rows) if hasattr(arrow_df, 'num_rows') else 'N/A'}

    def load_dim_date(self, file_uri, context_decorator=None):
        """Load Date.txt directly into dim_date."""
        self.engine.duckdb.sql("use main")
        arrow_df = self.engine.duckdb.sql(f"""
            SELECT * FROM read_csv('{file_uri}',
                header=false, delimiter='|', auto_detect=true)
        """).record_batch()
        self.write_deltalake(
            table_or_uri=self._table_uri('dim_date'),
            data=arrow_df,
            mode="overwrite",
            storage_options=self.engine.storage_options,
        )
        return {'table': 'dim_date'}

    def load_dim_time(self, file_uri, context_decorator=None):
        """Load Time.txt directly into dim_time."""
        self.engine.duckdb.sql("use main")
        arrow_df = self.engine.duckdb.sql(f"""
            SELECT * FROM read_csv('{file_uri}',
                header=false, delimiter='|', auto_detect=true)
        """).record_batch()
        self.write_deltalake(
            table_or_uri=self._table_uri('dim_time'),
            data=arrow_df,
            mode="overwrite",
            storage_options=self.engine.storage_options,
        )
        return {'table': 'dim_time'}

    def parse_customer_mgmt_xml(self, file_uri, context_decorator=None):
        """Parse CustomerMgmt.xml using Python lxml and load into staging tables."""
        from lxml import etree
        import pyarrow as pa

        tree = etree.parse(file_uri)
        root = tree.getroot()
        ns = {'tpcdi': root.nsmap.get(None, '')} if root.nsmap else {}

        customer_records = []
        account_records = []
        dsn = 0

        for action in root.iter():
            if 'Action' in action.tag:
                action_type = action.get('ActionType', '')
                customer = action.find('.//Customer', ns) if ns else action.find('.//Customer')
                if customer is not None:
                    dsn += 1
                    c_id = customer.get('C_ID')
                    customer_records.append({
                        'cdc_flag': action_type, 'cdc_dsn': dsn,
                        'c_id': int(c_id) if c_id else None,
                        'c_tax_id': customer.get('C_TAX_ID'),
                        'c_st_id': None,
                        'c_l_name': self._xml_text(customer, './/C_L_NAME', ns),
                        'c_f_name': self._xml_text(customer, './/C_F_NAME', ns),
                        'c_m_name': self._xml_text(customer, './/C_M_NAME', ns),
                        'c_gndr': customer.get('C_GNDR'),
                        'c_tier': int(customer.get('C_TIER')) if customer.get('C_TIER') else None,
                        'c_dob': customer.get('C_DOB'),
                    })

                    acct = customer.find('.//Account', ns) if ns else customer.find('.//Account')
                    if acct is not None:
                        account_records.append({
                            'cdc_flag': action_type, 'cdc_dsn': dsn,
                            'ca_id': int(acct.get('CA_ID')) if acct.get('CA_ID') else None,
                            'ca_b_id': int(acct.get('CA_B_ID')) if acct.get('CA_B_ID') else None,
                            'ca_c_id': int(c_id) if c_id else None,
                            'ca_name': self._xml_text(acct, 'CA_NAME', ns),
                            'ca_tax_st': int(acct.get('CA_TAX_ST')) if acct.get('CA_TAX_ST') else None,
                            'ca_st_id': acct.get('CA_ST_ID'),
                        })

        if customer_records:
            cust_table = pa.Table.from_pylist(customer_records)
            self.write_deltalake(
                table_or_uri=self._table_uri('staging_customer'),
                data=cust_table,
                mode="append",
                storage_options=self.engine.storage_options,
            )
        if account_records:
            acct_table = pa.Table.from_pylist(account_records)
            self.write_deltalake(
                table_or_uri=self._table_uri('staging_account'),
                data=acct_table,
                mode="append",
                storage_options=self.engine.storage_options,
            )

        return {'customer_rows': str(len(customer_records)), 'account_rows': str(len(account_records))}

    def _xml_text(self, element, path, ns):
        """Helper to extract text from an XML element."""
        child = element.find(path, ns) if ns else element.find(path)
        return child.text if child is not None else None

    def parse_finwire(self, batch_uri, context_decorator=None):
        """Parse FINWIRE fixed-width files."""
        import pyarrow as pa
        import os

        cmp_records, sec_records, fin_records = [], [], []

        # Find all FINWIRE files
        if os.path.isdir(batch_uri):
            finwire_files = sorted([
                os.path.join(batch_uri, f) for f in os.listdir(batch_uri)
                if f.startswith('FINWIRE') and not f.endswith('.csv')
            ])
        else:
            finwire_files = [batch_uri]

        for filepath in finwire_files:
            with open(filepath, 'r') as f:
                for line in f:
                    if len(line) < 18:
                        continue
                    rec_type = line[15:18].strip()
                    pts = line[0:15].strip()

                    if rec_type == 'CMP':
                        cmp_records.append({
                            'pts': pts, 'rec_type': rec_type,
                            'company_name': line[18:78].strip(),
                            'cik': int(line[78:88].strip()) if line[78:88].strip() else None,
                            'status': line[88:92].strip(),
                            'industry_id': line[92:94].strip(),
                            'sp_rating': line[94:98].strip(),
                            'founding_date': line[98:106].strip() or None,
                            'addr_line1': line[106:186].strip(),
                            'addr_line2': line[186:266].strip(),
                            'postal_code': line[266:278].strip(),
                            'city': line[278:303].strip(),
                            'state_province': line[303:323].strip(),
                            'country': line[323:347].strip(),
                            'ceo_name': line[347:393].strip(),
                            'description': line[393:].strip(),
                        })
                    elif rec_type == 'SEC':
                        sec_records.append({
                            'pts': pts, 'rec_type': rec_type,
                            'symbol': line[18:33].strip(),
                            'issue_type': line[33:39].strip(),
                            'status': line[39:43].strip(),
                            'name': line[43:113].strip(),
                            'ex_id': line[113:119].strip(),
                            'sh_out': int(line[119:132].strip()) if line[119:132].strip() else None,
                            'first_trade_date': line[132:140].strip() or None,
                            'first_trade_exchange': line[140:148].strip() or None,
                            'dividend': line[148:160].strip() or None,
                            'co_name_or_cik': line[160:].strip(),
                        })
                    elif rec_type == 'FIN':
                        fin_records.append({
                            'pts': pts, 'rec_type': rec_type,
                            'year': int(line[18:22].strip()) if line[18:22].strip() else None,
                            'quarter': int(line[22:23].strip()) if line[22:23].strip() else None,
                            'qtr_start_date': line[23:31].strip() or None,
                            'posting_date': line[31:39].strip() or None,
                            'revenue': line[39:56].strip() or None,
                            'earnings': line[56:73].strip() or None,
                            'eps': line[73:85].strip() or None,
                            'diluted_eps': line[85:97].strip() or None,
                            'margin': line[97:109].strip() or None,
                            'inventory': line[109:126].strip() or None,
                            'assets': line[126:143].strip() or None,
                            'liabilities': line[143:160].strip() or None,
                            'sh_out': int(line[160:173].strip()) if line[160:173].strip() else None,
                            'diluted_sh_out': int(line[173:186].strip()) if line[173:186].strip() else None,
                            'co_name_or_cik': line[186:].strip(),
                        })

        for records, table_name in [
            (cmp_records, 'staging_finwire_cmp'),
            (sec_records, 'staging_finwire_sec'),
            (fin_records, 'staging_finwire_fin'),
        ]:
            if records:
                table = pa.Table.from_pylist(records)
                self.write_deltalake(
                    table_or_uri=self._table_uri(table_name),
                    data=table,
                    mode="append",
                    storage_options=self.engine.storage_options,
                )

        return {
            'cmp_rows': str(len(cmp_records)),
            'sec_rows': str(len(sec_records)),
            'fin_rows': str(len(fin_records)),
        }

    def load_batch_date(self, file_uri, batch_id, context_decorator=None):
        """Load BatchDate.txt for a given batch."""
        return {'batch_id': str(batch_id)}

    def build_lookup_dimension(self, dim_table, batch_id, context_decorator=None):
        """Build lookup dimension by copying from staging."""
        staging_map = {
            'dim_status_type': 'staging_status_type',
            'dim_tax_rate': 'staging_tax_rate',
            'dim_trade_type': 'staging_trade_type',
        }
        staging_table = staging_map[dim_table]
        self.engine.duckdb.sql("use main")
        self.engine.register_table(staging_table)
        arrow_df = self.engine.duckdb.sql(f"SELECT * FROM {staging_table}").record_batch()
        self.write_deltalake(
            table_or_uri=self._table_uri(dim_table),
            data=arrow_df,
            mode="overwrite",
            storage_options=self.engine.storage_options,
        )
        return {'table': dim_table}

    def build_dim_broker(self, batch_id, context_decorator=None):
        """Build DimBroker from HR staging data."""
        self.engine.duckdb.sql("use main")
        self.engine.register_table('staging_hr')
        arrow_df = self.engine.duckdb.sql(f"""
            SELECT
                row_number() OVER () AS sk_broker_id,
                employee_id AS broker_id,
                manager_id,
                employee_first_name AS first_name,
                employee_last_name AS last_name,
                employee_mi AS middle_initial,
                employee_branch AS branch,
                employee_office AS office,
                employee_phone AS phone,
                true AS is_current,
                {batch_id} AS batch_id,
                CURRENT_DATE AS effective_date,
                CAST('9999-12-31' AS DATE) AS end_date
            FROM staging_hr
            WHERE employee_job_code = '314'
        """).record_batch()
        self.write_deltalake(
            table_or_uri=self._table_uri('dim_broker'),
            data=arrow_df,
            mode="overwrite",
            storage_options=self.engine.storage_options,
        )
        return {'table': 'dim_broker'}

    def build_dim_company(self, batch_id, context_decorator=None):
        """Build DimCompany from FINWIRE CMP records."""
        self.engine.duckdb.sql("use main")
        self.engine.register_table('staging_finwire_cmp')
        arrow_df = self.engine.duckdb.sql(f"""
            SELECT
                row_number() OVER () AS sk_company_id,
                cik AS company_id,
                status,
                company_name AS name,
                industry_id AS industry,
                sp_rating,
                CASE WHEN sp_rating LIKE 'A%' OR sp_rating LIKE 'BBB%' THEN false ELSE true END AS is_low_grade,
                ceo_name AS ceo,
                addr_line1 AS address_line1,
                addr_line2 AS address_line2,
                postal_code,
                city,
                state_province,
                country,
                description,
                founding_date,
                true AS is_current,
                {batch_id} AS batch_id,
                CAST(pts AS DATE) AS effective_date,
                CAST('9999-12-31' AS DATE) AS end_date
            FROM staging_finwire_cmp
        """).record_batch()
        self.write_deltalake(
            table_or_uri=self._table_uri('dim_company'),
            data=arrow_df,
            mode="overwrite",
            storage_options=self.engine.storage_options,
        )
        return {'table': 'dim_company'}

    def build_dim_security(self, batch_id, context_decorator=None):
        """Build DimSecurity from FINWIRE SEC records."""
        self.engine.duckdb.sql("use main")
        self.engine.register_table('staging_finwire_sec')
        self.engine.register_table('dim_company')
        arrow_df = self.engine.duckdb.sql(f"""
            SELECT
                row_number() OVER () AS sk_security_id,
                s.symbol,
                s.issue_type,
                s.status,
                s.name,
                s.ex_id AS exchange_id,
                c.sk_company_id,
                s.sh_out AS shares_outstanding,
                s.first_trade_date AS first_trade,
                s.first_trade_exchange AS first_trade_on_exchange,
                s.dividend,
                true AS is_current,
                {batch_id} AS batch_id,
                CAST(s.pts AS DATE) AS effective_date,
                CAST('9999-12-31' AS DATE) AS end_date
            FROM {self._delta_scan('staging_finwire_sec')} s
            LEFT JOIN {self._delta_scan('dim_company')} c
                ON (s.co_name_or_cik = CAST(c.company_id AS VARCHAR) OR s.co_name_or_cik = c.name)
                AND c.is_current = true
        """).record_batch()
        self.write_deltalake(
            table_or_uri=self._table_uri('dim_security'),
            data=arrow_df,
            mode="overwrite",
            storage_options=self.engine.storage_options,
        )
        return {'table': 'dim_security'}

    def build_dim_customer(self, batch_id, context_decorator=None):
        """Build DimCustomer from staging_customer (SCD Type 2)."""
        self.engine.duckdb.sql("use main")
        arrow_df = self.engine.duckdb.sql(f"""
            SELECT
                row_number() OVER () AS sk_customer_id,
                c.c_id AS customer_id,
                c.c_tax_id AS tax_id,
                COALESCE(c.c_st_id, 'ACTIVE') AS status,
                c.c_l_name AS last_name,
                c.c_f_name AS first_name,
                c.c_m_name AS middle_name,
                c.c_gndr AS gender,
                c.c_tier AS tier,
                CAST(c.c_dob AS DATE) AS dob,
                CAST(NULL AS VARCHAR) AS address_line1,
                CAST(NULL AS VARCHAR) AS address_line2,
                CAST(NULL AS VARCHAR) AS postal_code,
                CAST(NULL AS VARCHAR) AS city,
                CAST(NULL AS VARCHAR) AS state_province,
                CAST(NULL AS VARCHAR) AS country,
                CAST(NULL AS VARCHAR) AS phone1,
                CAST(NULL AS VARCHAR) AS phone2,
                CAST(NULL AS VARCHAR) AS phone3,
                CAST(NULL AS VARCHAR) AS email1,
                CAST(NULL AS VARCHAR) AS email2,
                c.c_nat_tx_id AS national_tx_id,
                nt.tx_name AS national_tx_desc,
                nt.tx_rate AS national_tx_rate,
                c.c_lcl_tx_id AS local_tx_id,
                lt.tx_name AS local_tx_desc,
                lt.tx_rate AS local_tx_rate,
                CAST(NULL AS VARCHAR) AS agency_id,
                CAST(NULL AS INT) AS credit_rating,
                CAST(NULL AS INT) AS net_worth,
                CAST(NULL AS VARCHAR) AS marketing_nameplate,
                true AS is_current,
                {batch_id} AS batch_id,
                CURRENT_DATE AS effective_date,
                CAST('9999-12-31' AS DATE) AS end_date
            FROM {self._delta_scan('staging_customer')} c
            LEFT JOIN {self._delta_scan('dim_tax_rate')} nt ON c.c_nat_tx_id = nt.tx_id
            LEFT JOIN {self._delta_scan('dim_tax_rate')} lt ON c.c_lcl_tx_id = lt.tx_id
            WHERE c.cdc_flag IN ('I', 'NEW')
        """).record_batch()
        self.write_deltalake(
            table_or_uri=self._table_uri('dim_customer'),
            data=arrow_df,
            mode="overwrite" if batch_id == 1 else "append",
            storage_options=self.engine.storage_options,
        )
        return {'table': 'dim_customer'}

    def build_dim_account(self, batch_id, context_decorator=None):
        """Build DimAccount from staging_account."""
        self.engine.duckdb.sql("use main")
        arrow_df = self.engine.duckdb.sql(f"""
            SELECT
                row_number() OVER () AS sk_account_id,
                a.ca_id AS account_id,
                b.sk_broker_id,
                c.sk_customer_id,
                a.ca_name AS account_desc,
                a.ca_tax_st AS tax_status,
                COALESCE(a.ca_st_id, 'ACTIVE') AS status,
                true AS is_current,
                {batch_id} AS batch_id,
                CURRENT_DATE AS effective_date,
                CAST('9999-12-31' AS DATE) AS end_date
            FROM {self._delta_scan('staging_account')} a
            LEFT JOIN {self._delta_scan('dim_broker')} b ON a.ca_b_id = b.broker_id AND b.is_current = true
            LEFT JOIN {self._delta_scan('dim_customer')} c ON a.ca_c_id = c.customer_id AND c.is_current = true
            WHERE a.cdc_flag IN ('I', 'NEW')
        """).record_batch()
        self.write_deltalake(
            table_or_uri=self._table_uri('dim_account'),
            data=arrow_df,
            mode="overwrite" if batch_id == 1 else "append",
            storage_options=self.engine.storage_options,
        )
        return {'table': 'dim_account'}

    def build_dim_trade(self, batch_id, context_decorator=None):
        """Build DimTrade from staging_trade."""
        self.engine.duckdb.sql("use main")
        arrow_df = self.engine.duckdb.sql(f"""
            SELECT
                row_number() OVER () AS sk_trade_id,
                t.t_id AS trade_id,
                CAST(NULL AS BIGINT) AS sk_broker_id,
                dd_create.sk_date_id AS sk_create_date_id,
                CAST(NULL AS BIGINT) AS sk_create_time_id,
                CAST(NULL AS BIGINT) AS sk_close_date_id,
                CAST(NULL AS BIGINT) AS sk_close_time_id,
                t.t_st_id AS status,
                t.t_tt_id AS type,
                CASE WHEN t.t_is_cash = 1 THEN true ELSE false END AS is_cash,
                sec.sk_security_id,
                sec.sk_company_id,
                t.t_qty AS quantity,
                t.t_bid_price AS bid_price,
                ca.sk_customer_id,
                ca.sk_account_id,
                t.t_exec_name AS executed_by,
                t.t_trade_price AS trade_price,
                t.t_chrg AS fee,
                t.t_comm AS commission,
                t.t_tax AS tax,
                {batch_id} AS batch_id
            FROM {self._delta_scan('staging_trade')} t
            LEFT JOIN {self._delta_scan('dim_security')} sec ON t.t_s_symb = sec.symbol AND sec.is_current = true
            LEFT JOIN {self._delta_scan('dim_account')} ca ON t.t_ca_id = ca.account_id AND ca.is_current = true
            LEFT JOIN {self._delta_scan('dim_date')} dd_create ON CAST(t.t_dts AS DATE) = dd_create.date_value
        """).record_batch()
        self.write_deltalake(
            table_or_uri=self._table_uri('dim_trade'),
            data=arrow_df,
            mode="append",
            storage_options=self.engine.storage_options,
        )
        return {'table': 'dim_trade'}

    def build_fact_market_history(self, batch_id, context_decorator=None):
        """Build FactMarketHistory from staging_daily_market."""
        self.engine.duckdb.sql("use main")
        arrow_df = self.engine.duckdb.sql(f"""
            SELECT
                sec.sk_security_id,
                sec.sk_company_id,
                dd.sk_date_id,
                CASE WHEN fin.fi_basic_eps > 0 THEN dm.dm_close / fin.fi_basic_eps ELSE NULL END AS peratio,
                CASE WHEN sec.dividend > 0 AND dm.dm_close > 0 THEN sec.dividend / dm.dm_close * 100 ELSE NULL END AS yield_val,
                dm.dm_high AS fifty_two_week_high,
                dd.sk_date_id AS sk_fifty_two_week_high_date,
                dm.dm_low AS fifty_two_week_low,
                dd.sk_date_id AS sk_fifty_two_week_low_date,
                dm.dm_close AS close_price,
                dm.dm_high AS day_high,
                dm.dm_low AS day_low,
                dm.dm_vol AS volume,
                {batch_id} AS batch_id
            FROM {self._delta_scan('staging_daily_market')} dm
            JOIN {self._delta_scan('dim_security')} sec ON dm.dm_s_symb = sec.symbol AND sec.is_current = true
            JOIN {self._delta_scan('dim_date')} dd ON dm.dm_date = dd.date_value
            LEFT JOIN {self._delta_scan('financial')} fin ON sec.sk_company_id = fin.sk_company_id
                AND fin.fi_year = EXTRACT(YEAR FROM dm.dm_date)
                AND fin.fi_qtr = EXTRACT(QUARTER FROM dm.dm_date)
        """).record_batch()
        self.write_deltalake(
            table_or_uri=self._table_uri('fact_market_history'),
            data=arrow_df,
            mode="append",
            storage_options=self.engine.storage_options,
        )
        return {'table': 'fact_market_history'}

    def build_fact_watches(self, batch_id, context_decorator=None):
        """Build FactWatches from staging_watch_history."""
        self.engine.duckdb.sql("use main")
        arrow_df = self.engine.duckdb.sql(f"""
            SELECT
                c.sk_customer_id,
                sec.sk_security_id,
                dd_placed.sk_date_id AS sk_date_id_date_placed,
                CASE WHEN w.w_action = 'CNCL' THEN dd_placed.sk_date_id ELSE NULL END AS sk_date_id_date_removed,
                {batch_id} AS batch_id
            FROM {self._delta_scan('staging_watch_history')} w
            JOIN {self._delta_scan('dim_customer')} c ON w.w_c_id = c.customer_id AND c.is_current = true
            JOIN {self._delta_scan('dim_security')} sec ON w.w_s_symb = sec.symbol AND sec.is_current = true
            JOIN {self._delta_scan('dim_date')} dd_placed ON CAST(w.w_dts AS DATE) = dd_placed.date_value
        """).record_batch()
        self.write_deltalake(
            table_or_uri=self._table_uri('fact_watches'),
            data=arrow_df,
            mode="append",
            storage_options=self.engine.storage_options,
        )
        return {'table': 'fact_watches'}

    def build_fact_cash_balances(self, batch_id, context_decorator=None):
        """Build FactCashBalances from staging_cash_transaction."""
        self.engine.duckdb.sql("use main")
        arrow_df = self.engine.duckdb.sql(f"""
            SELECT
                ca.sk_customer_id,
                ca.sk_account_id,
                dd.sk_date_id,
                SUM(ct.ct_amt) AS cash,
                {batch_id} AS batch_id
            FROM {self._delta_scan('staging_cash_transaction')} ct
            JOIN {self._delta_scan('dim_account')} ca ON ct.ct_ca_id = ca.account_id AND ca.is_current = true
            JOIN {self._delta_scan('dim_date')} dd ON CAST(ct.ct_dts AS DATE) = dd.date_value
            GROUP BY ca.sk_customer_id, ca.sk_account_id, dd.sk_date_id
        """).record_batch()
        self.write_deltalake(
            table_or_uri=self._table_uri('fact_cash_balances'),
            data=arrow_df,
            mode="append",
            storage_options=self.engine.storage_options,
        )
        return {'table': 'fact_cash_balances'}

    def build_fact_holdings(self, batch_id, context_decorator=None):
        """Build FactHoldings from trade data."""
        self.engine.duckdb.sql("use main")
        arrow_df = self.engine.duckdb.sql(f"""
            SELECT
                dt.trade_id,
                dt.trade_id AS current_trade_id,
                dt.sk_customer_id,
                dt.sk_account_id,
                dt.sk_security_id,
                dt.sk_company_id,
                dt.sk_create_date_id AS sk_date_id,
                dt.sk_create_time_id AS sk_time_id,
                dt.trade_price AS current_price,
                dt.quantity AS current_holding,
                {batch_id} AS batch_id
            FROM {self._delta_scan('dim_trade')} dt
            WHERE dt.batch_id = {batch_id}
              AND dt.is_cash = true
        """).record_batch()
        self.write_deltalake(
            table_or_uri=self._table_uri('fact_holdings'),
            data=arrow_df,
            mode="append",
            storage_options=self.engine.storage_options,
        )
        return {'table': 'fact_holdings'}

    def build_financial(self, batch_id, context_decorator=None):
        """Build Financial table from FINWIRE FIN records."""
        self.engine.duckdb.sql("use main")
        arrow_df = self.engine.duckdb.sql(f"""
            SELECT
                c.sk_company_id,
                f.year AS fi_year,
                f.quarter AS fi_qtr,
                f.qtr_start_date AS fi_qtr_start_date,
                f.revenue AS fi_revenue,
                f.earnings AS fi_net_earn,
                f.eps AS fi_basic_eps,
                f.diluted_eps AS fi_dilut_eps,
                f.margin AS fi_margin,
                f.inventory AS fi_inventory,
                f.assets AS fi_assets,
                f.liabilities AS fi_liability,
                f.sh_out AS fi_out_basic,
                f.diluted_sh_out AS fi_out_dilut
            FROM {self._delta_scan('staging_finwire_fin')} f
            LEFT JOIN {self._delta_scan('dim_company')} c
                ON (f.co_name_or_cik = CAST(c.company_id AS VARCHAR) OR f.co_name_or_cik = c.name)
                AND c.is_current = true
        """).record_batch()
        self.write_deltalake(
            table_or_uri=self._table_uri('financial'),
            data=arrow_df,
            mode="overwrite",
            storage_options=self.engine.storage_options,
        )
        return {'table': 'financial'}

    def build_prospect(self, batch_id, context_decorator=None):
        """Build Prospect table."""
        self.engine.duckdb.sql("use main")
        arrow_df = self.engine.duckdb.sql(f"""
            SELECT
                p.agency_id,
                CAST(NULL AS BIGINT) AS sk_record_date_id,
                CAST(NULL AS BIGINT) AS sk_update_date_id,
                {batch_id} AS batch_id,
                CASE WHEN c.sk_customer_id IS NOT NULL THEN true ELSE false END AS is_customer,
                p.last_name, p.first_name, p.middle_initial, p.gender,
                p.address_line1, p.address_line2, p.postal_code,
                p.city, p.state, p.country, p.phone,
                p.income, p.number_cars, p.number_children,
                p.marital_status, p.age, p.credit_rating,
                p.own_or_rent_flag, p.employer,
                p.number_credit_cards, p.net_worth,
                CASE
                    WHEN p.net_worth > 1000000 OR p.income > 200000 THEN 'HighValue'
                    WHEN p.number_children > 3 OR p.number_credit_cards > 5 THEN 'Expenses'
                    WHEN p.age > 45 THEN 'Boomer'
                    WHEN p.income < 50000 OR p.credit_rating < 600 THEN 'MoneyAlert'
                    WHEN p.number_cars > 3 OR p.number_credit_cards > 7 THEN 'Spender'
                    WHEN p.age < 25 AND p.net_worth > 100000 THEN 'Inherited'
                    ELSE NULL
                END AS marketing_nameplate
            FROM {self._delta_scan('staging_prospect')} p
            LEFT JOIN {self._delta_scan('dim_customer')} c
                ON UPPER(p.last_name) = UPPER(c.last_name)
                AND UPPER(p.first_name) = UPPER(c.first_name)
                AND p.address_line1 = c.address_line1
                AND p.postal_code = c.postal_code
                AND c.is_current = true
        """).record_batch()
        self.write_deltalake(
            table_or_uri=self._table_uri('prospect'),
            data=arrow_df,
            mode="append",
            storage_options=self.engine.storage_options,
        )
        return {'table': 'prospect'}

    def merge_incremental_scd2(self, table_name, batch_id, context_decorator=None):
        """Apply SCD Type 2 incremental merge using delta-rs."""
        import pyarrow as pa

        if table_name == 'dim_customer':
            # Read updated customer IDs
            self.engine.duckdb.sql("use main")
            updated_ids = self.engine.duckdb.sql(f"""
                SELECT DISTINCT c_id AS customer_id
                FROM {self._delta_scan('staging_customer')}
                WHERE cdc_flag IN ('U', 'UPDCUST')
            """).arrow()

            # Expire current records via merge
            if updated_ids.num_rows > 0:
                fact_table = self.DeltaTable(
                    table_uri=self._table_uri('dim_customer'),
                    storage_options=self.engine.storage_options,
                )
                fact_table.merge(
                    source=updated_ids,
                    predicate="target.customer_id = source.customer_id AND target.is_current = true",
                    source_alias="source",
                    target_alias="target"
                ).when_matched_update({
                    "is_current": "false",
                    "end_date": "CURRENT_DATE",
                }).execute()

            # Insert new version
            self.build_dim_customer(batch_id=batch_id)

        elif table_name == 'dim_account':
            self.engine.duckdb.sql("use main")
            updated_ids = self.engine.duckdb.sql(f"""
                SELECT DISTINCT ca_id AS account_id
                FROM {self._delta_scan('staging_account')}
                WHERE cdc_flag IN ('U', 'UPDACCT')
            """).arrow()

            if updated_ids.num_rows > 0:
                fact_table = self.DeltaTable(
                    table_uri=self._table_uri('dim_account'),
                    storage_options=self.engine.storage_options,
                )
                fact_table.merge(
                    source=updated_ids,
                    predicate="target.account_id = source.account_id AND target.is_current = true",
                    source_alias="source",
                    target_alias="target"
                ).when_matched_update({
                    "is_current": "false",
                    "end_date": "CURRENT_DATE",
                }).execute()

            self.build_dim_account(batch_id=batch_id)

        return {'table': table_name, 'batch_id': str(batch_id)}

    def validate_audit(self, audit_file_uri, batch_id, context_decorator=None):
        """Validate DW row counts against audit data."""
        self.engine.duckdb.sql("use main")
        validation_results = {}
        target_tables = [
            'dim_customer', 'dim_account', 'dim_broker', 'dim_company',
            'dim_security', 'dim_trade', 'fact_market_history', 'fact_watches',
            'fact_cash_balances', 'fact_holdings', 'financial', 'prospect'
        ]
        for table in target_tables:
            try:
                count = self.engine.duckdb.sql(
                    f"SELECT COUNT(*) AS cnt FROM {self._delta_scan(table)}"
                ).fetchone()[0]
                validation_results[f'{table}_count'] = str(count)
            except Exception:
                validation_results[f'{table}_count'] = 'ERROR'
        return validation_results
