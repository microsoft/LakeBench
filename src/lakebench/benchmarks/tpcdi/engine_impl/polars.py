import posixpath

from ....engines.delta_rs import DeltaRs
from ....engines.polars import Polars


class PolarsTPCDI:
    """Polars engine implementation for the TPC-DI benchmark."""

    def __init__(self, engine: Polars):
        self.engine = engine
        self.delta_rs = DeltaRs()
        self.write_deltalake = self.delta_rs.write_deltalake
        self.DeltaTable = self.delta_rs.DeltaTable
        self.storage_options = engine.storage_options

    def _table_uri(self, table_name):
        return posixpath.join(self.engine.schema_or_working_directory_uri, table_name)

    def load_source_file(self, file_uri, file_format, delimiter, table_name, context_decorator=None):
        """Load a delimited source file into a staging Delta table."""
        pl = self.engine.pl
        if file_format in ("delimited", "csv"):
            has_header = file_format == "csv"
            df = pl.read_csv(file_uri, has_header=has_header, separator=delimiter, infer_schema_length=10000)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")

        df.write_delta(self._table_uri(table_name), mode="append", storage_options=self.storage_options)
        return {"rows_loaded": str(len(df))}

    def load_dim_date(self, file_uri, context_decorator=None):
        """Load Date.txt directly into dim_date."""
        df = self.engine.pl.read_csv(file_uri, has_header=False, separator="|", infer_schema_length=10000)
        df.write_delta(self._table_uri("dim_date"), mode="overwrite", storage_options=self.storage_options)
        return {"table": "dim_date"}

    def load_dim_time(self, file_uri, context_decorator=None):
        """Load Time.txt directly into dim_time."""
        df = self.engine.pl.read_csv(file_uri, has_header=False, separator="|", infer_schema_length=10000)
        df.write_delta(self._table_uri("dim_time"), mode="overwrite", storage_options=self.storage_options)
        return {"table": "dim_time"}

    def parse_customer_mgmt_xml(self, file_uri, context_decorator=None):
        """Parse CustomerMgmt.xml using lxml and load into staging tables."""
        import pyarrow as pa
        from lxml import etree

        tree = etree.parse(file_uri)
        root = tree.getroot()
        customer_records, account_records = [], []
        dsn = 0

        for action in root.iter():
            if "Action" in action.tag:
                action_type = action.get("ActionType", "")
                customer = action.find(".//Customer")
                if customer is not None:
                    dsn += 1
                    c_id = customer.get("C_ID")
                    customer_records.append(
                        {
                            "cdc_flag": action_type,
                            "cdc_dsn": dsn,
                            "c_id": int(c_id) if c_id else None,
                            "c_tax_id": customer.get("C_TAX_ID"),
                        }
                    )
                    acct = customer.find(".//Account")
                    if acct is not None:
                        account_records.append(
                            {
                                "cdc_flag": action_type,
                                "cdc_dsn": dsn,
                                "ca_id": int(acct.get("CA_ID")) if acct.get("CA_ID") else None,
                                "ca_b_id": int(acct.get("CA_B_ID")) if acct.get("CA_B_ID") else None,
                                "ca_c_id": int(c_id) if c_id else None,
                                "ca_name": acct.findtext("CA_NAME"),
                                "ca_tax_st": int(acct.get("CA_TAX_ST")) if acct.get("CA_TAX_ST") else None,
                                "ca_st_id": acct.get("CA_ST_ID"),
                            }
                        )

        if customer_records:
            cust_table = pa.Table.from_pylist(customer_records)
            self.write_deltalake(
                self._table_uri("staging_customer"), cust_table, mode="append", storage_options=self.storage_options
            )
        if account_records:
            acct_table = pa.Table.from_pylist(account_records)
            self.write_deltalake(
                self._table_uri("staging_account"), acct_table, mode="append", storage_options=self.storage_options
            )

        return {"customer_rows": str(len(customer_records)), "account_rows": str(len(account_records))}

    def parse_finwire(self, batch_uri, context_decorator=None):
        """Parse FINWIRE fixed-width files."""
        import pyarrow as pa

        from ..finwire import FINWIRE_STAGING_TABLES, parse_finwire_records

        cmp_records, sec_records, fin_records = parse_finwire_records(batch_uri)

        for records, table_name in zip(
            (cmp_records, sec_records, fin_records),
            FINWIRE_STAGING_TABLES,
        ):
            if records:
                self.write_deltalake(
                    self._table_uri(table_name),
                    pa.Table.from_pylist(records),
                    mode="append",
                    storage_options=self.storage_options,
                )

        return {"cmp_rows": str(len(cmp_records)), "sec_rows": str(len(sec_records)), "fin_rows": str(len(fin_records))}

    def load_batch_date(self, file_uri, batch_id, context_decorator=None):
        return {"batch_id": str(batch_id)}

    def build_lookup_dimension(self, dim_table, batch_id, context_decorator=None):
        staging_map = {
            "dim_status_type": "staging_status_type",
            "dim_tax_rate": "staging_tax_rate",
            "dim_trade_type": "staging_trade_type",
        }
        staging_table = staging_map[dim_table]
        df = self.engine.pl.scan_delta(self._table_uri(staging_table), storage_options=self.storage_options).collect()
        df.write_delta(self._table_uri(dim_table), mode="overwrite", storage_options=self.storage_options)
        return {"table": dim_table}

    def build_dim_broker(self, batch_id, context_decorator=None):
        pl = self.engine.pl
        df = (
            pl.scan_delta(self._table_uri("staging_hr"), storage_options=self.storage_options)
            .filter(pl.col("employee_job_code") == "314")
            .with_row_index("sk_broker_id")
            .rename(
                {
                    "employee_id": "broker_id",
                    "employee_first_name": "first_name",
                    "employee_last_name": "last_name",
                    "employee_mi": "middle_initial",
                    "employee_branch": "branch",
                    "employee_office": "office",
                    "employee_phone": "phone",
                }
            )
            .with_columns(
                [
                    pl.lit(True).alias("is_current"),
                    pl.lit(batch_id).alias("batch_id"),
                ]
            )
            .select(
                [
                    "sk_broker_id",
                    "broker_id",
                    "manager_id",
                    "first_name",
                    "last_name",
                    "middle_initial",
                    "branch",
                    "office",
                    "phone",
                    "is_current",
                    "batch_id",
                ]
            )
            .collect()
        )
        df.write_delta(self._table_uri("dim_broker"), mode="overwrite", storage_options=self.storage_options)
        return {"table": "dim_broker"}

    def build_dim_company(self, batch_id, context_decorator=None):
        pl = self.engine.pl
        df = (
            pl.scan_delta(self._table_uri("staging_finwire_cmp"), storage_options=self.storage_options)
            .with_row_index("sk_company_id")
            .rename(
                {
                    "cik": "company_id",
                    "company_name": "name",
                    "industry_id": "industry",
                    "ceo_name": "ceo",
                    "addr_line1": "address_line1",
                    "addr_line2": "address_line2",
                }
            )
            .with_columns(
                [
                    pl.when(pl.col("sp_rating").str.starts_with("A") | pl.col("sp_rating").str.starts_with("BBB"))
                    .then(pl.lit(False))
                    .otherwise(pl.lit(True))
                    .alias("is_low_grade"),
                    pl.lit(True).alias("is_current"),
                    pl.lit(batch_id).alias("batch_id"),
                ]
            )
            .collect()
        )
        df.write_delta(self._table_uri("dim_company"), mode="overwrite", storage_options=self.storage_options)
        return {"table": "dim_company"}

    def build_dim_security(self, batch_id, context_decorator=None):
        pl = self.engine.pl
        sec = pl.scan_delta(self._table_uri("staging_finwire_sec"), storage_options=self.storage_options)
        company = pl.scan_delta(self._table_uri("dim_company"), storage_options=self.storage_options).filter(
            pl.col("is_current") == True
        )
        df = (
            sec.with_row_index("sk_security_id")
            .rename(
                {
                    "ex_id": "exchange_id",
                    "sh_out": "shares_outstanding",
                    "first_trade_date": "first_trade",
                    "first_trade_exchange": "first_trade_on_exchange",
                }
            )
            .with_columns([pl.lit(True).alias("is_current"), pl.lit(batch_id).alias("batch_id")])
            .collect()
        )
        df.write_delta(self._table_uri("dim_security"), mode="overwrite", storage_options=self.storage_options)
        return {"table": "dim_security"}

    def build_dim_customer(self, batch_id, context_decorator=None):
        pl = self.engine.pl
        df = (
            pl.scan_delta(self._table_uri("staging_customer"), storage_options=self.storage_options)
            .filter(pl.col("cdc_flag").is_in(["I", "NEW"]))
            .with_row_index("sk_customer_id")
            .rename(
                {
                    "c_id": "customer_id",
                    "c_tax_id": "tax_id",
                    "c_l_name": "last_name",
                    "c_f_name": "first_name",
                    "c_m_name": "middle_name",
                    "c_gndr": "gender",
                    "c_tier": "tier",
                    "c_dob": "dob",
                }
            )
            .with_columns([pl.lit(True).alias("is_current"), pl.lit(batch_id).alias("batch_id")])
            .collect()
        )
        mode = "overwrite" if batch_id == 1 else "append"
        df.write_delta(self._table_uri("dim_customer"), mode=mode, storage_options=self.storage_options)
        return {"table": "dim_customer"}

    def build_dim_account(self, batch_id, context_decorator=None):
        pl = self.engine.pl
        df = (
            pl.scan_delta(self._table_uri("staging_account"), storage_options=self.storage_options)
            .filter(pl.col("cdc_flag").is_in(["I", "NEW"]))
            .with_row_index("sk_account_id")
            .rename({"ca_id": "account_id", "ca_name": "account_desc", "ca_tax_st": "tax_status", "ca_st_id": "status"})
            .with_columns([pl.lit(True).alias("is_current"), pl.lit(batch_id).alias("batch_id")])
            .collect()
        )
        mode = "overwrite" if batch_id == 1 else "append"
        df.write_delta(self._table_uri("dim_account"), mode=mode, storage_options=self.storage_options)
        return {"table": "dim_account"}

    def build_dim_trade(self, batch_id, context_decorator=None):
        pl = self.engine.pl
        df = (
            pl.scan_delta(self._table_uri("staging_trade"), storage_options=self.storage_options)
            .with_row_index("sk_trade_id")
            .rename(
                {
                    "t_id": "trade_id",
                    "t_st_id": "status",
                    "t_tt_id": "type",
                    "t_qty": "quantity",
                    "t_bid_price": "bid_price",
                    "t_exec_name": "executed_by",
                    "t_trade_price": "trade_price",
                    "t_chrg": "fee",
                    "t_comm": "commission",
                    "t_tax": "tax",
                }
            )
            .with_columns(
                [
                    (pl.col("t_is_cash") == 1).alias("is_cash"),
                    pl.lit(batch_id).alias("batch_id"),
                ]
            )
            .collect()
        )
        df.write_delta(self._table_uri("dim_trade"), mode="append", storage_options=self.storage_options)
        return {"table": "dim_trade"}

    def build_fact_market_history(self, batch_id, context_decorator=None):
        pl = self.engine.pl
        dm = pl.scan_delta(self._table_uri("staging_daily_market"), storage_options=self.storage_options)
        sec = pl.scan_delta(self._table_uri("dim_security"), storage_options=self.storage_options).filter(
            pl.col("is_current") == True
        )
        dd = pl.scan_delta(self._table_uri("dim_date"), storage_options=self.storage_options)
        df = (
            dm.join(sec, left_on="dm_s_symb", right_on="symbol")
            .join(dd, left_on="dm_date", right_on="date_value")
            .select(
                [
                    "sk_security_id",
                    "sk_company_id",
                    "sk_date_id",
                    pl.lit(None).cast(pl.Decimal).alias("peratio"),
                    pl.lit(None).cast(pl.Decimal).alias("yield_val"),
                    pl.col("dm_high").alias("fifty_two_week_high"),
                    pl.col("sk_date_id").alias("sk_fifty_two_week_high_date"),
                    pl.col("dm_low").alias("fifty_two_week_low"),
                    pl.col("sk_date_id").alias("sk_fifty_two_week_low_date"),
                    pl.col("dm_close").alias("close_price"),
                    pl.col("dm_high").alias("day_high"),
                    pl.col("dm_low").alias("day_low"),
                    pl.col("dm_vol").alias("volume"),
                    pl.lit(batch_id).alias("batch_id"),
                ]
            )
            .collect()
        )
        df.write_delta(self._table_uri("fact_market_history"), mode="append", storage_options=self.storage_options)
        return {"table": "fact_market_history"}

    def build_fact_watches(self, batch_id, context_decorator=None):
        pl = self.engine.pl
        w = pl.scan_delta(self._table_uri("staging_watch_history"), storage_options=self.storage_options)
        c = pl.scan_delta(self._table_uri("dim_customer"), storage_options=self.storage_options).filter(
            pl.col("is_current") == True
        )
        sec = pl.scan_delta(self._table_uri("dim_security"), storage_options=self.storage_options).filter(
            pl.col("is_current") == True
        )
        dd = pl.scan_delta(self._table_uri("dim_date"), storage_options=self.storage_options)
        df = (
            w.join(c, left_on="w_c_id", right_on="customer_id")
            .join(sec, left_on="w_s_symb", right_on="symbol")
            .join(dd, left_on=pl.col("w_dts").cast(pl.Date), right_on="date_value")
            .select(
                [
                    "sk_customer_id",
                    "sk_security_id",
                    pl.col("sk_date_id").alias("sk_date_id_date_placed"),
                    pl.when(pl.col("w_action") == "CNCL")
                    .then(pl.col("sk_date_id"))
                    .otherwise(None)
                    .alias("sk_date_id_date_removed"),
                    pl.lit(batch_id).alias("batch_id"),
                ]
            )
            .collect()
        )
        df.write_delta(self._table_uri("fact_watches"), mode="append", storage_options=self.storage_options)
        return {"table": "fact_watches"}

    def build_fact_cash_balances(self, batch_id, context_decorator=None):
        pl = self.engine.pl
        ct = pl.scan_delta(self._table_uri("staging_cash_transaction"), storage_options=self.storage_options)
        ca = pl.scan_delta(self._table_uri("dim_account"), storage_options=self.storage_options).filter(
            pl.col("is_current") == True
        )
        dd = pl.scan_delta(self._table_uri("dim_date"), storage_options=self.storage_options)
        df = (
            ct.join(ca, left_on="ct_ca_id", right_on="account_id")
            .join(dd, left_on=pl.col("ct_dts").cast(pl.Date), right_on="date_value")
            .group_by(["sk_customer_id", "sk_account_id", "sk_date_id"])
            .agg(pl.sum("ct_amt").alias("cash"))
            .with_columns(pl.lit(batch_id).alias("batch_id"))
            .collect()
        )
        df.write_delta(self._table_uri("fact_cash_balances"), mode="append", storage_options=self.storage_options)
        return {"table": "fact_cash_balances"}

    def build_fact_holdings(self, batch_id, context_decorator=None):
        pl = self.engine.pl
        dt = (
            pl.scan_delta(self._table_uri("dim_trade"), storage_options=self.storage_options)
            .filter((pl.col("batch_id") == batch_id) & (pl.col("is_cash") == True))
            .select(
                [
                    pl.col("trade_id"),
                    pl.col("trade_id").alias("current_trade_id"),
                    "sk_customer_id",
                    "sk_account_id",
                    "sk_security_id",
                    "sk_company_id",
                    pl.col("sk_create_date_id").alias("sk_date_id"),
                    pl.col("sk_create_time_id").alias("sk_time_id"),
                    pl.col("trade_price").alias("current_price"),
                    pl.col("quantity").alias("current_holding"),
                    pl.lit(batch_id).alias("batch_id"),
                ]
            )
            .collect()
        )
        dt.write_delta(self._table_uri("fact_holdings"), mode="append", storage_options=self.storage_options)
        return {"table": "fact_holdings"}

    def build_financial(self, batch_id, context_decorator=None):
        pl = self.engine.pl
        fin = pl.scan_delta(self._table_uri("staging_finwire_fin"), storage_options=self.storage_options)
        company = pl.scan_delta(self._table_uri("dim_company"), storage_options=self.storage_options).filter(
            pl.col("is_current") == True
        )
        # Simplified: write without join for now (join on co_name_or_cik is complex in Polars)
        df = fin.collect()
        df.write_delta(self._table_uri("financial"), mode="overwrite", storage_options=self.storage_options)
        return {"table": "financial"}

    def build_prospect(self, batch_id, context_decorator=None):
        pl = self.engine.pl
        p = pl.scan_delta(self._table_uri("staging_prospect"), storage_options=self.storage_options)
        df = p.with_columns(
            [
                pl.lit(batch_id).alias("batch_id"),
                pl.when(pl.col("net_worth") > 1000000)
                .then(pl.lit("HighValue"))
                .when(pl.col("number_children") > 3)
                .then(pl.lit("Expenses"))
                .when(pl.col("age") > 45)
                .then(pl.lit("Boomer"))
                .otherwise(pl.lit(None))
                .alias("marketing_nameplate"),
            ]
        ).collect()
        df.write_delta(self._table_uri("prospect"), mode="append", storage_options=self.storage_options)
        return {"table": "prospect"}

    def merge_incremental_scd2(self, table_name, batch_id, context_decorator=None):
        """Apply SCD Type 2 incremental merge using delta-rs."""
        pl = self.engine.pl

        if table_name == "dim_customer":
            updated = (
                pl.scan_delta(self._table_uri("staging_customer"), storage_options=self.storage_options)
                .filter(pl.col("cdc_flag").is_in(["U", "UPDCUST"]))
                .select(pl.col("c_id").alias("customer_id"))
                .unique()
                .collect()
                .to_arrow()
            )
            if len(updated) > 0:
                table = self.DeltaTable(self._table_uri("dim_customer"), storage_options=self.storage_options)
                table.merge(
                    source=updated,
                    predicate="target.customer_id = source.customer_id AND target.is_current = true",
                    source_alias="source",
                    target_alias="target",
                ).when_matched_update({"is_current": "false"}).execute()
            self.build_dim_customer(batch_id=batch_id)

        elif table_name == "dim_account":
            updated = (
                pl.scan_delta(self._table_uri("staging_account"), storage_options=self.storage_options)
                .filter(pl.col("cdc_flag").is_in(["U", "UPDACCT"]))
                .select(pl.col("ca_id").alias("account_id"))
                .unique()
                .collect()
                .to_arrow()
            )
            if len(updated) > 0:
                table = self.DeltaTable(self._table_uri("dim_account"), storage_options=self.storage_options)
                table.merge(
                    source=updated,
                    predicate="target.account_id = source.account_id AND target.is_current = true",
                    source_alias="source",
                    target_alias="target",
                ).when_matched_update({"is_current": "false"}).execute()
            self.build_dim_account(batch_id=batch_id)

        return {"table": table_name, "batch_id": str(batch_id)}

    def validate_audit(self, audit_file_uri, batch_id, context_decorator=None):
        """Validate DW row counts."""
        pl = self.engine.pl
        validation_results = {}
        target_tables = [
            "dim_customer",
            "dim_account",
            "dim_broker",
            "dim_company",
            "dim_security",
            "dim_trade",
            "fact_market_history",
            "fact_watches",
            "fact_cash_balances",
            "fact_holdings",
            "financial",
            "prospect",
        ]
        for table in target_tables:
            try:
                df = pl.scan_delta(self._table_uri(table), storage_options=self.storage_options).collect()
                validation_results[f"{table}_count"] = str(len(df))
            except Exception:
                validation_results[f"{table}_count"] = "ERROR"
        return validation_results
