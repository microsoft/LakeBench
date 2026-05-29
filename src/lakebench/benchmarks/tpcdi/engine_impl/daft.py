import pathlib
import posixpath

from ....engines.daft import Daft
from ....engines.delta_rs import DeltaRs
from ....utils.path_utils import _REMOTE_SCHEMES, to_file_uri


class DaftTPCDI:
    """Daft engine implementation for the TPC-DI benchmark."""

    def __init__(self, engine: Daft):
        self.engine = engine
        self.delta_rs = DeltaRs()
        self.DeltaTable = self.delta_rs.DeltaTable

    def _table_path(self, table_name):
        raw = posixpath.join(self.engine.schema_or_working_directory_uri, table_name)
        is_local = not any(raw.startswith(s) for s in _REMOTE_SCHEMES)
        return str(pathlib.Path(raw)) if is_local else raw

    def _read_delta(self, table_name):
        path = self._table_path(table_name)
        is_local = not any(path.startswith(s) for s in _REMOTE_SCHEMES)
        if is_local:
            from deltalake import DeltaTable

            file_uris = DeltaTable(path).file_uris()
            return self.engine.daft.read_parquet(file_uris)
        return self.engine.daft.read_deltalake(to_file_uri(path))

    def _write_delta(self, df, table_name, mode="overwrite"):
        path = self._table_path(table_name)
        is_local = not any(path.startswith(s) for s in _REMOTE_SCHEMES)
        if is_local:
            pathlib.Path(path).mkdir(parents=True, exist_ok=True)
        df.write_deltalake(table=to_file_uri(path), mode=mode)

    def load_source_file(self, file_uri, file_format, delimiter, table_name, context_decorator=None):
        """Load a delimited source file into staging."""
        daft = self.engine.daft
        if file_format in ("delimited", "csv"):
            has_header = file_format == "csv"
            df = daft.read_csv(file_uri, has_headers=has_header, delimiter=delimiter)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
        self._write_delta(df, table_name, mode="append")
        return {"table": table_name}

    def load_dim_date(self, file_uri, context_decorator=None):
        df = self.engine.daft.read_csv(file_uri, has_headers=False, delimiter="|")
        self._write_delta(df, "dim_date")
        return {"table": "dim_date"}

    def load_dim_time(self, file_uri, context_decorator=None):
        df = self.engine.daft.read_csv(file_uri, has_headers=False, delimiter="|")
        self._write_delta(df, "dim_time")
        return {"table": "dim_time"}

    def parse_customer_mgmt_xml(self, file_uri, context_decorator=None):
        """Parse CustomerMgmt.xml using lxml."""
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
                        {"cdc_flag": action_type, "cdc_dsn": dsn, "c_id": int(c_id) if c_id else None}
                    )
                    acct = customer.find(".//Account")
                    if acct is not None:
                        account_records.append(
                            {
                                "cdc_flag": action_type,
                                "cdc_dsn": dsn,
                                "ca_id": int(acct.get("CA_ID")) if acct.get("CA_ID") else None,
                                "ca_c_id": int(c_id) if c_id else None,
                            }
                        )

        if customer_records:
            self.delta_rs.write_deltalake(
                self._table_path("staging_customer"), pa.Table.from_pylist(customer_records), mode="append"
            )
        if account_records:
            self.delta_rs.write_deltalake(
                self._table_path("staging_account"), pa.Table.from_pylist(account_records), mode="append"
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
                self.delta_rs.write_deltalake(
                    self._table_path(table_name), pa.Table.from_pylist(records), mode="append"
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
        df = self._read_delta(staging_map[dim_table])
        self._write_delta(df, dim_table)
        return {"table": dim_table}

    def build_dim_broker(self, batch_id, context_decorator=None):
        daft = self.engine.daft
        df = (
            self._read_delta("staging_hr")
            .where(daft.col("employee_job_code") == "314")
            .with_columns(
                {
                    "is_current": daft.lit(True),
                    "batch_id": daft.lit(batch_id),
                }
            )
        )
        self._write_delta(df, "dim_broker")
        return {"table": "dim_broker"}

    def build_dim_company(self, batch_id, context_decorator=None):
        daft = self.engine.daft
        df = self._read_delta("staging_finwire_cmp").with_columns(
            {
                "is_current": daft.lit(True),
                "batch_id": daft.lit(batch_id),
                "is_low_grade": ~(
                    daft.col("sp_rating").str.starts_with("A") | daft.col("sp_rating").str.starts_with("BBB")
                ),
            }
        )
        self._write_delta(df, "dim_company")
        return {"table": "dim_company"}

    def build_dim_security(self, batch_id, context_decorator=None):
        daft = self.engine.daft
        df = self._read_delta("staging_finwire_sec").with_columns(
            {"is_current": daft.lit(True), "batch_id": daft.lit(batch_id)}
        )
        self._write_delta(df, "dim_security")
        return {"table": "dim_security"}

    def build_dim_customer(self, batch_id, context_decorator=None):
        daft = self.engine.daft
        df = (
            self._read_delta("staging_customer")
            .where(daft.col("cdc_flag").is_in(["I", "NEW"]))
            .with_columns({"is_current": daft.lit(True), "batch_id": daft.lit(batch_id)})
        )
        mode = "overwrite" if batch_id == 1 else "append"
        self._write_delta(df, "dim_customer", mode=mode)
        return {"table": "dim_customer"}

    def build_dim_account(self, batch_id, context_decorator=None):
        daft = self.engine.daft
        df = (
            self._read_delta("staging_account")
            .where(daft.col("cdc_flag").is_in(["I", "NEW"]))
            .with_columns({"is_current": daft.lit(True), "batch_id": daft.lit(batch_id)})
        )
        mode = "overwrite" if batch_id == 1 else "append"
        self._write_delta(df, "dim_account", mode=mode)
        return {"table": "dim_account"}

    def build_dim_trade(self, batch_id, context_decorator=None):
        daft = self.engine.daft
        df = self._read_delta("staging_trade").with_columns(
            {
                "is_cash": daft.col("t_is_cash") == 1,
                "batch_id": daft.lit(batch_id),
            }
        )
        self._write_delta(df, "dim_trade", mode="append")
        return {"table": "dim_trade"}

    def build_fact_market_history(self, batch_id, context_decorator=None):
        daft = self.engine.daft
        dm = self._read_delta("staging_daily_market")
        sec = self._read_delta("dim_security").where(daft.col("is_current") == True)
        dd = self._read_delta("dim_date")
        df = (
            dm.join(sec, left_on="dm_s_symb", right_on="symbol")
            .join(dd, left_on="dm_date", right_on="date_value")
            .select(
                "sk_security_id",
                "sk_company_id",
                "sk_date_id",
                daft.col("dm_close").alias("close_price"),
                daft.col("dm_high").alias("day_high"),
                daft.col("dm_low").alias("day_low"),
                daft.col("dm_vol").alias("volume"),
            )
            .with_columns({"batch_id": daft.lit(batch_id)})
        )
        self._write_delta(df, "fact_market_history", mode="append")
        return {"table": "fact_market_history"}

    def build_fact_watches(self, batch_id, context_decorator=None):
        daft = self.engine.daft
        w = self._read_delta("staging_watch_history")
        c = self._read_delta("dim_customer").where(daft.col("is_current") == True)
        sec = self._read_delta("dim_security").where(daft.col("is_current") == True)
        df = (
            w.join(c, left_on="w_c_id", right_on="customer_id")
            .join(sec, left_on="w_s_symb", right_on="symbol")
            .select("sk_customer_id", "sk_security_id")
            .with_columns({"batch_id": daft.lit(batch_id)})
        )
        self._write_delta(df, "fact_watches", mode="append")
        return {"table": "fact_watches"}

    def build_fact_cash_balances(self, batch_id, context_decorator=None):
        daft = self.engine.daft
        ct = self._read_delta("staging_cash_transaction")
        ca = self._read_delta("dim_account").where(daft.col("is_current") == True)
        df = (
            ct.join(ca, left_on="ct_ca_id", right_on="account_id")
            .groupby("sk_customer_id", "sk_account_id")
            .agg(daft.col("ct_amt").sum().alias("cash"))
            .with_columns({"batch_id": daft.lit(batch_id)})
        )
        self._write_delta(df, "fact_cash_balances", mode="append")
        return {"table": "fact_cash_balances"}

    def build_fact_holdings(self, batch_id, context_decorator=None):
        daft = self.engine.daft
        dt = self._read_delta("dim_trade").where((daft.col("batch_id") == batch_id) & (daft.col("is_cash") == True))
        self._write_delta(dt, "fact_holdings", mode="append")
        return {"table": "fact_holdings"}

    def build_financial(self, batch_id, context_decorator=None):
        df = self._read_delta("staging_finwire_fin")
        self._write_delta(df, "financial")
        return {"table": "financial"}

    def build_prospect(self, batch_id, context_decorator=None):
        daft = self.engine.daft
        df = self._read_delta("staging_prospect").with_columns({"batch_id": daft.lit(batch_id)})
        self._write_delta(df, "prospect", mode="append")
        return {"table": "prospect"}

    def merge_incremental_scd2(self, table_name, batch_id, context_decorator=None):
        """Apply SCD Type 2 merge using delta-rs."""

        if table_name == "dim_customer":
            updated = (
                self._read_delta("staging_customer")
                .where(self.engine.daft.col("cdc_flag").is_in(["U", "UPDCUST"]))
                .select("c_id")
                .to_arrow()
            )
            if len(updated) > 0:
                table = self.DeltaTable(self._table_path("dim_customer"))
                table.merge(
                    source=updated,
                    predicate="target.customer_id = source.c_id AND target.is_current = true",
                    source_alias="source",
                    target_alias="target",
                ).when_matched_update({"is_current": "false"}).execute()
            self.build_dim_customer(batch_id=batch_id)
        elif table_name == "dim_account":
            updated = (
                self._read_delta("staging_account")
                .where(self.engine.daft.col("cdc_flag").is_in(["U", "UPDACCT"]))
                .select("ca_id")
                .to_arrow()
            )
            if len(updated) > 0:
                table = self.DeltaTable(self._table_path("dim_account"))
                table.merge(
                    source=updated,
                    predicate="target.account_id = source.ca_id AND target.is_current = true",
                    source_alias="source",
                    target_alias="target",
                ).when_matched_update({"is_current": "false"}).execute()
            self.build_dim_account(batch_id=batch_id)
        return {"table": table_name, "batch_id": str(batch_id)}

    def validate_audit(self, audit_file_uri, batch_id, context_decorator=None):
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
                df = self._read_delta(table).collect()
                validation_results[f"{table}_count"] = str(len(df))
            except Exception:
                validation_results[f"{table}_count"] = "ERROR"
        return validation_results
