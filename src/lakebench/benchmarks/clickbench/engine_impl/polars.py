import posixpath
from typing import Optional

from ....engines.polars import Polars


class PolarsClickBench:
    def __init__(self, engine: Polars):
        self.engine = engine

    def load_parquet_to_delta(
        self, parquet_folder_uri: str, table_name: str, table_is_precreated: bool = False, context_decorator: str = None
    ):
        pl = self.engine.pl
        df = pl.read_parquet(posixpath.join(parquet_folder_uri, "*.parquet"))

        # Binary columns → Utf8 (ClickBench parquet omits logical string type on some columns)
        binary_cols = [name for name, dtype in zip(df.columns, df.dtypes) if dtype == pl.Binary]
        if binary_cols:
            df = df.with_columns([pl.col(c).cast(pl.Utf8) for c in binary_cols])

        # EventDate: integer (days since epoch) → Date
        df = df.with_columns(pl.from_epoch("EventDate", time_unit="d"))

        # Timestamp columns: integer (seconds since epoch) → Datetime
        for ts_col in ("EventTime", "ClientEventTime", "LocalEventTime"):
            if ts_col in df.columns:
                df = df.with_columns(pl.from_epoch(ts_col, time_unit="s"))

        df.write_delta(
            posixpath.join(self.engine.schema_or_working_directory_uri, table_name),
            mode="append",
            storage_options=self.engine.storage_options,
        )

    def execute_sql_query(self, query: str, context_decorator: Optional[str] = None):
        return self.engine.execute_sql_query(query)
