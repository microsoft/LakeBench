import pathlib
import posixpath
from typing import Optional

from ....engines.daft import Daft
from ....utils.path_utils import _REMOTE_SCHEMES, to_file_uri


class DaftClickBench:
    def __init__(self, engine: Daft):
        self.engine = engine

    def load_parquet_to_delta(
        self, parquet_folder_uri: str, table_name: str, table_is_precreated: bool = False, context_decorator: str = None
    ):
        daft = self.engine.daft
        df = daft.read_parquet(parquet_folder_uri)

        # Binary columns → string (ClickBench parquet omits logical string type on some columns)
        binary_cols = [f.name for f in df.schema() if f.dtype == daft.DataType.binary()]
        if binary_cols:
            df = df.with_columns({c: daft.col(c).cast(daft.DataType.string()) for c in binary_cols})

        # EventDate: integer (days since epoch) → Date
        df = df.with_columns({"EventDate": daft.col("EventDate").cast(daft.DataType.date())})

        # Timestamp columns: integer (seconds since epoch) → Timestamp(us)
        # Delta Lake requires microsecond precision; multiply seconds by 1_000_000.
        col_names = [f.name for f in df.schema()]
        for ts_col in ("EventTime", "ClientEventTime", "LocalEventTime"):
            if ts_col in col_names:
                df = df.with_columns(
                    {
                        ts_col: (daft.col(ts_col).cast(daft.DataType.int64()) * 1_000_000).cast(
                            daft.DataType.timestamp("us")
                        )
                    }
                )

        # Write delta — pre-create dir + to_file_uri (same pattern as Daft.load_parquet_to_delta)
        raw_path = posixpath.join(self.engine.schema_or_working_directory_uri, table_name)
        is_local = not any(raw_path.startswith(s) for s in _REMOTE_SCHEMES)
        if is_local:
            pathlib.Path(raw_path).mkdir(parents=True, exist_ok=True)
        df.write_deltalake(table=to_file_uri(raw_path), mode="append")

    def execute_sql_query(self, query: str, context_decorator: Optional[str] = None):
        return self.engine.execute_sql_query(query)
