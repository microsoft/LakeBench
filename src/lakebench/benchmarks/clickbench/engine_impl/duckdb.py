import posixpath
from typing import Optional

from ....engines.duckdb import DuckDB


class DuckDBClickBench:
    def __init__(self, engine: DuckDB):

        self.engine = engine

    def load_parquet_to_delta(
        self, parquet_folder_uri: str, table_name: str, table_is_precreated: bool = False, context_decorator: str = None
    ):
        """
        Loads the ClickBench parquet data into Delta format using Spark.

        Parameters
        ----------
        parquet_folder_uri : str
            Path to the source parquet files.
        """
        arrow_df = self.engine.duckdb.sql(f"""
            SELECT * REPLACE (make_date(EventDate) AS EventDate) 
            FROM parquet_scan('{posixpath.join(parquet_folder_uri, "*.parquet")}')
        """).record_batch()

        self.engine.deltars.write_deltalake(
            table_or_uri=posixpath.join(self.engine.schema_or_working_directory_uri, table_name),
            data=arrow_df,
            mode="append",
            storage_options=self.engine.storage_options,
        )

    def execute_sql_query(self, query: str, context_decorator: Optional[str] = None):
        return self.engine.execute_sql_query(query)
