from .spark import Spark
from .base import BaseEngine
from typing import Optional
import logging
import os
import re
import subprocess
import sys

logger = logging.getLogger("lakebench.engines.databricks")


class Databricks(Spark):
    """
    Databricks Engine — connects to a Databricks cluster via Databricks Connect.

    Uses the databricks-connect package to establish a remote SparkSession
    against a Databricks cluster. All Spark-based benchmark implementations
    work automatically since this inherits from Spark.

    Requires: databricks-connect>=14.0

    Parameters
    ----------
    host : str
        Databricks workspace URL (e.g., 'https://xxx.cloud.databricks.com').
    cluster_id : str
        Databricks cluster ID.
    auth : str, default 'token'
        Authentication method: 'token' (PAT from env var) or 'az' (Azure CLI).
    token_env : str, default 'DATABRICKS_TOKEN'
        Name of the environment variable containing the Databricks PAT.
        The token is never stored directly — only the env var name is kept.
        Only used when auth='token'.
    schema_name : str
        The name of the schema (database) to use.
    catalog_name : str, optional
        The name of the catalog (Unity Catalog) to use.
    schema_uri : str, optional
        The URI of the schema.
    spark_measure_telemetry : bool, default False
        Whether to enable sparkmeasure telemetry.
    cost_per_vcore_hour : float, optional
        Cost per vCore hour for cost estimation.
    compute_stats_all_cols : bool, default False
        Whether to compute statistics for all columns after loading.
    use_temp_views : bool, default False
        Use createOrReplaceTempView instead of saveAsTable for loading.
        Avoids strict schema enforcement / cast errors with external parquet data.
    auto_align_connect_version : bool, default True
        On version-mismatch errors from databricks-connect, query the cluster's
        DBR version via the REST API, pip-install the matching
        `databricks-connect` into the current interpreter, and retry once.
    """

    SUPPORTS_SCHEMA_PREP = True

    def __init__(
        self,
        host: str,
        cluster_id: str,
        schema_name: str,
        auth: str = "token",
        token_env: str = "DATABRICKS_TOKEN",
        catalog_name: Optional[str] = None,
        schema_uri: Optional[str] = None,
        spark_measure_telemetry: bool = False,
        cost_per_vcore_hour: Optional[float] = None,
        compute_stats_all_cols: bool = False,
        use_temp_views: bool = False,
        auto_align_connect_version: bool = True,
    ):
        if auth == "az":
            token = self._get_az_token()
        else:
            token = os.environ.get(token_env)
            if not token:
                raise EnvironmentError(
                    f"Environment variable '{token_env}' is not set. "
                    f"Set it to your Databricks personal access token."
                )

        # Build session via Databricks Connect, with one retry on version mismatch.
        spark_session = self._build_session(
            host, token, cluster_id, auto_align_connect_version
        )
        from databricks.connect import DatabricksSession  # noqa: F401  (verify import after possible reinstall)
        import pyspark.sql.functions as sf

        # Call BaseEngine.__init__ directly (skip Spark's local session creation)
        BaseEngine.__init__(self, schema_or_working_directory_uri=schema_uri)
        self.sf = sf
        self.spark = spark_session

        self.schema_uri = schema_uri
        self._host = host
        self._cluster_id = cluster_id

        if spark_measure_telemetry:
            try:
                from sparkmeasure import StageMetrics
                self.capture_metrics = StageMetrics(self.spark)
            except ModuleNotFoundError:
                raise ModuleNotFoundError(
                    "`sparkmeasure` is not installed. Install with: "
                    "`pip install lakebench[sparkmeasure]`."
                )
        self.spark_measure_telemetry = spark_measure_telemetry

        self.version = f"databricks ({host})"

        self.catalog_name = catalog_name
        self.schema_name = schema_name
        self.full_catalog_schema_reference = (
            f"`{self.catalog_name}`.`{self.schema_name}`" if catalog_name
            else f"`{self.schema_name}`"
        )
        self.cost_per_vcore_hour = cost_per_vcore_hour
        self.compute_stats_all_cols = compute_stats_all_cols
        self.run_analyze_after_load = self.compute_stats_all_cols
        self.spark_configs = {}
        self.extended_engine_metadata.update({
            "databricks_host": host,
            "databricks_cluster_id": cluster_id,
        })
        self._use_temp_views = use_temp_views
        if use_temp_views:
            self.SUPPORTS_SCHEMA_PREP = False
            # Don't qualify table names — temp views are session-scoped
            self.catalog_name = None
            self.schema_name = None
            self.full_catalog_schema_reference = None

    def load_parquet_to_delta(self, parquet_folder_uri: str, table_name: str, table_is_precreated: bool = False, context_decorator: Optional[str] = None):
        """Load parquet data. Uses temp views when use_temp_views=True to avoid cast errors."""
        df = self.spark.read.parquet(parquet_folder_uri)
        if self._use_temp_views:
            df.createOrReplaceTempView(table_name)
        elif table_is_precreated:
            df.write.insertInto(table_name, overwrite=True)
        else:
            df.write.format('delta').mode("append").saveAsTable(table_name)

        if self.run_analyze_after_load and not self._use_temp_views:
            self.spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS FOR ALL COLUMNS;")

    def get_table_columns(self, table_name: str) -> list:
        """Return column names. Uses unqualified name for temp views."""
        if self._use_temp_views:
            return [f.name for f in self.spark.table(table_name).schema.fields]
        return super().get_table_columns(table_name)

    def get_total_cores(self):
        """Databricks Connect doesn't support sparkContext; return local cpu_count."""
        import os
        return os.cpu_count() or 1

    def get_compute_size(self):
        """Databricks Connect doesn't support sparkContext; return basic info."""
        return f"Databricks ({self._cluster_id})"

    @staticmethod
    def _get_az_token() -> str:
        """Get a Databricks AAD token via Azure CLI."""
        import subprocess
        # Resource ID for Azure Databricks
        result = subprocess.run(
            ["az", "account", "get-access-token",
             "--resource", "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d",
             "--query", "accessToken", "-o", "tsv"],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"Failed to get Databricks token via 'az' CLI: {result.stderr.strip()}\n"
                f"Make sure you are logged in with 'az login'."
            )
        return result.stdout.strip()

    # ------------------------------------------------------------------
    # Dynamic databricks-connect version alignment
    # ------------------------------------------------------------------

    # Pattern that matches the runtime's complaint, e.g.:
    #   "Unsupported combination of Databricks Runtime & Databricks Connect versions:
    #    14.3 (Databricks Runtime) < 16.1.7 (Databricks Connect)."
    _VERSION_MISMATCH_RE = re.compile(
        r"Databricks\s+Runtime.*Databricks\s+Connect|"
        r"Unsupported.*combination.*Databricks",
        re.IGNORECASE,
    )

    @classmethod
    def _build_session(cls, host: str, token: str, cluster_id: str,
                       auto_align: bool):
        """Create the DatabricksSession; on version mismatch optionally
        reinstall a compatible databricks-connect and retry once."""
        try:
            from databricks.connect import DatabricksSession
            return (
                DatabricksSession.builder
                .host(host)
                .token(token)
                .clusterId(cluster_id)
                .getOrCreate()
            )
        except Exception as e:
            msg = str(e)
            if not (auto_align and cls._VERSION_MISMATCH_RE.search(msg)):
                raise

            logger.warning(
                "databricks-connect version mismatch detected; attempting to "
                "auto-align with the cluster's DBR version. (%s)", msg.splitlines()[0]
            )
            dbr = cls._fetch_cluster_dbr(host, token, cluster_id)
            if not dbr:
                raise RuntimeError(
                    "Could not detect cluster DBR version for auto-alignment. "
                    "Pin databricks-connect manually, e.g. "
                    "`uv pip install 'databricks-connect~=14.3'`."
                ) from e
            target_spec = cls._connect_spec_for_dbr(dbr)
            cls._reinstall_databricks_connect(target_spec)

            # Re-import after reinstall (fresh module)
            import importlib
            import databricks.connect as _dc
            importlib.reload(_dc)
            from databricks.connect import DatabricksSession
            return (
                DatabricksSession.builder
                .host(host)
                .token(token)
                .clusterId(cluster_id)
                .getOrCreate()
            )

    @staticmethod
    def _fetch_cluster_dbr(host: str, token: str, cluster_id: str) -> Optional[str]:
        """Hit /api/2.0/clusters/get and pull spark_version (e.g. '14.3.x-scala2.12')."""
        try:
            import requests
        except ImportError:
            return None
        url = host.rstrip("/") + "/api/2.0/clusters/get"
        try:
            r = requests.get(
                url,
                headers={"Authorization": f"Bearer {token}"},
                params={"cluster_id": cluster_id},
                timeout=30,
            )
            r.raise_for_status()
            spark_version = r.json().get("spark_version", "")
        except Exception as exc:
            logger.warning("Failed to query cluster DBR version: %s", exc)
            return None
        # spark_version looks like "14.3.x-scala2.12" or "16.1.x-photon-scala2.12"
        m = re.match(r"^(\d+)\.(\d+)", spark_version)
        if not m:
            return None
        return f"{m.group(1)}.{m.group(2)}"

    @staticmethod
    def _connect_spec_for_dbr(dbr: str) -> str:
        """Map DBR major.minor → a pip spec for databricks-connect.

        Databricks publishes databricks-connect with versions matching DBR
        (e.g. 14.3.*, 15.4.*, 16.1.*). Compatible-release `~=` keeps us on the
        latest patch within the same major.minor.
        """
        return f"databricks-connect~={dbr}.0"

    @staticmethod
    def _reinstall_databricks_connect(spec: str) -> None:
        """pip-install (force) the requested databricks-connect into the
        active interpreter. databricks-connect ships its own pyspark; we let
        pip resolve the matching pyspark."""
        logger.warning("Installing %s into %s ...", spec, sys.executable)
        cmd = [sys.executable, "-m", "pip", "install", "--quiet",
               "--upgrade", "--force-reinstall", spec]
        try:
            subprocess.check_call(cmd)
        except subprocess.CalledProcessError as exc:
            raise RuntimeError(
                f"Failed to install {spec}: {exc}. "
                f"Run manually: `{' '.join(cmd)}`"
            ) from exc
