import logging
import os
import re
import subprocess
import sys
from typing import Optional

from .base import BaseEngine
from .spark import Spark

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
                    f"Environment variable '{token_env}' is not set. Set it to your Databricks personal access token."
                )

        # Build session via Databricks Connect. _build_session handles
        # proactive DBR alignment, import-time pyspark conflicts, and
        # reactive version mismatches — so by the time we get here the
        # bundled pyspark is guaranteed importable.
        spark_session = self._build_session(host, token, cluster_id, auto_align_connect_version)
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
                    "`sparkmeasure` is not installed. Install with: `pip install lakebench[sparkmeasure]`."
                )
        self.spark_measure_telemetry = spark_measure_telemetry

        self.version = f"databricks ({host})"

        self.catalog_name = catalog_name
        self.schema_name = schema_name
        self.full_catalog_schema_reference = (
            f"`{self.catalog_name}`.`{self.schema_name}`" if catalog_name else f"`{self.schema_name}`"
        )
        self.cost_per_vcore_hour = cost_per_vcore_hour
        self.compute_stats_all_cols = compute_stats_all_cols
        self.run_analyze_after_load = self.compute_stats_all_cols
        self.spark_configs = {}
        self.extended_engine_metadata.update(
            {
                "databricks_host": host,
                "databricks_cluster_id": cluster_id,
            }
        )
        self._use_temp_views = use_temp_views
        if use_temp_views:
            self.SUPPORTS_SCHEMA_PREP = False
            # Don't qualify table names — temp views are session-scoped
            self.catalog_name = None
            self.schema_name = None
            self.full_catalog_schema_reference = None

    def load_parquet_to_delta(
        self,
        parquet_folder_uri: str,
        table_name: str,
        table_is_precreated: bool = False,
        context_decorator: Optional[str] = None,
    ):
        """Load parquet data. Uses temp views when use_temp_views=True to avoid cast errors."""
        df = self.spark.read.parquet(parquet_folder_uri)
        if self._use_temp_views:
            df.createOrReplaceTempView(table_name)
        elif table_is_precreated:
            df.write.insertInto(table_name, overwrite=True)
        else:
            df.write.format("delta").mode("append").saveAsTable(table_name)

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
            [
                "az",
                "account",
                "get-access-token",
                "--resource",
                "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d",
                "--query",
                "accessToken",
                "-o",
                "tsv",
            ],
            capture_output=True,
            text=True,
            timeout=30,
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

    # Patterns that indicate the installed pyspark is incompatible with the
    # installed databricks-connect (commonly: a standalone `pyspark` shadows
    # the bundled one). Caught at import time, before any session is built.
    _PYSPARK_IMPORT_ERROR_HINTS = (
        "_from_numpy_type",  # pyspark <3.4 missing symbol used by dbconnect 14+
        "from pyspark.sql.types import",  # generic pyspark.sql.types import failure
        "pyspark.sql.connect",  # connect submodule shape mismatch
        "no module named 'pyspark",
        "cannot import name",  # broad catch-all for bundled-pyspark mismatch
    )

    @classmethod
    def _build_session(cls, host: str, token: str, cluster_id: str, auto_align: bool):
        """Create the DatabricksSession.

        Auto-alignment fires in three places:
          1. **Proactively** — before importing `databricks.connect`, query
             the cluster's DBR. If the installed `databricks-connect`
             major.minor doesn't match, reinstall.
          2. **At import time** — if `from databricks.connect import …`
             raises ImportError (typically because a standalone `pyspark`
             is shadowing the bundled one), reinstall with --force-reinstall
             so the correct pyspark is pulled in.
          3. **At session-build time** — if the cluster rejects the client
             version with the classic "Unsupported combination …" message,
             reinstall.

        All three paths share the same reinstall + retry helper.
        """

        # ---- 1) Proactive DBR check ----------------------------------
        if auto_align:
            target_dbr = cls._fetch_cluster_dbr(host, token, cluster_id)
            if target_dbr and not cls._installed_connect_matches(target_dbr):
                logger.warning(
                    "Installed databricks-connect (%s) does not match cluster DBR %s; reinstalling for alignment.",
                    cls._installed_connect_version() or "none",
                    target_dbr,
                )
                cls._reinstall_databricks_connect(cls._connect_spec_for_dbr(target_dbr))
                cls._reexec_after_install(f"aligned databricks-connect to DBR {target_dbr}")
                cls._purge_pyspark_modules()  # fallback if reexec was skipped

        # ---- 2) Import-time failure (typically pyspark conflict) -----
        try:
            from databricks.connect import DatabricksSession  # noqa: F401 — probe import
        except ImportError as e:
            msg = str(e).lower()
            hint_matched = any(h in msg for h in cls._PYSPARK_IMPORT_ERROR_HINTS)
            if not (auto_align and hint_matched):
                raise RuntimeError(
                    f"Failed to import databricks.connect: {e}\n"
                    "This is usually caused by a standalone `pyspark` package "
                    "shadowing the one bundled with databricks-connect, or by "
                    "missing pandas/pyarrow runtime deps.\n"
                    "Try: `pip uninstall -y pyspark && pip install --force-reinstall "
                    "'databricks-connect~=<DBR_MAJOR.MINOR>' pandas pyarrow`"
                ) from e

            logger.warning(
                "databricks-connect import failed (%s); attempting to repair by "
                "reinstalling a matching client + bundled pyspark.",
                e,
            )
            target_dbr = cls._fetch_cluster_dbr(host, token, cluster_id)
            target_spec = (
                cls._connect_spec_for_dbr(target_dbr) if target_dbr else cls._installed_connect_spec_or_default()
            )
            cls._reinstall_databricks_connect(target_spec)
            cls._reexec_after_install("repaired databricks-connect import")
            cls._purge_pyspark_modules()  # fallback

        # ---- 3) Session build, with reactive realign on version error
        try:
            return cls._open_session(host, token, cluster_id)
        except Exception as e:
            msg = str(e)
            if not (auto_align and cls._VERSION_MISMATCH_RE.search(msg)):
                raise

            logger.warning(
                "databricks-connect rejected by cluster (%s); auto-aligning.",
                msg.splitlines()[0],
            )
            dbr = cls._fetch_cluster_dbr(host, token, cluster_id)
            if not dbr:
                raise RuntimeError(
                    "Could not detect cluster DBR version for auto-alignment. "
                    "Pin databricks-connect manually, e.g. "
                    "`pip install 'databricks-connect~=14.3'`."
                ) from e
            cls._reinstall_databricks_connect(cls._connect_spec_for_dbr(dbr))
            cls._reexec_after_install(f"aligned databricks-connect to DBR {dbr} after cluster rejection")
            cls._purge_pyspark_modules()  # fallback
            return cls._open_session(host, token, cluster_id)

    @staticmethod
    def _open_session(host: str, token: str, cluster_id: str):
        from databricks.connect import DatabricksSession

        return DatabricksSession.builder.host(host).token(token).clusterId(cluster_id).getOrCreate()

    @staticmethod
    def _installed_connect_version() -> Optional[str]:
        """Return the installed databricks-connect version, or None if absent."""
        try:
            from importlib.metadata import PackageNotFoundError, version
        except ImportError:  # pragma: no cover (py<3.8)
            return None
        try:
            return version("databricks-connect")
        except PackageNotFoundError:
            return None

    @classmethod
    def _installed_connect_matches(cls, target_dbr: str) -> bool:
        """True iff installed databricks-connect is "close enough" to the
        target DBR — same major within ±5 minors is acceptable (DBR minor
        releases roughly every 2 months; databricks-connect tolerates this
        drift, and the reactive `_VERSION_MISMATCH_RE` path catches any
        cluster-side rejection).

        Also considers the LAKEBENCH_DATABRICKS_REEXECED sentinel: if we've
        already re-execed in this session for this engine, treat the
        installed version as final — we can't usefully reinstall again.
        """
        if os.environ.get(cls._REEXEC_ENV) == "1":
            return True
        installed = cls._installed_connect_version()
        if not installed:
            return False
        m_inst = re.match(r"^(\d+)\.(\d+)", installed)
        m_tgt = re.match(r"^(\d+)\.(\d+)", target_dbr)
        if not (m_inst and m_tgt):
            return False
        inst_major, inst_minor = int(m_inst.group(1)), int(m_inst.group(2))
        tgt_major, tgt_minor = int(m_tgt.group(1)), int(m_tgt.group(2))
        if inst_major != tgt_major:
            return False
        return abs(inst_minor - tgt_minor) <= 5

    @classmethod
    def _installed_connect_spec_or_default(cls) -> str:
        """Pin spec for the *currently installed* databricks-connect, used as
        a fallback when we cannot reach the cluster to learn its DBR."""
        installed = cls._installed_connect_version()
        if installed:
            m = re.match(r"^(\d+)\.(\d+)", installed)
            if m:
                return f"databricks-connect~={m.group(1)}.{m.group(2)}.0"
        # Last resort: latest 14.3 LTS line
        return "databricks-connect~=14.3.0"

    @staticmethod
    def _purge_pyspark_modules() -> None:
        """Drop already-imported pyspark + databricks.connect modules so the
        next `import` picks up the freshly installed versions.

        Note: this is best-effort. Python caches `pkg_resources` distribution
        metadata and some C extensions in ways that are not reliably purged
        by `sys.modules.pop`. After a pip install in the running interpreter,
        a clean re-exec is more reliable — see `_reexec_after_install`.
        """
        for name in list(sys.modules):
            if name == "pyspark" or name.startswith("pyspark."):
                sys.modules.pop(name, None)
            elif name == "databricks.connect" or name.startswith("databricks.connect."):
                sys.modules.pop(name, None)
        # Invalidate importlib's caches so the new package on disk is seen
        import importlib

        importlib.invalidate_caches()
        # Reset importlib.metadata cache (ensures `version()` re-reads dist-info)
        try:
            from importlib.metadata import _meta  # type: ignore[attr-defined]

            _meta.cache_clear()  # type: ignore[attr-defined]
        except Exception:
            pass

    # Sentinel env var: set after we've reinstalled-and-reexeced so we don't
    # loop forever if the cluster is genuinely unreachable.
    _REEXEC_ENV = "LAKEBENCH_DATABRICKS_REEXECED"

    @classmethod
    def _reexec_after_install(cls, reason: str) -> None:
        """Re-exec the current process with the same argv so pyspark and
        databricks-connect load fresh from disk. No-op if we've already
        re-execed (prevents infinite loops on genuine connectivity failures).
        """
        if os.environ.get(cls._REEXEC_ENV) == "1":
            logger.warning(
                "Already re-execed once after databricks-connect reinstall; not retrying. Reason: %s",
                reason,
            )
            return
        logger.warning(
            "Re-executing process after databricks-connect reinstall (%s) ...",
            reason,
        )
        new_env = dict(os.environ)
        new_env[cls._REEXEC_ENV] = "1"
        os.execvpe(sys.executable, [sys.executable, *sys.argv], new_env)

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
        pip resolve the matching pyspark.

        If the exact major.minor spec isn't installable on the current Python
        (newer DBR wheels can require a newer Python), fall back to the
        highest-available patch within the same major series — clusters
        generally accept clients up to one minor behind without complaint,
        and any leftover gap will be caught by the reactive
        `_VERSION_MISMATCH_RE` path.
        """
        attempts = [spec]
        # Build fallback: ~={major}.0 (any version in same major)
        m = re.match(r"databricks-connect~=(\d+)\.(\d+)\.0$", spec)
        if m:
            major = m.group(1)
            attempts.append(f"databricks-connect~={major}.0")
        # Last-ditch fallback: latest LTS line
        attempts.append("databricks-connect~=14.3.0")

        last_err: Optional[Exception] = None
        for attempt in attempts:
            logger.warning("Installing %s into %s ...", attempt, sys.executable)
            # databricks-connect 14+ uses pandas/pyarrow at import time but
            # doesn't always pin them as hard requirements. Pin to known
            # compatible ranges:
            #   - pyspark 3.5/4.0 expects pandas 1.x or 2.x (NOT 3.x — it
            #     drops C extensions pyspark relies on).
            #   - pyarrow >= 4.0 is the documented floor.
            cmd = [
                sys.executable,
                "-m",
                "pip",
                "install",
                "--quiet",
                "--upgrade",
                "--force-reinstall",
                attempt,
                "pandas>=1.0.5,<3.0",
                "pyarrow>=4.0.0",
            ]
            try:
                subprocess.check_call(cmd)
                if attempt != spec:
                    logger.warning(
                        "Could not satisfy %s on this Python interpreter; "
                        "fell back to %s. (Newer DBR clients may require a "
                        "newer Python — see "
                        "https://docs.databricks.com/dev-tools/databricks-connect/python/install.html)",
                        spec,
                        attempt,
                    )
                return
            except subprocess.CalledProcessError as exc:
                last_err = exc
                logger.warning("Install of %s failed; trying next fallback.", attempt)
        raise RuntimeError(
            f"Failed to install databricks-connect (tried: {attempts}). "
            f"Last error: {last_err}. "
            f"Run manually: `pip install --force-reinstall {attempts[0]}`"
        )
