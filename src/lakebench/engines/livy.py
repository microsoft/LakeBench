from .base import BaseEngine
from typing import Any, Dict, List, Optional
import os
import time
import json
from datetime import datetime


class Livy(BaseEngine):
    """
    Livy Engine — executes Spark workloads via the Apache Livy REST API.

    Submits PySpark code snippets to a remote Livy server. Unlike SparkConnect
    and Databricks engines, there is no local SparkSession — all execution
    happens remotely via HTTP.

    Requires: requests

    Parameters
    ----------
    url : str
        Livy server URL (e.g., 'https://livy.example.com' or Fabric Livy endpoint).
    schema_or_working_directory_uri : str
        Working directory URI for Delta tables on the remote cluster.
    auth : str, default 'none'
        Authentication method: 'none', 'basic', 'kerberos', 'bearer', 'az'.
        - 'bearer': Uses token from env var specified by token_env.
        - 'az': Uses Azure CLI to get a token for the specified scope.
    kind : str, default 'pyspark'
        Livy session kind.
    username : str, optional
        Username for basic auth.
    password_env : str, optional
        Env var name containing password for basic auth.
    token_env : str, optional
        Env var name containing bearer token (for auth='bearer').
    az_scope : str, optional
        Azure AD scope for az CLI auth (default: 'https://api.fabric.microsoft.com/.default').
    session_conf : dict, optional
        Additional Spark configuration to pass when creating the Livy session.
    cost_per_vcore_hour : float, optional
        Cost per vCore hour for cost estimation.
    storage_options : dict, optional
        Storage options for remote filesystem access.
    """

    SQLGLOT_DIALECT = "spark"
    SUPPORTS_SCHEMA_PREP = False

    def __init__(
        self,
        url: str,
        schema_or_working_directory_uri: str,
        auth: str = "none",
        kind: str = "pyspark",
        schema_name: Optional[str] = None,
        catalog_name: Optional[str] = None,
        username: Optional[str] = None,
        password_env: Optional[str] = None,
        token_env: Optional[str] = None,
        az_scope: Optional[str] = None,
        session_conf: Optional[Dict[str, str]] = None,
        cost_per_vcore_hour: Optional[float] = None,
        storage_options: Optional[Dict[str, Any]] = None,
        query_timeout_seconds: Optional[int] = None,
    ):
        super().__init__(
            schema_or_working_directory_uri=schema_or_working_directory_uri,
            storage_options=storage_options,
        )
        import requests

        self._url = url.rstrip("/")
        self._kind = kind
        self._requests = requests
        self._session_conf = session_conf or {}
        self.cost_per_vcore_hour = cost_per_vcore_hour
        self.version = f"livy ({url})"
        self.schema_name = schema_name
        self.catalog_name = catalog_name
        self.query_timeout_seconds = query_timeout_seconds

        # Set up auth
        self._session = requests.Session()
        if auth == "basic":
            password = os.environ.get(password_env or "") if password_env else None
            self._session.auth = (username or "", password or "")
        elif auth == "kerberos":
            from requests_kerberos import HTTPKerberosAuth
            self._session.auth = HTTPKerberosAuth()
        elif auth == "bearer":
            token = os.environ.get(token_env or "")
            if not token:
                raise EnvironmentError(
                    f"Environment variable '{token_env}' is not set for bearer auth."
                )
            self._session.headers.update({"Authorization": f"Bearer {token}"})
        elif auth == "az":
            self._az_scope = az_scope or "https://api.fabric.microsoft.com/.default"
            self._auth_method = "az"
            self._token_expiry = 0.0
            token = self._get_az_token(self._az_scope)
            self._session.headers.update({"Authorization": f"Bearer {token}"})

        self._session.headers.update({"Content-Type": "application/json"})

        # Create Livy session
        self._livy_session_id = self._create_session()
        self.extended_engine_metadata.update({
            "livy_url": url,
            "livy_session_id": str(self._livy_session_id),
        })

    def _get_az_token(self, scope: str) -> str:
        """Get an Azure AD token via the az CLI and record its real expiry."""
        import subprocess
        result = subprocess.run(
            ["az", "account", "get-access-token", "--scope", scope, "-o", "json"],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"Failed to get Azure token via 'az' CLI: {result.stderr.strip()}\n"
                f"Make sure you are logged in with 'az login'."
            )
        data = json.loads(result.stdout)
        # expiresOn format: "YYYY-MM-DD HH:MM:SS.ffffff" in local time
        try:
            self._token_expiry = datetime.fromisoformat(data["expiresOn"]).timestamp()
        except (KeyError, ValueError):
            # Fallback: assume 55 minutes (azure tokens are nominally 1h)
            self._token_expiry = time.time() + 55 * 60
        return data["accessToken"]

    def _refresh_token_if_needed(self, force: bool = False):
        """Refresh Azure AD token before it expires (2-min safety margin)."""
        if getattr(self, "_auth_method", None) != "az":
            return
        if force or time.time() > (self._token_expiry - 120):
            token = self._get_az_token(self._az_scope)
            self._session.headers.update({"Authorization": f"Bearer {token}"})

    def _create_session(self):
        """Create a new Livy interactive session and wait until it's ready."""
        # Synapse's Livy REST API requires a non-empty session name
        # ("Cannot be empty (Parameter 'Name')"). Fabric/standard Livy accept
        # it harmlessly, so we always include one.
        session_name = f"lakebench-{int(time.time())}"
        payload = {"kind": self._kind, "name": session_name}
        if self._session_conf:
            payload["conf"] = self._session_conf
        resp = self._session.post(
            f"{self._url}/sessions",
            data=json.dumps(payload),
        )
        if not resp.ok:
            raise RuntimeError(
                f"Failed to create Livy session ({resp.status_code}): {resp.text}"
            )
        session_id = resp.json()["id"]

        # Wait for session to be ready
        for _ in range(120):  # 10 minute timeout
            resp = self._session.get(f"{self._url}/sessions/{session_id}")
            resp.raise_for_status()
            data = resp.json()
            # Fabric uses livyInfo.currentState; standard Livy uses state
            state = data.get("state") or data.get("livyInfo", {}).get("currentState", "")
            if state == "idle":
                return session_id
            elif state in ("error", "dead", "shutting_down", "killed"):
                raise RuntimeError(
                    f"Livy session {session_id} entered state '{state}'. "
                    f"Check Livy server logs."
                )
            time.sleep(5)

        raise TimeoutError(f"Livy session {session_id} did not become ready within 10 minutes.")

    def _submit_statement(self, code: str, timeout_seconds: Optional[int] = None) -> Dict[str, Any]:
        """Submit a code statement to the Livy session and wait for result.

        Parameters
        ----------
        code : str
            PySpark/SQL code to run.
        timeout_seconds : int, optional
            Per-statement wall-clock cap. None = use the engine default
            (``self.query_timeout_seconds`` if set, else 3 hours). On
            timeout we POST to the cancel endpoint, mark the session
            wedged, and raise ``TimeoutError``.
        """
        effective_timeout = (
            timeout_seconds
            if timeout_seconds is not None
            else (self.query_timeout_seconds or 3 * 60 * 60)
        )
        deadline = time.time() + effective_timeout
        poll_interval = 5

        self._refresh_token_if_needed()
        resp = self._session.post(
            f"{self._url}/sessions/{self._livy_session_id}/statements",
            data=json.dumps({"code": code, "kind": self._kind}),
        )
        if resp.status_code == 401:
            # Token may have been invalidated server-side despite our expiry check.
            self._refresh_token_if_needed(force=True)
            resp = self._session.post(
                f"{self._url}/sessions/{self._livy_session_id}/statements",
                data=json.dumps({"code": code, "kind": self._kind}),
            )
        if not resp.ok:
            raise RuntimeError(
                f"Livy statement submission failed ({resp.status_code}): {resp.text}"
            )
        statement_id = resp.json()["id"]

        # Poll for completion
        while time.time() < deadline:
            self._refresh_token_if_needed()
            resp = self._session.get(
                f"{self._url}/sessions/{self._livy_session_id}/statements/{statement_id}"
            )
            if resp.status_code == 401:
                self._refresh_token_if_needed(force=True)
                resp = self._session.get(
                    f"{self._url}/sessions/{self._livy_session_id}/statements/{statement_id}"
                )
            resp.raise_for_status()
            result = resp.json()
            state = result["state"]
            if state == "available":
                output = result.get("output", {})
                if output.get("status") == "error":
                    raise RuntimeError(
                        f"Livy statement error: {output.get('evalue', 'Unknown error')}\n"
                        f"{output.get('traceback', '')}"
                    )
                return output
            elif state in ("error", "cancelled"):
                raise RuntimeError(f"Livy statement {statement_id} failed with state '{state}'.")
            time.sleep(poll_interval)

        # Timed out — best-effort cancel, then mark the session wedged
        # so callers can decide whether to recreate it.
        self._cancel_statement(statement_id)
        self._session_wedged = True
        raise TimeoutError(
            f"Livy statement {statement_id} did not complete within "
            f"{effective_timeout} seconds."
        )

    def _cancel_statement(self, statement_id: int) -> None:
        """Best-effort POST to the Livy cancel endpoint; never raises."""
        try:
            self._refresh_token_if_needed()
            self._session.post(
                f"{self._url}/sessions/{self._livy_session_id}/statements/{statement_id}/cancel",
                timeout=30,
            )
        except Exception:
            pass

    def _close_session(self) -> None:
        """Best-effort DELETE of the Livy session."""
        try:
            self._refresh_token_if_needed()
            self._session.delete(
                f"{self._url}/sessions/{self._livy_session_id}",
                timeout=30,
            )
        except Exception:
            pass

    def _recreate_session(self) -> None:
        """Tear down the wedged session and start a fresh one."""
        old_id = getattr(self, "_livy_session_id", None)
        self._close_session()
        self._livy_session_id = self._create_session()
        self._session_wedged = False
        self.extended_engine_metadata.update({
            "livy_session_id": str(self._livy_session_id),
            "livy_session_recreated_from": str(old_id),
        })

    def get_table_columns(self, table_name: str) -> list:
        """Return column names for a Spark table/view via Livy."""
        escaped = table_name.replace('\\', '\\\\').replace('"', '\\"')
        code = f'print(spark.table("{escaped}").columns)'
        output = self._submit_statement(code)
        # output data text looks like "['col1', 'col2', ...]"
        text = output.get("data", {}).get("text/plain", "")
        if text:
            import ast
            try:
                return ast.literal_eval(text.strip())
            except (ValueError, SyntaxError):
                return []
        return []

    def list_databases(self) -> list:
        """List databases visible to the Livy-attached Spark session."""
        code = (
            'rows = spark.sql("SHOW DATABASES").collect()\n'
            'print("\\n".join([(r.asDict().get("namespace") '
            'or r.asDict().get("databaseName") '
            'or list(r.asDict().values())[0]) for r in rows]))'
        )
        output = self._submit_statement(code)
        text = output.get("data", {}).get("text/plain", "") or ""
        return [s.strip() for s in text.splitlines() if s.strip()]

    def list_tables(self, database: str) -> list:
        """List tables in `database` via Livy.

        Backtick each dotted segment separately so multi-part names like
        Fabric's `workspace.lakehouse.schema` resolve as a real namespace
        rather than a single literal identifier.
        """
        segments = [seg.replace('`', '') for seg in database.split('.')]
        qualified = ".".join(f"`{seg}`" for seg in segments)
        code = (
            f'rows = spark.sql("SHOW TABLES IN {qualified}").collect()\n'
            'print("\\n".join([r.asDict().get("tableName", "") for r in rows]))'
        )
        output = self._submit_statement(code)
        text = output.get("data", {}).get("text/plain", "") or ""
        return [s.strip() for s in text.splitlines() if s.strip()]

    def execute_sql_query(self, query: str, context_decorator: Optional[str] = None):
        """Execute a SQL query via Livy."""
        self._heal_session_if_wedged()
        escaped = query.replace('\\', '\\\\').replace('"""', '\\"\\"\\"')
        code = f'spark.sql("""{escaped}""").collect()'
        try:
            self._submit_statement(code)
        except (TimeoutError, ConnectionError, self._requests.exceptions.ConnectionError):
            # Session is now wedged/unreachable; mark it for recovery on
            # the next call so subsequent queries don't all cascade-fail.
            self._session_wedged = True
            raise

    def execute_sql_statement(self, statement: str, context_decorator: Optional[str] = None):
        """Execute a SQL statement (DDL/DML) via Livy."""
        self._heal_session_if_wedged()
        escaped = statement.replace('\\', '\\\\').replace('"""', '\\"\\"\\"')
        code = f'spark.sql("""{escaped}""")'
        try:
            self._submit_statement(code)
        except (TimeoutError, ConnectionError, self._requests.exceptions.ConnectionError):
            self._session_wedged = True
            raise

    def _heal_session_if_wedged(self) -> None:
        """If the previous statement timed out / dropped the connection,
        recreate the Livy session before the next call.

        Logged as a warning. If session recreation itself fails the
        original error propagates so the caller knows the engine is dead.
        """
        if not getattr(self, "_session_wedged", False):
            return
        import logging
        logging.getLogger("lakebench.engines.livy").warning(
            "Livy session %s appears wedged; recreating before next call.",
            getattr(self, "_livy_session_id", "?"),
        )
        try:
            self._recreate_session()
        except Exception as exc:
            raise RuntimeError(
                f"Failed to recreate Livy session after previous timeout: {exc}"
            ) from exc

    def load_parquet_to_delta(
        self,
        parquet_folder_uri: str,
        table_name: str,
        table_is_precreated: bool = False,
        context_decorator: Optional[str] = None,
    ):
        """Load parquet data via Livy.

        Uses createOrReplaceTempView instead of saveAsTable to avoid a
        Fabric Spark bug where DeltaOptimizedWriterColumnarExec crashes
        with a NoSuchMethodError in the Gluten/Velox columnar engine.
        Temp views keep NEE (Native Execution Engine) active for queries.
        """
        escaped_uri = parquet_folder_uri.replace('\\', '\\\\').replace('"""', '\\"\\"\\"')
        escaped_name = table_name.replace('\\', '\\\\').replace('"""', '\\"\\"\\"')
        code = f'''
df = spark.read.parquet("{escaped_uri}")
df.createOrReplaceTempView("{escaped_name}")
'''
        self._submit_statement(code)

    def optimize_table(self, table_name: str):
        """Run OPTIMIZE on a Delta table."""
        self.execute_sql_statement(f"OPTIMIZE {table_name}")

    def vacuum_table(self, table_name: str, retention_hours: int = 168):
        """Run VACUUM on a Delta table."""
        self.execute_sql_statement(
            f"VACUUM {table_name} RETAIN {retention_hours} HOURS"
        )

    def create_schema_if_not_exists(self, drop_before_create: bool = False):
        """Create schema via remote Spark SQL."""
        # Livy sessions on Fabric use the lakehouse's default schema
        # No explicit schema creation needed
        pass

    def create_external_location(self, uri: str):
        """No-op for Livy — locations are managed by the cluster."""
        pass

    def _create_empty_table(self, table_name: str, ddl: str):
        """Create an empty table using DDL via Livy."""
        # Use CREATE OR REPLACE to handle re-runs
        ddl = ddl.replace("CREATE TABLE", "CREATE OR REPLACE TABLE")
        ddl = ddl.replace("CREATE OR REPLACE OR REPLACE", "CREATE OR REPLACE")
        self.execute_sql_statement(ddl)

    def _delete_session(self):
        """Delete the Livy session."""
        try:
            self._session.delete(
                f"{self._url}/sessions/{self._livy_session_id}"
            )
        except Exception:
            pass

    def __del__(self):
        self._delete_session()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._delete_session()
        return False
