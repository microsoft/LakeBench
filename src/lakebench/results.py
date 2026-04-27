"""
LakeBench Results Manager — per-run storage with full environment metadata.

Storage layout:
    ~/.lakebench/results/
    ├── runs/
    │   ├── 2026-04-17T160556_tpcds_sf1_duckdb_e6306de6/
    │   │   ├── results.parquet
    │   │   └── metadata.json
    │   └── ...
    ├── index.parquet
    └── all_results.parquet
"""

import json
import os
import platform
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq


DEFAULT_RESULTS_DIR = os.path.expanduser("~/.lakebench/results")

# Schema for per-run results (matches BaseBenchmark.RESULT_SCHEMA)
RESULTS_SCHEMA = pa.schema([
    ("run_id", pa.string()),
    ("run_datetime", pa.timestamp("us", tz="UTC")),
    ("lakebench_version", pa.string()),
    ("engine", pa.string()),
    ("engine_version", pa.string()),
    ("benchmark", pa.string()),
    ("benchmark_version", pa.string()),
    ("mode", pa.string()),
    ("scale_factor", pa.int32()),
    ("scenario", pa.string()),
    ("total_cores", pa.int16()),
    ("compute_size", pa.string()),
    ("phase", pa.string()),
    ("test_item", pa.string()),
    ("start_datetime", pa.timestamp("us", tz="UTC")),
    ("duration_ms", pa.int32()),
    ("estimated_retail_job_cost", pa.decimal128(18, 10)),
    ("iteration", pa.int8()),
    ("success", pa.bool_()),
    ("error_message", pa.string()),
    ("engine_properties", pa.map_(pa.string(), pa.string())),
    ("execution_telemetry", pa.map_(pa.string(), pa.string())),
])

# Schema for the run index (one row per run)
INDEX_SCHEMA = pa.schema([
    ("run_id", pa.string()),
    ("run_datetime", pa.timestamp("us", tz="UTC")),
    ("benchmark", pa.string()),
    ("engine", pa.string()),
    ("engine_version", pa.string()),
    ("scenario", pa.string()),
    ("scale_factor", pa.int32()),
    ("mode", pa.string()),
    ("profile", pa.string()),
    ("total_cores", pa.int16()),
    ("compute_size", pa.string()),
    ("total_duration_ms", pa.int64()),
    ("total_items", pa.int32()),
    ("success_count", pa.int32()),
    ("failed_count", pa.int32()),
    ("run_dir", pa.string()),
])


class ResultsManager:
    """
    Manages benchmark results storage with per-run directories and metadata.

    Parameters
    ----------
    results_dir : str
        Root directory for results storage. Default: ~/.lakebench/results
    """

    def __init__(self, results_dir: str = DEFAULT_RESULTS_DIR):
        self.results_dir = os.path.expanduser(results_dir)
        self.runs_dir = os.path.join(self.results_dir, "runs")
        self.index_path = os.path.join(self.results_dir, "index.parquet")
        self.all_results_path = os.path.join(self.results_dir, "all_results.parquet")
        os.makedirs(self.runs_dir, exist_ok=True)

    def save_run(
        self,
        benchmark,
        profile_name: Optional[str] = None,
        profile_config: Optional[Dict] = None,
        fail_on_collision: bool = False,
    ):
        """
        Save a completed benchmark run — results.parquet + metadata.json + update index.

        Parameters
        ----------
        benchmark : BaseBenchmark
            The completed benchmark instance (must have .results, .header_detail_dict, .engine).
        profile_name : str, optional
            Name of the profile used.
        profile_config : dict, optional
            Full profile configuration dict.
        fail_on_collision : bool, optional
            If True and an existing run with the same run_id is found, raise
            FileExistsError instead of silently suffixing the directory name.
            Default False (legacy behaviour — warn and suffix).
        """
        results = benchmark.results
        if not results:
            return

        header = benchmark.header_detail_dict
        engine = benchmark.engine
        run_id = header["run_id"]
        run_dt = header["run_datetime"]

        # Build run directory name
        dirname = self._build_run_dirname(
            run_dt, header["benchmark"], header["scenario"], header["engine"], run_id
        )
        run_dir = os.path.join(self.runs_dir, dirname)

        # Detect collisions: same run_id already in index OR directory exists
        collision_source = None
        existing_dir = self._find_run_dir(run_id)
        if existing_dir and os.path.isdir(existing_dir):
            collision_source = existing_dir
        elif os.path.isdir(run_dir):
            collision_source = run_dir

        if collision_source:
            msg = (
                f"run_id '{run_id}' already exists at {collision_source}."
            )
            if fail_on_collision:
                raise FileExistsError(
                    msg + " Use a different --run-id or omit --fail-on-run-id-collision."
                )
            # Suffix the new directory and warn loudly.
            import itertools
            for n in itertools.count(2):
                alt = f"{run_dir}__{n}"
                if not os.path.exists(alt):
                    run_dir = alt
                    break
            print(
                f"WARNING: {msg} Writing new run to {run_dir} (suffix applied). "
                "Pass --fail-on-run-id-collision to make this fatal.",
                file=sys.stderr,
            )

        os.makedirs(run_dir, exist_ok=True)

        # 1. Save results.parquet
        results_table = self._results_to_arrow(results)
        pq.write_table(results_table, os.path.join(run_dir, "results.parquet"))

        # 2. Save metadata.json
        metadata = self._build_metadata(
            header, results, engine, profile_name, profile_config
        )
        with open(os.path.join(run_dir, "metadata.json"), "w") as f:
            json.dump(metadata, f, indent=2, default=str)

        # 3. Update index
        self._append_to_index(header, results, run_dir, profile_name)

        # 4. Append to all_results
        self._append_to_all_results(results_table)

        print(f"Results saved to: {run_dir}")
        return run_dir

    def list_runs(
        self,
        benchmark: Optional[str] = None,
        engine: Optional[str] = None,
        scenario: Optional[str] = None,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """List runs from the index, optionally filtered."""
        if not os.path.exists(self.index_path):
            return []

        table = pq.read_table(self.index_path)
        df_dict = table.to_pydict()
        n = len(df_dict.get("run_id", []))

        runs = []
        for i in range(n):
            row = {k: v[i] for k, v in df_dict.items()}
            if benchmark and row.get("benchmark", "").lower() != benchmark.lower():
                continue
            if engine and row.get("engine", "").lower() != engine.lower():
                continue
            if scenario and row.get("scenario", "").lower() != scenario.lower():
                continue
            runs.append(row)

        # Sort by run_datetime descending
        runs.sort(key=lambda r: r.get("run_datetime", ""), reverse=True)
        return runs[:limit]

    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific run by ID.

        Returns dict with 'metadata' and 'results' (list of dicts).
        """
        run_dir = self._find_run_dir(run_id)
        if not run_dir:
            return None

        result = {}

        meta_path = os.path.join(run_dir, "metadata.json")
        if os.path.exists(meta_path):
            with open(meta_path) as f:
                result["metadata"] = json.load(f)

        results_path = os.path.join(run_dir, "results.parquet")
        if os.path.exists(results_path):
            table = pq.read_table(results_path)
            result["results"] = table.to_pydict()

        return result

    def get_all_results(
        self,
        benchmark: Optional[str] = None,
        engine: Optional[str] = None,
        scenario: Optional[str] = None,
    ) -> Optional[pa.Table]:
        """Get consolidated results, optionally filtered."""
        if not os.path.exists(self.all_results_path):
            return None

        table = pq.read_table(self.all_results_path)

        filters = []
        if benchmark:
            mask = pa.compute.equal(table.column("benchmark"), benchmark)
            table = table.filter(mask)
        if engine:
            mask = pa.compute.equal(table.column("engine"), engine)
            table = table.filter(mask)
        if scenario:
            mask = pa.compute.equal(table.column("scenario"), scenario)
            table = table.filter(mask)

        return table

    def delete_run(self, run_id: str) -> bool:
        """Delete a run and update index/all_results."""
        run_dir = self._find_run_dir(run_id)
        if not run_dir:
            return False

        shutil.rmtree(run_dir)

        # Rebuild index and all_results without this run
        self._rebuild_consolidated(exclude_run_id=run_id)
        return True

    # --- Private methods ---

    def _build_run_dirname(
        self, run_datetime, benchmark: str, scenario: str, engine: str, run_id: str
    ) -> str:
        if isinstance(run_datetime, datetime):
            ts = run_datetime.strftime("%Y-%m-%dT%H%M%S")
        else:
            ts = str(run_datetime).replace(" ", "T").replace(":", "")[:17]
        short_id = run_id.split("-")[0] if "-" in run_id else run_id[:8]
        return f"{ts}_{benchmark}_{scenario}_{engine}_{short_id}".lower()

    def _results_to_arrow(self, results: List[Dict]) -> pa.Table:
        """Convert result dicts to an Arrow table."""
        columns = {field.name: [] for field in RESULTS_SCHEMA}
        for row in results:
            for field in RESULTS_SCHEMA:
                val = row.get(field.name)
                # Handle MAP columns
                if field.name in ("engine_properties", "execution_telemetry"):
                    if isinstance(val, dict):
                        val = [(str(k), str(v)) for k, v in val.items()]
                    else:
                        val = []
                # Handle timestamps
                elif "datetime" in field.name and isinstance(val, datetime):
                    pass  # pyarrow handles datetime objects
                # Handle Decimal/NaN
                elif field.name == "estimated_retail_job_cost":
                    import math
                    if val is None or (isinstance(val, float) and math.isnan(val)):
                        val = None
                    else:
                        from decimal import Decimal
                        val = Decimal(str(val))
                columns[field.name].append(val)

        arrays = []
        for field in RESULTS_SCHEMA:
            arr = pa.array(columns[field.name], type=field.type)
            arrays.append(arr)

        return pa.table(arrays, schema=RESULTS_SCHEMA)

    def _build_metadata(
        self,
        header: Dict,
        results: List[Dict],
        engine,
        profile_name: Optional[str],
        profile_config: Optional[Dict],
    ) -> Dict[str, Any]:
        """Build the full metadata.json for a run."""
        # Compute summary
        phases = {}
        total_ms = 0
        for r in results:
            phase = r.get("phase", "Unknown")
            if phase not in phases:
                phases[phase] = {"count": 0, "total_ms": 0, "success": 0, "failed": 0}
            phases[phase]["count"] += 1
            phases[phase]["total_ms"] += r.get("duration_ms", 0)
            if r.get("success", False):
                phases[phase]["success"] += 1
            else:
                phases[phase]["failed"] += 1
            total_ms += r.get("duration_ms", 0)

        metadata = {
            "run_id": header.get("run_id"),
            "run_datetime": str(header.get("run_datetime")),
            "benchmark": header.get("benchmark"),
            "engine": header.get("engine"),
            "engine_version": header.get("engine_version"),
            "scenario": header.get("scenario"),
            "scale_factor": header.get("scale_factor"),
            "mode": getattr(engine, "mode", None) if hasattr(engine, "mode") else None,
            "profile": profile_name,
            "lakebench_version": header.get("lakebench_version"),
            "platform": self._collect_platform_metadata(engine),
            "engine_properties": dict(getattr(engine, "extended_engine_metadata", {})),
            "engine_config": dict(getattr(engine, "spark_configs", {})),
            "profile_config": profile_config or {},
            "summary": {
                "total_duration_ms": total_ms,
                "phases": phases,
            },
        }
        return metadata

    def _collect_platform_metadata(self, engine) -> Dict[str, Any]:
        """Gather platform/hardware metadata."""
        import os

        total_mem_gb = None
        try:
            import psutil
            total_mem_gb = round(psutil.virtual_memory().total / (1024**3), 1)
        except ImportError:
            try:
                with open("/proc/meminfo") as f:
                    for line in f:
                        if line.startswith("MemTotal:"):
                            kb = int(line.split()[1])
                            total_mem_gb = round(kb / (1024**2), 1)
                            break
            except (FileNotFoundError, ValueError):
                pass

        cpu_model = "unknown"
        try:
            with open("/proc/cpuinfo") as f:
                for line in f:
                    if line.startswith("model name"):
                        cpu_model = line.split(":", 1)[1].strip()
                        break
        except FileNotFoundError:
            cpu_model = platform.processor() or "unknown"

        return {
            "runtime": getattr(engine, "runtime", "unknown"),
            "os": platform.system().lower(),
            "os_version": platform.platform(),
            "python_version": platform.python_version(),
            "hostname": platform.node(),
            "cpu_model": cpu_model,
            "total_cores": os.cpu_count(),
            "total_memory_gb": total_mem_gb,
            "compute_size": getattr(engine, "get_compute_size", lambda: "unknown")(),
        }

    def _append_to_index(
        self,
        header: Dict,
        results: List[Dict],
        run_dir: str,
        profile_name: Optional[str],
    ):
        """Append one row to the run index."""
        total_ms = sum(r.get("duration_ms", 0) for r in results)
        success = sum(1 for r in results if r.get("success", False))
        failed = sum(1 for r in results if not r.get("success", True))

        new_row = pa.table(
            {
                "run_id": [header["run_id"]],
                "run_datetime": [header["run_datetime"]],
                "benchmark": [header["benchmark"]],
                "engine": [header["engine"]],
                "engine_version": [header["engine_version"]],
                "scenario": [header["scenario"]],
                "scale_factor": [header.get("scale_factor")],
                "mode": [None],
                "profile": [profile_name],
                "total_cores": [header.get("total_cores")],
                "compute_size": [header.get("compute_size")],
                "total_duration_ms": [total_ms],
                "total_items": [len(results)],
                "success_count": [success],
                "failed_count": [failed],
                "run_dir": [run_dir],
            },
            schema=INDEX_SCHEMA,
        )

        if os.path.exists(self.index_path):
            existing = pq.read_table(self.index_path)
            combined = pa.concat_tables([existing, new_row])
        else:
            combined = new_row

        pq.write_table(combined, self.index_path)

    def _append_to_all_results(self, results_table: pa.Table):
        """Append results to the consolidated all_results.parquet."""
        if os.path.exists(self.all_results_path):
            existing = pq.read_table(self.all_results_path)
            combined = pa.concat_tables([existing, results_table])
        else:
            combined = results_table

        pq.write_table(combined, self.all_results_path)

    def _find_run_dir(self, run_id: str) -> Optional[str]:
        """Find the directory for a given run_id."""
        short_id = run_id.split("-")[0] if "-" in run_id else run_id[:8]
        for dirname in os.listdir(self.runs_dir):
            if short_id in dirname:
                return os.path.join(self.runs_dir, dirname)

        # Also check index
        if os.path.exists(self.index_path):
            table = pq.read_table(self.index_path)
            ids = table.column("run_id").to_pylist()
            dirs = table.column("run_dir").to_pylist()
            for i, rid in enumerate(ids):
                if rid == run_id or rid.startswith(short_id):
                    if os.path.isdir(dirs[i]):
                        return dirs[i]
        return None

    def _rebuild_consolidated(self, exclude_run_id: Optional[str] = None):
        """Rebuild index and all_results from individual run directories."""
        all_index_rows = []
        all_result_tables = []

        for dirname in sorted(os.listdir(self.runs_dir)):
            run_dir = os.path.join(self.runs_dir, dirname)
            if not os.path.isdir(run_dir):
                continue

            meta_path = os.path.join(run_dir, "metadata.json")
            results_path = os.path.join(run_dir, "results.parquet")

            if not os.path.exists(results_path):
                continue

            results_table = pq.read_table(results_path)
            run_ids = results_table.column("run_id").to_pylist()
            if run_ids and run_ids[0] == exclude_run_id:
                continue

            all_result_tables.append(results_table)

            # Build index row from metadata or results
            if os.path.exists(meta_path):
                with open(meta_path) as f:
                    meta = json.load(f)
                summary = meta.get("summary", {})
                phases = summary.get("phases", {})
                success = sum(p.get("success", 0) for p in phases.values())
                failed = sum(p.get("failed", 0) for p in phases.values())
                total_items = sum(p.get("count", 0) for p in phases.values())

                all_index_rows.append({
                    "run_id": meta["run_id"],
                    "run_datetime": meta["run_datetime"],
                    "benchmark": meta["benchmark"],
                    "engine": meta["engine"],
                    "engine_version": meta.get("engine_version", ""),
                    "scenario": meta.get("scenario", ""),
                    "scale_factor": meta.get("scale_factor"),
                    "mode": meta.get("mode"),
                    "profile": meta.get("profile"),
                    "total_cores": meta.get("platform", {}).get("total_cores"),
                    "compute_size": meta.get("platform", {}).get("compute_size", ""),
                    "total_duration_ms": summary.get("total_duration_ms", 0),
                    "total_items": total_items,
                    "success_count": success,
                    "failed_count": failed,
                    "run_dir": run_dir,
                })

        # Write consolidated files
        if all_result_tables:
            pq.write_table(pa.concat_tables(all_result_tables), self.all_results_path)
        elif os.path.exists(self.all_results_path):
            os.remove(self.all_results_path)

        if all_index_rows:
            index_table = pa.table(
                {k: [r[k] for r in all_index_rows] for k in INDEX_SCHEMA.names},
                schema=INDEX_SCHEMA,
            )
            pq.write_table(index_table, self.index_path)
        elif os.path.exists(self.index_path):
            os.remove(self.index_path)
