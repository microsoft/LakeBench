import hashlib
import json
from pathlib import Path

import pytest
import sqlglot

from lakebench.benchmarks import TPCDS, TPCH
from lakebench.engines.duckdb import DuckDB
from lakebench.engines.spark import Spark
from tests.test_tpch_query_generation import _uninitialized_engine

SNAPSHOTS = json.loads((Path(__file__).parent / "fixtures" / "tpc_query_rendering.json").read_text(encoding="utf-8"))


@pytest.mark.parametrize("benchmark_class", [TPCH, TPCDS])
@pytest.mark.parametrize("scale", [1000, 10000])
@pytest.mark.parametrize("dialect", ["spark", "tsql", "fabric"])
def test_tpc_runtime_sql_matches_reviewed_sqlglot_snapshots(benchmark_class, scale, dialect):
    assert sqlglot.__version__ in SNAPSHOTS["versions"], (
        "Review query-output differences before updating SQLGlot snapshots."
    )
    expected = dict(SNAPSHOTS["sha256"])
    expected.update(SNAPSHOTS["overrides"].get(sqlglot.__version__, {}))
    engine = _uninitialized_engine(Spark if dialect == "spark" else DuckDB)
    engine.SQLGLOT_DIALECT = dialect
    engine.schema_name = "dbo"
    benchmark = benchmark_class(
        engine=engine,
        scenario_name="sqlglot-snapshots",
        scale_factor=scale,
        input_parquet_folder_uri="file:///tmp/tpc",
    )
    prefix = f"{benchmark_class.__name__.lower()}/sf{scale}/{dialect}/"
    assert {key[len(prefix) :] for key in expected if key.startswith(prefix)} == set(benchmark.QUERY_REGISTRY)
    for name in benchmark.QUERY_REGISTRY:
        rendered = benchmark._return_query_definition(name)
        assert hashlib.sha256(rendered.encode("utf-8")).hexdigest() == expected[prefix + name], prefix + name


def test_not_like_spelling_preserves_null_and_pattern_semantics():
    duckdb = pytest.importorskip("duckdb")
    with duckdb.connect() as connection:
        rows = connection.execute(
            "SELECT NOT value LIKE '%accounts%', value NOT LIKE '%accounts%' "
            "FROM (VALUES ('unusual accounts'), ('regular orders'), (NULL)) t(value)"
        ).fetchall()
    assert rows == [(False, False), (True, True), (None, None)]
