import hashlib
import json
import re
from pathlib import Path

import pytest
import sqlglot
from sqlglot import exp

from lakebench.benchmarks import ClickBench
from lakebench.benchmarks.clickbench._query_normalizers import (
    NORMALIZER_VERSION,
    QUERY_NORMALIZERS,
)
from lakebench.engines.daft import Daft
from lakebench.engines.duckdb import DuckDB
from lakebench.engines.polars import Polars
from lakebench.engines.sail import Sail
from lakebench.engines.spark import Spark
from tests.conftest import _uninitialized_engine

CANONICAL_ROOT = (
    Path(__file__).parents[1]
    / "src"
    / "lakebench"
    / "benchmarks"
    / "clickbench"
    / "resources"
    / "queries"
    / "canonical"
)
FIXTURE = Path(__file__).parent / "fixtures" / "clickbench_query_rendering.json"
RENDER_DIALECTS = ("spark", "tsql", "fabric", "duckdb", "mysql")
QUERY_NAMES = tuple(f"q{number}" for number in range(1, 44))


def _manifest():
    return json.loads((CANONICAL_ROOT / "source_manifest.json").read_text(encoding="utf-8"))


def _canonical_bytes(query_name):
    """Reads a canonical source with line endings normalized to the upstream form."""
    return (CANONICAL_ROOT / f"{query_name}.sql").read_bytes().replace(b"\r\n", b"\n")


def _rendered(engine_class, dialect=None):
    engine = _uninitialized_engine(engine_class)
    if dialect is not None:
        engine.SQLGLOT_DIALECT = dialect
    benchmark = ClickBench(
        engine=engine,
        scenario_name="rendering",
        input_parquet_folder_uri="file:///tmp/clickbench",
    )
    return {name: benchmark._return_query_definition(name) for name in QUERY_NAMES}


def test_canonical_sources_match_pinned_upstream_manifest():
    manifest = _manifest()
    assert manifest["upstream_path"] == "clickhouse/queries.sql"
    assert manifest["query_count"] == len(QUERY_NAMES)
    assert ClickBench.VERSION.endswith(manifest["upstream_commit"][:7])

    combined = b"".join(_canonical_bytes(name) for name in QUERY_NAMES)
    assert hashlib.sha256(combined).hexdigest() == manifest["source_sha256"]
    for name in QUERY_NAMES:
        assert hashlib.sha256(_canonical_bytes(name)).hexdigest() == manifest["sha256"][f"{name}.sql"]


@pytest.mark.parametrize("query_name", QUERY_NAMES)
def test_canonical_sources_are_single_unmodified_statements(query_name):
    source = _canonical_bytes(query_name).decode("utf-8")
    assert source.endswith(";\n")
    assert source.count(";") == 1
    # Upstream ships one statement per line; anything else means the file was edited.
    assert source.count("\n") == 1


def test_clickbench_reads_clickhouse_sources_without_overrides():
    assert ClickBench.CANONICAL_QUERY_DIALECT == "clickhouse"
    assert ClickBench.ALLOW_QUERY_OVERRIDES is False
    assert ClickBench.QUERY_NORMALIZERS is QUERY_NORMALIZERS


def test_query_set_provenance_is_reported_as_engine_metadata():
    manifest = _manifest()
    engine = _uninitialized_engine(DuckDB)
    ClickBench(
        engine=engine,
        scenario_name="provenance",
        input_parquet_folder_uri="file:///tmp/clickbench",
    )
    assert engine.extended_engine_metadata["query_set_revision"] == manifest["upstream_commit"]
    assert engine.extended_engine_metadata["query_set_license"] == "CC-BY-NC-SA-4.0"
    assert engine.extended_engine_metadata["query_set_normalizer_version"] == NORMALIZER_VERSION


@pytest.mark.parametrize("dialect", RENDER_DIALECTS)
def test_rendered_queries_match_recorded_fingerprints(dialect):
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    assert sqlglot.__version__ in fixture["versions"], (
        "Review query-output differences before updating ClickBench rendering fingerprints."
    )
    expected = dict(fixture["sha256"][dialect])
    for key, digest in fixture["overrides"].get(sqlglot.__version__, {}).items():
        override_dialect, _, query_name = key.partition("/")
        if override_dialect == dialect:
            expected[query_name] = digest
    rendered = _rendered(Spark, dialect)
    for name in QUERY_NAMES:
        assert hashlib.sha256(rendered[name].encode("utf-8")).hexdigest() == expected[name], name


@pytest.mark.parametrize("engine_class", [Spark, DuckDB, Sail, Polars, Daft])
def test_every_query_renders_and_parses_for_each_supported_engine(engine_class):
    rendered = _rendered(engine_class)
    dialect = engine_class.SQLGLOT_DIALECT
    for name in QUERY_NAMES:
        statements = sqlglot.parse(rendered[name], read=dialect)
        assert len(statements) == 1, name
        assert isinstance(statements[0], exp.Query), name


@pytest.mark.parametrize("dialect", RENDER_DIALECTS)
def test_minute_truncation_is_preserved_for_every_target(dialect):
    query = _rendered(Spark, dialect)["q43"].upper()
    assert "MINUTE" in query
    # A date-only truncation silently discards the requested precision.
    assert not re.search(r"\bTRUNC\(\s*EVENTTIME\s*,", query)
    assert not re.search(r"\bDATE\(\s*EVENTTIME\s*\)", query)


@pytest.mark.parametrize("dialect", ["tsql", "fabric"])
def test_tsql_grouping_keys_are_expressions_rather_than_aliases_or_positions(dialect):
    rendered = _rendered(Spark, dialect)

    grouped = sqlglot.parse_one(rendered["q35"], read=dialect)
    assert [key.sql(dialect) for key in grouped.args["group"].expressions] == ["URL"]
    assert grouped.expressions[0].sql(dialect) == "1"

    for name, aliases in (("q19", ["m"]), ("q29", ["k"]), ("q40", ["Src", "Dst"])):
        group = sqlglot.parse_one(rendered[name], read=dialect).args["group"]
        names = {key.name for key in group.find_all(exp.Column)}
        assert not names & set(aliases), (name, names)


@pytest.mark.parametrize("dialect", ["spark", "duckdb", "mysql"])
def test_other_targets_keep_the_upstream_grouping_form(dialect):
    rendered = _rendered(Spark, dialect)
    assert "GROUP BY\n  1,\n  URL" in rendered["q35"]
    assert re.search(r"GROUP BY\s+k\b", rendered["q29"])


@pytest.mark.parametrize("query_name", ["q3", "q4", "q10", "q28", "q31", "q32", "q33"])
def test_tsql_average_widening_uses_a_real_type_rather_than_an_integer_one(query_name):
    """Keeps AVG returning a fraction, matching ClickHouse's Float64 result.

    Earlier Fabric Warehouse overrides widened these with BIGINT or
    DECIMAL(38, 0). That prevents the overflow but makes T-SQL truncate the
    average to a whole number, which silently diverges from the benchmark.
    """
    for aggregate in sqlglot.parse_one(_rendered(Spark, "fabric")[query_name], read="fabric").find_all(exp.Avg):
        assert isinstance(aggregate.this, exp.Cast), query_name
        assert aggregate.this.args["to"].this in exp.DataType.REAL_TYPES, query_name


@pytest.mark.parametrize(
    ("query_name", "aggregate", "expected_type"),
    [("q30", "SUM", "BIGINT"), ("q3", "AVG", "FLOAT"), ("q4", "AVG", "FLOAT")],
)
def test_integer_aggregates_are_widened_only_for_tsql(query_name, aggregate, expected_type):
    tsql = _rendered(Spark, "tsql")[query_name]
    assert f"{aggregate}(CAST(" in tsql
    assert f"AS {expected_type})" in tsql
    for dialect in ("spark", "duckdb", "mysql"):
        assert "CAST(" not in _rendered(Spark, dialect)[query_name]


def test_q30_widens_every_generated_sum_without_changing_its_addends():
    tsql = sqlglot.parse_one(_rendered(Spark, "tsql")["q30"], read="tsql")
    sums = list(tsql.find_all(exp.Sum))
    assert len(sums) == 90
    addends = []
    for aggregate in sums:
        assert isinstance(aggregate.this, exp.Cast)
        assert aggregate.this.args["to"].this == exp.DataType.Type.BIGINT
        inner = aggregate.this.this
        addends.append(int(inner.expression.this) if isinstance(inner, exp.Add) else 0)
    assert addends == list(range(0, 90))


@pytest.mark.parametrize(
    ("dialect", "expected_backreference"),
    [("spark", "'$1'"), ("mysql", "'$1'"), ("duckdb", "'\\1'")],
)
def test_q29_uses_each_regex_engines_backreference_syntax(dialect, expected_backreference):
    query = _rendered(Spark, dialect)["q29"]
    assert "REGEXP_REPLACE" in query
    assert expected_backreference in query


@pytest.mark.parametrize("dialect", ["tsql", "fabric"])
def test_q29_is_lowered_to_native_string_operations_for_tsql(dialect):
    query = _rendered(Spark, dialect)["q29"]
    assert "REGEXP_REPLACE" not in query
    assert "CHARINDEX" in query and "SUBSTRING" in query
    assert "Latin1_General_100_BIN2_UTF8" in query


@pytest.mark.parametrize("dialect", RENDER_DIALECTS)
def test_normalization_is_idempotent_and_preserves_generated_literals(dialect):
    first = _rendered(Spark, dialect)
    assert first == _rendered(Spark, dialect)
    for name in QUERY_NAMES:
        source = _canonical_bytes(name).decode("utf-8")
        for literal in re.findall(r"'([^']*)'", source):
            # A DATE_TRUNC unit is a keyword in some targets, so compare case-insensitively.
            if literal and "\\" not in literal:
                assert literal.lower() in first[name].lower(), (name, literal)


def _q29_host_expression(dialect):
    """Extracts the lowered host expression, rewritten to run under DuckDB."""
    expression = sqlglot.parse_one(_rendered(Spark, dialect)["q29"], read=dialect)
    lowered = next(
        projection.this
        for projection in expression.expressions
        if isinstance(projection, exp.Alias) and projection.alias == "k"
    ).copy()
    # DuckDB has no COLLATE clause for these binary collations; the binary
    # collation only enforces the case sensitivity DuckDB already applies.
    for collate in list(lowered.find_all(exp.Collate)):
        collate.replace(collate.this.copy())
    for column in lowered.find_all(exp.Column):
        column.replace(exp.column("referer"))
    return lowered


def _evaluate_q29(duckdb, dialect, samples):
    values = ", ".join("(NULL)" if sample is None else "('%s')" % sample.replace("'", "''") for sample in samples)
    return duckdb.sql(
        "SELECT referer, %s FROM (VALUES %s) AS t(referer)" % (_q29_host_expression(dialect).sql("duckdb"), values)
    ).fetchall()


@pytest.mark.parametrize("dialect", ["tsql", "fabric"])
def test_q29_lowering_strips_the_optional_www_prefix(dialect):
    """Guards the grouping key against a historical hand-written override.

    An earlier Fabric Warehouse override tested ``CHARINDEX('www.', Referer) = 1``
    against the raw Referer. That is never true, because a matching Referer
    begins with its scheme, so the prefix was never stripped and ``www.host``
    and ``host`` were counted as separate groups. The source pattern treats them
    as one, which changes both the grouping and the reported row count.
    """
    duckdb = pytest.importorskip("duckdb")
    rows = _evaluate_q29(
        duckdb,
        dialect,
        ["http://www.example.com/path", "https://www.example.com/a/b", "http://example.com/path"],
    )
    assert {host for _, host in rows} == {"example.com"}


@pytest.mark.parametrize("dialect", ["tsql", "fabric"])
def test_q29_lowering_matches_the_upstream_pattern(dialect):
    """Executes the lowered host extraction against DuckDB and the source regex."""
    duckdb = pytest.importorskip("duckdb")

    samples = [
        "http://example.com/path",
        "https://www.example.com/a/b?q=1",
        "http://www./path",
        "http://www.example.com/",
        "HTTP://Example.com/path",
        "ftp://example.com/path",
        "http://example.com",
        "https://пример.рф/страница",
        "http://example.com/a\nb",
        "http://exa\nmple.com/path",
        "http://www.example.com/path ",
        "",
        None,
    ]
    rows = _evaluate_q29(duckdb, dialect, samples)

    # RE2 leaves `.` excluding newline and anchors `$` at end of text, so a line
    # feed in the path prevents a match and the original Referer is returned.
    pattern = re.compile(r"^https?://(?:www\.)?([^/]+)/.*\Z")
    for referer, actual in rows:
        if referer is None:
            assert actual is None
            continue
        match = pattern.match(referer)
        expected = match.group(1) if match else referer
        assert actual == expected, referer
