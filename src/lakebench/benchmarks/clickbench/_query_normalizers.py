"""Registered AST rules that make the immutable ClickBench sources portable.

The canonical sources are the exact upstream ClickHouse statements, so every
engine accommodation below is a structural rule applied to the parsed tree.
Rules never alter generated literals, projections, filters, grouping keys, or
limits; they only re-express an operation the target dialect spells differently
or evaluates with a narrower type than ClickHouse.
"""

from typing import Dict, List, Optional, Tuple

from sqlglot import exp

from ...engines.fabric_data_warehouse import FabricDataWarehouse
from .._load_and_query._query_normalizers import (
    EngineNormalizerRegistry,
    QueryNormalizer,
    QueryNormalizerContext,
)

NORMALIZER_VERSION = "2"

# Dialects whose replacement strings use Java-style group references.
_DOLLAR_BACKREFERENCE_TARGETS = frozenset({"spark", "databricks", "mysql"})

_INTEGER_TYPES = frozenset(
    {
        exp.DataType.Type.TINYINT,
        exp.DataType.Type.SMALLINT,
        exp.DataType.Type.INT,
        exp.DataType.Type.BIGINT,
    }
)

_Q29_PATTERN = "^https?://(?:www\\.)?([^/]+)/.*$"
_BINARY_COLLATION = "Latin1_General_100_BIN2_UTF8"


def _column_type(column: exp.Column, context: QueryNormalizerContext) -> Optional[str]:
    """Resolves a column's declared type from the benchmark DDL schema."""
    for columns in context.schema.values():
        for name, data_type in columns.items():
            if name.lower() == column.name.lower():
                return data_type
    return None


def _selects(expression: exp.Expression) -> List[exp.Select]:
    return list(expression.find_all(exp.Select))


def _projection_aliases(select: exp.Select) -> Dict[str, exp.Expression]:
    return {
        projection.alias.lower(): projection.this
        for projection in select.expressions
        if isinstance(projection, exp.Alias) and projection.alias
    }


def _expand_group_by_aliases(expression: exp.Expression, context: QueryNormalizerContext) -> None:
    """Replaces SELECT aliases used as grouping keys, which T-SQL does not resolve."""
    for select in _selects(expression):
        group = select.args.get("group")
        if group is None:
            continue
        aliases = _projection_aliases(select)
        for key in list(group.expressions):
            if not isinstance(key, exp.Column) or key.table:
                continue
            aliased = aliases.get(key.name.lower())
            if aliased is None:
                continue
            if _column_type(key, context) is not None:
                raise ValueError(f"{context.query_name} grouping key {key.name} is both a column and an alias.")
            key.replace(aliased.copy())


def _resolve_positional_group_by(expression: exp.Expression, context: QueryNormalizerContext) -> None:
    """Resolves ClickHouse positional grouping keys, which T-SQL does not accept."""
    for select in _selects(expression):
        group = select.args.get("group")
        if group is None:
            continue
        projections = select.expressions
        for key in list(group.expressions):
            if not isinstance(key, exp.Literal) or key.is_string:
                continue
            position = int(key.this)
            if not 1 <= position <= len(projections):
                raise ValueError(f"{context.query_name} grouping position {position} has no matching projection.")
            projection = projections[position - 1]
            referenced = projection.this if isinstance(projection, exp.Alias) else projection
            # A constant projection contributes no grouping key in any dialect.
            if isinstance(referenced, exp.Literal):
                group.expressions.remove(key)
            else:
                key.replace(referenced.copy())
        if not group.expressions:
            select.set("group", None)


def _normalize_native_length(expression: exp.Expression, context: QueryNormalizerContext) -> None:
    """Renders each target's native LENGTH, matching upstream's per-engine query sets.

    The ClickHouse reader marks ``length`` as a byte-length call because
    ``length(String)`` counts bytes. Upstream ClickBench does not preserve that
    across engines: its DuckDB and Spark query sets call each engine's native
    ``length``, which counts characters. Clearing the flag reproduces those
    upstream variants and avoids DuckDB's ``CASE TYPEOF`` byte-length expansion,
    which is not portable to the other engines sharing that dialect.
    """
    for length in list(expression.find_all(exp.Length)):
        length.set("binary", None)


def _is_integer_expression(node: exp.Expression, context: QueryNormalizerContext) -> bool:
    """Reports whether a target evaluates an expression using integer arithmetic."""
    if isinstance(node, exp.Column):
        data_type = _column_type(node, context)
        return data_type is not None and exp.DataType.build(data_type).this in _INTEGER_TYPES
    if isinstance(node, exp.Literal):
        return not node.is_string and "." not in node.this
    if isinstance(node, exp.Length):
        return True
    if isinstance(node, (exp.Paren, exp.Neg)):
        return _is_integer_expression(node.this, context)
    if isinstance(node, (exp.Add, exp.Sub, exp.Mul)):
        return _is_integer_expression(node.this, context) and _is_integer_expression(node.expression, context)
    return False


def _widen_integer_aggregates(expression: exp.Expression, context: QueryNormalizerContext) -> None:
    """Widens integer SUM and AVG inputs, which T-SQL otherwise overflows or truncates."""
    for aggregate in list(expression.find_all(exp.Sum, exp.Avg)):
        argument = aggregate.this
        if argument is None or isinstance(argument, exp.Cast):
            continue
        if not _is_integer_expression(argument, context):
            continue
        # SUM(int) accumulates in INT and AVG(int) truncates, while ClickHouse
        # returns a 64-bit sum and a Float64 average. AVG must widen to a real
        # type: BIGINT or DECIMAL(38, 0) would stop the overflow but keep
        # truncating the result to a whole number.
        to_type = "BIGINT" if isinstance(aggregate, exp.Sum) else "FLOAT"
        aggregate.set("this", exp.Cast(this=argument.copy(), to=exp.DataType.build(to_type)))


def _normalize_minute_truncation(expression: exp.Expression, context: QueryNormalizerContext) -> None:
    """Truncates a timestamp rather than a date, which several targets silently coerce."""
    truncations = list(expression.find_all(exp.DateTrunc))
    if not truncations:
        raise ValueError(f"Expected DATE_TRUNC in {context.query_name}.")
    for truncation in truncations:
        unit = truncation.args.get("unit")
        if unit is None:
            raise ValueError(f"Expected a DATE_TRUNC unit in {context.query_name}.")
        # DateTrunc renders as a date-only TRUNC for Spark and drops to DATE()
        # for MySQL, both of which discard the requested minute precision.
        truncation.replace(
            exp.TimestampTrunc(
                this=truncation.this.copy(),
                unit=exp.Var(this=unit.name.upper()),
            )
        )


def _lower_q29_referer(referer: exp.Expression) -> exp.Expression:
    """Expresses q29's fixed host extraction with native, binary-collated string operations.

    Two guarded branches recognize the exact lowercase http:// and https://
    prefixes, locate the first slash after the prefix, require a non-empty host,
    and reject a line feed after that slash. A line feed inside the host stays
    allowed. The optional www. is removed only when at least one host character
    remains, which reproduces the backtracking behaviour of the source pattern
    for http://www./path. Non-matching input and NULL return the original value.
    Every SUBSTRING length is clamped at zero rather than relying on CASE
    evaluation order.
    """

    def number(value: int) -> exp.Literal:
        return exp.Literal.number(value)

    def source() -> exp.Collate:
        # A binary collation forces case-sensitive matching and keeps every
        # position and subsequence offset on the same UTF-8 encoded input.
        return exp.Collate(this=referer.copy(), expression=exp.Var(this=_BINARY_COLLATION))

    def substring(start: int, length: exp.Expression) -> exp.Substring:
        return exp.Substring(this=source(), start=number(start), length=length)

    def branch(scheme: str, start: int) -> exp.If:
        def slash() -> exp.StrPosition:
            return exp.StrPosition(this=source(), substr=exp.Literal.string("/"), position=number(start))

        def host(host_start: int) -> exp.Substring:
            length = exp.Case(
                ifs=[
                    exp.If(
                        this=exp.GT(this=slash(), expression=number(host_start)),
                        true=exp.Sub(this=slash(), expression=number(host_start)),
                    )
                ],
                default=number(0),
            )
            return substring(host_start, length)

        prefix_matches = exp.EQ(
            this=substring(1, number(start - 1)),
            expression=exp.Literal.string(scheme),
        )
        nonempty_host = exp.GT(this=slash(), expression=number(start))
        path_has_no_line_feed = exp.EQ(
            this=exp.StrPosition(
                this=source(),
                substr=exp.Chr(expressions=[number(10)]),
                position=exp.Add(this=slash(), expression=number(1)),
            ),
            expression=number(0),
        )
        removable_www = exp.and_(
            exp.EQ(this=substring(start, number(4)), expression=exp.Literal.string("www.")),
            exp.GT(this=slash(), expression=number(start + 4)),
        )
        replacement = exp.Case(
            ifs=[exp.If(this=removable_www, true=host(start + 4))],
            default=host(start),
        )
        return exp.If(
            this=exp.and_(prefix_matches, nonempty_host, path_has_no_line_feed),
            true=replacement,
        )

    return exp.Case(
        ifs=[branch("http://", 8), branch("https://", 9)],
        default=referer.copy(),
    )


def _q29_replacements(expression: exp.Expression, context: QueryNormalizerContext) -> List[exp.RegexpReplace]:
    """Returns q29's host-extraction calls after checking the pattern is unchanged."""
    replacements = list(expression.find_all(exp.RegexpReplace))
    if not replacements:
        raise ValueError(f"Expected REGEXP_REPLACE in {context.query_name}.")
    for replacement in replacements:
        pattern = replacement.expression
        if not isinstance(pattern, exp.Literal) or pattern.this != _Q29_PATTERN:
            raise ValueError(f"Unexpected q29 pattern: {pattern}.")
    return replacements


def _normalize_q29_backreference(expression: exp.Expression, context: QueryNormalizerContext) -> None:
    """Rewrites q29's capture-group reference for targets that read \\1 as a literal digit."""
    for replacement in _q29_replacements(expression, context):
        if (context.target_dialect or "") in _DOLLAR_BACKREFERENCE_TARGETS:
            # Java-style replacement strings read \1 as a literal digit.
            replacement.set("replacement", exp.Literal.string("$1"))


def _lower_q29_host_extraction(expression: exp.Expression, context: QueryNormalizerContext) -> None:
    """Lowers q29's host extraction to native string operations.

    Fabric Data Warehouse does not expose REGEXP_REPLACE in its T-SQL surface area,
    so the fixed pattern is re-expressed rather than substituting a different
    workload.
    """
    for replacement in _q29_replacements(expression, context):
        replacement.replace(_lower_q29_referer(replacement.this))


QUERY_NORMALIZERS: Dict[str, Tuple[QueryNormalizer, ...]] = {
    "*": (_normalize_native_length,),
    "q29": (_normalize_q29_backreference,),
    "q43": (_normalize_minute_truncation,),
}

#: Accommodations for engines whose renderer emits the SQL Server grammar. They
#: are registered per engine rather than gated on the target dialect so that a
#: new T-SQL-family engine opts in explicitly.
ENGINE_QUERY_NORMALIZERS: EngineNormalizerRegistry = {
    FabricDataWarehouse: {
        "*": (
            _resolve_positional_group_by,
            _expand_group_by_aliases,
            _widen_integer_aggregates,
        ),
        "q29": (_lower_q29_host_extraction,),
    },
}
