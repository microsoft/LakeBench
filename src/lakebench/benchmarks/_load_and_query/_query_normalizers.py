import calendar
import datetime
from collections import defaultdict
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List, Mapping, Optional, Tuple, Type

import sqlglot
from sqlglot import exp
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.scope import Scope, build_scope

from ...engines.base import BaseEngine


@dataclass(frozen=True)
class QueryNormalizerContext:
    query_name: str
    schema: Dict[str, Dict[str, str]]
    dialect: str
    source_sql: str = ""
    target_dialect: Optional[str] = None


QueryNormalizer = Callable[[exp.Expression, QueryNormalizerContext], None]
QueryNormalizerRegistry = Mapping[str, Tuple[QueryNormalizer, ...]]
SourceNormalizer = Callable[[List[exp.Expression], QueryNormalizerContext], None]
SourceNormalizerRegistry = Mapping[str, Tuple[SourceNormalizer, ...]]
EngineNormalizerRegistry = Mapping[Type[BaseEngine], QueryNormalizerRegistry]

TPC_ANSI_READ_DIALECT = "tsql"


def parse_tpc_ansi_statements(query: str) -> List[exp.Expression]:
    """Uses the built-in reader that accepts TOP without enabling safe or floating-point division."""
    return [
        statement
        for statement in sqlglot.parse(query, read=TPC_ANSI_READ_DIALECT)
        if statement is not None and not isinstance(statement, exp.Semicolon)
    ]


def apply_source_normalizers(
    statements: List[exp.Expression],
    context: QueryNormalizerContext,
    registry: SourceNormalizerRegistry,
    applied_rules: Optional[List[str]] = None,
) -> exp.Expression:
    normalized = [statement.copy() for statement in statements]
    for key in ("*", context.query_name):
        for rule in registry.get(key, ()):
            rule(normalized, context)
            if applied_rules is not None:
                applied_rules.append(f"source:{rule.__module__}.{rule.__name__}")
    if len(normalized) != 1 or not isinstance(normalized[0], exp.Query):
        raise ValueError(
            f"Expected one executable query after source normalization for {context.query_name}; "
            f"found {[type(statement).__name__ for statement in normalized]}."
        )
    return normalized[0]


def normalize_date_interval_arithmetic(
    expression: exp.Expression,
    context: QueryNormalizerContext,
) -> None:
    """Lowers DATE casts +/- whole DAY/MONTH/YEAR intervals to portable DateAdd nodes."""
    for interval in list(expression.find_all(exp.Interval)):
        arithmetic = interval.parent
        if not isinstance(arithmetic, (exp.Add, exp.Sub)) or arithmetic.expression is not interval:
            raise ValueError(f"Expected DATE +/- INTERVAL arithmetic in {context.query_name}.")
        date = arithmetic.this
        if not isinstance(date, exp.Cast) or date.args["to"].this != exp.DataType.Type.DATE:
            raise ValueError(f"Expected an explicit DATE cast before INTERVAL in {context.query_name}.")
        unit = interval.args.get("unit")
        if unit is None or unit.name.upper() not in {"DAY", "MONTH", "YEAR"}:
            raise ValueError(f"Expected a DAY, MONTH, or YEAR interval in {context.query_name}.")
        value = interval.this
        if not isinstance(value, exp.Literal):
            raise ValueError(f"Expected a literal interval amount in {context.query_name}.")
        try:
            amount = int(value.this)
        except ValueError as error:
            raise ValueError(f"Expected a whole-number interval amount in {context.query_name}.") from error
        if isinstance(arithmetic, exp.Sub):
            amount = -amount
        amount_expression: exp.Expression = exp.Literal.number(abs(amount))
        # A Neg node lets DuckDB render the required INTERVAL (-N) syntax.
        if amount < 0:
            amount_expression = exp.Neg(this=amount_expression)
        arithmetic.replace(
            exp.DateAdd(
                this=date.copy(),
                expression=amount_expression,
                unit=exp.Var(this=unit.name.upper()),
            )
        )


def _parse_date_literal(value: str) -> Optional[datetime.date]:
    """Parses a generated date literal, which may use unpadded month or day fields."""
    parts = value.strip().split("-")
    if len(parts) != 3:
        return None
    try:
        year, month, day = (int(part) for part in parts)
        return datetime.date(year, month, day)
    except ValueError:
        return None


def _fold_date_literal(date: datetime.date, amount: int, unit: str) -> datetime.date:
    """Applies a whole DAY, MONTH, or YEAR offset to a constant date."""
    if unit == "DAY":
        return date + datetime.timedelta(days=amount)
    months = amount if unit == "MONTH" else amount * 12
    total = (date.year * 12 + date.month - 1) + months
    year, month = divmod(total, 12)
    month += 1
    # Clamp to the last valid day, matching every supported engine's month arithmetic.
    day = min(date.day, calendar.monthrange(year, month)[1])
    return datetime.date(year, month, day)


def fold_constant_date_arithmetic(
    expression: exp.Expression,
    context: QueryNormalizerContext,
) -> None:
    """Evaluates DateAdd over a constant date, for engines whose SQL parser rejects intervals.

    Polars' SQL frontend cannot parse any interval syntax, so the portable
    ``DateAdd`` nodes produced by ``normalize_date_interval_arithmetic`` fail to
    render for it even though its DuckDB output dialect is valid DuckDB. Both
    operands of these generated offsets are literals, so evaluating them here
    produces exactly the date the other engines compute at runtime.

    Only fully constant offsets are folded. A ``DateAdd`` over a column, such as
    TPC-DS q72's ``d1.d_date + 5``, is left untouched: there is nothing to
    evaluate, and substituting a value would change the query.
    """
    for offset in list(expression.find_all(exp.DateAdd)):
        date = offset.this
        if not isinstance(date, exp.Cast) or date.args["to"].this != exp.DataType.Type.DATE:
            continue
        literal = date.this
        if not isinstance(literal, exp.Literal) or not literal.is_string:
            continue
        unit = offset.args.get("unit")
        if unit is None or unit.name.upper() not in {"DAY", "MONTH", "YEAR"}:
            continue
        amount_expression = offset.expression
        negated = isinstance(amount_expression, exp.Neg)
        if negated:
            amount_expression = amount_expression.this
        if not isinstance(amount_expression, exp.Literal) or amount_expression.is_string:
            continue
        try:
            amount = int(amount_expression.this)
        except ValueError:
            continue
        start = _parse_date_literal(literal.this)
        if start is None:
            continue
        folded = _fold_date_literal(start, -amount if negated else amount, unit.name.upper())
        offset.replace(
            exp.Cast(
                this=exp.Literal.string(folded.isoformat()),
                to=exp.DataType.build("DATE"),
            )
        )


def load_query_schema(ddl: str, dialect: str = "spark") -> Dict[str, Dict[str, str]]:
    schema = {}
    for statement in sqlglot.parse(ddl, read=dialect):
        if not isinstance(statement, exp.Create) or not isinstance(statement.this, exp.Schema):
            continue
        table_name = statement.this.this.name
        schema[table_name] = {
            column.this.name: column.args["kind"].sql(dialect=dialect) for column in statement.this.expressions
        }
    if not schema:
        raise ValueError("Query-normalization DDL does not contain table definitions.")
    return schema


def _conjunctions(condition: Optional[exp.Expression]) -> List[exp.Expression]:
    if condition is None:
        return []
    if isinstance(condition, exp.And):
        return list(condition.flatten())
    return [condition]


def _combine_conjunctions(
    predicates: Iterable[exp.Expression],
) -> Optional[exp.Expression]:
    predicates = list(predicates)
    if not predicates:
        return None
    return exp.and_(*predicates, copy=False)


def _scope_aliases(scope: Scope) -> List[str]:
    select = scope.expression
    from_ = select.args.get("from_") or select.args.get("from")
    if not from_:
        return []
    return [from_.this.alias_or_name] + [join.this.alias_or_name for join in select.args.get("joins") or []]


def _paired_scopes(
    expression: exp.Expression,
    schema: Dict[str, Dict[str, str]],
    dialect: str,
) -> List[Tuple[Scope, Scope]]:
    original_root = build_scope(expression)
    qualified_root = build_scope(
        qualify(
            expression.copy(),
            dialect=dialect,
            schema=schema,
            validate_qualify_columns=False,
            quote_identifiers=False,
            identify=False,
        )
    )
    if original_root is None or qualified_root is None:
        raise ValueError("Query does not contain a selectable scope.")

    original_scopes = list(original_root.traverse())
    qualified_scopes = list(qualified_root.traverse())
    if len(original_scopes) != len(qualified_scopes):
        raise ValueError("Schema qualification changed the query scope count.")

    pairs = list(zip(original_scopes, qualified_scopes))
    for original_scope, qualified_scope in pairs:
        if len(_scope_aliases(original_scope)) != len(_scope_aliases(qualified_scope)):
            raise ValueError("Schema qualification changed a query scope's relation count.")
    return pairs


def _is_implicit_join(join: exp.Join) -> bool:
    return not any(join.args.get(attribute) for attribute in ("on", "side", "kind", "using", "method"))


def _is_convertible_join(join: exp.Join) -> bool:
    return not any(join.args.get(attribute) for attribute in ("on", "side", "using", "method")) and join.kind in (
        "",
        "CROSS",
    )


def _convert_scope_joins(
    original_scope: Scope,
    qualified_scope: Scope,
    implicit_join_ids: set,
) -> None:
    select = original_scope.expression
    qualified_select = qualified_scope.expression
    joins = select.args.get("joins") or []
    if not joins:
        return
    # Do not move WHERE predicates to either side of a null-extending boundary.
    if any(join.side for join in joins):
        for join in joins:
            if id(join) in implicit_join_ids:
                join.set("kind", "CROSS")
        return

    aliases = _scope_aliases(qualified_scope)
    alias_positions = {alias: index for index, alias in enumerate(aliases)}
    original_where = select.args.get("where")
    qualified_where = qualified_select.args.get("where")
    original_predicates = _conjunctions(original_where.this if original_where else None)
    qualified_predicates = _conjunctions(qualified_where.this if qualified_where else None)
    if len(original_predicates) != len(qualified_predicates):
        raise ValueError("Schema qualification changed the WHERE predicate count.")

    join_predicates = defaultdict(list)
    remaining_predicates = []

    for original_predicate, qualified_predicate in zip(
        original_predicates,
        qualified_predicates,
    ):
        referenced_aliases = exp.column_table_names(qualified_predicate)
        positions = {alias_positions[alias] for alias in referenced_aliases if alias in alias_positions}
        target_position = max(positions) if positions else 0
        target_join_index = target_position - 1
        target_join = joins[target_join_index] if target_join_index >= 0 else None
        is_safe_join_predicate = (
            len(positions) >= 2
            and len(positions) == len(referenced_aliases)
            and min(positions) < target_position
            and target_join is not None
            and id(target_join) in implicit_join_ids
            and not qualified_predicate.find(exp.Or)
            and not qualified_predicate.find(exp.Query)
        )
        if is_safe_join_predicate:
            join_predicates[target_join_index].append(original_predicate)
        else:
            remaining_predicates.append(original_predicate)

    for index, join in enumerate(joins):
        predicates = join_predicates.get(index)
        if predicates:
            join.set("on", _combine_conjunctions(predicates))
            join.set("kind", None)
        elif id(join) in implicit_join_ids:
            join.set("kind", "CROSS")

    remaining_condition = _combine_conjunctions(remaining_predicates)
    if remaining_condition is None:
        select.set("where", None)
    elif original_where:
        original_where.set("this", remaining_condition)
    else:
        select.set("where", exp.Where(this=remaining_condition))


def normalize_implicit_joins(
    expression: exp.Expression,
    context: QueryNormalizerContext,
) -> None:
    """Converts implicit joins to JOIN ... ON or explicit CROSS JOIN."""
    implicit_join_ids = {
        id(join) for join in expression.find_all(exp.Join) if _is_implicit_join(join) or _is_convertible_join(join)
    }
    for original_scope, qualified_scope in _paired_scopes(
        expression,
        context.schema,
        context.dialect,
    ):
        _convert_scope_joins(original_scope, qualified_scope, implicit_join_ids)


def apply_query_normalizers(
    expression: exp.Expression,
    query_name: str,
    schema: Dict[str, Dict[str, str]],
    dialect: str,
    query_normalizers: QueryNormalizerRegistry,
    engine_type: Optional[Type[BaseEngine]] = None,
    engine_normalizers: Optional[EngineNormalizerRegistry] = None,
    applied_rules: Optional[List[str]] = None,
    target_dialect: Optional[str] = None,
) -> exp.Expression:
    normalized = expression.copy()
    context = QueryNormalizerContext(
        query_name=query_name,
        schema=schema,
        dialect=dialect,
        target_dialect=target_dialect,
    )
    for registry_key in ("*", query_name):
        for normalizer in query_normalizers.get(registry_key, ()):
            normalizer(normalized, context)
            if applied_rules is not None:
                applied_rules.append(f"canonical:{normalizer.__module__}.{normalizer.__name__}")
    if engine_type is not None and engine_normalizers:
        for cls in reversed(engine_type.__mro__):
            registry = engine_normalizers.get(cls, {})
            for key in ("*", query_name):
                for normalizer in registry.get(key, ()):
                    normalizer(normalized, context)
                    if applied_rules is not None:
                        applied_rules.append(f"engine:{normalizer.__module__}.{normalizer.__name__}")
    return normalized


def implicit_join_count(expression: exp.Expression) -> int:
    return sum(1 for join in expression.find_all(exp.Join) if _is_implicit_join(join))
