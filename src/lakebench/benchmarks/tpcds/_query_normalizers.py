import re
from typing import Dict, List, Tuple

from sqlglot import exp

from ...engines.polars import Polars
from ...engines.sail import Sail
from .._load_and_query._query_normalizers import (
    EngineNormalizerRegistry,
    QueryNormalizer,
    QueryNormalizerContext,
    fold_constant_date_arithmetic,
    normalize_date_interval_arithmetic,
    parse_tpc_ansi_statements,
)

NORMALIZER_VERSION = "11"


def _normalize_ansi_syntax(query: str) -> str:
    query = re.sub(
        r"(?is)\bselect\s+top\s+(\d+)\s+distinct\s*\(([^()]+)\)",
        r"select distinct top \1 \2",
        query,
    )
    return re.sub(
        r"(?i)([+-])\s*(\d+)\s+days\b",
        r"\1 INTERVAL '\2' DAY",
        query,
    )


def _qualify_order_column(
    expression: exp.Expression,
    column_name: str,
    table_name: str,
) -> None:
    order = expression.args.get("order")
    unqualified = []
    qualified = []
    if order is not None:
        columns = [
            ordered.this
            for ordered in order.expressions
            if isinstance(ordered.this, exp.Column) and ordered.this.name == column_name
        ]
        unqualified = [column for column in columns if not column.table]
        qualified = [column for column in columns if column.table == table_name]
    if len(qualified) == 1 and not unqualified:
        return
    if len(unqualified) != 1 or qualified:
        raise ValueError(
            f"Expected one unqualified ORDER BY column '{column_name}' or "
            f"one qualified as '{table_name}.{column_name}'."
        )
    unqualified[0].set("table", exp.to_identifier(table_name))


def _normalize_q58(
    expression: exp.Expression,
    context: QueryNormalizerContext,
) -> None:
    _qualify_order_column(expression, "item_id", "ss_items")


def _normalize_q1_fee_identifier(
    expression: exp.Expression,
    context: QueryNormalizerContext,
) -> None:
    fees = [
        aggregate.this
        for aggregate in expression.find_all(exp.Sum)
        if isinstance(aggregate.this, exp.Column) and aggregate.this.name.casefold() == "sr_fee"
    ]
    if (
        len(fees) != 1
        or fees[0].table
        or fees[0].this.args.get("quoted")
        or "sr_fee" not in context.schema.get("store_returns", {})
    ):
        raise ValueError("Expected q1 to sum the unquoted store_returns.sr_fee column.")
    fees[0].this.set("this", "sr_fee")


def _normalize_tsql_sample_stddev(
    expression: exp.Expression,
    context: QueryNormalizerContext,
) -> None:
    if context.target_dialect not in {"tsql", "fabric"}:
        return
    for sample in list(expression.find_all(exp.StddevSamp)):
        sample.replace(exp.Stddev(**sample.copy().args))


def _normalize_q72(
    expression: exp.Expression,
    context: QueryNormalizerContext,
) -> None:
    _qualify_order_column(expression, "d_week_seq", "d1")


def _normalize_q72_date_offset(
    expression: exp.Expression,
    context: QueryNormalizerContext,
) -> None:
    date = exp.column("d_date", table="d1")
    offsets = [
        node
        for node in expression.find_all(exp.Add, exp.DateAdd)
        if node.this == date and node.expression == exp.Literal.number(5)
    ]
    if (
        len(offsets) != 1
        or context.schema.get("date_dim", {}).get("d_date") != "DATE"
        or not any(table.name == "date_dim" and table.alias == "d1" for table in expression.find_all(exp.Table))
    ):
        raise ValueError("Expected q72's d1.d_date + 5 day offset.")
    offset = offsets[0]
    if isinstance(offset, exp.DateAdd):
        unit = offset.args.get("unit")
        if unit is None or unit.name.upper() != "DAY":
            raise ValueError("Expected q72's normalized offset to use DAY.")
        return
    offset.replace(exp.DateAdd(this=date, expression=exp.Literal.number(5), unit=exp.Var(this="DAY")))


def _normalize_rollup_order_by(
    expression: exp.Expression,
    context: QueryNormalizerContext,
) -> None:
    aliases = [
        projection
        for projection in expression.expressions
        if isinstance(projection, exp.Alias) and projection.alias == "lochierarchy"
    ]
    order = expression.args.get("order")
    if len(aliases) != 1 or order is None:
        raise ValueError(f"Expected one lochierarchy projection and ORDER BY in {context.query_name}.")
    definition = aliases[0].this
    if not isinstance(definition, exp.Add) or any(
        not isinstance(term, exp.Func)
        or (term.name.upper() if isinstance(term, exp.Anonymous) else term.sql_name()) != "GROUPING"
        or len(term.expressions) != 1
        or not isinstance(term.expressions[0], exp.Column)
        for term in (definition.this, definition.expression)
    ):
        raise ValueError(f"Expected lochierarchy to sum two GROUPING expressions in {context.query_name}.")

    for ordered in order.expressions:
        if isinstance(ordered.this, exp.Column):
            continue
        for column in list(ordered.find_all(exp.Column)):
            if column.name == "lochierarchy" and not column.table and column.find_ancestor(exp.Select) is expression:
                column.replace(exp.Paren(this=definition.copy()))


def _normalize_q90(
    expression: exp.Expression,
    context: QueryNormalizerContext,
) -> None:
    aliases = {
        subquery.alias: subquery
        for subquery in expression.find_all(exp.Subquery)
        if subquery.alias in {"at", "pt", "am_counts", "pm_counts"}
    }
    if set(aliases) == {"am_counts", "pm_counts"}:
        return
    if set(aliases) != {"at", "pt"}:
        raise ValueError("Expected q90 derived-table aliases 'at' and 'pt' or their normalized names.")
    for old_alias, new_alias in (("at", "am_counts"), ("pt", "pm_counts")):
        aliases[old_alias].set(
            "alias",
            exp.TableAlias(this=exp.to_identifier(new_alias)),
        )


def _normalize_q9_wide_counts(
    expression: exp.Expression,
    context: QueryNormalizerContext,
) -> None:
    """Widens q9's bucket counters, which T-SQL otherwise overflows.

    Each bucket compares a threshold against COUNT(*) over store_sales filtered
    only by an ss_quantity range, so the count is a fifth of the fact table. At
    SF10000 that exceeds the 2,147,483,647 limit of T-SQL's INT counter and the
    query fails with "Arithmetic overflow error converting expression to data
    type int". The other TPC-DS queries that count a fact table without grouping
    (q88, q90, q96) bound the count with selective dimension joins.

    Only the counter width changes; Spark, DuckDB, and MySQL still render
    COUNT(*). Casting the result cannot help, because the overflow happens
    inside the aggregate.
    """
    counts = [count for count in expression.find_all(exp.Count) if isinstance(count.this, exp.Star)]
    if len(counts) != 5:
        raise ValueError(f"Expected five COUNT(*) bucket thresholds in q9, found {len(counts)}.")
    for count in counts:
        subquery = count.find_ancestor(exp.Subquery)
        if subquery is None or not isinstance(subquery.parent, exp.GT) or subquery.parent.this is not subquery:
            raise ValueError("Expected each q9 COUNT(*) to be a scalar subquery compared to a threshold.")
        count.set("big_int", True)


def _is_bigint_cast(expression: exp.Expression) -> bool:
    return (
        isinstance(expression, exp.Cast)
        and isinstance(expression.args.get("to"), exp.DataType)
        and expression.args["to"].this == exp.DataType.Type.BIGINT
    )


def _cast_bigint(expression: exp.Expression) -> exp.Expression:
    if _is_bigint_cast(expression):
        return expression
    return exp.Cast(
        this=expression.copy(),
        to=exp.DataType.build("BIGINT"),
    )


def _normalize_q22_inventory_average(
    expression: exp.Expression,
    context: QueryNormalizerContext,
) -> None:
    if context.target_dialect not in {"tsql", "fabric"}:
        return
    averages = [projection.this for projection in expression.expressions if projection.alias == "qoh"]
    if len(averages) != 1 or not isinstance(averages[0], exp.Avg):
        raise ValueError("Expected q22 qoh projection to average inv_quantity_on_hand.")
    operand = averages[0].this
    column = operand.this if _is_bigint_cast(operand) else operand
    if (
        column != exp.column("inv_quantity_on_hand")
        or context.schema.get("inventory", {}).get("inv_quantity_on_hand") != "INT"
    ):
        raise ValueError("Expected q22 to average the integer inventory.inv_quantity_on_hand column.")
    averages[0].set("this", _cast_bigint(operand))


def _normalize_q97(
    expression: exp.Expression,
    context: QueryNormalizerContext,
) -> None:
    cases = [aggregate.this for aggregate in expression.find_all(exp.Sum) if isinstance(aggregate.this, exp.Case)]
    if len(cases) != 3:
        raise ValueError(f"Expected three SUM(CASE ...) expressions in q97, found {len(cases)}.")

    for case in cases:
        branches = case.args.get("ifs") or []
        if len(branches) != 1 or case.args.get("default") is None:
            raise ValueError("Expected q97 CASE expressions with one branch and an ELSE value.")
        branch = branches[0]
        branch.set("true", _cast_bigint(branch.args["true"]))
        case.set("default", _cast_bigint(case.args["default"]))


QUERY_NORMALIZERS: Dict[str, Tuple[QueryNormalizer, ...]] = {
    "q1": (_normalize_q1_fee_identifier,),
    "q5": (normalize_date_interval_arithmetic,),
    "q9": (_normalize_q9_wide_counts,),
    "q12": (normalize_date_interval_arithmetic,),
    "q16": (normalize_date_interval_arithmetic,),
    "q17": (_normalize_tsql_sample_stddev,),
    "q20": (normalize_date_interval_arithmetic,),
    "q21": (normalize_date_interval_arithmetic,),
    "q22": (_normalize_q22_inventory_average,),
    "q29": (_normalize_tsql_sample_stddev,),
    "q32": (normalize_date_interval_arithmetic,),
    "q35": (_normalize_tsql_sample_stddev,),
    "q36": (_normalize_rollup_order_by,),
    "q37": (normalize_date_interval_arithmetic,),
    "q39a": (_normalize_tsql_sample_stddev,),
    "q39b": (_normalize_tsql_sample_stddev,),
    "q40": (normalize_date_interval_arithmetic,),
    "q58": (_normalize_q58,),
    "q70": (_normalize_rollup_order_by,),
    "q72": (_normalize_q72, _normalize_q72_date_offset),
    "q77": (normalize_date_interval_arithmetic,),
    "q80": (normalize_date_interval_arithmetic,),
    "q82": (normalize_date_interval_arithmetic,),
    "q86": (_normalize_rollup_order_by,),
    "q90": (_normalize_q90,),
    "q92": (normalize_date_interval_arithmetic,),
    "q94": (normalize_date_interval_arithmetic,),
    "q95": (normalize_date_interval_arithmetic,),
    "q97": (_normalize_q97,),
    "q98": (normalize_date_interval_arithmetic,),
}


def _sail_q12_safe_denominator(expression: exp.Expression, context: QueryNormalizerContext) -> None:
    ratios = [projection.this for projection in expression.expressions if projection.alias == "revenueratio"]
    if len(ratios) != 1 or not isinstance(ratios[0], exp.Div):
        raise ValueError("Expected one q12 revenueratio division.")
    denominator = ratios[0].expression
    if isinstance(denominator, exp.Nullif) and denominator.expression == exp.Literal.number(0):
        window = denominator.this
    else:
        window = denominator
    if (
        not isinstance(window, exp.Window)
        or not isinstance(window.this, exp.Sum)
        or not isinstance(window.this.this, exp.Sum)
        or not isinstance(window.this.this.this, exp.Column)
        or window.this.this.this.name != "ws_ext_sales_price"
    ):
        raise ValueError("Expected q12's windowed SUM(SUM(ws_ext_sales_price)) denominator.")
    if not isinstance(denominator, exp.Nullif):
        ratios[0].set("expression", exp.Nullif(this=window.copy(), expression=exp.Literal.number(0)))


ENGINE_QUERY_NORMALIZERS: EngineNormalizerRegistry = {
    Polars: {"*": (fold_constant_date_arithmetic,)},
    Sail: {"q12": (_sail_q12_safe_denominator,)},
}


def parse_tpcds_ansi_query(query: str) -> List[exp.Expression]:
    """Parses the T-SQL-compatible forms emitted by dsqgen's ANSI dialect."""
    return parse_tpc_ansi_statements(_normalize_ansi_syntax(query))
