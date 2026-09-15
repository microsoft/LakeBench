import re
from typing import Dict, List, Set, Tuple

from sqlglot import exp

from ...engines.daft import Daft
from ...engines.polars import Polars
from .._load_and_query._query_normalizers import (
    EngineNormalizerRegistry,
    QueryNormalizer,
    QueryNormalizerContext,
    SourceNormalizerRegistry,
    fold_constant_date_arithmetic,
    normalize_date_interval_arithmetic,
    parse_tpc_ansi_statements,
)

NORMALIZER_VERSION = "9"


def _normalize_q1_wide_count(expression: exp.Expression, context: QueryNormalizerContext) -> None:
    counts = [
        projection.this
        for projection in expression.expressions
        if isinstance(projection, exp.Alias) and projection.alias == "count_order"
    ]
    if (
        len(counts) != 1
        or not isinstance(counts[0], exp.Count)
        or not isinstance(counts[0].this, exp.Star)
        or counts[0].expressions
    ):
        raise ValueError("Expected q1 count_order projection to be COUNT(*).")
    counts[0].set("big_int", True)


QUERY_NORMALIZERS: Dict[str, Tuple[QueryNormalizer, ...]] = {
    "q1": (normalize_date_interval_arithmetic, _normalize_q1_wide_count),
    "q4": (normalize_date_interval_arithmetic,),
    "q5": (normalize_date_interval_arithmetic,),
    "q6": (normalize_date_interval_arithmetic,),
    "q10": (normalize_date_interval_arithmetic,),
    "q12": (normalize_date_interval_arithmetic,),
    "q14": (normalize_date_interval_arithmetic,),
    "q15": (normalize_date_interval_arithmetic,),
    "q20": (normalize_date_interval_arithmetic,),
}

_ROW_LIMIT_PATTERN = re.compile(r"(?im)^--#SET ROWS_FETCH\s+(-?\d+)\s*$")
_INTERVAL_PRECISION_PATTERN = re.compile(
    r"(?i)\b(DAY|MONTH|YEAR)\s*\(\d+\)",
)


def _apply_qgen_row_limit(statements: List[exp.Expression], context: QueryNormalizerContext) -> None:
    matches = _ROW_LIMIT_PATTERN.findall(context.source_sql)
    if len(matches) != 1:
        raise ValueError("TPC-H qgen output must contain exactly one ROWS_FETCH directive.")
    queries = [statement for statement in statements if isinstance(statement, exp.Query)]
    if len(queries) != 1:
        raise ValueError("Expected one top-level query for the qgen row limit.")
    row_limit = int(matches[0])
    if row_limit != -1 and row_limit <= 0:
        raise ValueError(f"Unsupported qgen row limit: {row_limit}.")
    if row_limit > 0:
        queries[0].set(
            "limit",
            exp.Limit(expression=exp.Literal.number(row_limit)),
        )


def _q15_view_to_cte(statements: List[exp.Expression], context: QueryNormalizerContext) -> None:
    if (
        len(statements) != 3
        or not isinstance(statements[0], exp.Create)
        or not isinstance(statements[1], exp.Select)
        or not isinstance(statements[2], exp.Drop)
    ):
        raise ValueError("Expected q15 to contain CREATE VIEW, SELECT, and DROP VIEW.")

    create, query, drop = statements
    if create.kind != "VIEW" or drop.kind != "VIEW":
        raise ValueError("Expected q15 to create and drop a view.")
    if not isinstance(create.this, exp.Schema) or not isinstance(create.expression, exp.Select):
        raise ValueError("Expected q15 CREATE VIEW to include a named column schema.")

    view_name = create.this.this.name
    drop_targets = drop.args.get("tables") or ([drop.this] if drop.this is not None else [])
    if drop_targets != [create.this.this]:
        raise ValueError("q15 creates and drops different view names.")
    if create.this.this.db or create.this.this.catalog:
        raise ValueError("Expected an unqualified q15 view name.")
    with_key = "with_" if "with_" in query.arg_types else "with"
    if query.args.get(with_key) or not any(table == create.this.this for table in query.find_all(exp.Table)):
        raise ValueError("Expected q15 SELECT to reference its generated view without existing CTEs.")
    columns = [column.copy() for column in create.this.expressions]
    if not columns or len(columns) != len(create.expression.expressions):
        raise ValueError("q15 view columns do not match its projections.")
    query.set(
        with_key,
        exp.With(
            expressions=[
                exp.CTE(
                    this=create.expression.copy(),
                    alias=exp.TableAlias(
                        this=exp.to_identifier(view_name),
                        columns=columns,
                    ),
                )
            ]
        ),
    )
    statements[:] = [query]


SOURCE_NORMALIZERS: SourceNormalizerRegistry = {
    "*": (_apply_qgen_row_limit,),
    "q15": (_q15_view_to_cte,),
}


def _cast_daft_arithmetic(expression: exp.Expression, names: Set[str]) -> None:
    matched = set()
    for column in list(expression.find_all(exp.Column)):
        if column.name not in names:
            continue
        parent = column.parent
        already_cast = isinstance(parent, exp.Cast) and parent.args["to"].this == exp.DataType.Type.DOUBLE
        arithmetic_parent = parent.parent if already_cast else parent
        if not isinstance(arithmetic_parent, (exp.Add, exp.Sub, exp.Mul, exp.Div)):
            continue
        matched.add(column.name)
        if not already_cast:
            column.replace(exp.Cast(this=column.copy(), to=exp.DataType.build("DOUBLE")))
    if matched != names:
        raise ValueError(f"Expected Daft arithmetic columns {sorted(names)}, found {sorted(matched)}.")


def _cast_daft_case_defaults(expression: exp.Expression) -> None:
    cases = list(expression.find_all(exp.Case))
    if len(cases) != 1:
        raise ValueError("Expected one CASE expression for the Daft numeric rule.")
    default = cases[0].args.get("default")
    already_cast = isinstance(default, exp.Cast) and default.args["to"].this == exp.DataType.Type.DOUBLE
    if (default.this if already_cast else default) != exp.Literal.number(0):
        raise ValueError("Expected a zero ELSE value for the Daft numeric rule.")
    if not already_cast:
        cases[0].set("default", exp.Cast(this=default.copy(), to=exp.DataType.build("DOUBLE")))


def _daft_q1_numeric_casts(expression: exp.Expression, context: QueryNormalizerContext) -> None:
    _cast_daft_arithmetic(expression, {"l_extendedprice", "l_discount", "l_tax"})


def _daft_q8_q14_numeric_casts(expression: exp.Expression, context: QueryNormalizerContext) -> None:
    _cast_daft_arithmetic(expression, {"l_extendedprice", "l_discount"})
    _cast_daft_case_defaults(expression)


def _daft_q9_numeric_casts(expression: exp.Expression, context: QueryNormalizerContext) -> None:
    _cast_daft_arithmetic(expression, {"l_extendedprice", "l_discount", "ps_supplycost", "l_quantity"})


ENGINE_QUERY_NORMALIZERS: EngineNormalizerRegistry = {
    Daft: {
        "q1": (_daft_q1_numeric_casts,),
        "q8": (_daft_q8_q14_numeric_casts,),
        "q9": (_daft_q9_numeric_casts,),
        "q14": (_daft_q8_q14_numeric_casts,),
    },
    Polars: {"*": (fold_constant_date_arithmetic,)},
}


def parse_tpch_ansi_query(query: str) -> List[exp.Expression]:
    """Parses qgen syntax without applying query-specific transformations."""
    normalized_query = _INTERVAL_PRECISION_PATTERN.sub(r"\1", query)
    return parse_tpc_ansi_statements(normalized_query)
