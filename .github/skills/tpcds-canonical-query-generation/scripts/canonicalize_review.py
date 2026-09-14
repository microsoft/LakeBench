from __future__ import annotations

import argparse
import json
import shutil
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

import sqlglot
from sqlglot import exp
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.scope import Scope, build_scope


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create formatted TPC-DS canonical Spark SQL with explicit joins."
    )
    parser.add_argument("--review-dir", type=Path, required=True)
    parser.add_argument("--ddl", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args()


def load_schema(ddl_path: Path) -> Dict[str, Dict[str, str]]:
    schema = {}
    for statement in sqlglot.parse(ddl_path.read_text(encoding="utf-8"), read="spark"):
        if not isinstance(statement, exp.Create) or not isinstance(statement.this, exp.Schema):
            continue
        table_name = statement.this.this.name
        schema[table_name] = {
            column.this.name: column.args["kind"].sql(dialect="spark")
            for column in statement.this.expressions
        }
    if not schema:
        raise RuntimeError(f"No table definitions found in DDL: {ddl_path}")
    return schema


def conjunctions(condition: Optional[exp.Expression]) -> List[exp.Expression]:
    if condition is None:
        return []
    if isinstance(condition, exp.And):
        return list(condition.flatten())
    return [condition]


def combine_conjunctions(predicates: Iterable[exp.Expression]) -> Optional[exp.Expression]:
    predicates = list(predicates)
    if not predicates:
        return None
    return exp.and_(*predicates, copy=False)


def scope_aliases(scope: Scope) -> List[str]:
    select = scope.expression
    from_ = select.args.get("from_") or select.args.get("from")
    if not from_:
        return []
    return [from_.this.alias_or_name] + [
        join.this.alias_or_name for join in select.args.get("joins") or []
    ]


def paired_scopes(
    expression: exp.Expression,
    schema: Dict[str, Dict[str, str]],
) -> List[Tuple[Scope, Scope]]:
    original_root = build_scope(expression)
    qualified_root = build_scope(
        qualify(
            expression.copy(),
            dialect="spark",
            schema=schema,
            validate_qualify_columns=False,
            quote_identifiers=False,
            identify=False,
        )
    )
    if original_root is None or qualified_root is None:
        raise RuntimeError("Query does not contain a selectable scope")

    original_scopes = list(original_root.traverse())
    qualified_scopes = list(qualified_root.traverse())
    if len(original_scopes) != len(qualified_scopes):
        raise RuntimeError("Schema qualification changed the query scope count")

    pairs = list(zip(original_scopes, qualified_scopes))
    for original_scope, qualified_scope in pairs:
        if len(scope_aliases(original_scope)) != len(scope_aliases(qualified_scope)):
            raise RuntimeError("Schema qualification changed the relation count")
    return pairs


def convert_scope_joins(original_scope: Scope, qualified_scope: Scope) -> int:
    select = original_scope.expression
    qualified_select = qualified_scope.expression
    joins = select.args.get("joins") or []
    if not joins:
        return 0

    aliases = scope_aliases(qualified_scope)
    alias_positions = {alias: index for index, alias in enumerate(aliases)}
    original_where = select.args.get("where")
    qualified_where = qualified_select.args.get("where")
    original_predicates = conjunctions(original_where.this if original_where else None)
    qualified_predicates = conjunctions(qualified_where.this if qualified_where else None)
    if len(original_predicates) != len(qualified_predicates):
        raise RuntimeError("Schema qualification changed the WHERE predicate count")

    join_predicates = defaultdict(list)
    remaining_predicates = []

    for original_predicate, qualified_predicate in zip(
        original_predicates, qualified_predicates
    ):
        referenced_aliases = exp.column_table_names(qualified_predicate)
        positions = {
            alias_positions[alias]
            for alias in referenced_aliases
            if alias in alias_positions
        }
        target_position = max(positions) if positions else 0
        target_join_index = target_position - 1
        target_join = joins[target_join_index] if target_join_index >= 0 else None
        is_safe_join_predicate = (
            len(positions) >= 2
            and len(positions) == len(referenced_aliases)
            and min(positions) < target_position
            and target_join is not None
            and not target_join.args.get("on")
            and not target_join.args.get("using")
            and not target_join.side
            and not qualified_predicate.find(exp.Or)
        )
        if is_safe_join_predicate:
            join_predicates[target_join_index].append(original_predicate)
        else:
            remaining_predicates.append(original_predicate)

    for index, join in enumerate(joins):
        predicates = join_predicates.get(index)
        if predicates:
            join.set("on", combine_conjunctions(predicates))
            join.set("kind", None)
        elif not any(
            join.args.get(attribute)
            for attribute in ("on", "side", "kind", "using", "method")
        ):
            join.set("kind", "CROSS")

    remaining_condition = combine_conjunctions(remaining_predicates)
    if remaining_condition is None:
        select.set("where", None)
    elif original_where:
        original_where.set("this", remaining_condition)
    else:
        select.set("where", exp.Where(this=remaining_condition))

    return sum(len(predicates) for predicates in join_predicates.values())


def canonical_semantic_form(expression: exp.Expression) -> str:
    normalized = expression.copy()
    root = build_scope(normalized)
    if root is None:
        raise RuntimeError("Query does not contain a selectable scope")

    for scope in root.traverse():
        select = scope.expression
        where = select.args.get("where")
        predicates = conjunctions(where.this if where else None)

        for join in select.args.get("joins") or []:
            if join.side:
                continue
            predicates.extend(conjunctions(join.args.get("on")))
            join.set("on", None)
            join.set("kind", "CROSS")

        predicates.sort(key=lambda predicate: predicate.sql(dialect="spark"))
        condition = combine_conjunctions(predicates)
        if condition is None:
            select.set("where", None)
        elif where:
            where.set("this", condition)
        else:
            select.set("where", exp.Where(this=condition))

    return normalized.sql(dialect="spark", pretty=False, normalize=True)


def convert_query(
    source_sql: str,
    schema: Dict[str, Dict[str, str]],
) -> Tuple[str, int]:
    source = sqlglot.parse_one(source_sql, read="spark")
    candidate = source.copy()
    moved_predicate_count = 0
    for original_scope, qualified_scope in paired_scopes(candidate, schema):
        moved_predicate_count += convert_scope_joins(original_scope, qualified_scope)

    candidate_sql = candidate.sql(dialect="spark", pretty=True, normalize=True).rstrip() + "\n"
    reparsed_candidate = sqlglot.parse_one(candidate_sql, read="spark")
    if canonical_semantic_form(source) != canonical_semantic_form(reparsed_candidate):
        raise RuntimeError("Canonicalization changed query semantics")
    return candidate_sql, moved_predicate_count


def implicit_join_count(expression: exp.Expression) -> int:
    return sum(
        1
        for join in expression.find_all(exp.Join)
        if not any(
            join.args.get(attribute)
            for attribute in ("on", "side", "kind", "using", "method")
        )
    )


def cross_join_locations(expression: exp.Expression) -> Set[str]:
    return {
        join.this.alias_or_name
        for join in expression.find_all(exp.Join)
        if join.kind == "CROSS"
    }


def write_readme(
    output_directory: Path,
    manifest: dict,
    moved_predicate_count: int,
    cross_join_query_count: int,
) -> None:
    readme = f"""# TPC-DS 4.0.0 Spark SQL review candidates

This directory contains a reviewable TPC-DS 4.0.0 query set rendered as Spark SQL.
It is a review artifact, not the active LakeBench query source. Runtime queries
remain exact generated ANSI under `canonical/sf<scale>` with registered AST
normalization and target-dialect rendering. Do not replace them with these
Spark candidates.

## Generation

- TPC-DS Tools: {manifest["tpcds_tools_version"]}
- Scale factor: {manifest["scale_factor"]}
- RNG seed: {manifest["rng_seed"]}
- Qualification mode: `Y`
- dsqgen dialect: `ansi`
- Output dialect: Spark SQL
- Query files: {manifest["query_count"]}
- Variant templates: q14, q23, q24, and q39 (`a` and `b` files)
- dsqgen SHA-256: `{manifest["dsqgen_sha256"]}`
- `templates.lst` SHA-256: `{manifest["templates_list_sha256"]}`
- Generated stream SHA-256: `{manifest["query_stream_sha256"]}`

The official kit, templates, `dsqgen` executable, and `tpcds.idx` are not stored in
this repository.

## Syntax-only transformations

The exact qualification output was parsed as T-SQL-compatible ANSI and emitted as
Spark SQL with SQLGlot. The canonicalization step then:

- formatted each query with SQLGlot;
- normalized all unquoted table, column, CTE, and table-alias identifiers to lowercase;
- preserved source relation order;
- moved {moved_predicate_count} existing conjunctive multi-relation predicates from
  `WHERE` to the corresponding `JOIN ... ON`;
- rendered every remaining implicit comma relation as an explicit `CROSS JOIN`;
- retained filters that could not be safely moved without reordering relations;
- preserved projections, expressions, literals, grouping, ordering, limits, casts,
  set operations, outer joins, and predicate text.

{cross_join_query_count} queries contain at least one explicit `CROSS JOIN`. Some are
required by source relation order; q77 intentionally retains the generated `cs` x
`cr` Cartesian relation and does not invent a call-center key predicate.

Each transformed query was reparsed as Spark SQL and compared with its generated
source after normalizing only the location of inner-join predicates.
"""
    (output_directory / "README.md").write_text(
        readme,
        encoding="utf-8",
        newline="\n",
    )


def main() -> None:
    args = parse_arguments()
    review_directory = args.review_dir.resolve()
    source_directory = review_directory / "spark-transpiled"
    manifest_path = review_directory / "manifest.json"
    output_directory = args.output_dir.resolve()

    if output_directory.exists():
        raise FileExistsError(f"Output directory already exists: {output_directory}")
    if not source_directory.is_dir() or not manifest_path.is_file():
        raise FileNotFoundError(f"Invalid generated review directory: {review_directory}")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("tpcds_tools_version") != "4.0.0":
        raise RuntimeError("Canonical v4 queries require TPC-DS Tools 4.0.0")
    if manifest.get("query_count") != 103:
        raise RuntimeError("Expected a 103-query generated review set")

    schema = load_schema(args.ddl.resolve())
    output_directory.mkdir(parents=True)
    (output_directory / "__init__.py").write_text("", encoding="utf-8", newline="\n")

    moved_predicate_count = 0
    cross_join_query_count = 0
    source_paths = sorted(source_directory.glob("q*.sql"))
    if len(source_paths) != 103:
        raise RuntimeError(f"Expected 103 Spark query files, found {len(source_paths)}")

    for source_path in source_paths:
        candidate_sql, moved = convert_query(
            source_path.read_text(encoding="utf-8"),
            schema,
        )
        candidate = sqlglot.parse_one(candidate_sql, read="spark")
        if implicit_join_count(candidate):
            raise RuntimeError(f"Implicit join remains in {source_path.name}")
        if cross_join_locations(candidate):
            cross_join_query_count += 1
        (output_directory / source_path.name).write_text(
            candidate_sql,
            encoding="utf-8",
            newline="\n",
        )
        moved_predicate_count += moved

    shutil.copy2(manifest_path, output_directory / "generation_manifest.json")
    write_readme(
        output_directory,
        manifest,
        moved_predicate_count,
        cross_join_query_count,
    )
    print(
        f"Created {len(source_paths)} canonical Spark queries in {output_directory}; "
        f"moved {moved_predicate_count} predicates across "
        f"{cross_join_query_count} queries containing CROSS JOIN."
    )


if __name__ == "__main__":
    main()
