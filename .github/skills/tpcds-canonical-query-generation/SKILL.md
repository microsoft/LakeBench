---
name: tpcds-canonical-query-generation
description: "Generate or refresh LakeBench immutable TPC-DS ANSI queries from a user-supplied official dsqgen kit. Use when regenerating TPC-DS queries, updating qualification literals, comparing dsqgen output, or validating canonical query semantics."
---

# TPC-DS Canonical Query Generation

Generate LakeBench's canonical TPC-DS queries from a user-supplied official TPC-DS tools kit. Never download, copy, commit, or package `dsqgen`, the TPC query templates, or other TPC-licensed tool files.

Runtime sources are exact generated ANSI under `canonical/sf<scale>`.
LakeBench parses generator syntax, applies registered source/shared/query/engine
AST rules, then renders directly to each engine's `SQLGLOT_DIALECT`.
TPC-H/TPC-DS never search SQL-file overrides. Do not bake compatibility fixes
into generated sources. Spark candidates below are historical review artifacts,
not the runtime source of truth.

## Required inputs

Collect these values before generation:

- Path to an extracted official TPC-DS tools kit.
- Scale factor, normally `1000`.
- RNG seed. Always require an explicit integer for a committed refresh; `dsqgen`'s default is acceptable only for exploratory output.
- Path to a compatible `dsqgen` executable, or permission to build it outside the repository.
- Review output directory outside the canonical query directory.

The kit must report TPC-DS tools version `4.0.0`. Stop if the version differs.

## Generate the review set

Run:

```powershell
uv run python .github\skills\tpcds-canonical-query-generation\scripts\generate_review.py `
  --kit-path "<TPC-DS kit path>" `
  --dsqgen "<dsqgen executable path>" `
  --scale-factor 1000 `
  --rng-seed <integer> `
  --output-dir "<review output path>"
```

For a Linux `dsqgen` executable built under WSL, add `--wsl`.

The helper:

- copies only the required templates into a temporary directory;
- patches the copied ANSI dialect with the missing `_BEGIN` and `_END` substitutions;
- invokes `dsqgen` without modifying the supplied kit;
- splits 99 template blocks into the 103 LakeBench query names;
- preserves exact generated statements under `ansi`;
- writes minimally transpiled Spark SQL under `spark-transpiled`;
- records source, command, and output hashes in `manifest.json`.

Do not commit the review directory.

## Produce canonical candidates

Create a formatted candidate set with explicit joins:

```powershell
uv run python .github\skills\tpcds-canonical-query-generation\scripts\canonicalize_review.py `
  --review-dir "<review output path>" `
  --ddl "src\lakebench\benchmarks\tpcds\resources\ddl\canonical\ddl_v4.0.0.simple.sql" `
  --output-dir "<review output path>\spark-candidates\sf<scale-factor>"
```

The helper transforms syntax only:

1. Convert comma joins to explicit joins while preserving table order.
2. Move only original conjunctive join predicates into `ON`.
3. Leave non-join filters in `WHERE`.
4. Render an intentional Cartesian relation as `CROSS JOIN`; never invent a key predicate.
5. Preserve all literals, selected columns, aggregate functions, aliases, grouping expressions, ordering expressions, limits, casts, and projections.
6. Convert date interval syntax to valid Spark `DATE_ADD` or equivalent Spark interval syntax.
7. Reformat for readability and normalize all unquoted table, column, CTE, and
   table-alias identifiers to lowercase. Spark identifier resolution is
   case-insensitive by default. Preserve quoted presentation aliases.

It also writes `README.md` with reproducibility details and copies the generated
hash manifest as `generation_manifest.json`. Neither file contains TPC templates
or binaries.

Do not move predicates across `LEFT`, `RIGHT`, or `FULL OUTER JOIN` boundaries. Do not rewrite predicates containing `OR` unless equivalence is proven.

## Semantic validation

Compare every candidate to its corresponding exact `ansi` statement after parsing/transpilation. Fail the refresh if any query changes:

- table multiplicity or table order;
- join type or join predicate;
- filter expression;
- selected expression or projection count;
- aggregate or window expression;
- grouping or ordering expression;
- literal value;
- limit;
- set operation;
- cast or null-handling expression.

Pay special attention to known hazards:

- `q18`: retain `c_current_cdemo_sk = cd2.cd_demo_sk`.
- `q41`: retain the generated limit.
- `q77`: the 4.0.0 template has an intentional/generated `cs` × `cr` Cartesian relation; render it as `CROSS JOIN`, not a keyed join.
- Overflow, precision, and divide-by-zero protections are functional changes.
  Implement them only as documented registered AST rules, never alternate SQL
  files or edits to generated ANSI. TPC-DS currently registers q97 widening
  generally and q12 denominator protection specifically for Sail.

Run all 103 candidate files through SQLGlot using `read="spark"`. Then resolve every query through each supported engine's `_return_query_definition()` to verify transpilation.

Where practical, execute generated and candidate forms against the same small dataset and compare ordered rows, or row multisets when the query has no ordering.

## Update LakeBench

Never replace canonical files during initial generation. Present the semantic diff and obtain explicit approval first.

After approval:

1. Import exact ANSI statements into `src\lakebench\benchmarks\tpcds\resources\queries\canonical\sf<scale>\q*.sql`.
2. Register required compatibility rules in the benchmark's `_query_normalizers.py`;
   use `ENGINE_QUERY_NORMALIZERS` for engine-specific accommodations.
3. Commit a checked-in provenance manifest that contains no TPC templates or binaries.
4. Run targeted query-resolution tests and the supported TPC-DS integration tests.
