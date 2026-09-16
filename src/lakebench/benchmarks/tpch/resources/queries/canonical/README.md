# TPC-H 3.0.1 generated ANSI queries

This directory contains exact static qgen ANSI output for stream 0 at SF1000
and SF10000. LakeBench keeps these generated files immutable and parses them
with SQLGlot when a benchmark query is resolved.

Generation settings:

- TPC-H specification 3.0.1
- qgen 3.0.0
- RNG seed `19620718`
- ANSI mode enabled
- stream 0
- 22 query templates

Each scale directory contains `generation_manifest.json` with qgen,
distribution, stream, template, and per-query hashes. The official kit,
templates, qgen executable, and distribution data are not stored in this
repository.

Runtime query resolution is:

1. Load the matching static ANSI query; SQL-file overrides are not searched.
2. Parse qgen syntax into a statement bundle with SQLGlot's built-in `tsql`
   reader and the existing interval-precision adaptation, without query-specific lowering.
3. Apply registered source rules: qgen row-limit directives for every query,
   then the q15 view-to-CTE rule.
4. Apply registered shared and query-specific AST rules. Join normalization is
   not registered by default; WHERE join predicates remain in place. SQLGlot
   may render comma joins as CROSS JOIN without relocating predicates.
5. Apply engine-targeted registered AST rules, including inherited rules.
6. Qualify and transpile the AST directly to the engine dialect.

TPC-H q15 is lowered by its registered source rule from `CREATE VIEW`, `SELECT`, and
`DROP VIEW` sequence into one equivalent CTE expression so it can use the
same single-query execution interface as the other templates.
The source rule accepts SQLGlot 26's single DROP target and SQLGlot 30's target
list, requiring exactly the one matching view, and uses the installed version's
WITH argument name. Extra cleanup targets and pre-existing CTEs are rejected.

The shared `normalize_date_interval_arithmetic` rule is registered for q1,
q4, q5, q6, q10, q12, q14, q15, and q20. It lowers DATE casts plus/minus whole
DAY/MONTH/YEAR intervals to portable `DateAdd` nodes, including inside q15's
CTE. T-SQL renders `DATEADD` with a negative amount for subtraction, rather
than unsupported ANSI `INTERVAL` or `DATE_SUB` syntax. Other engines render
their native date arithmetic. Generated dates, magnitudes, and directions
are preserved; unsupported interval shapes raise explicitly.

q1's `_normalize_q1_wide_count` rule marks its `count_order` COUNT(*) projection
with SQLGlot's `big_int` metadata. T-SQL renders `COUNT_BIG(*)` to avoid INT
overflow above 2,147,483,647 rows per group. Spark, DuckDB, and MySQL-dialect
output still uses COUNT(*). This widens the T-SQL result type without changing
the counted rows; it does not wrap COUNT(*) in an ineffective post-aggregation
cast. The generated source remains unchanged.

Daft rules cast arithmetic operands in q1/q8/q9/q14 and the CASE defaults in
q8/q14 to DOUBLE without replacing generated filters or literals. These
precision accommodations are engine-specific semantic changes. Applied source,
canonical, and engine rule identifiers are recorded in each query's execution
telemetry; the normalizer version is recorded in engine metadata.

The shared ANSI reader is a built-in parser choice, not a custom dialect or
an intermediate T-SQL serialization. It accepts all generated TPC-H statements
and preserves typed, non-safe division and the original COUNT syntax. Unlike
the previous PostgreSQL reader, it does not introduce NULLS FIRST for descending
sorts and trigger invalid SELECT-alias references inside T-SQL ORDER BY CASE
expressions. Unlike Spark/default parsing, it does not introduce FLOAT casts
when rendering q8/q14/q17 division to T-SQL.

The reader's implicit NULL-ordering convention is NULLs first for ascending
and last for descending, matching the previous Spark canonical path; explicit
NULLS FIRST/LAST clauses are honored. This is an explicit parser convention,
not a claim that ANSI specifies universal NULL ordering. Target rendering
still follows each engine's SQLGLOT_DIALECT directly from the normalized AST.
