# TPC-DS 4.0.0 generated ANSI queries

This directory contains exact static `dsqgen` ANSI output for stream 0 at
SF1000 and SF10000. LakeBench treats these files as immutable generated input
and transpiles them to the selected engine dialect when a benchmark query is
resolved.

Generation settings:

- TPC-DS Tools 4.0.0
- RNG seed `19620718`
- qualification mode enabled
- ANSI dialect
- stream 0
- 103 executable statements from 99 templates

Each scale directory includes `generation_manifest.json` with the dsqgen,
template-list, complete-stream, and per-query hashes. The official kit,
templates, dsqgen executable, and distribution index are not stored in this
repository.

Runtime query resolution is:

1. Load the matching static ANSI query; SQL-file overrides are not searched.
2. Parse generator syntax with SQLGlot's built-in `tsql` reader and apply any
   registered source rules. This reader accepts the kit's TOP syntax; it is
   shared with TPC-H and does not serialize the AST to intermediate T-SQL.
3. Apply shared `"*"` and then query-specific structural normalizers.
4. Apply engine-targeted registered AST rules, including inherited rules.
5. Qualify and transpile the AST directly to the engine's SQLGlot dialect.

Join normalization is not registered by default; WHERE join predicates remain
in place even when SQLGlot renders comma joins as CROSS JOIN. The shared
function remains available for explicit registration.
One shared query rule expands the `lochierarchy` SELECT alias to its original
`GROUPING(...) + GROUPING(...)` expression inside compound ORDER BY expressions
in q36, q70, and q86, for all engines. Standalone ordering aliases are preserved.
This avoids T-SQL's restriction on using SELECT aliases inside ORDER BY expressions.
The rule recognizes both SQLGlot 26's anonymous GROUPING calls and SQLGlot 30's
dedicated GROUPING nodes without relaxing validation of their column arguments.
Other query rules qualify ambiguous ordering columns in q58 and q72,
rename q90 aliases that are reserved by some engines, and cast q97 `CASE`
outputs to `BIGINT` before aggregation to avoid integer overflow.

The shared `normalize_date_interval_arithmetic` rule is registered for q5,
q12, q16, q20, q21, q32, q37, q40, q77, q80, q82, q92, q94, q95, and q98.
It lowers the generated DATE casts plus/minus whole-day intervals to portable
`DateAdd` nodes. T-SQL renders `DATEADD`, with negative amounts for subtraction
in q21/q40; other engines render their native date arithmetic. Generated dates,
magnitudes, and directions are preserved. Unsupported interval shapes raise
explicitly: accepting ANSI `INTERVAL` under SQLGlot's T-SQL reader does not
establish that Fabric Warehouse supports it.

q1's case rule changes the unquoted SR_FEE aggregate operand to the DDL's
sr_fee spelling, without changing aliases or string literals. This is required
by Warehouse's default case-sensitive collation. q72 additionally lowers the
specific d1.d_date + 5 expression to DATEADD(DAY, 5, d1.d_date) for T-SQL/Fabric, retaining the
comparison and all other predicates.

Rules inspect context.target_dialect, not the source-parser context.dialect,
for T-SQL/Fabric-only accommodations. q17/q29/q35/q39a/q39b map StddevSamp to
the STDEV sample-standard-deviation function; scale variants without that
aggregate are unchanged. Population standard deviation is never substituted.
q22 casts inv_quantity_on_hand to BIGINT inside AVG so the rollup accumulator
does not overflow INT. The AVG result remains integer-valued on T-SQL/Fabric.
Neither aggregate accommodation changes Spark, DuckDB, or MySQL output.

Sail's q12 rule wraps its windowed denominator in NULLIF without changing
the selected scale's generated substitutions. Returning NULL instead of
raising for zero denominators is an engine-specific semantic accommodation.
Applied rule identifiers are recorded in each query's execution telemetry;
the normalizer version is recorded in engine metadata.
