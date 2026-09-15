# TPC-H Query Normalization

See [Query Normalization](README.md) for the shared pipeline and rule contract.

- **Module:** `src/lakebench/benchmarks/tpch/_query_normalizers.py`
- **Normalizer version:** `8`
- **Queries:** 22

---

## Source of truth

`resources/queries/canonical/sf<scale>/q*.sql` holds **exact `qgen` output** —
official TPC-H tools, seed `19620718`, stream 0, at SF1000 and SF10000. A
`generation_manifest.json` records the generator version, seed, stream, and
per-query hashes so the set is reproducible.

Running at a scale factor without a matching query set logs a warning, falls
back to SF1000 substitutions, and records `query_set_scale_matches_data: false`
in the result metadata. Query *selectivity* does not match the data in that case
— the run is still valid, but it is not a like-for-like comparison.

The TPC tools kits, templates, and `qgen` executable are **not** redistributed.

### Reader

Parsed with SQLGlot's built-in **`tsql`** reader
(`TPC_ANSI_READ_DIALECT`), not the default or PostgreSQL reader.

This is a deliberate choice driven by what each reader *injects*:

| Reader | Problem |
|---|---|
| `postgres` | injects NULL-ordering `CASE` expressions into the output |
| default / `spark` | injects `FLOAT` casts and safe-division `NULLIF` guards |
| **`tsql`** | accepts `TOP`, preserves typed division and `COUNT` as written |

An injected `NULLIF` or `FLOAT` cast is a silent change to the arithmetic being
measured, so the reader that adds nothing is the correct one. This is
independent of the target engine.

`tsql` implicit NULL ordering is *first* for ascending and *last* for
descending; explicit NULL ordering in the source is preserved.

`parse_tpch_ansi_query()` applies one lexical adaptation before parsing:
stripping interval precision qualifiers (`DAY(3)` → `DAY`), which `qgen` emits
but SQLGlot does not accept.

---

## Registered rules

### Source normalizers

| Rule | Applies to | Category |
|---|---|---|
| `_apply_qgen_row_limit` | `*` | spelling |
| `_q15_view_to_cte` | q15 | spelling |

**`_apply_qgen_row_limit`** — `qgen` emits row limits as a `--#SET ROWS_FETCH n`
comment rather than SQL, because the limit clause is dialect-specific. The rule
reads that directive and sets the limit on the AST. It requires exactly one
directive and rejects any value other than `-1` (no limit) or a positive
integer, so a malformed source fails rather than silently running unlimited.

**`_q15_view_to_cte`** — q15 ships as three statements: `CREATE VIEW`, `SELECT`,
`DROP VIEW`. Many engines cannot create a view in a benchmark session, and
issuing DDL mid-measurement would distort timings. The rule folds the view into
a CTE on the select, preserving the declared column list.

It verifies the created and dropped view names match, that the name is
unqualified, that the select actually references the view, that no CTEs already
exist, and that the column count matches the projections — then rewrites. Any
mismatch raises.

### Query normalizers

| Rule | Applies to | Category |
|---|---|---|
| `normalize_date_interval_arithmetic` | q1, q4, q5, q6, q10, q12, q14, q15, q20 | spelling |
| `_normalize_q1_wide_count` | q1 | type widening |

**`normalize_date_interval_arithmetic`** (shared with TPC-DS) lowers
`CAST(... AS DATE) ± INTERVAL 'n' UNIT` to a portable `DateAdd` node for whole
`DAY`/`MONTH`/`YEAR` intervals.

Two implementation details matter:

- Subtraction becomes a **negative amount wrapped in `exp.Neg`**, not `DateSub`
  (which renders unsupported T-SQL) and not a negative numeric literal (which
  renders invalid DuckDB). The `Neg` node is what makes DuckDB emit
  `INTERVAL (-n)`.
- The rule **raises** on any shape it does not recognize. SQLGlot's `tsql`
  reader accepting ANSI `INTERVAL` syntax does **not** mean Fabric Warehouse can
  execute it, so silently passing unknown forms through would defer a failure to
  runtime.

Dates, magnitudes, and direction are unchanged.

**`_normalize_q1_wide_count`** sets `big_int=True` on q1's `count_order`
projection, rendering `COUNT_BIG(*)` for T-SQL/Fabric while leaving `COUNT(*)`
for Spark, DuckDB, and MySQL.

T-SQL's `COUNT(*)` returns `INT` and errors above 2,147,483,647 rows in a group.
At SF1000+ q1's groups exceed that.

> Casting the result — `CAST(COUNT(*) AS BIGINT)` — does **not** work. The
> overflow happens *inside* the aggregate, before any cast applies. The counter
> width itself has to change.

### Engine normalizers

Registered for **Daft** only, which lacks implicit numeric promotion in
arithmetic on decimal columns.

| Rule | Queries | Casts |
|---|---|---|
| `_daft_q1_numeric_casts` | q1 | `l_extendedprice`, `l_discount`, `l_tax` |
| `_daft_q8_q14_numeric_casts` | q8, q14 | `l_extendedprice`, `l_discount`, plus the `CASE` `ELSE 0` |
| `_daft_q9_numeric_casts` | q9 | `l_extendedprice`, `l_discount`, `ps_supplycost`, `l_quantity` |

Each casts to `DOUBLE` only where the column participates in arithmetic, is
idempotent (an existing `DOUBLE` cast is left alone), and **raises if the
expected columns are not all found** — so a source change cannot leave Daft
silently uncast.

---

## Intentionally not done

**Join normalization is not registered.** TPC-H sources express joins as
predicates in `WHERE`. SQLGlot may render comma-separated tables as `CROSS
JOIN`, which looks alarming but is semantically identical — the `WHERE`
predicates still constrain the result, and every engine's optimizer handles it.
Rewriting joins would alter the query structure being measured for no
correctness gain. `normalize_implicit_joins` remains available for explicit
registration, and a test asserts it stays unregistered.

---

## Verification

- 22 queries × 2 scale factors × 3 dialects (Spark, T-SQL, Fabric) = 132
  renderings, pinned in `tests/fixtures/tpc_query_rendering.json` alongside
  TPC-DS for a combined 750.
- ScriptDom grammar validation of T-SQL and Fabric output.
- DuckDB integration run: 22/22.
- Rules asserted idempotent, and literal-preserving against the generated
  source.
