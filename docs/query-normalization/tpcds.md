# TPC-DS Query Normalization

See [Query Normalization](README.md) for the shared pipeline and rule contract.

- **Module:** `src/lakebench/benchmarks/tpcds/_query_normalizers.py`
- **Normalizer version:** `11`
- **Queries:** 103 (99 templates; q14, q23, q24, q39 each yield two parts)

---

## Source of truth

`resources/queries/canonical/sf<scale>/q*.sql` holds **exact `dsqgen` output** — official TPC-DS tools 4.0.0, ANSI dialect, stream 0, at SF1000 and SF10000, with a `generation_manifest.json` recording generator version, seed, stream, and per-query hashes.

Scale-factor fallback behaves as in TPC-H: an unmatched scale factor warns, falls back to SF1000 substitutions, and records `query_set_scale_matches_data: false`.

The TPC tools kits, templates, and `dsqgen` executable are **not** redistributed. See `.github/skills/tpcds-canonical-query-generation/` for the regeneration procedure.

### Reader

Parsed with SQLGlot's built-in **`tsql`** reader — same rationale as [TPC-H](tpch.md#reader): it accepts `TOP` and injects neither NULL-ordering `CASE` expressions (PostgreSQL) nor `FLOAT` casts and safe-division `NULLIF` guards (default/Spark). Adding either would silently change the arithmetic being measured.

`parse_tpcds_ansi_query()` applies `_normalize_ansi_syntax()` before parsing — purely lexical adaptations of generator syntax SQLGlot does not accept:

- reordering `SELECT TOP n DISTINCT` to `SELECT DISTINCT TOP n`;
- rewriting bare `± N days` date arithmetic to `INTERVAL 'N' DAY`, which the interval rule below then lowers.

---

## Registered rules

No source normalizers. 29 query entries; one engine entry.

### Correctness against the DDL

| Rule | Query | Category |
|---|---|---|
| `_normalize_q1_fee_identifier` | q1 | spelling |

`dsqgen` emits `sr_fee` unquoted in a context where the generated text's casing does not match the DDL column. The rule repairs the identifier **by looking it up in the benchmark schema**, so the query binds on case-sensitive engines.

> Identifier case must **not** be normalized globally before case-sensitive binding checks. Doing so masks exactly this class of bug on the engines where it matters.

### Date arithmetic

| Rule | Queries | Category |
|---|---|---|
| `normalize_date_interval_arithmetic` | q5, q12, q16, q20, q21, q32, q37, q40, q77, q80, q82, q92, q94, q95, q98 | spelling |
| `_normalize_q72_date_offset` | q72 | spelling |

`normalize_date_interval_arithmetic` is shared with TPC-H; see [TPC-H](tpch.md#query-normalizers) for the `exp.Neg` requirement and the reason it raises on unrecognized shapes.

`_normalize_q72_date_offset` handles q72's `d1.d_date + 5` — bare integer addition to a date column, which is not portable. It resolves the operand against the schema to confirm it is a date column before lowering to `DateAdd`, so it cannot misfire on numeric arithmetic.

### Type widening

| Rule | Queries | Applies to | Category |
|---|---|---|---|
| `_normalize_tsql_sample_stddev` | q17, q29, q35, q39a, q39b | `FabricDataWarehouse` engine | spelling |
| `_normalize_q22_inventory_average` | q22 | `FabricDataWarehouse` engine | type widening |
| `_normalize_q9_wide_counts` | q9 | all (renders per dialect) | type widening |
| `_normalize_q97` | q97 | all | type widening |

The first two are registered in `ENGINE_QUERY_NORMALIZERS` under `FabricDataWarehouse`, not gated on the target dialect. Both work around Fabric Data Warehouse's T-SQL surface area specifically, so an engine that renders `tsql` or `fabric` without those limits is unaffected, and a downstream integration can unregister them. See the [rule contract](README.md#rule-contract) for when to prefer engine registration over a target-dialect gate.

**`_normalize_tsql_sample_stddev`** maps `StddevSamp` → `Stddev`. T-SQL's `STDEV` *is* the sample standard deviation (`STDEVP` is the population form), so this is a naming difference only — but SQLGlot renders `StddevSamp` as `STDDEV_SAMP`, which Fabric Data Warehouse does not have. Registered to the engine, so Spark and DuckDB keep `STDDEV_SAMP`.

**`_normalize_q22_inventory_average`** widens the input of q22's `AVG(inv_quantity_on_hand)` to `BIGINT` for Fabric Data Warehouse. T-SQL evaluates `AVG` over an `INT` column using an `INT` accumulator; at SF1000 the running sum overflows.

> Widening the **input**, not casting the **output**, is what matters — the overflow occurs inside the aggregate.

**`_normalize_q9_wide_counts`** marks each of q9's five scalar-subquery `COUNT(*)` bucket counters with `big_int`, rendering `COUNT_BIG(*)` for T-SQL/Fabric while Spark, DuckDB, and MySQL keep `COUNT(*)`. This one stays global: `big_int` is a portable AST flag that each dialect renders in its own way, so it is a renderer-family property rather than an engine workaround. T-SQL's `COUNT` returns `INT`. Each bucket counts `store_sales` filtered only by an `ss_quantity` range — roughly a fifth of the fact table, about 5.8 billion rows at SF10000 — so the counter overflows before any result is produced. The same `big_int` mechanism handles TPC-H q1.

> q88, q90, and q96 also count ungrouped over a fact table but bound their counts with selective dimension joins (a specific store, a half-hour window, household demographics), keeping them far below the 32-bit limit. They are deliberately left unwidened; `test_other_ungrouped_fact_counts_are_left_alone` pins that decision.

**`_normalize_q97`** widens the `THEN` and `ELSE` values of q97's three `SUM(CASE ... END)` expressions to `BIGINT`, so the summed values are already wide before aggregation. Unlike the two above this is **not** target-gated: the counts exceed 32-bit range on any engine whose `SUM` over an integer expression stays 32-bit. Idempotent — an existing `BIGINT` cast is left alone.

### Ordering and aliasing

| Rule | Queries | Category |
|---|---|---|
| `_normalize_rollup_order_by` | q36, q70, q86 | spelling |
| `_normalize_q58` | q58 | spelling |
| `_normalize_q72` | q72 | spelling |
| `_normalize_q90` | q90 | spelling |

**`_normalize_rollup_order_by`** — these queries define `lochierarchy` as a `GROUPING(...)` expression in the projection and then reference that alias in `ORDER BY`. Engines differ on whether a select alias is visible to `ORDER BY` when it wraps `GROUPING`. The rule substitutes the underlying expression. `GROUPING` nodes have different AST layouts across the supported SQLGlot versions, which the module adapts for.

**`_normalize_q58` / `_normalize_q72`** use the shared `_qualify_order_column` helper to qualify `ORDER BY` columns that are ambiguous across the query's joined relations.

**`_normalize_q90`** renames the derived tables `at` and `pt` to `am_counts` and `pm_counts`. `AT` and `PT` are reserved words in some target dialects; the generated aliases are not part of the measured workload and renaming them affects nothing but parseability.

### Engine normalizers

| Engine | Query | Rule | Category |
|---|---|---|---|
| Polars | `*` | `fold_constant_date_arithmetic` | spelling |
| Sail | q12 | `_sail_q12_safe_denominator` | lowering |
| Sail | q90 | `_sail_q90_safe_denominator` | lowering |

**`fold_constant_date_arithmetic`** evaluates constant date offsets at compile time, because Polars' SQL frontend cannot parse interval syntax. It is shared with TPC-H and fully documented on [the TPC-H page](tpch.md#polars-constant-date-folding). q72 is deliberately left alone: its offset is applied to a column, so there is nothing to fold.

**`_sail_q12_safe_denominator`** — Sail evaluates q12's `revenueratio` denominator — a windowed `SUM(SUM(ws_ext_sales_price))` — in a way that raises a division-by-zero at runtime where other engines return NULL. The rule wraps the denominator in `NULLIF(..., 0)`, restoring NULL. Registered **only** for Sail: divide-by-zero protection is a functional change and is not applied where it is not needed.

**`_sail_q90_safe_denominator`** — same accommodation, same reasoning, for q90's `am_pm_ratio`. The denominator is `CAST(pmc AS DECIMAL(15,4))`, where `pmc` is a `COUNT(*)` of web sales in a two-hour window under a narrow `wp_char_count`/`hd_dep_count` filter. That count is legitimately zero at small scale factors, so this is a property of the query, not of the data: Sail raises `AnalysisException: Division by zero` where Spark and DuckDB return NULL. The rule wraps the denominator in `NULLIF(..., 0)`, and asserts the expected `CAST(pmc ...)` shape so a future query-set refresh cannot silently skip the guard. Registered only for Sail.

---

## Intentionally not done

**Join normalization is not registered**, for the same reason as [TPC-H](tpch.md#intentionally-not-done): the generated sources express joins in `WHERE`, comma-rendered `CROSS JOIN` output is semantically identical, and rewriting joins would change the structure being measured.

**Overflow, precision, and divide-by-zero protections are functional changes.** They are added only where an engine genuinely fails, as documented rules — never by editing a generated source file.

---

## Known generator hazards

Preserved deliberately; do not "fix" these:

- **q18** — retains `c_current_cdemo_sk = cd2.cd_demo_sk`.
- **q41** — retains the generated limit.
- **q77** — the 4.0.0 template contains an intentional `cs` × `cr` Cartesian relation. It is rendered as a cross join; no key predicate is invented.

---

## Spec compliance notes

LakeBench follows the official TPC-DS specification where `spark-sql-perf` diverges from it, notably `customer.c_last_review_date_sk` and `store.s_tax_percentage`. Results are therefore not directly comparable to `spark-sql-perf`-derived numbers.

---

## Verification

- 103 queries × 2 scale factors × 2 dialects (Spark, Fabric) = 412 renderings, pinned in `tests/fixtures/tpc_query_rendering.json` alongside TPC-H for a combined 500. Fabric is rendered through the `FabricDataWarehouse` engine, so the engine-registered rules are pinned too.
- ScriptDom grammar validation of Fabric output.
- DuckDB integration run: 103/103.
- Rules asserted idempotent and literal-preserving against the generated source.
