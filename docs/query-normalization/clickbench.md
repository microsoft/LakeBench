# ClickBench Query Normalization

See [Query Normalization](README.md) for the shared pipeline and rule contract.

- **Module:** `src/lakebench/benchmarks/clickbench/_query_normalizers.py`
- **Normalizer version:** `1`
- **Queries:** 43

---

## Source of truth

`resources/queries/canonical/q1.sql` … `q43.sql` are the **exact lines of
upstream's `clickhouse/queries.sql`** at a pinned commit, one statement per
file. Nothing is reformatted, re-cased, or rewritten.

`source_manifest.json` records the pinned commit, upstream blob hash, and
per-query hashes; `PROVENANCE.md` sits alongside it. The pinned commit and
manifest hashes are surfaced in result metadata by `_configure_query_resources`,
so every run is traceable to an upstream revision.

Upstream ClickBench is published under **CC BY-NC-SA 4.0** and is *not* covered
by LakeBench's MIT license. See
[`THIRD-PARTY-NOTICES.md`](../../THIRD-PARTY-NOTICES.md).

### Reader

Parsed with SQLGlot's **`clickhouse`** reader, because the source *is*
ClickHouse SQL. This differs from the TPC benchmarks, whose sources are
generated ANSI.

One consequence worth knowing: the `clickhouse` reader sets `Count.big_int=True`,
because ClickHouse `count()` returns `UInt64`. T-SQL output therefore contains
`COUNT_BIG` on 35 of the 43 queries. This is correct — it is the source's result
type — but it is visibly noisier than hand-written T-SQL would be.

Parsing q43 emits a harmless `WARNING:sqlglot:Unexpected interval unit: MINUTE`.

---

## Registered rules

Two rules apply globally (`_normalize_native_length` to all queries, and the q29 backreference fix), and three are registered to the `FabricDataWarehouse` engine in `ENGINE_QUERY_NORMALIZERS`, plus one target-neutral rule for q43.

The warehouse rules are keyed by engine class rather than gated on the target dialect, so an engine that renders `tsql` or `fabric` without Fabric Data Warehouse's particular limits is unaffected and a downstream integration can unregister them. See the [rule contract](README.md#rule-contract).

### `_resolve_positional_group_by` — `FabricDataWarehouse` engine — *spelling*

q35 uses ClickHouse's `GROUP BY 1`. T-SQL does not accept positional grouping
keys, so the position is resolved against the projection list.

If the referenced projection is a **constant**, the key is dropped rather than
replaced: a constant contributes no grouping in any dialect, and T-SQL rejects
grouping by a literal. If every key is dropped the `GROUP BY` clause is removed.
An out-of-range position raises.

### `_expand_group_by_aliases` — `FabricDataWarehouse` engine — *spelling*

T-SQL cannot group by a `SELECT` alias. Affects q19 (`m`), q29 (`k`), and q40
(`Src`, `Dst`). The alias is replaced with its defining expression.

The rule **raises** if the grouping key is *both* a real column in the DDL and a
select alias, since which one the source meant would then be ambiguous.

### `_normalize_native_length` — all targets — *spelling*

Clears `Length.binary`, rendering each target's native `LENGTH`.

The ClickHouse reader marks `length` as a **byte**-length call, because
ClickHouse's `length(String)` counts bytes. That flag is not worth preserving
across engines, for two independent reasons:

1. **Upstream itself does not preserve it.** ClickBench's own DuckDB and Spark
   query sets call each engine's native `length`, which counts characters.
   Per-engine native length *is* the upstream behavior; forcing byte length
   everywhere would be the deviation.
2. **Byte length is not portable.** An earlier version of this rule emitted
   `OCTET_LENGTH`, which failed the DuckDB integration run — DuckDB's
   `octet_length` accepts `BLOB` only and errors on strings. SQLGlot's
   alternative is a `CASE TYPEOF` expansion, which is not portable to the other
   engines sharing the `duckdb` dialect (Polars).

> **Never emit `OCTET_LENGTH` for DuckDB.** Polars and DuckDB share
> `SQLGLOT_DIALECT = "duckdb"` but not an executor, so dialect-keyed
> engine-specific functions are unsafe in general.

### `_widen_integer_aggregates` — `FabricDataWarehouse` engine — *type widening*

Widens the **input** of `SUM` and `AVG` when the argument evaluates as integer
arithmetic, determined by resolving columns against the DDL schema.

| Aggregate | Widened to | Why |
|---|---|---|
| `SUM` | `BIGINT` | T-SQL accumulates `SUM(int)` in `INT`. q30's 90 sums over the full table overflow. ClickHouse returns a 64-bit sum. |
| `AVG` | `FLOAT` | T-SQL's `AVG(int)` **truncates to a whole number**. ClickHouse `avg()` returns `Float64`. |

> The `AVG` target type matters more than it looks. `BIGINT` or
> `DECIMAL(38, 0)` stop the overflow but leave the result truncated — the
> answer is still wrong, just not an error. A real type is required, and a test
> asserts the cast type is in `exp.DataType.REAL_TYPES`.

Arguments already wrapped in a `CAST` are left alone, so the rule is idempotent.

### `_normalize_minute_truncation` — q43, all targets — *spelling*

Replaces `exp.DateTrunc` with
`exp.TimestampTrunc(this=..., unit=exp.Var(this="MINUTE"))`.

q43 truncates to the minute. `exp.DateTrunc` renders as:

| Target | Output | Problem |
|---|---|---|
| Spark | `TRUNC(x, 'MINUTE')` | date-only; minute precision discarded |
| T-SQL | `DATE_TRUNC('MINUTE', x)` | not valid T-SQL |
| MySQL (Daft) | `DATE(x)` | **silently** degrades to day granularity |

The MySQL case is the dangerous one — no error, just wrong grouping.
`TimestampTrunc` renders correctly on all three. The rule raises if no
truncation or no unit is found.

### q29 host extraction — *lowering*

q29 extracts a host with `REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1')`. Both rules below validate the pattern matches the source exactly before acting, sharing a single `_q29_replacements()` validator.

| Rule | Scope | Action |
|---|---|---|
| `_normalize_q29_backreference` | global, target-gated | spark, databricks, mysql: replacement string `\1` → `$1` (Java-style group references); duckdb and others unchanged |
| `_lower_q29_host_extraction` | `FabricDataWarehouse` engine | lowered to native binary-collated string operations |

The backreference fix stays global and target-gated because it follows the regex engine each dialect maps onto — a renderer-family property spanning Spark, Databricks, and MySQL — rather than any one engine's limitation. The lowering is the opposite: it exists solely because of Fabric Data Warehouse's surface area.

**Why lower it for Fabric Data Warehouse.** Fabric Data Warehouse's documented T-SQL surface area does not include `REGEXP_REPLACE`. Substituting a different expression that merely *resembles* host extraction would change the workload, so the fixed pattern is re-expressed with primitives instead.

The lowering (`_lower_q29_referer`) reproduces the pattern's behavior rather
than approximating it:

- two guarded branches for the exact lowercase `http://` and `https://`
  prefixes;
- a `Latin1_General_100_BIN2_UTF8` collation, forcing case-sensitive matching
  and keeping all offsets on the same UTF-8 encoded input;
- a required non-empty host, and rejection of a line feed after the first slash
  (a line feed *inside* the host stays allowed, matching `[^/]+`);
- `www.` removed only when at least one host character remains, reproducing the
  source pattern's backtracking on `http://www./path`;
- every `SUBSTRING` length clamped at zero rather than relying on `CASE`
  evaluation order;
- non-matching input and NULL return the original value.

It is verified by **executing it against the source regex** over match,
non-match, NULL, Unicode, and `www.`-backtracking inputs — not by inspection.

> If your warehouse *does* expose `REGEXP_REPLACE`, unregister `_lower_q29_host_extraction` from the engine's registry to stay closer to the upstream text. Availability was never confirmed against a live Fabric Data Warehouse endpoint; the published T-SQL surface-area documentation is the basis for the default.

---

## Divergence from earlier hand-written overrides

These rules replace a set of hand-written Fabric Data Warehouse SQL files (q3, q4, q10, q19, q29, q30, q35, q40, q43). Four of them — q19, q35, q40, q43 — matched the registered rules' semantics exactly, which is independent confirmation that the rules address real Fabric limitations.

Two intentional differences remain. **Both move results toward the ClickHouse source, so Fabric Data Warehouse numbers are not comparable to runs predating this change.**

### q29 never stripped `www.`

The override used:

```sql
CASE WHEN CHARINDEX('www.', Referer) = 1 THEN ... END
```

This tests the **raw** `Referer`, which always starts with its scheme, so the
condition is never true and `www.` was never stripped. `www.host` and `host`
were counted as separate groups, changing the query's grouping cardinality and
reported row count.

Verified in DuckDB — for `http://www.example.com/path`:

| | Result |
|---|---|
| Override | `www.example.com` |
| Upstream regex | `example.com` |

A regression test guards against reintroducing this.

### `AVG` truncation

The overrides widened `AVG` with `CONVERT(BIGINT, ...)` or
`CONVERT(DECIMAL(38, 0), ...)`. That prevents the overflow but makes T-SQL
return a whole-number average where ClickHouse returns `Float64`. They also left
q28 and q31–q33 truncating entirely.

The registered rule widens to `FLOAT` and covers every integer `AVG`.

### Note on q30

The registered rule produces `SUM(CAST(RW + N AS BIGINT))`; the override used
`SUM(CONVERT(BIGINT, RW) + N)`. Both are safe — `SMALLINT + INT literal`
promotes to `INT` with a maximum of 32,856, so there is no per-row overflow
either way.

---

## Verification

- 43 queries × 4 dialects (DuckDB, Fabric, MySQL, Spark), pinned in `tests/fixtures/clickbench_query_rendering.json` (172 fingerprints), verified under both pinned SQLGlot versions. Fabric is rendered through the `FabricDataWarehouse` engine, so the engine-registered rules are pinned too.
- `tests/test_clickbench_query_generation.py`, including the q29 regex-equivalence harness, the `www.`-stripping regression guard, and the `AVG` real-type guard.
- ScriptDom grammar validation: 0 errors across 86 parses (43 × `fabric` × `All`/`SqlAzure`).
- Live DuckDB execution of all 43 queries.
- Source hashes checked against `source_manifest.json`.

Not verified: live Spark or Fabric Data Warehouse execution.

### SQLGlot version-dependent rendering

Five of the 172 fingerprints differ on SQLGlot 26.30.0 and are recorded under `overrides` in the fixture. Both differences are rendering-only:

| Queries | 30.18.0 | 26.30.0 | Why it is equivalent |
|---|---|---|---|
| q23, all 4 dialects | `URL NOT LIKE '%.google.%'` | `NOT URL LIKE '%.google.%'` | Identical predicate, different placement of the negation; both are valid in every target dialect. |
| q29, DuckDB only | `REGEXP_REPLACE(..., '\1', 'g')` | `REGEXP_REPLACE(..., '\1')` | The pattern is fully anchored (`^...$`), so it can match at most once; the global flag cannot change the result. |

Neither warrants a normalizer rule — a rule would pin one version's cosmetic
choice and add a substitution the source did not ask for.
