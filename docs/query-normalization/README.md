# Query Normalization

LakeBench runs every benchmark from an **immutable upstream SQL source**. Engine
compatibility is handled by registered AST rules applied at runtime, never by
hand-edited SQL files or a custom SQLGlot dialect.

This directory explains, per benchmark, exactly which rules exist and why.

| Benchmark | Source of truth | Reader | Rules |
|---|---|---|---|
| [TPC-H](tpch.md) | `qgen` ANSI output, SF1000 / SF10000, stream 0 | `tsql` | [see page](tpch.md#registered-rules) |
| [TPC-DS](tpcds.md) | `dsqgen` ANSI output, SF1000 / SF10000, stream 0 | `tsql` | [see page](tpcds.md#registered-rules) |
| [ClickBench](clickbench.md) | Upstream `clickhouse/queries.sql`, pinned commit | `clickhouse` | [see page](clickbench.md#registered-rules) |

ELTBench is authored by LakeBench rather than transcribed from an external
specification, so it has no normalization layer.

---

## Why rules instead of per-engine SQL files

A hand-written per-engine SQL file is a *replacement workload*. Once it exists,
nothing forces it to stay equivalent to the source, and divergence is invisible:
the query still runs and still reports a time.

This is not hypothetical. The Fabric Warehouse ClickBench overrides that
preceded this design contained a q29 rewrite that silently never stripped the
`www.` prefix, changing the query's grouping and its result. See
[ClickBench: divergence from earlier overrides](clickbench.md#divergence-from-earlier-hand-written-overrides).

Registered rules avoid this because they:

- start from the unmodified source every run, so drift cannot accumulate;
- validate the shape they expect and **raise** rather than silently skipping, so
  a changed source fails loudly instead of quietly rendering something else;
- are recorded per query in execution telemetry, so a result can be traced to
  the exact transformations behind it;
- are covered by tests that compare behavior against the source semantics.

---

## Pipeline

Resolution happens in `_load_and_query.py` (`_normalize_canonical_query`):

1. **Parse** the canonical source with the benchmark's `CANONICAL_QUERY_DIALECT`.
2. **`SOURCE_NORMALIZERS`** — operate on the parsed *statement list*, before it
   is reduced to one query. This is where multi-statement sources collapse
   (TPC-H q15) and where generator directives outside the SQL grammar are
   applied (TPC-H row limits).
3. **`QUERY_NORMALIZERS`** — operate on the single query expression.
4. **`ENGINE_QUERY_NORMALIZERS`** — rules registered per engine class, applied
   base-class first via reversed MRO, so a subclass inherits its parent's rules.
5. **Qualify** catalog/schema references and **render** the AST directly to the
   engine's `SQLGLOT_DIALECT`.

Each registry applies its `"*"` entry first, then entries for the specific query
ID.

> **Never serialize to intermediate SQL between stages.** Rendering to a
> stopgap dialect and reparsing has previously changed semantics (for example
> string concatenation). The AST goes straight from source to target.

### Rule contract

A rule takes `(expression, context)`, mutates the expression in place, and
returns `None`. It must:

- **validate its expected shape and raise `ValueError` if absent.** A rule that
  silently does nothing when the source changes is worse than no rule.
- **be idempotent.** Rules are asserted to be safe to apply twice.
- **preserve generated substitutions** — literals, dates, magnitudes, grouping
  keys, ordering, and limits are never altered.

`QueryNormalizerContext` carries:

| Field | Meaning |
|---|---|
| `query_name` | e.g. `"q29"` |
| `schema` | benchmark DDL schema, for type-aware and case-sensitive decisions |
| `dialect` | the **source** dialect being parsed |
| `target_dialect` | the **engine's output** dialect — gate engine-specific rules on this |
| `source_sql` | raw source text, for directives outside the SQL grammar |

`dialect` and `target_dialect` are easy to confuse. A rule that only exists to
satisfy Fabric Warehouse must test `context.target_dialect`, not
`context.dialect`.

> **A dialect is not an engine.** Polars and DuckDB both render through
> `SQLGLOT_DIALECT = "duckdb"`, but only DuckDB executes DuckDB SQL — Polars'
> SQL frontend rejects interval syntax and several functions DuckDB accepts.
> When an accommodation is for one engine rather than one output grammar,
> register it in `ENGINE_QUERY_NORMALIZERS`, which is keyed by engine class.

---

## Categories of accommodation

Rules fall into three kinds, in increasing order of how carefully they need to
be justified:

1. **Spelling** — the same operation, named differently by the target
   (`DATE_TRUNC` vs `DATETRUNC`). No semantic content.
2. **Type widening** — the operation is right but the target evaluates it in a
   narrower type than the source, producing an overflow error or a truncated
   result. The fix restores the source's result type.
3. **Lowering** — the target lacks the construct, so it is re-expressed with
   primitives. This is the only category that can plausibly change results, so
   each instance is documented and tested against the source semantics.

---

## Verification

- **Rendering fingerprints** — `tests/fixtures/tpc_query_rendering.json` (750
  TPC outputs) and `tests/fixtures/clickbench_query_rendering.json` (215) pin
  every rendered query. Any unintended change to any engine's SQL fails.
- **Grammar validation** — T-SQL and Fabric output is parsed with
  `Microsoft.SqlServer.TransactSql.ScriptDom` (`TSql160Parser`) under both
  `SqlEngineType.All` and `SqlEngineType.SqlAzure`.
- **Execution** — the DuckDB integration suite runs every query in every
  benchmark against real data.
- **Semantic equivalence** — lowerings are executed and compared against the
  construct they replace, not merely checked for parseability.
- **Version floor** — SQLGlot is pinned to 30.18.0 on Python 3.9+ and 26.30.0 on
  Python 3.8; AST adapters cover both layouts and both are exercised.

---

## Adding a rule

1. Confirm the problem is real on the target engine. A parser accepting syntax
   does not mean the engine executes it.
2. Add the rule to the benchmark's `_query_normalizers.py`, gating on
   `context.target_dialect` if it is engine-specific.
3. Register it under `"*"` or the query ID.
4. **Bump `NORMALIZER_VERSION`** in that module. It is recorded in results as
   `query_set_normalizer_version`, so results stay attributable.
5. Add a test covering the behavior, not just the rendering.
6. Regenerate the affected rendering fingerprints and review the SQL diff.
7. If the change affects results rather than only syntax, say so on the
   benchmark's page under its comparability note.

---

## Inspecting resolved SQL

```python
benchmark = TPCH(engine=MyEngine(...))
print(benchmark._return_query_definition("q14"))
```

Applied rule IDs for each executed query are recorded in per-query execution
telemetry.
