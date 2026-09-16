# 🌊 LakeBench
[![PyPI Release](https://img.shields.io/pypi/v/lakebench)](https://pypi.org/project/lakebench/)
[![PyPI Downloads](https://img.shields.io/pepy/dt/lakebench.svg?label=PyPI%20Downloads)](https://pypi.org/project/lakebench/)
[![Python version](https://img.shields.io/pypi/pyversions/LakeBench)](https://pypi.org/project/LakeBench)
[![Tests](https://img.shields.io/github/actions/workflow/status/microsoft/LakeBench/tests.yml?logo=github&label=tests&branch=main)](https://github.com/microsoft/LakeBench/actions/workflows/tests.yml)


LakeBench is the first Python-based, multi-modal benchmarking framework designed to evaluate performance across multiple lakehouse compute engines and ELT scenarios. Supporting a variety of engines and both industry-standard and novel benchmarks, LakeBench enables comprehensive, apples-to-apples comparisons in a single, extensible Python library.

## 🚀 The Mission of LakeBench
LakeBench exists to bring clarity, trust, accessibility, and relevance to engine benchmarking by focusing on four core pillars:
1. **End-to-End ELT Workflows Matter**
    
    Most benchmarks focus solely on analytic queries. But in practice, data engineers manage full data pipelines — loading data, transforming it (in batch, incrementally, or even streaming), maintaining tables, and then querying.

    > LakeBench proposes that **the entire end-to-end data lifecycle managed by data engineers is relevant**, not just queries.

1. **Variety in Benchmarks Is Essential**

    Real-world pipelines deal with with different data shapes, sizes, and patterns. One-size-fits-all benchmarks miss this nuance.

    > LakeBench covers a **variety of benchmarks** that represent **diverse workloads** — from bulk loads to incremental merges to maintenance jobs to ad-hoc queries — providing a richer picture of engine behavior under different conditions.

1. **Consistency Enables Trustworthy Comparisons**

    Somehow, every engine claims to be the fastest at the same benchmark, _at the same time_. Without a standardized framework, with support for many engines, comparisons are hard to trust and even more difficult to reproduce.

    > LakeBench ensures **consistent methodology across engines**, reducing the likelihood of implementation bias and enabling repeatable, trustworthy results. Engine subject matter experts are _encouraged_ to submit PRs to tune code as needed so that their preferred engine is best represented.

1. **Accessibility starts with `pip install`**

    Most benchmarking toolkits are highly inaccessible to the beginner data engineer, requiring the user to build the package or installation via a JAR, absent of Python bindings.

    > LakeBench is intentionally built as a **Python-native library**, installable via `pip` from PyPi, so it's easy for any engineer to get started—no JVM or compilation required. It's so lightweight and approachable, you could even use it just for generating high-quality sample data.


## ✅ Why LakeBench?
- **Multi-Engine**: Benchmark Spark, DuckDB, Polars, Daft, Sail and others, side-by-side
- **Lifecycle Coverage**: Ingest, transform, maintain, and query—just like real workloads
- **Diverse Workloads**: Test performance across varied data shapes and operations
- **Consistent Execution**: One framework, many engines
- **Extensible by Design**: Add engines or additional benchmarks with minimal friction
- **Dataset Generation**: Out-of-the box dataset generation for all benchmarks
- **Rich Logs**: Automatically logged engine version, compute size, duration, estimated execution cost, etc.

LakeBench empowers data teams to make informed engine decisions based on real workloads, not just marketing claims.

## 💪 Benchmarks

LakeBench currently supports four benchmarks with more to come:

- **ELTBench**: An benchmark that simulates typicaly ELT workloads:
  - Raw data load (Parquet → Delta)
  - Fact table generation
  - Incremental merge processing
  - Table maintenance (e.g. OPTIMIZE/VACUUM)
  - Ad-hoc analytical queries
- **[TPC-DS](https://www.tpc.org/tpcds/)**: An industry-standard benchmark for complex analytical queries, featuring 24 source tables and 99 queries. Designed to simulate decision support systems and analytics workloads.
- **[TPC-H](https://www.tpc.org/tpch/)**: Focuses on ad-hoc decision support with 8 tables and 22 queries, evaluating performance on business-oriented analytical workloads.
- **[ClickBench](https://github.com/ClickHouse/ClickBench)**: A benchmark that simulates ad-hoc analytical and real-time queries on clickstream, traffic analysis, web analytics, machine-generated data, structured logs, and events data. The load phase (single flat table) is followed by 43 queries.

_Planned_
- **[TPC-DI](https://www.tpc.org/tpcdi/)**: An industry-standard benchmark for data integration workloads, evaluating end-to-end ETL/ELT performance across heterogeneous sources—including data ingestion, transformation, and loading processes.

## ⚙️ Engine Support Matrix

LakeBench supports multiple lakehouse compute engines. Each benchmark scenario declares which engines it supports via `<BenchmarkClassName>.BENCHMARK_IMPL_REGISTRY`.

| Engine          | ELTBench | TPC-DS | TPC-H   | ClickBench |
|-----------------|:--------:|:------:|:-------:|:----------:|
| Spark (Generic) |    ✅    |   ✅   |   ✅  |    ✅    |
| Fabric Spark    |    ✅    |   ✅   |   ✅  |    ✅    |
| Synapse Spark   |    ✅    |   ✅   |   ✅  |    ✅    |
| HDInsight Spark |    ✅    |   ✅   |   ✅  |    ✅    |
| DuckDB          |    ✅    |   ✅   |   ✅  |    ✅    |
| Polars          |    ✅    |   ⚠️   |   ⚠️  |    ⚠️    |
| Daft            |    ✅    |   ⚠️   |   ⚠️  |    ⚠️    |
| Sail            |    ✅    |   ✅   |   ✅  |    ✅    |

> **Legend:**  
> ✅ = Supported  
> ⚠️ = Some queries fail due to syntax issues (i.e. Polars doesn't support SQL non-equi joins, Daft is missing a lot of standard SQL contructs, i.e. DATE_ADD, CROSS JOIN, Subqueries, non-equi joins, CASE with operand, etc.).
> 🔜 = Coming Soon  
> (Blank) = Not currently supported

For detailed pass rates and per-query failure analysis, see the [coverage reports](reports/coverage/).

## 📊 Engine Coverage Reports

Per-engine coverage reports are auto-generated by the integration test suite and show pass rates with individual query failure details.  
To refresh: run the integration tests for your engine of choice (see [`tests/integration/README.md`](tests/integration/README.md)).

| Engine | Report |
|--------|--------|
| DuckDB | [reports/coverage/duckdb.md](reports/coverage/duckdb.md) |
| Polars | [reports/coverage/polars.md](reports/coverage/polars.md) |
| Daft   | [reports/coverage/daft.md](reports/coverage/daft.md) |
| Spark  | [reports/coverage/spark.md](reports/coverage/spark.md) |
| Sail   | [reports/coverage/sail.md](reports/coverage/sail.md) |

## Where Can I Run LakeBench?
Multiple modalities doesn't end at just benchmarks and engines, LakeBench also supports different runtimes and storage backends:

**Runtimes**:
  - Local (Windows)
  - Fabric
  - Synapse
  - HDInsight
  - Google Colab ⚠️

**Storage Systems**:
  - Local filesystem (Windows)
  - OneLake
  - ADLS gen2 (temporarily only in Fabric, Synapse, and HDInsight)
  - S3 ⚠️
  - GS ⚠️

_* ⚠️ denotes experimental storage backends_

## What Table Formats Are Supported?
LakeBench currently only supports Delta Lake.

## 🔌 Extensibility by Design

LakeBench is designed to be _extensible_, both for additional engines and benchmarks. 

- You can register **new engines** without modifying core benchmark logic.
- You can add **new benchmarks** that reuse existing engines and shared engine methods.
- LakeBench extension libraries can be created to extend core LakeBench capabilities with additional custom benchmarks and engines (i.e. `MyCustomSynapseSpark(Spark)`, `MyOrgsELT(BaseBenchmark)`).

New engines can be added via subclassing an existing engine class. Existing benchmarks can then register support for additional engines via the below:

```python
from lakebench.benchmarks import TPCDS
TPCDS.register_engine(MyNewEngine, None)
```

_`register_engine` is a class method to update `<BenchmarkClassName>.BENCHMARK_IMPL_REGISTRY`. It requires two inputs, the engine class that is being registered and the engine specific benchmark implementation class if required (otherwise specifying `None` will leverage methods in the generic engine class)._

This architecture encourages experimentation, benchmarking innovation, and easy adaptation.

_Example:_
```python
from lakebench.engines import BaseEngine

class MyCustomEngine(BaseEngine):
    ...

from lakebench.benchmarks.elt_bench import ELTBench
# registering the engine is only required if you aren't subclassing an existing registered engine
ELTBench.register_engine(MyCustomEngine, None)

benchmark = ELTBench(engine=MyCustomEngine(...))
benchmark.run()
```

---

# Using LakeBench

## 📦 Installation

Install from PyPi:

```bash
pip install lakebench[duckdb,polars,tpcds_datagen,tpch_datagen,sparkmeasure]
```

> _Note: the `daft` extra pins `deltalake` to 1.5.x (Daft cannot read the Arrow `Utf8View` parquet that `deltalake` 1.6.x emits from `MERGE`), so it must be installed in its own environment rather than alongside `duckdb`, `polars`, or `sail`._
>
> `tpch_datagen` and `tpcds_datagen` use the same self-contained Rust
> `tpcgen-cli` binary bundled in the Windows x86_64 and Linux x86_64 LakeBench
> wheels. The legacy DuckDB TPC-DS generator remains available separately
> through `tpcds_duckdb_datagen`.

## Example Usage
To run any LakeBench benchmark, first do a one time generation of the data required for the benchmark and scale of interest. LakeBench provides datagen classes to quickly generate parquet datasets required by the benchmarks.

### Data Generation
- **TPC-H** and **TPC-DS** data generation is blazing fast via a pinned build of the unified Rust `tpcgen-cli` from the [tpcgen-rs](https://github.com/datafusion-contrib/tpcgen-rs) project. The temporary Windows x86_64 and manylinux 2.17 x86_64 executables are committed under `native/tpcgen`. LakeBench will migrate to the official `tpcgen-cli` Python package after it is released on PyPI.

    _The below are generation runtimes on a 64 v-core VM writing to OneLake. Scale factors below 1000 can easily be generated on a 2 v-core machine._
    | Scale Factor | TPC-H Duration (hh:mm:ss)| TPC-DS Duration (hh:mm:ss)|
    |:------------:|:------------------:|:------------------:|
    | 1            | 00:00:20           | 00:00:24           |
    | 10           | 00:00:34           | 00:01:01           |
    | 100          | 00:01:26           | 00:02:17           |
    | 1000         | 00:07:49           | 00:10:30           |

- **ClickBench** data is downloaded directly from the Clickhouse host site.

#### TPC-H Data Generation
```python
from lakebench.datagen import TPCHDataGenerator

datagen = TPCHDataGenerator(
    scale_factor=1,
    target_folder_uri='/lakehouse/default/Files/tpch_sf1'
)
datagen.run()
```

#### TPC-DS Data Generation
```python
from lakebench.datagen import TPCDSDataGenerator

datagen = TPCDSDataGenerator(
    scale_factor=1,
    target_folder_uri='/lakehouse/default/Files/tpcds_sf1'
)
datagen.run()
```

_Notes:_
- By default, each table is split automatically using its estimated total
  compressed size: 128 MiB files below 10 GiB, 256 MiB below 1 TiB, 512 MiB
  below 5 TiB, and 1 GiB for larger tables. Estimated physical size is
  calculated directly from each table's SF1000 baseline for any supported
  scale factor; part counts are always selected automatically.
- `target_row_group_size_mb` is an on-disk compressed-size target. LakeBench
  converts it to the uncompressed-byte value expected by `tpcgen-cli` using
  benchmark- and table-specific ZSTD(1) or Snappy compression ratios measured
  from SF10 output. All `ZSTD(N)` levels use the ZSTD(1) measurements for
  planning while the requested compression level is passed through unchanged.
  Automatic part counts are adjusted for the selected codec. The row-group
  conversion includes a 5% planning margin for upstream's estimated
  bytes-per-source-row model. Other compressed codecs require an explicit
  `compression_factor`, which is used for both row groups and file estimates.
  TPC-DS generation uses the upstream C-reference compatibility mode.
- Output remains organized as `<root>/<table>/*.parquet`. Filenames include
  the one-based part number and codec for quick inspection, for example
  `lineitem/lineitem-00001.zstd.parquet` or
  `store_sales/store_sales-00001.zstd.parquet`.
- To use the legacy implementation, install
  `lakebench[tpcds_duckdb_datagen]` on Python 3.10+ and pass
  `backend="duckdb"`.- Editable/source installations use the matching vendored binary from
- `output_format="native"` emits the TPC generators' pipe-delimited text
  instead of Parquet, matching what the official `dsdgen`/`dbgen` tools
  produce. See
  [Native TPC Generator Format](#native-tpc-generator-format-dat--tbl) below.- Editable/source installations use the matching vendored binary from
  `native/tpcgen`; installed wheels always use their packaged binary.
- Large generations targeting mounted filesystems can set `num_threads=8` or
  `num_threads=16` to limit concurrent file creation and atomic renames. The
  default remains all available CPU cores.
- The ClickBench dataset (only 1 size) should download with partitioned files in ~ 1 minute and ~ 6 minutes as a single file. 

#### Native TPC Generator Format (`.dat` / `.tbl`)

TPC-H and TPC-DS can be generated and loaded in the TPC tools' native
pipe-delimited text format instead of Parquet. This measures the load phase
against the same raw format the official `dsdgen` and `dbgen` tools emit,
rather than a pre-typed columnar file.

```python
from lakebench.datagen import TPCHDataGenerator
from lakebench.benchmarks import TPCH
from lakebench.engines import Polars

TPCHDataGenerator(
    scale_factor=1,
    target_folder_uri='/lakehouse/default/Files/tpch_sf1_native',
    output_format="native",      # .tbl for TPC-H, .dat for TPC-DS
).run()

benchmark = TPCH(
    engine=Polars(schema_or_working_directory_uri='...'),
    scenario_name='native-load',
    scale_factor=1,
    input_parquet_folder_uri='/lakehouse/default/Files/tpch_sf1_native',
    input_format="native",
)
benchmark.run(mode='load_and_query')
```

_Notes:_
- Files are named exactly as the official tools name them, one folder per
  table. A single-part table uses the serial name `<table>.tbl` / `<table>.dat`.
  A multi-part table uses the parallel names: `dbgen`'s
  `<table>.tbl.<step>` for TPC-H and `dsdgen`'s
  `<table>_<child>_<parallel>.dat` for TPC-DS.
- Neither `dsdgen` nor `dbgen` splits output on its own; parts exist only
  because the operator runs the tool once per chunk, which is the normal way
  to generate large scale factors. LakeBench picks the part count for you from
  the estimated table size, so bigger scale factors naturally produce more
  parts, matching what a parallel `dsdgen`/`dbgen` run would leave on disk.
- The format carries no header and no types, so LakeBench derives each
  reader's schema from the benchmark's resolved DDL. As a result, native loads
  are always typed exactly as the DDL declares. The generator's Parquet output
  does not always agree with the DDL on integer width (for example TPC-H
  `n_nationkey` is `int64` in Parquet but `integer` in the DDL), so on engines
  that do not pre-create tables the two formats can differ in integer width.
  Values are identical.
- Every generated line ends with a trailing delimiter. LakeBench reads one
  extra trailing column and drops it, so no reader needs a lenient mode.
- Empty fields are read as `NULL`, and quoting is disabled so `"` is treated
  as ordinary data.
- Parquet-only options (`target_row_group_size_mb`, `compression`, and
  `compression_factor`) are rejected with `output_format="native"`. Automatic
  part counts use per-table native-to-uncompressed-Parquet size ratios measured
  at SF1.
- Supported on the DuckDB, Polars, Daft, Sail, and Spark engines. Engines with
  a benchmark-specific Parquet loader (such as Fabric Warehouse) reject
  `input_format="native"` rather than silently loading Parquet.
- `output_format="native"` requires `backend="rust"`; `tpcgen-cli tpcds dat`
  does not accept `--num-threads`, so that option is ignored for TPC-DS native
  generation only.

#### Is BYO Data Supported?If you want to use your own TPC-DS, TPC-H, or ClickBench Parquet datasets, that is fine and encouraged as long as they are to specification. LakeBench keeps the canonical TPC-DS schema as its table and query contract, but automatically corrects these recognized legacy input names while loading Parquet:

| Benchmark | Table | Legacy input name | Canonical LakeBench name |
|---|---|---|---|
| TPC-DS | `catalog_returns` | `cr_return_amount_inc_tax` | `cr_return_amt_inc_tax` |
| TPC-DS | `income_band` | `ib_income_band_id` | `ib_income_band_sk` |
| TPC-DS | `reason` | `r_reason_description` | `r_reason_desc` |
| TPC-DS | `store` | `s_tax_precentage` | `s_tax_percentage` |
| TPC-DS | `web_returns` | `wr_store_credit` | `wr_account_credit` |

Canonical names are accepted unchanged. Input containing both names, or neither required name, is rejected as ambiguous or invalid. Loaded Delta tables always use the canonical name.

### Load-Time Statistics

TPC-H and TPC-DS benchmarks can include statistics generation in the measured load phase:

```python
benchmark = TPCH(
    engine=engine,
    scenario_name="sf10",
    input_parquet_folder_uri="abfss://...",
    analyze="selective",
)
```

The `analyze` option supports:

- `"none"` (default): Do not generate statistics during load.
- `"full"`: Ask the engine to generate statistics for every table column.
- `"selective"`: Generate statistics only for the benchmark-maintained columns used by the workload.

For backward compatibility, `analyze=True` is equivalent to `"full"` and `analyze=False` is equivalent to `"none"`.

Fabric Spark separately enables Delta extended statistics during writes by default. Set
`collect_stats_on_write=False` only when isolating explicit analyze costs:

```python
engine = FabricSpark(
    lakehouse_name="lakehouse",
    lakehouse_schema_name="schema",
    collect_stats_on_write=False,
)
```

### Fabric Spark
```python
from lakebench.engines import FabricSpark
from lakebench.benchmarks import ELTBench

engine = FabricSpark(
    lakehouse_workspace_name="workspace",
    lakehouse_name="lakehouse",
    lakehouse_schema_name="schema",
    spark_measure_telemetry=True
)

benchmark = ELTBench(
    engine=engine,
    scenario_name="sf10",
    mode="light",
    input_parquet_folder_uri="abfss://...",
    save_results=True,
    result_table_uri="abfss://..."
)

benchmark.run()
```

> _Note: The `spark_measure_telemetry` flag can be enabled to capture stage metrics in the results. The `sparkmeasure` install option must be used when `spark_measure_telemetry` is enabled (`%pip install lakebench[sparkmeasure]`). Additionally, the Spark-Measure JAR must be installed from Maven: https://mvnrepository.com/artifact/ch.cern.sparkmeasure/spark-measure_2.13/0.24_

### Polars
```python
from lakebench.engines import Polars
from lakebench.benchmarks import ELTBench

engine = Polars( 
    schema_or_working_directory_uri = 'abfss://...'
)

benchmark = ELTBench(
    engine=engine,
    scenario_name="sf10",
    mode="light",
    input_parquet_folder_uri="abfss://...",
    save_results=True,
    result_table_uri="abfss://..."
)

benchmark.run()
```
---

## Managing Queries Over Various Dialects

LakeBench uses SQLGlot to translate benchmark queries to each engine's dialect. Every benchmark starts from an immutable upstream SQL source; compatibility changes are registered AST rules, not alternate SQL files.

Each benchmark's full rule inventory — what each rule does, why it exists, and what it does and does not change — is documented on its own page:

| Benchmark | Source of truth | Reader | Normalization rules |
|---|---|---|---|
| TPC-H | `qgen` ANSI output, SF1000 / SF10000, stream 0 | `tsql` | [docs/query-normalization/tpch.md](docs/query-normalization/tpch.md) |
| TPC-DS | `dsqgen` ANSI output, SF1000 / SF10000, stream 0 | `tsql` | [docs/query-normalization/tpcds.md](docs/query-normalization/tpcds.md) |
| ClickBench | Upstream `clickhouse/queries.sql`, pinned commit | `clickhouse` | [docs/query-normalization/clickbench.md](docs/query-normalization/clickbench.md) |

See [docs/query-normalization/](docs/query-normalization/README.md) for the shared pipeline, the rule contract, and guidance on adding a rule.

### Query Resolution Strategy

Runtime compilation proceeds in this order for every benchmark:

1. Load the canonical source and parse it with the benchmark's `CANONICAL_QUERY_DIALECT`.
2. Apply `SOURCE_NORMALIZERS` to parsed statement bundles, for lowerings that must happen before the bundle is reduced to a single query.
3. Apply `QUERY_NORMALIZERS`: shared `"*"` rules first, then rules for the query ID.
4. Apply `ENGINE_QUERY_NORMALIZERS` registered for the engine class and its ancestors, base classes first. Each class uses the same `"*"`-then-query convention.
5. Qualify catalog/schema references and render the AST directly to the engine's `SQLGLOT_DIALECT`, without an intermediate SQL serialization.

Join normalization is not registered by default for any benchmark; WHERE join predicates remain in place even when SQLGlot renders comma joins as CROSS JOIN. The shared join-normalization function remains available for explicit registration.

Static TPC query sets currently cover SF1000 and SF10000. Other data scales log a warning and use SF1000 query substitutions; result metadata records the mismatch via `query_set_scale_matches_data`.

**Breaking change in v2:** engine, parent-engine, and third-party SQL-file query overrides are no longer searched for any benchmark, including ClickBench. Existing overrides must be migrated to registered structural rules. Engine-specific DDL resolution is unchanged. ClickBench results on Fabric Warehouse are **not** comparable to runs predating this change — see [the ClickBench page](docs/query-normalization/clickbench.md#divergence-from-earlier-hand-written-overrides).

### SQLGlot Upgrade Guardrails

SQLGlot is pinned to **30.18.0 on Python 3.9+**. Python 3.8 retains **26.30.0**
because newer SQLGlot releases require Python 3.9+. The AST adapters support both
versions, including FROM/WITH argument names, DROP VIEW target lists, and GROUPING
function nodes.

`tests/test_tpc_sqlglot_compatibility.py` checks reviewed output fingerprints in
`tests/fixtures/tpc_query_rendering.json` for all **750** TPC-H/TPC-DS renderings
(both static scales, Spark, T-SQL, and Fabric, on both pinned SQLGlot versions).
Review actual SQL differences before
refreshing these fingerprints; do not regenerate them just to clear a failure.
The 26.30.0-to-30.18.0 comparison found 472 identical outputs and 28 differences
limited to equivalent NOT LIKE spelling and generated subquery alias names.
Generated source hashes remain independently checked against their manifests.

The upgrade does not make the existing compatibility rules redundant. The
built-in Fabric dialect is a suitable Warehouse target, but does not replace
them — see the per-benchmark pages linked above for what each rule still covers.

Case-sensitive binding checks intentionally bypass identifier normalization so
they catch mismatches like SR_FEE versus the declared sr_fee column. Successful
grammar parsing alone does not establish Warehouse execution support.

### Registering Compatibility Rules

Registries live in each benchmark's `_query_normalizers.py` and are bound on the benchmark class. AST rules accept `(expression, context)`, modify the supplied copied AST, and return `None`. Source rules instead receive a mutable list of parsed statements; after source lowering, exactly one query must remain. Rules must validate expected shapes and raise explicitly rather than substitute another query.

`context.dialect` is the source parser dialect; `context.target_dialect` is the
engine's output dialect. Target-specific rules must inspect the latter. The
runtime supplies it to both source and query rules; direct callers of
`apply_query_normalizers` can pass `target_dialect` explicitly.

For example, an engine integration can extend the TPC-H engine registry:

```python
TPCH.ENGINE_QUERY_NORMALIZERS = {
    **TPCH.ENGINE_QUERY_NORMALIZERS,
    MyEngine: {"q14": (normalize_q14_for_my_engine,)},
}
```

Preserve generated literals and make AST rules idempotent. Current engine accommodations include Daft DOUBLE arithmetic casts in TPC-H q1/q8/q9/q14 and Sail's NULLIF denominator in TPC-DS q12. These are **semantic accommodations** (numeric precision and division-by-zero behavior), not merely syntax fixes. Applied rule identifiers are recorded per query in `execution_telemetry["query_normalization_rules"]`, alongside the benchmark's normalizer version in engine metadata. Successful transpilation alone does not establish specification equivalence or engine execution support.

Full guidance on writing and registering a rule — including bumping `NORMALIZER_VERSION` and refreshing rendering fingerprints — is in [docs/query-normalization/](docs/query-normalization/README.md#adding-a-rule).

### Viewing Generated Queries

To inspect the final query that will be executed for any engine:

```python
benchmark = TPCH(engine=MyEngine(...))
query_str = benchmark._return_query_definition('q14')
print(query_str)  # Shows final transpiled/customized query
```

All engines now receive the same selected TPC source substitutions, with compatibility changes explicit and reviewable.

# 📬 Feedback / Contributions
Got ideas? Found a bug? Want to contribute a benchmark or engine wrapper? PRs and issues are welcome!


# Licensing and Third-Party Material

LakeBench is released under the MIT License (see [`LICENSE`](LICENSE)). It also redistributes material from third-party projects that remain under their own licenses and are **not** covered by MIT. These are itemized, with attribution and a description of modifications, in [`THIRD-PARTY-NOTICES.md`](THIRD-PARTY-NOTICES.md).

Most notably:

- **ClickBench** queries and schema come from [ClickHouse/ClickBench](https://github.com/ClickHouse/ClickBench) (Alexey Milovidov and the ClickHouse team, 2022), which is published under [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/). The queries are vendored verbatim from a pinned upstream commit; provenance and hashes are recorded in [`PROVENANCE.md`](src/lakebench/benchmarks/clickbench/resources/queries/canonical/PROVENANCE.md) and `source_manifest.json` alongside them. If the NonCommercial or ShareAlike terms matter for your use, review them before redistributing LakeBench or building on it.
- **tpcgen-rs** is bundled as a prebuilt binary in platform wheels under Apache 2.0.
- **TPC-H / TPC-DS** tools kits, templates, and generator executables are *not* redistributed; only generated query text and a reproducibility manifest are checked in. TPC-H and TPC-DS are trademarks of the [Transaction Processing Performance Council](https://www.tpc.org), and LakeBench results are not audited, endorsed, or comparable to published TPC results.


# Acknowledgement of Other _LakeBench_ Projects
The **LakeBench** name is also used by two unrelated academic and research efforts:
- **[RLGen/LAKEBENCH](https://github.com/RLGen/LAKEBENCH)**: A benchmark designed for evaluating vision-language models on multimodal tasks.
- **LakeBench: Benchmarks for Data Discovery over Lakes** ([paper link](https://www.catalyzex.com/paper/lakebench-benchmarks-for-data-discovery-over)):
    A benchmark suite focused on improving data discovery and exploration over large data lakes.

While these projects target very different problem domains — such as machine learning and data discovery — they coincidentally share the same name. This project, focused on ELT benchmarking across lakehouse engines, is not affiliated with or derived from either.
