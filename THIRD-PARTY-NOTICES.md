# Third-Party Notices

LakeBench itself is licensed under the MIT License (see `LICENSE`). It also
redistributes material from the third-party projects listed below, each of which
remains under its own license and copyright. The MIT License does **not** apply
to these components.

---

## ClickBench

- **Project:** ClickBench — a Benchmark For Analytical Databases
- **Upstream:** https://github.com/ClickHouse/ClickBench
- **Attribution:** Alexey Milovidov and the ClickHouse team, 2022
  (https://github.com/ClickHouse/ClickBench#references-and-citation)
- **Upstream license:** Creative Commons Attribution-NonCommercial-ShareAlike
  4.0 International (CC BY-NC-SA 4.0)
  - Deed: https://creativecommons.org/licenses/by-nc-sa/4.0/
  - Legal code: https://creativecommons.org/licenses/by-nc-sa/4.0/legalcode
  - Upstream `LICENSE`: https://github.com/ClickHouse/ClickBench/blob/main/LICENSE

**What LakeBench redistributes**

`src/lakebench/benchmarks/clickbench/resources/queries/canonical/` contains the
43-query ClickBench workload, copied from the upstream query set at commit
`314839c510d59c62a27f9f16118460011df1f031` (file `clickhouse/queries.sql`). The
accompanying `hits` table definition in
`src/lakebench/benchmarks/clickbench/resources/ddl/` is derived from the
upstream ClickBench schema.

**Indication of changes made** (required by the license's attribution term)

- The upstream file holds one statement per line. Each line is stored verbatim
  as an individual `q<N>.sql` file, numbered to match its upstream line
  position. The statement text itself is unmodified; a `source_manifest.json`
  alongside the queries records per-query hashes so any change is detectable.
- Queries are adapted at runtime, per engine, by LakeBench's query normalization
  and rendering pipeline. These adaptations are applied to the parsed syntax
  tree and are not stored in the query files.
- The schema is expressed using the type names and conventions required by the
  engines LakeBench targets.

Query selection, filter literals, grouping, ordering, and result limits are
otherwise preserved from upstream.

**Dataset**

LakeBench does not redistribute the ClickBench dataset. `hits` Parquet data is
downloaded at runtime, on the user's request, directly from the ClickHouse-hosted
source (`https://datasets.clickhouse.com/hits_compatible/`). The dataset
originates from anonymized Yandex Metrica web-analytics traffic and carries its
own terms from its publisher.

**Note on scope**

Upstream's `LICENSE` file is a bare copy of the CC BY-NC-SA 4.0 legal code with
no copyright line and no statement of scope, and the upstream README contains no
licensing section. LakeBench therefore attributes the query workload to
ClickBench under those terms without asserting a position on which upstream
materials the license was intended to cover. Redistributors and downstream users
who need certainty about the NonCommercial or ShareAlike terms should consult
their own legal counsel or contact the upstream project.

---

## tpcgen-rs

- **Project:** tpcgen-rs — TPC data generation CLI
- **Upstream:** https://github.com/datafusion-contrib/tpcgen-rs
- **Build source:** https://github.com/mwc360/tpcgen-rs
- **License:** Apache License 2.0 — see `native/tpcgen/LICENSE.tpcgen-rs`

Platform wheels bundle a prebuilt `tpcgen-cli` binary under `native/tpcgen/`.
Per-platform build provenance, including upstream commit and binary checksum, is
recorded in `native/tpcgen/<platform>/provenance.json`.

---

## TPC-H and TPC-DS

TPC-H and TPC-DS are trademarks of the Transaction Processing Performance
Council (https://www.tpc.org). LakeBench does **not** redistribute the TPC tools
kits, query templates, `dsqgen`/`qgen` executables, or distribution data.

`src/lakebench/benchmarks/tpch/resources/queries/canonical/` and
`src/lakebench/benchmarks/tpcds/resources/queries/canonical/` contain query text
produced by running the official TPC generators, along with a
`generation_manifest.json` recording the generator version, RNG seed, stream, and
per-query hashes needed to reproduce that output. LakeBench benchmark results are
not audited or endorsed by the TPC and are not comparable to published TPC
results.
