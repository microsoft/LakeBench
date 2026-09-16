# ClickBench query provenance and attribution

The SQL files in this directory are copied from the ClickBench benchmark.

- **Upstream:** https://github.com/ClickHouse/ClickBench
- **Upstream file:** `clickhouse/queries.sql`
- **Pinned revision:** commit `314839c510d59c62a27f9f16118460011df1f031`
- **Attribution:** Alexey Milovidov and the ClickHouse team, 2022
- **Upstream license:** CC BY-NC-SA 4.0 — https://creativecommons.org/licenses/by-nc-sa/4.0/
- **Upstream license text:** https://github.com/ClickHouse/ClickBench/blob/main/LICENSE

This material is **not** covered by LakeBench's MIT license. See `THIRD-PARTY-NOTICES.md` in the repository root for the full notice.

`source_manifest.json` records the pinned commit, the upstream blob hash, and a SHA256 for each query so any modification is detectable. Those hashes cover the LF-normalized bytes, so they hold regardless of checkout line endings.

## Changes made

Relative to the upstream query set, and required to be indicated by the license's attribution term:

- The upstream file contains one statement per line. Each line is stored verbatim as its own `q<N>.sql` file, numbered to match its upstream line position. The statement text itself is unmodified.
- Queries are adapted per engine at runtime by LakeBench's normalization and rendering pipeline. Those adaptations are applied to the parsed syntax tree and are not stored here; see `_query_normalizers.py` in the ClickBench benchmark package for the registered rules and the reason each one exists.

Query selection, filter literals, grouping, ordering, and result limits are otherwise preserved from upstream. No query has been added, removed, or substituted.

## Dataset

The `hits` dataset is not redistributed by LakeBench. It is downloaded at runtime, on request, from the ClickHouse-hosted source; see `src/lakebench/datagen/clickbench.py`. The dataset originates from anonymized Yandex Metrica web-analytics traffic and carries its own terms from its publisher.
