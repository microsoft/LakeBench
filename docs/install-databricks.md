# LakeBench on Azure Databricks — Install & First Run

LakeBench talks to Databricks via **Databricks Connect** — runs in your laptop's
Python, executes on the remote DBR cluster. No JVM needed locally.

## TL;DR

```bash
# 1. Install LakeBench with the Databricks extra (pulls databricks-connect)
pip install 'lakebench[databricks]'

# 2. Install Azure CLI (one-time, OS-specific)
brew install azure-cli                                                # macOS
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash                # Ubuntu/Debian
winget install -e --id Microsoft.AzureCLI                             # Windows

# 3. Sign in (interactive; opens a browser)
az login

# 4. Sanity check
lakebench doctor

# 5. Discover what's already in the cluster's catalog
lakebench discover --profile my-databricks --catalog hive_metastore
```

---

## Authoring a Databricks profile

```jsonc
"databricks-mycluster": {
  "engine": "databricks",
  "engine_options": {
    "host": "https://adb-XXXXXXXXXXXXXXXX.X.azuredatabricks.net",
    "cluster_id": "0412-235310-wjl9eshf",
    "auth": "az",
    "schema_name": "default",
    "catalog_name": "hive_metastore",
    "use_temp_views": true
  }
}
```

| Key | How to get it |
|---|---|
| `host` | Databricks UI top-right URL bar |
| `cluster_id` | Compute → cluster → URL: `…/clusters/<this-is-the-id>` |
| `auth` | `"az"` (recommended) or `"pat"` + `token_env` |
| `catalog_name` | `hive_metastore` for legacy, or your Unity Catalog name |

---

## DBR ↔ databricks-connect compatibility

This is the one thing that bites everyone. The Python wheel must match the
cluster's runtime *minor version*: a DBR-14 cluster needs
`databricks-connect~=14.3.0`, a DBR-16 cluster needs `~=16.x`, etc.

**LakeBench handles this for you (Wave H).** On the first connect to a
mismatched cluster, the engine catches the version error, hits the cluster REST
API to fetch the actual DBR version, and `pip install --force-reinstall`s the
right `databricks-connect` build. Costs ~30 s once per (host, DBR-version).

If you want to **disable** the auto-realign:

```jsonc
"engine_options": { "auto_align_connect_version": false, ... }
```

Manual install once you know your DBR (e.g. 14.3):

```bash
pip install --force-reinstall 'databricks-connect~=14.3.0'
```

---

## Cluster requirements

Databricks Connect refuses to attach to:

- **Standard Cluster** (legacy access mode) — error `BAD_REQUEST: SingleClusterComputeMode … is not Shared or Single User Cluster`.
- Clusters that aren't running.

Fix in Databricks UI → Compute → Edit cluster → **Access mode = Single user** or **Shared**.

If you run into this with `databricks-wcus-dbr14`, see ticket #N in PROGRESS.md.

---

## What "just works" — and what doesn't

| Behavior | Status |
|---|---|
| Auto-align `databricks-connect` to cluster DBR | ✅ Wave H |
| Token refresh via `az` CLI | ✅ |
| Catalog discovery (`lakebench discover`) | ✅ Wave G |
| Cross-engine `--query-timeout` | ⚠️ Honored by Livy today; Databricks engine no-ops it (will not break the run) |
| Cluster start/stop | ❌ Out of scope — start the cluster from the Databricks UI |
| Cluster access-mode fix | ❌ Manual UI edit (LakeBench surfaces a friendly error) |
| Unity Catalog setup | ❌ Out of scope |

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `Unsupported combination of Databricks Runtime & Databricks Connect` | Wheel version mismatch | Wave H auto-handles. To disable: `auto_align_connect_version=false` |
| `BAD_REQUEST: SingleClusterComputeMode … is not Shared or Single User Cluster` | Cluster has Standard access mode | Edit cluster → Access mode = Single user or Shared |
| `Cluster <id> is in TERMINATED state` | Cluster not running | Start it in the UI; `databricks clusters start <id>` |
| `Failed to get Azure token via 'az' CLI` | Not signed in | `az login` |
| Hanging on first connect | First-time JAR download (~200MB Py4J + Spark) | Wait it out; subsequent connects are <10s |

---

## One-liner benchmarks

```bash
# Power test against an existing TPC-DS dataset
lakebench run --profile my-databricks \
              --benchmark tpcds --scenario sf1000 --scale-factor 1000 \
              --catalog hive_metastore --database tpcds_sf1000 \
              --mode query --save-results

# Compare two DBR versions on the same workspace
lakebench run --profile my-databricks  --benchmark tpch --scenario sf100 \
              --catalog hive_metastore --database tpch_sf100 --mode query
lakebench run --profile my-databricks-16  --benchmark tpch --scenario sf100 \
              --catalog hive_metastore --database tpch_sf100 --mode query
lakebench report compare --benchmark tpch --scenario sf100 \
              --engines databricks
```

See [`cli-quickstart.md`](./cli-quickstart.md) for more recipes.
