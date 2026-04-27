# LakeBench on Microsoft Fabric — Install & First Run

LakeBench talks to Fabric via the **Livy REST API** — no JVM, no Spark cluster
on your laptop. Everything runs on the remote Fabric Spark pool.

## TL;DR

```bash
# 1. Install LakeBench with the Fabric extra (alias for [livy])
pip install 'lakebench[fabric]'

# 2. Install Azure CLI (one-time, OS-specific)
brew install azure-cli                                                # macOS
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash                # Ubuntu/Debian
winget install -e --id Microsoft.AzureCLI                             # Windows

# 3. Sign in (interactive; opens a browser)
az login

# 4. Sanity check
lakebench doctor

# 5. Discover what TPC-* / ClickBench / ELTBench data is already in your lakehouse
lakebench discover --profile my-fabric --format table
```

That's the full happy path.

---

## Authoring a Fabric profile

Add a block under `profiles` in `~/.lakebench.json`:

```jsonc
"fabric-mylake": {
  "engine": "livy",
  "engine_options": {
    "url": "https://api.fabric.microsoft.com/v1/workspaces/<WORKSPACE_GUID>/lakehouses/<LAKEHOUSE_GUID>/livyApi/versions/2023-12-01",
    "schema_or_working_directory_uri": "abfss://<WORKSPACE_GUID>@onelake.dfs.fabric.microsoft.com/<LAKEHOUSE_GUID>/Tables",
    "auth": "az",
    "session_conf": {
      "spark.executor.instances": "10",
      "spark.executor.cores": "8",
      "spark.executor.memory": "56g",
      "spark.driver.cores": "8",
      "spark.driver.memory": "56g"
    }
  }
}
```

Required keys:

| Key | Where to find it |
|---|---|
| `url` | Fabric portal → Workspace → Settings → Workspace ID; Lakehouse → Settings → Lakehouse ID. Plug into the template above. |
| `schema_or_working_directory_uri` | Same two GUIDs; `Tables` is the OneLake convention. |
| `auth` | `"az"` for interactive login (recommended), `"bearer"` + `token_env` for service principal / PAT. |

Optional but useful: `session_conf` — anything you'd put in a Fabric notebook
`%%configure`. Ours mirrors a Standard pool with 10 workers (see
`my-fabric` in the bundled `~/.lakebench.json` for a battle-tested set).

---

## What "just works" — and what doesn't

| Behavior | Status |
|---|---|
| Auto session-name on Synapse-flavored Livy (Fabric is fine without) | ✅ Wave H |
| Multi-part schema names (`workspace.lakehouse.schema`) | ✅ Wave H |
| Per-query timeout via `--query-timeout SECONDS` | ✅ Wave H |
| Auto-recovery on Livy session hang/network drop | ✅ Wave H |
| Token refresh (Azure AD tokens last ~1h) | ✅ Refreshed 2 min before expiry |
| First-time `az login` | ❌ Not auto-launched — `lakebench doctor` warns if missing |
| Lakehouse provisioning | ❌ Out of scope — create lakehouses in the Fabric UI |

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `Failed to get Azure token via 'az' CLI` | Not signed in | `az login` |
| `Failed to create Livy session (400): Cannot be empty (Parameter 'Name')` | Synapse-style endpoint | Already fixed in Wave H — `pip install -U lakebench` |
| `SCHEMA_NOT_FOUND` on multi-part name | Pre-Wave-H sqlglot bug | Already fixed — `pip install -U lakebench` |
| `q71 hangs forever` | Known SF=1000 pathology in Fabric Native Engine | Use `--query-timeout 1800` to bound it; ticket #6 in PROGRESS.md |
| `Capacity not active` | Capacity paused | Resume in Azure portal → your Fabric capacity → Resume |

---

## One-liner benchmarks

```bash
# Run TPC-DS SF=1000 against an existing 3-part-named delta dataset
lakebench run --profile my-fabric \
              --benchmark tpcds --scenario sf1000 --scale-factor 1000 \
              --database 'tpcds-westus-bench.tpcds_bench.tpcds_sf1000_delta' \
              --mode query \
              --query-timeout 1800 \
              --save-results

# Re-run only the failed queries
lakebench run --profile my-fabric --benchmark tpcds --scenario sf1000 \
              --scale-factor 1000 \
              --database 'tpcds-westus-bench.tpcds_bench.tpcds_sf1000_delta' \
              --mode query \
              --query-list q23a,q23b,q72
```

See [`cli-quickstart.md`](./cli-quickstart.md) for more recipes.
