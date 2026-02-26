# langsmith-billing-report

A zero-dependency Python script that generates trace-count billing reports from a LangSmith deployment, broken down by Org → Workspace, with optional Project-level detail.

## Requirements

- Python 3.10+
- A LangSmith service-account API key (`lsv2_sk_...`) with billing read access

No external packages required — uses only the Python standard library.

## Usage

### Single org

```bash
python langsmith_billing_report.py \
  --base-url https://api.smith.langchain.com \
  --api-key lsv2_sk_... \
  --start 2026-01-01 \
  --end 2026-02-01
```

For SaaS or environments where the API key is not implicitly org-scoped, also pass `--org-id`:

```bash
python langsmith_billing_report.py \
  --base-url https://api.smith.langchain.com \
  --api-key lsv2_sk_... \
  --org-id <org-uuid> \
  --start 2026-01-01 \
  --end 2026-02-01
```

### Multiple orgs (parallel)

Create an `orgs.json` file. Only `api_key` is required per entry; include `org_id` for SaaS environments or where the key is not implicitly org-scoped:

```json
[
  {"api_key": "lsv2_sk_..."},
  {"api_key": "lsv2_sk_...", "org_id": "uuid", "org_name": "Org B"}
]
```

Then run:

```bash
python langsmith_billing_report.py \
  --base-url https://api.smith.langchain.com \
  --orgs orgs.json \
  --start 2026-01-01 \
  --end 2026-02-01 \
  --output report.csv
```

Orgs are fetched in parallel (default: 4 workers, configurable with `--workers`). If multiple API keys resolve to the same org, duplicate rows are automatically removed.

## Options

| Flag | Description |
|---|---|
| `--base-url` | LangSmith base URL (required) |
| `--start` | Start date `YYYY-MM-DD`, inclusive (required) |
| `--end` | End date `YYYY-MM-DD`, exclusive — i.e. the day *after* the last day (required) |
| `--mode` | `granular` (default) or `overview` — see below |
| `--level` | `workspace` (default) or `project` — granular mode only |
| `--output` | Optional path to write CSV output. When set, suppresses table output to stdout |
| `--silent` | Suppress all output including progress messages. Requires `--output`, otherwise a warning is printed and results are lost |
| `--api-key` | Service account API key |
| `--org-id` | Organization UUID (optional if API key is org-scoped) |
| `--org-name` | Human-readable org name for the report (optional, auto-fetched if omitted) |
| `--orgs` | Path to JSON file for multi-org mode |
| `--workers` | Parallel workers for multi-org fetch (default: 4) |

## Modes

### Granular (`--mode granular`, default)

Uses the `/billing/granular-usage` endpoint, scoped to the org the API key belongs to. Supports `--level workspace` (default) and `--level project`.

> **Self-hosted**: Requires LangSmith v0.13.12 or later. Use `--mode overview` on older versions.

### Overview (`--mode overview`)

Uses the `/billing/usage` endpoint, which pulls data from the billing provider. Shows a per-metric breakdown per workspace — useful for seeing the base/extended trace split as it appears in billing.

```
ORG   WORKSPACE     METRIC            VALUE
--------------------------------------------
Acme  production    base_traces       98000
Acme  production    extended_traces   44300
Acme  dev           base_traces        8204
```

Note: `--level` is ignored in overview mode — the billing endpoint only provides workspace-level data.

## Report levels (granular mode)

### Workspace (default)

Prints one row per workspace showing total traces for the period. Workspaces with zero traces are omitted.

```
ORG       WORKSPACE    TRACES
------------------------------
Acme      production   142300
Acme      dev            8204
```

### Project (`--level project`)

Prints one row per project within each workspace.

```
ORG   WORKSPACE     PROJECT          TRACES
--------------------------------------------
Acme  production    customer-chat    98000
Acme  production    internal-tools   44300
Acme  dev           experiments       8204
```

## Security note

Keep your `orgs.json` file private — it contains API keys. Add it to `.gitignore` if you store it alongside the script.
