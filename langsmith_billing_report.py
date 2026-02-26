#!/usr/bin/env python3
"""
LangSmith Billing Usage Report
Generates an Org -> Workspace -> Project trace count report.

Usage:
    python langsmith_billing_report.py \
        --base-url https://your-langsmith-instance.example.com \
        --api-key lsv2_sk_... \
        --start 2026-01-01 \
        --end 2026-02-01 \
        [--org-id uuid] \
        [--output report.csv]

Multi-org (runs orgs in parallel):
    python langsmith_billing_report.py \
        --base-url https://... \
        --orgs orgs.json \
        --start 2026-01-01 \
        --end 2026-02-01 \
        --output report.csv

orgs.json format (only api_key is required; org_id and org_name are optional):
    [
      {"api_key": "lsv2_sk_..."},
      {"api_key": "lsv2_sk_...", "org_id": "uuid", "org_name": "Org B"}
    ]

Modes:
    --mode granular  (default) Uses /billing/granular-usage. Scoped to the org
                     the API key belongs to. Supports --level workspace|project.
    --mode overview  Uses /billing/usage. Pulls from the billing provider
                     and shows a per-metric breakdown (e.g.
                     base_traces, extended_traces) per workspace.
"""

import argparse
import csv
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


_DEFAULT_TIMEOUT_SECONDS = 120


def make_request(
    base_url: str,
    api_key: str,
    org_id: str,
    path: str,
    params: dict,
    timeout: int = _DEFAULT_TIMEOUT_SECONDS,
) -> dict | list:
    qs = urlencode(params, doseq=True)
    url = f"{base_url}/api/v1{path}?{qs}" if qs else f"{base_url}/api/v1{path}"
    headers = {"accept": "application/json", "X-API-Key": api_key}
    if org_id:
        headers["X-Organization-Id"] = org_id
    req = Request(url, headers=headers)
    try:
        with urlopen(req, timeout=timeout) as resp:
            status = resp.status
            raw = resp.read()
        if not raw:
            raise ValueError(f"Empty response body from {url} (HTTP {status})")
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            preview = raw[:500].decode(errors="replace")
            raise ValueError(
                f"Non-JSON response from {url} (HTTP {status}):\n{preview}"
            ) from None
    except HTTPError as e:
        body = e.read().decode(errors="replace")
        print(f"HTTP {e.code} from {url}: {body}", file=sys.stderr)
        raise
    except URLError as e:
        print(f"Request failed for {url}: {e.reason}", file=sys.stderr)
        raise


def fetch_org_info(base_url: str, api_key: str, org_id: str = "") -> tuple[str, str]:
    """GET /orgs/current — returns (org_id, display_name).

    org_id may be omitted for self-hosted deployments where the API key is
    already org-scoped. For SaaS environments, org_id is typically required.
    """
    try:
        result = make_request(base_url, api_key, org_id, "/orgs/current", {})
    except HTTPError as e:
        if e.code == 403 and not org_id:
            print(
                "Hint: got 403 on /orgs/current with no org_id. "
                "Try passing --org-id <uuid> (find it under Settings → Organization).",
                file=sys.stderr,
            )
        raise
    resolved_id = str(result.get("id") or org_id)
    name = result.get("display_name") or resolved_id
    return resolved_id, name


def fetch_workspaces(base_url: str, api_key: str, org_id: str) -> list[dict]:
    """GET /workspaces — returns [{id, display_name, ...}]."""
    result = make_request(base_url, api_key, org_id, "/workspaces", {})
    return result


def fetch_granular_usage(
    base_url: str,
    api_key: str,
    org_id: str,
    workspace_ids: list[str],
    start: str,
    end: str,
    group_by: str = "project",
) -> list[dict]:
    """GET /orgs/current/billing/granular-usage.

    Returns time-bucketed records; the caller aggregates across buckets.
    workspace_ids must be non-empty.
    """
    params = {
        "start_time": f"{start}T00:00:00Z",
        "end_time": f"{end}T00:00:00Z",
        "group_by": group_by,
        "workspace_ids": workspace_ids,
    }
    result = make_request(
        base_url, api_key, org_id, "/orgs/current/billing/granular-usage", params
    )
    return result.get("usage", [])


def fetch_billing_usage(
    base_url: str,
    api_key: str,
    org_id: str,
    start: str,
    end: str,
) -> list[dict]:
    """GET /orgs/current/billing/usage.

    Returns a list of OrgUsage records from the billing provider.
    Each record has billable_metric_name, value, and groups (workspace_id -> float).
    """
    params = {
        "starting_on": f"{start}T00:00:00Z",
        "ending_before": f"{end}T00:00:00Z",
    }
    result = make_request(
        base_url, api_key, org_id, "/orgs/current/billing/usage", params
    )
    return result


# ---------------------------------------------------------------------------
# Row builders
# ---------------------------------------------------------------------------

def build_project_rows(
    granular_usage: list[dict],
    org_name: str,
    workspace_name: str = "",
) -> list[dict]:
    """Aggregate time-bucketed granular records into one row per (workspace, project).

    workspace_name should be supplied when group_by=project is used, because the
    API does not include workspace_id/workspace_name in the response dimensions for
    that grouping mode.
    """
    # Single pass: accumulate traces and record labels together
    aggregated: dict[tuple[str, str], list] = {}  # key -> [ws_name, proj_name, traces]

    for record in granular_usage:
        dims = record.get("dimensions", {})
        proj_id = str(dims.get("project_id") or "")
        key = (workspace_name, proj_id)
        entry = aggregated.get(key)
        if entry is None:
            proj_name = dims.get("project_name") or f"[unknown project: {proj_id}]"
            aggregated[key] = [workspace_name, proj_name, record.get("traces", 0)]
        else:
            entry[2] += record.get("traces", 0)

    return sorted(
        (
            {"org": org_name, "workspace": ws_name, "project": proj_name, "traces": traces}
            for (_, _), (ws_name, proj_name, traces) in aggregated.items()
        ),
        key=lambda r: (r["workspace"], r["project"]),
    )


def build_workspace_rows(granular_usage: list[dict], org_name: str) -> list[dict]:
    """Aggregate time-bucketed granular records into one row per workspace."""
    aggregated: dict[str, list] = {}  # ws_id -> [ws_name, traces]

    for record in granular_usage:
        dims = record.get("dimensions", {})
        ws_id = str(dims.get("workspace_id") or "")
        entry = aggregated.get(ws_id)
        if entry is None:
            ws_name = dims.get("workspace_name") or f"[unknown workspace: {ws_id}]"
            aggregated[ws_id] = [ws_name, record.get("traces", 0)]
        else:
            entry[1] += record.get("traces", 0)

    return sorted(
        (
            {"org": org_name, "workspace": ws_name, "traces": traces}
            for ws_name, traces in aggregated.values()
        ),
        key=lambda r: r["workspace"],
    )


def build_overview_rows(
    billing_usage: list[dict],
    org_name: str,
    workspace_map: dict[str, str],
) -> list[dict]:
    """Build one row per (workspace, metric) from /billing/usage response.

    workspace_map maps workspace_id -> display_name and is used to resolve
    the workspace IDs returned in the groups dict.
    """
    # Accumulate (ws_id, metric_name) -> value
    aggregated: dict[tuple[str, str], float] = {}

    for item in billing_usage:
        metric = item.get("billable_metric_name") or "unknown"
        groups = item.get("groups") or {}
        for ws_id, value in groups.items():
            key = (ws_id, metric)
            aggregated[key] = aggregated.get(key, 0.0) + (value or 0.0)

    rows = []
    for (ws_id, metric), value in aggregated.items():
        ws_name = workspace_map.get(ws_id) or f"[unknown workspace: {ws_id}]"
        rows.append({
            "org": org_name,
            "workspace": ws_name,
            "metric": metric,
            "value": int(value),
        })

    return sorted(rows, key=lambda r: (r["workspace"], r["metric"]))


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def print_table(rows: list[dict], columns: list[str]) -> None:
    widths = {col: len(col) for col in columns}
    for row in rows:
        for col in columns:
            widths[col] = max(widths[col], len(str(row.get(col, ""))))
    header = "  ".join(col.upper().ljust(widths[col]) for col in columns)
    print(header)
    print("-" * len(header))
    for row in rows:
        print("  ".join(str(row.get(col, "")).ljust(widths[col]) for col in columns))


def write_csv(rows: list[dict], columns: list[str], path: str, quiet: bool = False) -> None:
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    if not quiet:
        print(f"Saved to {path}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Per-org fetch
# ---------------------------------------------------------------------------

def fetch_org_rows(
    base_url: str,
    api_key: str,
    org_id: str,
    org_name: str,
    start: str,
    end: str,
    mode: str,
    level: str,
    quiet: bool = False,
) -> list[dict]:
    """Fetch and build rows for a single org. Safe to run in a thread."""
    def log(msg: str, **kwargs) -> None:
        if not quiet:
            print(msg, file=sys.stderr, **kwargs)

    if not org_name:
        _, org_name = fetch_org_info(base_url, api_key, org_id)
    log(f"[{org_name}] Fetching workspaces...")
    workspaces = fetch_workspaces(base_url, api_key, org_id)
    workspace_map = {str(ws["id"]): ws["display_name"] for ws in workspaces}
    workspace_ids = list(workspace_map.keys())
    log(
        f"[{org_name}] Found {len(workspace_ids)} workspace(s): "
        f"{', '.join(workspace_map.values())}"
    )

    if not workspace_ids:
        return []

    if mode == "overview":
        log(f"[{org_name}] Fetching billing usage (overview) {start} -> {end}...")
        billing = fetch_billing_usage(base_url, api_key, org_id, start, end)
        return build_overview_rows(billing, org_name, workspace_map)

    # mode == "granular"
    if level == "project":
        # The API does not include workspace_id in the project-level grouping response,
        # so we call the endpoint once per workspace and apply the known name.
        log(
            f"[{org_name}] Fetching granular usage (by project) for "
            f"{len(workspace_ids)} workspace(s) {start} -> {end}..."
        )
        all_rows = []
        total = len(workspace_map)
        for i, (ws_id, ws_name) in enumerate(workspace_map.items(), 1):
            log(f"[{org_name}]   [{i}/{total}] {ws_name}...", flush=True)
            granular = fetch_granular_usage(base_url, api_key, org_id, [ws_id], start, end)
            ws_rows = build_project_rows(granular, org_name, ws_name)
            log(f"[{org_name}]         -> {len(ws_rows)} project(s)", flush=True)
            all_rows.extend(ws_rows)
        return sorted(all_rows, key=lambda r: (r["workspace"], r["project"]))
    else:
        log(f"[{org_name}] Fetching workspace-level granular usage {start} -> {end}...")
        granular = fetch_granular_usage(base_url, api_key, org_id, workspace_ids, start, end, group_by="workspace")
        rows = build_workspace_rows(granular, org_name)
        return sorted(rows, key=lambda r: r["workspace"])


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="LangSmith billing usage report")
    parser.add_argument("--base-url", required=True,
                        help="LangSmith base URL, e.g. https://api.smith.langchain.com or your self-hosted URL")
    parser.add_argument("--start", required=True,
                        help="Start date YYYY-MM-DD (inclusive)")
    parser.add_argument("--end", required=True,
                        help="End date YYYY-MM-DD (exclusive, i.e. day after last day)")
    parser.add_argument("--mode", choices=["granular", "overview"], default="granular",
                        help="granular (default): uses /billing/granular-usage, scoped to org; "
                             "overview: uses /billing/usage from the billing provider, "
                             "shows per-metric breakdown per workspace")
    parser.add_argument("--level", choices=["workspace", "project"], default="workspace",
                        help="Report granularity for granular mode (default: workspace). "
                             "Ignored in overview mode.")
    parser.add_argument("--output", default="",
                        help="Optional CSV output file path")
    parser.add_argument("--silent", action="store_true",
                        help="Suppress all output including progress messages. Use with --output to save results silently.")

    # Single-org args
    single = parser.add_argument_group("single org")
    single.add_argument("--api-key", help="Service account API key (lsv2_sk_...)")
    single.add_argument("--org-id", default="", help="Organization UUID (optional if API key is org-scoped)")
    single.add_argument("--org-name", default="", help="Human-readable org name for the report")

    # Multi-org args
    multi = parser.add_argument_group("multi-org")
    multi.add_argument("--orgs", help="Path to JSON file with list of {api_key, org_id, org_name}")
    multi.add_argument("--workers", type=int, default=4,
                       help="Parallel workers for multi-org fetch (default: 4)")

    args = parser.parse_args()
    base_url = args.base_url.rstrip("/")

    if args.silent and not args.output:
        print("Warning: --silent is set but --output is not — results will not be saved anywhere.", file=sys.stderr)

    if args.mode == "overview" and args.level != "workspace":
        print("Warning: --level is ignored in overview mode (billing/usage only provides workspace-level data).", file=sys.stderr)

    # Build org list
    if args.orgs:
        with open(args.orgs) as f:
            orgs = json.load(f)
    elif args.api_key:
        orgs = [{"api_key": args.api_key, "org_id": args.org_id or "",
                 "org_name": args.org_name or ""}]
    else:
        parser.error("Provide either --api-key or --orgs")

    # Fetch all orgs (parallel for multi-org)
    all_rows: list[dict] = []
    if len(orgs) == 1:
        org = orgs[0]
        all_rows = fetch_org_rows(
            base_url, org["api_key"], org.get("org_id", ""),
            org.get("org_name", ""),
            args.start, args.end, args.mode, args.level,
            quiet=args.silent,
        )
    else:
        futures = {}
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            for org in orgs:
                future = pool.submit(
                    fetch_org_rows,
                    base_url, org["api_key"], org.get("org_id", ""),
                    org.get("org_name", ""),
                    args.start, args.end, args.mode, args.level,
                    args.silent,
                )
                futures[future] = org.get("org_name") or org.get("org_id", "")
            for future in as_completed(futures):
                org_name = futures[future]
                try:
                    all_rows.extend(future.result())
                except Exception as e:
                    print(f"[{org_name}] Failed: {e}", file=sys.stderr)

        # Sort combined results by org then workspace (then project/metric if present)
        all_rows.sort(key=lambda r: (r["org"], r.get("workspace", ""), r.get("project", ""), r.get("metric", "")))

    # Drop rows with no usage
    value_field = "value" if args.mode == "overview" else "traces"
    all_rows = [r for r in all_rows if r.get(value_field, 0)]

    if not all_rows:
        if not args.silent:
            print("No usage data found for the specified period.", file=sys.stderr)
        return

    if args.mode == "overview":
        columns = ["org", "workspace", "metric", "value"]
        dedup_key = lambda r: (r["org"], r["workspace"], r["metric"])
    elif args.level == "project":
        columns = ["org", "workspace", "project", "traces"]
        dedup_key = lambda r: (r["org"], r["workspace"], r["project"])
    else:
        columns = ["org", "workspace", "traces"]
        dedup_key = lambda r: (r["org"], r["workspace"])

    seen = set()
    deduped = []
    for row in all_rows:
        k = dedup_key(row)
        if k not in seen:
            seen.add(k)
            deduped.append(row)
    if len(deduped) < len(all_rows) and not args.silent:
        print(f"Removed {len(all_rows) - len(deduped)} duplicate row(s).", file=sys.stderr)
    all_rows = deduped

    if not args.silent and not args.output:
        print_table(all_rows, columns)

    if args.output:
        write_csv(all_rows, columns, args.output, quiet=args.silent)


if __name__ == "__main__":
    main()
