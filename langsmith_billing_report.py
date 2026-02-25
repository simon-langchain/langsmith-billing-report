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


def write_csv(rows: list[dict], columns: list[str], path: str) -> None:
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
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
    level: str,
) -> list[dict]:
    """Fetch and build rows for a single org. Safe to run in a thread."""
    if not org_name:
        _, org_name = fetch_org_info(base_url, api_key, org_id)
    print(f"[{org_name}] Fetching workspaces...", file=sys.stderr)
    workspaces = fetch_workspaces(base_url, api_key, org_id)
    workspace_map = {str(ws["id"]): ws["display_name"] for ws in workspaces}
    workspace_ids = list(workspace_map.keys())
    print(
        f"[{org_name}] Found {len(workspace_ids)} workspace(s): "
        f"{', '.join(workspace_map.values())}",
        file=sys.stderr,
    )

    if not workspace_ids:
        return []

    if level == "project":
        # The API does not include workspace_id in the project-level grouping response,
        # so we call the endpoint once per workspace and apply the known name.
        print(
            f"[{org_name}] Fetching granular usage (by project) for "
            f"{len(workspace_ids)} workspace(s) {start} -> {end}...",
            file=sys.stderr,
        )
        all_rows = []
        total = len(workspace_map)
        for i, (ws_id, ws_name) in enumerate(workspace_map.items(), 1):
            print(f"[{org_name}]   [{i}/{total}] {ws_name}...", file=sys.stderr, flush=True)
            granular = fetch_granular_usage(base_url, api_key, org_id, [ws_id], start, end)
            ws_rows = build_project_rows(granular, org_name, ws_name)
            print(f"[{org_name}]         -> {len(ws_rows)} project(s)", file=sys.stderr, flush=True)
            all_rows.extend(ws_rows)
        return sorted(all_rows, key=lambda r: (r["workspace"], r["project"]))
    else:
        print(f"[{org_name}] Fetching workspace-level granular usage {start} -> {end}...", file=sys.stderr)
        granular = fetch_granular_usage(base_url, api_key, org_id, workspace_ids, start, end, group_by="workspace")
        rows = build_workspace_rows(granular, org_name)
        # Fill in workspaces that had no usage in this period
        reported = {r["workspace"] for r in rows}
        for ws_name in workspace_map.values():
            if ws_name not in reported:
                rows.append({"org": org_name, "workspace": ws_name, "traces": 0})
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
    parser.add_argument("--level", choices=["workspace", "project"], default="workspace",
                        help="Report granularity (default: workspace)")
    parser.add_argument("--output", default="",
                        help="Optional CSV output file path")

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
            args.start, args.end, args.level,
        )
    else:
        futures = {}
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            for org in orgs:
                future = pool.submit(
                    fetch_org_rows,
                    base_url, org["api_key"], org.get("org_id", ""),
                    org.get("org_name", ""),
                    args.start, args.end, args.level,
                )
                futures[future] = org.get("org_name") or org.get("org_id", "")
            for future in as_completed(futures):
                org_name = futures[future]
                try:
                    all_rows.extend(future.result())
                except Exception as e:
                    print(f"[{org_name}] Failed: {e}", file=sys.stderr)

        # Sort combined results by org then workspace (then project if present)
        all_rows.sort(key=lambda r: (r["org"], r.get("workspace", ""), r.get("project", "")))

    if not all_rows:
        print("No usage data found for the specified period.", file=sys.stderr)
        return

    columns = (
        ["org", "workspace", "project", "traces"]
        if args.level == "project"
        else ["org", "workspace", "traces"]
    )

    print_table(all_rows, columns)

    if args.output:
        write_csv(all_rows, columns, args.output)


if __name__ == "__main__":
    main()
