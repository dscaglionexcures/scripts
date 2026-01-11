# export XCURES_BEARER_TOKEN="PASTE_TOKEN_HERE" to set the bearer token from your CLI so you don't add a token to the script and it accidentally gets saved to the repo

"""
Backup xCures Patient Registry user permissions (and projectIds) for all users in a tenant.

Flow:
1) GET  /api/patient-registry/user            -> list users (paged)
2) GET  /api/patient-registry/user/{id}       -> fetch full user record
3) Write backups locally (JSON + CSV)

Auth:
- Provide bearer token via --bearer or env var XCURES_BEARER_TOKEN
- Optionally provide ProjectId header via --project-id (some deployments expect it)

Outputs:
- JSON: permissions_backup_<timestamp>.json
- CSV:  permissions_backup_<timestamp>.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

try:
    from tqdm import tqdm  # type: ignore
except Exception:
    tqdm = None  # fallback


DEFAULT_BASE_URL = "https://partner.xcures.com"
DEFAULT_TIMEOUT_SECONDS = 60


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def load_dotenv(path: str = ".env") -> None:
    """
    Minimal .env loader (no external dependency).
    Loads KEY=VALUE pairs into environment if not already set.
    """
    p = Path(path)
    if not p.exists():
        return
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        os.environ.setdefault(k, v)


def build_headers(bearer: str, project_id: Optional[str]) -> Dict[str, str]:
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {bearer}",
        "Content-Type": "application/json",
    }
    if project_id:
        headers["ProjectId"] = project_id
    return headers


def safe_json_preview(text: str, limit: int = 800) -> str:
    text = text.strip()
    if len(text) <= limit:
        return text
    return text[:limit] + "…"


def request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    *,
    headers: Dict[str, str],
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    max_retries: int = 5,
    backoff_seconds: float = 1.0,
) -> requests.Response:
    """
    Retries on transient failures (429, 5xx) and surfaces useful error info if exhausted.
    """
    last_resp: Optional[requests.Response] = None
    last_exc: Optional[BaseException] = None

    for attempt in range(1, max_retries + 1):
        try:
            resp = session.request(
                method,
                url,
                headers=headers,
                params=params,
                json=json_body,
                timeout=timeout,
            )
            last_resp = resp

            # Success
            if 200 <= resp.status_code < 300:
                return resp

            # Retryable status codes
            if resp.status_code in (429, 500, 502, 503, 504):
                sleep_s = backoff_seconds * (2 ** (attempt - 1))
                time.sleep(sleep_s)
                continue

            # Non-retryable error: raise with body preview
            body_preview = safe_json_preview(resp.text or "")
            raise RuntimeError(f"HTTP {resp.status_code} for {url} body={body_preview}")

        except requests.RequestException as e:
            last_exc = e
            sleep_s = backoff_seconds * (2 ** (attempt - 1))
            time.sleep(sleep_s)

    # Exhausted retries
    if last_resp is not None:
        body_preview = safe_json_preview(last_resp.text or "")
        raise RuntimeError(
            f"request_with_retry: exhausted retries; last_status={last_resp.status_code} url={url} body={body_preview}"
        )
    raise RuntimeError(f"request_with_retry: exhausted retries; no response; last_exc={last_exc}")


def parse_json(resp: requests.Response) -> Any:
    try:
        return resp.json()
    except Exception:
        preview = safe_json_preview(resp.text or "")
        raise RuntimeError(f"Response was not valid JSON. status={resp.status_code} body={preview}")


def get_all_users(
    session: requests.Session,
    base_url: str,
    headers: Dict[str, str],
    *,
    page_size: int = 50,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    max_pages: int = 10_000,
) -> List[Dict[str, Any]]:
    """
    Fetches all users using pageNumber/pageSize pagination.

    Expects the API to return either:
      A) {"results":[...], "totalCount": N, ...}
      B) a raw list [...]
    """
    url = f"{base_url}/api/patient-registry/user"
    all_users: List[Dict[str, Any]] = []

    # Try paged first
    page_number = 1
    for _ in range(max_pages):
        params = {"pageNumber": page_number, "pageSize": page_size}
        resp = request_with_retry(session, "GET", url, headers=headers, params=params, timeout=timeout)
        data = parse_json(resp)

        if isinstance(data, list):
            # Unpaged list response
            return data

        if not isinstance(data, dict):
            raise RuntimeError(f"Unexpected response type from {url}: {type(data)}")

        results = data.get("results")
        if not isinstance(results, list):
            # Some APIs may use different property names; fallback to empty.
            results = []

        all_users.extend([r for r in results if isinstance(r, dict)])

        total_count = data.get("totalCount")
        if isinstance(total_count, int) and len(all_users) >= total_count:
            break

        if len(results) == 0:
            break

        page_number += 1

    return all_users


def get_user_detail(
    session: requests.Session,
    base_url: str,
    user_id: str,
    headers: Dict[str, str],
    *,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
) -> Dict[str, Any]:
    url = f"{base_url}/api/patient-registry/user/{user_id}"
    resp = request_with_retry(session, "GET", url, headers=headers, timeout=timeout)
    data = parse_json(resp)
    if not isinstance(data, dict):
        raise RuntimeError(f"Unexpected user detail type for user_id={user_id}: {type(data)}")
    return data


def simple_progress(items: List[Dict[str, Any]], *, desc: str = "Processing") -> Iterable[Dict[str, Any]]:
    total = len(items)
    bar_width = 30
    for idx, item in enumerate(items, start=1):
        progress = idx / total if total else 1
        filled = int(bar_width * progress)
        bar = "█" * filled + "-" * (bar_width - filled)
        percent = progress * 100
        print(f"\r{desc}: |{bar}| {percent:6.2f}% ({idx}/{total})", end="", flush=True)
        yield item
    print()


def extract_backup_row(user_detail: Dict[str, Any]) -> Dict[str, Any]:
    """
    Pull out the core backup fields without assuming too much about the shape.
    """
    return {
        "id": user_detail.get("id"),
        "email": user_detail.get("email"),
        "name": user_detail.get("name"),
        "permissions": user_detail.get("permissions") if isinstance(user_detail.get("permissions"), list) else [],
        "projectIds": user_detail.get("projectIds") if isinstance(user_detail.get("projectIds"), list) else [],
        # Store any additional fields you might want later, but keep it shallow by default.
    }


def write_json(path: Path, obj: Any) -> None:
    path.write_text(json.dumps(obj, indent=2, sort_keys=False, ensure_ascii=False), encoding="utf-8")


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    """
    Writes a CSV with permissions and projectIds as pipe-delimited strings.
    """
    fieldnames = ["id", "email", "name", "permissions", "projectIds"]
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            perms = r.get("permissions") or []
            projs = r.get("projectIds") or []
            w.writerow(
                {
                    "id": r.get("id") or "",
                    "email": r.get("email") or "",
                    "name": r.get("name") or "",
                    "permissions": "|".join([str(x) for x in perms]),
                    "projectIds": "|".join([str(x) for x in projs]),
                }
            )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Backup all user permissions/projectIds from xCures Patient Registry.")
    p.add_argument("--base-url", default=DEFAULT_BASE_URL, help="API base URL (default: %(default)s)")
    p.add_argument("--project-id", default=None, help="Optional ProjectId header value")
    p.add_argument("--bearer", default=None, help="Bearer token. Prefer env var XCURES_BEARER_TOKEN.")
    p.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS, help="HTTP timeout seconds (default: %(default)s)")
    p.add_argument("--page-size", type=int, default=50, help="Page size for list users (default: %(default)s)")
    p.add_argument("--out-dir", default=".", help="Output directory (default: current directory)")
    p.add_argument("--include-full", action="store_true", help="Also store full user detail records in JSON backup")
    p.add_argument("--dotenv", default=".env", help="Path to .env file to load (default: %(default)s)")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    # Load .env if present
    if args.dotenv:
        load_dotenv(args.dotenv)

    bearer = args.bearer or os.environ.get("XCURES_BEARER_TOKEN")
    if not bearer:
        print("Error: bearer token is required. Use --bearer or env var XCURES_BEARER_TOKEN.", file=sys.stderr)
        return 2

    headers = build_headers(bearer, args.project_id)
    base_url = args.base_url.rstrip("/")

    out_dir = Path(args.out_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    stamp = utc_timestamp()
    json_path = out_dir / f"permissions_backup_{stamp}.json"
    csv_path = out_dir / f"permissions_backup_{stamp}.csv"

    with requests.Session() as session:
        users = get_all_users(
            session,
            base_url,
            headers,
            page_size=args.page_size,
            timeout=args.timeout,
        )

        if not users:
            print("No users returned by the API.")
            return 0

        total = len(users)
        if tqdm is not None:
            iterator = tqdm(users, total=total, desc="Backing up users", unit="user")
        else:
            iterator = simple_progress(users, desc="Backing up users")

        backup_rows: List[Dict[str, Any]] = []
        full_records: List[Dict[str, Any]] = []
        failed: List[Tuple[str, str]] = []

        for user in iterator:
            user_id = None
            if isinstance(user, dict):
                user_id = user.get("id") or user.get("userId")
            if not user_id:
                failed.append(("", "Missing user id in list response item"))
                continue

            try:
                detail = get_user_detail(session, base_url, str(user_id), headers, timeout=args.timeout)
                backup_rows.append(extract_backup_row(detail))
                if args.include_full:
                    full_records.append(detail)
            except Exception as e:
                failed.append((str(user_id), str(e)))
                continue

    # Write backups
    payload: Dict[str, Any] = {
        "generatedAtUtc": stamp,
        "baseUrl": base_url,
        "projectIdHeaderUsed": args.project_id,
        "count": len(backup_rows),
        "rows": backup_rows,
    }
    if args.include_full:
        payload["fullUserRecords"] = full_records

    write_json(json_path, payload)
    write_csv(csv_path, backup_rows)

    print("\nBackup complete.")
    print(f"JSON: {json_path}")
    print(f"CSV:  {csv_path}")
    if failed:
        print(f"\nFailures: {len(failed)} (showing up to 25)")
        for uid, err in failed[:25]:
            print(f"- {uid}: {err}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
