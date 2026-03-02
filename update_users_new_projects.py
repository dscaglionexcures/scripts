#!/usr/bin/env python3
# export XCURES_BEARER_TOKEN="PASTE_TOKEN_HERE"

"""
Hardened bulk project assignment tool for xCures users.

Safety model:
- Default mode is --dry-run (no writes).
- Use explicit --apply to perform PUT updates.
- Pre-write backup snapshot is required in apply mode.
- Audit log (JSONL) captures plan and execution events.
- Config-driven target project list via JSON file.

Default config path:
  configs/update_users_new_projects.json
"""

from __future__ import annotations

import argparse
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
    tqdm = None


DEFAULT_BASE_URL = "https://partner.xcures.com"
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "configs" / "update_users_new_projects.json"
DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_MAX_RETRIES = 5
DEFAULT_BACKOFF_SECONDS = 1.0
DEFAULT_PAGE_SIZE = 25


def utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def utc_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def parse_env_file(path: Path) -> Dict[str, str]:
    loaded: Dict[str, str] = {}
    if not path.exists():
        raise RuntimeError(f"Env file not found: {path}")

    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if not key:
            continue
        loaded[key] = value
        os.environ.setdefault(key, value)

    return loaded


class AuditLog:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def write(self, event: str, **fields: Any) -> None:
        row = {
            "ts": utc_iso(),
            "event": event,
            **fields,
        }
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=True) + "\n")


def progress_iter(items: Iterable[Any], *, desc: str, total: int):
    if tqdm is not None:
        return tqdm(items, total=total, desc=desc, unit="user")

    width = 30

    def _gen():
        for idx, item in enumerate(items, start=1):
            pct = idx / total if total else 1
            filled = int(width * pct)
            bar = "#" * filled + "-" * (width - filled)
            print(f"\r{desc}: |{bar}| {pct*100:6.2f}% ({idx}/{total})", end="", flush=True)
            yield item
        print()

    return _gen()


def body_preview(text: str, limit: int = 1000) -> str:
    flat = (text or "").strip().replace("\n", " ")
    if len(flat) > limit:
        return flat[:limit] + "...<truncated>"
    return flat


def get_bearer_token() -> str:
    token = os.environ.get("XCURES_BEARER_TOKEN", "").strip()
    if not token:
        raise RuntimeError(
            "XCURES_BEARER_TOKEN is not set. "
            "Run: export XCURES_BEARER_TOKEN='your_token_here'"
        )
    return token


def dedupe_strings(values: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        if not isinstance(value, str):
            continue
        cleaned = value.strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        out.append(cleaned)
    return out


def build_headers(token: str, project_id_header: Optional[str]) -> Dict[str, str]:
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    if project_id_header:
        headers["ProjectId"] = project_id_header
    return headers


def request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    *,
    headers: Dict[str, str],
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    timeout_seconds: int,
    max_retries: int,
    backoff_seconds: float,
) -> requests.Response:
    last_response: Optional[requests.Response] = None
    last_exception: Optional[BaseException] = None

    for attempt in range(1, max_retries + 1):
        try:
            response = session.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=json_body,
                timeout=timeout_seconds,
            )
            last_response = response

            if 200 <= response.status_code < 300:
                return response

            if response.status_code in (429, 500, 502, 503, 504):
                time.sleep(backoff_seconds * (2 ** (attempt - 1)))
                continue

            raise RuntimeError(
                f"HTTP {response.status_code} {url} body={body_preview(response.text)}"
            )

        except (requests.Timeout, requests.ConnectionError, requests.RequestException) as exc:
            last_exception = exc
            time.sleep(backoff_seconds * (2 ** (attempt - 1)))

    if last_response is not None:
        raise RuntimeError(
            f"request_with_retry exhausted; last_status={last_response.status_code} "
            f"url={url} body={body_preview(last_response.text)}"
        )

    raise RuntimeError(
        "request_with_retry exhausted with no HTTP response; "
        f"url={url} last_exception={type(last_exception).__name__ if last_exception else 'unknown'}"
    )


def load_config(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise RuntimeError(f"Config not found: {path}")

    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise RuntimeError(f"Config must be a JSON object: {path}")
    return raw


def require_int(name: str, value: Any, default: int) -> int:
    if value is None:
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    raise RuntimeError(f"Invalid integer for {name}: {value}")


def require_float(name: str, value: Any, default: float) -> float:
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except Exception:
            pass
    raise RuntimeError(f"Invalid float for {name}: {value}")


def get_all_users(
    session: requests.Session,
    base_url: str,
    headers: Dict[str, str],
    *,
    page_size: int,
    timeout_seconds: int,
    max_retries: int,
    backoff_seconds: float,
) -> List[Dict[str, Any]]:
    endpoint = f"{base_url}/api/patient-registry/user"
    users: List[Dict[str, Any]] = []
    page_number = 1

    while True:
        response = request_with_retry(
            session,
            "GET",
            endpoint,
            headers=headers,
            params={
                "pageSize": page_size,
                "pageNumber": page_number,
                "hasActiveFilter": "false",
                "numberOfActiveFilters": "0",
            },
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            backoff_seconds=backoff_seconds,
        )
        payload = response.json()

        if isinstance(payload, dict):
            page_results = payload.get("results") or []
            total_count = payload.get("totalCount")
        elif isinstance(payload, list):
            page_results = payload
            total_count = None
        else:
            raise RuntimeError(f"Unexpected user list response type: {type(payload)}")

        if not isinstance(page_results, list):
            raise RuntimeError("Unexpected user list response shape: results is not a list")

        page_users = [u for u in page_results if isinstance(u, dict)]
        users.extend(page_users)

        if not page_users:
            break

        if isinstance(total_count, int) and len(users) >= total_count:
            break

        page_number += 1

    return users


def get_user_detail(
    session: requests.Session,
    base_url: str,
    headers: Dict[str, str],
    user_id: str,
    *,
    timeout_seconds: int,
    max_retries: int,
    backoff_seconds: float,
) -> Dict[str, Any]:
    endpoint = f"{base_url}/api/patient-registry/user/{user_id}"
    response = request_with_retry(
        session,
        "GET",
        endpoint,
        headers=headers,
        params={"userId": user_id},
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        backoff_seconds=backoff_seconds,
    )
    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError(f"Unexpected user detail response type for {user_id}: {type(payload)}")
    return payload


def put_user(
    session: requests.Session,
    base_url: str,
    headers: Dict[str, str],
    user_id: str,
    payload: Dict[str, Any],
    *,
    timeout_seconds: int,
    max_retries: int,
    backoff_seconds: float,
) -> None:
    endpoint = f"{base_url}/api/patient-registry/user/{user_id}"
    request_with_retry(
        session,
        "PUT",
        endpoint,
        headers=headers,
        json_body=payload,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        backoff_seconds=backoff_seconds,
    )


@dataclass
class PlannedUpdate:
    user_id: str
    email: str
    project_ids_before: List[str]
    project_ids_after: List[str]
    user_record: Dict[str, Any]


def build_updated_project_ids(existing: List[str], target: List[str]) -> Tuple[List[str], List[str]]:
    existing_clean = dedupe_strings(existing)
    current = set(existing_clean)
    missing = [pid for pid in target if pid not in current]
    return existing_clean + missing, missing


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Safely bulk-add project IDs to all users in a tenant.",
    )
    parser.add_argument("--env", default=None, help="Path to .env file to preload")
    parser.add_argument(
        "--config",
        default=str(DEFAULT_CONFIG_PATH),
        help=f"Path to config JSON (default: {DEFAULT_CONFIG_PATH})",
    )
    parser.add_argument("--base-url", default=None, help="Override base URL from config")
    parser.add_argument(
        "--project-id-header",
        default=None,
        help="Optional ProjectId header override",
    )
    parser.add_argument(
        "--project-id",
        action="append",
        default=None,
        help="Target project ID override (repeatable); overrides config list when provided.",
    )

    mode = parser.add_mutually_exclusive_group(required=False)
    mode.add_argument("--dry-run", action="store_true", help="Preview only (default)")
    mode.add_argument("--apply", action="store_true", help="Execute PUT updates")

    parser.add_argument("--limit", type=int, default=None, help="Only process first N users")
    parser.add_argument("--page-size", type=int, default=None, help="User list page size")
    parser.add_argument("--timeout", type=int, default=None, help="Request timeout seconds")
    parser.add_argument("--max-retries", type=int, default=None, help="Retry attempts for transient failures")
    parser.add_argument("--backoff", type=float, default=None, help="Backoff base seconds")
    parser.add_argument("--audit-log", default=None, help="Audit JSONL path")
    parser.add_argument("--backup-path", default=None, help="Backup JSON path (apply mode)")
    parser.add_argument("--verbose", action="store_true", help="Print each planned/apply action")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    mode_name = "apply" if args.apply else "dry-run"

    if args.env:
        env_path = Path(args.env).expanduser().resolve()
        loaded = parse_env_file(env_path)
    else:
        env_path = None
        loaded = {}

    config_path = Path(args.config).expanduser().resolve()
    config = load_config(config_path)

    base_url = str(args.base_url or config.get("base_url") or DEFAULT_BASE_URL).rstrip("/")

    project_id_header = args.project_id_header
    if project_id_header is None:
        cfg_header = config.get("project_id_header")
        project_id_header = str(cfg_header).strip() if isinstance(cfg_header, str) and cfg_header.strip() else None

    if args.project_id:
        target_project_ids = dedupe_strings(args.project_id)
    else:
        target_project_ids = dedupe_strings(config.get("target_project_ids") or [])

    if not target_project_ids:
        print("Error: no target project IDs provided via --project-id or config.target_project_ids", file=sys.stderr)
        return 2

    page_size = args.page_size if args.page_size is not None else require_int(
        "user_page_size", config.get("user_page_size"), DEFAULT_PAGE_SIZE
    )
    timeout_seconds = args.timeout if args.timeout is not None else require_int(
        "request_timeout_seconds", config.get("request_timeout_seconds"), DEFAULT_TIMEOUT_SECONDS
    )
    max_retries = args.max_retries if args.max_retries is not None else require_int(
        "max_retries", config.get("max_retries"), DEFAULT_MAX_RETRIES
    )
    backoff_seconds = args.backoff if args.backoff is not None else require_float(
        "backoff_seconds", config.get("backoff_seconds"), DEFAULT_BACKOFF_SECONDS
    )

    if args.limit is not None and args.limit < 0:
        print("Error: --limit must be >= 0", file=sys.stderr)
        return 2

    token = get_bearer_token()
    headers = build_headers(token, project_id_header)

    if args.audit_log:
        audit_path = Path(args.audit_log).expanduser().resolve()
    else:
        audit_path = Path.cwd() / "logs" / f"update_users_new_projects_{utc_compact()}.jsonl"
    audit = AuditLog(audit_path)

    audit.write(
        "run_start",
        mode=mode_name,
        base_url=base_url,
        config_path=str(config_path),
        env_path=str(env_path) if env_path else None,
        env_keys=sorted(loaded.keys()),
        page_size=page_size,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        backoff_seconds=backoff_seconds,
        target_project_count=len(target_project_ids),
        project_id_header=project_id_header,
    )

    print(f"Mode: {mode_name}")
    print(f"Config: {config_path}")
    print(f"Audit log: {audit_path}")
    print(f"Target project IDs: {len(target_project_ids)}")

    planned_updates: List[PlannedUpdate] = []
    failed_planning: List[Tuple[str, str]] = []

    with requests.Session() as session:
        users = get_all_users(
            session,
            base_url,
            headers,
            page_size=page_size,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            backoff_seconds=backoff_seconds,
        )

        if args.limit is not None:
            users = users[: args.limit]

        if not users:
            print("No users found.")
            audit.write("run_end", ok=True, users=0, planned_updates=0, updated=0, failed=0)
            return 0

        print(f"Users discovered: {len(users)}")

        for u in progress_iter(users, desc="Planning updates", total=len(users)):
            user_id = str(u.get("id") or "").strip()
            if not user_id:
                failed_planning.append(("", "missing user id in list payload"))
                continue

            try:
                detail = get_user_detail(
                    session,
                    base_url,
                    headers,
                    user_id,
                    timeout_seconds=timeout_seconds,
                    max_retries=max_retries,
                    backoff_seconds=backoff_seconds,
                )

                existing_project_ids = detail.get("projectIds") if isinstance(detail.get("projectIds"), list) else []
                existing_project_ids = [str(x) for x in existing_project_ids]

                merged_project_ids, missing = build_updated_project_ids(existing_project_ids, target_project_ids)
                if not missing:
                    continue

                planned = PlannedUpdate(
                    user_id=user_id,
                    email=str(detail.get("email") or "").strip(),
                    project_ids_before=existing_project_ids,
                    project_ids_after=merged_project_ids,
                    user_record=detail,
                )
                planned_updates.append(planned)

                audit.write(
                    "planned_update",
                    user_id=user_id,
                    email=planned.email,
                    missing_project_count=len(missing),
                    missing_project_ids=missing,
                )

                if args.verbose:
                    print(f"[PLAN] {user_id} add {len(missing)} project(s)")

            except Exception as exc:
                failed_planning.append((user_id, str(exc)))
                audit.write("planning_error", user_id=user_id, error=str(exc))

        print(f"Planned updates: {len(planned_updates)}")
        print(f"Planning failures: {len(failed_planning)}")

        if mode_name == "dry-run":
            if planned_updates:
                print("\nDry-run preview (up to 20 users):")
                for planned in planned_updates[:20]:
                    print(
                        f"- {planned.user_id} ({planned.email or 'no-email'}): "
                        f"{len(planned.project_ids_before)} -> {len(planned.project_ids_after)} projects"
                    )

            if failed_planning:
                print("\nPlanning errors (up to 20):")
                for user_id, err in failed_planning[:20]:
                    print(f"- {user_id or '(unknown)'}: {err}")

            audit.write(
                "run_end",
                ok=True,
                mode="dry-run",
                users_discovered=len(users),
                planned_updates=len(planned_updates),
                planning_failures=len(failed_planning),
                updated=0,
                apply_failures=0,
            )
            return 0

        if not planned_updates:
            print("No updates required.")
            audit.write(
                "run_end",
                ok=True,
                mode="apply",
                users_discovered=len(users),
                planned_updates=0,
                planning_failures=len(failed_planning),
                updated=0,
                apply_failures=0,
            )
            return 0

        if args.backup_path:
            backup_path = Path(args.backup_path).expanduser().resolve()
        else:
            backup_path = Path.cwd() / "backups" / "update_users_new_projects" / f"prewrite_snapshot_{utc_compact()}.json"
        backup_path.parent.mkdir(parents=True, exist_ok=True)

        backup_payload = {
            "generatedAtUtc": utc_iso(),
            "baseUrl": base_url,
            "targetProjectIds": target_project_ids,
            "plannedUpdateCount": len(planned_updates),
            "plannedUpdates": [
                {
                    "userId": p.user_id,
                    "email": p.email,
                    "projectIdsBefore": p.project_ids_before,
                    "projectIdsAfter": p.project_ids_after,
                    "userRecordBefore": p.user_record,
                }
                for p in planned_updates
            ],
        }
        backup_path.write_text(json.dumps(backup_payload, indent=2), encoding="utf-8")
        print(f"Backup written: {backup_path}")
        audit.write("backup_written", backup_path=str(backup_path), planned_updates=len(planned_updates))

        updated = 0
        apply_failures: List[Tuple[str, str]] = []

        for planned in progress_iter(planned_updates, desc="Applying updates", total=len(planned_updates)):
            try:
                payload = dict(planned.user_record)
                payload["projectIds"] = planned.project_ids_after
                payload = {k: v for k, v in payload.items() if v is not None}

                put_user(
                    session,
                    base_url,
                    headers,
                    planned.user_id,
                    payload,
                    timeout_seconds=timeout_seconds,
                    max_retries=max_retries,
                    backoff_seconds=backoff_seconds,
                )

                updated += 1
                audit.write(
                    "update_applied",
                    user_id=planned.user_id,
                    email=planned.email,
                    project_count_before=len(planned.project_ids_before),
                    project_count_after=len(planned.project_ids_after),
                )

                if args.verbose:
                    print(f"[APPLY] updated {planned.user_id}")

            except Exception as exc:
                apply_failures.append((planned.user_id, str(exc)))
                audit.write("apply_error", user_id=planned.user_id, email=planned.email, error=str(exc))

        print("\nApply complete.")
        print(f"Updated: {updated}")
        print(f"Apply failures: {len(apply_failures)}")
        if failed_planning:
            print(f"Planning failures: {len(failed_planning)}")

        if apply_failures:
            print("\nApply errors (up to 20):")
            for user_id, err in apply_failures[:20]:
                print(f"- {user_id}: {err}")

        audit.write(
            "run_end",
            ok=len(apply_failures) == 0,
            mode="apply",
            users_discovered=len(users),
            planned_updates=len(planned_updates),
            planning_failures=len(failed_planning),
            updated=updated,
            apply_failures=len(apply_failures),
        )

        return 1 if apply_failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
