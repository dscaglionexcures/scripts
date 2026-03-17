#!/usr/bin/env python3
"""
Backup xCures Patient Registry user permissions (and projects) for all users in a tenant.

Includes:
- created (YYYY-MM-DD)
- lastLogin (YYYY-MM-DD)
- roleCode
- permissions
- project IDs in CSV
- Progress bar indicator
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from xcures_toolkit.api_common import DEFAULT_BACKOFF_SECONDS, DEFAULT_TIMEOUT_SECONDS
from xcures_toolkit.progress_common import progress_iter
from xcures_toolkit.auth_common import get_xcures_bearer_token, load_env_file
from xcures_toolkit.xcures_client import XcuresApiClient
from xcures_toolkit.csv_common import write_csv_dict_rows

load_env_file(Path(__file__).resolve().parent.parent / ".env")
BASE_URL = "https://partner.xcures.com"


# ------------------------------------------------
# Utilities
# ------------------------------------------------

def utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def normalize_date_only(value: Optional[str]) -> str:
    """
    Convert ISO UTC timestamp to YYYY-MM-DD.
    If null or invalid, return empty string.
    """
    if not value:
        return ""

    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt.date().isoformat()
    except Exception:
        if "T" in value:
            return value.split("T")[0]
        return ""


def get_user_id(record: Dict[str, Any]) -> str:
    return str(record.get("id") or record.get("userId") or record.get("_id") or "").strip()


# ------------------------------------------------
# API Calls
# ------------------------------------------------

def get_project_name_map(client: XcuresApiClient, project_id: Optional[str]) -> Dict[str, str]:
    """
    Build map of project UUID -> human-readable name.
    Falls back to an empty map if the endpoint is unavailable to this token.
    """
    params: Dict[str, Any] = {}
    if project_id:
        params["projectId"] = project_id

    data = client.request_json("GET", "/api/patient-registry/project", params=params or None)
    if not isinstance(data, list):
        raise RuntimeError("Unexpected project list response")

    mapping: Dict[str, str] = {}
    for p in data:
        if isinstance(p, dict):
            pid = str(p.get("id") or "").strip()
            name = str(p.get("name") or "").strip()
            if pid:
                mapping[pid] = name or pid
    return mapping


def get_role_name_map(client: XcuresApiClient, project_id: Optional[str]) -> Dict[str, str]:
    """
    Build map of role code -> human-readable role name.
    Falls back to an empty map if the endpoint is unavailable.
    """
    params: Dict[str, Any] = {}
    if project_id:
        params["projectId"] = project_id

    data = client.request_json("GET", "/api/patient-registry/roles", params=params or None)

    entries: List[Dict[str, Any]] = []
    if isinstance(data, list):
        entries = [item for item in data if isinstance(item, dict)]
    elif isinstance(data, dict):
        for key in ("items", "roles", "data"):
            maybe = data.get(key)
            if isinstance(maybe, list):
                entries = [item for item in maybe if isinstance(item, dict)]
                break

    mapping: Dict[str, str] = {}
    for role in entries:
        code = str(role.get("code") or role.get("roleCode") or role.get("id") or "").strip()
        name = str(
            role.get("name")
            or role.get("displayName")
            or role.get("label")
            or role.get("title")
            or ""
        ).strip()
        if code:
            mapping[code] = name or code
    return mapping


def get_all_users_and_last_login(
    client: XcuresApiClient,
    project_id: Optional[str],
    page_size: int = 50,
) -> Tuple[List[Dict[str, Any]], Dict[str, str]]:

    users: List[Dict[str, Any]] = []
    last_login_by_id: Dict[str, str] = {}

    params: Dict[str, Any] = {}
    if project_id:
        params["projectId"] = project_id

    for u in client.iter_paginated(
        "/api/patient-registry/user",
        params=params or None,
        page_size=page_size,
    ):
        users.append(u)
        uid = get_user_id(u)
        if uid:
            last_login_by_id[uid] = normalize_date_only(u.get("lastLogin"))

    return users, last_login_by_id


def get_user_detail(client: XcuresApiClient, user_id: str, project_id: Optional[str]) -> Dict[str, Any]:
    params: Dict[str, Any] = {}
    if project_id:
        params["projectId"] = project_id
    data = client.request_json("GET", f"/api/patient-registry/user/{user_id}", params=params or None)
    if not isinstance(data, dict):
        raise RuntimeError(f"Unexpected user detail response for id={user_id}: {type(data)}")
    return data


# ------------------------------------------------
# CSV Output
# ------------------------------------------------

def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fieldnames = [
        "email",
        "firstName",
        "lastName",
        "role",
        "created",
        "lastLogin",
        "permissions",
        "projects",
    ]
    write_csv_dict_rows(
        path,
        fieldnames=fieldnames,
        rows=[
            {
                "email": r.get("email") or "",
                "firstName": r.get("firstName") or "",
                "lastName": r.get("lastName") or "",
                "role": r.get("role") or "",
                "created": r.get("created") or "",
                "lastLogin": r.get("lastLogin") or "",
                "permissions": "|".join(r.get("permissions") or []),
                "projects": "|".join(r.get("projectNames") or []),
            }
            for r in rows
        ],
    )


# ------------------------------------------------
# Main
# ------------------------------------------------

def main() -> int:
    load_env_file(Path(__file__).resolve().parent.parent / ".env")

    base_url = os.environ.get("BASE_URL", BASE_URL).strip().rstrip("/") or BASE_URL
    project_id = os.environ.get("XCURES_PROJECT_ID", "").strip() or None

    timeout_raw = os.environ.get("request_timeout_seconds", "").strip()
    retries_raw = os.environ.get("max_retries", "").strip()
    backoff_raw = os.environ.get("backoff_seconds", "").strip()
    page_size_raw = os.environ.get("user_page_size", "").strip()

    try:
        timeout_seconds = int(timeout_raw) if timeout_raw else DEFAULT_TIMEOUT_SECONDS
    except Exception:
        timeout_seconds = DEFAULT_TIMEOUT_SECONDS
    try:
        max_retries = int(retries_raw) if retries_raw else 2
    except Exception:
        max_retries = 2
    try:
        backoff_seconds = float(backoff_raw) if backoff_raw else DEFAULT_BACKOFF_SECONDS
    except Exception:
        backoff_seconds = DEFAULT_BACKOFF_SECONDS
    try:
        page_size = int(page_size_raw) if page_size_raw else 50
    except Exception:
        page_size = 50

    try:
        # Prefer explicitly supplied runtime bearer token (from UI run input),
        # then fall back to client-credentials token generation.
        bearer = os.environ.get("XCURES_BEARER_TOKEN", "").strip() or get_xcures_bearer_token(
            timeout_seconds=timeout_seconds
        )
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    timestamp = utc_timestamp()
    backup_dir = Path.cwd() / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    json_file = backup_dir / f"permissions_backup_{timestamp}.json"
    csv_file = backup_dir / f"permissions_backup_{timestamp}.csv"

    with requests.Session() as session:
        client = XcuresApiClient(
            session=session,
            base_url=base_url,
            project_id=project_id,
            bearer_token=bearer,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            backoff_seconds=backoff_seconds,
        )
        print(
            f"Using projectId={project_id or '(none)'} "
            f"pageSize={page_size} timeout={timeout_seconds}s retries={max_retries} backoff={backoff_seconds}s"
        )
        project_name_map: Dict[str, str] = {}
        try:
            project_name_map = get_project_name_map(client, project_id=project_id)
            print(f"Resolved {len(project_name_map)} project names.")
        except Exception as exc:
            print(
                f"Warning: unable to load project name map; project UUIDs will be used ({exc})",
                file=sys.stderr,
            )
        role_name_map: Dict[str, str] = {}
        try:
            role_name_map = get_role_name_map(client, project_id=project_id)
            print(f"Resolved {len(role_name_map)} roles.")
        except Exception as exc:
            print(
                f"Warning: unable to load role name map; role codes will be used ({exc})",
                file=sys.stderr,
            )
        users, last_login_map = get_all_users_and_last_login(client, project_id=project_id, page_size=page_size)

        total = len(users)
        print(f"Fetched {total} users from /api/patient-registry/user")
        rows: List[Dict[str, Any]] = []
        full_records: List[Dict[str, Any]] = []

        for u in progress_iter(users, desc="Backing up users", total=total, unit="user"):

            user_id = get_user_id(u)
            if not user_id:
                continue

            try:
                detail = get_user_detail(client, user_id, project_id=project_id)
            except Exception as exc:
                print(f"Warning: detail lookup failed for user {user_id}: {exc}", file=sys.stderr)
                detail = dict(u)

            project_ids = detail.get("projectIds") if isinstance(detail.get("projectIds"), list) else []
            project_names = [project_name_map.get(str(pid), str(pid)) for pid in project_ids]
            role_code = str(detail.get("roleCode") or "").strip()
            role_name = role_name_map.get(role_code, role_code)

            row = {
                "id": get_user_id(detail),
                "email": detail.get("email"),
                "firstName": detail.get("firstName"),
                "lastName": detail.get("lastName"),
                "role": role_name,
                "roleCode": role_code,
                "created": normalize_date_only(detail.get("created")),
                "lastLogin": last_login_map.get(user_id, ""),
                "permissions": detail.get("permissions") if isinstance(detail.get("permissions"), list) else [],
                "projectIds": project_ids,
                "projectNames": project_names,
            }

            rows.append(row)
            full_records.append(detail)

    json_file.write_text(
        json.dumps(
            {
                "generatedAtUtc": timestamp,
                "count": len(rows),
                "rows": rows,
                "fullUserRecords": full_records,
                "notes": {
                    "dates": "created and lastLogin formatted as YYYY-MM-DD",
                    "projectsCsvColumn": "CSV stores project names",
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    write_csv(csv_file, rows)

    print("Backup complete.")
    print(f"Rows: {len(rows)}")
    print(f"JSON: {json_file}")
    print(f"CSV:  {csv_file}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
