#!/usr/bin/env python3
"""
Backup xCures Patient Registry user permissions (and projects) for all users in a tenant.

Includes:
- created (YYYY-MM-DD)
- lastLogin (YYYY-MM-DD)
- roleCode
- permissions
- project names (instead of raw projectIds in CSV)
- Progress bar indicator
"""

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from api_common import DEFAULT_BACKOFF_SECONDS, DEFAULT_TIMEOUT_SECONDS
from progress_common import progress_iter
from auth_common import get_xcures_bearer_token, load_env_file
from xcures_client import XcuresApiClient
from csv_common import write_csv_dict_rows

BASE_URL = "https://partner.xcures.com"
TIMEOUT = DEFAULT_TIMEOUT_SECONDS
BACKOFF = DEFAULT_BACKOFF_SECONDS

load_env_file(Path(__file__).resolve().parent / ".env")


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


# ------------------------------------------------
# API Calls
# ------------------------------------------------

def get_project_name_map(client: XcuresApiClient) -> Dict[str, str]:
    data = client.request_json("GET", "/api/patient-registry/project")

    if not isinstance(data, list):
        raise RuntimeError("Unexpected project list response")

    mapping: Dict[str, str] = {}
    for p in data:
        if isinstance(p, dict):
            pid = str(p.get("id") or "").strip()
            name = str(p.get("name") or "").strip()
            if pid:
                mapping[pid] = name

    return mapping


def get_all_users_and_last_login(
    client: XcuresApiClient,
    page_size: int = 50,
) -> Tuple[List[Dict[str, Any]], Dict[str, str]]:

    users: List[Dict[str, Any]] = []
    last_login_by_id: Dict[str, str] = {}

    for u in client.iter_paginated("/api/patient-registry/user", page_size=page_size):
        users.append(u)
        uid = str(u.get("id") or "").strip()
        if uid:
            last_login_by_id[uid] = normalize_date_only(u.get("lastLogin"))

    return users, last_login_by_id


def get_user_detail(client: XcuresApiClient, user_id: str) -> Dict[str, Any]:
    data = client.request_json("GET", f"/api/patient-registry/user/{user_id}")
    if not isinstance(data, dict):
        raise RuntimeError(f"Unexpected user detail response for id={user_id}: {type(data)}")
    return data


# ------------------------------------------------
# CSV Output
# ------------------------------------------------

def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fieldnames = [
        "id",
        "email",
        "firstName",
        "lastName",
        "roleCode",
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
                "id": r.get("id") or "",
                "email": r.get("email") or "",
                "firstName": r.get("firstName") or "",
                "lastName": r.get("lastName") or "",
                "roleCode": r.get("roleCode") or "",
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
    try:
        bearer = get_xcures_bearer_token(timeout_seconds=TIMEOUT)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    timestamp = utc_timestamp()
    json_file = Path(f"permissions_backup_{timestamp}.json")
    csv_file = Path(f"permissions_backup_{timestamp}.csv")

    with requests.Session() as session:
        client = XcuresApiClient(
            session=session,
            base_url=BASE_URL,
            bearer_token=bearer,
            timeout_seconds=TIMEOUT,
            backoff_seconds=BACKOFF,
        )
        project_name_map = get_project_name_map(client)
        users, last_login_map = get_all_users_and_last_login(client)

        total = len(users)
        rows: List[Dict[str, Any]] = []
        full_records: List[Dict[str, Any]] = []

        for u in progress_iter(users, desc="Backing up users", total=total, unit="user"):

            user_id = str(u.get("id") or "").strip()
            if not user_id:
                continue

            detail = get_user_detail(client, user_id)

            project_ids = detail.get("projectIds") if isinstance(detail.get("projectIds"), list) else []
            project_names = [
                project_name_map.get(str(pid), str(pid))
                for pid in project_ids
            ]

            row = {
                "id": detail.get("id"),
                "email": detail.get("email"),
                "firstName": detail.get("firstName"),
                "lastName": detail.get("lastName"),
                "roleCode": detail.get("roleCode"),
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
    print(f"JSON: {json_file}")
    print(f"CSV:  {csv_file}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
