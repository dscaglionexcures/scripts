#!/usr/bin/env python3
# export XCURES_BEARER_TOKEN="PASTE_TOKEN_HERE"

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

import csv
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

BASE_URL = "https://partner.xcures.com"
TIMEOUT = 60
MAX_RETRIES = 5
BACKOFF = 1.0


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


def progress_bar(current: int, total: int) -> None:
    width = 30
    total = max(total, 1)
    progress = current / total
    filled = int(width * progress)
    bar = "█" * filled + "-" * (width - filled)
    percent = progress * 100
    print(f"\rProgress: |{bar}| {percent:6.2f}% ({current}/{total})", end="", flush=True)


def headers(bearer: str) -> Dict[str, str]:
    return {
        "accept": "application/json",
        "Authorization": f"Bearer {bearer}",
        "Content-Type": "application/json",
    }


def request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    *,
    bearer: str,
    params: Optional[Dict[str, Any]] = None,
) -> requests.Response:

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.request(
                method,
                url,
                headers=headers(bearer),
                params=params,
                timeout=TIMEOUT,
            )

            if 200 <= resp.status_code < 300:
                return resp

            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(BACKOFF * (2 ** (attempt - 1)))
                continue

            raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:800]}")

        except requests.RequestException:
            time.sleep(BACKOFF * (2 ** (attempt - 1)))

    raise RuntimeError(f"Request failed after retries: {url}")


# ------------------------------------------------
# API Calls
# ------------------------------------------------

def get_project_name_map(session: requests.Session, bearer: str) -> Dict[str, str]:
    url = f"{BASE_URL}/api/patient-registry/project"
    resp = request_with_retry(session, "GET", url, bearer=bearer)
    data = resp.json()

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
    session: requests.Session,
    bearer: str,
    page_size: int = 50,
) -> Tuple[List[Dict[str, Any]], Dict[str, str]]:

    url = f"{BASE_URL}/api/patient-registry/user"
    page_number = 1

    users: List[Dict[str, Any]] = []
    last_login_by_id: Dict[str, str] = {}

    while True:
        resp = request_with_retry(
            session,
            "GET",
            url,
            bearer=bearer,
            params={"pageNumber": page_number, "pageSize": page_size},
        )

        data = resp.json()
        results = data.get("results", []) if isinstance(data, dict) else data

        if not isinstance(results, list):
            break

        users.extend(results)

        for u in results:
            uid = str(u.get("id") or "").strip()
            if uid:
                last_login_by_id[uid] = normalize_date_only(u.get("lastLogin"))

        total = data.get("totalCount") if isinstance(data, dict) else None
        if isinstance(total, int) and len(users) >= total:
            break

        if not results:
            break

        page_number += 1

    return users, last_login_by_id


def get_user_detail(session: requests.Session, bearer: str, user_id: str) -> Dict[str, Any]:
    url = f"{BASE_URL}/api/patient-registry/user/{user_id}"
    resp = request_with_retry(session, "GET", url, bearer=bearer)
    return resp.json()


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

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for r in rows:
            writer.writerow(
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
            )


# ------------------------------------------------
# Main
# ------------------------------------------------

def main() -> int:
    bearer = os.environ.get("XCURES_BEARER_TOKEN")
    if not bearer:
        print("Error: XCURES_BEARER_TOKEN not set", file=sys.stderr)
        return 1

    timestamp = utc_timestamp()
    json_file = Path(f"permissions_backup_{timestamp}.json")
    csv_file = Path(f"permissions_backup_{timestamp}.csv")

    with requests.Session() as session:
        project_name_map = get_project_name_map(session, bearer)
        users, last_login_map = get_all_users_and_last_login(session, bearer)

        total = len(users)
        rows: List[Dict[str, Any]] = []
        full_records: List[Dict[str, Any]] = []

        for i, u in enumerate(users, start=1):
            progress_bar(i, total)

            user_id = str(u.get("id") or "").strip()
            if not user_id:
                continue

            detail = get_user_detail(session, bearer, user_id)

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

        print()

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