"""
Bulk create Patient Registry users from a CSV file (xCures).

CSV columns:
  Required:
    - email
    - firstName
    - lastName

  Optional:
    - roleCode
    - npi
    - tin

Behavior:
  - Generates a UUID per row (used for "id" and "identityProviderId" as "auth0|<uuid>").
  - Uses bearer token generated from XCURES_CLIENT_ID / XCURES_CLIENT_SECRET.
  - Permissions, projectIds, and organizationMembership are configured inside this script.
  - identityProvider is always "auth0"
  - type is always "patient_registry_user"
  - blocked is always false
  - Shows a progress bar (tqdm if available; otherwise a fallback percent bar).
  - Writes a results CSV with the generated ids and per-row status.

IMPORTANT:
  You must explicitly choose one mode:
    --dry-run   (no API calls that mutate state)
    --apply     (actually creates users)

Examples:
  python3 bulk_create_users_from_csv.py --csv users.csv --dry-run
  python3 bulk_create_users_from_csv.py --csv users.csv --apply --limit 10 --verbose --log-file run.log
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests
from api_common import (
    DEFAULT_BACKOFF_SECONDS,
    DEFAULT_TIMEOUT_SECONDS,
    body_preview,
    parse_json_or_raise,
    request_with_retry as common_request_with_retry,
)
from progress_common import progress_iter
from auth_common import build_json_headers, get_xcures_bearer_token, load_env_file
from csv_common import read_csv_dict_rows, write_csv_rows

# ----------------------------
# Script configuration (edit these once)
# ----------------------------

DEFAULT_BASE_URL = "https://partner.xcures.com"
SCRIPT_DEFAULT_TIMEOUT_SECONDS = 20

# Permissions assigned to every created user (array of strings)
DEFAULT_PERMISSIONS: List[str] = [
    "Annotation_Read",
    "Dashboard_Read",
    "Document_Read",
    "Fhir_Read",
    "Project_Read",
    "Query_Read",
    "Subject_Cohort_Access_All",
    "Subject_Cohort_Access_Unassigned",
    "Subject_Overview",
    "Subject_Read",
    "Term_Read"
]

# Projects assigned to every created user (array of project UUID strings)
DEFAULT_PROJECT_IDS: List[str] = [
     "de2e5623-9b21-4391-bdfb-5bc2fac5473d",
]

# Organization membership object copied onto every created user.
# Leave as {} if not used in your tenant.
DEFAULT_ORGANIZATION_MEMBERSHIP: Dict[str, Any] = {}

DEFAULT_IDENTITY_PROVIDER = "auth0"
DEFAULT_TYPE = "patient_registry_user"
DEFAULT_BLOCKED = False

load_env_file(Path(__file__).resolve().parent / ".env")

# ----------------------------
# CSV schema
# ----------------------------

REQUIRED_CSV_COLUMNS = ["email", "firstName", "lastName"]
OPTIONAL_CSV_COLUMNS = ["roleCode", "npi", "tin"]

# ----------------------------
# Auth + headers (standard)
# ----------------------------


def get_bearer_token() -> str:
    return get_xcures_bearer_token(timeout_seconds=SCRIPT_DEFAULT_TIMEOUT_SECONDS)


def auth_headers() -> Dict[str, str]:
    return build_json_headers(bearer_token=get_bearer_token())


# ----------------------------
# Logging
# ----------------------------

VERBOSE = False
LOG_FILE_PATH: Optional[str] = None


def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    line = f"[{ts}] {msg}"
    print(line, file=sys.stderr)
    if LOG_FILE_PATH:
        try:
            with open(LOG_FILE_PATH, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            pass


def request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    timeout: int = SCRIPT_DEFAULT_TIMEOUT_SECONDS,
    backoff_seconds: float = DEFAULT_BACKOFF_SECONDS,
) -> requests.Response:
    return common_request_with_retry(
        session,
        method,
        url,
        headers=auth_headers(),
        params=params,
        json_body=json_body,
        timeout_seconds=timeout,
        backoff_seconds=backoff_seconds,
        logger=_log if VERBOSE else None,
    )


def read_csv_rows(csv_path: str) -> List[Dict[str, str]]:
    try:
        rows = read_csv_dict_rows(
            csv_path,
            required_columns=REQUIRED_CSV_COLUMNS,
            encoding="utf-8-sig",
        )
        return rows
    except Exception as e:
        raise RuntimeError(
            f"Error reading CSV: {e}\n"
            f"Required: {', '.join(REQUIRED_CSV_COLUMNS)}; "
            f"optional: {', '.join(OPTIONAL_CSV_COLUMNS)}"
        )


def require_nonempty(value: str, field: str, row_index: int) -> None:
    if not (value or "").strip():
        raise RuntimeError(f"Row {row_index}: '{field}' is required and cannot be empty.")


# ----------------------------
# Payload builder
# ----------------------------


def build_user_payload(row: Dict[str, str], *, user_id: str) -> Dict[str, Any]:
    email = (row.get("email") or "").strip()
    first_name = (row.get("firstName") or "").strip()
    last_name = (row.get("lastName") or "").strip()

    role_code = (row.get("roleCode") or "").strip()
    npi = (row.get("npi") or "").strip()
    tin = (row.get("tin") or "").strip()

    payload: Dict[str, Any] = {
        "id": user_id,
        "identityProviderId": f"{DEFAULT_IDENTITY_PROVIDER}|{user_id}",
        "permissions": list(DEFAULT_PERMISSIONS),
        "projectIds": list(DEFAULT_PROJECT_IDS),
        "email": email,
        "firstName": first_name,
        "lastName": last_name,
        "type": DEFAULT_TYPE,
        "roleCode": role_code,
        "npi": npi,
        "tin": tin,
        "identityProvider": DEFAULT_IDENTITY_PROVIDER,
        "organizationMembership": dict(DEFAULT_ORGANIZATION_MEMBERSHIP),
        "blocked": DEFAULT_BLOCKED,
    }

    # Drop None values (keep empty strings because API examples use them)
    payload = {k: v for k, v in payload.items() if v is not None}
    return payload


# ----------------------------
# API
# ----------------------------


def create_user(session: requests.Session, base_url: str, payload: Dict[str, Any], *, timeout: int, backoff: float) -> Dict[str, Any]:
    url = f"{base_url}/api/patient-registry/user"
    resp = request_with_retry(
        session,
        "POST",
        url,
        json_body=payload,
        timeout=timeout,
        backoff_seconds=backoff,
    )
    data = parse_json_or_raise(resp)
    if not isinstance(data, dict):
        raise RuntimeError(
            f"Unexpected create-user response type: {type(data)} body={body_preview(resp.text, 1200)}"
        )
    return data


# ----------------------------
# Results
# ----------------------------

@dataclass
class CreateResult:
    id: str
    email: str
    status: str  # created|dry_run|failed
    http_status: Optional[int]
    message: str


def write_results_csv(results: Sequence[CreateResult], out_dir: str) -> str:
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = os.path.join(out_dir, f"bulk_create_users_results_{ts}.csv")
    write_csv_rows(
        out_path,
        header=["id", "email", "status", "http_status", "message"],
        rows=[
            [r.id, r.email, r.status, r.http_status or "", r.message]
            for r in results
        ],
    )
    return out_path


# ----------------------------
# CLI
# ----------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bulk create Patient Registry users from CSV (xCures).",
    )
    parser.add_argument("--csv", required=True, help="Path to input CSV")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="Preview only; do not create users")
    mode.add_argument("--apply", action="store_true", help="Actually create users")

    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="API base URL (default: %(default)s)")
    parser.add_argument("--limit", type=int, default=None, help="Only process first N rows")
    parser.add_argument("--timeout", type=int, default=SCRIPT_DEFAULT_TIMEOUT_SECONDS, help="HTTP timeout seconds (default: %(default)s)")
    parser.add_argument("--backoff", type=float, default=DEFAULT_BACKOFF_SECONDS, help="Backoff base seconds (default: %(default)s)")
    parser.add_argument("--out-dir", default=".", help="Directory to write results CSV (default: current dir)")
    parser.add_argument("--verbose", action="store_true", help="Log each API call (does not log secrets)")
    parser.add_argument("--log-file", default=None, help="Append logs to this file path")
    return parser.parse_args()


# ----------------------------
# Main
# ----------------------------


def main() -> int:
    global VERBOSE, LOG_FILE_PATH

    args = parse_args()
    VERBOSE = bool(args.verbose)
    LOG_FILE_PATH = args.log_file
    # Validate config
    if not DEFAULT_PERMISSIONS:
        print("Error: DEFAULT_PERMISSIONS must include at least one permission.", file=sys.stderr)
        return 2

    # Auth presence check early
    try:
        _ = get_bearer_token()
    except Exception as e:
        print(f"Auth error: {e}", file=sys.stderr)
        return 2

    # Read CSV
    try:
        rows = read_csv_rows(args.csv)
    except Exception as e:
        print(str(e), file=sys.stderr)
        return 2

    if args.limit is not None:
        if args.limit < 0:
            print("Error: --limit must be >= 0", file=sys.stderr)
            return 2
        rows = rows[: args.limit]

    if not rows:
        print("No rows found in CSV.")
        return 0

    # Determine mode
    is_dry_run = bool(args.dry_run)
    mode_label = "DRY-RUN" if is_dry_run else "APPLY"

    base_url = (args.base_url or DEFAULT_BASE_URL).rstrip("/")

    results: List[CreateResult] = []
    created = 0
    failed = 0

    with requests.Session() as session:
        for idx, row in enumerate(
            progress_iter(rows, desc=f"Creating users ({mode_label})", total=len(rows), unit="user"),
            start=1,
        ):
            email = (row.get("email") or "").strip()
            try:
                require_nonempty(email, "email", idx)
                require_nonempty(row.get("firstName", ""), "firstName", idx)
                require_nonempty(row.get("lastName", ""), "lastName", idx)

                user_id = str(uuid.uuid4())
                payload = build_user_payload(row, user_id=user_id)

                if is_dry_run:
                    print(f"[DRY-RUN] would create id={user_id} email={email}")
                    results.append(CreateResult(id=user_id, email=email, status="dry_run", http_status=None, message="preview"))
                    continue

                try:
                    create_user(
                        session,
                        base_url,
                        payload,
                        timeout=args.timeout,
                        backoff=args.backoff,
                    )
                    created += 1
                    results.append(CreateResult(id=user_id, email=email, status="created", http_status=200, message="created"))
                except Exception as e:
                    failed += 1
                    results.append(CreateResult(id=user_id, email=email, status="failed", http_status=None, message=str(e)))

            except Exception as e:
                failed += 1
                results.append(CreateResult(id="", email=email, status="failed", http_status=None, message=str(e)))

    out_csv = write_results_csv(results, args.out_dir)

    print(f"\nDone ({mode_label}).")
    print(f"Rows processed: {len(rows)}")
    print(f"Created: {created}")
    print(f"Failed:  {failed}")
    print(f"Results CSV: {out_csv}")

    # Exit non-zero if failures occurred in apply mode
    if (not is_dry_run) and failed > 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
