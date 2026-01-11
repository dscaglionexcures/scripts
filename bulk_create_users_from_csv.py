# export XCURES_BEARER_TOKEN="PASTE_TOKEN_HERE" to set the bearer token from your CLI so you don't add a token to the script and it accidentally gets saved to the repo
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
  - Uses bearer token from env var XCURES_BEARER_TOKEN (required).
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
  export XCURES_BEARER_TOKEN="...jwt..."
  python3 bulk_create_users_from_csv.py --csv users.csv --dry-run
  python3 bulk_create_users_from_csv.py --csv users.csv --apply --limit 10 --verbose --log-file run.log
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests

try:
    from tqdm import tqdm  # type: ignore
except Exception:
    tqdm = None  # fallback

# ----------------------------
# Script configuration (edit these once)
# ----------------------------

DEFAULT_BASE_URL = "https://partner.xcures.com"
DEFAULT_TIMEOUT_SECONDS = 60

# Permissions assigned to every created user (array of strings)
DEFAULT_PERMISSIONS: List[str] = [
    # Replace with your standard set
    "User_Read",
    "User_Create",
]

# Projects assigned to every created user (array of project UUID strings)
DEFAULT_PROJECT_IDS: List[str] = [
     "b114ceeb-2adf-4c80-aae2-b6ccae3eac7b",
]

# Organization membership object copied onto every created user.
# Leave as {} if not used in your tenant.
DEFAULT_ORGANIZATION_MEMBERSHIP: Dict[str, Any] = {}

DEFAULT_IDENTITY_PROVIDER = "auth0"
DEFAULT_TYPE = "patient_registry_user"
DEFAULT_BLOCKED = False

# ----------------------------
# CSV schema
# ----------------------------

REQUIRED_CSV_COLUMNS = ["email", "firstName", "lastName"]
OPTIONAL_CSV_COLUMNS = ["roleCode", "npi", "tin"]

# ----------------------------
# Auth + headers (standard)
# ----------------------------


def get_bearer_token() -> str:
    token = os.environ.get("XCURES_BEARER_TOKEN")
    if not token:
        raise RuntimeError(
            "XCURES_BEARER_TOKEN is not set.\n"
            "Run:\n"
            "  export XCURES_BEARER_TOKEN='your_token_here'"
        )
    return token


def auth_headers() -> Dict[str, str]:
    return {
        "accept": "application/json",
        "Authorization": "Bearer " + get_bearer_token(),
        "Content-Type": "application/json",
    }


# ----------------------------
# Progress (standard)
# ----------------------------


def progress_iter(iterable, *, desc: str, total: Optional[int] = None):
    if tqdm is not None:
        return tqdm(iterable, total=total, desc=desc, unit="user")

    total = total if total is not None else len(iterable)  # type: ignore[arg-type]
    bar_width = 30

    def _gen():
        for i, item in enumerate(iterable, start=1):
            progress = i / total if total else 1
            filled = int(bar_width * progress)
            bar = "█" * filled + "-" * (bar_width - filled)
            print(
                f"\r{desc}: |{bar}| {progress*100:6.2f}% ({i}/{total})",
                end="",
                flush=True,
            )
            yield item
        print()

    return _gen()


# ----------------------------
# Logging
# ----------------------------

VERBOSE = False
LOG_FILE_PATH: Optional[str] = None


def _log(msg: str) -> None:
    ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    line = f"[{ts}] {msg}"
    print(line, file=sys.stderr)
    if LOG_FILE_PATH:
        try:
            with open(LOG_FILE_PATH, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            pass


def _body_preview(text: str, limit: int = 400) -> str:
    t = (text or "").strip().replace("\n", " ")
    if len(t) > limit:
        return t[:limit] + "...<truncated>"
    return t


def request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    max_retries: int = 5,
    backoff_seconds: float = 1.0,
) -> requests.Response:
    """
    Retries on transient failures (429, 5xx) and basic network errors.
    """
    headers = auth_headers()
    last_resp: Optional[requests.Response] = None
    last_exc: Optional[BaseException] = None

    for attempt in range(1, max_retries + 1):
        try:
            if VERBOSE:
                _log(f"{method} {url} params={params or {}}")
            t0 = time.time()
            resp = session.request(
                method,
                url,
                headers=headers,
                params=params,
                json=json_body,
                timeout=timeout,
            )
            last_resp = resp
            elapsed_ms = int((time.time() - t0) * 1000)

            if VERBOSE:
                _log(f"-> {resp.status_code} in {elapsed_ms}ms body={_body_preview(resp.text)}")

            if 200 <= resp.status_code < 300:
                return resp

            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(backoff_seconds * (2 ** (attempt - 1)))
                continue

            raise RuntimeError(f"HTTP {resp.status_code} {url} body={_body_preview(resp.text, 1200)}")

        except (requests.Timeout, requests.ConnectionError) as e:
            last_exc = e
            if VERBOSE:
                _log(f"!! network error {type(e).__name__}: {e}")
            time.sleep(backoff_seconds * (2 ** (attempt - 1)))

    if last_resp is not None:
        raise RuntimeError(
            f"request_with_retry exhausted; last_status={last_resp.status_code} url={url} body={_body_preview(last_resp.text, 1200)}"
        )
    raise RuntimeError(f"request_with_retry exhausted; no response; last_exc={last_exc}")


def parse_json(resp: requests.Response) -> Any:
    try:
        return resp.json()
    except Exception:
        raise RuntimeError(f"Non-JSON response status={resp.status_code} body={_body_preview(resp.text, 1200)}")


# ----------------------------
# CSV helpers
# ----------------------------


def normalize_header(s: str) -> str:
    return (s or "").strip()


def read_csv_rows(csv_path: str) -> List[Dict[str, str]]:
    try:
        with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                raise RuntimeError("CSV has no header row.")

            headers = [normalize_header(h) for h in reader.fieldnames]
            missing = [c for c in REQUIRED_CSV_COLUMNS if c not in headers]
            if missing:
                raise RuntimeError(
                    "CSV missing required columns: " + ", ".join(missing)
                    + " (optional: " + ", ".join(OPTIONAL_CSV_COLUMNS) + ")"
                )

            rows: List[Dict[str, str]] = []
            for row in reader:
                cleaned: Dict[str, str] = {}
                for k, v in row.items():
                    if k is None:
                        continue
                    cleaned[normalize_header(k)] = (v or "").strip()
                rows.append(cleaned)
            return rows
    except Exception as e:
        raise RuntimeError(f"Error reading CSV: {e}")


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


def create_user(session: requests.Session, base_url: str, payload: Dict[str, Any], *, timeout: int, max_retries: int, backoff: float) -> Dict[str, Any]:
    url = f"{base_url}/api/patient-registry/user"
    resp = request_with_retry(
        session,
        "POST",
        url,
        json_body=payload,
        timeout=timeout,
        max_retries=max_retries,
        backoff_seconds=backoff,
    )
    data = parse_json(resp)
    if not isinstance(data, dict):
        raise RuntimeError(f"Unexpected create-user response type: {type(data)} body={_body_preview(resp.text, 1200)}")
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
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    out_path = os.path.join(out_dir, f"bulk_create_users_results_{ts}.csv")
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id", "email", "status", "http_status", "message"])
        for r in results:
            w.writerow([r.id, r.email, r.status, r.http_status or "", r.message])
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
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS, help="HTTP timeout seconds (default: %(default)s)")
    parser.add_argument("--max-retries", type=int, default=5, help="Max retries on 429/5xx (default: %(default)s)")
    parser.add_argument("--backoff", type=float, default=1.0, help="Backoff base seconds (default: %(default)s)")
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
        for idx, row in enumerate(progress_iter(rows, desc=f"Creating users ({mode_label})", total=len(rows)), start=1):
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
                        max_retries=args.max_retries,
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
