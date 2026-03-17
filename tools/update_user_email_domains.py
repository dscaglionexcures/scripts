"""
Update user email domains in xCures Patient Registry, excluding @xcures.com users.

Flow:
1) GET  /api/patient-registry/user              -> list users (paged)
2) GET  /api/patient-registry/user/{id}         -> fetch full user record
3) PUT  /api/patient-registry/user/{id}         -> update user (email changed)

Safety:
- Always excludes emails ending with @xcures.com (case-insensitive).
- Default is --dry-run (no writes). Use --apply to perform updates.
- Supports --limit to process only first N users for testing.
- Progress bar: tqdm if installed, otherwise a built-in fallback.
- Auth: bearer token is generated from XCURES_CLIENT_ID / XCURES_CLIENT_SECRET.

Examples:
  python3 tools/update_user_email_domains.py --from-domain old.com --to-domain new.com --dry-run
  python3 tools/update_user_email_domains.py --from-domain old.com --to-domain new.com --apply --only-missing
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from xcures_toolkit.api_common import DEFAULT_BACKOFF_SECONDS, DEFAULT_TIMEOUT_SECONDS
from xcures_toolkit.progress_common import progress_iter
from xcures_toolkit.auth_common import load_env_file
from xcures_toolkit.xcures_client import XcuresApiClient


DEFAULT_BASE_URL = "https://partner.xcures.com"
EXCLUDE_DOMAIN = "xcures.com"

load_env_file(Path(__file__).resolve().parent.parent / ".env")


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


# ----------------------------
# API operations
# ----------------------------
def get_all_users(
    client: XcuresApiClient,
    *,
    page_size: int,
    max_pages: int = 10_000,
) -> List[Dict[str, Any]]:
    return client.list_paginated(
        "/api/patient-registry/user",
        page_size=page_size,
        max_pages=max_pages,
    )


def get_user_detail(client: XcuresApiClient, user_id: str) -> Dict[str, Any]:
    data = client.request_json("GET", f"/api/patient-registry/user/{user_id}")
    if not isinstance(data, dict):
        raise RuntimeError(f"Unexpected user detail type for id={user_id}: {type(data)}")
    return data


def update_user(client: XcuresApiClient, user_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    data = client.request_json("PUT", f"/api/patient-registry/user/{user_id}", json_body=payload)
    if not isinstance(data, dict):
        raise RuntimeError(f"Unexpected update response type for id={user_id}: {type(data)}")
    return data


# ----------------------------
# Email rewrite logic
# ----------------------------
def split_email(email: str) -> Tuple[str, str]:
    if "@" not in email:
        return email, ""
    local, domain = email.rsplit("@", 1)
    return local, domain


def should_exclude(email: str) -> bool:
    _, domain = split_email(email.strip())
    return domain.lower() == EXCLUDE_DOMAIN


def rewrite_domain(email: str, from_domain: str, to_domain: str) -> Optional[str]:
    email = (email or "").strip()
    if not email or "@" not in email:
        return None
    local, domain = split_email(email)
    if domain.lower() != from_domain.lower():
        return None
    return f"{local}@{to_domain}"


# ----------------------------
# CLI + main
# ----------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Update user email domains in xCures Patient Registry (exclude @xcures.com).")
    p.add_argument("--base-url", default=DEFAULT_BASE_URL, help="API base URL (default: %(default)s)")
    p.add_argument("--from-domain", required=True, help="Only update users whose email domain matches this value.")
    p.add_argument("--to-domain", required=True, help="Replace the email domain with this value.")
    p.add_argument("--page-size", type=int, default=50, help="List users page size (default: %(default)s)")
    p.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS, help="HTTP timeout seconds (default: %(default)s)")
    p.add_argument("--limit", type=int, default=None, help="Process only first N users returned by list endpoint.")
    p.add_argument("--dry-run", action="store_true", help="Preview changes only (no PUTs).")
    p.add_argument("--apply", action="store_true", help="Perform updates (PUT).")
    p.add_argument("--verbose", action="store_true", help="Log each API call (does not log secrets).")
    p.add_argument("--log-file", default=None, help="Optional log file path (appends).")
    p.add_argument("--only-missing", action="store_true", help="Only PUT when email actually changes (recommended).")
    return p.parse_args()


def main() -> int:
    global VERBOSE, LOG_FILE_PATH

    args = parse_args()
    VERBOSE = bool(args.verbose)
    LOG_FILE_PATH = args.log_file

    if args.apply and args.dry_run:
        print("Error: choose either --dry-run or --apply (not both).", file=sys.stderr)
        return 2
    if not args.apply and not args.dry_run:
        # Safer default: dry-run unless explicitly applying
        args.dry_run = True

    base_url = args.base_url.rstrip("/")
    from_domain = args.from_domain.strip().lstrip("@")
    to_domain = args.to_domain.strip().lstrip("@")

    if from_domain.lower() == EXCLUDE_DOMAIN.lower():
        print(f"Refusing: --from-domain is {EXCLUDE_DOMAIN}, but @xcures.com is always excluded.", file=sys.stderr)
        return 2
    if to_domain.lower() == EXCLUDE_DOMAIN.lower():
        print(f"Refusing: --to-domain is {EXCLUDE_DOMAIN}, but @xcures.com is always excluded.", file=sys.stderr)
        return 2

    with requests.Session() as session:
        client = XcuresApiClient(
            session=session,
            base_url=base_url,
            project_id=os.environ.get("XCURES_PROJECT_ID"),
            timeout_seconds=args.timeout,
            backoff_seconds=DEFAULT_BACKOFF_SECONDS,
            logger=_log if VERBOSE else None,
        )
        users = get_all_users(client, page_size=args.page_size)

        if args.limit is not None:
            if args.limit < 0:
                print("Error: --limit must be >= 0", file=sys.stderr)
                return 2
            users = users[: args.limit]

        if not users:
            print("No users returned by the API.")
            return 0

        updated = 0
        skipped = 0
        excluded = 0
        failed: List[Tuple[str, str]] = []

        for u in progress_iter(users, desc="Updating emails", total=len(users), unit="user"):
            user_id = None
            if isinstance(u, dict):
                user_id = u.get("id") or u.get("userId")
            if not user_id:
                skipped += 1
                continue

            try:
                detail = get_user_detail(client, str(user_id))
                email = str(detail.get("email") or "").strip()

                if not email:
                    skipped += 1
                    continue

                if should_exclude(email):
                    excluded += 1
                    continue

                new_email = rewrite_domain(email, from_domain, to_domain)
                if not new_email:
                    skipped += 1
                    continue

                if args.only_missing and new_email == email:
                    skipped += 1
                    continue

                if args.dry_run:
                    print(f"[DRY-RUN] {user_id}: {email} -> {new_email}")
                    updated += 1
                    continue

                # Apply update: preserve everything else, change email only.
                payload = dict(detail)
                payload["email"] = new_email

                # Some backends dislike None values; optionally drop keys with None.
                payload = {k: v for k, v in payload.items() if v is not None}

                update_user(client, str(user_id), payload)
                updated += 1

            except Exception as e:
                failed.append((str(user_id), str(e)))
                continue

    mode = "DRY-RUN" if args.dry_run else "APPLY"
    print(f"\nDone ({mode}).")
    print(f"Updated:  {updated}")
    print(f"Skipped:  {skipped}")
    print(f"Excluded (@{EXCLUDE_DOMAIN}): {excluded}")
    print(f"Failed:   {len(failed)}")
    if failed:
        print("\nFailures (up to 20):")
        for uid, err in failed[:20]:
            print(f"- {uid}: {err}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
