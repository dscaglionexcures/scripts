"""
Bulk Update xCures Patient Registry user permissions.

Flow:
1) GET  /api/patient-registry/user               -> list users (paginated)
2) GET  /api/patient-registry/user/{id}          -> fetch full user record
3) PUT  /api/patient-registry/user/{id}          -> update user using UpdateUserDto
   - permissions are copied from the GET {id} response, with "Summary_Checklist" added if missing

Auth:
- Bearer token via --bearer OR auto-generated from XCURES_CLIENT_ID / XCURES_CLIENT_SECRET

Optional header:
- ProjectId header via --project-id (some deployments may require it)

Outputs:
- Prints progress and a summary of updated/skipped/failed users
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

import requests
from progress_common import progress_iter
from auth_common import get_xcures_bearer_token, load_env_file
from xcures_client import XcuresApiClient


DEFAULT_BASE_URL = "https://partner.xcures.com"
DEFAULT_TIMEOUT_SECONDS = 60
PERMISSION_TO_ADD = "Summary_Checklist"

load_env_file(Path(__file__).resolve().parent / ".env")


def get_all_users(
    client: XcuresApiClient,
    *,
    page_size: int = 50,
) -> List[Dict[str, Any]]:
    """
    Paginates GET /api/patient-registry/user until all users are collected.

    The OpenAPI spec indicates a UserPaginationResultDto with:
      - totalCount
      - results (array)
    """
    try:
        return client.list_paginated("/api/patient-registry/user", page_size=page_size)
    except RuntimeError as e:
        # Fallback for occasional 500s when requesting larger page sizes.
        msg = str(e)
        if page_size > 50 and ("HTTP 500" in msg or "last_status=500" in msg):
            return client.list_paginated("/api/patient-registry/user", page_size=50)
        raise


def get_user_detail(
    client: XcuresApiClient,
    user_id: str,
) -> Dict[str, Any]:
    data = client.request_json("GET", f"/api/patient-registry/user/{user_id}")
    if not isinstance(data, dict):
        raise RuntimeError(f"Unexpected user detail response for id={user_id}: {type(data)}")
    return data


def make_update_payload_from_user_detail(user_detail: Dict[str, Any]) -> Dict[str, Any]:
    """
    UpdateUserDto fields (from the provided OpenAPI spec):
      email, firstName, lastName, roleCode, npi, tin,
      permissions, projectIds, lastLogin, loginCount, blocked,
      type, organizationMembership, identityProvider

    We intentionally exclude: id, created, updated, identityProviderId
    """
    allowed_keys = {
        "email",
        "firstName",
        "lastName",
        "roleCode",
        "npi",
        "tin",
        "permissions",
        "projectIds",
        "lastLogin",
        "loginCount",
        "blocked",
        "type",
        "organizationMembership",
        "identityProvider",
    }

    payload: Dict[str, Any] = {k: user_detail.get(k) for k in allowed_keys if k in user_detail}

    # Ensure arrays exist as arrays if present
    if "permissions" in payload and payload["permissions"] is None:
        payload["permissions"] = []
    if "projectIds" in payload and payload["projectIds"] is None:
        payload["projectIds"] = []

    return payload


def put_user_update(
    client: XcuresApiClient,
    user_id: str,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    data = client.request_json("PUT", f"/api/patient-registry/user/{user_id}", json_body=payload)
    if not isinstance(data, dict):
        raise RuntimeError(f"Unexpected update response for id={user_id}: {type(data)}")
    return data


def main() -> int:
    parser = argparse.ArgumentParser(
        description='Add "Summary_Checklist" permission to all users in a tenant.'
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help=f"API base URL (default: {DEFAULT_BASE_URL})",
    )
    parser.add_argument(
        "--bearer",
        default=None,
        help="Optional bearer token override; otherwise uses XCURES_CLIENT_ID/XCURES_CLIENT_SECRET",
    )
    parser.add_argument(
        "--project-id",
        default=os.environ.get("XCURES_PROJECT_ID"),
        help="Optional ProjectId header value (or set env var XCURES_PROJECT_ID)",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=50,
        help="Page size for user list pagination (default: 50)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process only the first N users (useful for testing).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
        help=f"Request timeout seconds (default: {DEFAULT_TIMEOUT_SECONDS})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not perform PUT updates, only report which users would be updated.",
    )
    parser.add_argument(
        "--only-missing",
        action="store_true",
        default=True,
        help='Only update users missing "Summary_Checklist" (default: enabled).',
    )

    args = parser.parse_args()

    if not args.bearer:
        try:
            args.bearer = get_xcures_bearer_token(timeout_seconds=args.timeout)
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            return 2

    updated = 0
    skipped = 0
    failed = 0
    failures: List[Tuple[str, str]] = []

    with requests.Session() as session:
        client = XcuresApiClient(
            session=session,
            base_url=args.base_url.rstrip("/"),
            bearer_token=args.bearer,
            project_id=args.project_id,
            timeout_seconds=args.timeout,
            max_retries=5,
            backoff_seconds=2.0,
            max_sleep_seconds=20.0,
        )
        # 1) list users
        users = get_all_users(
            client,
            page_size=args.page_size,
        )

        if args.limit is not None:
            if args.limit < 0:
                raise ValueError("--limit must be >= 0")
            users = users[: args.limit]

        total = len(users)
        if total == 0:
            print("No users returned by the API.")
            return 0

        for user in progress_iter(users, desc="Updating users", total=total, unit="user"):
            user_id = user.get("id")
            if not user_id:
                skipped += 1
                continue

            try:
                # 2) detail fetch
                detail = get_user_detail(
                    client,
                    user_id,
                )

                permissions = detail.get("permissions") or []
                if not isinstance(permissions, list):
                    permissions = []

                already_has = PERMISSION_TO_ADD in permissions
                if already_has and args.only_missing:
                    skipped += 1
                    continue

                if not already_has:
                    permissions.append(PERMISSION_TO_ADD)

                # 3) update
                payload = make_update_payload_from_user_detail(detail)
                payload["permissions"] = permissions

                if args.dry_run:
                    updated += 1
                    continue

                put_user_update(
                    client,
                    user_id,
                    payload,
                )
                updated += 1

            except Exception as e:
                failed += 1
                failures.append((str(user_id), str(e)))
                continue

    print("\nDone.")
    print(f"Total users processed: {total}")
    print(f"Updated: {updated}")
    print(f"Skipped: {skipped}")
    print(f"Failed: {failed}")

    if failures:
        print("\nFailures:")
        for uid, err in failures[:50]:
            print(f"- {uid}: {err}")
        if len(failures) > 50:
            print(f"... plus {len(failures) - 50} more failures")

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
