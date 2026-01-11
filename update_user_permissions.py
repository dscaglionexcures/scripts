#!/usr/bin/env python3
"""
Bulk Update xCures Patient Registry user permissions.

Flow:
1) GET  /api/patient-registry/user               -> list users (paginated)
2) GET  /api/patient-registry/user/{id}          -> fetch full user record
3) PUT  /api/patient-registry/user/{id}          -> update user using UpdateUserDto
   - permissions are copied from the GET {id} response, with "Summary_Checklist" added if missing

Auth:
- Bearer token via --bearer or env var XCURES_BEARER_TOKEN

Optional header:
- ProjectId header via --project-id (some deployments may require it)

Outputs:
- Prints progress and a summary of updated/skipped/failed users
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

try:
    from tqdm import tqdm  # type: ignore
except Exception:
    tqdm = None  # fallback to simple progress if tqdm isn't installed


DEFAULT_BASE_URL = "https://partner.xcures.com"
DEFAULT_TIMEOUT_SECONDS = 60
PERMISSION_TO_ADD = "Summary_Checklist"


def build_headers(bearer_token: str, project_id: Optional[str] = None) -> Dict[str, str]:
    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {bearer_token}",
        "content-type": "application/json",
    }
    if project_id:
        headers["ProjectId"] = project_id
    return headers


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
) -> requests.Response:
    """
    Basic retry for transient errors (429/5xx/timeouts).
    - Keeps the last HTTP response (status + body snippet) so failures are actionable.
    - Includes the last exception message when the failure is network-level.
    """
    last_exc: Optional[Exception] = None
    last_resp: Optional[requests.Response] = None

    for attempt in range(1, max_retries + 1):
        try:
            resp = session.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=json_body,
                timeout=timeout,
            )
            last_resp = resp

            if resp.status_code in (429, 500, 502, 503, 504):
                # Exponential backoff with cap
                sleep_s = min(2 ** attempt, 20)
                time.sleep(sleep_s)
                continue

            return resp

        except (requests.Timeout, requests.ConnectionError) as e:
            last_exc = e
            sleep_s = min(2 ** attempt, 20)
            time.sleep(sleep_s)

    # If we got HTTP responses but they were always retryable, surface details.
    if last_resp is not None:
        body_preview = (last_resp.text or "").strip().replace("\\n", " ")
        if len(body_preview) > 800:
            body_preview = body_preview[:800] + "...<truncated>"
        raise RuntimeError(
            "request_with_retry: exhausted retries; "
            f"last_status={last_resp.status_code} "
            f"url={url} "
            f"body={body_preview}"
        )

    # Otherwise it was a network-level failure with no HTTP response.
    if last_exc is not None:
        raise RuntimeError(
            "request_with_retry: exhausted retries due to network error; "
            f"url={url} error={type(last_exc).__name__}: {last_exc}"
        ) from last_exc

    raise RuntimeError(
        "request_with_retry: exhausted retries without response or exception (unexpected)"
    )


def get_all_users(
    session: requests.Session,
    base_url: str,
    headers: Dict[str, str],
    *,
    page_size: int = 50,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
) -> List[Dict[str, Any]]:
    """
    Paginates GET /api/patient-registry/user until all users are collected.

    The OpenAPI spec indicates a UserPaginationResultDto with:
      - totalCount
      - results (array)
    """
    users: List[Dict[str, Any]] = []
    page_number = 1

    while True:
        url = f"{base_url}/api/patient-registry/user"
        params = {
            "pageNumber": page_number,
            "pageSize": page_size,
        }

        try:
            resp = request_with_retry(
                session, "GET", url, headers=headers, params=params, timeout=timeout
            )
        except RuntimeError as e:
            # Fallback for occasional 500s when requesting larger page sizes.
            # The OpenAPI spec default is 50, and some deployments appear sensitive to larger values.
            msg = str(e)
            if page_number == 1 and page_size > 50 and "last_status=500" in msg:
                page_size = 50
                params["pageSize"] = page_size
                resp = request_with_retry(
                    session, "GET", url, headers=headers, params=params, timeout=timeout
                )
            else:
                raise

        if not resp.ok:
            raise RuntimeError(f"GET {url} failed: {resp.status_code} {resp.text}")

        payload = resp.json()

        # Defensive parsing: some APIs use "results", some use "items"
        page_results = payload.get("results") or payload.get("items") or []
        if not isinstance(page_results, list):
            raise RuntimeError(f"Unexpected list payload format on page {page_number}: {payload}")

        users.extend(page_results)

        total_count = payload.get("totalCount")
        if isinstance(total_count, (int, float)) and len(users) >= int(total_count):
            break

        # If API doesn't return totalCount, stop when no results
        if not page_results:
            break

        page_number += 1

    return users


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
    if not resp.ok:
        raise RuntimeError(f"GET {url} failed: {resp.status_code} {resp.text}")
    return resp.json()


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
    session: requests.Session,
    base_url: str,
    user_id: str,
    headers: Dict[str, str],
    payload: Dict[str, Any],
    *,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
) -> Dict[str, Any]:
    url = f"{base_url}/api/patient-registry/user/{user_id}"
    resp = request_with_retry(session, "PUT", url, headers=headers, json_body=payload, timeout=timeout)
    if not resp.ok:
        raise RuntimeError(f"PUT {url} failed: {resp.status_code} {resp.text}")
    return resp.json()


def simple_progress(iterable, total: int, desc: str):
    """
    Fallback progress indicator if tqdm isn't installed.
    """
    done = 0
    last_pct = -1
    print(desc)
    for item in iterable:
        done += 1
        pct = int((done / total) * 100) if total else 100
        if pct != last_pct:
            print(f"  {pct}% ({done}/{total})")
            last_pct = pct
        yield item


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
        default=os.environ.get("XCURES_BEARER_TOKEN") or os.environ.get("XCURES_BEARER_TOKEN".upper()),
        help="Bearer token (or set env var XCURES_BEARER_TOKEN)",
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
        print("Error: bearer token is required. Use --bearer or env var XCURES_BEARER_TOKEN.", file=sys.stderr)
        return 2

    headers = build_headers(args.bearer, args.project_id)

    updated = 0
    skipped = 0
    failed = 0
    failures: List[Tuple[str, str]] = []

    with requests.Session() as session:
        # 1) list users
        users = get_all_users(
            session,
            args.base_url.rstrip("/"),
            headers,
            page_size=args.page_size,
            timeout=args.timeout,
        )

        if args.limit is not None:
            if args.limit < 0:
                raise ValueError("--limit must be >= 0")
            users = users[: args.limit]

        total = len(users)
        if total == 0:
            print("No users returned by the API.")
            return 0

        iterator = None
        if tqdm is not None:
            iterator = tqdm(users, total=total, desc="Updating users", unit="user")
        else:
            iterator = simple_progress(users, total=total, desc="Updating users")

        for user in iterator:
            user_id = user.get("id")
            if not user_id:
                skipped += 1
                continue

            try:
                # 2) detail fetch
                detail = get_user_detail(
                    session,
                    args.base_url.rstrip("/"),
                    user_id,
                    headers,
                    timeout=args.timeout,
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
                    session,
                    args.base_url.rstrip("/"),
                    user_id,
                    headers,
                    payload,
                    timeout=args.timeout,
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