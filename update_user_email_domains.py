# export XCURES_BEARER_TOKEN="PASTE_TOKEN_HERE" to set the bearer token from your CLI so you don't add a token to the script and it accidentally gets saved to the repo
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
- Auth: bearer token must be exported to env var XCURES_BEARER_TOKEN.

Examples:
  export XCURES_BEARER_TOKEN="...jwt..."
  python3 update_user_email_domain.py --from-domain old.com --to-domain new.com --dry-run
  python3 update_user_email_domain.py --from-domain old.com --to-domain new.com --apply --only-missing
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests

try:
    from tqdm import tqdm  # type: ignore
except Exception:
    tqdm = None  # fallback


DEFAULT_BASE_URL = "https://partner.xcures.com"
DEFAULT_TIMEOUT_SECONDS = 60
EXCLUDE_DOMAIN = "xcures.com"


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
# Progress Bar Function (standard)
# ----------------------------
def progress_iter(iterable, *, desc: str, total: Optional[int] = None):
    if tqdm is not None:
        return tqdm(iterable, total=total, desc=desc, unit="user")

    total = total if total is not None else len(iterable)
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


def _body_preview(text: str, limit: int = 300) -> str:
    t = (text or "").strip().replace("\n", " ")
    if len(t) > limit:
        return t[:limit] + "...<truncated>"
    return t


def request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    *,
    params: Optional[Dictifra[str, Any]] = None,  # type: ignore[name-defined]
    json_body: Optional[Dict[str, Any]] = None,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    max_retries: int = 5,
    backoff_seconds: float = 1.0,
) -> requests.Response:
    """
    Retries on transient failures (429, 5xx). Surfaces useful details if exhausted.
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

            raise RuntimeError(f"HTTP {resp.status_code} {url} body={_body_preview(resp.text, 800)}")

        except (requests.Timeout, requests.ConnectionError) as e:
            last_exc = e
            if VERBOSE:
                _log(f"!! network error {type(e).__name__}: {e}")
            time.sleep(backoff_seconds * (2 ** (attempt - 1)))

    if last_resp is not None:
        raise RuntimeError(
            f"request_with_retry exhausted; last_status={last_resp.status_code} url={url} body={_body_preview(last_resp.text, 800)}"
        )
    raise RuntimeError(f"request_with_retry exhausted; no response; last_exc={last_exc}")


def parse_json(resp: requests.Response) -> Any:
    try:
        return resp.json()
    except Exception:
        raise RuntimeError(f"Non-JSON response status={resp.status_code} body={_body_preview(resp.text, 800)}")


# ----------------------------
# API operations
# ----------------------------
def get_all_users(
    session: requests.Session,
    base_url: str,
    *,
    page_size: int,
    timeout: int,
    max_pages: int = 10_000,
) -> List[Dict[str, Any]]:
    url = f"{base_url}/api/patient-registry/user"
    all_users: List[Dict[str, Any]] = []

    page_number = 1
    for _ in range(max_pages):
        params = {"pageNumber": page_number, "pageSize": page_size}
        resp = request_with_retry(session, "GET", url, params=params, timeout=timeout)
        data = parse_json(resp)

        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]

        if not isinstance(data, dict):
            raise RuntimeError(f"Unexpected list-users response type: {type(data)}")

        results = data.get("results") if isinstance(data.get("results"), list) else []
        all_users.extend([r for r in results if isinstance(r, dict)])

        total_count = data.get("totalCount")
        if isinstance(total_count, int) and len(all_users) >= total_count:
            break

        if not results:
            break

        page_number += 1

    return all_users


def get_user_detail(session: requests.Session, base_url: str, user_id: str, *, timeout: int) -> Dict[str, Any]:
    url = f"{base_url}/api/patient-registry/user/{user_id}"
    resp = request_with_retry(session, "GET", url, timeout=timeout)
    data = parse_json(resp)
    if not isinstance(data, dict):
        raise RuntimeError(f"Unexpected user detail type for id={user_id}: {type(data)}")
    return data


def update_user(session: requests.Session, base_url: str, user_id: str, payload: Dict[str, Any], *, timeout: int) -> Dict[str, Any]:
    url = f"{base_url}/api/patient-registry/user/{user_id}"
    resp = request_with_retry(session, "PUT", url, json_body=payload, timeout=timeout)
    data = parse_json(resp)
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

    # Auth presence check early
    _ = get_bearer_token()

    with requests.Session() as session:
        users = get_all_users(session, base_url, page_size=args.page_size, timeout=args.timeout)

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

        for u in progress_iter(users, desc="Updating emails", total=len(users)):
            user_id = None
            if isinstance(u, dict):
                user_id = u.get("id") or u.get("userId")
            if not user_id:
                skipped += 1
                continue

            try:
                detail = get_user_detail(session, base_url, str(user_id), timeout=args.timeout)
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

                update_user(session, base_url, str(user_id), payload, timeout=args.timeout)
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
