# Use this script to update all users in a tenant when new projects are created
# First use case is MedSync as they have a large number of projects that grows weekly
# export XCURES_BEARER_TOKEN="PASTE_TOKEN_HERE" to set the bearer token from your CLI so you don't add a token to the script and it 
# accidentally gets saved to the repo

""" Update all users so they include all current projectIds in the organization.

Standards:
- No CLI arguments
- Auth via XCURES_BEARER_TOKEN environment variable
- Optional XCURES_BASE_URL override (defaults to https://partner.xcures.com)
- Shows project names for confirmation (Y/N)
- Progress bar included
"""

from __future__ import annotations

import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

DEFAULT_BASE_URL = "https://partner.xcures.com"
DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_MAX_RETRIES = 1
DEFAULT_BACKOFF_SECONDS = 1.0

VERBOSE = True


def log(msg: str) -> None:
    if VERBOSE:
        print(msg, file=sys.stderr)


def body_preview(text: str, limit: int = 800) -> str:
    t = (text or "").strip().replace("\n", " ")
    return t if len(t) <= limit else t[:limit] + "...<truncated>"


def get_bearer_token() -> str:
    token = os.environ.get("XCURES_BEARER_TOKEN")
    if not token:
        raise RuntimeError(
            "XCURES_BEARER_TOKEN is not set.\n"
            "Run:\n"
            "  export XCURES_BEARER_TOKEN='your_token_here'"
        )
    return token


def get_base_url() -> str:
    return os.environ.get("XCURES_BASE_URL", DEFAULT_BASE_URL).rstrip("/")


def auth_headers() -> Dict[str, str]:
    return {
        "accept": "application/json",
        "Authorization": "Bearer " + get_bearer_token(),
        "Content-Type": "application/json",
    }


def request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    max_retries: int = DEFAULT_MAX_RETRIES,
    backoff_seconds: float = DEFAULT_BACKOFF_SECONDS,
) -> requests.Response:
    last_resp: Optional[requests.Response] = None
    last_exc: Optional[BaseException] = None

    for attempt in range(1, max_retries + 1):
        try:
            if VERBOSE:
                log(f"{method} {url} attempt={attempt}/{max_retries}")
            resp = session.request(
                method,
                url,
                headers=auth_headers(),
                params=params,
                json=json_body,
                timeout=timeout,
            )
            last_resp = resp

            if 200 <= resp.status_code < 300:
                return resp

            if resp.status_code in (429, 500, 502, 503, 504):
                log(f"HTTP {resp.status_code} retryable. body={body_preview(resp.text)}")
                time.sleep(backoff_seconds * (2 ** (attempt - 1)))
                continue

            raise RuntimeError(f"HTTP {resp.status_code} {url} body={body_preview(resp.text)}")

        except (requests.Timeout, requests.ConnectionError, requests.RequestException) as e:
            last_exc = e
            log(f"Request error: {type(e).__name__}: {e}")
            time.sleep(backoff_seconds * (2 ** (attempt - 1)))

    if last_resp is not None:
        raise RuntimeError(
            f"request_with_retry exhausted; last_status={last_resp.status_code} "
            f"url={url} body={body_preview(last_resp.text)}"
        )
    raise RuntimeError(
        f"request_with_retry exhausted; no HTTP response received; url={url} "
        f"last_error={type(last_exc).__name__ if last_exc else 'unknown'}: {last_exc}"
    )


def parse_json(resp: requests.Response) -> Any:
    try:
        return resp.json()
    except Exception:
        raise RuntimeError(f"Non-JSON response: status={resp.status_code} body={body_preview(resp.text)}")


def prompt_yes_no(msg: str) -> bool:
    raw = input(f"{msg} (Y/N): ").strip().lower()
    return raw in ("y", "yes")


def progress_bar(current: int, total: int, *, prefix: str = "Progress") -> None:
    bar_width = 30
    if total <= 0:
        total = 1
    progress = current / total
    filled = int(bar_width * progress)
    bar = "█" * filled + "-" * (bar_width - filled)
    percent = progress * 100
    print(f"\r{prefix}: |{bar}| {percent:6.2f}% ({current}/{total})", end="", flush=True)


def get_all_projects(session: requests.Session, base_url: str) -> List[Dict[str, Any]]:
    url = f"{base_url}/api/patient-registry/project"
    resp = request_with_retry(session, "GET", url)
    data = parse_json(resp)
    if not isinstance(data, list):
        raise RuntimeError(f"Unexpected projects response type: {type(data)}")
    return [p for p in data if isinstance(p, dict)]


def get_all_users(session: requests.Session, base_url: str, page_size: int = 25) -> List[Dict[str, Any]]:
    all_results: List[Dict[str, Any]] = []
    page_number = 1

    while True:
        params = {
            "pageSize": page_size,
            "pageNumber": page_number,
            "hasActiveFilter": "false",
            "numberOfActiveFilters": 0,
        }
        url = f"{base_url}/api/patient-registry/user"
        resp = request_with_retry(session, "GET", url, params=params)
        data = parse_json(resp)
        if not isinstance(data, dict):
            raise RuntimeError(f"Unexpected users response type: {type(data)}")

        results = data.get("results", [])
        if isinstance(results, list):
            all_results.extend([r for r in results if isinstance(r, dict)])

        total_count = data.get("totalCount")
        if isinstance(total_count, int):
            if page_number * page_size >= total_count:
                break
        else:
            if not results:
                break

        page_number += 1

    return all_results


def get_user_detail(session: requests.Session, base_url: str, user_id: str) -> Dict[str, Any]:
    url = f"{base_url}/api/patient-registry/user/{user_id}"
    params = {"userId": user_id}
    resp = request_with_retry(session, "GET", url, params=params)
    data = parse_json(resp)
    if not isinstance(data, dict):
        raise RuntimeError(f"Unexpected user detail response type: {type(data)}")
    return data


def update_user(session: requests.Session, base_url: str, user_id: str, user_obj: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{base_url}/api/patient-registry/user/{user_id}"
    resp = request_with_retry(session, "PUT", url, json_body=user_obj)
    data = parse_json(resp)
    if not isinstance(data, dict):
        raise RuntimeError(f"Unexpected user update response type: {type(data)}")
    return data


def main() -> int:
    try:
        get_bearer_token()
    except Exception as e:
        print(f"Auth error: {e}", file=sys.stderr)
        return 2

    base_url = get_base_url()

    with requests.Session() as session:
        projects = get_all_projects(session, base_url)

        project_pairs: List[Tuple[str, str]] = []
        for p in projects:
            pid = str(p.get("id") or "").strip()
            name = str(p.get("name") or "").strip()
            if pid:
                project_pairs.append((pid, name))

        project_pairs.sort(key=lambda x: (x[1].lower(), x[0]))

        print("\nProjects that will be added to every user (by name):\n")
        for _, name in project_pairs:
            print(f" - {name or '(no name)'}")

        if not project_pairs:
            print("\nNo projects found. Exiting.", file=sys.stderr)
            return 2

        if not prompt_yes_no("\nProceed and add ALL of the above projects to ALL users?"):
            print("Cancelled.")
            return 0

        target_project_ids = [pid for pid, _ in project_pairs]

        users = get_all_users(session, base_url, page_size=25)
        total_users = len(users)
        print(f"\nFound {total_users} users. Updating...\n")

        updated_count = 0
        skipped_count = 0
        error_count = 0

        for idx, u in enumerate(users, start=1):
            progress_bar(idx, total_users, prefix="Progress")
            user_id = str(u.get("id") or "").strip()
            if not user_id:
                skipped_count += 1
                continue

            try:
                user_obj = get_user_detail(session, base_url, user_id)
                existing = user_obj.get("projectIds")
                if not isinstance(existing, list):
                    existing = []
                    user_obj["projectIds"] = existing

                existing_set = set(str(x) for x in existing if x is not None)
                missing = [pid for pid in target_project_ids if pid not in existing_set]

                if not missing:
                    skipped_count += 1
                    continue

                for pid in missing:
                    existing.append(pid)

                _ = update_user(session, base_url, user_id, user_obj)
                updated_count += 1

            except Exception as e:
                error_count += 1
                log(f"\nERROR updating user_id={user_id}: {e}")

        print()
        print("\nDone.")
        print(f"Users updated: {updated_count}")
        print(f"Users skipped (already had all projects / missing id): {skipped_count}")
        print(f"Errors: {error_count}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
