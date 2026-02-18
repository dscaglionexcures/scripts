#!/usr/bin/env python3
"""
Fetch people names from https://app.himssconference.com/api/graphql
using a persisted GraphQL query observed in your cURL.

Security:
  - Do NOT hardcode tokens in this file.
  - Set environment variable HIMSS_BEARER_TOKEN before running.

Usage:
  export HIMSS_BEARER_TOKEN='eyJhbGciOi...'
  python himss_people_fetch.py

Output:
  himss_people.csv with columns: name, company, title, id
"""

from __future__ import annotations

import csv
import json
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

ENDPOINT = "https://app.himssconference.com/api/graphql"

# From your pasted cURL
OPERATION_NAME = "EventRecommendedPeopleListViewQuery"
PERSISTED_QUERY_SHA256 = "ec1391c7763df8e2e77e30b55b2b6e47323e8af8d056849a52e315808c361278"
VIEW_ID = "RXZlbnRWaWV3XzEyMTIxNDY="

# These came from your request headers. Keep them because some backends gate behavior on these.
BASE_HEADERS = {
    "accept": "*/*",
    "content-type": "application/json",
    "origin": "https://app.himssconference.com",
    "referer": "https://app.himssconference.com/",
    "user-agent": "Mozilla/5.0 (compatible; python-requests)",
    "x-client-origin": "app.himssconference.com",
    "x-client-platform": "Event App",
    "x-client-version": "2.309.454",
    "x-feature-flags": "fixBackwardPaginationOrder",
}


def build_session() -> requests.Session:
    token = os.getenv("HIMSS_BEARER_TOKEN", "").strip()
    if not token:
        raise RuntimeError("Missing HIMSS_BEARER_TOKEN environment variable.")

    s = requests.Session()
    s.headers.update(BASE_HEADERS)
    s.headers["authorization"] = f"Bearer {token}"
    return s


def make_payload(end_cursor: Optional[str]) -> Dict[str, Any]:
    variables: Dict[str, Any] = {"viewId": VIEW_ID}
    # Some list queries paginate via endCursor. If the backend ignores it, it is harmless.
    if end_cursor:
        variables["endCursor"] = end_cursor

    return {
        "operationName": OPERATION_NAME,
        "variables": variables,
        "extensions": {
            "persistedQuery": {"version": 1, "sha256Hash": PERSISTED_QUERY_SHA256}
        },
    }


def post_graphql(session: requests.Session, end_cursor: Optional[str]) -> Dict[str, Any]:
    payload = make_payload(end_cursor)
    r = session.post(ENDPOINT, data=json.dumps(payload), timeout=60)

    if r.status_code in (401, 403):
        raise RuntimeError(
            f"Auth failed (HTTP {r.status_code}). "
            "Make sure HIMSS_BEARER_TOKEN is valid and not expired."
        )
    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:2000]}")

    return r.json()


def find_first_connection(obj: Any) -> Optional[Dict[str, Any]]:
    """
    Walk the JSON response and return the first dict that looks like a GraphQL connection:
      - has 'nodes' (list) and 'pageInfo' (dict), or
      - has 'edges' (list) and 'pageInfo' (dict)
    """
    if isinstance(obj, dict):
        if isinstance(obj.get("pageInfo"), dict) and (
            isinstance(obj.get("nodes"), list) or isinstance(obj.get("edges"), list)
        ):
            return obj
        for v in obj.values():
            found = find_first_connection(v)
            if found:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = find_first_connection(item)
            if found:
                return found
    return None


def extract_nodes(connection: Dict[str, Any]) -> List[Dict[str, Any]]:
    if isinstance(connection.get("nodes"), list):
        return [n for n in connection["nodes"] if isinstance(n, dict)]
    if isinstance(connection.get("edges"), list):
        out = []
        for e in connection["edges"]:
            if isinstance(e, dict) and isinstance(e.get("node"), dict):
                out.append(e["node"])
        return out
    return []


def node_to_row(n: Dict[str, Any]) -> Dict[str, str]:
    # These are the fields we observed in your HAR export (name/company/title/id).
    # If the schema differs, this still writes blanks gracefully.
    name = (n.get("name") or n.get("fullName") or "").strip() if isinstance(n.get("name") or n.get("fullName"), str) else ""
    company = ""
    if isinstance(n.get("company"), str):
        company = n["company"].strip()
    elif isinstance(n.get("organization"), str):
        company = n["organization"].strip()
    elif isinstance(n.get("company"), dict) and isinstance(n["company"].get("name"), str):
        company = n["company"]["name"].strip()

    title = ""
    for k in ("title", "jobTitle", "position", "headline"):
        if isinstance(n.get(k), str) and n[k].strip():
            title = n[k].strip()
            break

    pid = ""
    for k in ("id", "userId", "profileId", "attendeeId"):
        if isinstance(n.get(k), (str, int)) and str(n[k]).strip():
            pid = str(n[k]).strip()
            break

    return {"name": name, "company": company, "title": title, "id": pid}


def dedupe(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen = set()
    out = []
    for r in rows:
        key = (r.get("id", ""), r.get("name", "").lower(), r.get("company", "").lower(), r.get("title", "").lower())
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def main() -> int:
    session = build_session()

    all_rows: List[Dict[str, str]] = []
    end_cursor: Optional[str] = None
    page = 0

    while True:
        page += 1
        print(f"Fetching page {page}...", file=sys.stderr)
        resp = post_graphql(session, end_cursor=end_cursor)

        if resp.get("errors"):
            raise RuntimeError(f"GraphQL errors: {json.dumps(resp['errors'], indent=2)[:4000]}")

        data = resp.get("data", {})
        connection = find_first_connection(data)
        if not connection:
            # Dump keys to help debug if schema changes
            raise RuntimeError(
                "Could not locate a connection (nodes/edges + pageInfo) in response. "
                f"Top-level data keys: {list(data.keys())}"
            )

        nodes = extract_nodes(connection)
        for n in nodes:
            all_rows.append(node_to_row(n))

        page_info = connection.get("pageInfo", {}) if isinstance(connection.get("pageInfo"), dict) else {}
        has_next = page_info.get("hasNextPage")
        new_cursor = page_info.get("endCursor")

        # If pagination info is missing or falsey, stop after the first fetch.
        if not has_next or not new_cursor:
            break

        # Avoid infinite loops if cursor does not change
        if new_cursor == end_cursor:
            print("Cursor did not advance. Stopping to avoid infinite loop.", file=sys.stderr)
            break

        end_cursor = new_cursor
        time.sleep(0.25)

    all_rows = dedupe(all_rows)

    out_file = "himss_people.csv"
    with open(out_file, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["name", "company", "title", "id"])
        w.writeheader()
        w.writerows(all_rows)

    print(f"Wrote {len(all_rows)} rows to {out_file}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())