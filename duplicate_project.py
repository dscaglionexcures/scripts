#!/usr/bin/env python3
"""
duplicate_project.py

Duplicate an xCures Patient Registry project by copying settings from an existing project.

Flow:
1) Prompt for source projectId
2) GET /api/patient-registry/project (list)
   - Find source project record
   - Pick a template project (most populated for create fields)
3) GET /api/patient-registry/project/{id} (detail)
4) Merge list + detail, then fill create fields from template where needed
5) Normalize the POST payload to match the CreateProjectDto shape and avoid nulls that often cause 500s:
   - For boolean fields: if None -> default
   - For integer fields: if None -> default
   - For string fields: if None -> "" (empty string)
   - For list fields: if None -> [] (empty list)
6) Prompt only for new project name
7) Show review screen (including which fields were defaulted/normalized) and confirm Y/N
8) POST /api/patient-registry/project

No CLI arguments.
Auth:
- XCURES_BEARER_TOKEN environment variable required
Optional:
- XCURES_BASE_URL override (defaults to https://partner.xcures.com)

Progress indicator included (tqdm if available, else fallback).
"""

from __future__ import annotations

import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

try:
    from tqdm import tqdm  # type: ignore
except Exception:
    tqdm = None

SCRIPT_BUILD = "2026-01-12-non-null-create-payload"
DEFAULT_BASE_URL = "https://partner.xcures.com"
DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_MAX_RETRIES = 5
DEFAULT_BACKOFF_SECONDS = 1.0
VERBOSE = True


def utc_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(msg: str) -> None:
    if VERBOSE:
        print(f"[{utc_ts()}] {msg}", file=sys.stderr)


def body_preview(text: str, limit: int = 1200) -> str:
    t = (text or "").strip().replace("\n", " ")
    return t if len(t) <= limit else t[:limit] + "...<truncated>"


def pretty(obj: Any) -> str:
    return json.dumps(obj, indent=2, sort_keys=True)


def prompt(msg: str, default: Optional[str] = None) -> str:
    if default is not None and default != "":
        raw = input(f"{msg} [{default}]: ").strip()
        return raw if raw else default
    return input(f"{msg}: ").strip()


def prompt_yes_no(msg: str) -> bool:
    raw = input(f"{msg} (Y/N): ").strip().lower()
    return raw in ("y", "yes")


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


def request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    *,
    json_body: Optional[Dict[str, Any]] = None,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    max_retries: int = DEFAULT_MAX_RETRIES,
    backoff_seconds: float = DEFAULT_BACKOFF_SECONDS,
) -> requests.Response:
    last_resp: Optional[requests.Response] = None
    last_exc: Optional[BaseException] = None

    for attempt in range(1, max_retries + 1):
        try:
            log(f"{method} {url} attempt={attempt}/{max_retries} timeout={timeout}s")
            resp = session.request(
                method,
                url,
                headers=auth_headers(),
                json=json_body,
                timeout=timeout,
            )
            last_resp = resp

            if 200 <= resp.status_code < 300:
                return resp

            if resp.status_code in (429, 500, 502, 503, 504):
                log(f"HTTP {resp.status_code} retryable. body={body_preview(resp.text, 400)}")
                time.sleep(backoff_seconds * (2 ** (attempt - 1)))
                continue

            raise RuntimeError(f"HTTP {resp.status_code} {url} body={body_preview(resp.text)}")

        except (requests.Timeout, requests.ConnectionError, requests.RequestException) as e:
            last_exc = e
            log(f"Request error (no HTTP response): {type(e).__name__}: {e}")
            time.sleep(backoff_seconds * (2 ** (attempt - 1)))

    if last_resp is not None:
        raise RuntimeError(
            f"request_with_retry exhausted; last_status={last_resp.status_code} url={url} body={body_preview(last_resp.text)}"
        )
    raise RuntimeError(
        f"request_with_retry exhausted; no HTTP response received; url={url} last_error={type(last_exc).__name__ if last_exc else 'unknown'}: {last_exc}"
    )


def parse_json(resp: requests.Response) -> Any:
    try:
        return resp.json()
    except Exception:
        raise RuntimeError(f"Non-JSON response: status={resp.status_code} body={body_preview(resp.text)}")


def progress_iter(total: int, desc: str):
    if tqdm is not None:
        for _ in tqdm(range(total), desc=desc, unit="step"):
            yield
        return

    bar_width = 30
    for i in range(1, total + 1):
        progress = i / total
        filled = int(bar_width * progress)
        bar = "█" * filled + "-" * (bar_width - filled)
        print(f"\r{desc}: |{bar}| {progress*100:6.2f}% ({i}/{total})", end="", flush=True)
        yield
    print()


# Create schema fields (excluding id/name)
CREATE_SCHEMA_FIELDS: List[str] = [
    "lifeomicProjectId",
    "createLifeomicSubjects",
    "addressLookup",
    "defaultQueryType",
    "ccdaToFhirStrategy",
    "requesterId",
    "requesterFullName",
    "requesterRoleCode",
    "requesterPurposeOfUseCode",
    "requesterOrganizationId",
    "requesterNpi",
    "requesterTin",
    "authorizingOrganizationId",
    "automatedTreatmentEncounter",
    "subjectSummaryEnabled",
    "documentSearchEnabled",
    "governance",
    "limitDocumentExtractionByDate",
    "generationDocumentDateExtentDays",
    "autoTriggerDocumentExtraction",
    "questionAnsweringAiEnabled",
    "enabledExtractionSchemaIds",
    "yearsToDownload",
    "downloadBadDates",
]

LIST_KEYS = {"governance", "enabledExtractionSchemaIds"}

# Types for normalization
STRING_KEYS = {
    "lifeomicProjectId",
    "defaultQueryType",
    "ccdaToFhirStrategy",
    "requesterId",
    "requesterFullName",
    "requesterRoleCode",
    "requesterPurposeOfUseCode",
    "requesterOrganizationId",
    "requesterNpi",
    "requesterTin",
    "authorizingOrganizationId",
}
BOOL_KEYS = {
    "createLifeomicSubjects",
    "addressLookup",
    "automatedTreatmentEncounter",
    "subjectSummaryEnabled",
    "documentSearchEnabled",
    "limitDocumentExtractionByDate",
    "autoTriggerDocumentExtraction",
    "questionAnsweringAiEnabled",
    "downloadBadDates",
}
INT_KEYS = {"generationDocumentDateExtentDays", "yearsToDownload"}

# Defaults chosen to match your known-working schema shape
DEFAULTS_BOOL: Dict[str, bool] = {
    "createLifeomicSubjects": True,
    "addressLookup": True,
    "automatedTreatmentEncounter": True,
    "subjectSummaryEnabled": True,
    "documentSearchEnabled": True,
    "limitDocumentExtractionByDate": True,
    "autoTriggerDocumentExtraction": True,
    "questionAnsweringAiEnabled": True,
    "downloadBadDates": True,
}
DEFAULTS_INT: Dict[str, int] = {
    "generationDocumentDateExtentDays": 0,
    "yearsToDownload": 7,
}


def normalize_string_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v) for v in value]
    if isinstance(value, str):
        return [value]
    return [str(value)]


def find_project_in_list(projects: Any, project_id: str) -> Dict[str, Any]:
    if not isinstance(projects, list):
        raise RuntimeError(f"Unexpected /project list response type: {type(projects)}")
    for p in projects:
        if isinstance(p, dict) and str(p.get("id", "")).lower() == project_id.lower():
            return p
    raise RuntimeError(f"Project id not found in GET /api/patient-registry/project list: {project_id}")


def pick_template_project(projects: List[Dict[str, Any]], exclude_project_id: str) -> Dict[str, Any]:
    best: Optional[Dict[str, Any]] = None
    best_score = -1
    for p in projects:
        if not isinstance(p, dict):
            continue
        if str(p.get("id", "")).lower() == exclude_project_id.lower():
            continue
        score = 0
        for k in CREATE_SCHEMA_FIELDS:
            if k in p:
                score += 1
                if p.get(k) is not None:
                    score += 1
        if score > best_score:
            best_score = score
            best = p
    if best is None:
        return find_project_in_list(projects, exclude_project_id)
    return best


def merge_detail_over_list(list_item: Dict[str, Any], detail: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(list_item)
    for k, v in detail.items():
        if k not in merged:
            merged[k] = v
            continue
        if v is not None:
            merged[k] = v
    return merged


def fill_absent_keys_from_template(source: Dict[str, Any], template: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
    filled = dict(source)
    filled_keys: List[str] = []
    for k in CREATE_SCHEMA_FIELDS:
        if k not in filled and k in template:
            filled[k] = template.get(k)
            filled_keys.append(k)
    return filled, filled_keys


def build_create_payload_raw(source: Dict[str, Any], new_id: str, new_name: str) -> Tuple[Dict[str, Any], List[str]]:
    """
    Build payload with raw values (including None), only failing if keys are absent.
    """
    payload: Dict[str, Any] = {"id": new_id, "name": new_name}
    missing: List[str] = []
    for key in CREATE_SCHEMA_FIELDS:
        if key not in source:
            missing.append(key)
            continue
        if key in LIST_KEYS:
            payload[key] = source.get(key)
        else:
            payload[key] = source.get(key)
    return payload, sorted(set(missing))


def normalize_payload_non_null(payload: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
    """
    Normalize the payload so it matches the create schema shape and avoids nulls
    for fields that frequently cause server-side 500s.

    Returns (normalized_payload, normalized_fields) where normalized_fields is a list
    of keys that were changed.
    """
    normalized = dict(payload)
    changed: List[str] = []

    for k in CREATE_SCHEMA_FIELDS:
        if k not in normalized:
            continue

        v = normalized.get(k)

        if k in LIST_KEYS:
            if v is None:
                normalized[k] = []
                changed.append(k)
            else:
                normalized[k] = normalize_string_list(v)
                # normalize_string_list always returns list[str]; if input wasn't already that, mark changed
                if not isinstance(v, list) or any(not isinstance(x, str) for x in v):
                    changed.append(k)
            continue

        if k in BOOL_KEYS:
            if v is None:
                normalized[k] = DEFAULTS_BOOL.get(k, False)
                changed.append(k)
            elif not isinstance(v, bool):
                # Coerce common truthy/falsey strings/numbers
                if isinstance(v, str):
                    vv = v.strip().lower()
                    normalized[k] = vv in ("1", "true", "t", "yes", "y")
                else:
                    normalized[k] = bool(v)
                changed.append(k)
            continue

        if k in INT_KEYS:
            if v is None:
                normalized[k] = DEFAULTS_INT.get(k, 0)
                changed.append(k)
            elif not isinstance(v, int):
                try:
                    normalized[k] = int(v)
                    changed.append(k)
                except Exception:
                    normalized[k] = DEFAULTS_INT.get(k, 0)
                    changed.append(k)
            continue

        if k in STRING_KEYS:
            if v is None:
                normalized[k] = ""
                changed.append(k)
            elif not isinstance(v, str):
                normalized[k] = str(v)
                changed.append(k)
            continue

        # Any other fields: leave as-is

    return normalized, sorted(set(changed))


def summarize_copied_settings(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in payload.items() if k not in ("id", "name")}


def main() -> int:
    print(f"duplicate_project.py build: {SCRIPT_BUILD}")
    try:
        get_bearer_token()
    except Exception as e:
        print(f"Auth error: {e}", file=sys.stderr)
        return 2

    base_url = os.environ.get("XCURES_BASE_URL", DEFAULT_BASE_URL).rstrip("/")

    source_project_id = prompt("Enter source projectId")
    if not source_project_id:
        print("projectId is required.", file=sys.stderr)
        return 2

    prog = progress_iter(3, "Duplicate project")

    with requests.Session() as session:
        # Step 1: list all projects
        next(prog)
        list_url = f"{base_url}/api/patient-registry/project"
        list_resp = request_with_retry(session, "GET", list_url)
        projects_raw = parse_json(list_resp)
        if not isinstance(projects_raw, list):
            raise RuntimeError(f"Unexpected /project list response type: {type(projects_raw)}")
        projects: List[Dict[str, Any]] = [p for p in projects_raw if isinstance(p, dict)]

        source_list_item = find_project_in_list(projects, source_project_id)
        template_project = pick_template_project(projects, exclude_project_id=source_project_id)

        # Step 2: detail endpoint
        next(prog)
        detail_url = f"{base_url}/api/patient-registry/project/{source_project_id}"
        detail_resp = request_with_retry(session, "GET", detail_url)
        detail = parse_json(detail_resp)
        if not isinstance(detail, dict):
            raise RuntimeError(f"Unexpected /project/{{id}} response type: {type(detail)}")

        # Merge + template fill (for absent keys)
        merged = merge_detail_over_list(source_list_item, detail)
        merged, filled_from_template = fill_absent_keys_from_template(merged, template_project)

        # Prompt only for new name
        src_name = str(merged.get("name") or "").strip()
        default_new_name = (src_name + " (Copy)").strip() if src_name else None
        new_name = prompt("New project name", default=default_new_name)
        if not new_name:
            print("New project name is required.", file=sys.stderr)
            return 2

        new_project_id = str(uuid.uuid4())

        # Build raw payload, ensure keys exist
        raw_payload, missing_keys = build_create_payload_raw(merged, new_project_id, new_name)
        if missing_keys:
            print("\n!!! Cannot proceed. These required fields are missing as KEYS after list/detail/template:\n", file=sys.stderr)
            for k in missing_keys:
                print(f" - {k}", file=sys.stderr)
            return 2

        # Normalize to avoid nulls and match create schema expectations
        normalized_payload, normalized_fields = normalize_payload_non_null(raw_payload)

        # Review
        print("\n=== Project Duplication Review ===")
        print(f"Source projectId:   {source_project_id}")
        print(f"Source name:        {src_name or '(unknown)'}")
        print(f"Template project:   {template_project.get('name', '(unknown)')} ({template_project.get('id','')})")
        if filled_from_template:
            print(f"Keys filled from template ({len(filled_from_template)}): {', '.join(sorted(filled_from_template))}")
        else:
            print("Keys filled from template: (none)")
        if normalized_fields:
            print(f"Keys normalized to non-null/schema shape ({len(normalized_fields)}): {', '.join(normalized_fields)}")
        else:
            print("Keys normalized to non-null/schema shape: (none)")
        print(f"New projectId:      {new_project_id}")
        print(f"New name:           {new_name}")

        print("\n--- POST payload (raw, before normalization) ---\n")
        print(pretty(raw_payload))

        print("\n--- POST payload (final, normalized) ---\n")
        print(pretty(normalized_payload))

        if not prompt_yes_no("\nProceed with project creation using the FINAL normalized payload?"):
            print("Cancelled.")
            return 0

        # Step 3: POST create project
        next(prog)
        post_url = f"{base_url}/api/patient-registry/project"
        post_resp = request_with_retry(session, "POST", post_url, json_body=normalized_payload)
        created = parse_json(post_resp)

    print("\n=== Created Project Response ===\n")
    print(pretty(created))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
