#!/usr/bin/env python3
"""
duplicate_project.py

Duplicate an xCures Patient Registry project by copying settings from an existing project.

Flow:
1) Read source projectId (CLI arg or prompt)
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
6) Read new project name (CLI arg or prompt)
7) Show review screen (including which fields were defaulted/normalized)
8) POST /api/patient-registry/project

CLI arguments supported for UI/non-interactive execution.
Auth:
- XCURES_CLIENT_ID / XCURES_CLIENT_SECRET environment variables required
Optional:
- BASE_URL (or XCURES_BASE_URL) override (defaults to https://partner.xcures.com)

Progress indicator included (tqdm if available, else fallback).
"""

from __future__ import annotations

import json
import argparse
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from xcures_toolkit.api_common import (
    DEFAULT_BACKOFF_SECONDS,
    DEFAULT_TIMEOUT_SECONDS,
    parse_json_or_raise,
    request_with_retry as common_request_with_retry,
)
from xcures_toolkit.progress_common import progress_iter
from xcures_toolkit.auth_common import build_json_headers, get_xcures_bearer_token, load_env_file

SCRIPT_BUILD = "2026-03-18-create-raw-first-with-projectid-header"
DEFAULT_BASE_URL = "https://partner.xcures.com"
VERBOSE = True

load_env_file(Path(__file__).resolve().parent.parent / ".env")


def utc_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(msg: str) -> None:
    if VERBOSE:
        print(f"[{utc_ts()}] {msg}", file=sys.stderr)


def pretty(obj: Any) -> str:
    return json.dumps(obj, indent=2, sort_keys=True)


def prompt(msg: str, default: Optional[str] = None) -> str:
    if default is not None and default != "":
        raw = input(f"{msg} [{default}]: ").strip()
        return raw if raw else default
    return input(f"{msg}: ").strip()


def get_bearer_token() -> str:
    # Prefer a run-scoped bearer token injected by the UI for Internal API scripts.
    # Fallback to client-credentials token generation if not provided.
    runtime_token = os.environ.get("XCURES_BEARER_TOKEN", "").strip()
    if runtime_token:
        return runtime_token
    return get_xcures_bearer_token(timeout_seconds=DEFAULT_TIMEOUT_SECONDS)


def auth_headers(*, project_id_header: Optional[str] = None) -> Dict[str, str]:
    return build_json_headers(
        bearer_token=get_bearer_token(),
        project_id=project_id_header or None,
    )


def request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    *,
    json_body: Optional[Dict[str, Any]] = None,
    project_id_header: Optional[str] = None,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    backoff_seconds: float = DEFAULT_BACKOFF_SECONDS,
) -> requests.Response:
    return common_request_with_retry(
        session,
        method,
        url,
        headers=auth_headers(project_id_header=project_id_header),
        json_body=json_body,
        timeout_seconds=timeout,
        backoff_seconds=backoff_seconds,
        logger=log,
    )


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
    "limitDocumentExtractionByDate": False,
    "autoTriggerDocumentExtraction": True,
    "questionAnsweringAiEnabled": True,
    "downloadBadDates": True,
}
DEFAULTS_INT: Dict[str, int] = {
    "generationDocumentDateExtentDays": 365,
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
        if (k not in filled or filled.get(k) is None) and k in template:
            filled[k] = template.get(k)
            filled_keys.append(k)
    return filled, filled_keys


def build_non_null_donor_values(projects: List[Dict[str, Any]]) -> Dict[str, Any]:
    donor_values: Dict[str, Any] = {}
    for project in projects:
        if not isinstance(project, dict):
            continue
        for key in CREATE_SCHEMA_FIELDS:
            if key in donor_values:
                continue
            value = project.get(key)
            if value is None:
                continue
            if key in STRING_KEYS and str(value).strip() == "":
                continue
            if key in LIST_KEYS and isinstance(value, list) and len(value) == 0:
                continue
            donor_values[key] = value
    return donor_values


def fill_from_donor_values(source: Dict[str, Any], donor_values: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
    filled = dict(source)
    filled_keys: List[str] = []
    for key in CREATE_SCHEMA_FIELDS:
        if key not in donor_values:
            continue
        current = filled.get(key)
        should_fill = current is None
        if key in STRING_KEYS and isinstance(current, str) and current.strip() == "":
            should_fill = True
        if key in LIST_KEYS and isinstance(current, list) and len(current) == 0:
            should_fill = True
        if should_fill:
            filled[key] = donor_values[key]
            filled_keys.append(key)
    return filled, sorted(set(filled_keys))


def find_project_by_name(projects: List[Dict[str, Any]], name: str, *, exclude_project_id: str = "") -> Optional[Dict[str, Any]]:
    target = str(name or "").strip().lower()
    if not target:
        return None
    excluded = str(exclude_project_id or "").strip().lower()
    for project in projects:
        if not isinstance(project, dict):
            continue
        project_id = str(project.get("id") or "").strip().lower()
        if excluded and project_id == excluded:
            continue
        project_name = str(project.get("name") or "").strip().lower()
        if project_name == target:
            return project
    return None


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

    # Backend appears sensitive to this combination.
    if bool(normalized.get("limitDocumentExtractionByDate")) and int(normalized.get("generationDocumentDateExtentDays", 0)) <= 0:
        normalized["generationDocumentDateExtentDays"] = max(1, DEFAULTS_INT.get("generationDocumentDateExtentDays", 365))
        changed.append("generationDocumentDateExtentDays")

    return normalized, sorted(set(changed))


def validate_payload_before_apply(payload: Dict[str, Any]) -> List[str]:
    issues: List[str] = []
    required_non_empty_string_keys = [
        "name",
        "ccdaToFhirStrategy",
        "requesterId",
        "requesterFullName",
        "requesterRoleCode",
        "requesterPurposeOfUseCode",
        "requesterOrganizationId",
    ]
    for key in required_non_empty_string_keys:
        value = str(payload.get(key) or "").strip()
        if not value:
            issues.append(f"{key} is empty")

    for key in ("governance", "enabledExtractionSchemaIds"):
        value = payload.get(key)
        if not isinstance(value, list) or len(value) == 0:
            issues.append(f"{key} must be a non-empty list")

    if bool(payload.get("limitDocumentExtractionByDate")):
        try:
            days = int(payload.get("generationDocumentDateExtentDays", 0))
        except Exception:
            days = 0
        if days <= 0:
            issues.append("generationDocumentDateExtentDays must be > 0 when limitDocumentExtractionByDate is true")

    return issues


def summarize_copied_settings(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in payload.items() if k not in ("id", "name")}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Duplicate an xCures Patient Registry project by copying settings from an existing project.",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Preview payload and skip project creation (default).")
    mode.add_argument("--apply", action="store_true", help="Create the project via POST.")
    parser.add_argument(
        "--source-project-id",
        default="",
        help="Source project id to duplicate. If omitted, script prompts for it.",
    )
    parser.add_argument(
        "--new-project-name",
        default="",
        help="Name for the new project. If omitted, script prompts for it.",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose stderr logs.")
    return parser.parse_args()


def main() -> int:
    global VERBOSE

    args = parse_args()
    VERBOSE = bool(args.verbose)
    if not args.apply and not args.dry_run:
        args.dry_run = True

    print(f"duplicate_project.py build: {SCRIPT_BUILD}")
    try:
        get_bearer_token()
    except Exception as e:
        print(f"Auth error: {e}", file=sys.stderr)
        return 2

    base_url = (
        os.environ.get("BASE_URL")
        or os.environ.get("XCURES_BASE_URL")
        or DEFAULT_BASE_URL
    ).rstrip("/")

    source_project_id = args.source_project_id.strip() or prompt("Enter source projectId")
    if not source_project_id:
        print("projectId is required.", file=sys.stderr)
        return 2

    total_steps = 2 if args.dry_run else 3
    prog = progress_iter(range(total_steps), desc="Duplicate project", total=total_steps, unit="step")

    with requests.Session() as session:
        # Step 1: list all projects
        next(prog)
        list_url = f"{base_url}/api/patient-registry/project"
        list_resp = request_with_retry(session, "GET", list_url)
        projects_raw = parse_json_or_raise(list_resp)
        if not isinstance(projects_raw, list):
            raise RuntimeError(f"Unexpected /project list response type: {type(projects_raw)}")
        projects: List[Dict[str, Any]] = [p for p in projects_raw if isinstance(p, dict)]

        source_list_item = find_project_in_list(projects, source_project_id)
        template_project = pick_template_project(projects, exclude_project_id=source_project_id)

        # Step 2: detail endpoint
        next(prog)
        detail_url = f"{base_url}/api/patient-registry/project/{source_project_id}"
        detail_resp = request_with_retry(session, "GET", detail_url)
        detail = parse_json_or_raise(detail_resp)
        if not isinstance(detail, dict):
            raise RuntimeError(f"Unexpected /project/{{id}} response type: {type(detail)}")

        # Merge + template fill (for absent keys)
        merged = merge_detail_over_list(source_list_item, detail)
        merged, filled_from_template = fill_absent_keys_from_template(merged, template_project)
        donor_values = build_non_null_donor_values([template_project, detail, source_list_item, *projects])
        merged, filled_from_donors = fill_from_donor_values(merged, donor_values)

        # Prompt only for new name
        src_name = str(merged.get("name") or "").strip()
        default_new_name = (src_name + " (Copy)").strip() if src_name else None
        new_name = args.new_project_name.strip() or prompt("New project name", default=default_new_name)
        if not new_name:
            print("New project name is required.", file=sys.stderr)
            return 2
        existing_name_match = find_project_by_name(projects, new_name, exclude_project_id=source_project_id)
        if existing_name_match:
            print(
                "Refusing to create project: project name already exists "
                f"({existing_name_match.get('name')} / {existing_name_match.get('id')}).",
                file=sys.stderr,
            )
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
        total_filled = sorted(set(filled_from_template + filled_from_donors))
        if total_filled:
            print(f"Keys filled from template/donor projects ({len(total_filled)}): {', '.join(total_filled)}")
        else:
            print("Keys filled from template/donor projects: (none)")
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

        if args.dry_run:
            print("\nDry-run mode selected. Skipping POST /api/patient-registry/project.")
            print("No project was created.")
            return 0

        apply_issues = validate_payload_before_apply(normalized_payload)
        if apply_issues:
            print("\nRefusing apply: payload likely invalid for create endpoint.", file=sys.stderr)
            for issue in apply_issues:
                print(f" - {issue}", file=sys.stderr)
            return 2

        # Step 3: POST create project
        next(prog)
        post_url = f"{base_url}/api/patient-registry/project"
        post_project_id_header = (
            os.environ.get("XCURES_PROJECT_ID", "").strip() or source_project_id
        )
        post_errors: List[str] = []
        created: Optional[Any] = None

        post_attempts: List[Tuple[str, Dict[str, Any]]] = [("raw", raw_payload)]
        if raw_payload != normalized_payload:
            post_attempts.append(("normalized", normalized_payload))

        for label, candidate_payload in post_attempts:
            try:
                log(f"POST create attempt using {label} payload (ProjectId header={post_project_id_header})")
                post_resp = request_with_retry(
                    session,
                    "POST",
                    post_url,
                    json_body=candidate_payload,
                    project_id_header=post_project_id_header,
                )
                created = parse_json_or_raise(post_resp)
                print(f"\nCreate succeeded using {label} payload.")
                break
            except Exception as exc:
                post_errors.append(f"{label}: {exc}")

        if created is None:
            raise RuntimeError(
                "Create project failed for all payload variants. "
                + " | ".join(post_errors)
            )

    print("\n=== Created Project Response ===\n")
    print(pretty(created))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
