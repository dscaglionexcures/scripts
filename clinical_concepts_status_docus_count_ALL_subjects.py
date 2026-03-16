# Use this script to gather a list of all subjects in a project and display how many documents each subject has
# as well as the Clinical Concepts Status for each subject
"""
fetch_subjects_and_statuses.py

Progress bars for:
1) Fetching subject IDs via paginated Search Subjects
2) Processing each subject:
   - clinical concepts loaded status
   - documents totalCount

Writes:
- SUBJECTS_CSV_PATH: subjectId list as discovered
- RESULTS_CSV_PATH: realtime results with loaded + documentTotalCount
"""

import argparse
import csv
import math
import os
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from api_common import DEFAULT_MAX_RETRIES
from progress_common import progress_bar
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from auth_common import (
    build_json_headers,
    fetch_client_credentials_token,
    load_env_file,
)


# ============================================================
# HARD-CODED CONFIGURATION
# ============================================================

BASE_URL = "https://partner.xcures.com"

load_env_file(Path(__file__).resolve().parent / ".env")

BASE_URL = os.getenv("BASE_URL", BASE_URL).strip()
PROJECT_ID = (
    os.getenv("XCURES_PROJECT_ID")
    or "f5dcc615-89f1-4e0e-b886-78a105f94f86"
).strip()
CLIENT_ID = (os.getenv("XCURES_CLIENT_ID") or "").strip()
CLIENT_SECRET = (os.getenv("XCURES_CLIENT_SECRET") or "").strip()
OUTPUT_DIR = (Path(__file__).resolve().parent / "downloads")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
SUBJECTS_CSV_PATH = str(OUTPUT_DIR / "all_subject_ids.csv")
RESULTS_CSV_PATH = str(OUTPUT_DIR / "subject_clinical_concepts_and_doc_counts.csv")

REQUEST_TIMEOUT_SECONDS = 30
MAX_WORKERS = 10

SUBJECT_PAGE_SIZE = 200

DOCUMENT_PAGE_NUMBER = 1
DOCUMENT_PAGE_SIZE = 1

DOCUMENT_SUBJECT_FILTER_PARAM = "subjectId"

# ============================================================


TOKEN_ENDPOINT = "/oauth/token"
SEARCH_SUBJECTS_ENDPOINT = "/api/v1/patient-registry/subject"
CLINICAL_CONCEPTS_STATUS_ENDPOINT = (
    "/api/v1/patient-registry/subject/{subject_id}/status/clinical-concepts"
)
DOCUMENT_SEARCH_ENDPOINT = "/api/v1/patient-registry/document"


@dataclass
class SubjectResult:
    subject_id: str

    loaded: Optional[bool]
    loaded_http_status: Optional[int]
    loaded_error: Optional[str]

    document_total_count: Optional[int]
    document_http_status: Optional[int]
    document_error: Optional[str]


def build_session() -> requests.Session:
    session = requests.Session()

    retry = Retry(
        total=DEFAULT_MAX_RETRIES,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
        respect_retry_after_header=True,
    )

    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def fetch_token(session: requests.Session) -> str:
    if not CLIENT_ID or not CLIENT_SECRET:
        raise RuntimeError(
            "Missing client credentials. Set XCURES_CLIENT_ID and XCURES_CLIENT_SECRET."
        )
    return fetch_client_credentials_token(
        session,
        auth_url=BASE_URL + TOKEN_ENDPOINT,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        timeout_seconds=REQUEST_TIMEOUT_SECONDS,
    )


def build_headers(token: str, project_id: str) -> Dict[str, str]:
    return build_json_headers(bearer_token=token, project_id=project_id)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect clinical concepts status and document counts for all subjects."
    )
    parser.add_argument(
        "--project-id",
        default="",
        help="Optional project ID override. Defaults to XCURES_PROJECT_ID from environment/profile.",
    )
    return parser.parse_args()


def _extract_total_count(payload: Any) -> Optional[int]:
    if isinstance(payload, dict):
        v = payload.get("totalCount")
        if isinstance(v, int):
            return v
    return None


def _extract_subject_ids_from_payload(payload: Any) -> List[str]:
    """
    [Unverified] Attempts common response shapes:
    - payload is list
    - payload has keys: items, results, subjects, data -> list

    Each item tries id keys: id, subjectId, subject_id
    """
    items = None
    if isinstance(payload, list):
        items = payload
    elif isinstance(payload, dict):
        for key in ("items", "results", "subjects", "data"):
            v = payload.get(key)
            if isinstance(v, list):
                items = v
                break

    if not isinstance(items, list):
        raise RuntimeError(
            "Could not locate list of subjects in response payload. "
            "Expected a list or a dict with one of keys: items, results, subjects, data."
        )

    subject_ids: List[str] = []
    for obj in items:
        if not isinstance(obj, dict):
            continue
        for k in ("id", "subjectId", "subject_id"):
            v = obj.get(k)
            if isinstance(v, str) and v.strip():
                subject_ids.append(v.strip())
                break

    return subject_ids


def fetch_all_subject_ids_with_progress(
    session: requests.Session,
    headers: Dict[str, str],
    subjects_csv_path: str,
) -> List[str]:
    subject_ids: List[str] = []
    total_expected: Optional[int] = None

    with open(subjects_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["subjectId"])
        writer.writeheader()
        f.flush()

        page_number = 1

        # Create a progress bar for pages. If we learn totalCount, we can set a total pages count.
        pbar = progress_bar(desc="Fetching subject IDs", unit="page", total=None)

        try:
            while True:
                url = BASE_URL + SEARCH_SUBJECTS_ENDPOINT
                params = {"pageNumber": page_number, "pageSize": SUBJECT_PAGE_SIZE}

                resp = session.get(
                    url, headers=headers, params=params, timeout=REQUEST_TIMEOUT_SECONDS
                )
                if resp.status_code != 200:
                    if resp.status_code == 403:
                        raise RuntimeError(
                            "Search Subjects returned HTTP 403. "
                            "The selected projectId is visible in Internal API but not accessible "
                            "to the current Public API client credentials/profile."
                        )
                    raise RuntimeError(
                        f"Search Subjects failed on page {page_number} "
                        f"(HTTP {resp.status_code}): {resp.text}"
                    )

                payload = resp.json()

                # If this response contains totalCount, configure expected page count once
                if total_expected is None:
                    total_expected = _extract_total_count(payload)
                    if total_expected is not None:
                        total_pages = max(1, math.ceil(total_expected / SUBJECT_PAGE_SIZE))
                        pbar.total = total_pages
                        if hasattr(pbar, "refresh"):
                            pbar.refresh()

                page_subject_ids = _extract_subject_ids_from_payload(payload)

                # Update progress bar for this page fetch
                pbar.update(1)

                if not page_subject_ids:
                    break

                for sid in page_subject_ids:
                    subject_ids.append(sid)
                    writer.writerow({"subjectId": sid})
                f.flush()

                # Stop conditions
                if len(page_subject_ids) < SUBJECT_PAGE_SIZE:
                    break
                if total_expected is not None and len(subject_ids) >= total_expected:
                    break

                page_number += 1
        finally:
            pbar.close()

    # De-dup while preserving order
    seen = set()
    deduped: List[str] = []
    for sid in subject_ids:
        if sid not in seen:
            seen.add(sid)
            deduped.append(sid)

    return deduped


def get_clinical_concepts_loaded(
    session: requests.Session,
    headers: Dict[str, str],
    subject_id: str,
) -> Tuple[Optional[bool], Optional[int], Optional[str]]:
    url = BASE_URL + CLINICAL_CONCEPTS_STATUS_ENDPOINT.format(subject_id=subject_id)

    try:
        resp = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
    except Exception as exc:
        return None, None, str(exc)

    if resp.status_code != 200:
        return None, resp.status_code, resp.text

    try:
        payload = resp.json()
    except Exception as exc:
        return None, resp.status_code, f"Invalid JSON response: {exc}"

    loaded_value = payload.get("loaded") if isinstance(payload, dict) else None
    if isinstance(loaded_value, bool):
        return loaded_value, resp.status_code, None

    return None, resp.status_code, f"Unexpected response shape: {payload}"


def get_document_total_count_for_subject(
    session: requests.Session,
    headers: Dict[str, str],
    subject_id: str,
) -> Tuple[Optional[int], Optional[int], Optional[str]]:
    url = BASE_URL + DOCUMENT_SEARCH_ENDPOINT

    params = {
        DOCUMENT_SUBJECT_FILTER_PARAM: subject_id,
        "pageNumber": DOCUMENT_PAGE_NUMBER,
        "pageSize": DOCUMENT_PAGE_SIZE,
    }

    try:
        resp = session.get(url, headers=headers, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
    except Exception as exc:
        return None, None, str(exc)

    if resp.status_code != 200:
        return None, resp.status_code, resp.text

    try:
        payload = resp.json()
    except Exception as exc:
        return None, resp.status_code, f"Invalid JSON response: {exc}"

    total_count = payload.get("totalCount") if isinstance(payload, dict) else None
    if isinstance(total_count, int):
        return total_count, resp.status_code, None

    return None, resp.status_code, f"Missing or non-integer totalCount in response: {payload}"


def process_subject(
    session: requests.Session,
    headers: Dict[str, str],
    subject_id: str,
) -> SubjectResult:
    loaded, loaded_status, loaded_err = get_clinical_concepts_loaded(session, headers, subject_id)
    doc_total, doc_status, doc_err = get_document_total_count_for_subject(session, headers, subject_id)

    return SubjectResult(
        subject_id=subject_id,
        loaded=loaded,
        loaded_http_status=loaded_status,
        loaded_error=loaded_err,
        document_total_count=doc_total,
        document_http_status=doc_status,
        document_error=doc_err,
    )


def main() -> int:
    args = parse_args()
    project_id = (args.project_id or PROJECT_ID).strip()
    if not project_id:
        raise RuntimeError("Missing project ID. Set XCURES_PROJECT_ID or provide --project-id.")

    print("Authenticating...")
    session = build_session()
    token = fetch_token(session)
    headers = build_headers(token, project_id)
    print(f"Using projectId={project_id}")

    print("Phase 1/2: Fetching subject IDs (paginated)...")
    subject_ids = fetch_all_subject_ids_with_progress(session, headers, SUBJECTS_CSV_PATH)
    print(f"Fetched {len(subject_ids)} subject IDs.")
    print(f"Subject IDs CSV written to: {SUBJECTS_CSV_PATH}")

    print("Phase 2/2: Querying clinical concepts status and document counts...")
    write_lock = threading.Lock()

    with open(RESULTS_CSV_PATH, "w", newline="", encoding="utf-8") as out_file:
        writer = csv.DictWriter(
            out_file,
            fieldnames=[
                "Subject",
                "# of Documents",
                "Clinical Concepts Loaded",
            ],
        )
        writer.writeheader()
        out_file.flush()

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [
                executor.submit(process_subject, session, headers, sid)
                for sid in subject_ids
            ]

            with progress_bar(total=len(futures), desc="Processing subjects", unit="subject") as pbar:
                for future in as_completed(futures):
                    result = future.result()
                    doc_count_value = (
                        result.document_total_count
                        if result.document_total_count is not None
                        else ""
                    )
                    if isinstance(doc_count_value, int) and doc_count_value == 0:
                        loaded_value = "NO DOCUMENTS"
                    else:
                        loaded_value = result.loaded if result.loaded is not None else ""

                    row = {
                        "Subject": result.subject_id,
                        "# of Documents": doc_count_value,
                        "Clinical Concepts Loaded": loaded_value,
                    }

                    with write_lock:
                        writer.writerow(row)
                        out_file.flush()

                    pbar.update(1)

    print("Done.")
    print(f"Results CSV written to: {RESULTS_CSV_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
