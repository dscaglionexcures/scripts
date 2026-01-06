#!/usr/bin/env python3
"""
clinical_concepts_status.py

Reads a CSV with a column named XCURESID.

For each subjectId:
1) GET /api/v1/patient-registry/subject/{id}/status/clinical-concepts
   Extracts: loaded (true/false)

2) GET /api/v1/patient-registry/document
   Extracts: totalCount (integer)

Writes results to an output CSV in real time.
"""

import csv
import threading
from dataclasses import dataclass
from typing import List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm


# ============================================================
# HARD-CODED CONFIGURATION
# ============================================================

BASE_URL = "https://partner.xcures.com"

INPUT_CSV_PATH = "/Users/dScaglione/downloads/list.csv"
OUTPUT_CSV_PATH = "/Users/dScaglione/downloads/clinical_concepts_status.csv"

CSV_SUBJECT_ID_COLUMN = "XCURESID"

PROJECT_ID = "f5dcc615-89f1-4e0e-b886-78a105f94f86"

CLIENT_ID = "59adc695-5070-4144-a547-b07c3b9d8d4d"
CLIENT_SECRET = "eaef494901c3ba86de370ee31621c3af3676d8594404dcd31c064dfc9a4791c0"

MAX_WORKERS = 10
REQUEST_TIMEOUT_SECONDS = 30

DOCUMENT_PAGE_NUMBER = 1
DOCUMENT_PAGE_SIZE = 1

# ============================================================


CLINICAL_CONCEPTS_STATUS_ENDPOINT = (
    "/api/v1/patient-registry/subject/{subject_id}/status/clinical-concepts"
)
DOCUMENT_SEARCH_ENDPOINT = "/api/v1/patient-registry/document"
TOKEN_ENDPOINT = "/oauth/token"


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
        total=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )

    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session


def read_subject_ids() -> List[str]:
    subject_ids: List[str] = []

    with open(INPUT_CSV_PATH, newline="", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)

        if not reader.fieldnames or CSV_SUBJECT_ID_COLUMN not in reader.fieldnames:
            raise ValueError(
                f"CSV must contain column '{CSV_SUBJECT_ID_COLUMN}'. "
                f"Found columns: {reader.fieldnames}"
            )

        for row in reader:
            value = (row.get(CSV_SUBJECT_ID_COLUMN) or "").strip()
            if value:
                subject_ids.append(value)

    return subject_ids


def fetch_token(session: requests.Session) -> str:
    response = session.post(
        BASE_URL + TOKEN_ENDPOINT,
        json={
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "grant_type": "client_credentials",
        },
        timeout=REQUEST_TIMEOUT_SECONDS,
    )

    if not (200 <= response.status_code < 300):
        raise RuntimeError(
            f"Token request failed ({response.status_code}): {response.text}"
        )

    data = response.json()
    token = data.get("access_token")

    if not token:
        raise RuntimeError(f"Token response missing access_token: {data}")

    return token


def get_clinical_concepts_loaded(
    session: requests.Session,
    headers: dict,
    subject_id: str,
) -> Tuple[Optional[bool], Optional[int], Optional[str]]:
    url = BASE_URL + CLINICAL_CONCEPTS_STATUS_ENDPOINT.format(subject_id=subject_id)

    try:
        response = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
    except Exception as exc:
        return None, None, str(exc)

    if response.status_code != 200:
        return None, response.status_code, response.text

    try:
        payload = response.json()
    except Exception as exc:
        return None, response.status_code, f"Invalid JSON response: {exc}"

    loaded_value = payload.get("loaded")
    if isinstance(loaded_value, bool):
        return loaded_value, response.status_code, None

    return None, response.status_code, f"Unexpected response shape: {payload}"


def get_document_total_count_for_subject(
    session: requests.Session,
    headers: dict,
    subject_id: str,
) -> Tuple[Optional[int], Optional[int], Optional[str]]:
    url = BASE_URL + DOCUMENT_SEARCH_ENDPOINT

    params = {
        "subjectId": subject_id,  # adjust if API uses a different param name
        "pageNumber": DOCUMENT_PAGE_NUMBER,
        "pageSize": DOCUMENT_PAGE_SIZE,
    }

    try:
        response = session.get(
            url, headers=headers, params=params, timeout=REQUEST_TIMEOUT_SECONDS
        )
    except Exception as exc:
        return None, None, str(exc)

    if response.status_code != 200:
        return None, response.status_code, response.text

    try:
        payload = response.json()
    except Exception as exc:
        return None, response.status_code, f"Invalid JSON response: {exc}"

    total_count = payload.get("totalCount")
    if isinstance(total_count, int):
        return total_count, response.status_code, None

    return None, response.status_code, f"Missing totalCount in response: {payload}"


def process_subject(
    session: requests.Session,
    token: str,
    subject_id: str,
) -> SubjectResult:
    headers = {
        "Authorization": f"Bearer {token}",
        "ProjectId": PROJECT_ID,
        "Accept": "application/json",
    }

    loaded, loaded_status, loaded_err = get_clinical_concepts_loaded(
        session, headers, subject_id
    )

    doc_total, doc_status, doc_err = get_document_total_count_for_subject(
        session, headers, subject_id
    )

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
    session = build_session()
    token = fetch_token(session)
    subject_ids = read_subject_ids()

    write_lock = threading.Lock()

    with open(OUTPUT_CSV_PATH, "w", newline="", encoding="utf-8") as out_file:
        writer = csv.DictWriter(
            out_file,
            fieldnames=[
                "subjectId",
                "loaded",
                "loadedHttpStatus",
                "loadedError",
                "documentTotalCount",
                "documentHttpStatus",
                "documentError",
            ],
        )
        writer.writeheader()
        out_file.flush()

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [
                executor.submit(process_subject, session, token, sid)
                for sid in subject_ids
            ]

            with tqdm(total=len(futures), desc="Processing subjects", unit="subject") as pbar:
                for future in as_completed(futures):
                    result = future.result()

                    with write_lock:
                        writer.writerow(
                            {
                                "subjectId": result.subject_id,
                                "loaded": result.loaded if result.loaded is not None else "",
                                "loadedHttpStatus": result.loaded_http_status or "",
                                "loadedError": result.loaded_error or "",
                                "documentTotalCount": (
                                    result.document_total_count
                                    if result.document_total_count is not None
                                    else ""
                                ),
                                "documentHttpStatus": result.document_http_status or "",
                                "documentError": result.document_error or "",
                            }
                        )
                        out_file.flush()

                    pbar.update(1)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())