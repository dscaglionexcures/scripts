import os
import time
from pathlib import Path

import requests
from progress_common import progress_bar
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from requests.exceptions import ConnectionError, Timeout
from xcures_client import XcuresApiClient
from auth_common import (
    load_env_file,
    require_env,
)


# ----------------------------
# Load Config
# ----------------------------
load_env_file(Path(__file__).resolve().parent / ".env")

BASE_URL = os.getenv("BASE_URL", "https://partner.xcures.com").strip()
PROJECT_ID = require_env("XCURES_PROJECT_ID")
_ = require_env("XCURES_CLIENT_ID")
_ = require_env("XCURES_CLIENT_SECRET")

DOWNLOAD_ROOT = Path("downloads")
DOWNLOAD_ROOT.mkdir(exist_ok=True)


# ----------------------------
# Build Resilient Session
# ----------------------------
def build_session():
    session = requests.Session()

    retry = Retry(
        total=5,
        backoff_factor=1.5,
        status_forcelist=[502, 503, 504],
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )

    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=20,
        pool_maxsize=20,
    )

    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session


session = build_session()
api_client = XcuresApiClient(
    session=session,
    base_url=BASE_URL,
    project_id=PROJECT_ID,
    timeout_seconds=120,
    max_retries=3,
    backoff_seconds=2.0,
    max_sleep_seconds=6.0,
)


# ----------------------------
# Safe Request Wrapper
# ----------------------------
def safe_request(method, url, **kwargs):
    try:
        return api_client.request(
            method=method,
            path_or_url=url,
            params=kwargs.get("params"),
            json_body=kwargs.get("json"),
            timeout_seconds=kwargs.get("timeout", 60),
        )
    except RuntimeError as e:
        raise SystemExit(f"Request failed: {e}")


# ----------------------------
# Subjects
# ----------------------------
def iter_subjects(page_size=200):
    yield from api_client.iter_paginated(
        "/api/v1/patient-registry/subject",
        page_size=page_size,
        timeout_seconds=60,
    )


# ----------------------------
# Document Search
# ----------------------------
def iter_documents_for_subject(subject_id, page_size=200):
    yield from api_client.iter_paginated(
        "/api/v1/patient-registry/document",
        params={"subjectId": subject_id},
        page_size=page_size,
        timeout_seconds=120,
    )


# ----------------------------
# Document Details (contains signedS3Url)
# ----------------------------
def get_document_details(document_id):
    data = api_client.request_json(
        "GET",
        f"/api/v1/patient-registry/document/{document_id}",
        timeout_seconds=120,
    )
    if not isinstance(data, dict):
        raise SystemExit(f"Unexpected document details for {document_id}: {type(data)}")
    return data


# ----------------------------
# Download from Signed S3 URL
# ----------------------------
def download_file(signed_url, output_path: Path):
    for attempt in range(3):
        try:
            with session.get(signed_url, stream=True, timeout=300) as resp:
                if not resp.ok:
                    raise SystemExit(
                        f"S3 download failed: {resp.status_code} {resp.text}"
                    )

                with open(output_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)

            return

        except (ConnectionError, Timeout):
            print(f"S3 connection reset. Retry {attempt + 1}/3...")
            time.sleep(2 * (attempt + 1))

    raise SystemExit("S3 download failed after retries.")


# ----------------------------
# Main
# ----------------------------
def main():
    print("Starting export...")

    subjects = list(iter_subjects())
    print("Subjects found:", len(subjects))

    total_docs_found = 0
    total_docs_downloaded = 0

    with progress_bar(total=len(subjects), desc="Patients Processed", unit="patient") as subj_bar:
        for subject in subjects:
            subject_id = subject.get("id")
            if not subject_id:
                subj_bar.update(1)
                continue

            documents = list(iter_documents_for_subject(subject_id))
            total_docs_found += len(documents)

            if documents:
                with progress_bar(
                    total=len(documents),
                    desc=f"Docs for {subject_id[:8]}",
                    unit="doc",
                    leave=False,
                ) as doc_bar:

                    for doc in documents:
                        doc_id = doc.get("id")
                        if not doc_id:
                            doc_bar.update(1)
                            continue

                        details = get_document_details(doc_id)
                        signed_url = details.get("signedS3Url")
                        file_name = details.get("fileName") or f"{doc_id}"

                        if not signed_url:
                            print(f"No signedS3Url for document {doc_id}")
                            doc_bar.update(1)
                            continue

                        subject_folder = DOWNLOAD_ROOT / subject_id
                        subject_folder.mkdir(exist_ok=True)

                        output_path = subject_folder / file_name

                        download_file(signed_url, output_path)

                        total_docs_downloaded += 1
                        doc_bar.update(1)

            subj_bar.update(1)

    print("\n-----------------------------------------")
    print("Processing Complete")
    print("Total Documents Found:", total_docs_found)
    print("Total Documents Downloaded:", total_docs_downloaded)
    print("-----------------------------------------")


if __name__ == "__main__":
    main()
