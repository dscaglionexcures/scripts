import os
import time
from pathlib import Path

import requests
from xcures_toolkit.api_common import DEFAULT_MAX_RETRIES
from xcures_toolkit.progress_common import progress_bar
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from requests.exceptions import ConnectionError, Timeout
from xcures_toolkit.xcures_client import XcuresApiClient
from xcures_toolkit.auth_common import (
    load_env_file,
    require_env,
)


# ----------------------------
# Load Config
# ----------------------------
load_env_file(Path(__file__).resolve().parent.parent / ".env")

# Prefer internal API base-url env var used across scripts; keep BASE_URL fallback.
BASE_URL = os.getenv(
    "XCURES_BASE_URL",
    os.getenv("BASE_URL", "https://partner.xcures.com"),
).strip()
PROJECT_ID = require_env("XCURES_PROJECT_ID")
INITIAL_BEARER_TOKEN = require_env("XCURES_BEARER_TOKEN")
TOKEN_ROTATE_AFTER_SECONDS = 55 * 60
DOCUMENT_DELAY_SECONDS = float(os.getenv("DOCUMENT_DELAY_SECONDS", "2").strip() or "2")

DOWNLOAD_ROOT = Path("downloads")
DOWNLOAD_ROOT.mkdir(exist_ok=True)

SUBJECTS_ENDPOINT = "/api/patient-registry/subject"
DOCUMENTS_ENDPOINT = "/api/patient-registry/document"


class ManualBearerTokenManager:
    def __init__(self, initial_token: str, rotate_after_seconds: int) -> None:
        self._token = initial_token.strip()
        self._rotate_after_seconds = rotate_after_seconds
        self._updated_at = time.monotonic()

    def _prompt_for_new_token(self, reason: str) -> None:
        print(f"\n{reason}")
        while True:
            new_token = input("Enter new XCURES_BEARER_TOKEN: ").strip()
            if new_token:
                self._token = new_token
                self._updated_at = time.monotonic()
                print("Token updated. Resuming...\n")
                return
            print("Token cannot be blank.")

    def get_token(self, *, force_refresh: bool = False) -> str:
        elapsed = time.monotonic() - self._updated_at
        if force_refresh:
            self._prompt_for_new_token("API returned 401. A new bearer token is required.")
        elif elapsed >= self._rotate_after_seconds:
            mins = int(elapsed // 60)
            self._prompt_for_new_token(
                f"Bearer token has been active for {mins} minutes (rotation threshold: 55)."
            )
        return self._token


class RotatingBearerXcuresApiClient(XcuresApiClient):
    def __init__(self, *, token_manager: ManualBearerTokenManager, **kwargs) -> None:
        super().__init__(**kwargs)
        self._token_manager = token_manager

    def _get_bearer_token(self, *, force_refresh: bool = False) -> str:
        return self._token_manager.get_token(force_refresh=force_refresh)


# ----------------------------
# Build Resilient Session
# ----------------------------
def build_session():
    session = requests.Session()

    retry = Retry(
        total=DEFAULT_MAX_RETRIES,
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
token_manager = ManualBearerTokenManager(
    initial_token=INITIAL_BEARER_TOKEN,
    rotate_after_seconds=TOKEN_ROTATE_AFTER_SECONDS,
)
api_client = RotatingBearerXcuresApiClient(
    session=session,
    token_manager=token_manager,
    base_url=BASE_URL,
    project_id=PROJECT_ID,
    timeout_seconds=120,
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
        SUBJECTS_ENDPOINT,
        page_size=page_size,
        timeout_seconds=60,
    )


# ----------------------------
# Document Search
# ----------------------------
def iter_documents_for_subject(subject_id, page_size=200):
    yield from api_client.iter_paginated(
        DOCUMENTS_ENDPOINT,
        params={"subjectId": subject_id},
        page_size=page_size,
        timeout_seconds=120,
    )


def get_document_details(document_id):
    data = api_client.request_json(
        "GET",
        f"{DOCUMENTS_ENDPOINT}/{document_id}",
        timeout_seconds=120,
    )
    if not isinstance(data, dict):
        raise SystemExit(f"Unexpected document details for {document_id}: {type(data)}")
    return data


def get_document_pdf_url(document_id):
    data = api_client.request_json(
        "GET",
        f"{DOCUMENTS_ENDPOINT}/{document_id}/pdf",
        timeout_seconds=120,
    )
    if not isinstance(data, dict):
        raise SystemExit(f"Unexpected PDF URL payload for {document_id}: {type(data)}")
    return data


def is_xml_document(file_name: str, content_type: str) -> bool:
    file_name_l = (file_name or "").strip().lower()
    content_type_l = (content_type or "").strip().lower()
    return file_name_l.endswith(".xml") or ("xml" in content_type_l)


# ----------------------------
# Download from Signed URL
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


def pause_between_documents() -> None:
    if DOCUMENT_DELAY_SECONDS > 0:
        time.sleep(DOCUMENT_DELAY_SECONDS)


# ----------------------------
# Main
# ----------------------------
def main():
    print("Starting export...")

    subjects = list(iter_subjects())
    print("Subjects found:", len(subjects))

    total_docs_found = 0
    total_docs_downloaded = 0
    total_docs_skipped = 0

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
                            pause_between_documents()
                            continue

                        details = get_document_details(doc_id)
                        file_name = str(details.get("fileName") or f"{doc_id}")
                        content_type = str(details.get("contentType") or "")

                        if is_xml_document(file_name, content_type):
                            try:
                                pdf_meta = get_document_pdf_url(doc_id)
                            except RuntimeError as e:
                                print(f"XML->PDF failed for document {doc_id}: {e}")
                                total_docs_skipped += 1
                                doc_bar.update(1)
                                pause_between_documents()
                                continue
                            signed_url = pdf_meta.get("signedUrl")
                            file_name = str(pdf_meta.get("fileName") or f"{doc_id}.pdf")
                            if not file_name.lower().endswith(".pdf"):
                                file_name = f"{file_name}.pdf"
                        else:
                            signed_url = details.get("signedS3Url")

                        if not signed_url:
                            print(f"No downloadable URL available for document {doc_id}")
                            total_docs_skipped += 1
                            doc_bar.update(1)
                            pause_between_documents()
                            continue

                        subject_folder = DOWNLOAD_ROOT / subject_id
                        subject_folder.mkdir(exist_ok=True)

                        output_path = subject_folder / file_name

                        download_file(signed_url, output_path)

                        total_docs_downloaded += 1
                        doc_bar.update(1)
                        pause_between_documents()

            subj_bar.update(1)

    print("\n-----------------------------------------")
    print("Processing Complete")
    print("Total Documents Found:", total_docs_found)
    print("Total Documents Downloaded:", total_docs_downloaded)
    print("Total Documents Skipped:", total_docs_skipped)
    print("-----------------------------------------")


if __name__ == "__main__":
    main()
