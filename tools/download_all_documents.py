import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock, local

import requests
from xcures_toolkit.api_common import DEFAULT_MAX_RETRIES
from xcures_toolkit.progress_common import progress_bar
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from requests.exceptions import ConnectionError, Timeout
from xcures_toolkit.xcures_client import XcuresApiClient
from xcures_toolkit.auth_common import (
    get_xcures_bearer_token,
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
DOCUMENT_DOWNLOAD_WORKERS = int(
    os.getenv("DOCUMENT_DOWNLOAD_WORKERS", "4").strip() or "4"
)

DOWNLOAD_ROOT = Path("downloads")
DOWNLOAD_ROOT.mkdir(exist_ok=True)

SUBJECTS_ENDPOINT = "/api/v1/patient-registry/subject"
DOCUMENTS_ENDPOINT = "/api/v1/patient-registry/document"
INTERNAL_DOCUMENTS_ENDPOINT = "/api/patient-registry/document"


class ManualBearerTokenManager:
    def __init__(self, initial_token: str, rotate_after_seconds: int) -> None:
        self._token = initial_token.strip()
        self._rotate_after_seconds = rotate_after_seconds
        self._updated_at = time.monotonic()
        self._lock = Lock()

    def _refresh_from_client_credentials(self) -> bool:
        try:
            token = get_xcures_bearer_token(force_refresh=True, timeout_seconds=60)
        except Exception:
            return False
        token = str(token or "").strip()
        if not token:
            return False
        self._token = token
        self._updated_at = time.monotonic()
        print("Token refreshed via client credentials. Resuming...\n")
        return True

    def _prompt_for_new_token(self, reason: str) -> None:
        print(f"\n{reason}")

        # Try non-interactive refresh first to avoid blocking UI jobs.
        if self._refresh_from_client_credentials():
            return

        if not sys.stdin.isatty():
            raise RuntimeError(
                "Token refresh required, but this run is non-interactive. "
                "Set XCURES_CLIENT_ID/XCURES_CLIENT_SECRET for auto-refresh, "
                "or update XCURES_BEARER_TOKEN and rerun."
            )

        while True:
            new_token = input("Enter new XCURES_BEARER_TOKEN: ").strip()
            if new_token:
                self._token = new_token
                self._updated_at = time.monotonic()
                print("Token updated. Resuming...\n")
                return
            print("Token cannot be blank.")

    def get_token(self, *, force_refresh: bool = False) -> str:
        with self._lock:
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
public_api_client = XcuresApiClient(
    session=session,
    base_url=BASE_URL,
    project_id=PROJECT_ID,
    timeout_seconds=120,
    backoff_seconds=2.0,
    max_sleep_seconds=6.0,
)
internal_token_manager = ManualBearerTokenManager(
    initial_token=INITIAL_BEARER_TOKEN,
    rotate_after_seconds=TOKEN_ROTATE_AFTER_SECONDS,
)
internal_api_client = RotatingBearerXcuresApiClient(
    session=session,
    token_manager=internal_token_manager,
    base_url=BASE_URL,
    project_id=PROJECT_ID,
    timeout_seconds=120,
    backoff_seconds=2.0,
    max_sleep_seconds=6.0,
)
_thread_state = local()


def _get_worker_clients() -> tuple[XcuresApiClient, RotatingBearerXcuresApiClient, requests.Session]:
    state = getattr(_thread_state, "clients", None)
    if state is not None:
        return state

    thread_session = build_session()
    thread_public_client = XcuresApiClient(
        session=thread_session,
        base_url=BASE_URL,
        project_id=PROJECT_ID,
        timeout_seconds=120,
        backoff_seconds=2.0,
        max_sleep_seconds=6.0,
    )
    thread_internal_client = RotatingBearerXcuresApiClient(
        session=thread_session,
        token_manager=internal_token_manager,
        base_url=BASE_URL,
        project_id=PROJECT_ID,
        timeout_seconds=120,
        backoff_seconds=2.0,
        max_sleep_seconds=6.0,
    )
    state = (thread_public_client, thread_internal_client, thread_session)
    _thread_state.clients = state
    return state


# ----------------------------
# Safe Request Wrapper
# ----------------------------
def safe_request(method, url, **kwargs):
    try:
        return public_api_client.request(
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
    yield from public_api_client.iter_paginated(
        SUBJECTS_ENDPOINT,
        page_size=page_size,
        timeout_seconds=60,
    )


# ----------------------------
# Document Search
# ----------------------------
def iter_documents_for_subject(subject_id, page_size=200):
    yield from public_api_client.iter_paginated(
        DOCUMENTS_ENDPOINT,
        params={"subjectId": subject_id},
        page_size=page_size,
        timeout_seconds=120,
    )


def get_document_details(document_id, *, api_client: XcuresApiClient = public_api_client):
    data = api_client.request_json(
        "GET",
        f"{DOCUMENTS_ENDPOINT}/{document_id}",
        timeout_seconds=120,
    )
    if not isinstance(data, dict):
        raise SystemExit(f"Unexpected document details for {document_id}: {type(data)}")
    return data


def get_document_pdf_url(
    document_id,
    *,
    api_client: RotatingBearerXcuresApiClient = internal_api_client,
):
    # This endpoint requires Internal API bearer auth in this environment.
    # Prefer internal (non-v1) path; fallback to v1 for compatibility.
    try:
        data = api_client.request_json(
            "GET",
            f"{INTERNAL_DOCUMENTS_ENDPOINT}/{document_id}/pdf",
            timeout_seconds=120,
        )
    except RuntimeError as e:
        message = str(e)
        if "HTTP 404 " not in message and "last_status=404" not in message:
            raise
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
def download_file(signed_url, output_path: Path, *, download_session: requests.Session = session):
    for attempt in range(3):
        try:
            with download_session.get(signed_url, stream=True, timeout=300) as resp:
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


def pause_between_documents(delay_seconds: float) -> None:
    if delay_seconds > 0:
        time.sleep(delay_seconds)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download all subject documents for a project.")
    parser.add_argument(
        "--workers",
        type=int,
        default=DOCUMENT_DOWNLOAD_WORKERS,
        help="Parallel workers per subject for document processing.",
    )
    parser.add_argument(
        "--document-delay-seconds",
        type=float,
        default=DOCUMENT_DELAY_SECONDS,
        help="Delay between per-document operations.",
    )
    parser.add_argument(
        "--document-limit",
        type=int,
        default=0,
        help="Optional max number of documents to process for test runs (0 = no limit).",
    )
    return parser.parse_args()


def process_document_for_subject(subject_id: str, document: dict, *, delay_seconds: float) -> tuple[int, int]:
    public_client, internal_client, download_session = _get_worker_clients()
    doc_id = document.get("id")
    if not doc_id:
        pause_between_documents(delay_seconds)
        return 0, 1

    details = get_document_details(doc_id, api_client=public_client)
    file_name = str(details.get("fileName") or f"{doc_id}")
    content_type = str(details.get("contentType") or "")
    subject_folder = DOWNLOAD_ROOT / subject_id
    subject_folder.mkdir(exist_ok=True)
    downloaded_files = 0

    if is_xml_document(file_name, content_type):
        # For XML documents, download both raw XML and rendered PDF.
        raw_signed_url = details.get("signedS3Url")
        if raw_signed_url:
            raw_output_path = subject_folder / file_name
            try:
                download_file(raw_signed_url, raw_output_path, download_session=download_session)
                downloaded_files += 1
            except Exception as exc:
                print(f"Raw XML download failed for document {doc_id}: {exc}")
        else:
            print(f"No raw XML URL available for document {doc_id}")

        try:
            pdf_meta = get_document_pdf_url(doc_id, api_client=internal_client)
            pdf_signed_url = pdf_meta.get("signedUrl")
            pdf_file_name = str(pdf_meta.get("fileName") or f"{doc_id}.pdf")
            if not pdf_file_name.lower().endswith(".pdf"):
                pdf_file_name = f"{pdf_file_name}.pdf"
            if pdf_signed_url:
                pdf_output_path = subject_folder / pdf_file_name
                try:
                    download_file(pdf_signed_url, pdf_output_path, download_session=download_session)
                    downloaded_files += 1
                except Exception as exc:
                    print(f"PDF download failed for XML document {doc_id}: {exc}")
            else:
                print(f"No XML->PDF URL available for document {doc_id}")
        except RuntimeError as e:
            print(f"XML->PDF failed for document {doc_id}: {e}")
    else:
        signed_url = details.get("signedS3Url")
        if not signed_url:
            print(f"No downloadable URL available for document {doc_id}")
            pause_between_documents(delay_seconds)
            return 0, 1
        output_path = subject_folder / file_name
        download_file(signed_url, output_path, download_session=download_session)
        downloaded_files += 1

    pause_between_documents(delay_seconds)
    if downloaded_files > 0:
        return downloaded_files, 0
    return 0, 1


def process_subject_documents(subject_id: str, documents: list[dict], *, workers: int, delay_seconds: float) -> tuple[int, int]:
    downloaded = 0
    skipped = 0
    max_workers = max(1, min(workers, len(documents)))

    with progress_bar(
        total=len(documents),
        desc=f"Docs for {subject_id[:8]}",
        unit="doc",
        leave=False,
    ) as doc_bar:
        if max_workers == 1:
            for doc in documents:
                try:
                    downloaded_count, skipped_count = process_document_for_subject(
                        subject_id,
                        doc,
                        delay_seconds=delay_seconds,
                    )
                except Exception as exc:
                    print(f"Document processing failed for subject {subject_id}: {exc}")
                    downloaded_count, skipped_count = (0, 1)
                downloaded += downloaded_count
                skipped += skipped_count
                doc_bar.update(1)
            return downloaded, skipped

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(
                    process_document_for_subject,
                    subject_id,
                    doc,
                    delay_seconds=delay_seconds,
                )
                for doc in documents
            ]

            for future in as_completed(futures):
                try:
                    downloaded_count, skipped_count = future.result()
                except Exception as exc:
                    print(f"Document processing failed for subject {subject_id}: {exc}")
                    downloaded_count, skipped_count = (0, 1)
                downloaded += downloaded_count
                skipped += skipped_count
                doc_bar.update(1)

    return downloaded, skipped


# ----------------------------
# Main
# ----------------------------
def main():
    args = parse_args()
    workers = max(1, int(args.workers or 1))
    document_delay_seconds = max(0.0, float(args.document_delay_seconds or 0.0))
    document_limit = max(0, int(args.document_limit or 0))
    limited_mode = document_limit > 0

    print("Starting export...")
    print(f"Parallel workers per subject: {workers}")
    print(f"Delay between document operations: {document_delay_seconds:.2f}s")
    if limited_mode:
        print(f"Document processing limit: {document_limit}")

    subjects = list(iter_subjects())
    print("Subjects found:", len(subjects))

    total_docs_found = 0
    total_docs_downloaded = 0
    total_docs_skipped = 0
    total_docs_attempted = 0

    with progress_bar(total=len(subjects), desc="Patients Processed", unit="patient") as subj_bar:
        for subject in subjects:
            if limited_mode and total_docs_attempted >= document_limit:
                break
            subject_id = subject.get("id")
            if not subject_id:
                subj_bar.update(1)
                continue

            documents = list(iter_documents_for_subject(subject_id))
            total_docs_found += len(documents)
            if limited_mode:
                remaining = document_limit - total_docs_attempted
                if remaining <= 0:
                    break
                documents = documents[:remaining]

            if documents:
                downloaded, skipped = process_subject_documents(
                    subject_id,
                    documents,
                    workers=workers,
                    delay_seconds=document_delay_seconds,
                )
                total_docs_downloaded += downloaded
                total_docs_skipped += skipped
                total_docs_attempted += downloaded + skipped

            subj_bar.update(1)

    print("\n-----------------------------------------")
    print("Processing Complete")
    print("Total Documents Found:", total_docs_found)
    print("Total Documents Downloaded:", total_docs_downloaded)
    print("Total Documents Skipped:", total_docs_skipped)
    print("-----------------------------------------")


if __name__ == "__main__":
    main()
