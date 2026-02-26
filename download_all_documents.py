import os
import time
from pathlib import Path

import requests
from progress_common import progress_bar
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from requests.exceptions import ConnectionError, Timeout
from auth_common import (
    build_json_headers,
    fetch_client_credentials_token,
    load_env_file,
    require_env,
)


# ----------------------------
# Load Config
# ----------------------------
load_env_file(Path(__file__).resolve().parent / ".env")

BASE_URL = os.getenv("BASE_URL", "https://partner.xcures.com").strip()
AUTH_URL = os.getenv("AUTH_URL", f"{BASE_URL.rstrip('/')}/oauth/token").strip()
CLIENT_ID = require_env("XCURES_CLIENT_ID")
CLIENT_SECRET = require_env("XCURES_CLIENT_SECRET")
PROJECT_ID = require_env("XCURES_PROJECT_ID")

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


# ----------------------------
# Token Manager
# ----------------------------
class TokenManager:
    def __init__(self):
        self.token = None
        self.acquired_at = 0
        self.refresh_window = 55 * 60  # refresh slightly before 60 min

    def authenticate(self):
        print("Authenticating...")
        try:
            self.token = fetch_client_credentials_token(
                session,
                auth_url=AUTH_URL,
                client_id=CLIENT_ID,
                client_secret=CLIENT_SECRET,
                timeout_seconds=60,
            )
        except Exception as e:
            raise SystemExit(str(e))
        self.acquired_at = time.time()

        print("Token acquired.")
        return self.token

    def get_token(self):
        if (
            not self.token
            or (time.time() - self.acquired_at) > self.refresh_window
        ):
            return self.authenticate()
        return self.token

    def get_headers(self):
        return build_json_headers(bearer_token=self.get_token(), project_id=PROJECT_ID)


token_manager = TokenManager()


# ----------------------------
# Safe Request Wrapper
# ----------------------------
def safe_request(method, url, **kwargs):
    for attempt in range(3):
        try:
            kwargs["headers"] = token_manager.get_headers()
            resp = session.request(method, url, **kwargs)

            if resp.status_code == 401:
                print("Token expired mid-run. Re-authenticating...")
                token_manager.authenticate()
                continue

            if not resp.ok:
                raise SystemExit(f"Request failed: {resp.status_code} {resp.text}")

            return resp

        except (ConnectionError, Timeout):
            print(f"Network error. Retry attempt {attempt + 1}/3...")
            time.sleep(2 * (attempt + 1))

    raise SystemExit("Request failed after retries.")


# ----------------------------
# Subjects
# ----------------------------
def iter_subjects(page_size=200):
    url = f"{BASE_URL}/api/v1/patient-registry/subject"
    page = 1

    while True:
        params = {"pageNumber": page, "pageSize": page_size}
        resp = safe_request("GET", url, params=params, timeout=60)

        data = resp.json()
        results = data.get("results") or []

        if not results:
            break

        for s in results:
            yield s

        total = data.get("totalCount")
        if total and page * page_size >= total:
            break

        page += 1


# ----------------------------
# Document Search
# ----------------------------
def iter_documents_for_subject(subject_id, page_size=200):
    url = f"{BASE_URL}/api/v1/patient-registry/document"
    page = 1

    while True:
        params = {
            "subjectId": subject_id,
            "pageNumber": page,
            "pageSize": page_size,
        }

        resp = safe_request("GET", url, params=params, timeout=120)

        data = resp.json()
        results = data.get("results") or []

        if not results:
            break

        for d in results:
            yield d

        total = data.get("totalCount")
        if total and page * page_size >= total:
            break

        page += 1


# ----------------------------
# Document Details (contains signedS3Url)
# ----------------------------
def get_document_details(document_id):
    url = f"{BASE_URL}/api/v1/patient-registry/document/{document_id}"
    resp = safe_request("GET", url, timeout=120)
    return resp.json()


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
