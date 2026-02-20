#!/usr/bin/env python3

from __future__ import annotations

import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

import requests


CHECK = "\u2713"
CROSS = "\u2717"

BASE_URL = "https://partner.xcures.com"
PROJECT_ID = "b114ceeb-2adf-4c80-aae2-b6ccae3eac7b"
FIXED_SUBJECT_ID_FOR_DOCS_AND_CC = "45cb7e25-7d74-43cc-9cc5-69d0aaa77c73"


class ApiError(RuntimeError):
    pass


# -------------------------
# Utility Functions
# -------------------------

def load_dotenv_if_present() -> None:
    env_path = Path(__file__).resolve().parent / ".env"
    if not env_path.exists():
        return

    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)
        os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))


def get_required_env(name: str) -> str:
    val = os.getenv(name, "").strip()
    if not val:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val


def request_json(
    method: str,
    url: str,
    token: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    body: Optional[Any] = None,
    timeout_s: int = 30,
) -> Any:
    headers = {
        "Authorization": f"Bearer {token}",
        "ProjectId": PROJECT_ID,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    resp = requests.request(
        method=method,
        url=url,
        headers=headers,
        params=params,
        json=body,
        timeout=timeout_s,
    )

    if not resp.ok:
        raise ApiError(
            f"{method} {url} failed: HTTP {resp.status_code}\n{resp.text}"
        )

    if resp.status_code == 204 or not resp.text.strip():
        return None

    return resp.json()


# -------------------------
# Business Operations
# -------------------------

def get_bearer_token(client_id: str, client_secret: str) -> str:
    print("\nRunning Test: Authentication")

    resp = requests.post(
        f"{BASE_URL}/oauth/token",
        json={
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials",
        },
        timeout=30,
    )

    if not resp.ok:
        raise ApiError(resp.text)

    print(f"{CHECK} Authentication successful")
    return resp.json()["access_token"]


def create_subject(token: str) -> str:
    print("\nRunning Test: Create Subject")

    new_id = str(uuid.uuid4())

    subject = {
        "id": new_id,
        "firstName": "Test",
        "lastName": "Patient",
        "email": "test@xcures.com",
        "birthDate": "1980-04-05",
        "gender": "M",
        "addressLine1": "123 Main St.",
        "addressLine2": "Apt 4B",
        "addressCity": "Minneapolis",
        "addressState": "MN",
        "addressPostalCode": "55401",
        "phoneNumber": "123-456-7890",
    }

    request_json(
        "POST",
        f"{BASE_URL}/api/v1/patient-registry/subject",
        token,
        body=[subject],
    )

    print(f"{CHECK} Subject created (id={new_id})")
    return new_id


def upload_pdf_signed_s3(token: str, subject_id: str) -> str:
    print("\nRunning Test: Upload biometric.pdf via Signed S3")

    pdf_path = Path(__file__).resolve().parent / "biometric.pdf"
    if not pdf_path.exists():
        raise RuntimeError("biometric.pdf not found")

    metadata = request_json(
        "POST",
        f"{BASE_URL}/api/v1/patient-registry/document",
        token,
        body={
            "subjectId": subject_id,
            "fileName": "biometric.pdf",
            "contentType": "application/pdf",
        },
    )

    document_id = metadata["documentId"]
    signed_url = metadata["signedS3Url"]

    print("Uploading binary to signed S3 URL...")

    with open(pdf_path, "rb") as f:
        put_resp = requests.put(
            signed_url,
            data=f,
            headers={"Content-Type": "application/pdf"},
        )

    if not put_resp.ok:
        raise RuntimeError("S3 upload failed")

    print(f"{CHECK} PDF uploaded (documentId={document_id})")
    return document_id


def read_subject(token: str, subject_id: str) -> Dict[str, Any]:
    print("\nRunning Test: Get Subject")

    result = request_json(
        "GET",
        f"{BASE_URL}/api/v1/patient-registry/subject/{subject_id}",
        token,
    )

    print(f"{CHECK} Subject retrieved")
    return result


def update_subject(token: str, subject_id: str) -> None:
    print("\nRunning Test: Update Subject (Add additionalAddresses)")

    # Retrieve full subject first (PUT requires full DTO)
    current = request_json(
        "GET",
        f"{BASE_URL}/api/v1/patient-registry/subject/{subject_id}",
        token,
    )

    # Modify existing fields
    current["addressCity"] = "Louisville"
    current["addressState"] = "KY"

    # Add additionalAddresses array
    current["additionalAddresses"] = [
        {
            "addressLine1": "3350 Burkes Spg Rd",
            "addressCity": "Loretto",
            "addressState": "KY",
            "addressPostalCode": "40037"
        }
    ]

    # Send full object back
    request_json(
        "PUT",
        f"{BASE_URL}/api/v1/patient-registry/subject/{subject_id}",
        token,
        body=current,
    )

    print(f"{CHECK} Subject updated with additionalAddresses")


def verify_update(token: str, subject_id: str) -> None:
    print("\nRunning Test: Verify Subject Update")

    subj = read_subject(token, subject_id)

    if subj.get("addressCity") != "Louisville":
        raise RuntimeError("City mismatch")

    print(f"{CHECK} Update verified")


def wait_for_document(token: str, document_id: str) -> None:
    print("\nRunning Test: Wait for Uploaded Document")

    start = time.time()

    while time.time() - start < 30:
        try:
            request_json(
                "GET",
                f"{BASE_URL}/api/v1/patient-registry/document/{document_id}",
                token,
            )
            print(f"{CHECK} Document available")
            return
        except Exception:
            print("  Document not ready yet, retrying...")
            time.sleep(2)

    raise RuntimeError("Document did not become available in time")


def check_clinical_concepts(token: str) -> None:
    print("\nRunning Test: Check Clinical Concepts - Conditions")

    request_json(
        "GET",
        f"{BASE_URL}/api/v1/patient-registry/clinical-concepts/condition",
        token,
        params={"subjectId": FIXED_SUBJECT_ID_FOR_DOCS_AND_CC},
    )

    print(f"{CHECK} Clinical Concepts retrieved")


# -------------------------
# Main Test Harness
# -------------------------

def main() -> int:
    passed = []
    failed = []

    try:
        load_dotenv_if_present()

        token = get_bearer_token(
            get_required_env("XCURES_CLIENT_ID"),
            get_required_env("XCURES_CLIENT_SECRET"),
        )
        passed.append("Authentication")
    except Exception as e:
        print(f"{CROSS} Authentication failed: {e}")
        return 1

    subject_id = None
    document_id = None

    try:
        subject_id = create_subject(token)
        passed.append("Create Subject")
    except Exception as e:
        print(f"{CROSS} Create Subject failed: {e}")
        failed.append("Create Subject")

    try:
        if subject_id:
            document_id = upload_pdf_signed_s3(token, subject_id)
            passed.append("Upload biometric.pdf")
    except Exception as e:
        print(f"{CROSS} Upload failed: {e}")
        failed.append("Upload biometric.pdf")

    try:
        if subject_id:
            read_subject(token, subject_id)
            passed.append("Read Subject")
    except Exception as e:
        print(f"{CROSS} Read Subject failed: {e}")
        failed.append("Read Subject")

    try:
        if subject_id:
            update_subject(token, subject_id)
            passed.append("Update Subject")
    except Exception as e:
        print(f"{CROSS} Update Subject failed: {e}")
        failed.append("Update Subject")

    try:
        if subject_id:
            verify_update(token, subject_id)
            passed.append("Verify Update")
    except Exception as e:
        print(f"{CROSS} Verify failed: {e}")
        failed.append("Verify Update")

    try:
        if document_id:
            wait_for_document(token, document_id)
            passed.append("Get Document")
    except Exception as e:
        print(f"{CROSS} Get Document failed: {e}")
        failed.append("Get Document")

    try:
        check_clinical_concepts(token)
        passed.append("Clinical Concepts")
    except Exception as e:
        print(f"{CROSS} Clinical Concepts failed: {e}")
        failed.append("Clinical Concepts")

    print("\n==============================")
    print("Smoke Test Summary")
    print("==============================")

    for step in passed:
        print(f"{CHECK} {step}")

    for step in failed:
        print(f"{CROSS} {step}")

    if failed:
        print("\nSome tests failed.")
        return 2

    print("\nAll tests passed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())