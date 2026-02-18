#!/usr/bin/env python3
"""
xCures API smoke test (post-deploy) using .env for credentials

Flow:
1) OAuth client_credentials -> bearer token
2) Create Subject (POST /subject) with a new UUID
3) Read Subject (GET /subject/{id})
4) Update Subject (PUT /subject/{id}) setting addressCity=Louisville, addressState=KY
5) Read Subject again to verify update
6) Search Documents for a fixed subjectId (GET /document?subjectId=...)
7) Get one Document by id (GET /document/{documentId})
8) Search Clinical Concepts: Condition (GET /clinical-concepts/condition?subjectId=...)

Credentials:
- Loaded from a local .env file (same folder as this script), or from environment variables.
- Expected keys:
    XCURES_CLIENT_ID=...
    XCURES_CLIENT_SECRET=...

Notes:
- ProjectId header is required for these endpoints.
- Subject create expects an array payload.
"""

from __future__ import annotations

import json
import os
import sys
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

import requests


BASE_URL = "https://partner.xcures.com"

# Per your instruction
PROJECT_ID = "b114ceeb-2adf-4c80-aae2-b6ccae3eac7b"

# Per your instruction for doc + clinical concepts checks
FIXED_SUBJECT_ID_FOR_DOCS_AND_CC = "45cb7e25-7d74-43cc-9cc5-69d0aaa77c73"


class ApiError(RuntimeError):
    pass


def pretty(obj: Any) -> str:
    return json.dumps(obj, indent=2, sort_keys=True, default=str)


def die(msg: str) -> None:
    raise ApiError(msg)


def load_dotenv_if_present() -> None:
    """
    Minimal .env loader (no extra deps).
    Loads .env from the same directory as this script.
    Values already present in environment variables are not overwritten.
    """
    env_path = Path(__file__).resolve().parent / ".env"
    if not env_path.exists():
        return

    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)
        key = key.strip()
        val = val.strip().strip('"').strip("'")
        os.environ.setdefault(key, val)


def get_required_env(name: str) -> str:
    val = os.getenv(name, "").strip()
    if not val:
        die(f"Missing required environment variable: {name} (check your .env file)")
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
    }
    if body is not None:
        headers["Content-Type"] = "application/json"

    resp = requests.request(
        method=method,
        url=url,
        headers=headers,
        params=params,
        json=body,
        timeout=timeout_s,
    )

    if not (200 <= resp.status_code <= 299):
        detail = resp.text.strip()
        die(f"{method} {url} failed: HTTP {resp.status_code}\n{detail}")

    if resp.status_code == 204:
        return None
    if resp.text.strip() == "":
        return None

    try:
        return resp.json()
    except Exception:
        die(f"{method} {url} returned non-JSON response:\n{resp.text}")


def get_bearer_token(client_id: str, client_secret: str) -> str:
    url = f"{BASE_URL}/oauth/token"
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
    }
    resp = requests.post(url, json=payload, timeout=30)
    if not (200 <= resp.status_code <= 299):
        die(f"POST {url} failed: HTTP {resp.status_code}\n{resp.text}")

    data = resp.json()
    token = data.get("access_token")
    if not token:
        die(f"Token response missing access_token:\n{pretty(data)}")
    return token


def create_subject(token: str) -> str:
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

    url = f"{BASE_URL}/api/v1/patient-registry/subject"
    created = request_json("POST", url, token, body=[subject])

    if not isinstance(created, list) or len(created) == 0:
        die(f"Create subject response was not a non-empty array:\n{pretty(created)}")

    print(f"Created subject id: {new_id}")
    return new_id


def get_subject(token: str, subject_id: str) -> Dict[str, Any]:
    url = f"{BASE_URL}/api/v1/patient-registry/subject/{subject_id}"
    data = request_json("GET", url, token)
    if not isinstance(data, dict):
        die(f"Get subject did not return an object:\n{pretty(data)}")
    return data


def update_subject_city_state(token: str, subject_id: str) -> Dict[str, Any]:
    url = f"{BASE_URL}/api/v1/patient-registry/subject/{subject_id}"

    # Send the full known record to avoid accidental nulling if the API treats missing fields as null.
    update_body = {
        "firstName": "Test",
        "lastName": "Patient",
        "email": "test@xcures.com",
        "birthDate": "1980-04-05",
        "gender": "M",
        "addressLine1": "123 Main St.",
        "addressLine2": "Apt 4B",
        "addressCity": "Louisville",
        "addressState": "KY",
        "addressPostalCode": "55401",
        "phoneNumber": "123-456-7890",
    }

    updated = request_json("PUT", url, token, body=update_body)
    if not isinstance(updated, dict):
        die(f"Update subject did not return an object:\n{pretty(updated)}")
    return updated


def search_documents(token: str, subject_id: str) -> Dict[str, Any]:
    url = f"{BASE_URL}/api/v1/patient-registry/document"
    params = {"subjectId": subject_id, "pageNumber": 1, "pageSize": 10}
    data = request_json("GET", url, token, params=params)
    if not isinstance(data, dict):
        die(f"Search documents did not return an object:\n{pretty(data)}")
    return data


def get_document_by_id(token: str, document_id: str) -> Any:
    url = f"{BASE_URL}/api/v1/patient-registry/document/{document_id}"
    return request_json("GET", url, token)


def search_clinical_concepts_condition(token: str, subject_id: str) -> Dict[str, Any]:
    url = f"{BASE_URL}/api/v1/patient-registry/clinical-concepts/condition"
    params = {"subjectId": subject_id, "pageNumber": 1, "pageSize": 10}
    data = request_json("GET", url, token, params=params)
    if not isinstance(data, dict):
        die(f"Clinical concepts condition did not return an object:\n{pretty(data)}")
    return data


def main() -> int:
    try:
        load_dotenv_if_present()

        client_id = get_required_env("XCURES_CLIENT_ID")
        client_secret = get_required_env("XCURES_CLIENT_SECRET")

        print("Authenticating...")
        token = get_bearer_token(client_id, client_secret)
        print("Got bearer token.")
        print(f"ProjectId: {PROJECT_ID}")
        print()

        print("Creating subject...")
        created_subject_id = create_subject(token)

        print("\nReading subject...")
        subj = get_subject(token, created_subject_id)
        print(pretty({k: subj.get(k) for k in ["id", "firstName", "lastName", "addressCity", "addressState"]}))

        print("\nUpdating subject city/state to Louisville, KY...")
        updated = update_subject_city_state(token, created_subject_id)
        print(pretty({k: updated.get(k) for k in ["id", "firstName", "lastName", "addressCity", "addressState"]}))

        print("\nReading subject again to verify...")
        subj2 = get_subject(token, created_subject_id)
        city = subj2.get("addressCity")
        state = subj2.get("addressState")
        print(pretty({k: subj2.get(k) for k in ["id", "firstName", "lastName", "addressCity", "addressState"]}))

        if city != "Louisville" or state != "KY":
            die(f"Update verification failed: addressCity={city}, addressState={state}")
        print("Verified subject update.")

        print(f"\nSearching documents for subjectId={FIXED_SUBJECT_ID_FOR_DOCS_AND_CC} ...")
        doc_page = search_documents(token, FIXED_SUBJECT_ID_FOR_DOCS_AND_CC)
        results = doc_page.get("results") or []
        print(f"Documents returned: {len(results)} (showing up to 1)")

        if len(results) == 0:
            print("No documents found. Skipping get-document step.")
        else:
            first_doc = results[0]
            document_id = first_doc.get("id")
            if not document_id:
                die(f"First document missing id:\n{pretty(first_doc)}")

            print(f"Getting document by id: {document_id}")
            doc_detail = get_document_by_id(token, str(document_id))
            print("Document detail (raw):")
            print(pretty(doc_detail))

        print(f"\nSearching clinical concepts (condition) for subjectId={FIXED_SUBJECT_ID_FOR_DOCS_AND_CC} ...")
        cc = search_clinical_concepts_condition(token, FIXED_SUBJECT_ID_FOR_DOCS_AND_CC)
        cc_results = cc.get("results") or []
        print(f"Condition concepts returned: {len(cc_results)}")
        if len(cc_results) > 0:
            print("First condition concept (preview):")
            print(pretty(cc_results[0]))

        print("\nSmoke test completed successfully.")
        return 0

    except ApiError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2
    except requests.RequestException as e:
        print(f"HTTP ERROR: {e}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())