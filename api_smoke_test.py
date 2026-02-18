#!/usr/bin/env python3
"""
xCures Patient Registry API smoke test

Flow:
  1) Authenticate (OAuth client_credentials)
  2) Create Subject (POST /api/v1/patient-registry/subject) with a new UUID
  3) Get Subject by id (GET /api/v1/patient-registry/subject/{id})
  4) Search Documents for a fixed subjectId
  5) Get one Document by documentId
  6) Search Clinical Concepts Conditions for the same fixed subjectId

Requires:
  pip install requests

Credential loading order:
  1) Environment variables:
       XCURES_CLIENT_ID
       XCURES_CLIENT_SECRET
  2) Optional local .env file in the same folder as this script:
       XCURES_CLIENT_ID=...
       XCURES_CLIENT_SECRET=...
  3) Interactive prompt (no echo for secret)

Optional:
  XCURES_BASE_URL (default: https://partner.xcures.com)
"""

from __future__ import annotations

import json
import os
import sys
import uuid
from dataclasses import dataclass
from getpass import getpass
from pathlib import Path
from typing import Any, Dict, Tuple

import requests


PROJECT_ID = "b114ceeb-2adf-4c80-aae2-b6ccae3eac7b"
DOCUMENTS_SUBJECT_ID = "45cb7e25-7d74-43cc-9cc5-69d0aaa77c73"


@dataclass
class ApiConfig:
    base_url: str
    client_id: str
    client_secret: str
    timeout_seconds: int = 30


class ApiError(RuntimeError):
    pass


def _pretty(obj: Any) -> str:
    try:
        return json.dumps(obj, indent=2, sort_keys=True)
    except Exception:
        return str(obj)


def _raise_for_status(resp: requests.Response, context: str) -> None:
    if 200 <= resp.status_code < 300:
        return
    try:
        body: Any = resp.json()
    except Exception:
        body = resp.text
    raise ApiError(f"{context} failed with HTTP {resp.status_code}\nResponse:\n{_pretty(body)}")


def load_dotenv_if_present() -> None:
    """
    Minimal .env loader to avoid adding dependencies.
    Looks for a file named '.env' in the same directory as this script.
    """
    env_path = Path(__file__).resolve().parent / ".env"
    if not env_path.exists():
        return

    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        os.environ.setdefault(k, v)


def get_required_env(name: str) -> str:
    val = os.getenv(name, "").strip()
    return val


def resolve_credentials() -> Tuple[str, str]:
    """
    Return (client_id, client_secret) from env/.env or prompt.
    """
    load_dotenv_if_present()

    client_id = get_required_env("XCURES_CLIENT_ID")
    client_secret = get_required_env("XCURES_CLIENT_SECRET")

    if client_id and client_secret:
        return client_id, client_secret

    # Prompt fallback
    if not client_id:
        client_id = input("XCURES_CLIENT_ID: ").strip()
    if not client_secret:
        client_secret = getpass("XCURES_CLIENT_SECRET (hidden): ").strip()

    if not client_id or not client_secret:
        raise ApiError("Missing XCURES_CLIENT_ID or XCURES_CLIENT_SECRET.")

    return client_id, client_secret


def auth_token(cfg: ApiConfig) -> str:
    url = f"{cfg.base_url}/oauth/token"
    payload = {
        "client_id": cfg.client_id,
        "client_secret": cfg.client_secret,
        "grant_type": "client_credentials",
    }

    resp = requests.post(url, json=payload, timeout=cfg.timeout_seconds)
    _raise_for_status(resp, "OAuth token")
    data = resp.json()

    access_token = data.get("access_token")
    token_type = data.get("token_type", "Bearer")

    if not access_token:
        raise ApiError(f"OAuth token response missing access_token:\n{_pretty(data)}")

    if str(token_type).lower() != "bearer":
        print(f"Warning: token_type was {token_type!r}, proceeding anyway.", file=sys.stderr)

    return access_token


def make_headers(access_token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {access_token}",
        "ProjectId": PROJECT_ID,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def create_subject(cfg: ApiConfig, headers: Dict[str, str]) -> Tuple[str, Dict[str, Any]]:
    subject_id = str(uuid.uuid4())

    subject_payload = {
        "id": subject_id,
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

    url = f"{cfg.base_url}/api/v1/patient-registry/subject"
    resp = requests.post(url, headers=headers, json=[subject_payload], timeout=cfg.timeout_seconds)
    _raise_for_status(resp, "Create Subject")

    data = resp.json()
    if not isinstance(data, list) or len(data) < 1:
        raise ApiError(f"Unexpected Create Subject response shape:\n{_pretty(data)}")

    return subject_id, {"request": subject_payload, "response": data[0]}


def get_subject(cfg: ApiConfig, headers: Dict[str, str], subject_id: str) -> Dict[str, Any]:
    url = f"{cfg.base_url}/api/v1/patient-registry/subject/{subject_id}"
    resp = requests.get(url, headers=headers, timeout=cfg.timeout_seconds)
    _raise_for_status(resp, "Get Subject")
    return resp.json()


def search_documents(cfg: ApiConfig, headers: Dict[str, str], subject_id: str) -> Dict[str, Any]:
    url = f"{cfg.base_url}/api/v1/patient-registry/document"
    params = {"subjectId": subject_id, "pageNumber": 1, "pageSize": 50}
    resp = requests.get(url, headers=headers, params=params, timeout=cfg.timeout_seconds)
    _raise_for_status(resp, "Search Documents")
    return resp.json()


def get_document(cfg: ApiConfig, headers: Dict[str, str], document_id: str) -> Any:
    url = f"{cfg.base_url}/api/v1/patient-registry/document/{document_id}"
    resp = requests.get(url, headers=headers, timeout=cfg.timeout_seconds)
    _raise_for_status(resp, "Get Document")
    return resp.json()


def search_conditions(cfg: ApiConfig, headers: Dict[str, str], subject_id: str) -> Dict[str, Any]:
    url = f"{cfg.base_url}/api/v1/patient-registry/clinical-concepts/condition"
    params = {"subjectId": subject_id, "pageNumber": 1, "pageSize": 50}
    resp = requests.get(url, headers=headers, params=params, timeout=cfg.timeout_seconds)
    _raise_for_status(resp, "Search Clinical Concepts Conditions")
    return resp.json()


def main() -> int:
    try:
        base_url = os.getenv("XCURES_BASE_URL", "https://partner.xcures.com").rstrip("/")
        client_id, client_secret = resolve_credentials()

        cfg = ApiConfig(
            base_url=base_url,
            client_id=client_id,
            client_secret=client_secret,
        )

        print(f"Base URL: {cfg.base_url}")
        print(f"ProjectId: {PROJECT_ID}")
        print()

        token = auth_token(cfg)
        headers = make_headers(token)
        print("Auth: OK")
        print()

        created_subject_id, create_details = create_subject(cfg, headers)
        print("Create Subject: OK")
        print(f"  New subject id: {created_subject_id}")
        print(f"  CreateSubjectResult:\n{_pretty(create_details['response'])}")
        print()

        subject = get_subject(cfg, headers, created_subject_id)
        print("Get Subject: OK")
        print(f"  Subject:\n{_pretty(subject)}")
        print()

        docs = search_documents(cfg, headers, DOCUMENTS_SUBJECT_ID)
        print("Search Documents: OK")
        results = docs.get("results") or []
        print(f"  totalCount: {docs.get('totalCount')}")
        print(f"  results returned: {len(results)}")
        print()

        if not results:
            print(
                f"No documents found for subjectId={DOCUMENTS_SUBJECT_ID}. Skipping Get Document.",
                file=sys.stderr,
            )
        else:
            first_doc = results[0]
            doc_id = first_doc.get("id") or first_doc.get("documentId")
            if not doc_id:
                raise ApiError(f"Could not find document id field in first result:\n{_pretty(first_doc)}")

            doc_detail = get_document(cfg, headers, str(doc_id))
            print("Get Document: OK")
            print(f"  documentId: {doc_id}")
            print(f"  DocumentDetail response:\n{_pretty(doc_detail)}")
            print()

        cond = search_conditions(cfg, headers, DOCUMENTS_SUBJECT_ID)
        print("Search Clinical Concepts Conditions: OK")
        cond_results = cond.get("results") if isinstance(cond, dict) else None
        if isinstance(cond_results, list):
            print(f"  results returned: {len(cond_results)}")
        else:
            print("  (no top-level 'results' list found, printing full response)")
        print(f"  Response:\n{_pretty(cond)}")
        print()

        print("Smoke test completed successfully.")
        return 0

    except ApiError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2
    except requests.RequestException as e:
        print(f"HTTP ERROR: {e}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())