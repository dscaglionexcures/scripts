from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Dict, Optional, Tuple, Union

import requests


PathLike = Union[str, Path]
_TOKEN_CACHE: Dict[Tuple[str, str], Tuple[str, float]] = {}
_TOKEN_TTL_SECONDS = 55 * 60


def load_env_file(path: PathLike = ".env", *, required: bool = False) -> Dict[str, str]:
    env_path = Path(path).expanduser().resolve()
    loaded: Dict[str, str] = {}

    if not env_path.exists():
        if required:
            raise RuntimeError(f"Env file not found: {env_path}")
        return loaded

    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if not key:
            continue

        loaded[key] = value
        os.environ.setdefault(key, value)

    return loaded


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def get_bearer_token(env_var: str = "XCURES_BEARER_TOKEN") -> str:
    token = os.environ.get(env_var, "").strip()
    if not token:
        raise RuntimeError(
            f"{env_var} is not set.\n"
            "Run:\n"
            f"  export {env_var}='your_token_here'"
        )
    return token


def get_xcures_auth_config() -> Tuple[str, str, str]:
    base_url = os.environ.get("BASE_URL", "https://partner.xcures.com").strip().rstrip("/")
    auth_url = os.environ.get("AUTH_URL", f"{base_url}/oauth/token").strip()
    client_id = require_env("XCURES_CLIENT_ID")
    client_secret = require_env("XCURES_CLIENT_SECRET")
    return auth_url, client_id, client_secret


def build_json_headers(
    *,
    bearer_token: str,
    project_id: Optional[str] = None,
    accept: str = "application/json",
) -> Dict[str, str]:
    headers: Dict[str, str] = {
        "Accept": accept,
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json",
    }
    if project_id:
        headers["ProjectId"] = project_id
    return headers


def fetch_client_credentials_token(
    session: requests.Session,
    *,
    auth_url: str,
    client_id: str,
    client_secret: str,
    timeout_seconds: int = 60,
) -> str:
    response = session.post(
        auth_url,
        json={
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials",
        },
        timeout=timeout_seconds,
    )

    if not response.ok:
        raise RuntimeError(
            f"Client credentials auth failed: HTTP {response.status_code} "
            f"body={(response.text or '')[:1200]}"
        )

    payload = response.json()
    token = payload.get("access_token") if isinstance(payload, dict) else None
    if not isinstance(token, str) or not token.strip():
        raise RuntimeError(f"Auth response missing access_token: {payload}")

    return token.strip()


def get_xcures_bearer_token(
    *,
    timeout_seconds: int = 60,
    force_refresh: bool = False,
) -> str:
    auth_url, client_id, client_secret = get_xcures_auth_config()
    cache_key = (auth_url, client_id)
    now = time.time()

    if not force_refresh:
        cached = _TOKEN_CACHE.get(cache_key)
        if cached and (now - cached[1]) < _TOKEN_TTL_SECONDS:
            return cached[0]

    with requests.Session() as session:
        token = fetch_client_credentials_token(
            session,
            auth_url=auth_url,
            client_id=client_id,
            client_secret=client_secret,
            timeout_seconds=timeout_seconds,
        )
    _TOKEN_CACHE[cache_key] = (token, now)
    return token
