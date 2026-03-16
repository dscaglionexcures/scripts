from __future__ import annotations

import time
from typing import Any, Callable, Dict, Optional, Sequence

import requests

DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_MAX_RETRIES = 2
DEFAULT_BACKOFF_SECONDS = 1.0
DEFAULT_MAX_SLEEP_SECONDS = 20.0
DEFAULT_RETRY_STATUSES: Sequence[int] = (429, 500, 502, 503, 504)


def body_preview(text: str, limit: int = 800) -> str:
    flat = (text or "").strip().replace("\n", " ")
    if len(flat) > limit:
        return flat[:limit] + "...<truncated>"
    return flat


def build_url(base_url: str, path_or_url: str) -> str:
    if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
        return path_or_url
    return f"{base_url.rstrip('/')}/{path_or_url.lstrip('/')}"


def format_http_error(
    *,
    status_code: int,
    url: str,
    body: str,
    prefix: str = "HTTP request failed",
) -> str:
    return f"{prefix}: HTTP {status_code} {url} body={body_preview(body, 1200)}"


def parse_json_or_raise(resp: requests.Response) -> Any:
    try:
        return resp.json()
    except Exception:
        raise RuntimeError(
            f"Non-JSON response: status={resp.status_code} body={body_preview(resp.text, 1200)}"
        )


def require_json_object(payload: Any, *, context: str) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise RuntimeError(f"Expected JSON object for {context}; got {type(payload)}")
    return payload


def require_json_list(payload: Any, *, context: str) -> list[Any]:
    if not isinstance(payload, list):
        raise RuntimeError(f"Expected JSON list for {context}; got {type(payload)}")
    return payload


def request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    max_retries: int = DEFAULT_MAX_RETRIES,
    backoff_seconds: float = DEFAULT_BACKOFF_SECONDS,
    max_sleep_seconds: Optional[float] = DEFAULT_MAX_SLEEP_SECONDS,
    retry_statuses: Sequence[int] = DEFAULT_RETRY_STATUSES,
    logger: Optional[Callable[[str], None]] = None,
) -> requests.Response:
    last_resp: Optional[requests.Response] = None
    last_exc: Optional[BaseException] = None

    for attempt in range(1, max_retries + 1):
        try:
            if logger:
                logger(f"{method} {url} attempt={attempt}/{max_retries} params={params or {}}")

            resp = session.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=json_body,
                timeout=timeout_seconds,
            )
            last_resp = resp

            if logger:
                logger(f"-> {resp.status_code} body={body_preview(resp.text, 300)}")

            if 200 <= resp.status_code < 300:
                return resp

            if resp.status_code in retry_statuses:
                sleep_s = backoff_seconds * (2 ** (attempt - 1))
                if max_sleep_seconds is not None:
                    sleep_s = min(sleep_s, max_sleep_seconds)
                time.sleep(sleep_s)
                continue

            raise RuntimeError(
                format_http_error(
                    status_code=resp.status_code,
                    url=url,
                    body=resp.text,
                    prefix="HTTP request failed",
                )
            )

        except (requests.Timeout, requests.ConnectionError, requests.RequestException) as exc:
            last_exc = exc
            if logger:
                logger(f"network error {type(exc).__name__}: {exc}")
            sleep_s = backoff_seconds * (2 ** (attempt - 1))
            if max_sleep_seconds is not None:
                sleep_s = min(sleep_s, max_sleep_seconds)
            time.sleep(sleep_s)

    if last_resp is not None:
        raise RuntimeError(
            f"request_with_retry exhausted; last_status={last_resp.status_code} "
            f"url={url} body={body_preview(last_resp.text, 1200)}"
        )

    raise RuntimeError(
        "request_with_retry exhausted; no response; "
        f"last_exc={type(last_exc).__name__ if last_exc else 'unknown'}: {last_exc}"
    )


def request_json_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    max_retries: int = DEFAULT_MAX_RETRIES,
    backoff_seconds: float = DEFAULT_BACKOFF_SECONDS,
    max_sleep_seconds: Optional[float] = DEFAULT_MAX_SLEEP_SECONDS,
    retry_statuses: Sequence[int] = DEFAULT_RETRY_STATUSES,
    logger: Optional[Callable[[str], None]] = None,
) -> Any:
    resp = request_with_retry(
        session=session,
        method=method,
        url=url,
        headers=headers,
        params=params,
        json_body=json_body,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        backoff_seconds=backoff_seconds,
        max_sleep_seconds=max_sleep_seconds,
        retry_statuses=retry_statuses,
        logger=logger,
    )
    if resp.status_code == 204 or not (resp.text or "").strip():
        return None
    return parse_json_or_raise(resp)


def request_json_with_retry_and_headers(
    session: requests.Session,
    method: str,
    url: str,
    *,
    headers: Dict[str, str],
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    logger: Optional[Callable[[str], None]] = None,
) -> Any:
    return request_json_with_retry(
        session=session,
        method=method,
        url=url,
        headers=headers,
        params=params,
        json_body=json_body,
        timeout_seconds=timeout_seconds,
        logger=logger,
    )
