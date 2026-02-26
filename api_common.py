from __future__ import annotations

import time
from typing import Any, Callable, Dict, Optional, Sequence

import requests


def body_preview(text: str, limit: int = 800) -> str:
    flat = (text or "").strip().replace("\n", " ")
    if len(flat) > limit:
        return flat[:limit] + "...<truncated>"
    return flat


def request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    timeout_seconds: int = 60,
    max_retries: int = 5,
    backoff_seconds: float = 1.0,
    max_sleep_seconds: Optional[float] = None,
    retry_statuses: Sequence[int] = (429, 500, 502, 503, 504),
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

            raise RuntimeError(f"HTTP {resp.status_code} {url} body={body_preview(resp.text, 1200)}")

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
