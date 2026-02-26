from __future__ import annotations

from typing import Any, Callable, Dict, Iterator, Optional, Sequence

import requests
from api_common import request_with_retry
from auth_common import build_json_headers, get_xcures_bearer_token


def _is_401_error_message(message: str) -> bool:
    return "HTTP 401 " in message or "last_status=401" in message


class XcuresApiClient:
    def __init__(
        self,
        *,
        session: requests.Session,
        base_url: str = "https://partner.xcures.com",
        project_id: Optional[str] = None,
        bearer_token: Optional[str] = None,
        timeout_seconds: int = 60,
        max_retries: int = 5,
        backoff_seconds: float = 1.0,
        max_sleep_seconds: Optional[float] = 20.0,
        retry_statuses: Sequence[int] = (429, 500, 502, 503, 504),
        logger: Optional[Callable[[str], None]] = None,
    ) -> None:
        self.session = session
        self.base_url = base_url.rstrip("/")
        self.project_id = project_id
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds
        self.max_sleep_seconds = max_sleep_seconds
        self.retry_statuses = retry_statuses
        self.logger = logger
        self._bearer_token_override = bearer_token.strip() if bearer_token else None

    def _get_bearer_token(self, *, force_refresh: bool = False) -> str:
        if self._bearer_token_override:
            return self._bearer_token_override
        return get_xcures_bearer_token(
            timeout_seconds=self.timeout_seconds,
            force_refresh=force_refresh,
        )

    def _headers(self, *, force_refresh: bool = False) -> Dict[str, str]:
        return build_json_headers(
            bearer_token=self._get_bearer_token(force_refresh=force_refresh),
            project_id=self.project_id,
        )

    def request(
        self,
        method: str,
        path_or_url: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        timeout_seconds: Optional[int] = None,
    ) -> requests.Response:
        url = path_or_url if path_or_url.startswith("http") else f"{self.base_url}/{path_or_url.lstrip('/')}"
        timeout = timeout_seconds if timeout_seconds is not None else self.timeout_seconds

        for auth_attempt in range(2):
            try:
                return request_with_retry(
                    session=self.session,
                    method=method,
                    url=url,
                    headers=self._headers(force_refresh=(auth_attempt == 1)),
                    params=params,
                    json_body=json_body,
                    timeout_seconds=timeout,
                    max_retries=self.max_retries,
                    backoff_seconds=self.backoff_seconds,
                    max_sleep_seconds=self.max_sleep_seconds,
                    retry_statuses=self.retry_statuses,
                    logger=self.logger,
                )
            except RuntimeError as e:
                if auth_attempt == 0 and _is_401_error_message(str(e)):
                    continue
                raise

        raise RuntimeError(f"{method} {url} failed after auth retry.")

    def request_json(
        self,
        method: str,
        path_or_url: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        timeout_seconds: Optional[int] = None,
    ) -> Any:
        resp = self.request(
            method,
            path_or_url,
            params=params,
            json_body=json_body,
            timeout_seconds=timeout_seconds,
        )
        if resp.status_code == 204 or not (resp.text or "").strip():
            return None
        try:
            return resp.json()
        except Exception:
            raise RuntimeError(
                f"Non-JSON response: status={resp.status_code} body={(resp.text or '')[:1200]}"
            )

    def iter_paginated(
        self,
        path_or_url: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        page_size: int = 50,
        start_page: int = 1,
        page_param: str = "pageNumber",
        size_param: str = "pageSize",
        max_pages: int = 10_000,
        results_keys: Sequence[str] = ("results", "items", "subjects", "data"),
        total_count_key: str = "totalCount",
        timeout_seconds: Optional[int] = None,
    ) -> Iterator[Dict[str, Any]]:
        base_params = dict(params or {})
        page_number = start_page
        emitted = 0

        for _ in range(max_pages):
            page_params = dict(base_params)
            page_params[page_param] = page_number
            page_params[size_param] = page_size

            payload = self.request_json(
                "GET",
                path_or_url,
                params=page_params,
                timeout_seconds=timeout_seconds,
            )

            if isinstance(payload, list):
                page_items = payload
            elif isinstance(payload, dict):
                page_items = []
                for key in results_keys:
                    value = payload.get(key)
                    if isinstance(value, list):
                        page_items = value
                        break
            else:
                raise RuntimeError(
                    f"Unexpected paginated payload type for {path_or_url}: {type(payload)}"
                )

            count_this_page = 0
            for item in page_items:
                if isinstance(item, dict):
                    yield item
                    emitted += 1
                    count_this_page += 1

            if isinstance(payload, dict):
                total_count = payload.get(total_count_key)
                if isinstance(total_count, int) and emitted >= total_count:
                    break

            if count_this_page == 0:
                break

            page_number += 1

    def list_paginated(
        self,
        path_or_url: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        page_size: int = 50,
        start_page: int = 1,
        page_param: str = "pageNumber",
        size_param: str = "pageSize",
        max_pages: int = 10_000,
        results_keys: Sequence[str] = ("results", "items", "subjects", "data"),
        total_count_key: str = "totalCount",
        timeout_seconds: Optional[int] = None,
    ) -> list[Dict[str, Any]]:
        return list(
            self.iter_paginated(
                path_or_url,
                params=params,
                page_size=page_size,
                start_page=start_page,
                page_param=page_param,
                size_param=size_param,
                max_pages=max_pages,
                results_keys=results_keys,
                total_count_key=total_count_key,
                timeout_seconds=timeout_seconds,
            )
        )
