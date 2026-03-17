from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Optional
from urllib.parse import quote

import requests
from fastapi import Body, FastAPI, File, HTTPException, Query, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from xcures_toolkit.auth_common import fetch_client_credentials_token
from xcures_toolkit.xcures_client import XcuresApiClient
from .env_store import EnvStore
from .job_manager import JobManager
from .models import (
    CreateProfileRequest,
    CreateJobRequest,
    CreateJobResponse,
    FunctionalStatusBulkRequest,
    FunctionalStatusRequest,
    InternalApiRequest,
    InternalUserPermissionsRequest,
    ListProjectsRequest,
    PublicChecklistRequest,
    SendStdinRequest,
    SetActiveProfileRequest,
    UpdateProfileRequest,
    UpdateEnvRequest,
)
from .script_registry import ENV_CATALOG, ROOT_DIR, SCRIPT_DEFINITIONS, SCRIPT_INDEX


APP_HOST = "127.0.0.1"
APP_PORT = 8765
PARTNER_BASE_URL = "https://partner.xcures.com"
FRONTEND_DIST = ROOT_DIR / "web_ui" / "dist"
HISTORY_PATH = ROOT_DIR / "logs" / "script_runner_job_history.json"
UPLOADS_DIR = ROOT_DIR / "uploads" / "ui"
PERMISSIONS_CATALOG_PATH = ROOT_DIR / "configs" / "user_permissions_list.json"
FUNCTIONAL_STATUS_PATH = ROOT_DIR / "configs" / "functional_scripts.json"
CONFIGS_DIR = ROOT_DIR / "configs"

env_store = EnvStore(ROOT_DIR / ".env")
job_manager = JobManager(
    root_dir=ROOT_DIR,
    env_store=env_store,
    scripts_by_id=SCRIPT_INDEX,
    history_path=HISTORY_PATH,
)

app = FastAPI(title="xCures Local Script Runner", version="0.1.0")


def _runtime_timeout_seconds(runtime_env: Dict[str, str]) -> int:
    timeout_raw = runtime_env.get("request_timeout_seconds", "60")
    try:
        return int(str(timeout_raw).strip() or "60")
    except Exception:
        return 60


def _runtime_base_url(runtime_env: Dict[str, str]) -> str:
    return runtime_env.get("BASE_URL", PARTNER_BASE_URL).strip().rstrip("/") or PARTNER_BASE_URL


def _runtime_project_id(runtime_env: Dict[str, str]) -> str:
    return runtime_env.get("XCURES_PROJECT_ID", "").strip()


def _runtime_env_with_profile(profile_id: Optional[str]) -> Dict[str, str]:
    runtime_env = env_store.get_runtime_env()
    profile_id_value = str(profile_id or "").strip()
    if not profile_id_value:
        return runtime_env

    try:
        profile = env_store.get_profile_detail(profile_id_value)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Profile not found: {profile_id_value}")
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    for key in [
        "XCURES_CLIENT_ID",
        "XCURES_CLIENT_SECRET",
        "XCURES_BEARER_TOKEN",
        "XCURES_PROJECT_ID",
        "BASE_URL",
        "AUTH_URL",
    ]:
        runtime_env.pop(key, None)

    profile_client_id = str(profile.get("client_id") or "").strip()
    profile_client_secret = str(profile.get("client_secret") or "").strip()
    profile_bearer_token = str(profile.get("bearer_token") or "").strip()
    profile_project_id = str(profile.get("project_id") or "").strip()
    profile_base_url = str(profile.get("base_url") or "").strip()
    profile_auth_url = str(profile.get("auth_url") or "").strip()

    if profile_client_id:
        runtime_env["XCURES_CLIENT_ID"] = profile_client_id
    if profile_client_secret:
        runtime_env["XCURES_CLIENT_SECRET"] = profile_client_secret
    if profile_bearer_token:
        runtime_env["XCURES_BEARER_TOKEN"] = profile_bearer_token
    if profile_project_id:
        runtime_env["XCURES_PROJECT_ID"] = profile_project_id
    if profile_base_url:
        runtime_env["BASE_URL"] = profile_base_url
    if profile_auth_url:
        runtime_env["AUTH_URL"] = profile_auth_url

    return runtime_env


def _parse_permissions(value: Any) -> List[str]:
    if not isinstance(value, list):
        return []
    seen = set()
    parsed: List[str] = []
    for raw in value:
        text = str(raw or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        parsed.append(text)
    return parsed


def _normalize_functional_map(value: Any) -> Dict[str, bool]:
    if not isinstance(value, dict):
        return {}
    normalized: Dict[str, bool] = {}
    for raw_key, raw_value in value.items():
        script_id = str(raw_key or "").strip()
        if not script_id:
            continue
        if script_id not in SCRIPT_INDEX:
            continue
        normalized[script_id] = bool(raw_value)
    return normalized


def _read_functional_statuses() -> Dict[str, bool]:
    if not FUNCTIONAL_STATUS_PATH.exists():
        return {}
    try:
        payload = json.loads(FUNCTIONAL_STATUS_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to read functional status file: {exc}")

    if not isinstance(payload, dict):
        raise HTTPException(status_code=500, detail="Functional status file must be a JSON object.")

    # Backward-compatible parsing for either full envelope or a direct map.
    source = payload.get("functional_by_script", payload)
    normalized = _normalize_functional_map(source)
    return normalized


def _write_functional_statuses(functional_by_script: Dict[str, bool]) -> None:
    normalized = _normalize_functional_map(functional_by_script)
    FUNCTIONAL_STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {"functional_by_script": normalized}
    tmp_path = Path(f"{FUNCTIONAL_STATUS_PATH}.tmp")
    tmp_path.write_text(f"{json.dumps(payload, indent=2, sort_keys=True)}\n", encoding="utf-8")
    tmp_path.replace(FUNCTIONAL_STATUS_PATH)


def _display_name_from_user(raw: Dict[str, Any], fallback: str) -> str:
    first = str(raw.get("firstName") or "").strip()
    last = str(raw.get("lastName") or "").strip()
    full = " ".join(part for part in [first, last] if part).strip()
    if full:
        return full
    email = str(raw.get("email") or "").strip()
    return email or fallback


def _list_internal_users(client: XcuresApiClient, *, page_size: int) -> List[Dict[str, Any]]:
    try:
        payload = client.list_paginated("/api/patient-registry/user", page_size=page_size)
    except RuntimeError as exc:
        message = str(exc)
        if page_size > 50 and ("HTTP 500" in message or "last_status=500" in message):
            payload = client.list_paginated("/api/patient-registry/user", page_size=50)
        else:
            raise
    out: List[Dict[str, Any]] = []
    for raw in payload:
        if not isinstance(raw, dict):
            continue
        user_id = str(raw.get("id") or "").strip()
        if not user_id:
            continue
        permissions = _parse_permissions(raw.get("permissions"))
        email = str(raw.get("email") or "").strip()
        out.append(
            {
                "id": user_id,
                "email": email,
                "name": _display_name_from_user(raw, user_id),
                "permissions": permissions,
                "permission_count": len(permissions),
            }
        )
    out.sort(key=lambda item: (str(item.get("name") or "").lower(), str(item.get("email") or "").lower()))
    return out


def _fetch_internal_user_permissions(client: XcuresApiClient, user_id: str) -> Dict[str, Any]:
    data = client.request_json("GET", f"/api/patient-registry/user/{user_id}")
    if not isinstance(data, dict):
        raise RuntimeError(f"Unexpected user detail response type: {type(data).__name__}")
    permissions = _parse_permissions(data.get("permissions"))
    email = str(data.get("email") or "").strip()
    return {
        "id": str(data.get("id") or user_id).strip() or user_id,
        "email": email,
        "name": _display_name_from_user(data, user_id),
        "permissions": permissions,
        "permission_count": len(permissions),
    }


async def _resolve_internal_token(runtime_env: Dict[str, str], *, bearer_token: str, timeout_seconds: int) -> str:
    token = bearer_token.strip()
    if token:
        return token
    client_id = runtime_env.get("XCURES_CLIENT_ID", "").strip()
    client_secret = runtime_env.get("XCURES_CLIENT_SECRET", "").strip()
    if not client_id or not client_secret:
        raise HTTPException(
            status_code=422,
            detail=(
                "Unable to authenticate Internal API request. Provide Bearer token, or configure "
                "XCURES_CLIENT_ID and XCURES_CLIENT_SECRET in the active profile."
            ),
        )

    auth_url = runtime_env.get("AUTH_URL", f"{_runtime_base_url(runtime_env)}/oauth/token").strip()
    try:
        with requests.Session() as auth_session:
            return fetch_client_credentials_token(
                auth_session,
                auth_url=auth_url,
                client_id=client_id,
                client_secret=client_secret,
                timeout_seconds=timeout_seconds,
            )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to obtain access token: {exc}")


@app.on_event("startup")
async def startup_event() -> None:
    await job_manager.start()


@app.on_event("shutdown")
async def shutdown_event() -> None:
    await job_manager.shutdown()


@app.get("/api/health")
async def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/api/scripts")
async def list_scripts() -> List[dict]:
    return [
        {
            **script.model_dump(mode="json"),
            "supports_mode": script.mode_behavior != "none",
        }
        for script in SCRIPT_DEFINITIONS
    ]


@app.get("/api/config/xsl-files")
async def list_config_xsl_files() -> dict:
    if not CONFIGS_DIR.exists():
        return {"items": [], "count": 0}

    items: List[Dict[str, str]] = []
    for path in sorted(CONFIGS_DIR.glob("*.xsl")):
        if not path.is_file():
            continue
        rel_path = str(path.relative_to(ROOT_DIR))
        items.append(
            {
                "id": rel_path,
                "name": path.name,
                "path": rel_path,
            }
        )
    return {"items": items, "count": len(items)}


@app.get("/api/functional-status")
async def get_functional_status() -> dict:
    functional_by_script = _read_functional_statuses()
    return {
        "functional_by_script": functional_by_script,
        "count": len(functional_by_script),
    }


@app.put("/api/functional-status")
async def set_functional_status_bulk(request: FunctionalStatusBulkRequest) -> dict:
    _write_functional_statuses(request.functional_by_script)
    functional_by_script = _read_functional_statuses()
    return {
        "functional_by_script": functional_by_script,
        "count": len(functional_by_script),
    }


@app.put("/api/functional-status/{script_id}")
async def set_functional_status(script_id: str, request: FunctionalStatusRequest) -> dict:
    script_id_value = str(script_id or "").strip()
    if not script_id_value:
        raise HTTPException(status_code=422, detail="script_id is required.")
    if script_id_value not in SCRIPT_INDEX:
        raise HTTPException(status_code=404, detail=f"Unknown script_id: {script_id_value}")

    functional_by_script = _read_functional_statuses()
    functional_by_script[script_id_value] = bool(request.functional)
    _write_functional_statuses(functional_by_script)
    updated = _read_functional_statuses()
    return {
        "script_id": script_id_value,
        "functional": updated.get(script_id_value, False),
        "functional_by_script": updated,
        "count": len(updated),
    }


@app.get("/api/env")
async def get_env_values(reveal: bool = Query(default=False)) -> dict:
    values = env_store.as_view(reveal=reveal, catalog=ENV_CATALOG, scripts=SCRIPT_DEFINITIONS)
    return {
        "values": [value.model_dump(mode="json") for value in values],
        "catalog": [item.model_dump(mode="json") for item in ENV_CATALOG],
        "profiles": env_store.get_profiles(),
    }


@app.put("/api/env")
async def update_env_values(request: UpdateEnvRequest) -> dict:
    updated = env_store.update(request.updates, clear_missing=request.clear_missing)
    return {"updated_keys": sorted(request.updates.keys()), "count": len(updated)}


@app.get("/api/profiles")
async def get_profiles() -> dict:
    return env_store.get_profiles()


@app.get("/api/profiles/{profile_id}")
async def get_profile(profile_id: str) -> dict:
    try:
        return env_store.get_profile_detail(profile_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Profile not found: {profile_id}")
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))


@app.post("/api/internal/projects")
async def list_internal_projects(request: ListProjectsRequest) -> dict:
    runtime_env = _runtime_env_with_profile(request.profile_id)
    timeout_seconds = _runtime_timeout_seconds(runtime_env)
    token = await _resolve_internal_token(
        runtime_env,
        bearer_token=request.bearer_token or runtime_env.get("XCURES_BEARER_TOKEN", ""),
        timeout_seconds=timeout_seconds,
    )

    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {token}",
    }
    project_id_header = _runtime_project_id(runtime_env)
    if project_id_header:
        headers["ProjectId"] = project_id_header

    url = f"{_runtime_base_url(runtime_env)}/api/patient-registry/project"

    def _fetch() -> requests.Response:
        return requests.get(url, headers=headers, timeout=timeout_seconds)

    try:
        response = await asyncio.to_thread(_fetch)
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail=f"Failed to load projects: {exc}")

    if response.status_code < 200 or response.status_code >= 300:
        detail = response.text.strip() or f"HTTP {response.status_code}"
        raise HTTPException(status_code=response.status_code, detail=f"Project list request failed: {detail}")

    try:
        payload = response.json()
    except ValueError:
        raise HTTPException(status_code=502, detail="Project list response was not valid JSON.")

    if not isinstance(payload, list):
        raise HTTPException(status_code=502, detail=f"Unexpected project list response type: {type(payload).__name__}")

    items = []
    for raw in payload:
        if not isinstance(raw, dict):
            continue
        project_id = str(raw.get("id") or "").strip()
        if not project_id:
            continue
        name = str(raw.get("name") or "").strip() or project_id
        items.append({"id": project_id, "name": name})

    items.sort(key=lambda item: item["name"].lower())
    return {"items": items, "count": len(items)}


@app.get("/api/public/checklists")
async def list_public_checklists(
    profile_id: Optional[str] = Query(default=None),
    project_id: Optional[str] = Query(default=None),
    bearer_token: Optional[str] = Query(default=None),
) -> dict:
    runtime_env = _runtime_env_with_profile(profile_id)

    timeout_seconds = _runtime_timeout_seconds(runtime_env)
    selected_project_id = str(project_id or "").strip()
    token = await _resolve_internal_token(
        runtime_env,
        bearer_token=str(bearer_token or "").strip() or runtime_env.get("XCURES_BEARER_TOKEN", ""),
        timeout_seconds=timeout_seconds,
    )

    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {token}",
    }
    project_id_header = selected_project_id or _runtime_project_id(runtime_env)
    if project_id_header:
        headers["ProjectId"] = project_id_header

    url = f"{_runtime_base_url(runtime_env)}/api/v1/patient-registry/checklist"

    def _fetch() -> requests.Response:
        params: Dict[str, str] = {"type": "questionnaire"}
        if project_id_header:
            params["projectId"] = project_id_header
        return requests.get(url, headers=headers, params=params, timeout=timeout_seconds)

    try:
        response = await asyncio.to_thread(_fetch)
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail=f"Failed to load checklists: {exc}")

    if response.status_code < 200 or response.status_code >= 300:
        detail = response.text.strip() or f"HTTP {response.status_code}"
        raise HTTPException(status_code=response.status_code, detail=f"Checklist request failed: {detail}")

    try:
        payload = response.json()
    except ValueError:
        raise HTTPException(status_code=502, detail="Checklist response was not valid JSON.")

    if not isinstance(payload, list):
        raise HTTPException(status_code=502, detail=f"Unexpected checklist response type: {type(payload).__name__}")

    items = []
    for raw in payload:
        if not isinstance(raw, dict):
            continue
        checklist_id = str(raw.get("id") or "").strip()
        if not checklist_id:
            continue
        name = str(raw.get("name") or "").strip() or checklist_id
        items.append({"id": checklist_id, "name": name})

    items.sort(key=lambda item: item["name"].lower())
    return {"items": items, "count": len(items)}


@app.post("/api/public/checklists")
async def list_public_checklists_post(request: PublicChecklistRequest) -> dict:
    return await list_public_checklists(
        profile_id=request.profile_id,
        project_id=request.project_id,
        bearer_token=request.bearer_token,
    )


@app.get("/api/public/projects")
async def list_public_projects(profile_id: Optional[str] = Query(default=None)) -> dict:
    runtime_env = _runtime_env_with_profile(profile_id)

    timeout_seconds = _runtime_timeout_seconds(runtime_env)
    token = await _resolve_internal_token(
        runtime_env,
        bearer_token=runtime_env.get("XCURES_BEARER_TOKEN", ""),
        timeout_seconds=timeout_seconds,
    )

    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {token}",
    }
    project_id_header = _runtime_project_id(runtime_env)
    if project_id_header:
        headers["ProjectId"] = project_id_header

    url = f"{_runtime_base_url(runtime_env)}/api/patient-registry/project"

    def _fetch() -> requests.Response:
        return requests.get(url, headers=headers, timeout=timeout_seconds)

    try:
        response = await asyncio.to_thread(_fetch)
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail=f"Failed to load projects: {exc}")

    if response.status_code < 200 or response.status_code >= 300:
        detail = response.text.strip() or f"HTTP {response.status_code}"
        raise HTTPException(status_code=response.status_code, detail=f"Project request failed: {detail}")

    try:
        payload = response.json()
    except ValueError:
        raise HTTPException(status_code=502, detail="Project response was not valid JSON.")

    if not isinstance(payload, list):
        raise HTTPException(status_code=502, detail=f"Unexpected project response type: {type(payload).__name__}")

    items = []
    for raw in payload:
        if not isinstance(raw, dict):
            continue
        project_id = str(raw.get("id") or "").strip()
        if not project_id:
            continue
        name = str(raw.get("name") or "").strip() or project_id
        items.append({"id": project_id, "name": name})

    items.sort(key=lambda item: item["id"].lower())
    return {"items": items, "count": len(items)}


@app.get("/api/internal/permissions")
async def get_permissions_catalog() -> dict:
    if not PERMISSIONS_CATALOG_PATH.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Permissions catalog not found: {PERMISSIONS_CATALOG_PATH.relative_to(ROOT_DIR)}",
        )
    try:
        payload = json.loads(PERMISSIONS_CATALOG_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to read permissions catalog: {exc}")
    if not isinstance(payload, dict):
        raise HTTPException(status_code=500, detail="Permissions catalog JSON must be an object.")
    permissions = _parse_permissions(payload.get("permissions"))
    items = [{"id": permission, "name": permission} for permission in permissions]
    return {"items": items, "permissions": permissions, "count": len(permissions)}


@app.post("/api/internal/users")
async def list_internal_users(request: InternalApiRequest) -> dict:
    runtime_env = env_store.get_runtime_env()
    timeout_seconds = _runtime_timeout_seconds(runtime_env)
    token = await _resolve_internal_token(
        runtime_env,
        bearer_token=request.bearer_token or runtime_env.get("XCURES_BEARER_TOKEN", ""),
        timeout_seconds=timeout_seconds,
    )
    page_size_raw = runtime_env.get("user_page_size", "50")
    try:
        page_size = max(1, min(250, int(str(page_size_raw).strip() or "50")))
    except Exception:
        page_size = 50
    base_url = _runtime_base_url(runtime_env)
    project_id = _runtime_project_id(runtime_env) or None

    def _fetch_users() -> List[Dict[str, Any]]:
        with requests.Session() as session:
            client = XcuresApiClient(
                session=session,
                base_url=base_url,
                bearer_token=token,
                project_id=project_id,
                timeout_seconds=timeout_seconds,
            )
            return _list_internal_users(client, page_size=page_size)

    try:
        items = await asyncio.to_thread(_fetch_users)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to load users: {exc}")
    return {"items": items, "count": len(items)}


@app.post("/api/internal/user-permissions")
async def get_internal_user_permissions(request: InternalUserPermissionsRequest) -> dict:
    user_id = str(request.user_id or "").strip()
    if not user_id:
        raise HTTPException(status_code=422, detail="user_id is required.")
    runtime_env = env_store.get_runtime_env()
    timeout_seconds = _runtime_timeout_seconds(runtime_env)
    token = await _resolve_internal_token(
        runtime_env,
        bearer_token=request.bearer_token or runtime_env.get("XCURES_BEARER_TOKEN", ""),
        timeout_seconds=timeout_seconds,
    )
    base_url = _runtime_base_url(runtime_env)
    project_id = _runtime_project_id(runtime_env) or None

    def _fetch_user() -> Dict[str, Any]:
        with requests.Session() as session:
            client = XcuresApiClient(
                session=session,
                base_url=base_url,
                bearer_token=token,
                project_id=project_id,
                timeout_seconds=timeout_seconds,
            )
            return _fetch_internal_user_permissions(client, user_id)

    try:
        user = await asyncio.to_thread(_fetch_user)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to load user permissions: {exc}")
    return {"user": user, "permissions": user["permissions"], "count": user["permission_count"]}


@app.post("/api/internal/users-permissions")
async def list_all_internal_user_permissions(request: InternalApiRequest) -> dict:
    runtime_env = env_store.get_runtime_env()
    timeout_seconds = _runtime_timeout_seconds(runtime_env)
    token = await _resolve_internal_token(
        runtime_env,
        bearer_token=request.bearer_token or runtime_env.get("XCURES_BEARER_TOKEN", ""),
        timeout_seconds=timeout_seconds,
    )
    page_size_raw = runtime_env.get("user_page_size", "50")
    try:
        page_size = max(1, min(250, int(str(page_size_raw).strip() or "50")))
    except Exception:
        page_size = 50
    base_url = _runtime_base_url(runtime_env)
    project_id = _runtime_project_id(runtime_env) or None

    def _fetch_all() -> dict:
        with requests.Session() as session:
            client = XcuresApiClient(
                session=session,
                base_url=base_url,
                bearer_token=token,
                project_id=project_id,
                timeout_seconds=timeout_seconds,
            )
            users = _list_internal_users(client, page_size=page_size)
            results: List[Dict[str, Any]] = []
            permission_set = set()
            for user in users:
                user_id = str(user.get("id") or "").strip()
                if not user_id:
                    continue
                try:
                    detail = _fetch_internal_user_permissions(client, user_id)
                    for permission in detail["permissions"]:
                        permission_set.add(permission)
                    results.append(detail)
                except Exception as exc:
                    results.append(
                        {
                            **user,
                            "permissions": [],
                            "permission_count": 0,
                            "error": str(exc),
                        }
                    )
            results.sort(key=lambda item: (str(item.get("name") or "").lower(), str(item.get("email") or "").lower()))
            return {
                "items": results,
                "count": len(results),
                "permission_keys": sorted(permission_set),
            }

    try:
        payload = await asyncio.to_thread(_fetch_all)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to load user permissions matrix: {exc}")
    return payload


@app.post("/api/profiles")
async def create_profile(request: CreateProfileRequest) -> dict:
    try:
        profile = env_store.create_profile(request.profile_id, request.model_dump(mode="json"))
    except KeyError:
        raise HTTPException(status_code=409, detail=f"Profile already exists: {request.profile_id}")
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    return {"profile": profile, **env_store.get_profiles()}


@app.put("/api/profiles/active")
async def set_active_profile(request: SetActiveProfileRequest) -> dict:
    try:
        return env_store.set_active_profile(request.profile_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Profile not found: {exc.args[0]}")


@app.put("/api/profiles/{profile_id}")
async def update_profile(profile_id: str, request: UpdateProfileRequest) -> dict:
    try:
        profile = env_store.update_profile(profile_id, request.model_dump(mode="json"))
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Profile not found: {profile_id}")
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    return {"profile": profile, **env_store.get_profiles()}


@app.delete("/api/profiles/{profile_id}")
async def delete_profile(profile_id: str) -> dict:
    try:
        return env_store.delete_profile(profile_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Profile not found: {profile_id}")
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))


@app.post("/api/jobs", response_model=CreateJobResponse)
async def create_job(request: CreateJobRequest) -> CreateJobResponse:
    try:
        record = await job_manager.create_job(request)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Unknown script_id: {request.script_id}")
    except ValueError as exc:
        detail = str(exc)
        try:
            parsed = json.loads(detail)
            detail = parsed
        except Exception:
            pass
        raise HTTPException(status_code=422, detail=detail)

    return CreateJobResponse(job_id=record.job_id, status=record.status)


@app.get("/api/jobs")
async def list_jobs() -> List[dict]:
    return [job.model_dump(mode="json") for job in job_manager.list_jobs()]


@app.get("/api/jobs/{job_id}")
async def get_job(job_id: str) -> dict:
    try:
        job = job_manager.get_job(job_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Job not found")
    return job.model_dump(mode="json")


@app.get("/api/jobs/{job_id}/events")
async def stream_job_events(request: Request, job_id: str) -> StreamingResponse:
    try:
        job_manager.get_job(job_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Job not found")

    last_event_id = request.headers.get("last-event-id")
    start_event_id = int(last_event_id) if last_event_id and last_event_id.isdigit() else 0

    async def event_generator() -> AsyncIterator[str]:
        for event in job_manager.events_since(job_id, start_event_id):
            yield _to_sse(event.event_id, event.type, event.model_dump(mode="json"))

        queue = job_manager.subscribe(job_id)
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=15)
                    yield _to_sse(event.event_id, event.type, event.model_dump(mode="json"))
                except asyncio.TimeoutError:
                    yield ": ping\n\n"
        finally:
            job_manager.unsubscribe(job_id, queue)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.post("/api/jobs/{job_id}/stdin")
async def send_stdin(job_id: str, payload: SendStdinRequest) -> dict:
    try:
        await job_manager.send_stdin(job_id, payload.text)
    except KeyError:
        raise HTTPException(status_code=404, detail="Job not found")
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return {"ok": True}


@app.post("/api/jobs/{job_id}/cancel")
async def cancel_job(job_id: str) -> dict:
    try:
        await job_manager.cancel_job(job_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"ok": True}


@app.get("/api/jobs/{job_id}/artifacts")
async def list_job_artifacts(job_id: str) -> dict:
    try:
        job = job_manager.get_job(job_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Job not found")

    items = []
    for rel_path in job.artifacts:
        items.append(
            {
                "path": rel_path,
                "url": f"/api/artifact?path={quote(rel_path)}",
            }
        )
    return {"items": items}


@app.get("/api/artifact")
async def get_artifact(path: str = Query(...)) -> FileResponse:
    try:
        target = job_manager.resolve_artifact_path(path)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Artifact not found")
    return FileResponse(path=target, filename=target.name)


@app.post("/api/uploads/csv")
async def upload_csv(file: UploadFile = File(...)) -> dict:
    filename = (file.filename or "upload.csv").strip()
    suffix = Path(filename).suffix.lower()
    if suffix != ".csv":
        raise HTTPException(status_code=400, detail="Only .csv files are allowed.")

    UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    safe_name = Path(filename).name.replace(" ", "_")
    target = UPLOADS_DIR / safe_name
    if target.exists():
        stem = Path(safe_name).stem
        ext = Path(safe_name).suffix
        idx = 2
        while True:
            candidate = UPLOADS_DIR / f"{stem}_{idx}{ext}"
            if not candidate.exists():
                target = candidate
                break
            idx += 1

    data = await file.read()
    if not data:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")
    target.write_bytes(data)
    rel_path = str(target.relative_to(ROOT_DIR))
    return {"path": rel_path}


@app.post("/api/uploads/file")
async def upload_file(file: UploadFile = File(...)) -> dict:
    filename = (file.filename or "upload.bin").strip()
    safe_name = Path(filename).name.replace(" ", "_")
    UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    target = UPLOADS_DIR / safe_name
    if target.exists():
        stem = target.stem
        ext = target.suffix
        idx = 2
        while True:
            candidate = UPLOADS_DIR / f"{stem}_{idx}{ext}"
            if not candidate.exists():
                target = candidate
                break
            idx += 1

    data = await file.read()
    if not data:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")
    target.write_bytes(data)
    return {"path": str(target.relative_to(ROOT_DIR))}


@app.post("/api/uploads/folder")
async def create_upload_folder(folder_name: str = Body(..., embed=True)) -> dict:
    clean = folder_name.strip().replace("/", "_").replace("\\", "_")
    if not clean:
        raise HTTPException(status_code=400, detail="Folder name is required.")
    target = UPLOADS_DIR / clean
    target.mkdir(parents=True, exist_ok=True)
    return {"path": str(target.relative_to(ROOT_DIR))}


def _to_sse(event_id: int, event_name: str, data: dict) -> str:
    payload = json.dumps(data, ensure_ascii=True)
    return f"id: {event_id}\nevent: {event_name}\ndata: {payload}\n\n"


@app.get("/")
async def serve_index():
    index_file = FRONTEND_DIST / "index.html"
    if index_file.exists():
        return FileResponse(index_file)
    return JSONResponse(
        status_code=503,
        content={
            "message": "Frontend not built yet. Run: npm --prefix web_ui install && npm --prefix web_ui run build",
            "api_only": True,
        },
    )


if FRONTEND_DIST.exists():
    app.mount("/assets", StaticFiles(directory=FRONTEND_DIST / "assets"), name="assets")


@app.get("/{full_path:path}")
async def spa_fallback(full_path: str):
    if full_path.startswith("api/"):
        raise HTTPException(status_code=404, detail="Not found")
    index_file = FRONTEND_DIST / "index.html"
    if index_file.exists():
        return FileResponse(index_file)
    return JSONResponse(status_code=404, content={"message": "Not found"})
