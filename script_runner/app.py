from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import AsyncIterator, Dict, List
from urllib.parse import quote

import requests
from fastapi import Body, FastAPI, File, HTTPException, Query, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from auth_common import fetch_client_credentials_token
from .env_store import EnvStore
from .job_manager import JobManager
from .models import (
    CreateProfileRequest,
    CreateJobRequest,
    CreateJobResponse,
    ListProjectsRequest,
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

env_store = EnvStore(ROOT_DIR / ".env")
job_manager = JobManager(
    root_dir=ROOT_DIR,
    env_store=env_store,
    scripts_by_id=SCRIPT_INDEX,
    history_path=HISTORY_PATH,
)

app = FastAPI(title="xCures Local Script Runner", version="0.1.0")


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
    runtime_env = env_store.get_runtime_env()
    token = (request.bearer_token or runtime_env.get("XCURES_BEARER_TOKEN", "")).strip()

    timeout_raw = runtime_env.get("request_timeout_seconds", "60")
    try:
        timeout_seconds = int(str(timeout_raw).strip() or "60")
    except Exception:
        timeout_seconds = 60

    if not token:
        client_id = runtime_env.get("XCURES_CLIENT_ID", "").strip()
        client_secret = runtime_env.get("XCURES_CLIENT_SECRET", "").strip()
        if not client_id or not client_secret:
            raise HTTPException(
                status_code=422,
                detail=(
                    "Unable to load tenant projects. Provide Bearer token, or configure "
                    "XCURES_CLIENT_ID and XCURES_CLIENT_SECRET in the active profile."
                ),
            )

        auth_base_url = runtime_env.get("BASE_URL", PARTNER_BASE_URL).strip().rstrip("/") or PARTNER_BASE_URL
        auth_url = runtime_env.get("AUTH_URL", f"{auth_base_url}/oauth/token").strip()

        try:
            with requests.Session() as auth_session:
                token = fetch_client_credentials_token(
                    auth_session,
                    auth_url=auth_url,
                    client_id=client_id,
                    client_secret=client_secret,
                    timeout_seconds=timeout_seconds,
                )
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"Failed to obtain access token: {exc}")

    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {token}",
    }
    project_id_header = runtime_env.get("XCURES_PROJECT_ID", "").strip()
    if project_id_header:
        headers["ProjectId"] = project_id_header

    url = f"{PARTNER_BASE_URL}/api/patient-registry/project"

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
