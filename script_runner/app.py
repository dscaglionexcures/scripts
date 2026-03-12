from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import AsyncIterator, Dict, List
from urllib.parse import quote

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from .env_store import EnvStore
from .job_manager import JobManager
from .models import (
    CreateProfileRequest,
    CreateJobRequest,
    CreateJobResponse,
    SendStdinRequest,
    SetActiveProfileRequest,
    UpdateProfileRequest,
    UpdateEnvRequest,
)
from .script_registry import ENV_CATALOG, ROOT_DIR, SCRIPT_DEFINITIONS, SCRIPT_INDEX


APP_HOST = "127.0.0.1"
APP_PORT = 8765
FRONTEND_DIST = ROOT_DIR / "web_ui" / "dist"
HISTORY_PATH = ROOT_DIR / "logs" / "script_runner_job_history.json"

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
