from __future__ import annotations

import asyncio
import contextlib
import json
import os
import re
import shlex
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple

from .env_store import EnvStore
from .models import (
    CreateJobRequest,
    JobLogEvent,
    JobRecord,
    ModeBehavior,
    ScriptDefinition,
    ScriptValidationError,
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    return False


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return False


class JobManager:
    def __init__(
        self,
        *,
        root_dir: Path,
        env_store: EnvStore,
        scripts_by_id: Dict[str, ScriptDefinition],
        history_path: Path,
    ):
        self.root_dir = root_dir
        self.env_store = env_store
        self.scripts_by_id = scripts_by_id
        self.history_path = history_path
        self.history_path.parent.mkdir(parents=True, exist_ok=True)

        self.jobs: Dict[str, JobRecord] = {}
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._worker_task: Optional[asyncio.Task[None]] = None
        self._subscribers: Dict[str, List[asyncio.Queue[JobLogEvent]]] = {}
        self._processes: Dict[str, asyncio.subprocess.Process] = {}
        self._cancel_requested: set[str] = set()
        self._event_counters: Dict[str, int] = {}
        self._history_limit = 150

        self.allowed_artifact_dirs = [
            (self.root_dir / "logs").resolve(),
            (self.root_dir / "downloads").resolve(),
            (self.root_dir / "backups").resolve(),
        ]
        self.allowed_root_file_exts = {".csv", ".pdf", ".json", ".jsonl", ".txt", ".xml"}

        self._load_history()

    async def start(self) -> None:
        if self._worker_task is None:
            self._worker_task = asyncio.create_task(self._worker(), name="script-runner-worker")

    async def shutdown(self) -> None:
        if self._worker_task is not None:
            self._worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._worker_task
            self._worker_task = None

        for job_id, process in list(self._processes.items()):
            try:
                process.terminate()
            except ProcessLookupError:
                pass
            self._cancel_requested.add(job_id)

    def list_jobs(self) -> List[JobRecord]:
        return sorted(self.jobs.values(), key=lambda j: j.created_at, reverse=True)

    def get_job(self, job_id: str) -> JobRecord:
        if job_id not in self.jobs:
            raise KeyError(job_id)
        return self.jobs[job_id]

    def validate_request(self, request: CreateJobRequest) -> Tuple[ScriptDefinition, List[str], str, Dict[str, str], ScriptValidationError]:
        if request.script_id not in self.scripts_by_id:
            raise KeyError(request.script_id)
        script = self.scripts_by_id[request.script_id]
        mode = request.mode or script.default_mode
        field_values = request.field_values or {}

        args: List[str] = []
        missing_fields: List[str] = []
        env_overrides: Dict[str, str] = {}
        internal_bearer = (request.internal_bearer_token or "").strip()
        if internal_bearer:
            env_overrides["XCURES_BEARER_TOKEN"] = internal_bearer

        for field in script.fields:
            value = field_values.get(field.id, field.default)
            if field.required and _is_blank(value):
                missing_fields.append(field.id)
                continue

            if field.env_alias and not _is_blank(value):
                env_overrides[field.env_alias] = str(value).strip()

            if field.type.value == "boolean":
                bool_value = _as_bool(value)
                if bool_value:
                    if field.true_arg:
                        args.append(field.true_arg)
                    elif field.arg:
                        args.append(field.arg)
                else:
                    if field.false_arg:
                        args.append(field.false_arg)
                continue

            if _is_blank(value):
                continue
            text_value = str(value).strip()
            if field.repeatable and field.arg:
                parts = [chunk.strip() for chunk in text_value.split(field.delimiter) if chunk.strip()]
                for part in parts:
                    args.extend([field.arg, part])
                continue
            if field.arg:
                args.extend([field.arg, text_value])

        if script.mode_behavior == ModeBehavior.DRY_RUN_FLAG:
            if mode == "dry-run":
                args.append("--dry-run")
        elif script.mode_behavior == ModeBehavior.DRY_RUN_APPLY_FLAGS:
            args.append("--dry-run" if mode == "dry-run" else "--apply")

        raw_args = (request.raw_args or "").strip()
        if raw_args:
            try:
                args.extend(shlex.split(raw_args))
            except ValueError as exc:
                missing_fields.append(f"raw_args_parse_error: {exc}")

        if "internal-api" in script.tags and not internal_bearer:
            missing_fields.append("internal_bearer_token")

        missing_env = self.env_store.validate_required_keys(script.required_env, extra_values=env_overrides)
        missing_any_env_sets = self.env_store.validate_any_keyset(script.env_sets_any, extra_values=env_overrides)
        validation = ScriptValidationError(
            missing_env=missing_env,
            missing_any_env_sets=missing_any_env_sets,
            missing_fields=missing_fields,
        )
        return script, args, mode, env_overrides, validation

    async def create_job(self, request: CreateJobRequest) -> JobRecord:
        script, args, mode, env_overrides, validation = self.validate_request(request)
        if validation.missing_env or validation.missing_any_env_sets or validation.missing_fields:
            raise ValueError(validation.model_dump_json())

        job_id = str(uuid.uuid4())
        created_at = utc_now()
        record = JobRecord(
            job_id=job_id,
            script_id=script.id,
            script_name=script.name,
            status="queued",
            mode=mode,
            args=args,
            raw_args=request.raw_args,
            field_values=request.field_values or {},
            env_overrides=env_overrides,
            created_at=created_at,
        )
        self.jobs[job_id] = record
        self._event_counters[job_id] = 0
        self._append_event(
            job_id,
            "status",
            f"Queued: {sys.executable} {script.file_path} {' '.join(args)}",
        )
        await self._queue.put(job_id)
        self._persist_history()
        return record

    async def send_stdin(self, job_id: str, text: str) -> None:
        job = self.get_job(job_id)
        if job.status != "running":
            raise RuntimeError("Job is not running.")
        process = self._processes.get(job_id)
        if process is None or process.stdin is None:
            raise RuntimeError("Job stdin is unavailable.")
        payload = text if text.endswith("\n") else text + "\n"
        process.stdin.write(payload.encode("utf-8"))
        await process.stdin.drain()
        self._append_event(job_id, "system", f"[stdin] {text}")
        self._persist_history()

    async def cancel_job(self, job_id: str) -> None:
        job = self.get_job(job_id)
        if job.status in {"succeeded", "failed", "canceled"}:
            return

        if job.status == "queued":
            job.status = "canceled"
            job.finished_at = utc_now()
            self._append_event(job_id, "status", "Canceled while queued.")
            self._persist_history()
            return

        process = self._processes.get(job_id)
        self._cancel_requested.add(job_id)
        if process is not None:
            try:
                process.terminate()
            except ProcessLookupError:
                pass
            self._append_event(job_id, "status", "Termination requested.")
            self._persist_history()

    def subscribe(self, job_id: str) -> asyncio.Queue[JobLogEvent]:
        if job_id not in self.jobs:
            raise KeyError(job_id)
        queue: asyncio.Queue[JobLogEvent] = asyncio.Queue()
        self._subscribers.setdefault(job_id, []).append(queue)
        return queue

    def unsubscribe(self, job_id: str, queue: asyncio.Queue[JobLogEvent]) -> None:
        subs = self._subscribers.get(job_id)
        if not subs:
            return
        with contextlib.suppress(ValueError):
            subs.remove(queue)
        if not subs:
            self._subscribers.pop(job_id, None)

    def events_since(self, job_id: str, event_id: int) -> List[JobLogEvent]:
        job = self.get_job(job_id)
        return [evt for evt in job.log_events if evt.event_id > event_id]

    def resolve_artifact_path(self, relative_path: str) -> Path:
        candidate = (self.root_dir / relative_path).resolve()
        if self._is_allowed_artifact(candidate):
            return candidate
        raise FileNotFoundError(relative_path)

    async def _worker(self) -> None:
        while True:
            job_id = await self._queue.get()
            try:
                job = self.jobs.get(job_id)
                if not job:
                    continue
                if job.status == "canceled":
                    continue
                await self._run_job(job_id)
            except Exception as exc:
                if job_id in self.jobs:
                    job = self.jobs[job_id]
                    job.status = "failed"
                    job.error = str(exc)
                    job.finished_at = utc_now()
                    self._append_event(job_id, "status", f"Runner failure: {exc}")
                    self._persist_history()
            finally:
                self._queue.task_done()

    async def _run_job(self, job_id: str) -> None:
        job = self.jobs[job_id]
        script = self.scripts_by_id[job.script_id]
        command = [sys.executable, "-u", script.file_path, *job.args]
        start_dt = utc_now()
        start_ts = start_dt.timestamp()
        job.status = "running"
        job.started_at = start_dt
        self._append_event(job_id, "status", "Running.")
        self._persist_history()

        env_values = self.env_store.get_runtime_env()

        process_env = os.environ.copy()
        process_env.update(env_values)
        process_env.update(job.env_overrides or {})
        process = await asyncio.create_subprocess_exec(
            *command,
            cwd=str(self.root_dir),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            stdin=asyncio.subprocess.PIPE,
            env=process_env,
        )
        self._processes[job_id] = process

        stdout_task = asyncio.create_task(self._consume_stream(job_id, process.stdout, "stdout"))
        stderr_task = asyncio.create_task(self._consume_stream(job_id, process.stderr, "stderr"))
        return_code = await process.wait()
        await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)

        self._processes.pop(job_id, None)
        was_canceled = job_id in self._cancel_requested
        self._cancel_requested.discard(job_id)

        job.exit_code = return_code
        job.finished_at = utc_now()
        if was_canceled:
            job.status = "canceled"
            self._append_event(job_id, "status", "Canceled.")
        elif return_code == 0:
            job.status = "succeeded"
            self._append_event(job_id, "status", "Completed successfully.")
        else:
            job.status = "failed"
            self._append_event(job_id, "status", f"Exited with code {return_code}.")

        job.artifacts = self._collect_artifacts_since(start_ts)
        self._persist_history()

    async def _consume_stream(
        self,
        job_id: str,
        stream: Optional[asyncio.StreamReader],
        stream_type: str,
    ) -> None:
        if stream is None:
            return
        buffer = ""
        split_pattern = re.compile(r"[\r\n]")
        while True:
            chunk = await stream.read(1024)
            if not chunk:
                break
            buffer += chunk.decode("utf-8", errors="replace")
            while True:
                match = split_pattern.search(buffer)
                if not match:
                    break
                text = buffer[: match.start()].strip()
                buffer = buffer[match.end() :]
                if text:
                    self._append_event(job_id, stream_type, text)

        remainder = buffer.strip()
        if remainder:
            self._append_event(job_id, stream_type, remainder)

    def _append_event(self, job_id: str, event_type: str, message: str) -> None:
        job = self.jobs[job_id]
        next_id = self._event_counters.get(job_id, 0) + 1
        self._event_counters[job_id] = next_id
        evt = JobLogEvent(event_id=next_id, ts=utc_now(), type=event_type, message=message)
        job.log_events.append(evt)
        if len(job.log_events) > 4000:
            job.log_events = job.log_events[-4000:]
        for subscriber in self._subscribers.get(job_id, []):
            subscriber.put_nowait(evt)

    def _collect_artifacts_since(self, start_ts: float) -> List[str]:
        found: List[Path] = []
        cutoff = start_ts - 1.0

        for directory in self.allowed_artifact_dirs:
            if not directory.exists():
                continue
            for path in directory.rglob("*"):
                if not path.is_file():
                    continue
                try:
                    if path.stat().st_mtime >= cutoff:
                        found.append(path.resolve())
                except FileNotFoundError:
                    continue

        for path in self.root_dir.iterdir():
            if not path.is_file():
                continue
            if path.suffix.lower() not in self.allowed_root_file_exts:
                continue
            try:
                if path.stat().st_mtime >= cutoff:
                    found.append(path.resolve())
            except FileNotFoundError:
                continue

        unique: List[str] = []
        seen = set()
        for path in sorted(found, key=lambda p: p.stat().st_mtime, reverse=True):
            rel = str(path.relative_to(self.root_dir))
            if rel in seen:
                continue
            seen.add(rel)
            unique.append(rel)
        return unique[:100]

    def _is_allowed_artifact(self, path: Path) -> bool:
        for directory in self.allowed_artifact_dirs:
            if path.is_relative_to(directory):
                return path.is_file()
        if path.parent == self.root_dir and path.suffix.lower() in self.allowed_root_file_exts:
            return path.is_file()
        return False

    def _load_history(self) -> None:
        if not self.history_path.exists():
            return
        try:
            payload = json.loads(self.history_path.read_text(encoding="utf-8"))
        except Exception:
            return
        if not isinstance(payload, list):
            return
        for row in payload:
            try:
                job = JobRecord.model_validate(row)
            except Exception:
                continue
            self.jobs[job.job_id] = job
            last_event_id = max((evt.event_id for evt in job.log_events), default=0)
            self._event_counters[job.job_id] = last_event_id

    def _persist_history(self) -> None:
        recent = sorted(self.jobs.values(), key=lambda j: j.created_at, reverse=True)[: self._history_limit]
        payload = [job.model_dump(mode="json") for job in recent]
        tmp_path = self.history_path.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
        tmp_path.replace(self.history_path)
