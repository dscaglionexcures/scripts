from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from script_runner.env_store import EnvStore
from script_runner.job_manager import JobManager
from script_runner.models import CreateJobRequest, ScriptDefinition, ScriptField, SafetyMode


async def wait_for_status(manager: JobManager, job_id: str, final_states: set[str], timeout: float = 10.0) -> str:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        status = manager.get_job(job_id).status
        if status in final_states:
            return status
        await asyncio.sleep(0.05)
    raise TimeoutError(f"Job {job_id} did not reach {final_states} in time.")


@pytest.mark.asyncio
async def test_job_lifecycle_with_stdin(tmp_path: Path) -> None:
    script_path = tmp_path / "echo_prompt.py"
    script_path.write_text(
        "print('ready', flush=True)\n"
        "value = input('Enter value: ')\n"
        "print(f'got:{value}', flush=True)\n",
        encoding="utf-8",
    )
    env_store = EnvStore(tmp_path / ".env")
    history_path = tmp_path / "logs" / "history.json"
    definition = ScriptDefinition(
        id="echo_prompt",
        name="Echo Prompt",
        description="prompt test",
        file_path=script_path.name,
        safety=SafetyMode.READ_ONLY,
    )
    manager = JobManager(
        root_dir=tmp_path,
        env_store=env_store,
        scripts_by_id={definition.id: definition},
        history_path=history_path,
    )
    await manager.start()
    try:
        request = CreateJobRequest(script_id=definition.id, field_values={}, raw_args="")
        record = await manager.create_job(request)

        # Wait until running so stdin is attached.
        for _ in range(100):
            if manager.get_job(record.job_id).status == "running":
                break
            await asyncio.sleep(0.05)
        await manager.send_stdin(record.job_id, "hello-world")

        status = await wait_for_status(manager, record.job_id, {"succeeded", "failed", "canceled"})
        assert status == "succeeded"
        events = manager.get_job(record.job_id).log_events
        messages = [evt.message for evt in events]
        assert any("got:hello-world" in message for message in messages)
    finally:
        await manager.shutdown()


@pytest.mark.asyncio
async def test_queue_runs_one_job_at_a_time(tmp_path: Path) -> None:
    script_path = tmp_path / "slow_job.py"
    script_path.write_text(
        "import time\n"
        "print('start', flush=True)\n"
        "time.sleep(0.4)\n"
        "print('done', flush=True)\n",
        encoding="utf-8",
    )

    env_store = EnvStore(tmp_path / ".env")
    history_path = tmp_path / "logs" / "history.json"
    definition = ScriptDefinition(
        id="slow_job",
        name="Slow Job",
        description="queue test",
        file_path=script_path.name,
        safety=SafetyMode.READ_ONLY,
    )
    manager = JobManager(
        root_dir=tmp_path,
        env_store=env_store,
        scripts_by_id={definition.id: definition},
        history_path=history_path,
    )
    await manager.start()
    try:
        first = await manager.create_job(CreateJobRequest(script_id=definition.id))
        second = await manager.create_job(CreateJobRequest(script_id=definition.id))

        await wait_for_status(manager, first.job_id, {"succeeded", "failed", "canceled"})
        await wait_for_status(manager, second.job_id, {"succeeded", "failed", "canceled"})

        first_job = manager.get_job(first.job_id)
        second_job = manager.get_job(second.job_id)
        assert first_job.started_at is not None
        assert first_job.finished_at is not None
        assert second_job.started_at is not None
        assert second_job.started_at >= first_job.finished_at
    finally:
        await manager.shutdown()


def test_validation_blocks_missing_env(tmp_path: Path) -> None:
    script_path = tmp_path / "noop.py"
    script_path.write_text("print('ok')\n", encoding="utf-8")

    env_store = EnvStore(tmp_path / ".env")
    history_path = tmp_path / "logs" / "history.json"
    definition = ScriptDefinition(
        id="needs_env",
        name="Needs Env",
        description="validation",
        file_path=script_path.name,
        safety=SafetyMode.READ_ONLY,
        required_env=["XCURES_BEARER_TOKEN"],
    )
    manager = JobManager(
        root_dir=tmp_path,
        env_store=env_store,
        scripts_by_id={definition.id: definition},
        history_path=history_path,
    )

    _, _, _, _, validation = manager.validate_request(
        CreateJobRequest(script_id=definition.id, field_values={}, raw_args="")
    )
    assert validation.missing_env == ["XCURES_BEARER_TOKEN"]


def test_validation_uses_active_profile_values(tmp_path: Path) -> None:
    script_path = tmp_path / "noop.py"
    script_path.write_text("print('ok')\n", encoding="utf-8")

    env_path = tmp_path / ".env"
    env_path.write_text(
        "XCURES_PROFILE__DEMO__CLIENT_ID=demo-id\n"
        "XCURES_PROFILE__DEMO__CLIENT_SECRET=demo-secret\n"
        "ACTIVE_XCURES_PROFILE=DEMO\n",
        encoding="utf-8",
    )
    env_store = EnvStore(env_path)
    history_path = tmp_path / "logs" / "history.json"
    definition = ScriptDefinition(
        id="needs_creds",
        name="Needs Creds",
        description="validation",
        file_path=script_path.name,
        safety=SafetyMode.READ_ONLY,
        required_env=["XCURES_CLIENT_ID", "XCURES_CLIENT_SECRET"],
    )
    manager = JobManager(
        root_dir=tmp_path,
        env_store=env_store,
        scripts_by_id={definition.id: definition},
        history_path=history_path,
    )

    _, _, _, _, validation = manager.validate_request(
        CreateJobRequest(script_id=definition.id, field_values={}, raw_args="")
    )
    assert validation.missing_env == []


def test_internal_api_requires_bearer_token_per_run(tmp_path: Path) -> None:
    script_path = tmp_path / "noop.py"
    script_path.write_text("print('ok')\n", encoding="utf-8")

    env_path = tmp_path / ".env"
    env_path.write_text("XCURES_BEARER_TOKEN=from-env\n", encoding="utf-8")
    env_store = EnvStore(env_path)
    history_path = tmp_path / "logs" / "history.json"
    definition = ScriptDefinition(
        id="internal_noop",
        name="Internal Noop",
        description="validation",
        file_path=script_path.name,
        safety=SafetyMode.READ_ONLY,
        required_env=["XCURES_BEARER_TOKEN"],
        tags=["internal-api"],
    )
    manager = JobManager(
        root_dir=tmp_path,
        env_store=env_store,
        scripts_by_id={definition.id: definition},
        history_path=history_path,
    )

    _, _, _, _, missing_validation = manager.validate_request(
        CreateJobRequest(script_id=definition.id, field_values={}, raw_args="")
    )
    assert "internal_bearer_token" in missing_validation.missing_fields

    _, _, _, _, ok_validation = manager.validate_request(
        CreateJobRequest(
            script_id=definition.id,
            field_values={},
            raw_args="",
            internal_bearer_token="token-from-ui",
        )
    )
    assert "internal_bearer_token" not in ok_validation.missing_fields


@pytest.mark.asyncio
async def test_env_alias_override_is_applied_to_process_env(tmp_path: Path) -> None:
    script_path = tmp_path / "print_token.py"
    script_path.write_text(
        "import os\n"
        "print(os.environ.get('XCURES_BEARER_TOKEN', 'missing'), flush=True)\n",
        encoding="utf-8",
    )

    env_store = EnvStore(tmp_path / ".env")
    history_path = tmp_path / "logs" / "history.json"
    definition = ScriptDefinition(
        id="print_token",
        name="Print Token",
        description="env override test",
        file_path=script_path.name,
        safety=SafetyMode.READ_ONLY,
        required_env=["XCURES_BEARER_TOKEN"],
        fields=[
            ScriptField(
                id="bearer_token",
                label="Bearer Token",
                env_alias="XCURES_BEARER_TOKEN",
            )
        ],
    )
    manager = JobManager(
        root_dir=tmp_path,
        env_store=env_store,
        scripts_by_id={definition.id: definition},
        history_path=history_path,
    )
    await manager.start()
    try:
        job = await manager.create_job(
            CreateJobRequest(
                script_id=definition.id,
                field_values={"bearer_token": "token-from-field"},
            )
        )
        status = await wait_for_status(manager, job.job_id, {"succeeded", "failed", "canceled"})
        assert status == "succeeded"
        messages = [evt.message for evt in manager.get_job(job.job_id).log_events]
        assert any("token-from-field" in message for message in messages)
    finally:
        await manager.shutdown()


@pytest.mark.asyncio
async def test_bulk_create_always_writes_logs_under_logs_folder(tmp_path: Path) -> None:
    script_path = tmp_path / "noop.py"
    script_path.write_text("print('ok')\n", encoding="utf-8")

    env_store = EnvStore(tmp_path / ".env")
    history_path = tmp_path / "logs" / "history.json"
    definition = ScriptDefinition(
        id="bulk_create_users_from_csv",
        name="Bulk Create New Users",
        description="bulk create test",
        file_path=script_path.name,
        safety=SafetyMode.MUTATING,
        fields=[ScriptField(id="csv", label="CSV Path", arg="--csv", required=True)],
    )
    manager = JobManager(
        root_dir=tmp_path,
        env_store=env_store,
        scripts_by_id={definition.id: definition},
        history_path=history_path,
    )

    record = await manager.create_job(
        CreateJobRequest(
            script_id=definition.id,
            field_values={"csv": "users.csv"},
            raw_args="--log-file custom.log",
        )
    )

    assert "--log-file" in record.args
    log_index = record.args.index("--log-file")
    assert log_index + 1 < len(record.args)
    assert record.args[log_index + 1].startswith("logs/bulk_create_users_")
    assert "custom.log" not in record.args
    assert not any(arg.startswith("--log-file=") for arg in record.args)


@pytest.mark.asyncio
async def test_update_users_new_projects_forces_audit_and_backup_paths(tmp_path: Path) -> None:
    script_path = tmp_path / "noop.py"
    script_path.write_text("print('ok')\n", encoding="utf-8")

    env_store = EnvStore(tmp_path / ".env")
    history_path = tmp_path / "logs" / "history.json"
    definition = ScriptDefinition(
        id="update_users_new_projects",
        name="Update Users with New Projects",
        description="update users test",
        file_path=script_path.name,
        safety=SafetyMode.MUTATING,
    )
    manager = JobManager(
        root_dir=tmp_path,
        env_store=env_store,
        scripts_by_id={definition.id: definition},
        history_path=history_path,
    )

    record = await manager.create_job(
        CreateJobRequest(
            script_id=definition.id,
            raw_args="--audit-log custom.jsonl --backup-path custom.json --audit-log=raw.jsonl --backup-path=raw.json",
        )
    )

    assert "--audit-log" in record.args
    audit_idx = record.args.index("--audit-log")
    assert audit_idx + 1 < len(record.args)
    assert record.args[audit_idx + 1].startswith("logs/update_users_new_projects_")
    assert record.args[audit_idx + 1].endswith(".jsonl")

    assert "--backup-path" in record.args
    backup_idx = record.args.index("--backup-path")
    assert backup_idx + 1 < len(record.args)
    assert record.args[backup_idx + 1].startswith("backups/update_users_new_projects/prewrite_snapshot_")
    assert record.args[backup_idx + 1].endswith(".json")

    assert "custom.jsonl" not in record.args
    assert "custom.json" not in record.args
    assert not any(arg.startswith("--audit-log=") for arg in record.args)
    assert not any(arg.startswith("--backup-path=") for arg in record.args)
