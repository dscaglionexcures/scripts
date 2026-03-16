from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class SafetyMode(str, Enum):
    READ_ONLY = "read-only"
    MUTATING = "mutating"


class ModeBehavior(str, Enum):
    NONE = "none"
    DRY_RUN_FLAG = "dry-run-flag"
    DRY_RUN_APPLY_FLAGS = "dry-run-apply-flags"


class FieldType(str, Enum):
    TEXT = "text"
    NUMBER = "number"
    BOOLEAN = "boolean"
    PATH = "path"
    SELECT = "select"


class ScriptField(BaseModel):
    id: str
    label: str
    arg: Optional[str] = None
    type: FieldType = FieldType.TEXT
    required: bool = False
    default: Any = None
    placeholder: Optional[str] = None
    description: Optional[str] = None
    choices: Optional[List[str]] = None
    true_arg: Optional[str] = None
    false_arg: Optional[str] = None
    repeatable: bool = False
    delimiter: str = ","
    env_alias: Optional[str] = None


class ScriptDefinition(BaseModel):
    id: str
    name: str
    description: str
    file_path: str
    safety: SafetyMode
    fields: List[ScriptField] = Field(default_factory=list)
    required_env: List[str] = Field(default_factory=list)
    env_sets_any: List[List[str]] = Field(default_factory=list)
    mode_behavior: ModeBehavior = ModeBehavior.NONE
    default_mode: Literal["dry-run", "apply"] = "apply"
    tags: List[str] = Field(default_factory=list)


class EnvCatalogEntry(BaseModel):
    key: str
    description: str
    example: Optional[str] = None
    secret: bool = False


class EnvValueView(BaseModel):
    key: str
    value: str
    has_value: bool
    secret: bool
    from_catalog: bool
    description: Optional[str] = None
    used_by_scripts: List[str] = Field(default_factory=list)


class JobLogEvent(BaseModel):
    event_id: int
    ts: datetime
    type: Literal["stdout", "stderr", "status", "system"]
    message: str


class JobRecord(BaseModel):
    job_id: str
    script_id: str
    script_name: str
    status: Literal["queued", "running", "succeeded", "failed", "canceled"]
    mode: Optional[Literal["dry-run", "apply"]] = None
    args: List[str] = Field(default_factory=list)
    raw_args: Optional[str] = None
    field_values: Dict[str, Any] = Field(default_factory=dict)
    env_overrides: Dict[str, str] = Field(default_factory=dict)
    created_at: datetime
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    exit_code: Optional[int] = None
    error: Optional[str] = None
    artifacts: List[str] = Field(default_factory=list)
    log_events: List[JobLogEvent] = Field(default_factory=list)


class CreateJobRequest(BaseModel):
    script_id: str
    mode: Optional[Literal["dry-run", "apply"]] = None
    field_values: Dict[str, Any] = Field(default_factory=dict)
    raw_args: Optional[str] = None
    internal_bearer_token: Optional[str] = None


class CreateJobResponse(BaseModel):
    job_id: str
    status: str


class UpdateEnvRequest(BaseModel):
    updates: Dict[str, str]
    clear_missing: bool = False


class SendStdinRequest(BaseModel):
    text: str


class ListProjectsRequest(BaseModel):
    bearer_token: Optional[str] = None


class SetActiveProfileRequest(BaseModel):
    profile_id: str = ""


class ScriptValidationError(BaseModel):
    missing_env: List[str] = Field(default_factory=list)
    missing_any_env_sets: List[List[str]] = Field(default_factory=list)
    missing_fields: List[str] = Field(default_factory=list)


class ProfileFields(BaseModel):
    name: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    bearer_token: Optional[str] = None
    project_id: Optional[str] = None
    base_url: Optional[str] = None
    auth_url: Optional[str] = None


class CreateProfileRequest(ProfileFields):
    profile_id: str


class UpdateProfileRequest(ProfileFields):
    pass
