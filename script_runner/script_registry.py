from __future__ import annotations

from pathlib import Path
from typing import Dict, List

from .models import (
    EnvCatalogEntry,
    FieldType,
    ModeBehavior,
    SafetyMode,
    ScriptDefinition,
    ScriptField,
)


ROOT_DIR = Path(__file__).resolve().parent.parent


ENV_CATALOG: List[EnvCatalogEntry] = [
    EnvCatalogEntry(
        key="user_page_size",
        description="Default user page size for internal user-list operations.",
        example="25",
        secret=False,
    ),
    EnvCatalogEntry(
        key="request_timeout_seconds",
        description="Default request timeout (seconds) for API operations.",
        example="60",
        secret=False,
    ),
    EnvCatalogEntry(
        key="max_retries",
        description="Default max retries for retryable API calls.",
        example="2",
        secret=False,
    ),
    EnvCatalogEntry(
        key="backoff_seconds",
        description="Default backoff base seconds for retryable API calls.",
        example="1.0",
        secret=False,
    ),
    EnvCatalogEntry(
        key="XCURES_CLIENT_ID",
        description="xCures OAuth client id for client credentials auth.",
        secret=False,
    ),
    EnvCatalogEntry(
        key="XCURES_CLIENT_SECRET",
        description="xCures OAuth client secret for client credentials auth.",
        secret=True,
    ),
    EnvCatalogEntry(
        key="XCURES_PROJECT_ID",
        description="Default projectId header value for API calls.",
        secret=False,
    ),
    EnvCatalogEntry(
        key="BASE_URL",
        description="Base URL override for xCures APIs.",
        example="https://partner.xcures.com",
        secret=False,
    ),
    EnvCatalogEntry(
        key="AUTH_URL",
        description="Optional OAuth token endpoint override.",
        example="https://partner.xcures.com/oauth/token",
        secret=False,
    ),
    EnvCatalogEntry(
        key="DOCUMENT_DELAY_SECONDS",
        description="Delay between document downloads to avoid timeouts.",
        example="2",
        secret=False,
    ),
]


SCRIPT_DEFINITIONS: List[ScriptDefinition] = [
    ScriptDefinition(
        id="api_smoke_test",
        name="API Smoke Test",
        description="End-to-end public API smoke tests for auth and CRUD operations.",
        file_path="api_smoke_test.py",
        safety=SafetyMode.MUTATING,
        required_env=["XCURES_CLIENT_ID", "XCURES_CLIENT_SECRET"],
        tags=["public-api", "smoke-test"],
    ),
    ScriptDefinition(
        id="backup_user_permissions",
        name="Backup Users",
        description="Exports all users in a tenant with their projects and permissions",
        file_path="backup_user_permissions.py",
        safety=SafetyMode.READ_ONLY,
        required_env=["XCURES_CLIENT_ID", "XCURES_CLIENT_SECRET"],
        tags=["internal-api", "backup"],
    ),
    ScriptDefinition(
        id="bulk_create_users_from_csv",
        name="Bulk Create New Users",
        description="Bulk create new users from a CSV",
        file_path="bulk_create_users_from_csv.py",
        safety=SafetyMode.MUTATING,
        required_env=["XCURES_BEARER_TOKEN"],
        mode_behavior=ModeBehavior.DRY_RUN_APPLY_FLAGS,
        default_mode="dry-run",
        tags=["internal-api", "users"],
        fields=[
            ScriptField(
                id="csv",
                label="CSV Path",
                arg="--csv",
                type=FieldType.PATH,
                default="users.csv",
                description="Input CSV path for users to create.",
            ),
            ScriptField(id="out_dir", label="Output Directory", arg="--out-dir", type=FieldType.PATH, default="."),
            ScriptField(id="verbose", label="Verbose", arg="--verbose", type=FieldType.BOOLEAN, default=False),
            ScriptField(id="log_file", label="Log File", arg="--log-file", type=FieldType.PATH),
        ],
    ),
    ScriptDefinition(
        id="clinical_concepts_status",
        name="Clinical Concepts Status + Document Count for All Subjects",
        description="Collects subjects and writes clinical concepts/document count CSV outputs.",
        file_path="clinical_concepts_status_docus_count_ALL_subjects.py",
        safety=SafetyMode.READ_ONLY,
        required_env=["XCURES_CLIENT_ID", "XCURES_CLIENT_SECRET"],
        tags=["public-api", "reporting"],
        fields=[
            ScriptField(
                id="project_id",
                label="Project ID",
                arg="--project-id",
                env_alias="XCURES_PROJECT_ID",
                required=False,
            ),
        ],
    ),
    ScriptDefinition(
        id="download_all_documents",
        name="Download All Documents",
        description="Downloads all available documents for a given project (Raw XML files need to be converted to PDF)",
        file_path="download_all_documents.py",
        safety=SafetyMode.READ_ONLY,
        required_env=["XCURES_PROJECT_ID", "XCURES_BEARER_TOKEN"],
        tags=["public-api", "downloads"],
    ),
    ScriptDefinition(
        id="duplicate_project",
        name="Duplicate Project",
        description="Duplicate an existing project.",
        file_path="duplicate_project.py",
        safety=SafetyMode.MUTATING,
        required_env=["XCURES_CLIENT_ID", "XCURES_CLIENT_SECRET"],
        tags=["internal-api", "projects", "interactive"],
    ),
    ScriptDefinition(
        id="evaluate_checklist_to_pdf",
        name="Evaluate Checklist to PDF",
        description="Evaluates checklist results and produces a PDF + optional JSON.",
        file_path="evaluate_checklist_to_pdf.py",
        safety=SafetyMode.READ_ONLY,
        env_sets_any=[["XCURES_BEARER_TOKEN"], ["XCURES_CLIENT_ID", "XCURES_CLIENT_SECRET"]],
        tags=["public-api", "pdf"],
        fields=[
            ScriptField(id="subject_id", label="Subject ID", arg="--subject-id"),
            ScriptField(id="checklist_id", label="Checklist ID", arg="--checklist-id"),
            ScriptField(
                id="project_id",
                label="Project ID",
                arg="--project-id",
                env_alias="XCURES_PROJECT_ID",
                required=False,
            ),
            ScriptField(
                id="base_url",
                label="Base URL",
                arg="--base-url",
                default="https://partner.xcures.com",
                env_alias="BASE_URL",
            ),
            ScriptField(
                id="bearer",
                label="Bearer Token Override",
                arg="--bearer",
                env_alias="XCURES_BEARER_TOKEN",
            ),
            ScriptField(id="regenerate", label="Regenerate", arg="--regenerate", type=FieldType.BOOLEAN, default=False),
            ScriptField(
                id="save_json",
                label="Save JSON Sidecar",
                type=FieldType.BOOLEAN,
                default=True,
                false_arg="--no-save-json",
            ),
            ScriptField(id="output_dir", label="Output Directory", arg="--output-dir", type=FieldType.PATH),
            ScriptField(id="timeout", label="Timeout (sec)", arg="--timeout", type=FieldType.NUMBER, default=60),
        ],
    ),
    ScriptDefinition(
        id="generate_ccda_pdf",
        name="Generate CCDA PDF",
        description="Builds sample CCDA XML and embeds it into rendered PDF.",
        file_path="generate_ccda_pdf.py",
        safety=SafetyMode.READ_ONLY,
        tags=["pdf", "local"],
    ),
    ScriptDefinition(
        id="recap",
        name="MedSync RECAP to PDF",
        description="RECAP checklist evaluation and PDF report generator for MedSync.",
        file_path="recap.py",
        safety=SafetyMode.READ_ONLY,
        env_sets_any=[["XCURES_BEARER_TOKEN"], ["XCURES_CLIENT_ID", "XCURES_CLIENT_SECRET"]],
        tags=["public-api", "pdf"],
        fields=[
            ScriptField(id="subject_id", label="Subject ID", arg="--subject-id"),
            ScriptField(id="checklist_id", label="Checklist ID", arg="--checklist-id"),
            ScriptField(
                id="project_id",
                label="Project ID",
                arg="--project-id",
                env_alias="XCURES_PROJECT_ID",
                required=False,
            ),
            ScriptField(
                id="base_url",
                label="Base URL",
                arg="--base-url",
                default="https://partner.xcures.com",
                env_alias="BASE_URL",
            ),
            ScriptField(
                id="bearer",
                label="Bearer Token Override",
                arg="--bearer",
                env_alias="XCURES_BEARER_TOKEN",
            ),
            ScriptField(id="regenerate", label="Regenerate", arg="--regenerate", type=FieldType.BOOLEAN, default=False),
            ScriptField(
                id="save_json",
                label="Save JSON Sidecar",
                type=FieldType.BOOLEAN,
                default=True,
                false_arg="--no-save-json",
            ),
            ScriptField(id="output_dir", label="Output Directory", arg="--output-dir", type=FieldType.PATH),
            ScriptField(id="timeout", label="Timeout (sec)", arg="--timeout", type=FieldType.NUMBER, default=60),
        ],
    ),
    ScriptDefinition(
        id="update_user_email_domains",
        name="Update User Email Domains",
        description="Bulk update email domains for all users in a tenant.",
        file_path="update_user_email_domains.py",
        safety=SafetyMode.MUTATING,
        required_env=["XCURES_CLIENT_ID", "XCURES_CLIENT_SECRET"],
        mode_behavior=ModeBehavior.DRY_RUN_APPLY_FLAGS,
        default_mode="dry-run",
        tags=["internal-api", "users"],
        fields=[
            ScriptField(id="from_domain", label="From Domain", arg="--from-domain", required=True),
            ScriptField(id="to_domain", label="To Domain", arg="--to-domain", required=True),
            ScriptField(id="verbose", label="Verbose", arg="--verbose", type=FieldType.BOOLEAN, default=False),
            ScriptField(id="log_file", label="Log File", arg="--log-file", type=FieldType.PATH),
            ScriptField(id="only_missing", label="Only Missing", arg="--only-missing", type=FieldType.BOOLEAN, default=False),
        ],
    ),
    ScriptDefinition(
        id="update_user_permissions",
        name="Update User Permissions",
        description='Update all user permissions for a tenant.',
        file_path="update_user_permissions.py",
        safety=SafetyMode.MUTATING,
        env_sets_any=[["XCURES_BEARER_TOKEN"], ["XCURES_CLIENT_ID", "XCURES_CLIENT_SECRET"]],
        mode_behavior=ModeBehavior.DRY_RUN_FLAG,
        default_mode="dry-run",
        tags=["internal-api", "users"],
        fields=[
            ScriptField(id="project_id", label="Project ID", arg="--project-id", env_alias="XCURES_PROJECT_ID"),
            ScriptField(id="only_missing", label="Only Missing", arg="--only-missing", type=FieldType.BOOLEAN, default=True),
        ],
    ),
    ScriptDefinition(
        id="update_users_new_projects",
        name="Update Users with New Projects",
        description="Updates all users with new projects (MedSync).",
        file_path="update_users_new_projects.py",
        safety=SafetyMode.MUTATING,
        required_env=["XCURES_BEARER_TOKEN"],
        mode_behavior=ModeBehavior.DRY_RUN_APPLY_FLAGS,
        default_mode="dry-run",
        tags=["internal-api", "users", "projects"],
        fields=[
            ScriptField(id="project_id_header", label="Project ID Header", arg="--project-id-header"),
            ScriptField(
                id="project_id",
                label="Project IDs (comma-separated)",
                arg="--project-id",
                repeatable=True,
                delimiter=",",
            ),
            ScriptField(id="audit_log", label="Audit Log", arg="--audit-log", type=FieldType.PATH),
            ScriptField(id="backup_path", label="Backup Path", arg="--backup-path", type=FieldType.PATH),
            ScriptField(id="verbose", label="Verbose", arg="--verbose", type=FieldType.BOOLEAN, default=False),
        ],
    ),
    ScriptDefinition(
        id="xml_to_pdf",
        name="XML to PDF",
        description="Downloads XML document and renders a PDF using the xCures XSLT.",
        file_path="xml_to_pdf.py",
        safety=SafetyMode.READ_ONLY,
        env_sets_any=[["XCURES_BEARER_TOKEN"], ["XCURES_CLIENT_ID", "XCURES_CLIENT_SECRET"]],
        tags=["public-api", "pdf"],
        fields=[
            ScriptField(id="document_id", label="Document ID", arg="--document-id"),
            ScriptField(id="endpoint_template", label="Endpoint Template", arg="--endpoint-template"),
            ScriptField(id="xsl_path", label="XSL Path", arg="--xsl-path", type=FieldType.PATH),
            ScriptField(id="voc_file", label="Localization XML Path", arg="--voc-file", type=FieldType.PATH),
            ScriptField(id="l10n_url", label="Localization URL", arg="--l10n-url"),
            ScriptField(id="renderer", label="Renderer", arg="--renderer", type=FieldType.SELECT, choices=["auto", "wkhtmltopdf", "playwright", "weasyprint"], default="auto"),
            ScriptField(id="pdf_source", label="PDF Source", arg="--pdf-source", type=FieldType.SELECT, choices=["auto", "platform", "local"], default="auto"),
            ScriptField(id="keep_html", label="Keep HTML", arg="--keep-html", type=FieldType.BOOLEAN, default=False),
            ScriptField(id="bearer_token", label="Bearer Token Override", arg="--bearer-token", env_alias="XCURES_BEARER_TOKEN"),
            ScriptField(id="non_interactive", label="Non-Interactive", arg="--non-interactive", type=FieldType.BOOLEAN, default=True),
            ScriptField(id="timeout_seconds", label="Timeout (sec)", arg="--timeout-seconds", type=FieldType.NUMBER, default=120),
        ],
    ),
]


SCRIPT_INDEX: Dict[str, ScriptDefinition] = {s.id: s for s in SCRIPT_DEFINITIONS}


def get_script(script_id: str) -> ScriptDefinition:
    return SCRIPT_INDEX[script_id]
