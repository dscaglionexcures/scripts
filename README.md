# xCures Script Toolkit

A collection of Python utilities for xCures Patient Registry operations, reporting, and document/PDF workflows.

## Quick Start

1. Install dependencies used by your target script(s):
   - `requests`
   - `pypdf`
   - `reportlab`
   - `tqdm` (optional, fallback progress bars are built in)
2. Configure `.env` with required xCures credentials (varies by script):
   - `XCURES_CLIENT_ID`
   - `XCURES_CLIENT_SECRET`
   - `XCURES_BEARER_TOKEN`
   - `XCURES_PROJECT_ID`
   - `BASE_URL` or `XCURES_BASE_URL` (optional; defaults to `https://partner.xcures.com`)

## Script Summary

| Script | Type | API | Summary |
|-|---|---|---|
| `api_smoke_test.py` | Script | Public API | Runs end-to-end smoke tests against the Public API (create subject/document, fetch/update subject, verify behavior) to validate API connectivity and credentials. |
| `backup_user_permissions.py` | Script | Internal API | Exports user permissions and project membership data for a tenant to CSV (including role, created/last-login dates, resolved project names) with progress reporting. |
| `bulk_create_users_from_csv.py` | Script | Internal API | Bulk-creates portal users from a CSV |
| `clinical_concepts_status_docus_count_ALL_subjects.py` | Script | Public API | Collects all subjects for a project, then computes each subject’s clinical-concepts status and document count, writing subject and result CSV outputs. |
| `download_all_documents.py` | Script | Public API | Iterates subjects/documents and downloads all available documents locally, with retry handling, progress display, and manual bearer-token rotation prompts. |
| `duplicate_project.py` | Script | Internal API | Interactive project duplication utility that reads source project settings, normalizes payload defaults, prompts for the new name, previews changes, and creates a new project. |
| `evaluate_checklist_to_pdf.py` | Script | Public API | Evaluates a checklist for a subject through the API and generates a polished, sectioned PDF report, optionally saving raw JSON output too. |
| `generate_ccda_pdf.py` | Script | Public API | Builds a sample Medication Dispense CCD-A XML, renders a human-readable PDF, and embeds the XML into the PDF as an attachment. |
| `recap.py` | Script | Public API | Customized checklist-to-report generator for MedSync's RECAP checklist. |
| `update_user_email_domains.py` | Script | Internal API | Bulk updates user email domains (excluding `@xcures.com`) with safe default dry-run behavior, optional apply mode, filtering, limits, and logs. |
| `update_user_permissions.py` | Script | Internal API | Bulk adds the `Summary_Checklist` permission across tenant users by reading each user, patching permissions when missing, and writing updates with progress/summary output. |
| `update_users_new_projects.py` | Script | Internal API | Hardened bulk project assignment workflow with config-driven target projects, mandatory pre-write backup in apply mode, and JSONL audit logging. |
| `xml_to_pdf.py` | Script | Public API | Downloads an XML document from the API, applies `cda2.xsl` transformation, and produces a PDF (auto-fetching `cda_l10n.xml` when needed). |
| `api_common.py` | Helper module | Shared HTTP utilities: URL building, retry/backoff request wrapper, JSON validation/parsing, and standardized HTTP error formatting. |
| `auth_common.py` | Helper module | Shared auth/env helpers: `.env` loading, required env validation, bearer/client-credentials token retrieval with in-memory token caching, and JSON header construction. |
| `csv_common.py` | Helper module | CSV read/write helpers with header normalization, required-column checks, and safe output creation. |
| `progress_common.py` | Helper module | Progress bar abstraction (`tqdm` when available, text fallback otherwise) for both iterator and manual progress use. |
| `xcures_client.py` | Helper module | Reusable xCures API client with retries, automatic auth header handling, optional token refresh on 401, and pagination helpers. |

## Non-Script Assets

- `cda2.xsl`: XSL stylesheet used by `xml_to_pdf.py`.
- `cda_l10n.xml`: Localization vocabulary used during C-CDA transform.
- `checklist_example.json`: Example checklist payload/sample data.
- `configs/`: Script configuration files (for example project-assignment runs).
- `backups/`, `logs/`, `downloads/`: Generated artifacts from previous executions.

## Safety Notes

- Prefer `--dry-run` first on all mutation scripts.
- Keep backups/audit logs for any bulk user/project update.
- Validate tenant/project IDs before apply operations.
