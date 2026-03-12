# xCures Customer Success Script Toolkit

A collection of utilities for xCures Patient Registry operations, reporting, and document/PDF workflows to help CS automate common client requests and needs.

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
   - `BASE_URL` (optional; defaults to `https://partner.xcures.com`)

## Local Script Runner UI (FastAPI + React)

The repo now includes a lightweight local web app for running scripts, managing env vars, and viewing live logs.

### 1) Install UI dependencies

```bash
python3 -m pip install -r requirements-ui.txt
npm --prefix web_ui install
```

### 2) Build React frontend for production serving

```bash
npm --prefix web_ui run build
```

### 3) Start backend + UI

```bash
python3 run_ui.py
```

Open: `http://127.0.0.1:8765`

### Optional: React learning/dev mode

Run FastAPI in one terminal:

```bash
python3 run_ui.py
```

Run React dev server in another terminal:

```bash
npm --prefix web_ui run dev
```

Open `http://127.0.0.1:5173` during development.

### Always-on with launchd (no admin required)

Use the helper script:

```bash
./scripts/manage_launchd.sh install
./scripts/manage_launchd.sh status
```

Common commands:

```bash
./scripts/manage_launchd.sh start
./scripts/manage_launchd.sh stop
./scripts/manage_launchd.sh restart
./scripts/manage_launchd.sh uninstall
```

Logs:
- `logs/launchd.script_runner.out.log`
- `logs/launchd.script_runner.err.log`

### Multiple Client Profiles in `.env`

You can define multiple named client profiles and manage them directly in the UI
(create, edit, delete, activate). Profiles are persisted in `.env`.

Example:

```bash
XCURES_PROFILE__DEMO__NAME="Demo Environment"
XCURES_PROFILE__DEMO__CLIENT_ID="demo-client-id"
XCURES_PROFILE__DEMO__CLIENT_SECRET="demo-client-secret"
XCURES_PROFILE__DEMO__PROJECT_ID="demo-project-id"
XCURES_PROFILE__DEMO__BASE_URL="https://partner.xcures.com"

XCURES_PROFILE__PROD__NAME="Production"
XCURES_PROFILE__PROD__CLIENT_ID="prod-client-id"
XCURES_PROFILE__PROD__CLIENT_SECRET="prod-client-secret"
XCURES_PROFILE__PROD__PROJECT_ID="prod-project-id"
XCURES_PROFILE__PROD__BASE_URL="https://partner.xcures.com"

ACTIVE_XCURES_PROFILE="DEMO"
```

When an active profile is selected, profile values are mapped to standard script env vars at runtime:
- `XCURES_CLIENT_ID`
- `XCURES_CLIENT_SECRET`
- `XCURES_PROJECT_ID` (if provided)
- `BASE_URL` (and mirrored to `XCURES_BASE_URL` for legacy compatibility)

Bearer token usage is managed per script run on the Scripts page for Internal API scripts.
The Environment page no longer exposes `XCURES_BEARER_TOKEN`.

## Script Summaries

#### API Smoke Test 
`api_smoke_test.py`<br>
**API:** Public API  
Runs end-to-end smoke tests against the Public API (create subject/document, fetch/update subject, verify behavior) to validate API connectivity and credentials.

---
#### Backup User Permissions 
`backup_user_permissions.py`<br>
**API:** Internal API  
Exports user permissions and project membership data for a tenant to CSV (including role, created/last-login dates, resolved project names) with progress reporting.

---
#### Bulk Create Users 
`bulk_create_users_from_csv.py`<br>
**API:** Internal API  
Bulk-creates portal users from a CSV.

---
#### `clinical_concepts_status_docus_count_ALL_subjects.py`<br>
**API:** Public API  
Collects all subjects for a project, then computes each subject’s clinical-concepts status and document count, writing subject and result CSV outputs.

---
#### Download all Documents in a Project 
`download_all_documents.py`<br>
**API:** Public API  
Iterates subjects/documents and downloads all available documents locally, with retry handling, progress display, and manual bearer-token rotation prompts.

---
#### Duplicate Project 
`duplicate_project.py`<br>
**API:** Internal API  
Interactive project duplication utility that reads source project settings, normalizes payload defaults, prompts for the new name, previews changes, and creates a new project.

---
#### Export Checklist to PDF 
`evaluate_checklist_to_pdf.py`<br>
**API:** Public API  
Evaluates a checklist for a subject through the API and generates a polished, sectioned PDF report, optionally saving raw JSON output too.

---
#### Custom CCDA Generator 
`generate_ccda_pdf.py`<br>
**API:** N/A
Builds a sample Medication Dispense CCD-A XML, renders a human-readable PDF, and embeds the XML into the PDF as an attachment.

---
#### MedSync RECAP to PDF 
`recap.py`<br>
**API:** Public API  
Customized checklist-to-report generator for MedSync's RECAP checklist.

---
#### Update Users Email Domain 
`update_user_email_domains.py`<br>
**API:** Internal API  
Bulk updates user email domains (excluding `@xcures.com`) with safe default dry-run behavior, optional apply mode, filtering, limits, and logs.

---
#### Update User Permissions 
`update_user_permissions.py`<br>
**API:** Internal API  
Bulk adds the `Summary_Checklist` permission across tenant users by reading each user, patching permissions when missing, and writing updates with progress/summary output.

---
#### Update Users w/ New Projects 
`update_users_new_projects.py`<br>
**API:** Internal API  
Built for MedSync - bulk project assignment workflow with config-driven target projects, mandatory pre-write backup in apply mode, and JSONL audit logging.

---
#### Download PDF Version of Documents 
`xml_to_pdf.py`<br>
**API:** Public API  
Downloads an XML document from the API, applies `cda2.xsl` transformation, and produces a PDF (auto-fetching `cda_l10n.xml` when needed).

---
#### Common API Utilities Module
`api_common.py`<br>
Shared HTTP utilities: URL building, retry/backoff request wrapper, JSON validation/parsing, and standardized HTTP error formatting.

---
#### Common Authentication Module
`auth_common.py`<br>
Shared auth/env helpers: `.env` loading, required env validation, bearer/client-credentials token retrieval with in-memory token caching, and JSON header construction.

---
#### CSV Handler Module
`csv_common.py`<br>
CSV read/write helpers with header normalization, required-column checks, and safe output creation.

---
#### Progress Bar Module 
`(progress_common.py)`<br>
Progress bar abstraction (`tqdm` when available, text fallback otherwise) for both iterator and manual progress use.

---
#### API Client Module
`xcures_client.py`<br>
Reusable xCures API client with retries, automatic auth header handling, optional token refresh on 401, and pagination helpers.

---
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
