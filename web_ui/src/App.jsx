import { useEffect, useMemo, useState } from "react";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import {
  faAt,
  faBoxArchive,
  faCircleChevronRight,
  faCircleCheck,
  faCircleXmark,
  faClone,
  faFile,
  faFileCircleCheck,
  faFileArrowDown,
  faFilePdf,
  faHospitalUser,
  faSmoking,
  faSquareRss,
  faUsers,
  faUsersGear,
  faUsersRectangle,
  faChartPie,
} from "@fortawesome/free-solid-svg-icons";
import logo from "./xcures.svg";

const TABS = [
  { id: "internal-scripts", label: "Internal API Operations" },
  { id: "public-scripts", label: "Public API Operations" },
  { id: "local-scripts", label: "Local Operations" },
  { id: "jobs", label: "jobs" },
  { id: "environment", label: "environment" },
  { id: "admin", label: "admin" }
];
const API_SETTINGS = [
  { key: "user_page_size", label: "User Page Size", defaultValue: "25" },
  { key: "request_timeout_seconds", label: "Request Timeout (in Seconds)", defaultValue: "60" },
  { key: "max_retries", label: "Max Retries", defaultValue: "2" },
  { key: "backoff_seconds", label: "Backoff (in Seconds)", defaultValue: "1.0" }
];
const SCRIPT_ICON_MAP = {
  backup_user_permissions: [faBoxArchive],
  bulk_create_users_from_csv: [faUsers],
  duplicate_project: [faClone],
  update_user_email_domains: [faAt],
  update_user_permissions: [faUsersGear],
  update_users_new_projects: [faUsersRectangle],
  api_smoke_test: [faSmoking],
  download_all_documents: [faFileArrowDown],
  clinical_concepts_status: [faFileCircleCheck],
  evaluate_checklist_to_pdf: [faFilePdf],
  generate_ccda_pdf: [faSquareRss, faCircleChevronRight, faFilePdf],
};

async function apiJson(path, options) {
  const response = await fetch(path, options);
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    const detail = payload?.detail ?? payload?.message ?? `Request failed (${response.status})`;
    throw new Error(typeof detail === "string" ? detail : JSON.stringify(detail));
  }
  return payload;
}

function fieldValueOrDefault(field, value) {
  if (value === undefined || value === null || value === "") {
    if (field.default !== undefined && field.default !== null) {
      return field.default;
    }
  }
  return value ?? "";
}

function formatTs(value) {
  if (!value) return "—";
  try {
    return new Date(value).toLocaleString();
  } catch {
    return value;
  }
}

function statusClass(status) {
  const mapping = {
    queued: "queued",
    running: "running",
    succeeded: "succeeded",
    failed: "failed",
    canceled: "canceled"
  };
  return mapping[status] ?? "queued";
}

function statusLabel(status) {
  if (status === "succeeded") return "✅ succeeded";
  if (status === "failed") return "❌ failed";
  return status;
}

function extractProgressPercentFromText(text) {
  const matches = [...String(text ?? "").matchAll(/(\d+(?:\.\d+)?)\s*%/g)];
  if (!matches.length) return null;
  const value = Number(matches[matches.length - 1][1]);
  if (!Number.isFinite(value)) return null;
  return Math.max(0, Math.min(100, value));
}

function isProgressFrameMessage(text) {
  const message = String(text ?? "");
  if (!message) return false;
  // Handles both fallback bars ("|###| 12.00% (3/25)") and tqdm-style frames.
  const hasPercent = extractProgressPercentFromText(message) !== null;
  const hasCounter = /\(\d+\s*\/\s*\d+\)/.test(message);
  const hasBar = /\|[#\-= >]+\|/.test(message);
  return hasPercent && (hasCounter || hasBar);
}

function safetyLabel(value) {
  return value === "mutating" ? "CRUD" : value;
}

function apiTypeLabel(script) {
  if (script.tags?.includes("internal-api")) return "Internal API";
  if (script.tags?.includes("public-api")) return "Public API";
  return "Local";
}

function apiTypeClass(script) {
  if (script.tags?.includes("internal-api")) return "internal";
  if (script.tags?.includes("public-api")) return "public";
  return "local";
}

function matchesScriptGroup(script, groupId) {
  if (groupId === "internal") return script.tags?.includes("internal-api");
  if (groupId === "public") return script.tags?.includes("public-api");
  if (groupId === "local") {
    const isInternal = script.tags?.includes("internal-api");
    const isPublic = script.tags?.includes("public-api");
    return !isInternal && !isPublic;
  }
  return true;
}

function defaultDraftForScript(script) {
  const initialFieldValues = {};
  for (const field of script.fields) {
    if (field.default !== null && field.default !== undefined) {
      initialFieldValues[field.id] = field.default;
    }
  }
  return {
    mode: script.default_mode ?? "apply",
    rawArgs: "",
    fieldValues: initialFieldValues
  };
}

function emptyProfileForm() {
  return {
    profile_id: "",
    name: "",
    client_id: "",
    client_secret: "",
    project_id: ""
  };
}

function sanitizeProfileId(value) {
  return (value ?? "")
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, "_")
    .replace(/_+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function csvParse(text) {
  const rows = [];
  let row = [];
  let value = "";
  let inQuotes = false;

  for (let i = 0; i < text.length; i += 1) {
    const char = text[i];
    const next = text[i + 1];
    if (char === '"') {
      if (inQuotes && next === '"') {
        value += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }
    if (char === "," && !inQuotes) {
      row.push(value);
      value = "";
      continue;
    }
    if ((char === "\n" || char === "\r") && !inQuotes) {
      row.push(value);
      value = "";
      if (char === "\r" && next === "\n") i += 1;
      rows.push(row);
      row = [];
      continue;
    }
    value += char;
  }

  if (value.length > 0 || row.length > 0) {
    row.push(value);
    rows.push(row);
  }

  if (!rows.length) {
    return { headers: [], rows: [] };
  }

  const headers = rows[0].map((header) => String(header ?? "").trim());
  const data = rows.slice(1).map((cells) => {
    const out = {};
    headers.forEach((header, index) => {
      out[header] = cells[index] ?? "";
    });
    return out;
  });
  return { headers, rows: data };
}

function csvEscapeCell(value) {
  const text = String(value ?? "");
  if (text.includes('"') || text.includes(",") || text.includes("\n") || text.includes("\r")) {
    return `"${text.replace(/"/g, '""')}"`;
  }
  return text;
}

function splitPipeValues(value) {
  return String(value ?? "")
    .split("|")
    .map((item) => item.trim())
    .filter(Boolean);
}

function parseCommaValues(value) {
  return String(value ?? "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function normalizeClinicalPreview(parsed) {
  const rows = parsed?.rows ?? [];
  const normalizedRows = rows.map((row) => {
    const loadedRaw =
      row["Clinical Concepts Loaded"] ?? row.Loaded ?? row.loaded ?? "";
    const docCountValue = row["# of Documents"] ?? row.documentTotalCount ?? "";
    const docCountNumber = Number(docCountValue);
    const loadedValue =
      Number.isFinite(docCountNumber) && docCountNumber === 0
        ? "NO DOCUMENTS"
        : loadedRaw;
    return {
      Subject: row.Subject ?? row.subjectId ?? "",
      "# of Documents": docCountValue,
      "Clinical Concepts Loaded": loadedValue,
    };
  });

  return {
    headers: ["Subject", "# of Documents", "Clinical Concepts Loaded"],
    rows: normalizedRows,
  };
}

function ScriptNameWithIcon({ scriptId, name }) {
  const icons = SCRIPT_ICON_MAP[scriptId] ?? [];
  return (
    <span className="script-name">
      {icons.length > 0 && (
        <span className="script-icons" aria-hidden="true">
          {icons.map((icon, index) => (
            <FontAwesomeIcon
              key={`${scriptId}-${index}`}
              icon={icon}
              style={scriptId === "clinical_concepts_status" ? { color: "#075b87" } : undefined}
            />
          ))}
        </span>
      )}
      <span>{name}</span>
    </span>
  );
}

function supportsTenantProjectPicker(scriptId) {
  return scriptId === "update_users_new_projects" || scriptId === "clinical_concepts_status";
}

function usesOptionalProjectLoaderBearer(scriptId) {
  return scriptId === "clinical_concepts_status";
}

function isChecklistPdfScript(scriptId) {
  return scriptId === "evaluate_checklist_to_pdf" || scriptId === "recap";
}

export default function App() {
  const [activeTab, setActiveTab] = useState("internal-scripts");
  const [scripts, setScripts] = useState([]);
  const [scriptSearch, setScriptSearch] = useState("");
  const [selectedScriptId, setSelectedScriptId] = useState("");
  const [draftByScript, setDraftByScript] = useState({});
  const [runError, setRunError] = useState("");
  const [runMessage, setRunMessage] = useState("");
  const [internalBearerToken, setInternalBearerToken] = useState("");
  const [bulkUploadBusy, setBulkUploadBusy] = useState(false);
  const [bulkUploadMessage, setBulkUploadMessage] = useState("");
  const [evaluateOutputDirBusy, setEvaluateOutputDirBusy] = useState(false);
  const [evaluateOutputDirMessage, setEvaluateOutputDirMessage] = useState("");
  const [evaluateChecklists, setEvaluateChecklists] = useState([]);
  const [evaluateChecklistsBusy, setEvaluateChecklistsBusy] = useState(false);
  const [evaluateChecklistsMessage, setEvaluateChecklistsMessage] = useState("");
  const [evaluateProjects, setEvaluateProjects] = useState([]);
  const [evaluateProjectsBusy, setEvaluateProjectsBusy] = useState(false);
  const [evaluateProjectsMessage, setEvaluateProjectsMessage] = useState("");
  const [evaluateProjectsBearerToken, setEvaluateProjectsBearerToken] = useState("");
  const [tenantProjects, setTenantProjects] = useState([]);
  const [tenantProjectsBusy, setTenantProjectsBusy] = useState(false);
  const [tenantProjectsMessage, setTenantProjectsMessage] = useState("");
  const [tenantUsers, setTenantUsers] = useState([]);
  const [tenantUsersBusy, setTenantUsersBusy] = useState(false);
  const [tenantUsersMessage, setTenantUsersMessage] = useState("");
  const [permissionLookupUserId, setPermissionLookupUserId] = useState("");
  const [singleUserPermissions, setSingleUserPermissions] = useState(null);
  const [clonePermissionsBusy, setClonePermissionsBusy] = useState(false);
  const [clonePermissionsMessage, setClonePermissionsMessage] = useState("");
  const [targetUsersMessage, setTargetUsersMessage] = useState("");
  const [allUsersPermissions, setAllUsersPermissions] = useState({ items: [], permission_keys: [] });
  const [allUsersPermissionsBusy, setAllUsersPermissionsBusy] = useState(false);
  const [allUsersPermissionsMessage, setAllUsersPermissionsMessage] = useState("");
  const [backupPreviewBusy, setBackupPreviewBusy] = useState(false);
  const [backupPreviewMessage, setBackupPreviewMessage] = useState("");
  const [clinicalPreviewBusy, setClinicalPreviewBusy] = useState(false);
  const [clinicalPreviewMessage, setClinicalPreviewMessage] = useState("");
  const [clinicalSort, setClinicalSort] = useState({ key: "", direction: "asc" });
  const [expandedPermissionRows, setExpandedPermissionRows] = useState({});
  const [backupPreview, setBackupPreview] = useState({
    jobId: "",
    artifactPath: "",
    headers: [],
    rows: []
  });
  const [clinicalPreview, setClinicalPreview] = useState({
    jobId: "",
    artifactPath: "",
    headers: [],
    rows: []
  });

  const [jobs, setJobs] = useState([]);
  const [selectedJobId, setSelectedJobId] = useState("");
  const [scriptPanelJobId, setScriptPanelJobId] = useState("");
  const [jobLogs, setJobLogs] = useState({});
  const [stdinText, setStdinText] = useState("");
  const [jobMessage, setJobMessage] = useState("");

  const [envValues, setEnvValues] = useState([]);
  const [envOriginal, setEnvOriginal] = useState({});
  const [envDrafts, setEnvDrafts] = useState({});
  const [profiles, setProfiles] = useState([]);
  const [activeProfileId, setActiveProfileId] = useState("");
  const [editingProfileId, setEditingProfileId] = useState("");
  const [profileForm, setProfileForm] = useState(emptyProfileForm());
  const [profileMessage, setProfileMessage] = useState("");
  const [revealSecrets, setRevealSecrets] = useState(false);
  const [envMessage, setEnvMessage] = useState("");
  const [functionalByScript, setFunctionalByScript] = useState({});
  const [adminMessage, setAdminMessage] = useState("");
  const [xmlXslFiles, setXmlXslFiles] = useState([]);
  const [xmlXslFilesBusy, setXmlXslFilesBusy] = useState(false);
  const [xmlXslFilesMessage, setXmlXslFilesMessage] = useState("");

  const selectedScript = useMemo(
    () => scripts.find((script) => script.id === selectedScriptId) ?? null,
    [scripts, selectedScriptId]
  );
  const selectedScriptDraft = useMemo(() => {
    if (!selectedScript) return null;
    return draftByScript[selectedScript.id] ?? defaultDraftForScript(selectedScript);
  }, [draftByScript, selectedScript]);
  const evaluateChecklistSelectWidth = useMemo(() => {
    const longestLoadedName = evaluateChecklists.reduce((maxLength, checklist) => {
      const length = String(checklist?.name ?? "").length;
      return Math.max(maxLength, length);
    }, 0);
    const selectedIdLength = String(selectedScriptDraft?.fieldValues?.checklist_id ?? "").length;
    const chars = Math.max(24, longestLoadedName, selectedIdLength);
    return `calc(${chars}ch + 48px)`;
  }, [evaluateChecklists, selectedScriptDraft]);
  const evaluateProjectSelectWidth = useMemo(() => {
    const longestLoadedProject = evaluateProjects.reduce((maxLength, project) => {
      const length = String(project?.id ?? "").length;
      return Math.max(maxLength, length);
    }, 0);
    const selectedIdLength = String(selectedScriptDraft?.fieldValues?.project_id ?? "").length;
    const chars = Math.max(24, longestLoadedProject, selectedIdLength);
    return `calc(${chars}ch + 48px)`;
  }, [evaluateProjects, selectedScriptDraft]);
  const selectedScriptIsInternalApi = useMemo(
    () => Boolean(selectedScript?.tags?.includes("internal-api")),
    [selectedScript]
  );

  const selectedJob = useMemo(
    () => jobs.find((job) => job.job_id === selectedJobId) ?? null,
    [jobs, selectedJobId]
  );
  const scriptPanelJob = useMemo(
    () => jobs.find((job) => job.job_id === scriptPanelJobId) ?? null,
    [jobs, scriptPanelJobId]
  );

  const selectedJobEvents = useMemo(() => {
    if (!selectedJob) return [];
    return jobLogs[selectedJob.job_id] ?? selectedJob.log_events ?? [];
  }, [selectedJob, jobLogs]);
  const scriptPanelJobEvents = useMemo(() => {
    if (!scriptPanelJob) return [];
    return jobLogs[scriptPanelJob.job_id] ?? scriptPanelJob.log_events ?? [];
  }, [scriptPanelJob, jobLogs]);
  const scriptPanelDisplayEvents = useMemo(() => {
    if (!scriptPanelJobEvents.length) return [];
    const display = [];
    for (const event of scriptPanelJobEvents) {
      const isProgressUpdate =
        (event.type === "stdout" || event.type === "stderr") &&
        isProgressFrameMessage(event.message);

      if (!isProgressUpdate) {
        display.push(event);
        continue;
      }

      const last = display[display.length - 1];
      if (last?.__isProgressUpdate) {
        display[display.length - 1] = { ...event, __isProgressUpdate: true };
      } else {
        display.push({ ...event, __isProgressUpdate: true });
      }
    }
    return display;
  }, [scriptPanelJob, scriptPanelJobEvents]);
  const scriptPanelProgressPercent = useMemo(() => {
    if (!scriptPanelJob || scriptPanelJob.script_id !== "backup_user_permissions") return null;

    for (let index = scriptPanelJobEvents.length - 1; index >= 0; index -= 1) {
      const event = scriptPanelJobEvents[index];
      if (event.type !== "stdout" && event.type !== "stderr") continue;
      const percent = extractProgressPercentFromText(event.message);
      if (percent !== null) return percent;
    }

    if (scriptPanelJob.status === "succeeded") return 100;
    return null;
  }, [scriptPanelJob, scriptPanelJobEvents]);
  const clinicalSummary = useMemo(() => {
    if (!clinicalPreview.rows.length) {
      return {
        totalSubjects: 0,
        totalDocuments: 0,
        percentEnabled: 0,
      };
    }
    let enabledCount = 0;
    let totalDocuments = 0;
    let subjectsWithDocuments = 0;
    for (const row of clinicalPreview.rows) {
      const docCount = Number(row["# of Documents"] ?? 0);
      if (Number.isFinite(docCount)) {
        totalDocuments += docCount;
        if (docCount > 0) {
          subjectsWithDocuments += 1;
          const loadedRaw = String(row["Clinical Concepts Loaded"] ?? "").trim().toLowerCase();
          const isEnabled = loadedRaw === "true" || loadedRaw === "1" || loadedRaw === "yes";
          if (isEnabled) enabledCount += 1;
        }
      }
    }
    return {
      totalSubjects: clinicalPreview.rows.length,
      totalDocuments,
      percentEnabled: subjectsWithDocuments > 0 ? (enabledCount / subjectsWithDocuments) * 100 : 0,
    };
  }, [clinicalPreview.rows]);
  const sortedClinicalRows = useMemo(() => {
    const rows = [...clinicalPreview.rows];
    if (!clinicalSort.key) return rows;

    const key = clinicalSort.key;
    const factor = clinicalSort.direction === "asc" ? 1 : -1;

    const asLoadedRank = (value) => {
      const raw = String(value ?? "").trim().toLowerCase();
      if (raw === "true" || raw === "1" || raw === "yes") return 2;
      if (raw === "false" || raw === "0" || raw === "no") return 1;
      return 0;
    };

    rows.sort((a, b) => {
      if (key === "# of Documents") {
        const left = Number(a[key] ?? 0);
        const right = Number(b[key] ?? 0);
        return (left - right) * factor;
      }
      if (key === "Clinical Concepts Loaded") {
        return (asLoadedRank(a[key]) - asLoadedRank(b[key])) * factor;
      }
      const left = String(a[key] ?? "").toLowerCase();
      const right = String(b[key] ?? "").toLowerCase();
      if (left < right) return -1 * factor;
      if (left > right) return 1 * factor;
      return 0;
    });
    return rows;
  }, [clinicalPreview.rows, clinicalSort]);
  const apiSettingKeys = useMemo(() => new Set(API_SETTINGS.map((item) => item.key)), []);
  const apiSettingRows = useMemo(
    () => envValues.filter((row) => apiSettingKeys.has(row.key)),
    [envValues, apiSettingKeys]
  );
  const generalEnvRows = useMemo(
    () => envValues.filter((row) => !apiSettingKeys.has(row.key)),
    [envValues, apiSettingKeys]
  );
  const permissionMatrixColumns = useMemo(
    () => allUsersPermissions.permission_keys ?? [],
    [allUsersPermissions.permission_keys]
  );
  const selectedBulkTargetUserIds = useMemo(() => {
    if (!selectedScript || selectedScript.id !== "update_user_permissions") return [];
    const draft = selectedScriptDraft ?? defaultDraftForScript(selectedScript);
    return parseCommaValues(draft.fieldValues?.user_ids ?? "");
  }, [selectedScript, selectedScriptDraft]);

  const filteredScripts = useMemo(() => {
    const query = scriptSearch.trim().toLowerCase();
    const scriptGroup =
      activeTab === "internal-scripts"
        ? "internal"
        : activeTab === "public-scripts"
          ? "public"
          : activeTab === "local-scripts"
            ? "local"
            : null;
    const grouped = scriptGroup
      ? scripts.filter((script) => matchesScriptGroup(script, scriptGroup))
      : scripts;
    if (!query) return grouped;
    return grouped.filter((script) => {
      const haystack = `${script.name} ${script.description} ${script.tags.join(" ")}`.toLowerCase();
      return haystack.includes(query);
    });
  }, [scripts, scriptSearch, activeTab]);

  useEffect(() => {
    loadScripts();
    loadJobs();
    loadEnv(false);
    loadProfiles();
    loadFunctionalStatus();
  }, []);

  useEffect(() => {
    loadEnv(revealSecrets);
  }, [revealSecrets]);

  useEffect(() => {
    const interval = window.setInterval(loadJobs, 3000);
    return () => window.clearInterval(interval);
  }, []);

  useEffect(() => {
    if (selectedScriptId !== "xml_to_pdf") return;
    void loadXmlXslFiles({ applyDefault: true });
  }, [selectedScriptId]);

  useEffect(() => {
    if (filteredScripts.length === 0) {
      setSelectedScriptId("");
      return;
    }
    if (!filteredScripts.some((script) => script.id === selectedScriptId)) {
      setSelectedScriptId(filteredScripts[0].id);
    }
  }, [filteredScripts, selectedScriptId]);

  useEffect(() => {
    setInternalBearerToken("");
    setBulkUploadMessage("");
    setTenantProjects([]);
    setTenantProjectsMessage("");
    setTenantUsers([]);
    setTenantUsersMessage("");
    setPermissionLookupUserId("");
    setSingleUserPermissions(null);
    setClonePermissionsMessage("");
    setTargetUsersMessage("");
    setAllUsersPermissions({ items: [], permission_keys: [] });
    setAllUsersPermissionsMessage("");
    setBackupPreview({ jobId: "", artifactPath: "", headers: [], rows: [] });
    setBackupPreviewMessage("");
    setClinicalPreview({ jobId: "", artifactPath: "", headers: [], rows: [] });
    setClinicalPreviewMessage("");
  }, [selectedScriptId]);

  useEffect(() => {
    if (selectedScriptId !== "backup_user_permissions") return;
    const latest = jobs.find(
      (job) =>
        job.script_id === "backup_user_permissions" &&
        job.status === "succeeded" &&
        (job.artifacts ?? []).some((path) => path.endsWith(".csv"))
    );
    if (!latest) {
      setBackupPreview({ jobId: "", artifactPath: "", headers: [], rows: [] });
      setBackupPreviewMessage("No successful backup CSV found yet.");
      return;
    }
    const csvPath = (latest.artifacts ?? []).find((path) => path.endsWith(".csv")) ?? "";
    if (!csvPath) return;
    if (backupPreview.jobId === latest.job_id && backupPreview.artifactPath === csvPath) return;
    loadBackupPreview(latest.job_id, csvPath);
  }, [selectedScriptId, jobs]);

  useEffect(() => {
    if (selectedScriptId !== "clinical_concepts_status") return;
    const latest = jobs.find(
      (job) =>
        job.script_id === "clinical_concepts_status" &&
        job.status === "succeeded" &&
        (job.artifacts ?? []).some((path) => path.endsWith(".csv"))
    );
    if (!latest) {
      setClinicalPreview({ jobId: "", artifactPath: "", headers: [], rows: [] });
      setClinicalPreviewMessage("No successful clinical concepts CSV found yet.");
      return;
    }
    const artifactCsv = (latest.artifacts ?? []).find(
      (path) => path.endsWith(".csv") && path.includes("subject_clinical_concepts_and_doc_counts")
    );
    const csvPath = artifactCsv ?? (latest.artifacts ?? []).find((path) => path.endsWith(".csv")) ?? "";
    if (!csvPath) return;
    if (clinicalPreview.jobId === latest.job_id && clinicalPreview.artifactPath === csvPath) return;
    loadClinicalPreview(latest.job_id, csvPath);
  }, [selectedScriptId, jobs]);

  useEffect(() => {
    if (selectedScriptId !== "clinical_concepts_status") return;
    if (!scriptPanelJob || scriptPanelJob.script_id !== "clinical_concepts_status") return;
    if (scriptPanelJob.status !== "succeeded") return;

    const artifacts = scriptPanelJob.artifacts ?? [];
    const artifactCsv = artifacts.find(
      (path) => path.endsWith(".csv") && path.includes("subject_clinical_concepts_and_doc_counts")
    );
    const csvPath = artifactCsv ?? artifacts.find((path) => path.endsWith(".csv")) ?? "";
    if (!csvPath) {
      setClinicalPreviewMessage("Run completed, but no CSV artifact was found.");
      return;
    }
    if (clinicalPreview.jobId === scriptPanelJob.job_id && clinicalPreview.artifactPath === csvPath) return;
    loadClinicalPreview(scriptPanelJob.job_id, csvPath);
  }, [selectedScriptId, scriptPanelJob, clinicalPreview.jobId, clinicalPreview.artifactPath]);

  useEffect(() => {
    setExpandedPermissionRows({});
  }, [backupPreview.jobId, backupPreview.artifactPath]);

  useEffect(() => {
    setClinicalSort({ key: "", direction: "asc" });
  }, [clinicalPreview.jobId, clinicalPreview.artifactPath]);

  useEffect(() => {
    if (!selectedJobId) return undefined;
    const source = new EventSource(`/api/jobs/${selectedJobId}/events`);
    const handler = (event) => {
      const data = JSON.parse(event.data);
      setJobLogs((previous) => {
        const current = previous[selectedJobId] ?? [];
        if (current.some((row) => row.event_id === data.event_id)) {
          return previous;
        }
        return {
          ...previous,
          [selectedJobId]: [...current, data]
        };
      });
      loadJobs();
    };
    ["stdout", "stderr", "status", "system"].forEach((type) =>
      source.addEventListener(type, handler)
    );
    source.onerror = () => {
      source.close();
    };
    return () => source.close();
  }, [selectedJobId]);

  useEffect(() => {
    if (!scriptPanelJobId || scriptPanelJobId === selectedJobId) return undefined;
    const source = new EventSource(`/api/jobs/${scriptPanelJobId}/events`);
    const handler = (event) => {
      const data = JSON.parse(event.data);
      setJobLogs((previous) => {
        const current = previous[scriptPanelJobId] ?? [];
        if (current.some((row) => row.event_id === data.event_id)) {
          return previous;
        }
        return {
          ...previous,
          [scriptPanelJobId]: [...current, data]
        };
      });
      loadJobs();
    };
    ["stdout", "stderr", "status", "system"].forEach((type) =>
      source.addEventListener(type, handler)
    );
    source.onerror = () => {
      source.close();
    };
    return () => source.close();
  }, [scriptPanelJobId, selectedJobId]);

  async function loadScripts() {
    try {
      const data = await apiJson("/api/scripts");
      setScripts(data);
      if (!selectedScriptId && data.length > 0) {
        setSelectedScriptId(data[0].id);
      }
      setDraftByScript((previous) => {
        const next = { ...previous };
        for (const script of data) {
          if (!next[script.id]) {
            next[script.id] = defaultDraftForScript(script);
          }
        }
        return next;
      });
    } catch (error) {
      setRunError(String(error.message ?? error));
    }
  }

  async function loadJobs() {
    try {
      const data = await apiJson("/api/jobs");
      setJobs(data);
      setSelectedJobId((previous) => {
        if (previous && data.some((job) => job.job_id === previous)) {
          return previous;
        }
        if (data.length > 0) {
          return data[0].job_id;
        }
        return "";
      });
    } catch (error) {
      setJobMessage(String(error.message ?? error));
    }
  }

  async function loadEnv(reveal = false) {
    try {
      const data = await apiJson(`/api/env?reveal=${reveal ? "true" : "false"}`);
      setEnvValues(data.values);
      if (data.profiles) {
        setProfiles(data.profiles.profiles ?? []);
        setActiveProfileId(data.profiles.active_profile_id ?? "");
      }
      const initial = {};
      for (const row of data.values) {
        initial[row.key] = row.value ?? "";
      }
      for (const setting of API_SETTINGS) {
        if (!initial[setting.key]) {
          initial[setting.key] = setting.defaultValue;
        }
      }
      setEnvOriginal(initial);
      setEnvDrafts(initial);
      setEnvMessage("");
    } catch (error) {
      setEnvMessage(String(error.message ?? error));
    }
  }

  async function loadProfiles() {
    try {
      const data = await apiJson("/api/profiles");
      setProfiles(data.profiles ?? []);
      setActiveProfileId(data.active_profile_id ?? "");
    } catch (error) {
      setProfileMessage(String(error.message ?? error));
    }
  }

  async function loadXmlXslFiles(options = {}) {
    const { applyDefault = false } = options;
    setXmlXslFilesBusy(true);
    setXmlXslFilesMessage("");
    try {
      const data = await apiJson("/api/config/xsl-files");
      const items = data.items ?? [];
      setXmlXslFiles(items);
      if (!items.length) {
        setXmlXslFilesMessage("No .xsl files found in configs.");
        return;
      }
      if (applyDefault) {
        const current = String(draftByScript?.xml_to_pdf?.fieldValues?.xsl_path ?? "").trim();
        if (!current) {
          setField("xml_to_pdf", "xsl_path", items[0].path);
        }
      }
    } catch (error) {
      setXmlXslFiles([]);
      setXmlXslFilesMessage(String(error.message ?? error));
    } finally {
      setXmlXslFilesBusy(false);
    }
  }

  async function loadFunctionalStatus() {
    try {
      const data = await apiJson("/api/functional-status");
      setFunctionalByScript(data.functional_by_script ?? {});
      setAdminMessage("");
    } catch (error) {
      setAdminMessage(String(error.message ?? error));
    }
  }

  async function changeActiveProfile(nextProfileId) {
    try {
      const payload = { profile_id: nextProfileId ?? "" };
      const data = await apiJson("/api/profiles/active", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      setProfiles(data.profiles ?? []);
      setActiveProfileId(data.active_profile_id ?? "");
      setProfileMessage("Active client updated.");
    } catch (error) {
      setProfileMessage(String(error.message ?? error));
    }
  }

  function beginCreateProfile() {
    setEditingProfileId("");
    setProfileForm(emptyProfileForm());
    setProfileMessage("");
  }

  async function beginEditProfile(profileId) {
    try {
      const data = await apiJson(`/api/profiles/${encodeURIComponent(profileId)}`);
      setEditingProfileId(data.id);
      setProfileForm({
        profile_id: data.id ?? "",
        name: data.name ?? "",
        client_id: data.client_id ?? "",
        client_secret: data.client_secret ?? "",
        project_id: data.project_id ?? ""
      });
      setProfileMessage("");
    } catch (error) {
      setProfileMessage(String(error.message ?? error));
    }
  }

  function setProfileField(key, value) {
    setProfileForm((previous) => ({
      ...previous,
      [key]: key === "profile_id" ? sanitizeProfileId(value) : value
    }));
  }

  function profilePayload() {
    return {
      name: profileForm.name,
      client_id: profileForm.client_id,
      client_secret: profileForm.client_secret,
      project_id: profileForm.project_id
    };
  }

  async function saveProfile() {
    try {
      if (editingProfileId) {
        const data = await apiJson(`/api/profiles/${encodeURIComponent(editingProfileId)}`, {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(profilePayload())
        });
        setProfiles(data.profiles ?? []);
        setActiveProfileId(data.active_profile_id ?? "");
        if (data.profile) {
          setProfileForm({
            profile_id: data.profile.id ?? "",
            name: data.profile.name ?? "",
            client_id: data.profile.client_id ?? "",
            client_secret: data.profile.client_secret ?? "",
            project_id: data.profile.project_id ?? ""
          });
        }
        setProfileMessage("Profile updated.");
        return;
      }

      if (!profileForm.profile_id.trim()) {
        setProfileMessage("Profile ID is required.");
        return;
      }

      const data = await apiJson("/api/profiles", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          profile_id: profileForm.profile_id,
          ...profilePayload()
        })
      });
      setProfiles(data.profiles ?? []);
      setActiveProfileId(data.active_profile_id ?? "");
      if (data.profile) {
        setEditingProfileId(data.profile.id ?? "");
        setProfileForm({
          profile_id: data.profile.id ?? "",
          name: data.profile.name ?? "",
          client_id: data.profile.client_id ?? "",
          client_secret: data.profile.client_secret ?? "",
          project_id: data.profile.project_id ?? ""
        });
      }
      setProfileMessage("Profile created.");
    } catch (error) {
      setProfileMessage(String(error.message ?? error));
    }
  }

  async function deleteProfile(profileId) {
    const ok = window.confirm(`Delete profile ${profileId}?`);
    if (!ok) return;
    try {
      const data = await apiJson(`/api/profiles/${encodeURIComponent(profileId)}`, {
        method: "DELETE"
      });
      setProfiles(data.profiles ?? []);
      setActiveProfileId(data.active_profile_id ?? "");
      if (editingProfileId === profileId) {
        setEditingProfileId("");
        setProfileForm(emptyProfileForm());
      }
      setProfileMessage("Profile deleted.");
    } catch (error) {
      setProfileMessage(String(error.message ?? error));
    }
  }

  function setField(scriptId, fieldId, value) {
    setDraftByScript((previous) => {
      const base = previous[scriptId] ?? { mode: "apply", rawArgs: "", fieldValues: {} };
      return {
        ...previous,
        [scriptId]: {
          ...base,
          fieldValues: {
            ...base.fieldValues,
            [fieldId]: value
          }
        }
      };
    });
  }

  function setRawArgs(scriptId, value) {
    setDraftByScript((previous) => {
      const base = previous[scriptId] ?? { mode: "apply", rawArgs: "", fieldValues: {} };
      return {
        ...previous,
        [scriptId]: {
          ...base,
          rawArgs: value
        }
      };
    });
  }

  function setMode(scriptId, value) {
    setDraftByScript((previous) => {
      const base = previous[scriptId] ?? { mode: "apply", rawArgs: "", fieldValues: {} };
      return {
        ...previous,
        [scriptId]: {
          ...base,
          mode: value
        }
      };
    });
  }

  async function setFunctional(scriptId, value) {
    const nextValue = Boolean(value);
    const previousValue = Boolean(functionalByScript[scriptId]);
    setFunctionalByScript((previous) => ({
      ...previous,
      [scriptId]: nextValue,
    }));
    try {
      const data = await apiJson(`/api/functional-status/${encodeURIComponent(scriptId)}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ functional: nextValue })
      });
      setFunctionalByScript(data.functional_by_script ?? {});
      setAdminMessage("Functional status saved.");
    } catch (error) {
      setFunctionalByScript((previous) => ({
        ...previous,
        [scriptId]: previousValue,
      }));
      setAdminMessage(String(error.message ?? error));
    }
  }

  function setChecklistIdForSelectedScript(checklistId) {
    if (!selectedScript || !isChecklistPdfScript(selectedScript.id)) return;
    setField(selectedScript.id, "checklist_id", checklistId);
  }

  async function runSelectedScript() {
    if (!selectedScript) return;
    const draft = selectedScriptDraft ?? defaultDraftForScript(selectedScript);
    if (selectedScriptIsInternalApi && !internalBearerToken.trim()) {
      setRunError("Bearer token is required for Internal API scripts.");
      return;
    }
    if (selectedScript.safety === "mutating" && draft.mode === "apply") {
      const ok = window.confirm(
        "This script is marked as mutating and is set to APPLY mode. Continue?"
      );
      if (!ok) return;
    }

    setRunError("");
    setRunMessage("");
    if (selectedScript.id === "clinical_concepts_status") {
      setClinicalPreview({ jobId: "", artifactPath: "", headers: [], rows: [] });
      setClinicalPreviewMessage("Waiting for run to finish...");
    }
    try {
      const payloadFieldValues = {
        ...(draft.fieldValues ?? {}),
      };
      if (selectedScript.id === "update_user_email_domains") {
        const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
        payloadFieldValues.log_file = `logs/update_user_email_domains_${timestamp}.log`;
      }
      const payload = {
        script_id: selectedScript.id,
        mode: selectedScript.supports_mode ? draft.mode : null,
        field_values: payloadFieldValues,
        raw_args: draft.rawArgs ?? "",
        internal_bearer_token: selectedScriptIsInternalApi ? internalBearerToken : null
      };
      const result = await apiJson("/api/jobs", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      setRunMessage(`Queued job ${result.job_id}`);
      setScriptPanelJobId(result.job_id);
      setSelectedJobId((previous) => previous || result.job_id);
      if (selectedScriptIsInternalApi) {
        setInternalBearerToken("");
      }
      await loadJobs();
    } catch (error) {
      setRunError(String(error.message ?? error));
    }
  }

  async function uploadBulkCreateCsv(file) {
    if (!file) return;
    setBulkUploadBusy(true);
    setBulkUploadMessage("");
    try {
      const form = new FormData();
      form.append("file", file);
      const response = await fetch("/api/uploads/csv", {
        method: "POST",
        body: form
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        const detail = payload?.detail ?? `Upload failed (${response.status})`;
        throw new Error(typeof detail === "string" ? detail : JSON.stringify(detail));
      }
      setField("bulk_create_users_from_csv", "csv", payload.path);
      setBulkUploadMessage(`CSV uploaded: ${payload.path}`);
    } catch (error) {
      setBulkUploadMessage(String(error.message ?? error));
    } finally {
      setBulkUploadBusy(false);
    }
  }

  async function uploadBulkCreateLogFile(file) {
    if (!file) return;
    setBulkUploadBusy(true);
    setBulkUploadMessage("");
    try {
      const form = new FormData();
      form.append("file", file);
      const response = await fetch("/api/uploads/file", {
        method: "POST",
        body: form
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        const detail = payload?.detail ?? `Upload failed (${response.status})`;
        throw new Error(typeof detail === "string" ? detail : JSON.stringify(detail));
      }
      setField("bulk_create_users_from_csv", "log_file", payload.path);
      setBulkUploadMessage(`Log file selected: ${payload.path}`);
    } catch (error) {
      setBulkUploadMessage(String(error.message ?? error));
    } finally {
      setBulkUploadBusy(false);
    }
  }

  async function selectBulkCreateOutputDirectory() {
    if (!window.showDirectoryPicker) {
      setBulkUploadMessage("Folder picker not supported in this browser.");
      return;
    }
    setBulkUploadBusy(true);
    setBulkUploadMessage("");
    try {
      const handle = await window.showDirectoryPicker();
      const payload = await apiJson("/api/uploads/folder", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ folder_name: handle.name })
      });
      setField("bulk_create_users_from_csv", "out_dir", payload.path);
      setBulkUploadMessage(`Output directory selected: ${payload.path}`);
    } catch (error) {
      if (error?.name === "AbortError") {
        setBulkUploadMessage("");
      } else {
        setBulkUploadMessage(String(error.message ?? error));
      }
    } finally {
      setBulkUploadBusy(false);
    }
  }

  async function selectEvaluateChecklistOutputDirectory() {
    if (!selectedScript || !isChecklistPdfScript(selectedScript.id)) {
      return;
    }
    if (!window.showDirectoryPicker) {
      setEvaluateOutputDirMessage("Folder picker not supported in this browser.");
      return;
    }
    setEvaluateOutputDirBusy(true);
    setEvaluateOutputDirMessage("");
    try {
      const handle = await window.showDirectoryPicker();
      const payload = await apiJson("/api/uploads/folder", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ folder_name: handle.name })
      });
      setField(selectedScript.id, "output_dir", payload.path);
      setEvaluateOutputDirMessage(`Output directory selected: ${payload.path}`);
    } catch (error) {
      if (error?.name === "AbortError") {
        setEvaluateOutputDirMessage("");
      } else {
        setEvaluateOutputDirMessage(String(error.message ?? error));
      }
    } finally {
      setEvaluateOutputDirBusy(false);
    }
  }

  async function loadEvaluateChecklists() {
    const selectedProjectId = String(selectedScriptDraft?.fieldValues?.project_id ?? "").trim();
    if (!selectedProjectId) {
      setEvaluateChecklistsMessage("Select a Project ID first.");
      setEvaluateChecklists([]);
      return;
    }
    setEvaluateChecklistsBusy(true);
    setEvaluateChecklistsMessage("");
    try {
      const payload = await apiJson("/api/public/checklists", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          profile_id: activeProfileId || null,
          project_id: selectedProjectId || null,
        })
      });
      const items = payload.items ?? [];
      setEvaluateChecklists(items);
      const currentChecklistId = String(selectedScriptDraft?.fieldValues?.checklist_id ?? "").trim();
      const currentExists = items.some((item) => String(item?.id ?? "") === currentChecklistId);
      if (items.length > 0 && !currentExists) {
        setChecklistIdForSelectedScript(items[0].id);
      }
      setEvaluateChecklistsMessage(`Loaded ${payload.count ?? items.length} checklists.`);
    } catch (error) {
      setEvaluateChecklistsMessage(String(error.message ?? error));
      setEvaluateChecklists([]);
    } finally {
      setEvaluateChecklistsBusy(false);
    }
  }

  async function loadEvaluateProjects() {
    if (!evaluateProjectsBearerToken.trim()) {
      setEvaluateProjectsMessage("Bearer token is required to load project IDs.");
      return;
    }
    setEvaluateProjectsBusy(true);
    setEvaluateProjectsMessage("");
    try {
      const payload = await apiJson("/api/internal/projects", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          bearer_token: evaluateProjectsBearerToken.trim() || null,
          profile_id: activeProfileId || null,
        })
      });
      const items = payload.items ?? [];
      setEvaluateProjects(items);
      setEvaluateProjectsMessage(`Loaded ${payload.count ?? items.length} project IDs.`);
    } catch (error) {
      setEvaluateProjectsMessage(String(error.message ?? error));
      setEvaluateProjects([]);
    } finally {
      setEvaluateProjectsBusy(false);
    }
  }

  async function loadTenantProjects() {
    if (!selectedScript || !supportsTenantProjectPicker(selectedScript.id)) return;
    setTenantProjectsBusy(true);
    setTenantProjectsMessage("");
    try {
      const payload = await apiJson("/api/internal/projects", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          bearer_token: internalBearerToken.trim() || null,
          profile_id: activeProfileId || null,
        })
      });
      setTenantProjects(payload.items ?? []);
      setTenantProjectsMessage(`Loaded ${payload.count ?? 0} projects.`);
    } catch (error) {
      setTenantProjectsMessage(String(error.message ?? error));
      setTenantProjects([]);
    } finally {
      setTenantProjectsBusy(false);
    }
  }

  function selectAllTenantProjects() {
    if (!selectedScript || selectedScript.id !== "update_users_new_projects") return;
    const allIds = tenantProjects.map((project) => project.id);
    setField(selectedScript.id, "project_id", allIds.join(","));
  }

  function clearSelectedTenantProjects() {
    if (!selectedScript || !supportsTenantProjectPicker(selectedScript.id)) return;
    setField(selectedScript.id, "project_id", "");
  }

  async function loadTenantUsers() {
    if (!internalBearerToken.trim()) {
      setTenantUsersMessage("Bearer token is required to load users.");
      return;
    }
    setTenantUsersBusy(true);
    setTenantUsersMessage("");
    try {
      const payload = await apiJson("/api/internal/users", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          bearer_token: internalBearerToken.trim() || null,
        })
      });
      const items = payload.items ?? [];
      setTenantUsers(items);
      setTenantUsersMessage(`Loaded ${payload.count ?? 0} users.`);
      setPermissionLookupUserId((previous) => previous || items[0]?.id || "");
    } catch (error) {
      setTenantUsersMessage(String(error.message ?? error));
      setTenantUsers([]);
    } finally {
      setTenantUsersBusy(false);
    }
  }

  function selectAllTargetUsers() {
    if (!selectedScript || selectedScript.id !== "update_user_permissions") return;
    const allUserIds = tenantUsers.map((user) => user.id);
    setField(selectedScript.id, "user_ids", allUserIds.join(","));
  }

  function clearTargetUsers() {
    if (!selectedScript || selectedScript.id !== "update_user_permissions") return;
    setField(selectedScript.id, "user_ids", "");
    setTargetUsersMessage("Target user filter cleared. Script will apply to all users.");
  }

  async function fetchUserPermissionsById(userId) {
    return apiJson("/api/internal/user-permissions", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        bearer_token: internalBearerToken.trim() || null,
        user_id: userId,
      })
    });
  }

  async function clonePermissionsFromUser() {
    if (!internalBearerToken.trim()) {
      setClonePermissionsMessage("Bearer token is required to clone permissions.");
      return;
    }
    const userId = permissionLookupUserId.trim();
    if (!userId) {
      setClonePermissionsMessage("Select a user first.");
      return;
    }
    setClonePermissionsBusy(true);
    setClonePermissionsMessage("");
    try {
      const payload = await fetchUserPermissionsById(userId);
      const sourceUser = payload.user ?? null;
      const permissions = Array.isArray(sourceUser?.permissions)
        ? sourceUser.permissions
        : Array.isArray(payload.permissions)
          ? payload.permissions
          : [];
      const normalized = [...new Set(permissions.map((value) => String(value ?? "").trim()).filter(Boolean))];
      if (selectedScript?.id === "update_user_permissions") {
        setField(selectedScript.id, "permissions", normalized.join(","));
      }
      setSingleUserPermissions(sourceUser);
      setClonePermissionsMessage(
        `Cloned ${normalized.length} permissions from ${sourceUser?.name ?? userId} into the Permissions field.`
      );
      setTargetUsersMessage("");
    } catch (error) {
      setClonePermissionsMessage(String(error.message ?? error));
    } finally {
      setClonePermissionsBusy(false);
    }
  }

  async function queryAllUsersPermissions() {
    if (!internalBearerToken.trim()) {
      setAllUsersPermissionsMessage("Bearer token is required to query all users and permissions.");
      return;
    }
    setAllUsersPermissionsBusy(true);
    setAllUsersPermissionsMessage("");
    try {
      const payload = await apiJson("/api/internal/users-permissions", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          bearer_token: internalBearerToken.trim() || null,
        })
      });
      setAllUsersPermissions({
        items: payload.items ?? [],
        permission_keys: payload.permission_keys ?? [],
      });
      setAllUsersPermissionsMessage(`Loaded permissions for ${payload.count ?? 0} users.`);
    } catch (error) {
      setAllUsersPermissionsMessage(String(error.message ?? error));
      setAllUsersPermissions({ items: [], permission_keys: [] });
    } finally {
      setAllUsersPermissionsBusy(false);
    }
  }

  async function loadBackupPreview(jobId, artifactPath) {
    setBackupPreviewBusy(true);
    setBackupPreviewMessage("");
    try {
      const response = await fetch(
        `/api/artifact?path=${encodeURIComponent(artifactPath)}&job_id=${encodeURIComponent(jobId)}`,
        { cache: "no-store" }
      );
      if (!response.ok) {
        throw new Error(`Failed to load preview CSV (${response.status})`);
      }
      const text = await response.text();
      const parsed = csvParse(text);
      setBackupPreview({
        jobId,
        artifactPath,
        headers: parsed.headers,
        rows: parsed.rows
      });
      setBackupPreviewMessage(`Loaded ${parsed.rows.length} rows from ${artifactPath}`);
    } catch (error) {
      setBackupPreview({ jobId: "", artifactPath: "", headers: [], rows: [] });
      setBackupPreviewMessage(String(error.message ?? error));
    } finally {
      setBackupPreviewBusy(false);
    }
  }

  function exportBackupPreviewCsv() {
    if (!backupPreview.headers.length) {
      setBackupPreviewMessage("Nothing to export yet.");
      return;
    }
    const lines = [];
    lines.push(backupPreview.headers.map(csvEscapeCell).join(","));
    for (const row of backupPreview.rows) {
      lines.push(backupPreview.headers.map((header) => csvEscapeCell(row[header] ?? "")).join(","));
    }
    const blob = new Blob([lines.join("\n")], { type: "text/csv;charset=utf-8;" });
    const href = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = href;
    link.download = `backup_user_permissions_preview_${new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-")}.csv`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(href);
  }

  async function loadClinicalPreview(jobId, artifactPath) {
    setClinicalPreviewBusy(true);
    setClinicalPreviewMessage("");
    try {
      const response = await fetch(
        `/api/artifact?path=${encodeURIComponent(artifactPath)}&job_id=${encodeURIComponent(jobId)}`,
        { cache: "no-store" }
      );
      if (!response.ok) {
        throw new Error(`Failed to load preview CSV (${response.status})`);
      }
      const text = await response.text();
      const parsed = csvParse(text);
      const normalized = normalizeClinicalPreview(parsed);
      setClinicalPreview({
        jobId,
        artifactPath,
        headers: normalized.headers,
        rows: normalized.rows
      });
      setClinicalPreviewMessage(`Loaded ${normalized.rows.length} rows from ${artifactPath}`);
    } catch (error) {
      setClinicalPreview({ jobId: "", artifactPath: "", headers: [], rows: [] });
      setClinicalPreviewMessage(String(error.message ?? error));
    } finally {
      setClinicalPreviewBusy(false);
    }
  }

  function exportClinicalPreviewCsv() {
    if (!clinicalPreview.headers.length) {
      setClinicalPreviewMessage("Nothing to export yet.");
      return;
    }
    const lines = [];
    lines.push(clinicalPreview.headers.map(csvEscapeCell).join(","));
    for (const row of clinicalPreview.rows) {
      lines.push(clinicalPreview.headers.map((header) => csvEscapeCell(row[header] ?? "")).join(","));
    }
    const blob = new Blob([lines.join("\n")], { type: "text/csv;charset=utf-8;" });
    const href = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = href;
    link.download = `clinical_concepts_preview_${new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-")}.csv`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(href);
  }

  async function cancelJobById(jobId) {
    if (!jobId) return;
    try {
      await apiJson(`/api/jobs/${jobId}/cancel`, { method: "POST" });
      setJobMessage("Cancel requested.");
      await loadJobs();
    } catch (error) {
      setJobMessage(String(error.message ?? error));
    }
  }

  async function cancelSelectedJob() {
    if (!selectedJob) return;
    await cancelJobById(selectedJob.job_id);
  }

  async function sendJobInput() {
    if (!selectedJob || !stdinText.trim()) return;
    try {
      await apiJson(`/api/jobs/${selectedJob.job_id}/stdin`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ text: stdinText })
      });
      setStdinText("");
    } catch (error) {
      setJobMessage(String(error.message ?? error));
    }
  }

  async function saveEnvChanges() {
    const updates = {};
    Object.entries(envDrafts).forEach(([key, value]) => {
      if ((envOriginal[key] ?? "") !== value) {
        updates[key] = value ?? "";
      }
    });
    if (Object.keys(updates).length === 0) {
      setEnvMessage("No changes to save.");
      return;
    }
    try {
      await apiJson("/api/env", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ updates, clear_missing: false })
      });
      setEnvMessage(`Saved ${Object.keys(updates).length} key(s).`);
      await loadEnv(revealSecrets);
    } catch (error) {
      setEnvMessage(String(error.message ?? error));
    }
  }

  return (
    <div className="shell">
      <header className="hero">
        <div className="brand">
          <img src={logo} alt="xCures logo" className="logo" />
          <h1>Customer Success User Ops</h1>
        </div>
        <div className="meta">
          <label className="profile-switch">
            <span>Client Profile</span>
            <select
              value={activeProfileId}
              onChange={(event) => changeActiveProfile(event.target.value)}
            >
              <option value="">Default .env Values</option>
              {profiles.map((profile) => (
                <option key={profile.id} value={profile.id}>
                  {profile.name}
                </option>
              ))}
            </select>
          </label>
          <span>Host: 127.0.0.1:8765</span>
          <span>Queue: 1 job at a time</span>
          {profileMessage && <span>{profileMessage}</span>}
        </div>
      </header>

      <nav className="tabs">
        {TABS.map((tab) => (
          <button
            key={tab.id}
            className={activeTab === tab.id ? "active" : ""}
            onClick={() => setActiveTab(tab.id)}
          >
            {tab.label}
          </button>
        ))}
      </nav>

      {(activeTab === "internal-scripts" ||
        activeTab === "public-scripts" ||
        activeTab === "local-scripts") && (
        <section className="panel scripts-grid">
          <aside className="script-list">
            <input
              value={scriptSearch}
              onChange={(event) => setScriptSearch(event.target.value)}
              placeholder="Search scripts..."
            />
            <ul>
              {filteredScripts.map((script) => (
                <li key={script.id}>
                  <button
                    className={selectedScriptId === script.id ? "selected" : ""}
                    onClick={() => setSelectedScriptId(script.id)}
                  >
                    <strong>
                      <span className="script-list-title-row">
                        {Boolean(functionalByScript[script.id]) && (
                          <span className="script-list-functional-indicator" title="Functional">
                            <FontAwesomeIcon icon={faCircleCheck} />
                          </span>
                        )}
                        <ScriptNameWithIcon scriptId={script.id} name={script.name} />
                      </span>
                    </strong>
                    <small className={`api-tag ${apiTypeClass(script)}`}>{apiTypeLabel(script)}</small>
                    <small className={`safety-tag ${script.safety}`}>{safetyLabel(script.safety)}</small>
                  </button>
                </li>
              ))}
            </ul>
          </aside>

          <div
            className={`script-detail script-${(selectedScript?.id ?? "").replace(/_/g, "-")}`}
          >
            {!selectedScript && <p>Select a script to configure and run it.</p>}
            {selectedScript && (
              <>
                <div className="script-header">
                  <h2>
                    <ScriptNameWithIcon scriptId={selectedScript.id} name={selectedScript.name} />
                  </h2>
                  <div className="script-pills">
                    <span className={`pill api ${apiTypeClass(selectedScript)}`}>{apiTypeLabel(selectedScript)}</span>
                    <span className={`pill ${selectedScript.safety}`}>{safetyLabel(selectedScript.safety)}</span>
                  </div>
                </div>
                {Boolean(functionalByScript[selectedScript.id]) && (
                  <div className="script-functional-indicator">
                    <FontAwesomeIcon icon={faCircleCheck} />
                    <span>Functional</span>
                  </div>
                )}
                <p>{selectedScript.description}</p>

                {selectedScriptIsInternalApi && (
                  <label>
                    <span>Bearer Token (required each run)</span>
                    <textarea
                      className="token-input"
                      value={internalBearerToken}
                      placeholder="Paste bearer token for this run"
                      onChange={(event) => setInternalBearerToken(event.target.value)}
                    />
                  </label>
                )}

                {!selectedScriptIsInternalApi && usesOptionalProjectLoaderBearer(selectedScript.id) && (
                  <label>
                    <span>Bearer Token (optional, only for Load Projects from Tenant)</span>
                    <textarea
                      className="token-input"
                      value={internalBearerToken}
                      placeholder="Optional: paste internal API bearer token for project loader"
                      onChange={(event) => setInternalBearerToken(event.target.value)}
                    />
                  </label>
                )}
                {isChecklistPdfScript(selectedScript.id) && (
                  <label>
                    <span>Bearer Token (required for Load Project IDs)</span>
                    <textarea
                      className="token-input"
                      value={evaluateProjectsBearerToken}
                      placeholder="Paste bearer token for project ID loader"
                      onChange={(event) => setEvaluateProjectsBearerToken(event.target.value)}
                    />
                  </label>
                )}

                {selectedScript.supports_mode && (
                  <div className="mode-row">
                    <label>Mode</label>
                    <select
                      value={selectedScriptDraft?.mode ?? selectedScript.default_mode ?? "apply"}
                      onChange={(event) => setMode(selectedScript.id, event.target.value)}
                    >
                      <option value="dry-run">dry-run</option>
                      <option value="apply">apply</option>
                    </select>
                  </div>
                )}

                {selectedScript.id === "update_user_permissions" && (
                  <div className="backup-preview">
                    <div className="script-header">
                      <h3>Step 1: Load Users</h3>
                    </div>
                    <div className="script-actions">
                      <button type="button" onClick={loadTenantUsers}>
                        Load Users
                      </button>
                      {(tenantUsersBusy || allUsersPermissionsBusy) && <small>Loading...</small>}
                    </div>
                    {tenantUsersMessage && <p>{tenantUsersMessage}</p>}
                    {allUsersPermissionsMessage && <p>{allUsersPermissionsMessage}</p>}
                    <p className="ux-note">
                      Source user sets the permissions to clone. Target User IDs controls who gets updated when you run.
                    </p>
                    <p className="ux-note">
                      Current targets: {selectedBulkTargetUserIds.length
                        ? `${selectedBulkTargetUserIds.length} selected`
                        : "All users (no target IDs selected)"}
                    </p>
                    {targetUsersMessage && <p>{targetUsersMessage}</p>}

                    <div className="permission-query-row">
                      <label>
                        <h3>Step 2: Select Source User</h3>
                        <select
                          value={permissionLookupUserId}
                          onChange={(event) => setPermissionLookupUserId(event.target.value)}
                        >
                          <option value="">Select a user...</option>
                          {tenantUsers.map((user) => (
                            <option key={user.id} value={user.id}>
                              {user.name} {user.email ? `(${user.email})` : ""}
                            </option>
                          ))}
                        </select>
                        <div className="script-actions">
                          <button type="button" onClick={clonePermissionsFromUser}>
                            Clone Permissions From User
                          </button>
                          {clonePermissionsBusy && <small>Loading...</small>}
                        </div>
                      </label>
                    </div>

                    {clonePermissionsMessage && <p>{clonePermissionsMessage}</p>}
                    {singleUserPermissions && (
                      <div className="single-user-permissions">
                        <p>
                          <strong>{singleUserPermissions.name}</strong>
                          {singleUserPermissions.email ? ` (${singleUserPermissions.email})` : ""} - {" "}
                          {singleUserPermissions.permission_count} permissions
                        </p>
                        <div className="permission-chips">
                          {singleUserPermissions.permissions.map((permission) => (
                            <span key={`${singleUserPermissions.id}-${permission}`} className="permission-chip">
                              {permission}
                            </span>
                          ))}
                        </div>
                      </div>
                    )}

                    {allUsersPermissions.items.length > 0 && (
                      <div className="backup-preview-table-wrap permissions-compare-wrap">
                        <table className="backup-preview-table permissions-compare-table">
                          <thead>
                            <tr>
                              <th>Name</th>
                              <th>Email</th>
                              <th>Count</th>
                              <th>Permissions</th>
                              {permissionMatrixColumns.map((permission) => (
                                <th key={`permission-col-${permission}`}>{permission}</th>
                              ))}
                            </tr>
                          </thead>
                          <tbody>
                            {allUsersPermissions.items.map((user) => {
                              const userPermissionSet = new Set(user.permissions ?? []);
                              return (
                                <tr key={`permission-row-${user.id}`}>
                                  <td>{user.name || user.id}</td>
                                  <td>{user.email || "—"}</td>
                                  <td>{user.permission_count ?? 0}</td>
                                  <td>
                                    {user.error ? (
                                      <span className="error">{user.error}</span>
                                    ) : (
                                      <div className="permission-chips">
                                        {(user.permissions ?? []).map((permission) => (
                                          <span key={`${user.id}-${permission}`} className="permission-chip">
                                            {permission}
                                          </span>
                                        ))}
                                      </div>
                                    )}
                                  </td>
                                  {permissionMatrixColumns.map((permission) => (
                                    <td key={`${user.id}-${permission}`} className="permission-cell">
                                      {userPermissionSet.has(permission) ? "✓" : ""}
                                    </td>
                                  ))}
                                </tr>
                              );
                            })}
                          </tbody>
                        </table>
                      </div>
                    )}
                  </div>
                )}

                <div className={isChecklistPdfScript(selectedScript.id) ? "evaluate-options-layout" : ""}>
                  <div className={isChecklistPdfScript(selectedScript.id) ? "evaluate-options-main" : ""}>
                    <div
                      className={`field-grid ${
                        selectedScript.id === "update_users_new_projects" || selectedScript.id === "update_user_permissions"
                          ? "field-grid-two-col"
                          : ""
                      }`}
                    >
                      {selectedScript.fields.map((field) => {
                    const draft = selectedScriptDraft ?? defaultDraftForScript(selectedScript);
                    const value = fieldValueOrDefault(field, draft.fieldValues?.[field.id]);
                    if (field.type === "boolean") {
                      return (
                        <label key={field.id} className="check">
                          <input
                            type="checkbox"
                            checked={Boolean(value)}
                            onChange={(event) =>
                              setField(selectedScript.id, field.id, event.target.checked)
                            }
                          />
                          <div>
                            <span>{field.label}</span>
                            {field.description && <small>{field.description}</small>}
                          </div>
                        </label>
                      );
                    }

                    if (field.type === "select") {
                      return (
                        <label key={field.id}>
                          <span>{field.label}</span>
                          <select
                            value={value ?? ""}
                            onChange={(event) =>
                              setField(selectedScript.id, field.id, event.target.value)
                            }
                          >
                            {(field.choices ?? []).map((choice) => (
                              <option key={choice} value={choice}>
                                {choice}
                              </option>
                            ))}
                          </select>
                        </label>
                      );
                    }

                    if (selectedScript.id === "xml_to_pdf" && field.id === "xsl_path") {
                      const currentValue = String(value ?? "").trim();
                      const hasCurrentValue = currentValue
                        ? xmlXslFiles.some((item) => item.path === currentValue)
                        : false;
                      return (
                        <label key={field.id}>
                          <span>{field.label}</span>
                          <div className="csv-upload-row">
                            <button
                              type="button"
                              onClick={() => {
                                void loadXmlXslFiles();
                              }}
                            >
                              Reload XSL Files
                            </button>
                            {xmlXslFilesBusy && <small>Loading...</small>}
                          </div>
                          <select
                            value={value ?? ""}
                            onChange={(event) =>
                              setField(selectedScript.id, field.id, event.target.value)
                            }
                          >
                            {!hasCurrentValue && currentValue && (
                              <option value={currentValue}>{currentValue}</option>
                            )}
                            {xmlXslFiles.length === 0 && (
                              <option value="">No .xsl files found</option>
                            )}
                            {xmlXslFiles.map((item) => (
                              <option key={item.id} value={item.path}>
                                {item.name}
                              </option>
                            ))}
                          </select>
                          {xmlXslFilesMessage && <small>{xmlXslFilesMessage}</small>}
                        </label>
                      );
                    }

                    if (
                      selectedScript.id === "bulk_create_users_from_csv" &&
                      field.id === "csv"
                    ) {
                      return (
                        <label key={field.id}>
                          <span>{field.label}</span>
                          <input
                            type="text"
                            value={value ?? ""}
                            placeholder={field.placeholder ?? ""}
                            onChange={(event) =>
                              setField(selectedScript.id, field.id, event.target.value)
                            }
                          />
                          <div className="csv-upload-row">
                            <input
                              type="file"
                              accept=".csv,text/csv"
                              onChange={(event) => uploadBulkCreateCsv(event.target.files?.[0])}
                            />
                            {bulkUploadBusy && <small>Uploading...</small>}
                          </div>
                          {bulkUploadMessage && <small>{bulkUploadMessage}</small>}
                        </label>
                      );
                    }

                    if (
                      selectedScript.id === "bulk_create_users_from_csv" &&
                      field.id === "log_file"
                    ) {
                      return (
                        <label key={field.id}>
                          <span>{field.label}</span>
                          <input
                            type="text"
                            value={value ?? ""}
                            placeholder={field.placeholder ?? ""}
                            onChange={(event) =>
                              setField(selectedScript.id, field.id, event.target.value)
                            }
                          />
                          <div className="csv-upload-row">
                            <input
                              type="file"
                              onChange={(event) => uploadBulkCreateLogFile(event.target.files?.[0])}
                            />
                            {bulkUploadBusy && <small>Uploading...</small>}
                          </div>
                          {bulkUploadMessage && <small>{bulkUploadMessage}</small>}
                        </label>
                      );
                    }

                    if (
                      selectedScript.id === "update_user_email_domains" &&
                      field.id === "log_file"
                    ) {
                      return null;
                    }

                    if (
                      selectedScript.id === "bulk_create_users_from_csv" &&
                      field.id === "out_dir"
                    ) {
                      return (
                        <label key={field.id}>
                          <span>{field.label}</span>
                          <input
                            type="text"
                            value={value ?? ""}
                            placeholder={field.placeholder ?? ""}
                            onChange={(event) =>
                              setField(selectedScript.id, field.id, event.target.value)
                            }
                          />
                          <div className="csv-upload-row">
                            <button type="button" onClick={selectBulkCreateOutputDirectory}>
                              Select Folder
                            </button>
                            {bulkUploadBusy && <small>Working...</small>}
                          </div>
                          {bulkUploadMessage && <small>{bulkUploadMessage}</small>}
                        </label>
                      );
                    }

                    if (
                      selectedScript.id === "clinical_concepts_status" &&
                      field.id === "project_id"
                    ) {
                      return (
                        <label key={field.id}>
                          <span>{field.label}</span>
                          <div className="csv-upload-row">
                            <button type="button" onClick={loadTenantProjects}>
                              Load Projects from Tenant
                            </button>
                            <button type="button" onClick={clearSelectedTenantProjects}>
                              Clear
                            </button>
                            {tenantProjectsBusy && <small>Loading...</small>}
                          </div>
                          <select
                            value={value ?? ""}
                            onChange={(event) =>
                              setField(selectedScript.id, field.id, event.target.value)
                            }
                          >
                            <option value="">Use Client Profile default</option>
                            {tenantProjects.map((project) => (
                              <option key={project.id} value={project.id}>
                                {project.name}
                              </option>
                            ))}
                          </select>
                          <input
                            type="text"
                            value={value ?? ""}
                            placeholder="Project ID override (optional)"
                            onChange={(event) =>
                              setField(selectedScript.id, field.id, event.target.value)
                            }
                          />
                          {tenantProjectsMessage && <small>{tenantProjectsMessage}</small>}
                        </label>
                      );
                    }

                    if (selectedScript.id === "update_user_permissions" && field.id === "permissions") {
                      return null;
                    }

                    if (
                      selectedScript.id === "update_user_permissions" &&
                      field.id === "user_ids"
                    ) {
                      const selectedUserIds = parseCommaValues(value);
                      return (
                        <label key={field.id}>
                          <h3>Step 3: Target Users</h3>
                          <div className="csv-upload-row">
                            <button type="button" onClick={loadTenantUsers}>
                              Load Users from Tenant
                            </button>
                            <button type="button" onClick={selectAllTargetUsers} disabled={!tenantUsers.length}>
                              Select All
                            </button>
                            <button type="button" onClick={clearTargetUsers}>
                              Clear
                            </button>
                            {tenantUsersBusy && <small>Loading...</small>}
                          </div>
                          <select
                            multiple
                            size={Math.min(Math.max(tenantUsers.length, 8), 14)}
                            value={selectedUserIds}
                            onChange={(event) => {
                              const selected = Array.from(event.target.selectedOptions).map(
                                (option) => option.value
                              );
                              setField(selectedScript.id, field.id, selected.join(","));
                            }}
                          >
                            {tenantUsers.map((user) => (
                              <option key={user.id} value={user.id}>
                                {user.name} {user.email ? `(${user.email})` : ""}
                              </option>
                            ))}
                          </select>
                          <input
                            type="text"
                            value={value ?? ""}
                            placeholder="Selected user IDs (comma-separated). Leave blank to target all users."
                            onChange={(event) =>
                              setField(selectedScript.id, field.id, event.target.value)
                            }
                          />
                          {tenantUsersMessage && <small>{tenantUsersMessage}</small>}
                        </label>
                      );
                    }

                    if (
                      selectedScript.id === "update_users_new_projects" &&
                      field.id === "project_id"
                    ) {
                      const selectedProjectIds = parseCommaValues(value);
                      return (
                        <label key={field.id}>
                          <span>{field.label}</span>
                          <div className="csv-upload-row">
                            <button type="button" onClick={loadTenantProjects}>
                              Load Projects from Tenant
                            </button>
                            <button type="button" onClick={selectAllTenantProjects} disabled={!tenantProjects.length}>
                              Select All
                            </button>
                            <button type="button" onClick={clearSelectedTenantProjects}>
                              Clear
                            </button>
                            {tenantProjectsBusy && <small>Loading...</small>}
                          </div>
                          <select
                            multiple
                            size={Math.min(Math.max(tenantProjects.length, 6), 12)}
                            value={selectedProjectIds}
                            onChange={(event) => {
                              const selected = Array.from(event.target.selectedOptions).map(
                                (option) => option.value
                              );
                              setField(selectedScript.id, field.id, selected.join(","));
                            }}
                          >
                            {tenantProjects.map((project) => (
                              <option key={project.id} value={project.id}>
                                {project.name}
                              </option>
                            ))}
                          </select>
                          <input
                            type="text"
                            value={value ?? ""}
                            placeholder="Selected project IDs (comma-separated)"
                            onChange={(event) =>
                              setField(selectedScript.id, field.id, event.target.value)
                            }
                          />
                          {tenantProjectsMessage && <small>{tenantProjectsMessage}</small>}
                        </label>
                      );
                    }

                    if (
                      isChecklistPdfScript(selectedScript.id) &&
                      field.id === "output_dir"
                    ) {
                      return (
                        <label key={field.id}>
                          <span>
                            {field.label}
                            {field.required ? " *" : ""}
                          </span>
                          <input
                            type="text"
                            value={value ?? ""}
                            placeholder={field.placeholder ?? ""}
                            onChange={(event) =>
                              setField(selectedScript.id, field.id, event.target.value)
                            }
                          />
                          <div className="csv-upload-row">
                            <button type="button" onClick={selectEvaluateChecklistOutputDirectory}>
                              Select Folder
                            </button>
                            {evaluateOutputDirBusy && <small>Working...</small>}
                          </div>
                          {evaluateOutputDirMessage && <small>{evaluateOutputDirMessage}</small>}
                        </label>
                      );
                    }

                        return (
                          <label key={field.id}>
                            <span>
                              {field.label}
                              {field.required ? " *" : ""}
                            </span>
                            <input
                              type={field.type === "number" ? "number" : "text"}
                              value={value ?? ""}
                              placeholder={field.placeholder ?? ""}
                              onChange={(event) =>
                                setField(selectedScript.id, field.id, event.target.value)
                              }
                            />
                          </label>
                        );
                      })}
                    </div>
                  </div>
                  {isChecklistPdfScript(selectedScript.id) && (
                    <aside className="evaluate-checklist-column">
                      <div className="evaluate-picker-section evaluate-checklists-section">
                        <h3>Project IDs</h3>
                        <div className="script-actions">
                          <button type="button" onClick={loadEvaluateProjects}>
                            Load Project IDs
                          </button>
                          {evaluateProjectsBusy && <small>Loading...</small>}
                        </div>
                        <select
                          size={Math.min(Math.max(evaluateProjects.length, 8), 14)}
                          style={{ width: evaluateProjectSelectWidth, maxWidth: "100%" }}
                          value={selectedScriptDraft?.fieldValues?.project_id ?? ""}
                          onChange={(event) =>
                            setField(selectedScript.id, "project_id", event.target.value)
                          }
                        >
                          {evaluateProjects.map((project) => (
                            <option key={project.id} value={project.id}>
                              {project.name}
                            </option>
                          ))}
                        </select>
                        {evaluateProjectsMessage && <small>{evaluateProjectsMessage}</small>}
                      </div>
                      <div className="evaluate-picker-section evaluate-projects-section">
                        <h3>Checklists</h3>
                        <div className="script-actions">
                          <button type="button" onClick={loadEvaluateChecklists}>
                            Load Checklists
                          </button>
                          {evaluateChecklistsBusy && <small>Loading...</small>}
                        </div>
                        <select
                          size={Math.min(Math.max(evaluateChecklists.length, 8), 14)}
                          style={{ width: evaluateChecklistSelectWidth, maxWidth: "100%" }}
                          value={selectedScriptDraft?.fieldValues?.checklist_id ?? ""}
                          onChange={(event) => setChecklistIdForSelectedScript(event.target.value)}
                          onInput={(event) => setChecklistIdForSelectedScript(event.target.value)}
                        >
                          {evaluateChecklists.map((checklist) => (
                            <option key={checklist.id} value={checklist.id}>
                              {checklist.name}
                            </option>
                          ))}
                        </select>
                        {evaluateChecklistsMessage && <small>{evaluateChecklistsMessage}</small>}
                      </div>
                    </aside>
                  )}
                </div>

                <div className="script-actions">
                  <button onClick={runSelectedScript}>
                    {selectedScript.id === "update_user_permissions" ? "Step 4: Run Script" : "Run Script"}
                  </button>
                  {runMessage && <span className="ok">{runMessage}</span>}
                  {runError && <span className="error">{runError}</span>}
                </div>

                {scriptPanelJob && scriptPanelJob.script_id === selectedScript.id && (
                  <div className="inline-run-panel">
                    <div className="job-head">
                      <h3>Current Run</h3>
                      <span className={`status ${statusClass(scriptPanelJob.status)}`}>
                        {statusLabel(scriptPanelJob.status)}
                      </span>
                    </div>
                    <p>
                      Job: <code>{scriptPanelJob.job_id}</code> | Created: {formatTs(scriptPanelJob.created_at)} |
                      Started: {formatTs(scriptPanelJob.started_at)} | Finished: {formatTs(scriptPanelJob.finished_at)}
                    </p>
                    <div className="script-actions">
                      <button
                        type="button"
                        onClick={() => {
                          setActiveTab("jobs");
                          setSelectedJobId(scriptPanelJob.job_id);
                        }}
                      >
                        Open in Jobs
                      </button>
                      {(scriptPanelJob.status === "queued" || scriptPanelJob.status === "running") && (
                        <button type="button" onClick={() => cancelJobById(scriptPanelJob.job_id)}>
                          Cancel
                        </button>
                      )}
                    </div>
                    {scriptPanelProgressPercent !== null && (
                      <div className="run-progress" aria-label="Backup progress">
                        <div className="run-progress-label">
                          <span>Progress</span>
                          <strong>{scriptPanelProgressPercent.toFixed(1)}%</strong>
                        </div>
                        <div className="run-progress-track">
                          <div
                            className="run-progress-fill"
                            style={{ width: `${scriptPanelProgressPercent}%` }}
                          />
                        </div>
                      </div>
                    )}
                    {scriptPanelJob.artifacts?.length > 0 && (
                      <div className="artifact-list">
                        <h3>Artifacts</h3>
                        <ul>
                          {scriptPanelJob.artifacts.map((artifact) => (
                            <li key={artifact}>
                              <a href={`/api/artifact?path=${encodeURIComponent(artifact)}`} target="_blank" rel="noreferrer">
                                {artifact}
                              </a>
                            </li>
                          ))}
                        </ul>
                      </div>
                    )}
                    <pre className="log-viewer">
                      {scriptPanelDisplayEvents.map((event) => (
                        <div key={event.event_id} className={event.type}>
                          [{event.type}] {event.message}
                        </div>
                      ))}
                    </pre>
                  </div>
                )}

                {selectedScript.id === "backup_user_permissions" && (
                  <div className="backup-preview">
                    <div className="script-header">
                      <h3>Results</h3>
                      <div className="script-actions">
                        <button
                          type="button"
                          onClick={() =>
                            backupPreview.artifactPath
                              ? loadBackupPreview(backupPreview.jobId, backupPreview.artifactPath)
                              : null
                          }
                          disabled={backupPreviewBusy || !backupPreview.artifactPath}
                        >
                          Refresh Preview
                        </button>
                        <button
                          type="button"
                          onClick={exportBackupPreviewCsv}
                          disabled={!backupPreview.headers.length}
                        >
                          Export to CSV
                        </button>
                      </div>
                    </div>
                    {backupPreviewMessage && <p>{backupPreviewMessage}</p>}
                    {backupPreview.headers.length > 0 && (
                      <div className="backup-preview-table-wrap">
                        <table className="backup-preview-table">
                          <thead>
                            <tr>
                              {backupPreview.headers.map((header) => (
                                <th key={header}>{header}</th>
                              ))}
                            </tr>
                          </thead>
                          <tbody>
                            {backupPreview.rows.map((row, index) => (
                              <tr key={`${row.email ?? "row"}-${index}`}>
                                {backupPreview.headers.map((header) => {
                                  const rowKey = `${backupPreview.jobId}-${row.email ?? "row"}-${index}`;
                                  if (header.toLowerCase() === "permissions") {
                                    const permissions = splitPipeValues(row[header]);
                                    const expanded = Boolean(expandedPermissionRows[rowKey]);
                                    const visible = expanded ? permissions : permissions.slice(0, 5);
                                    const remaining = Math.max(0, permissions.length - visible.length);
                                    return (
                                      <td key={`${header}-${index}`}>
                                        <div className="permission-chips">
                                          {visible.map((permission) => (
                                            <span key={permission} className="permission-chip">
                                              {permission}
                                            </span>
                                          ))}
                                        </div>
                                        {permissions.length > 5 && (
                                          <button
                                            type="button"
                                            className="permission-toggle"
                                            onClick={() =>
                                              setExpandedPermissionRows((previous) => ({
                                                ...previous,
                                                [rowKey]: !expanded,
                                              }))
                                            }
                                          >
                                            {expanded ? "Show less" : `+${remaining} more`}
                                          </button>
                                        )}
                                      </td>
                                    );
                                  }
                                  return <td key={`${header}-${index}`}>{row[header]}</td>;
                                })}
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    )}
                  </div>
                )}

                {selectedScript.id === "clinical_concepts_status" && (
                  <div className="backup-preview">
                    <div className="script-header">
                      <h3>Results</h3>
                      <div className="script-actions">
                        <button
                          type="button"
                          onClick={() =>
                            clinicalPreview.artifactPath
                              ? loadClinicalPreview(clinicalPreview.jobId, clinicalPreview.artifactPath)
                              : null
                          }
                          disabled={clinicalPreviewBusy || !clinicalPreview.artifactPath}
                        >
                          Refresh Preview
                        </button>
                        <button
                          type="button"
                          onClick={exportClinicalPreviewCsv}
                          disabled={!clinicalPreview.headers.length}
                        >
                          Export to CSV
                        </button>
                      </div>
                    </div>
                    {clinicalPreviewMessage && <p>{clinicalPreviewMessage}</p>}
                    {clinicalPreview.headers.length > 0 && (
                      <div className="clinical-summary">
                        <div className="clinical-summary-item">
                          <FontAwesomeIcon icon={faHospitalUser} />
                          <span>
                            Total Number of Subjects: <strong>{clinicalSummary.totalSubjects}</strong>
                          </span>
                        </div>
                        <div className="clinical-summary-item">
                          <FontAwesomeIcon icon={faFile} />
                          <span>
                            Total Number of Documents: <strong>{clinicalSummary.totalDocuments}</strong>
                          </span>
                        </div>
                        <div className="clinical-summary-item">
                          <FontAwesomeIcon icon={faChartPie} />
                          <span>
                            Percent of Clinical Concepts Enabled:{" "}
                            <strong>{clinicalSummary.percentEnabled.toFixed(1)}%</strong>
                          </span>
                        </div>
                      </div>
                    )}
                    {clinicalPreview.headers.length > 0 && (
                      <div className="backup-preview-table-wrap">
                        <table className="backup-preview-table">
                          <thead>
                            <tr>
                              {clinicalPreview.headers.map((header) => {
                                const isActive = clinicalSort.key === header;
                                const indicator = isActive ? (clinicalSort.direction === "asc" ? " ↑" : " ↓") : "";
                                return (
                                  <th key={header}>
                                    <button
                                      type="button"
                                      className="sort-header-btn"
                                      onClick={() =>
                                        setClinicalSort((previous) => {
                                          if (previous.key !== header) {
                                            return { key: header, direction: "asc" };
                                          }
                                          return {
                                            key: header,
                                            direction: previous.direction === "asc" ? "desc" : "asc",
                                          };
                                        })
                                      }
                                    >
                                      {header}
                                      <span>{indicator}</span>
                                    </button>
                                  </th>
                                );
                              })}
                            </tr>
                          </thead>
                          <tbody>
                            {sortedClinicalRows.map((row, index) => (
                              <tr key={`${row.Subject ?? "row"}-${index}`}>
                                {clinicalPreview.headers.map((header) => {
                                  if (header === "Clinical Concepts Loaded") {
                                    const raw = String(row[header] ?? "").trim().toLowerCase();
                                    const isTrue = raw === "true" || raw === "1" || raw === "yes";
                                    const isFalse = raw === "false" || raw === "0" || raw === "no";
                                    return (
                                      <td key={`${header}-${index}`}>
                                        {isTrue && (
                                          <FontAwesomeIcon
                                            icon={faCircleCheck}
                                            style={{ color: "#2ba156" }}
                                            title="Loaded"
                                          />
                                        )}
                                        {isFalse && (
                                          <FontAwesomeIcon
                                            icon={faCircleXmark}
                                            style={{ color: "rgb(209, 32, 32)" }}
                                            title="Not Loaded"
                                          />
                                        )}
                                        {!isTrue && !isFalse && (row[header] ?? "")}
                                      </td>
                                    );
                                  }
                                  return <td key={`${header}-${index}`}>{row[header]}</td>;
                                })}
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    )}
                  </div>
                )}
              </>
            )}
          </div>
        </section>
      )}

      {activeTab === "jobs" && (
        <section className="panel jobs-grid">
          <aside className="job-list">
            <button onClick={loadJobs}>Refresh</button>
            <ul>
              {jobs.map((job) => (
                <li key={job.job_id}>
                  <button
                    className={selectedJobId === job.job_id ? "selected" : ""}
                    onClick={() => setSelectedJobId(job.job_id)}
                  >
                    <strong>{job.script_name}</strong>
                    <small className={`status ${statusClass(job.status)}`}>{statusLabel(job.status)}</small>
                    <small>{formatTs(job.created_at)}</small>
                  </button>
                </li>
              ))}
            </ul>
          </aside>

          <div className="job-detail">
            {!selectedJob && <p>No job selected yet.</p>}
            {selectedJob && (
              <>
                <div className="job-head">
                  <h2>{selectedJob.script_name}</h2>
                  <span className={`status ${statusClass(selectedJob.status)}`}>{statusLabel(selectedJob.status)}</span>
                </div>
                <p>
                  Created: {formatTs(selectedJob.created_at)} | Started: {formatTs(selectedJob.started_at)} |
                  Finished: {formatTs(selectedJob.finished_at)}
                </p>
                <p>Args: {selectedJob.args.join(" ") || "(none)"}</p>
                <div className="job-actions">
                  {(selectedJob.status === "queued" || selectedJob.status === "running") && (
                    <button onClick={cancelSelectedJob}>Cancel</button>
                  )}
                  {selectedJob.status === "running" && (
                    <>
                      <input
                        value={stdinText}
                        placeholder="Send input line..."
                        onChange={(event) => setStdinText(event.target.value)}
                      />
                      <button onClick={sendJobInput}>Send Input</button>
                    </>
                  )}
                </div>

                {selectedJob.artifacts?.length > 0 && (
                  <div className="artifact-list">
                    <h3>Artifacts</h3>
                    <ul>
                      {selectedJob.artifacts.map((artifact) => (
                        <li key={artifact}>
                          <a href={`/api/artifact?path=${encodeURIComponent(artifact)}`} target="_blank" rel="noreferrer">
                            {artifact}
                          </a>
                        </li>
                      ))}
                    </ul>
                  </div>
                )}

                <pre className="log-viewer">
                  {selectedJobEvents.map((event) => (
                    <div key={event.event_id} className={event.type}>
                      [{event.type}] {event.message}
                    </div>
                  ))}
                </pre>
                {jobMessage && <p className="error">{jobMessage}</p>}
              </>
            )}
          </div>
        </section>
      )}

      {activeTab === "environment" && (
        <section className="panel env-panel">
          <div className="env-toolbar">
            <label className="check">
              <input
                type="checkbox"
                checked={revealSecrets}
                onChange={(event) => setRevealSecrets(event.target.checked)}
              />
              <span>Reveal secrets</span>
            </label>
            <button onClick={saveEnvChanges}>Save Env Changes</button>
            {envMessage && <span>{envMessage}</span>}
          </div>

          <div className="profile-manager">
            <div className="profile-list-panel">
              <div className="profile-list-head">
                <h3>Client Profiles</h3>
                <button type="button" onClick={beginCreateProfile}>
                  New Profile
                </button>
              </div>
              {profiles.length === 0 && <p>No profiles yet.</p>}
              <ul className="profile-list">
                {profiles.map((profile) => (
                  <li key={profile.id}>
                    <div className="profile-row">
                      <div>
                        <strong>{profile.name}</strong>
                        <small>
                          {profile.id}
                          {activeProfileId === profile.id ? " (active)" : ""}
                        </small>
                      </div>
                      <div className="profile-row-actions">
                        {activeProfileId !== profile.id && (
                          <button type="button" onClick={() => changeActiveProfile(profile.id)}>
                            Use
                          </button>
                        )}
                        <button type="button" onClick={() => beginEditProfile(profile.id)}>
                          Edit
                        </button>
                        <button type="button" onClick={() => deleteProfile(profile.id)}>
                          Delete
                        </button>
                      </div>
                    </div>
                  </li>
                ))}
              </ul>
            </div>

            <div className="profile-editor">
              <h3>{editingProfileId ? `Edit ${editingProfileId}` : "Create Profile"}</h3>
              <div className="field-grid">
                <label>
                  <span>Profile ID *</span>
                  <input
                    value={profileForm.profile_id}
                    disabled={Boolean(editingProfileId)}
                    placeholder="DEMO_ENV"
                    onChange={(event) => setProfileField("profile_id", event.target.value)}
                  />
                </label>
                <label>
                  <span>Friendly Name</span>
                  <input
                    value={profileForm.name}
                    placeholder="Demo Environment"
                    onChange={(event) => setProfileField("name", event.target.value)}
                  />
                </label>
                <label>
                  <span>Client ID</span>
                  <input
                    value={profileForm.client_id}
                    onChange={(event) => setProfileField("client_id", event.target.value)}
                  />
                </label>
                <label>
                  <span>Client Secret</span>
                  <input
                    type={revealSecrets ? "text" : "password"}
                    value={profileForm.client_secret}
                    onChange={(event) => setProfileField("client_secret", event.target.value)}
                  />
                </label>
                <label>
                  <span>Project ID</span>
                  <input
                    value={profileForm.project_id}
                    onChange={(event) => setProfileField("project_id", event.target.value)}
                  />
                </label>
              </div>
              <div className="profile-editor-actions">
                <button type="button" onClick={saveProfile}>
                  {editingProfileId ? "Update Profile" : "Create Profile"}
                </button>
                {editingProfileId && (
                  <button type="button" onClick={beginCreateProfile}>
                    Cancel Edit
                  </button>
                )}
              </div>
              {profileMessage && <p className="profile-message">{profileMessage}</p>}
              <p>
                Profile keys are stored in <code>.env</code> using
                <code>XCURES_PROFILE__&lt;ID&gt;__&lt;FIELD&gt;</code>.
                Global <code>BASE_URL</code> and <code>AUTH_URL</code> continue to come from standard env values.
              </p>
            </div>
          </div>

          <div className="api-settings">
            <h3>API Settings</h3>
            <div className="api-settings-grid">
              {API_SETTINGS.map((setting) => (
                <label key={setting.key}>
                  <span>{setting.label}</span>
                  <input
                    value={envDrafts[setting.key] ?? setting.defaultValue}
                    onChange={(event) =>
                      setEnvDrafts((previous) => ({
                        ...previous,
                        [setting.key]: event.target.value
                      }))
                    }
                  />
                </label>
              ))}
            </div>
          </div>

          <div className="env-grid">
            {generalEnvRows.map((row) => (
              <label key={row.key}>
                <span>
                  {row.key}
                  {row.secret ? " (secret)" : ""}
                </span>
                <input
                  value={envDrafts[row.key] ?? ""}
                  onChange={(event) =>
                    setEnvDrafts((previous) => ({
                      ...previous,
                      [row.key]: event.target.value
                    }))
                  }
                />
                <small>
                  {row.description ?? "Custom key"} | used by:{" "}
                  {row.used_by_scripts?.length ? row.used_by_scripts.join(", ") : "none"}
                </small>
              </label>
            ))}
          </div>
        </section>
      )}

      {activeTab === "admin" && (
        <section className="panel admin-panel">
          <div className="script-header">
            <h2>Admin</h2>
          </div>
          <p>Mark scripts as functional to show a green Functional indicator on the script page.</p>
          {adminMessage && <p>{adminMessage}</p>}
          <ul className="admin-script-list">
            {scripts.map((script) => (
              <li key={script.id} className="admin-script-row">
                <div>
                  <strong>
                    <ScriptNameWithIcon scriptId={script.id} name={script.name} />
                  </strong>
                  <small>{script.id}</small>
                </div>
                <label className="admin-functional-toggle">
                  <input
                    type="checkbox"
                    checked={Boolean(functionalByScript[script.id])}
                    onChange={(event) => {
                      void setFunctional(script.id, event.target.checked);
                    }}
                  />
                  <span>Functional</span>
                </label>
              </li>
            ))}
          </ul>
        </section>
      )}
    </div>
  );
}
