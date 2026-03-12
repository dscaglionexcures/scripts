import { useEffect, useMemo, useState } from "react";
import logo from "./xcures.svg";

const TABS = [
  { id: "internal-scripts", label: "Internal API Operations" },
  { id: "public-scripts", label: "Public API Operations" },
  { id: "jobs", label: "jobs" },
  { id: "environment", label: "environment" }
];
const API_SETTINGS = [
  { key: "user_page_size", label: "User Page Size", defaultValue: "25" },
  { key: "request_timeout_seconds", label: "Request Timeout (in Seconds)", defaultValue: "60" },
  { key: "max_retries", label: "Max Retries", defaultValue: "2" },
  { key: "backoff_seconds", label: "Backoff (in Seconds)", defaultValue: "1.0" }
];

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
    bearer_token: "",
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

export default function App() {
  const [activeTab, setActiveTab] = useState("internal-scripts");
  const [scripts, setScripts] = useState([]);
  const [scriptSearch, setScriptSearch] = useState("");
  const [selectedScriptId, setSelectedScriptId] = useState("");
  const [draftByScript, setDraftByScript] = useState({});
  const [runError, setRunError] = useState("");
  const [runMessage, setRunMessage] = useState("");
  const [internalBearerToken, setInternalBearerToken] = useState("");

  const [jobs, setJobs] = useState([]);
  const [selectedJobId, setSelectedJobId] = useState("");
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

  const selectedScript = useMemo(
    () => scripts.find((script) => script.id === selectedScriptId) ?? null,
    [scripts, selectedScriptId]
  );
  const selectedScriptDraft = useMemo(() => {
    if (!selectedScript) return null;
    return draftByScript[selectedScript.id] ?? defaultDraftForScript(selectedScript);
  }, [draftByScript, selectedScript]);
  const selectedScriptIsInternalApi = useMemo(
    () => Boolean(selectedScript?.tags?.includes("internal-api")),
    [selectedScript]
  );

  const selectedJob = useMemo(
    () => jobs.find((job) => job.job_id === selectedJobId) ?? null,
    [jobs, selectedJobId]
  );

  const selectedJobEvents = useMemo(() => {
    if (!selectedJob) return [];
    return jobLogs[selectedJob.job_id] ?? selectedJob.log_events ?? [];
  }, [selectedJob, jobLogs]);
  const apiSettingKeys = useMemo(() => new Set(API_SETTINGS.map((item) => item.key)), []);
  const apiSettingRows = useMemo(
    () => envValues.filter((row) => apiSettingKeys.has(row.key)),
    [envValues, apiSettingKeys]
  );
  const generalEnvRows = useMemo(
    () => envValues.filter((row) => !apiSettingKeys.has(row.key)),
    [envValues, apiSettingKeys]
  );

  const filteredScripts = useMemo(() => {
    const query = scriptSearch.trim().toLowerCase();
    const scriptGroup = activeTab === "internal-scripts" ? "internal" : "public";
    const grouped = scripts.filter((script) => matchesScriptGroup(script, scriptGroup));
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
  }, []);

  useEffect(() => {
    loadEnv(revealSecrets);
  }, [revealSecrets]);

  useEffect(() => {
    const interval = window.setInterval(loadJobs, 3000);
    return () => window.clearInterval(interval);
  }, []);

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
  }, [selectedScriptId]);

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
        bearer_token: data.bearer_token ?? "",
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
      bearer_token: profileForm.bearer_token,
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
            bearer_token: data.profile.bearer_token ?? "",
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
          bearer_token: data.profile.bearer_token ?? "",
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
    try {
      const payload = {
        script_id: selectedScript.id,
        mode: selectedScript.supports_mode ? draft.mode : null,
        field_values: draft.fieldValues ?? {},
        raw_args: draft.rawArgs ?? "",
        internal_bearer_token: selectedScriptIsInternalApi ? internalBearerToken : null
      };
      const result = await apiJson("/api/jobs", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      setRunMessage(`Queued job ${result.job_id}`);
      setActiveTab("jobs");
      setSelectedJobId(result.job_id);
      if (selectedScriptIsInternalApi) {
        setInternalBearerToken("");
      }
      await loadJobs();
    } catch (error) {
      setRunError(String(error.message ?? error));
    }
  }

  async function cancelSelectedJob() {
    if (!selectedJob) return;
    try {
      await apiJson(`/api/jobs/${selectedJob.job_id}/cancel`, { method: "POST" });
      setJobMessage("Cancel requested.");
      await loadJobs();
    } catch (error) {
      setJobMessage(String(error.message ?? error));
    }
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
          <div>
            <h1>xCures Customer Success User Operations Toolbox</h1>
            <p></p>
          </div>
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

      {(activeTab === "internal-scripts" || activeTab === "public-scripts") && (
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
                    <strong>{script.name}</strong>
                    <small className={`api-tag ${apiTypeClass(script)}`}>{apiTypeLabel(script)}</small>
                    <small className={`safety-tag ${script.safety}`}>{safetyLabel(script.safety)}</small>
                  </button>
                </li>
              ))}
            </ul>
          </aside>

          <div className="script-detail">
            {!selectedScript && <p>Select a script to configure and run it.</p>}
            {selectedScript && (
              <>
                <div className="script-header">
                  <h2>{selectedScript.name}</h2>
                  <div className="script-pills">
                    <span className={`pill api ${apiTypeClass(selectedScript)}`}>{apiTypeLabel(selectedScript)}</span>
                    <span className={`pill ${selectedScript.safety}`}>{safetyLabel(selectedScript.safety)}</span>
                  </div>
                </div>
                <p>{selectedScript.description}</p>

                {selectedScriptIsInternalApi && (
                  <label>
                    <span>Bearer Token (required each run)</span>
                    <input
                      type="password"
                      value={internalBearerToken}
                      placeholder="Paste bearer token for this run"
                      onChange={(event) => setInternalBearerToken(event.target.value)}
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

                <div className="field-grid">
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

                <label className="raw-args">
                  <span>Raw args (optional)</span>
                  <textarea
                    value={selectedScriptDraft?.rawArgs ?? ""}
                    placeholder="e.g. --limit 10 --verbose"
                    onChange={(event) => setRawArgs(selectedScript.id, event.target.value)}
                  />
                </label>

                <div className="script-actions">
                  <button onClick={runSelectedScript}>Run Script</button>
                  {runMessage && <span className="ok">{runMessage}</span>}
                  {runError && <span className="error">{runError}</span>}
                </div>
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
                  <span>Bearer Token</span>
                  <input
                    type={revealSecrets ? "text" : "password"}
                    value={profileForm.bearer_token}
                    onChange={(event) => setProfileField("bearer_token", event.target.value)}
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
    </div>
  );
}
