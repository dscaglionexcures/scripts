from __future__ import annotations

import os
import re
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from .models import EnvCatalogEntry, EnvValueView, ScriptDefinition


ENV_ASSIGNMENT_RE = re.compile(
    r"^(?P<prefix>\s*)(?P<key>[A-Za-z_][A-Za-z0-9_]*)(?P<sep>\s*=\s*)(?P<value>.*?)(?P<newline>\r?\n?)$"
)
PROFILE_KEY_RE = re.compile(r"^XCURES_PROFILE__(?P<profile_id>[A-Z0-9_]+)__(?P<field>[A-Z0-9_]+)$")
PROFILE_ID_RE = re.compile(r"^[A-Z0-9_]+$")
ACTIVE_PROFILE_KEY = "ACTIVE_XCURES_PROFILE"
HIDDEN_ENV_UI_KEYS = {
    "BASE_URL",
    "AUTH_URL",
    "XCURES_BEARER_TOKEN",
    "XCURES_PROJECT_ID",
    "XCURES_CLIENT_ID",
    "DOCUMENT_DELAY_SECONDS",
    "XCURES_CLIENT_SECRET",
}
PROFILE_FIELD_TO_ENV = {
    "CLIENT_ID": "XCURES_CLIENT_ID",
    "CLIENT_SECRET": "XCURES_CLIENT_SECRET",
    "PROJECT_ID": "XCURES_PROJECT_ID",
    "BASE_URL": "BASE_URL",
    "AUTH_URL": "AUTH_URL",
}


def parse_env_value(raw_value: str) -> str:
    value = raw_value.strip()
    if len(value) >= 2 and (
        (value.startswith('"') and value.endswith('"'))
        or (value.startswith("'") and value.endswith("'"))
    ):
        value = value[1:-1]
    return value.strip()


def encode_env_value(value: str) -> str:
    if value == "":
        return '""'
    needs_quotes = any(ch.isspace() for ch in value) or "#" in value or "=" in value
    if needs_quotes:
        escaped = value.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
    return value


def is_secret_key(key: str) -> bool:
    upper = key.upper()
    markers = ("SECRET", "TOKEN", "PASSWORD", "PRIVATE_KEY", "API_KEY")
    return any(marker in upper for marker in markers)


class EnvStore:
    def __init__(self, env_path: Path):
        self.env_path = env_path
        self.env_path.parent.mkdir(parents=True, exist_ok=True)

    def _read_lines(self) -> List[str]:
        if not self.env_path.exists():
            return []
        return self.env_path.read_text(encoding="utf-8").splitlines(keepends=True)

    def load_values(self) -> Dict[str, str]:
        values: Dict[str, str] = {}
        for line in self._read_lines():
            match = ENV_ASSIGNMENT_RE.match(line)
            if not match:
                continue
            key = match.group("key")
            values[key] = parse_env_value(match.group("value"))
        return values

    def get_runtime_env(self) -> Dict[str, str]:
        values = self.load_values()
        profiles = self._extract_profiles(values)
        active_profile_id = values.get(ACTIVE_PROFILE_KEY, "").strip().upper()

        if active_profile_id and active_profile_id in profiles:
            fields = profiles[active_profile_id]
            for profile_field, env_key in PROFILE_FIELD_TO_ENV.items():
                profile_value = fields.get(profile_field, "").strip()
                if profile_value:
                    values[env_key] = profile_value

        # Keep BASE_URL/XCURES_BASE_URL in sync for mixed legacy scripts.
        base_url = values.get("BASE_URL", "").strip()
        legacy_base_url = values.get("XCURES_BASE_URL", "").strip()
        if base_url and not legacy_base_url:
            values["XCURES_BASE_URL"] = base_url
        elif legacy_base_url and not base_url:
            values["BASE_URL"] = legacy_base_url
        elif base_url and legacy_base_url and base_url != legacy_base_url:
            values["XCURES_BASE_URL"] = base_url
        return values

    def get_profiles(self) -> dict:
        values = self.load_values()
        profiles = self._extract_profiles(values)
        active_profile_id = values.get(ACTIVE_PROFILE_KEY, "").strip().upper()
        if active_profile_id and active_profile_id not in profiles:
            active_profile_id = ""

        items = []
        for profile_id in sorted(profiles.keys()):
            fields = profiles[profile_id]
            items.append(
                {
                    "id": profile_id,
                    "name": fields.get("NAME", "").strip() or profile_id.replace("_", " ").title(),
                    "has_client_id": bool(fields.get("CLIENT_ID", "").strip()),
                    "has_client_secret": bool(fields.get("CLIENT_SECRET", "").strip()),
                    "has_project_id": bool(fields.get("PROJECT_ID", "").strip()),
                    "has_base_url": bool(fields.get("BASE_URL", "").strip()),
                }
            )
        return {"active_profile_id": active_profile_id, "profiles": items}

    def set_active_profile(self, profile_id: str) -> dict:
        normalized = (profile_id or "").strip().upper()
        if normalized:
            profiles = self._extract_profiles(self.load_values())
            if normalized not in profiles:
                raise KeyError(normalized)
        self.update({ACTIVE_PROFILE_KEY: normalized}, clear_missing=False)
        return self.get_profiles()

    def get_profile_detail(self, profile_id: str) -> dict:
        normalized = self._normalize_profile_id(profile_id)
        profiles = self._extract_profiles(self.load_values())
        fields = profiles.get(normalized)
        if fields is None:
            raise KeyError(normalized)
        return self._to_profile_detail(normalized, fields)

    def create_profile(self, profile_id: str, data: Dict[str, Any]) -> dict:
        normalized = self._normalize_profile_id(profile_id)
        profiles = self._extract_profiles(self.load_values())
        if normalized in profiles:
            raise KeyError(normalized)

        updates, _remove_keys = self._profile_update_payload(
            normalized,
            data,
            defaults={"NAME": normalized.replace("_", " ").title()},
        )
        self.update(updates, clear_missing=False)
        return self.get_profile_detail(normalized)

    def update_profile(self, profile_id: str, data: Dict[str, Any]) -> dict:
        normalized = self._normalize_profile_id(profile_id)
        profiles = self._extract_profiles(self.load_values())
        if normalized not in profiles:
            raise KeyError(normalized)

        updates, remove_keys = self._profile_update_payload(normalized, data)
        self.update(updates, clear_missing=False, remove_keys=remove_keys)
        return self.get_profile_detail(normalized)

    def delete_profile(self, profile_id: str) -> dict:
        normalized = self._normalize_profile_id(profile_id)
        values = self.load_values()
        profiles = self._extract_profiles(values)
        if normalized not in profiles:
            raise KeyError(normalized)

        remove_keys = [
            key
            for key in values.keys()
            if key.startswith(f"XCURES_PROFILE__{normalized}__")
        ]
        if values.get(ACTIVE_PROFILE_KEY, "").strip().upper() == normalized:
            remove_keys.append(ACTIVE_PROFILE_KEY)
        self.update({}, clear_missing=False, remove_keys=remove_keys)
        return self.get_profiles()

    def update(
        self,
        updates: Dict[str, str],
        clear_missing: bool = False,
        remove_keys: Optional[Iterable[str]] = None,
    ) -> Dict[str, str]:
        lines = self._read_lines()
        pending = {k: str(v) for k, v in updates.items()}
        remove = set(remove_keys or [])
        output: List[str] = []
        seen = set()

        for line in lines:
            match = ENV_ASSIGNMENT_RE.match(line)
            if not match:
                output.append(line)
                continue

            key = match.group("key")
            if clear_missing and key not in pending:
                continue
            if key in remove and key not in pending:
                continue

            if key in pending:
                prefix = match.group("prefix")
                sep = match.group("sep")
                newline = match.group("newline") or "\n"
                output.append(f"{prefix}{key}{sep}{encode_env_value(pending[key])}{newline}")
                seen.add(key)
            else:
                output.append(line)

        missing = [key for key in pending if key not in seen]
        if missing and output and not output[-1].endswith("\n"):
            output[-1] += "\n"
        for key in missing:
            output.append(f"{key}={encode_env_value(pending[key])}\n")

        self._write_atomic("".join(output))
        return self.load_values()

    def _write_atomic(self, content: str) -> None:
        self.env_path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=str(self.env_path.parent),
            delete=False,
        ) as tmp:
            tmp.write(content)
            tmp_path = Path(tmp.name)
        os.replace(tmp_path, self.env_path)

    def as_view(
        self,
        *,
        reveal: bool,
        catalog: Iterable[EnvCatalogEntry],
        scripts: Iterable[ScriptDefinition],
    ) -> List[EnvValueView]:
        current = self.load_values()
        # Normalize legacy aliases so the UI presents only BASE_URL.
        if current.get("XCURES_BASE_URL", "").strip() and not current.get("BASE_URL", "").strip():
            current["BASE_URL"] = current["XCURES_BASE_URL"]

        catalog_map = {entry.key: entry for entry in catalog}
        keys = sorted(set(current.keys()) | set(catalog_map.keys()))
        if "XCURES_BASE_URL" in keys:
            keys.remove("XCURES_BASE_URL")
        if "XCURES_BEARER_TOKEN" in keys:
            keys.remove("XCURES_BEARER_TOKEN")
        keys = [key for key in keys if key not in HIDDEN_ENV_UI_KEYS]
        keys = [key for key in keys if not key.startswith("XCURES_PROFILE__")]
        if ACTIVE_PROFILE_KEY in keys:
            keys.remove(ACTIVE_PROFILE_KEY)
        used_by: Dict[str, List[str]] = {}
        for script in scripts:
            for key in script.required_env:
                used_by.setdefault(key, []).append(script.id)
            for keyset in script.env_sets_any:
                for key in keyset:
                    used_by.setdefault(key, []).append(script.id)
            for field in script.fields:
                if field.env_alias:
                    used_by.setdefault(field.env_alias, []).append(script.id)

        view: List[EnvValueView] = []
        for key in keys:
            value = current.get(key, "")
            cat = catalog_map.get(key)
            secret = cat.secret if cat is not None else is_secret_key(key)
            rendered = value
            if secret and value and not reveal:
                rendered = "*" * min(12, len(value))
            view.append(
                EnvValueView(
                    key=key,
                    value=rendered,
                    has_value=bool(value),
                    secret=secret,
                    from_catalog=key in catalog_map,
                    description=cat.description if cat else None,
                    used_by_scripts=sorted(set(used_by.get(key, []))),
                )
            )
        return view

    def validate_required_keys(self, keys: Iterable[str], extra_values: Optional[Dict[str, str]] = None) -> List[str]:
        available = self.get_runtime_env()
        if extra_values:
            for key, value in extra_values.items():
                if value is not None and str(value).strip():
                    available[key] = str(value).strip()
        missing: List[str] = []
        for key in keys:
            if not available.get(key, "").strip():
                missing.append(key)
        return missing

    def validate_any_keyset(self, keysets: Iterable[List[str]], extra_values: Optional[Dict[str, str]] = None) -> List[List[str]]:
        available = self.get_runtime_env()
        if extra_values:
            for key, value in extra_values.items():
                if value is not None and str(value).strip():
                    available[key] = str(value).strip()
        missing_sets: List[List[str]] = []
        for keyset in keysets:
            if all(available.get(key, "").strip() for key in keyset):
                return []
            missing_sets.append(keyset)
        return missing_sets

    def _extract_profiles(self, values: Dict[str, str]) -> Dict[str, Dict[str, str]]:
        profiles: Dict[str, Dict[str, str]] = {}
        for key, value in values.items():
            match = PROFILE_KEY_RE.match(key)
            if not match:
                continue
            profile_id = match.group("profile_id").upper()
            field = match.group("field").upper()
            profiles.setdefault(profile_id, {})[field] = value
        return profiles

    def _normalize_profile_id(self, profile_id: str) -> str:
        raw = (profile_id or "").strip().upper()
        normalized = re.sub(r"[^A-Z0-9]+", "_", raw)
        normalized = re.sub(r"_+", "_", normalized).strip("_")
        if not normalized or not PROFILE_ID_RE.match(normalized):
            raise ValueError("profile_id must include at least one letter or number")
        return normalized

    def _to_profile_detail(self, profile_id: str, fields: Dict[str, str]) -> dict:
        return {
            "id": profile_id,
            "name": fields.get("NAME", "").strip() or profile_id.replace("_", " ").title(),
            "client_id": fields.get("CLIENT_ID", "").strip(),
            "client_secret": fields.get("CLIENT_SECRET", "").strip(),
            "bearer_token": fields.get("BEARER_TOKEN", "").strip(),
            "project_id": fields.get("PROJECT_ID", "").strip(),
            "base_url": fields.get("BASE_URL", "").strip(),
            "auth_url": fields.get("AUTH_URL", "").strip(),
        }

    def _profile_update_payload(
        self,
        profile_id: str,
        data: Dict[str, Any],
        *,
        defaults: Optional[Dict[str, str]] = None,
    ) -> tuple[Dict[str, str], List[str]]:
        updates: Dict[str, str] = {}
        remove_keys: List[str] = []

        if defaults:
            for field, value in defaults.items():
                updates[f"XCURES_PROFILE__{profile_id}__{field}"] = value

        source_fields = {
            "NAME": data.get("name"),
            "CLIENT_ID": data.get("client_id"),
            "CLIENT_SECRET": data.get("client_secret"),
            "BEARER_TOKEN": data.get("bearer_token"),
            "PROJECT_ID": data.get("project_id"),
            "BASE_URL": data.get("base_url"),
            "AUTH_URL": data.get("auth_url"),
        }

        for field, raw in source_fields.items():
            if raw is None:
                continue
            env_key = f"XCURES_PROFILE__{profile_id}__{field}"
            value = str(raw).strip()
            if value:
                updates[env_key] = value
            else:
                remove_keys.append(env_key)

        return updates, remove_keys
