from __future__ import annotations

from pathlib import Path

from script_runner.env_store import EnvStore


def test_env_round_trip_preserves_comments_and_updates_values(tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text(
        "# header comment\n"
        "XCURES_CLIENT_ID=abc123\n"
        "UNTOUCHED=value\n"
        "XCURES_CLIENT_SECRET=\"old secret\"\n",
        encoding="utf-8",
    )

    store = EnvStore(env_path)
    values = store.load_values()
    assert values["XCURES_CLIENT_ID"] == "abc123"
    assert values["XCURES_CLIENT_SECRET"] == "old secret"

    updated = store.update(
        {
            "XCURES_CLIENT_ID": "new-id",
            "XCURES_CLIENT_SECRET": "new secret",
            "NEW_KEY": "new-value",
        }
    )
    assert updated["XCURES_CLIENT_ID"] == "new-id"
    assert updated["XCURES_CLIENT_SECRET"] == "new secret"
    assert updated["NEW_KEY"] == "new-value"

    text = env_path.read_text(encoding="utf-8")
    assert "# header comment" in text
    assert "UNTOUCHED=value" in text
    assert "XCURES_CLIENT_ID=new-id" in text
    assert "XCURES_CLIENT_SECRET=\"new secret\"" in text
    assert "NEW_KEY=new-value" in text


def test_profile_runtime_overlay_and_switch(tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text(
        "BASE_URL=https://partner.xcures.com\n"
        "XCURES_PROFILE__DEMO__NAME=Demo Environment\n"
        "XCURES_PROFILE__DEMO__CLIENT_ID=demo-client\n"
        "XCURES_PROFILE__DEMO__CLIENT_SECRET=demo-secret\n"
        "XCURES_PROFILE__DEMO__BASE_URL=https://demo.xcures.local\n"
        "ACTIVE_XCURES_PROFILE=DEMO\n",
        encoding="utf-8",
    )

    store = EnvStore(env_path)
    profiles = store.get_profiles()
    assert profiles["active_profile_id"] == "DEMO"
    assert profiles["profiles"][0]["name"] == "Demo Environment"

    runtime = store.get_runtime_env()
    assert runtime["XCURES_CLIENT_ID"] == "demo-client"
    assert runtime["XCURES_CLIENT_SECRET"] == "demo-secret"
    assert runtime["BASE_URL"] == "https://demo.xcures.local"
    assert runtime["XCURES_BASE_URL"] == "https://demo.xcures.local"

    store.set_active_profile("")
    runtime_default = store.get_runtime_env()
    assert runtime_default["BASE_URL"] == "https://partner.xcures.com"


def test_profile_crud_lifecycle(tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text("BASE_URL=https://partner.xcures.com\n", encoding="utf-8")
    store = EnvStore(env_path)

    created = store.create_profile(
        "qa_env",
        {
            "name": "QA Environment",
            "client_id": "qa-client-id",
            "client_secret": "qa-client-secret",
            "project_id": "qa-project",
        },
    )
    assert created["id"] == "QA_ENV"
    assert created["name"] == "QA Environment"
    assert created["client_id"] == "qa-client-id"
    assert created["client_secret"] == "qa-client-secret"

    updated = store.update_profile(
        "QA_ENV",
        {
            "name": "QA Env Updated",
            "client_secret": "",
            "base_url": "https://qa.xcures.local",
        },
    )
    assert updated["name"] == "QA Env Updated"
    assert updated["client_secret"] == ""
    assert updated["base_url"] == "https://qa.xcures.local"

    store.set_active_profile("QA_ENV")
    runtime = store.get_runtime_env()
    assert runtime["XCURES_CLIENT_ID"] == "qa-client-id"
    assert runtime["BASE_URL"] == "https://qa.xcures.local"

    deleted = store.delete_profile("QA_ENV")
    assert deleted["profiles"] == []
    assert deleted["active_profile_id"] == ""

    text = env_path.read_text(encoding="utf-8")
    assert "XCURES_PROFILE__QA_ENV__CLIENT_ID" not in text
    assert "ACTIVE_XCURES_PROFILE" not in text
