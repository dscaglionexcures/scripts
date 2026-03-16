#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

PYTHON_BIN="${ROOT_DIR}/.venv_sdk/bin/python"
if [[ ! -x "${PYTHON_BIN}" ]]; then
  PYTHON_BIN="$(command -v python3)"
fi

PORT="${APP_PORT:-8765}"
KEEP_RUNNING=1

on_shutdown() {
  KEEP_RUNNING=0
}

trap on_shutdown INT TERM

is_port_busy() {
  if command -v lsof >/dev/null 2>&1; then
    lsof -iTCP:"${PORT}" -sTCP:LISTEN >/dev/null 2>&1
    return $?
  fi
  return 1
}

if is_port_busy; then
  echo "Port ${PORT} is already in use. Stop the existing UI server first."
  exit 1
fi

echo "Starting xCures local UI on port ${PORT}..."
while [[ "${KEEP_RUNNING}" -eq 1 ]]; do
  "${PYTHON_BIN}" run_ui.py || EXIT_CODE=$?
  EXIT_CODE="${EXIT_CODE:-0}"

  if [[ "${KEEP_RUNNING}" -eq 0 ]]; then
    break
  fi

  if [[ "${EXIT_CODE}" -eq 0 ]]; then
    # Normal clean exit; do not loop forever.
    break
  fi

  if is_port_busy; then
    echo "UI server exited (${EXIT_CODE}), but port ${PORT} is in use. Not restarting."
    break
  fi

  echo "UI server exited with code ${EXIT_CODE}. Restarting in 1s..."
  sleep 1
  EXIT_CODE=0
done

echo "UI launcher stopped."
