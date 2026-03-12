#!/usr/bin/env bash
set -euo pipefail

LABEL="com.xcures.script-runner"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LAUNCH_AGENTS_DIR="${HOME}/Library/LaunchAgents"
PLIST_PATH="${LAUNCH_AGENTS_DIR}/${LABEL}.plist"
LOG_DIR="${ROOT_DIR}/logs"
PYTHON_BIN="${ROOT_DIR}/.venv_sdk/bin/python"

if [[ ! -x "${PYTHON_BIN}" ]]; then
  PYTHON_BIN="$(command -v python3)"
fi

mkdir -p "${LAUNCH_AGENTS_DIR}" "${LOG_DIR}"

write_plist() {
  cat > "${PLIST_PATH}" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${LABEL}</string>
  <key>ProgramArguments</key>
  <array>
    <string>${PYTHON_BIN}</string>
    <string>${ROOT_DIR}/run_ui.py</string>
  </array>
  <key>WorkingDirectory</key>
  <string>${ROOT_DIR}</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PYTHONUNBUFFERED</key>
    <string>1</string>
    <key>PATH</key>
    <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
  </dict>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>StandardOutPath</key>
  <string>${LOG_DIR}/launchd.script_runner.out.log</string>
  <key>StandardErrorPath</key>
  <string>${LOG_DIR}/launchd.script_runner.err.log</string>
</dict>
</plist>
EOF
}

bootstrap_agent() {
  launchctl bootout "gui/$(id -u)" "${PLIST_PATH}" >/dev/null 2>&1 || true
  launchctl bootstrap "gui/$(id -u)" "${PLIST_PATH}"
  launchctl enable "gui/$(id -u)/${LABEL}" || true
  launchctl kickstart -k "gui/$(id -u)/${LABEL}"
}

case "${1:-}" in
  install)
    write_plist
    bootstrap_agent
    echo "Installed and started ${LABEL}"
    echo "URL: http://127.0.0.1:8765"
    ;;
  start)
    if [[ ! -f "${PLIST_PATH}" ]]; then
      write_plist
      launchctl bootstrap "gui/$(id -u)" "${PLIST_PATH}"
    fi
    launchctl kickstart -k "gui/$(id -u)/${LABEL}"
    echo "Started ${LABEL}"
    ;;
  stop)
    launchctl bootout "gui/$(id -u)" "${PLIST_PATH}" >/dev/null 2>&1 || true
    echo "Stopped ${LABEL}"
    ;;
  restart)
    launchctl kickstart -k "gui/$(id -u)/${LABEL}"
    echo "Restarted ${LABEL}"
    ;;
  status)
    launchctl print "gui/$(id -u)/${LABEL}"
    ;;
  uninstall)
    launchctl bootout "gui/$(id -u)" "${PLIST_PATH}" >/dev/null 2>&1 || true
    rm -f "${PLIST_PATH}"
    echo "Uninstalled ${LABEL}"
    ;;
  *)
    cat <<USAGE
Usage: $(basename "$0") <install|start|stop|restart|status|uninstall>
USAGE
    exit 1
    ;;
esac

