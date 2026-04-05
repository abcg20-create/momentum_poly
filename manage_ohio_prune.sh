#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
RUNNER_PATH="${ROOT_DIR}/run_ohio_prune.sh"
LABEL="${LAUNCHD_LABEL:-com.polymarket.ohio-prune}"
PLIST_DIR="${HOME}/Library/LaunchAgents"
PLIST_PATH="${PLIST_DIR}/${LABEL}.plist"
OUT_LOG="${OUT_LOG:-/tmp/polymarket_ohio_prune.out.log}"
ERR_LOG="${ERR_LOG:-/tmp/polymarket_ohio_prune.err.log}"
START_INTERVAL="${START_INTERVAL:-3600}"

ensure_plist() {
  mkdir -p "${PLIST_DIR}"
  cat > "${PLIST_PATH}" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${LABEL}</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string>
    <string>${RUNNER_PATH}</string>
  </array>
  <key>WorkingDirectory</key>
  <string>${ROOT_DIR}</string>
  <key>RunAtLoad</key>
  <true/>
  <key>StartInterval</key>
  <integer>${START_INTERVAL}</integer>
  <key>StandardOutPath</key>
  <string>${OUT_LOG}</string>
  <key>StandardErrorPath</key>
  <string>${ERR_LOG}</string>
</dict>
</plist>
EOF
  chmod 644 "${PLIST_PATH}"
}

do_install() {
  ensure_plist
  launchctl bootout "gui/$(id -u)/${LABEL}" >/dev/null 2>&1 || true
  launchctl bootstrap "gui/$(id -u)" "${PLIST_PATH}"
  launchctl enable "gui/$(id -u)/${LABEL}" >/dev/null 2>&1 || true
  launchctl kickstart -k "gui/$(id -u)/${LABEL}" >/dev/null 2>&1 || true
  echo "installed label=${LABEL} plist=${PLIST_PATH} interval=${START_INTERVAL}s"
}

do_uninstall() {
  launchctl bootout "gui/$(id -u)/${LABEL}" >/dev/null 2>&1 || true
  rm -f "${PLIST_PATH}"
  echo "uninstalled label=${LABEL}"
}

do_start() {
  launchctl kickstart -k "gui/$(id -u)/${LABEL}" >/dev/null 2>&1 || launchctl start "${LABEL}" >/dev/null 2>&1 || true
  echo "started label=${LABEL}"
}

do_stop() {
  launchctl bootout "gui/$(id -u)/${LABEL}" >/dev/null 2>&1 || true
  echo "stopped label=${LABEL}"
}

do_status() {
  if launchctl print "gui/$(id -u)/${LABEL}" >/dev/null 2>&1; then
    echo "loaded label=${LABEL} plist=${PLIST_PATH}"
    launchctl print "gui/$(id -u)/${LABEL}" 2>/dev/null | awk '
      /state =/ || /pid =/ || /last exit code =/ { print }
    '
    return 0
  fi
  echo "not_loaded label=${LABEL}"
  exit 1
}

do_logs() {
  echo "--- out: ${OUT_LOG} ---"
  tail -n 80 "${OUT_LOG}" 2>/dev/null || true
  echo "--- err: ${ERR_LOG} ---"
  tail -n 80 "${ERR_LOG}" 2>/dev/null || true
}

cmd="${1:-status}"
case "${cmd}" in
  install) do_install ;;
  uninstall) do_uninstall ;;
  start) do_start ;;
  stop) do_stop ;;
  restart) do_stop; do_install ;;
  status) do_status ;;
  logs) do_logs ;;
  *)
    echo "Usage: $0 {install|uninstall|start|stop|restart|status|logs}" >&2
    exit 1
    ;;
esac
