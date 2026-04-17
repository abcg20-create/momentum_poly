#!/bin/bash
set -euo pipefail

ROOT="${1:-/home/ec2-user/polymarket-bot-nonessential}"

kill_listener_pid() {
  local port="$1"
  local pid=""
  pid="$(ss -ltnp 2>/dev/null | awk -v want=":${port}" '$4 ~ want { if (match($0, /pid=([0-9]+)/, m)) { print m[1]; exit } }')"
  if [[ -n "${pid}" ]]; then
    kill -9 "${pid}" 2>/dev/null || true
    sleep 1
  fi
}

kill_listener_pid 9001
kill_listener_pid 9002
pkill -f 'tools_main_readonly_worker.mjs' || true
pkill -f 'tools_live_readonly_worker.mjs' || true

exec "$ROOT/launch_nonessential_workers_remote.sh" "$ROOT"
