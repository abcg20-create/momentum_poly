#!/bin/bash
set -euo pipefail

KEY_PATH="${KEY_PATH:-/home/ec2-user/.ssh/nonessential_worker.pem}"
WORKER_HOST="${WORKER_HOST:-18.219.88.73}"
WORKER_USER="${WORKER_USER:-ec2-user}"
MAIN_LOCAL_PORT="${MAIN_LOCAL_PORT:-9001}"
LIVE_LOCAL_PORT="${LIVE_LOCAL_PORT:-9002}"
SKIP_WORKER_HEALTH="${SKIP_WORKER_HEALTH:-0}"

kill_listener_pid() {
  local port="$1"
  local pid=""
  pid="$(ss -ltnp 2>/dev/null | awk -v want=":${port}" '$4 ~ want { if (match($0, /pid=([0-9]+)/, m)) { print m[1]; exit } }')"
  if [[ -n "${pid}" ]]; then
    kill -9 "${pid}" 2>/dev/null || true
    sleep 1
  fi
}

chmod 400 "${KEY_PATH}"
kill_listener_pid "${MAIN_LOCAL_PORT}"
kill_listener_pid "${LIVE_LOCAL_PORT}"
pkill -f '127.0.0.1:9001' || true
pkill -f '127.0.0.1:9002' || true

nohup ssh -NT \
  -o ExitOnForwardFailure=yes \
  -o ServerAliveInterval=30 \
  -o ServerAliveCountMax=3 \
  -o StrictHostKeyChecking=no \
  -i "${KEY_PATH}" \
  -L "${MAIN_LOCAL_PORT}:127.0.0.1:9001" \
  "${WORKER_USER}@${WORKER_HOST}" \
  >/tmp/nonessential_tunnel_home.log 2>&1 </dev/null &

nohup ssh -NT \
  -o ExitOnForwardFailure=yes \
  -o ServerAliveInterval=30 \
  -o ServerAliveCountMax=3 \
  -o StrictHostKeyChecking=no \
  -i "${KEY_PATH}" \
  -L "${LIVE_LOCAL_PORT}:127.0.0.1:9002" \
  "${WORKER_USER}@${WORKER_HOST}" \
  >/tmp/nonessential_tunnel_live.log 2>&1 </dev/null &

sleep 2
if [[ "${SKIP_WORKER_HEALTH}" != "1" ]]; then
  curl -fsS "http://127.0.0.1:${MAIN_LOCAL_PORT}/api/health"
  echo
  curl -fsS "http://127.0.0.1:${LIVE_LOCAL_PORT}/api/health"
  echo
fi
