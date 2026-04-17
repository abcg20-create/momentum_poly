#!/bin/bash
set -euo pipefail

KEY_PATH="${KEY_PATH:-/home/ec2-user/.ssh/nonessential_worker.pem}"
WORKER_HOST="${WORKER_HOST:-18.219.88.73}"
WORKER_USER="${WORKER_USER:-ec2-user}"
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
kill_listener_pid 28890
kill_listener_pid 9002
pkill -f '127.0.0.1:9001' || true
pkill -f '127.0.0.1:9002' || true
pkill -f '127.0.0.1:8788' || true
pkill -f '127.0.0.1:8791' || true

nohup ssh -NT \
  -o ExitOnForwardFailure=yes \
  -o ServerAliveInterval=30 \
  -o ServerAliveCountMax=3 \
  -o StrictHostKeyChecking=no \
  -i "${KEY_PATH}" \
  -L 28890:127.0.0.1:9001 \
  -R 18788:127.0.0.1:8788 \
  "${WORKER_USER}@${WORKER_HOST}" \
  >/tmp/nonessential_tunnel_home.log 2>&1 </dev/null &

nohup ssh -NT \
  -o ExitOnForwardFailure=yes \
  -o ServerAliveInterval=30 \
  -o ServerAliveCountMax=3 \
  -o StrictHostKeyChecking=no \
  -i "${KEY_PATH}" \
  -L 9002:127.0.0.1:9002 \
  -R 18791:127.0.0.1:8791 \
  "${WORKER_USER}@${WORKER_HOST}" \
  >/tmp/nonessential_tunnel_live.log 2>&1 </dev/null &

sleep 2
if [[ "${SKIP_WORKER_HEALTH}" != "1" ]]; then
  curl -fsS http://127.0.0.1:28890/api/health
  echo
  curl -fsS http://127.0.0.1:9002/api/health
  echo
fi
