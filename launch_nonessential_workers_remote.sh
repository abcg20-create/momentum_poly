#!/bin/bash
set -euo pipefail

ROOT="${1:-/home/ec2-user/polymarket-bot-nonessential}"
mkdir -p "$ROOT/logs" "$ROOT/cache_main" "$ROOT/cache_live"

if [[ -f "$ROOT/live_claim.env" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ROOT/live_claim.env"
  set +a
fi

MAIN_UPSTREAM_ORIGIN="${MAIN_UPSTREAM_ORIGIN:-http://127.0.0.1:8788}"
LIVE_UPSTREAM_ORIGIN="${LIVE_UPSTREAM_ORIGIN:-http://127.0.0.1:8791}"
LIVE_UPSTREAM_PATH_PREFIX="${LIVE_UPSTREAM_PATH_PREFIX:-}"
MAIN_RUN_AUDIT_PUBLIC_BASE="${MAIN_RUN_AUDIT_PUBLIC_BASE:-${MAIN_UPSTREAM_ORIGIN}}"
LIVE_RUN_AUDIT_PUBLIC_BASE="${LIVE_RUN_AUDIT_PUBLIC_BASE:-${LIVE_UPSTREAM_ORIGIN}}"

normalize_path_prefix() {
  local raw="${1:-}"
  raw="${raw#/}"
  raw="${raw%/}"
  if [[ -z "${raw}" ]]; then
    printf '%s' ""
  else
    printf '/%s' "${raw}"
  fi
}

build_health_url() {
  local origin="$1"
  local path_prefix="${2:-}"
  local normalized_prefix=""
  normalized_prefix="$(normalize_path_prefix "${path_prefix}")"
  printf '%s%s/api/health?lite=1' "${origin%/}" "${normalized_prefix}"
}

build_state_url() {
  local origin="$1"
  local path_prefix="${2:-}"
  local normalized_prefix=""
  normalized_prefix="$(normalize_path_prefix "${path_prefix}")"
  printf '%s%s/api/state?includeBots=0&lite=1' "${origin%/}" "${normalized_prefix}"
}

require_upstream() {
  local origin="$1"
  local label="$2"
  local path_prefix="${3:-}"
  local i=0
  while (( i < 6 )); do
    if curl -fsS --max-time 10 "$(build_health_url "${origin}" "${path_prefix}")" >/dev/null; then
      return 0
    fi
    if curl -fsS --max-time 10 "$(build_state_url "${origin}" "${path_prefix}")" >/dev/null; then
      return 0
    fi
    sleep 2
    i=$((i + 1))
  done
  echo "missing upstream origin for ${label}: $(build_health_url "${origin}" "${path_prefix}") or $(build_state_url "${origin}" "${path_prefix}")" >&2
  exit 1
}

kill_listener_pid() {
  local port="$1"
  local pid=""
  pid="$(ss -ltnp 2>/dev/null | awk -v want=":${port}" '$4 ~ want { if (match($0, /pid=([0-9]+)/, m)) { print m[1]; exit } }')"
  if [[ -n "${pid}" ]]; then
    kill -9 "${pid}" 2>/dev/null || true
    sleep 1
  fi
}

start_main_worker() {
  nohup env \
    NODE_OPTIONS=--max-old-space-size=4096 \
    PORT=9001 \
    HOST=0.0.0.0 \
    WORKER_LABEL=main-readonly \
    WORKER_SCOPE=main \
    UPSTREAM_ORIGIN="${MAIN_UPSTREAM_ORIGIN}" \
    UPSTREAM_ORIGIN_MAP="8788=${MAIN_UPSTREAM_ORIGIN},8790=${MAIN_UPSTREAM_ORIGIN}" \
    CACHE_ROOT="$ROOT/cache_main" \
    PARITY_ENABLED=0 \
    RUN_AUDIT_ENABLED=1 \
    RUN_AUDIT_SCRIPT_PATH="$ROOT/render_live_run_audit_review.py" \
    RUN_AUDIT_HOST_PORT=8788 \
    RUN_AUDIT_PUBLIC_BASE="${MAIN_RUN_AUDIT_PUBLIC_BASE}" \
    RUN_AUDIT_MAX_AGE_MS=86400000 \
    RUN_AUDIT_SYNC_BUILD_TIMEOUT_MS=300000 \
    SESSION_CARD_REMOTE_CANONICAL_READ_ENABLED=1 \
    SESSION_CARD_REMOTE_CANONICAL_WRITE_ENABLED=1 \
    LIVE_CLAIM_ENABLED=0 \
    node "$ROOT/tools_main_readonly_worker.mjs" \
    >"$ROOT/logs/main_worker.log" 2>&1 &
}

start_live_worker() {
  nohup env \
    NODE_OPTIONS=--max-old-space-size=4096 \
    PORT=9002 \
    HOST=0.0.0.0 \
    WORKER_LABEL=live-readonly \
    WORKER_SCOPE=live \
    LIVE_ONLY_SOURCE_HOST_PORT=8791 \
    UPSTREAM_ORIGIN="${LIVE_UPSTREAM_ORIGIN}" \
    UPSTREAM_PATH_PREFIX="${LIVE_UPSTREAM_PATH_PREFIX}" \
    UPSTREAM_ORIGIN_MAP="8791=${LIVE_UPSTREAM_ORIGIN}" \
    CACHE_ROOT="$ROOT/cache_live" \
    PARITY_ENABLED=0 \
    RUN_AUDIT_ENABLED=1 \
    RUN_AUDIT_SCRIPT_PATH="$ROOT/render_live_run_audit_review.py" \
    RUN_AUDIT_HOST_PORT=8791 \
    RUN_AUDIT_PUBLIC_BASE="${LIVE_RUN_AUDIT_PUBLIC_BASE}" \
    RUN_AUDIT_MAX_AGE_MS=86400000 \
    RUN_AUDIT_SYNC_BUILD_TIMEOUT_MS=300000 \
    SESSION_CARD_REMOTE_CANONICAL_READ_ENABLED=1 \
    SESSION_CARD_REMOTE_CANONICAL_WRITE_ENABLED=1 \
    LIVE_CLAIM_ENABLED="${LIVE_CLAIM_ENABLED:-0}" \
    LIVE_CLAIM_TARGET_SEC="${LIVE_CLAIM_TARGET_SEC:-0}" \
    LIVE_CLAIM_WINDOW_SEC="${LIVE_CLAIM_WINDOW_SEC:-60}" \
    LIVE_CLAIM_POLL_MS="${LIVE_CLAIM_POLL_MS:-5000}" \
    LIVE_CLAIM_MAX_PER_RUN="${LIVE_CLAIM_MAX_PER_RUN:-24}" \
    LIVE_CLAIM_LOOKBACK_SLUGS="${LIVE_CLAIM_LOOKBACK_SLUGS:-5}" \
    LIVE_CLAIM_PROFILE_ADDRESS="${LIVE_CLAIM_PROFILE_ADDRESS:-${POLY_FUNDER_ADDRESS:-${POLY_ADDRESS:-}}}" \
    POLY_PRIVATE_KEY="${POLY_PRIVATE_KEY:-}" \
    POLY_FUNDER_ADDRESS="${POLY_FUNDER_ADDRESS:-}" \
    POLY_ADDRESS="${POLY_ADDRESS:-}" \
    RELAYER_API_KEY="${RELAYER_API_KEY:-}" \
    RELAYER_API_KEY_ADDRESS="${RELAYER_API_KEY_ADDRESS:-}" \
    LIVE_CLAIM_RELAYER_BASE="${LIVE_CLAIM_RELAYER_BASE:-}" \
    RPC_URLS="${RPC_URLS:-${RPC_URL:-}}" \
    node "$ROOT/tools_live_readonly_worker.mjs" \
    >"$ROOT/logs/live_worker.log" 2>&1 &
}

require_upstream "${MAIN_UPSTREAM_ORIGIN}" main
require_upstream "${LIVE_UPSTREAM_ORIGIN}" live "${LIVE_UPSTREAM_PATH_PREFIX}"

kill_listener_pid 9001
kill_listener_pid 9002
pkill -f 'tools_main_readonly_worker.mjs' || true
pkill -f 'tools_live_readonly_worker.mjs' || true

start_main_worker
start_live_worker

sleep 2
curl -fsS http://127.0.0.1:9001/api/health
echo
curl -fsS http://127.0.0.1:9002/api/health
echo
