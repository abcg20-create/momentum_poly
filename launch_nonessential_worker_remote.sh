#!/bin/bash
set -euo pipefail

ROOT="${1:-/home/ec2-user/polymarket-bot-nonessential}"
mkdir -p "$ROOT/logs" "$ROOT/cache_dublin"

if [[ -f "$ROOT/live_claim.env" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ROOT/live_claim.env"
  set +a
fi

pkill -f "tools_nonessential_readonly_worker.mjs" || true

resolve_local_upstream_origin() {
  if curl -fsS --max-time 5 http://127.0.0.1:8791/api/health?lite=1 >/dev/null; then
    echo "http://127.0.0.1:8791"
    return 0
  fi
  return 1
}

build_upstream_origin_map() {
  local entries=()
  if curl -fsS --max-time 5 http://127.0.0.1:8791/api/health?lite=1 >/dev/null; then
    entries+=("8791=http://127.0.0.1:8791")
  fi
  local joined=""
  local entry=""
  for entry in "${entries[@]}"; do
    if [[ -n "${joined}" ]]; then
      joined+=","
    fi
    joined+="${entry}"
  done
  printf '%s\n' "${joined}"
}

DUBLIN_UPSTREAM_ORIGIN="$(resolve_local_upstream_origin || true)"
if [[ -z "${DUBLIN_UPSTREAM_ORIGIN}" ]]; then
  echo "no live upstream available on 8791; production launch requires the live service and must not fall back to paper or tunnels" >&2
  exit 1
fi
UPSTREAM_ORIGIN_MAP_VALUE="$(build_upstream_origin_map)"

nohup env \
  NODE_OPTIONS=--max-old-space-size=4096 \
  PORT=9002 \
  HOST=0.0.0.0 \
  BASE_PATH=/live/worker \
  WORKER_LABEL=dublin-readonly \
  WORKER_SCOPE=live \
  LIVE_ONLY_SOURCE_HOST_PORT=8791 \
  UPSTREAM_ORIGIN="${DUBLIN_UPSTREAM_ORIGIN}" \
  UPSTREAM_ORIGIN_MAP="${UPSTREAM_ORIGIN_MAP_VALUE}" \
  CACHE_ROOT="$ROOT/cache_dublin" \
  PARITY_ENABLED=0 \
  RUN_AUDIT_ENABLED=1 \
  RUN_AUDIT_SCRIPT_PATH="$ROOT/render_live_run_audit_review.py" \
  RUN_AUDIT_REMOTE_ROOT=/home/ec2-user/polymarket-bot/src \
  RUN_AUDIT_HOST_PORT=8791 \
  RUN_AUDIT_PUBLIC_BASE=http://127.0.0.1:8791 \
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
  node "$ROOT/tools_nonessential_readonly_worker.mjs" \
  >"$ROOT/logs/worker_9002.log" 2>&1 &

sleep 2
curl -fsS http://127.0.0.1:9002/api/health
echo
