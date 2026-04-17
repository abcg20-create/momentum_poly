#!/bin/zsh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
KEY_PATH="${KEY_PATH:-/Users/aliathar/Downloads/NonEssentialKepPair.pem}"
REMOTE_HOST="${REMOTE_HOST:-18.219.88.73}"
REMOTE_USER="${REMOTE_USER:-ec2-user}"
REMOTE_ROOT="${REMOTE_ROOT:-/home/ec2-user/polymarket-bot-nonessential}"
LIVE_CLAIM_ENV_TMP="/tmp/nonessential_live_claim.env"
MAIN_UPSTREAM_ORIGIN="${MAIN_UPSTREAM_ORIGIN:-}"
LIVE_UPSTREAM_ORIGIN="${LIVE_UPSTREAM_ORIGIN:-}"
LIVE_UPSTREAM_PATH_PREFIX="${LIVE_UPSTREAM_PATH_PREFIX:-}"
MAIN_RUN_AUDIT_PUBLIC_BASE="${MAIN_RUN_AUDIT_PUBLIC_BASE:-}"
LIVE_RUN_AUDIT_PUBLIC_BASE="${LIVE_RUN_AUDIT_PUBLIC_BASE:-}"

if [[ -f "$ROOT/.env" ]]; then
  set -a
  source "$ROOT/.env"
  set +a
fi
if [[ -f "$ROOT/../.env" ]]; then
  set -a
  source "$ROOT/../.env"
  set +a
fi

copy_file() {
  local src="$1"
  scp -i "$KEY_PATH" -o StrictHostKeyChecking=no "$src" "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_ROOT}/$(basename "$src")"
}

ssh -i "$KEY_PATH" -o StrictHostKeyChecking=no "${REMOTE_USER}@${REMOTE_HOST}" "mkdir -p '${REMOTE_ROOT}'"
copy_file "$ROOT/tools_main_readonly_worker.mjs"
copy_file "$ROOT/tools_live_readonly_worker.mjs"
copy_file "$ROOT/tools_nonessential_live_claim_worker.mjs"
copy_file "$ROOT/render_live_run_audit_review.py"
copy_file "$ROOT/launch_nonessential_workers_remote.sh"
copy_file "$ROOT/launch_nonessential_worker_remote.sh"
copy_file "$ROOT/restart_nonessential_workers_remote.sh"
copy_file "$ROOT/restart_worker_dublin_remote.sh"

cat > "$LIVE_CLAIM_ENV_TMP" <<EOF
LIVE_CLAIM_ENABLED=${LIVE_CLAIM_ENABLED:-1}
LIVE_CLAIM_TARGET_SEC=${LIVE_CLAIM_TARGET_SEC:-0}
LIVE_CLAIM_WINDOW_SEC=${LIVE_CLAIM_WINDOW_SEC:-60}
LIVE_CLAIM_POLL_MS=${LIVE_CLAIM_POLL_MS:-5000}
LIVE_CLAIM_MAX_PER_RUN=${LIVE_CLAIM_MAX_PER_RUN:-24}
LIVE_CLAIM_LOOKBACK_SLUGS=${LIVE_CLAIM_LOOKBACK_SLUGS:-5}
LIVE_CLAIM_PROFILE_ADDRESS=${LIVE_CLAIM_PROFILE_ADDRESS:-${POLY_FUNDER_ADDRESS:-${POLY_ADDRESS:-${PROFILE_ADDRESS:-}}}}
POLY_PRIVATE_KEY=${POLY_PRIVATE_KEY:-${PRIVATE_KEY:-}}
POLY_FUNDER_ADDRESS=${POLY_FUNDER_ADDRESS:-${PROFILE_ADDRESS:-}}
POLY_ADDRESS=${POLY_ADDRESS:-${PROFILE_ADDRESS:-}}
RPC_URLS=${RPC_URLS:-${RPC_URL:-}}
RELAYER_API_KEY=${RELAYER_API_KEY:-}
RELAYER_API_KEY_ADDRESS=${RELAYER_API_KEY_ADDRESS:-}
LIVE_CLAIM_RELAYER_BASE=${LIVE_CLAIM_RELAYER_BASE:-}
EOF
scp -i "$KEY_PATH" -o StrictHostKeyChecking=no "$LIVE_CLAIM_ENV_TMP" "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_ROOT}/live_claim.env"
rm -f "$LIVE_CLAIM_ENV_TMP"

ssh -i "$KEY_PATH" -o StrictHostKeyChecking=no "${REMOTE_USER}@${REMOTE_HOST}" "bash -lc '
  set -euo pipefail
  if ! command -v node >/dev/null 2>&1; then
    sudo dnf install -y nodejs20
  fi
  if ! command -v npm >/dev/null 2>&1; then
    sudo dnf install -y npm
  fi
  if [[ ! -d \"${REMOTE_ROOT}/node_modules/ethers\" || ! -d \"${REMOTE_ROOT}/node_modules/@polymarket/builder-relayer-client\" || ! -d \"${REMOTE_ROOT}/node_modules/@polymarket/clob-client\" ]]; then
    cd \"${REMOTE_ROOT}\"
    npm install --no-save ethers@5 @polymarket/builder-relayer-client@0.0.8 @polymarket/clob-client >/tmp/nonessential_npm_install.log 2>&1
  fi
  chmod +x \"${REMOTE_ROOT}/tools_main_readonly_worker.mjs\"
  chmod +x \"${REMOTE_ROOT}/tools_live_readonly_worker.mjs\"
  chmod +x \"${REMOTE_ROOT}/tools_nonessential_live_claim_worker.mjs\"
  chmod +x \"${REMOTE_ROOT}/launch_nonessential_workers_remote.sh\"
  chmod +x \"${REMOTE_ROOT}/launch_nonessential_worker_remote.sh\"
  chmod +x \"${REMOTE_ROOT}/restart_nonessential_workers_remote.sh\"
  chmod +x \"${REMOTE_ROOT}/restart_worker_dublin_remote.sh\"
  chmod 600 \"${REMOTE_ROOT}/live_claim.env\" || true
  export MAIN_UPSTREAM_ORIGIN=\"${MAIN_UPSTREAM_ORIGIN}\"
  export LIVE_UPSTREAM_ORIGIN=\"${LIVE_UPSTREAM_ORIGIN}\"
  export LIVE_UPSTREAM_PATH_PREFIX=\"${LIVE_UPSTREAM_PATH_PREFIX}\"
  export MAIN_RUN_AUDIT_PUBLIC_BASE=\"${MAIN_RUN_AUDIT_PUBLIC_BASE}\"
  export LIVE_RUN_AUDIT_PUBLIC_BASE=\"${LIVE_RUN_AUDIT_PUBLIC_BASE}\"
  bash \"${REMOTE_ROOT}/launch_nonessential_workers_remote.sh\" \"${REMOTE_ROOT}\"
'"
