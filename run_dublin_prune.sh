#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
KEY_PATH="${KEY_PATH:-${HOME}/.ssh/ireland-ec2.pem}"
AWS_USER="${AWS_USER:-ec2-user}"
AWS_HOST="${AWS_HOST:-3.250.161.191}"
REMOTE_SCRIPT_PATH="${REMOTE_SCRIPT_PATH:-/home/ec2-user/polymarket-bot/src/run_remote_server_prune.sh}"
SSH_OPTS=(
  -i "${KEY_PATH}"
  -o BatchMode=yes
  -o StrictHostKeyChecking=accept-new
  -o ConnectTimeout=10
  -o ServerAliveInterval=15
  -o ServerAliveCountMax=3
)

log() {
  printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

REMOTE_CMD="cd /home/ec2-user/polymarket-bot/src && /bin/bash ${REMOTE_SCRIPT_PATH} dublin"
SSH_OUT="$(ssh "${SSH_OPTS[@]}" "${AWS_USER}@${AWS_HOST}" "${REMOTE_CMD}")"
log "remote_prune ${SSH_OUT}"
