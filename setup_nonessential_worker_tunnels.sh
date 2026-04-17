#!/bin/zsh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
WORKER_HOST="${WORKER_HOST:-18.219.88.73}"
WORKER_USER="${WORKER_USER:-ec2-user}"
WORKER_KEY_LOCAL="${WORKER_KEY_LOCAL:-/Users/aliathar/Downloads/NonEssentialKepPair.pem}"
REMOTE_KEY_NAME="${REMOTE_KEY_NAME:-nonessential_worker.pem}"

copy_key() {
  local remote_host="$1"
  local remote_key="$2"
  scp -i "$remote_key" -o StrictHostKeyChecking=no "$WORKER_KEY_LOCAL" "ec2-user@${remote_host}:~/.ssh/${REMOTE_KEY_NAME}"
  ssh -i "$remote_key" -o StrictHostKeyChecking=no "ec2-user@${remote_host}" "chmod 400 ~/.ssh/${REMOTE_KEY_NAME}"
}

setup_tunnel() {
  local remote_host="$1"
  local remote_key="$2"
  local local_port="$3"
  local worker_port="$4"
  ssh -i "$remote_key" -o StrictHostKeyChecking=no "ec2-user@${remote_host}" \
    "bash -lc 'pkill -f \"${local_port}:127.0.0.1:${worker_port}\" || true; nohup ssh -fNT -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 -o ServerAliveCountMax=3 -o StrictHostKeyChecking=no -i ~/.ssh/${REMOTE_KEY_NAME} -L ${local_port}:127.0.0.1:${worker_port} ${WORKER_USER}@${WORKER_HOST} >/tmp/nonessential_tunnel_${local_port}.log 2>&1; sleep 1; curl -fsS http://127.0.0.1:${local_port}/api/health'"
}

copy_key "18.224.94.56" "/Users/aliathar/.ssh/polymarket-aws.pem"
copy_key "3.250.161.191" "/Users/aliathar/.ssh/ireland-ec2.pem"
setup_tunnel "18.224.94.56" "/Users/aliathar/.ssh/polymarket-aws.pem" "29001" "9001"
setup_tunnel "3.250.161.191" "/Users/aliathar/.ssh/ireland-ec2.pem" "29002" "9002"
