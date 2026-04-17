#!/bin/bash
set -euo pipefail

KEY_PATH="${KEY_PATH:-${HOME}/.ssh/ireland-ec2.pem}"
AWS_USER="${AWS_USER:-ec2-user}"
AWS_HOST="${AWS_HOST:-3.250.161.191}"

LOCAL_28888="${LOCAL_28888:-28888}"

REMOTE_8788="${REMOTE_8788:-8788}"

exec ssh -i "${KEY_PATH}" \
  -N \
  -o BatchMode=yes \
  -o StrictHostKeyChecking=accept-new \
  -o ConnectTimeout=10 \
  -o ExitOnForwardFailure=yes \
  -o ServerAliveInterval=15 \
  -o ServerAliveCountMax=3 \
  -o TCPKeepAlive=yes \
  -L "${LOCAL_28888}:127.0.0.1:${REMOTE_8788}" \
  "${AWS_USER}@${AWS_HOST}"
