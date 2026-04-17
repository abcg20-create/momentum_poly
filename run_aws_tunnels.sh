#!/bin/bash
set -euo pipefail

KEY_PATH="${KEY_PATH:-${HOME}/.ssh/polymarket-aws.pem}"
AWS_USER="${AWS_USER:-ec2-user}"
AWS_HOST="${AWS_HOST:-52.15.101.141}"

LOCAL_18888="${LOCAL_18888:-18888}"
LOCAL_18889="${LOCAL_18889:-18889}"
LOCAL_18890="${LOCAL_18890:-18890}"
LOCAL_18891="${LOCAL_18891:-18891}"

REMOTE_8788="${REMOTE_8788:-8788}"
REMOTE_8789="${REMOTE_8789:-8789}"
REMOTE_8790="${REMOTE_8790:-8790}"
REMOTE_8791="${REMOTE_8791:-8791}"

exec ssh -i "${KEY_PATH}" \
  -N \
  -o BatchMode=yes \
  -o StrictHostKeyChecking=accept-new \
  -o ExitOnForwardFailure=yes \
  -o ServerAliveInterval=15 \
  -o ServerAliveCountMax=3 \
  -o TCPKeepAlive=yes \
  -L "${LOCAL_18888}:127.0.0.1:${REMOTE_8788}" \
  -L "${LOCAL_18889}:127.0.0.1:${REMOTE_8789}" \
  -L "${LOCAL_18890}:127.0.0.1:${REMOTE_8790}" \
  -L "${LOCAL_18891}:127.0.0.1:${REMOTE_8791}" \
  "${AWS_USER}@${AWS_HOST}"
