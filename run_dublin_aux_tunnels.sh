#!/bin/bash
set -euo pipefail

KEY_PATH="${KEY_PATH:-${HOME}/.ssh/ireland-ec2.pem}"
AWS_USER="${AWS_USER:-ec2-user}"
AWS_HOST="${AWS_HOST:-3.250.161.191}"

LOCAL_28889="${LOCAL_28889:-28889}"
LOCAL_28890="${LOCAL_28890:-28890}"
LOCAL_28891="${LOCAL_28891:-28891}"
LOCAL_28892="${LOCAL_28892:-28892}"

REMOTE_8789="${REMOTE_8789:-8789}"
# Dublin's readonly worker listens on 9002 on the current live host.
REMOTE_8790="${REMOTE_8790:-9002}"
REMOTE_8791="${REMOTE_8791:-8791}"
REMOTE_COMPARE_8790="${REMOTE_COMPARE_8790:-8790}"

exec ssh -i "${KEY_PATH}" \
  -N \
  -o BatchMode=yes \
  -o StrictHostKeyChecking=accept-new \
  -o ConnectTimeout=10 \
  -o ExitOnForwardFailure=yes \
  -o ServerAliveInterval=15 \
  -o ServerAliveCountMax=3 \
  -o TCPKeepAlive=yes \
  -L "${LOCAL_28889}:127.0.0.1:${REMOTE_8789}" \
  -L "${LOCAL_28890}:127.0.0.1:${REMOTE_8790}" \
  -L "${LOCAL_28891}:127.0.0.1:${REMOTE_8791}" \
  -L "${LOCAL_28892}:127.0.0.1:${REMOTE_COMPARE_8790}" \
  "${AWS_USER}@${AWS_HOST}"
