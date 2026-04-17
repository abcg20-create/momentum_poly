#!/bin/bash
set -euo pipefail

chmod 400 /home/ec2-user/.ssh/nonessential_worker.pem
pkill -f '29002:127.0.0.1:9002' || true
ssh -fNT \
  -o ExitOnForwardFailure=yes \
  -o ServerAliveInterval=30 \
  -o ServerAliveCountMax=3 \
  -o StrictHostKeyChecking=no \
  -i /home/ec2-user/.ssh/nonessential_worker.pem \
  -L 29002:127.0.0.1:9002 \
  ec2-user@18.219.88.73
sleep 2
curl -fsS http://127.0.0.1:29002/api/health
echo
