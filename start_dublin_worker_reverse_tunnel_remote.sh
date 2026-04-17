#!/bin/bash
set -euo pipefail

cat >&2 <<'EOF'
start_dublin_worker_reverse_tunnel_remote.sh is disabled for production.

Production traffic must terminate on the AWS host and proxy directly to local
services on 127.0.0.1. Re-enabling reverse tunnels would reintroduce the exact
failure mode we are trying to remove.

Use direct local services instead:
- 8788 -> http://127.0.0.1:8788
- 8790 -> http://127.0.0.1:8790
- 8791 -> http://127.0.0.1:8791
EOF
exit 1
