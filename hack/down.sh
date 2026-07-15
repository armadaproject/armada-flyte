#!/usr/bin/env bash
# Tear the devbox down: delete the kind cluster, and drop the Flyte client's upload cache.
#
# The cache is keyed by endpoint (localhost:30080), which is the same for every devbox, so without
# clearing it here the next `up.sh` would reuse cache entries that point at this cluster's now-deleted
# minio, and the first example submit would 404 on its code bundle.
set -euo pipefail
cd "$(dirname "$0")/.."
CLUSTER="${KIND_CLUSTER:-armada}"

kind delete cluster --name "$CLUSTER" 2>/dev/null || true
rm -rf "$HOME/.flyte/local-cache"
echo "Devbox down."
