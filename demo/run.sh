#!/usr/bin/env bash
# Run a Python @env.task on Armada through a Flyte 2 backend, in one command (the default path).
# The task is registered with FlyteAdmin, runs as an Armada-scheduled pod, and shows up in the
# Flyte UI with its result.
#
# This script does the fiddly wiring for you:
#   - builds the task image and loads it into the Armada (kind) cluster,
#   - reads the backend's blob-store endpoint and credentials,
#   - starts the connector service pointed at that store,
#   - runs the example and prints the result.
#
# Run a different example by passing it as an argument (default: examples/hello.py):
#   ./demo/run.sh examples/ml_pipeline.py
#
# One-time prerequisites are in demo/README.md (an Armada cluster, and a Flyte backend whose
# executor routes `armada` tasks to the connector). Override defaults with env vars if needed:
#   KIND_CLUSTER (default armada-test), DEVBOX (default flyte-devbox), HOST_IP (auto-detected).
set -euo pipefail

cd "$(dirname "$0")/.."
PY=./.venv/bin/python
EXAMPLE="${1:-examples/hello.py}"
KIND_CLUSTER="${KIND_CLUSTER:-armada-test}"
DEVBOX="${DEVBOX:-flyte-devbox}"
# A non-latest tag so the driver pod (a normal backend pod) defaults to imagePullPolicy IfNotPresent
# and uses the locally loaded image instead of trying to pull it.
IMAGE=armada-flyte-task:v1
HOST_IP="${HOST_IP:-$(ipconfig getifaddr en0 2>/dev/null || hostname -I | awk '{print $1}')}"

echo "==> 1/3  Build the task image and load it into both clusters"
BUILD="$(mktemp -d)"
mkdir -p "$BUILD/pkg"
cp -R pyproject.toml src "$BUILD/pkg/"
cat > "$BUILD/Dockerfile" <<'EOF'
FROM python:3.11-slim
RUN pip install --no-cache-dir "flyte==2.5.1"
COPY pkg /pkg
RUN pip install --no-cache-dir /pkg
EOF
docker build -t "$IMAGE" "$BUILD" >/dev/null
rm -rf "$BUILD"
kind load docker-image "$IMAGE" --name "$KIND_CLUSTER" >/dev/null
# Import into the backend cluster too, so a multi-stage DAG's driver task can run there.
docker save "$IMAGE" | docker exec -i "$DEVBOX" ctr -n k8s.io images import - >/dev/null

echo "==> 2/3  Point the connector at the backend's blob store"
# Reuse a running connector. Restarting it while the backend holds a gRPC connection to the old
# process causes a transient "connector Unavailable" failure on the next task; to re-wire, stop it
# first (pkill -f 'bin/c0 --port 8000') and rerun. Force a restart with FORCE_CONNECTOR=1.
if [ "${FORCE_CONNECTOR:-0}" != "1" ] && lsof -nP -iTCP:8000 -sTCP:LISTEN >/dev/null 2>&1; then
  echo "    connector already running on :8000 (reusing; FORCE_CONNECTOR=1 to restart)"
else
  # The Armada pods (on the kind cluster) read/write the backend's RustFS bucket through its
  # host-published NodePort. The devbox ships RustFS with fixed sandbox defaults (creds
  # rustfs/rustfsstorage on NodePort 30002), so there is nothing to look up. Override these if your
  # backend uses a different store.
  blob="${FLYTE_BLOB_ENDPOINT:-http://$HOST_IP:30002}"
  key="${FLYTE_BLOB_ACCESS_KEY:-rustfs}"
  secret="${FLYTE_BLOB_SECRET_KEY:-rustfsstorage}"
  echo "    blob store: $blob"
  pkill -f "bin/c0 --port 8000" 2>/dev/null || true
  sleep 2
  ARMADA_URL="${ARMADA_URL:-localhost:50051}" BINOCULARS_URL="${BINOCULARS_URL:-localhost:50053}" \
    FLYTE_BLOB_ENDPOINT="$blob" FLYTE_BLOB_ACCESS_KEY="$key" FLYTE_BLOB_SECRET_KEY="$secret" \
    nohup ./.venv/bin/c0 --port 8000 --prometheus_port 9099 >/tmp/armada-flyte-c0.log 2>&1 &
  for _ in $(seq 1 15); do
    grep -aq "armada (0)" /tmp/armada-flyte-c0.log 2>/dev/null && break
    sleep 1
  done
  echo "    connector ready (log: /tmp/armada-flyte-c0.log)"
fi

# The devbox's baked connector endpoint is host.docker.internal, which resolves inside the devbox
# pods only on Docker Desktop. On Linux, point the connector at this host's LAN IP so the pods can
# reach c0. Idempotent: it patches and restarts the backend only when the endpoint is not already set.
if [ "$(uname -s)" = "Linux" ]; then
  KC="env KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl"
  if ! docker exec "$DEVBOX" sh -c "$KC get cm flyte-binary-config -n flyte -o yaml" 2>/dev/null | grep -q "endpoint: $HOST_IP:8000"; then
    echo "    linux: pointing the connector at $HOST_IP:8000 and restarting the backend"
    tmp="$(mktemp)"
    printf 'plugins:\n  connector-service:\n    defaultConnector:\n      endpoint: %s:8000\n      insecure: true\n    supportedTaskTypes:\n    - armada\n' "$HOST_IP" > "$tmp"
    python3 -c "import json,sys;print(json.dumps({'data':{'201-armada-endpoint.yaml':open(sys.argv[1]).read()}}))" "$tmp" > "$tmp.json"
    docker cp "$tmp.json" "$DEVBOX:/tmp/af-endpoint.json" >/dev/null
    docker exec "$DEVBOX" sh -c "$KC patch configmap flyte-binary-config -n flyte --type merge --patch-file /tmp/af-endpoint.json >/dev/null && $KC rollout restart deploy/flyte-binary -n flyte >/dev/null && $KC rollout status deploy/flyte-binary -n flyte --timeout=150s >/dev/null"
    rm -f "$tmp" "$tmp.json"
  fi
fi

echo "==> 3/3  Run $EXAMPLE through the backend"
exec "$PY" "$EXAMPLE"
