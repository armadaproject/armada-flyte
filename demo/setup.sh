#!/usr/bin/env bash
# Stand up a Flyte 2 backend in the same Kind cluster Armada already uses, and wire the connector.
#
# What it does:
#   1. deploys minio (blob store) + postgres (Flyte metadata) into the kind cluster,
#   2. installs the flyte-binary (v2) chart, pointed at that minio/postgres and at the host connector,
#   3. builds the task image from this checkout and loads it into the cluster,
#   4. starts the connector (c0) on the host, pointed at Armada and the in-cluster minio,
#   5. makes the Flyte API and blob store reachable from the host (port-forward, or a kind port map).
#
# Prerequisites (see README.md):
#   - Armada up with a real executor against the kind cluster $KIND_CLUSTER, and queue "flyte" created
#     (in the armada repo: `go run github.com/magefile/mage@v1.17.2 dev:full`, then create queue "flyte")
#   - this repo's venv built (`python3.11 -m venv .venv && ./.venv/bin/pip install -e .`)
#   - a checkout of the flyte fork with the flyte-binary chart (set $FLYTE_CHART)
#   - docker, helm, kubectl, kind on PATH
set -euo pipefail
cd "$(dirname "$0")"
ROOT="$(cd .. && pwd)"

KIND_CLUSTER="${KIND_CLUSTER:-armada-test}"
FLYTE_IMAGE="${FLYTE_IMAGE:-dpejcev/flyte-binary-v2:armada}"
TASK_IMAGE="${TASK_IMAGE:-armada-flyte-task:v1}"
FLYTE_CHART="${FLYTE_CHART:-$ROOT/../flyte/charts/flyte-binary}"
HOST_IP="${HOST_IP:-$(ipconfig getifaddr en0 2>/dev/null || hostname -I | awk '{print $1}')}"
C0="$ROOT/.venv/bin/c0"

# Make a host port reach an in-cluster service. If something already serves it (a kind port mapping
# or an earlier forward), reuse that. Otherwise start a background kubectl port-forward.
ensure_host_port() {
  local port=$1 svc=$2 target=$3
  if nc -z localhost "$port" 2>/dev/null; then
    echo "    localhost:$port already reachable (reusing)"
    return
  fi
  nohup kubectl -n flyte port-forward "svc/$svc" "$port:$target" >"/tmp/af-pf-$port.log" 2>&1 &
  for _ in $(seq 1 15); do
    nc -z localhost "$port" 2>/dev/null && break
    sleep 1
  done
  echo "    port-forward localhost:$port -> $svc:$target"
}

if [ ! -d "$FLYTE_CHART" ]; then
  echo "flyte-binary chart not found at $FLYTE_CHART" >&2
  echo "Clone the flyte fork and set FLYTE_CHART, e.g." >&2
  echo "  git clone -b armada https://github.com/dejanzele/flyte.git ../flyte" >&2
  exit 1
fi

echo "==> using kind cluster '$KIND_CLUSTER', host IP $HOST_IP"
# The armada Kind target writes a repo-local kubeconfig (.kube/external/config) rather than merging
# into ~/.kube/config. Honour an already-set KUBECONFIG. Otherwise select the kind context.
if [ -z "${KUBECONFIG:-}" ]; then
  kubectl config use-context "kind-${KIND_CLUSTER}" >/dev/null
fi

echo "==> 1/5  blob store + metadata database"
kubectl apply -f minio.yaml -f postgres.yaml >/dev/null
kubectl -n flyte rollout status deploy/postgres --timeout=120s >/dev/null
kubectl -n flyte rollout status deploy/minio --timeout=120s >/dev/null

echo "==> 2/5  install flyte-binary (v2)"
# Preload the backend image so the pod uses it without a registry round trip. Falls through to a
# normal pull if the image is not in local docker.
kind load docker-image "$FLYTE_IMAGE" --name "$KIND_CLUSTER" >/dev/null 2>&1 || true
rendered="$(mktemp)"
sed "s/__HOST_IP__/${HOST_IP}/g" flyte-binary-values.yaml > "$rendered"
helm upgrade --install flyte-binary "$FLYTE_CHART" -n flyte -f "$rendered" >/dev/null
rm -f "$rendered"
kubectl -n flyte rollout status deploy/flyte-binary --timeout=180s >/dev/null

echo "==> 3/5  task image -> $TASK_IMAGE"
build="$(mktemp -d)"
mkdir -p "$build/pkg"
cp -R "$ROOT/pyproject.toml" "$ROOT/README.md" "$ROOT/src" "$build/pkg/"
cat > "$build/Dockerfile" <<EOF
FROM python:3.11-slim
COPY pkg /pkg
RUN pip install --no-cache-dir "flyte==2.5.1" /pkg
EOF
docker build -t "$TASK_IMAGE" "$build" >/dev/null
rm -rf "$build"
kind load docker-image "$TASK_IMAGE" --name "$KIND_CLUSTER" >/dev/null

echo "==> 4/5  connector (c0) on the host"
# The connector hands each Armada pod the in-cluster minio address, which the pod reaches directly
# through cluster DNS. Nothing on the job path depends on a host-published port.
if lsof -nP -iTCP:8000 -sTCP:LISTEN >/dev/null 2>&1; then
  echo "    connector already on :8000 (reusing; 'pkill -f \"bin/c0 --port 8000\"' to restart)"
else
  ARMADA_URL="${ARMADA_URL:-localhost:50051}" \
    FLYTE_BLOB_ENDPOINT="http://minio.flyte.svc.cluster.local:9000" \
    FLYTE_BLOB_ACCESS_KEY=minio FLYTE_BLOB_SECRET_KEY=minio12345 \
    nohup "$C0" --port 8000 --prometheus_port 9099 >/tmp/armada-flyte-c0.log 2>&1 &
  for _ in $(seq 1 15); do
    grep -aq "armada (0)" /tmp/armada-flyte-c0.log 2>/dev/null && break
    sleep 1
  done
  echo "    connector ready (log: /tmp/armada-flyte-c0.log)"
fi

echo "==> 5/5  host access to the Flyte API and blob store"
# The Flyte client (the examples) and signed-URL uploads run on the host, so they need localhost to
# reach the in-cluster API and minio. These forwards run in the background until you kill them.
ensure_host_port 30080 flyte-binary-http 8090
ensure_host_port 30900 minio 9000

# Flyte's connector-service plugin does not retry a failed CreateTask, so the first task submitted
# must not race the backend's connection to the connector. The backend polls the connector for its
# task types every ~10s. Wait until it logs a successful discovery of the armada connector before
# declaring the backend ready, otherwise the first submit can fail with "connection refused".
echo "==> waiting for the backend to reach the connector"
fb=$(kubectl -n flyte get pods -l app.kubernetes.io/name=flyte-binary --field-selector=status.phase=Running -o name 2>/dev/null | head -1)
for _ in $(seq 1 45); do
  kubectl -n flyte logs "$fb" -c flyte --since=25s 2>/dev/null | grep -aq "supports the following task types: \[armada\]" && break
  sleep 2
done

echo
echo "Flyte backend up. UI: http://localhost:30080/v2   API: localhost:30080"
echo "Submit an example:  $ROOT/.venv/bin/python $ROOT/examples/hello.py"
