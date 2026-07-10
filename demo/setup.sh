#!/usr/bin/env bash
# Stand up a Flyte 2 backend in the same Kind cluster Armada already uses, and wire the connector.
#
# What it does:
#   1. deploys minio (blob store) + postgres (Flyte metadata) into the kind cluster,
#   2. installs the flyte-binary (v2) chart, pointed at that minio/postgres and the in-cluster connector,
#   3. builds the task image from this checkout and loads it into the cluster,
#   4. deploys the connector in-cluster, pointed at the in-cluster Armada and minio,
#   5. exposes the Flyte API and blob store on their NodePorts.
#
# Prerequisites (see README.md):
#   - An Armada cluster to submit to, running in the kind cluster $KIND_CLUSTER. The quickstart stands one
#     up with the armada-operator quickstart (`make kind-all`); this installs the Flyte side into the same
#     cluster. Create the queue this integration submits to: `armadactl create queue flyte`.
#   - this repo's venv built (`python3.11 -m venv .venv && ./.venv/bin/pip install -e .`)
#   - docker, helm, kubectl, kind on PATH
set -euo pipefail
cd "$(dirname "$0")"
ROOT="$(cd .. && pwd)"

KIND_CLUSTER="${KIND_CLUSTER:-armada}"
# Stock upstream Flyte 2 binary at the first commit that registers the connector-service plugin
# (flyteorg/flyte#7565). Keep this in sync with the image tag in flyte-binary-values.yaml.
FLYTE_IMAGE="${FLYTE_IMAGE:-cr.flyte.org/flyteorg/flyte-binary-v2:sha-d9e0ebe97be436c7c03c13a8243d3b399d1729e7}"
TASK_IMAGE="${TASK_IMAGE:-armada-flyte-task:v1}"
# The connector image, deployed in-cluster from deploy/kubernetes/connector.yaml. Published multi-arch.
CONNECTOR_IMAGE="${CONNECTOR_IMAGE:-dpejcev/armada-flyte-connector:0.1.0}"
# Published flyte-binary chart from the flyteorg helm repo. Set FLYTE_CHART to a local path (e.g. a
# flyteorg/flyte checkout) to use an unreleased chart. A local path skips the version pin.
FLYTE_CHART="${FLYTE_CHART:-flyteorg/flyte-binary}"
FLYTE_CHART_VERSION="${FLYTE_CHART_VERSION:-v2.0.27}"

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

echo "==> using kind cluster '$KIND_CLUSTER'"
# Honour an already-set KUBECONFIG. Otherwise select the kind context for this cluster.
if [ -z "${KUBECONFIG:-}" ]; then
  kubectl config use-context "kind-${KIND_CLUSTER}" >/dev/null
fi

echo "==> 1/5  blob store + metadata database"
kubectl apply -f minio.yaml -f postgres.yaml >/dev/null
kubectl -n flyte rollout status deploy/postgres --timeout=120s >/dev/null
kubectl -n flyte rollout status deploy/minio --timeout=120s >/dev/null

echo "==> 2/5  install flyte-binary (v2)"
# Preload the backend image so the pod uses it without a registry round trip. Falls through to a
# normal pull if the image is not in local docker (the default stock image is pulled from cr.flyte.org).
kind load docker-image "$FLYTE_IMAGE" --name "$KIND_CLUSTER" >/dev/null 2>&1 || true
# A local chart path (FLYTE_CHART points at a directory) is used as-is. Otherwise pull the pinned
# version from the published flyteorg helm repo.
chart_version_arg=()
if [ ! -d "$FLYTE_CHART" ]; then
  helm repo add flyteorg https://helm.flyte.org >/dev/null 2>&1 || true
  helm repo update flyteorg >/dev/null 2>&1 || true
  chart_version_arg=(--version "$FLYTE_CHART_VERSION")
fi
helm upgrade --install flyte-binary "$FLYTE_CHART" "${chart_version_arg[@]}" -n flyte -f flyte-binary-values.yaml >/dev/null
kubectl -n flyte rollout status deploy/flyte-binary --timeout=180s >/dev/null

# Serve the run UI. The console makes same-origin ConnectRPC calls to the Flyte API, so an nginx sidecar
# in the console pod routes /v2 to the console and everything else to flyte-binary-http. One URL,
# localhost:5001, then works from both the browser (through the port-forward) and the console's own
# server-side render (pod loopback). PORT/HOSTNAME make the console bind :8080 so the sidecar reaches it.
kubectl apply -f console-proxy.yaml >/dev/null
kubectl -n flyte patch deploy flyte-binary-console --type strategic -p '{"spec":{"template":{"spec":{"containers":[{"name":"proxy","image":"nginx:1.27-alpine","ports":[{"containerPort":5001}],"volumeMounts":[{"name":"proxy-conf","mountPath":"/etc/nginx/nginx.conf","subPath":"nginx.conf"}]}],"volumes":[{"name":"proxy-conf","configMap":{"name":"flyte-console-proxy"}}]}}}}' >/dev/null
kubectl -n flyte set env deploy/flyte-binary-console PORT=8080 HOSTNAME=0.0.0.0 NEXT_PUBLIC_ADMIN_API_URL="http://localhost:5001" >/dev/null
kubectl -n flyte rollout status deploy/flyte-binary-console --timeout=120s >/dev/null

echo "==> 3/5  task image -> $TASK_IMAGE"
build="$(mktemp -d)"
mkdir -p "$build/pkg"
cp -R "$ROOT/pyproject.toml" "$ROOT/README.md" "$ROOT/src" "$build/pkg/"
cat > "$build/Dockerfile" <<EOF
FROM python:3.11-slim
COPY pkg /pkg
RUN pip install --no-cache-dir "flyte==2.5.8" /pkg
EOF
docker build -t "$TASK_IMAGE" "$build" >/dev/null
rm -rf "$build"
kind load docker-image "$TASK_IMAGE" --name "$KIND_CLUSTER" >/dev/null

echo "==> 4/5  connector (in-cluster)"
# The connector runs as the armada-flyte-connector Deployment/Service. It submits to the Armada server
# and hands each Armada pod the minio address, both over cluster DNS (see connector.yaml's env).
kind load docker-image "$CONNECTOR_IMAGE" --name "$KIND_CLUSTER" >/dev/null 2>&1 || true
kubectl apply -f "$ROOT/deploy/kubernetes/connector.yaml" >/dev/null
kubectl -n flyte rollout status deploy/armada-flyte-connector --timeout=120s >/dev/null

echo "==> 5/5  host access to the Flyte API, blob store, and console"
# The examples, signed-URL uploads, and the browser run on the host, so port-forward the API, minio, and
# the console to localhost. These forwards run in the background until you kill them.
ensure_host_port 30080 flyte-binary-http 8090
ensure_host_port 30900 minio 9000
ensure_host_port 5001 flyte-console-ui 5001

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
echo "Flyte backend up."
echo "  API (examples submit here):   localhost:30080"
echo "  Flyte console (run graph):    http://localhost:5001/v2"
echo "  Armada Lookout (job status):  http://localhost:30000"
echo "Submit an example:  $ROOT/.venv/bin/python $ROOT/examples/hello.py"
