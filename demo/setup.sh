#!/usr/bin/env bash
# Stand up the Flyte 2 side in the kind cluster Armada runs in, and wire the connector.
#
# What it does:
#   1. deploys minio (blob store) + postgres (Flyte metadata) into the kind cluster,
#   2. installs Traefik (the ingress controller that serves the Flyte console),
#   3. installs the flyte-binary (v2) chart, pointed at that minio/postgres and the in-cluster connector,
#   4. builds the task image from this checkout and loads it into the cluster,
#   5. deploys the connector in-cluster, pointed at the in-cluster Armada and minio.
# The host reaches the API, blob store, and console through the kind-config.yaml NodePort mappings.
#
# Prerequisites (see README.md):
#   - An Armada cluster in the kind cluster $KIND_CLUSTER, with a queue "flyte" (hack/setup-armada.sh
#     installs it). This installs the Flyte side into the same cluster.
#   - docker, helm, kubectl, kind on PATH
# (Running the examples afterwards needs this repo's venv; hack/up.sh builds it.)
set -euo pipefail
cd "$(dirname "$0")"
ROOT="$(cd .. && pwd)"

KIND_CLUSTER="${KIND_CLUSTER:-armada}"
# Stock upstream Flyte 2 binary at the first commit that registers the connector-service plugin
# (flyteorg/flyte#7565). This var only controls the kind preload below; the tag the pod actually
# runs is pinned in flyte-binary-values.yaml, so change the two together.
FLYTE_IMAGE="${FLYTE_IMAGE:-cr.flyte.org/flyteorg/flyte-binary-v2:sha-d9e0ebe97be436c7c03c13a8243d3b399d1729e7}"
TASK_IMAGE="${TASK_IMAGE:-armada-flyte-task:v1}"
# The connector image tag. The devbox builds it locally from deploy/Dockerfile (below) so it always
# runs the current source and needs no registry. CI publishes this same name on a GitHub release.
CONNECTOR_IMAGE="${CONNECTOR_IMAGE:-gresearch/armada-flyte-connector:0.2.0}"
# Published flyte-binary chart from the flyteorg helm repo. Set FLYTE_CHART to a local path (e.g. a
# flyteorg/flyte checkout) to use an unreleased chart. A local path skips the version pin.
FLYTE_CHART="${FLYTE_CHART:-flyteorg/flyte-binary}"
FLYTE_CHART_VERSION="${FLYTE_CHART_VERSION:-v2.0.27}"

echo "==> using kind cluster '$KIND_CLUSTER'"
# Target this cluster's kind context explicitly on every kubectl/helm call, so the script never switches
# the user's current context and never installs into whatever cluster KUBECONFIG happens to point at.
CTX="kind-${KIND_CLUSTER}"
kc() { kubectl --context "$CTX" "$@"; }

echo "==> 1/5  blob store + metadata database"
kc apply -f minio.yaml -f postgres.yaml >/dev/null
kc -n flyte rollout status deploy/postgres --timeout=120s >/dev/null
kc -n flyte rollout status deploy/minio --timeout=120s >/dev/null

echo "==> 2/5  ingress controller (traefik)"
# Traefik serves the flyte-binary chart's ingress, which routes /v2 to the console and the flyteidl2
# API paths to flyte-binary-http, same origin. Its web entrypoint is a NodePort the kind config maps
# to localhost:5001, so the console is browsed there.
helm repo add traefik https://traefik.github.io/charts >/dev/null 2>&1 || true
helm repo update traefik >/dev/null 2>&1 || true
helm --kube-context "$CTX" upgrade --install traefik traefik/traefik -n traefik --create-namespace \
  --set service.type=NodePort --set ports.web.nodePort=30500 >/dev/null
kc -n traefik rollout status deploy/traefik --timeout=120s >/dev/null

echo "==> 3/5  install flyte-binary (v2)"
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
helm --kube-context "$CTX" upgrade --install flyte-binary "$FLYTE_CHART" "${chart_version_arg[@]}" -n flyte -f flyte-binary-values.yaml >/dev/null
kc -n flyte rollout status deploy/flyte-binary --timeout=180s >/dev/null
kc -n flyte rollout status deploy/flyte-binary-console --timeout=120s >/dev/null

echo "==> 4/5  task image -> $TASK_IMAGE"
build="$(mktemp -d)"
mkdir -p "$build/pkg"
cp -R "$ROOT/pyproject.toml" "$ROOT/README.md" "$ROOT/src" "$build/pkg/"
cat > "$build/Dockerfile" <<EOF
FROM python:3.13-slim
COPY pkg /pkg
RUN pip install --no-cache-dir "flyte==2.5.8" /pkg
EOF
docker build -t "$TASK_IMAGE" "$build" >/dev/null
rm -rf "$build"
kind load docker-image "$TASK_IMAGE" --name "$KIND_CLUSTER" >/dev/null

echo "==> 5/5  connector (build + deploy in-cluster)"
# Build the connector from source and load it into kind, so the devbox always runs the current source
# with no registry. connector.yaml pulls this image name only IfNotPresent, so the loaded build is used.
docker build -t "$CONNECTOR_IMAGE" -f "$ROOT/deploy/Dockerfile" "$ROOT" >/dev/null
kind load docker-image "$CONNECTOR_IMAGE" --name "$KIND_CLUSTER" >/dev/null
kc apply -f "$ROOT/deploy/kubernetes/connector.yaml" >/dev/null
kc -n flyte rollout status deploy/armada-flyte-connector --timeout=120s >/dev/null

# Flyte's connector-service plugin does not retry a failed CreateTask, so the first task submitted
# must not race the backend's connection to the connector. The backend polls the connector for its
# task types every ~10s. Wait until it logs a successful discovery of the armada connector before
# declaring the backend ready, otherwise the first submit can fail with "connection refused".
echo "==> waiting for the backend to reach the connector"
fb=$(kc -n flyte get pods -l app.kubernetes.io/name=flyte-binary --field-selector=status.phase=Running -o name 2>/dev/null | head -1)
discovered=""
if [ -n "$fb" ]; then
  for _ in $(seq 1 45); do
    if kc -n flyte logs "$fb" -c flyte --since=25s 2>/dev/null | grep -aq "supports the following task types: \[armada\]"; then
      discovered=1
      break
    fi
    sleep 2
  done
fi
if [ -z "$discovered" ]; then
  echo "    warning: the backend has not logged connector discovery yet; the first submit may fail" >&2
  echo "    with 'connection refused'. If it does, wait a few seconds and resubmit." >&2
fi

echo
echo "Flyte backend up."
echo "  API (examples submit here):   localhost:30080"
echo "  Flyte console (run graph):    http://localhost:5001/v2"
echo "  Armada Lookout (job status):  http://localhost:30000"
echo "Submit an example:  $ROOT/.venv/bin/python $ROOT/examples/hello.py"
