#!/usr/bin/env bash
# Install Armada into the current kind cluster: the armada-operator (upstream Helm chart), its
# dependencies (Pulsar, Redis, Postgres via their upstream charts), and the Armada component CRs from
# armada/ (vendored from the operator's quickstart, with the gang node-label config baked in and
# Prometheus disabled). Mirrors the armada-operator quickstart's install steps, minus the cluster
# creation, so it runs against a cluster created from hack/kind-config.yaml.
set -euo pipefail
cd "$(dirname "$0")"

# Derive the kube context from the cluster name up.sh passes, so KIND_CLUSTER=foo targets kind-foo.
CTX="${KUBECTL_CONTEXT:-kind-${KIND_CLUSTER:-armada}}"
# armadactl's endpoint. A dedicated override, not ARMADA_URL, which is the connector's gRPC endpoint
# (<host>:50051) documented elsewhere and would point armadactl at the wrong address.
ARMADACTL_URL="${ARMADACTL_URL:-localhost:30002}"
kc() { kubectl --context "$CTX" "$@"; }

# retry_until <timeout_secs> <desc> <cmd...>: run cmd every 5s until it exits 0 (or its stderr says
# "already exists"), or the timeout elapses. Mirrors the armada-operator e2e test's stabilization wait.
retry_until() {
  local timeout=$1 desc=$2; shift 2
  local start ef; start=$(date +%s); ef=$(mktemp)
  until "$@" 2>"$ef"; do
    grep -qiE 'already exists' "$ef" && { rm -f "$ef"; return 0; }
    if [ $(( $(date +%s) - start )) -ge "$timeout" ]; then
      echo "    timed out after ${timeout}s waiting for $desc" >&2; rm -f "$ef"; return 1
    fi
    sleep 5
  done
  rm -f "$ef"
}

# wait_ready <label> <poll_iters>: poll until a pod with app=<label> exists, then wait for it Ready.
# (kubectl wait errors if no pod yet matches the selector, hence the existence poll first.)
# Deliberately never fails: the gates that matter (queue creation, scheduler capacity) enforce
# readiness downstream with better error messages; this just avoids racing them.
wait_ready() {
  local label=$1 iters=$2
  for _ in $(seq 1 "$iters"); do
    kc -n armada get pod -l "app=$label" --field-selector=status.phase=Running \
      -o name 2>/dev/null | grep -q . && break
    sleep 4
  done
  kc -n armada wait pod -l "app=$label" --for=condition=Ready=true --timeout=600s >/dev/null 2>&1 || true
}

echo "==> helm repos"
helm repo add jetstack https://charts.jetstack.io >/dev/null 2>&1 || true
helm repo add groundhog2k https://groundhog2k.github.io/helm-charts/ >/dev/null 2>&1 || true
helm repo add apache https://pulsar.apache.org/charts >/dev/null 2>&1 || true
helm repo add dandydev https://dandydeveloper.github.io/charts >/dev/null 2>&1 || true
helm repo add gresearch https://g-research.github.io/charts >/dev/null 2>&1 || true
helm repo update >/dev/null 2>&1

# Install the slow dependencies first without waiting, so their pods come up while cert-manager and the
# operator install. Everything is waited on together below.
echo "==> dependencies: pulsar, postgres, redis (namespace data)"
helm upgrade --install pulsar apache/pulsar --version 4.4.0 -f armada/pulsar.values.yaml \
  -n data --create-namespace --kube-context "$CTX" >/dev/null
helm upgrade --install postgresql groundhog2k/postgres --version 1.6.1 -f armada/postgres.values.yaml \
  -n data --create-namespace --kube-context "$CTX" >/dev/null
helm upgrade --install redis-ha dandydev/redis-ha --version 4.35.5 -f armada/redis.values.yaml \
  -n data --create-namespace --kube-context "$CTX" >/dev/null

echo "==> cert-manager (the operator's admission webhooks need it)"
helm upgrade --install cert-manager jetstack/cert-manager -n cert-manager --create-namespace \
  --version v1.19.2 --set installCRDs=true --kube-context "$CTX" >/dev/null

echo "==> armada-operator"
helm upgrade --install armada-operator gresearch/armada-operator -n armada-system --create-namespace \
  --kube-context "$CTX" >/dev/null

echo "==> waiting for cert-manager, the operator, and the dependencies to be ready"
kc wait --for=condition=Available --timeout=300s -n cert-manager deploy --all >/dev/null
kc -n armada-system rollout status deploy/armada-operator-controller-manager --timeout=300s >/dev/null
# The operator's admission webhook must have endpoints before the CRs are applied, or apply races it
# with "connection refused". Wait for the webhook service to have a ready backend.
webhook_has_endpoints() {
  [ -n "$(kc -n armada-system get endpoints armada-operator-webhook-service \
      -o jsonpath='{.subsets[*].addresses[*].ip}' 2>/dev/null)" ]
}
retry_until 180 "the operator webhook to have endpoints" webhook_has_endpoints
for ss in postgresql pulsar-bookie pulsar-broker pulsar-proxy pulsar-toolset pulsar-zookeeper redis-ha-server; do
  kc -n data rollout status statefulset/"$ss" --timeout=600s >/dev/null
done

echo "==> Armada components (CRs)"
kc create namespace armada >/dev/null 2>&1 || true
kc apply -f armada/armada-crs.yaml >/dev/null
kc apply -f armada/priority-class.yaml >/dev/null
echo "    waiting for the Armada server..."
wait_ready armada-server 120

echo "==> armadactl"
if ! command -v armadactl >/dev/null 2>&1; then
  echo "    downloading armadactl to ~/bin"
  # Fail loudly on a download/install error, so it does not surface later as a misleading queue timeout.
  if ! curl -fsSL https://raw.githubusercontent.com/armadaproject/armada/master/scripts/get-armadactl.sh | bash; then
    echo "    armadactl install failed; install it manually and re-run" >&2; exit 1
  fi
  export PATH="$HOME/bin:$PATH"
  command -v armadactl >/dev/null 2>&1 || { echo "    armadactl not on PATH after install (add ~/bin)" >&2; exit 1; }
fi

# The queue table is created by the scheduler DB migration, which can finish after the server pod is
# Ready, so retry CreateQueue until it is accepted.
echo "==> queue flyte"
retry_until 300 "queue flyte" armadactl create queue flyte --armadaUrl "$ARMADACTL_URL"

# Wait for the executor pod to be Ready first, so a slow image pull is not counted against the capacity
# wait below (and a genuine registration stall is told apart from a still-pulling executor).
echo "==> waiting for the executor to be Ready"
wait_ready armada-executor 150

# A job submitted before the scheduler has registered the executor's nodes is Rejected. The scheduler
# logs each pool's capacity every cycle. Wait until pool "default" reports non-zero CPU (nodes registered).
# The executor's node reports reach the scheduler through Pulsar, which can be slow to stabilize on a
# first-ever boot, so allow generously.
echo "==> waiting for the scheduler to register the executor's capacity"
scheduler_has_capacity() {
  kc logs -n armada -l app=armada-scheduler --tail=200 2>/dev/null \
    | grep -qE "Scheduling on pool .* with capacity \(memory=[0-9]+,cpu=[1-9]"
}
if ! retry_until 1200 "the scheduler to register capacity" scheduler_has_capacity; then
  echo "    the executor is Ready but the scheduler still reports no capacity." >&2
  echo "    Pulsar can be slow to stabilize on a first-ever boot; re-run ./hack/up.sh (idempotent)." >&2
  exit 1
fi

echo "Armada up (Lookout: http://localhost:30000, gRPC: localhost:30002)."
