#!/usr/bin/env bash
# Bring up the whole devbox in one shot: a kind cluster with Armada (operator + deps + CRs), Traefik,
# the Flyte 2 backend, and the connector, plus the queue and the venv, ready to run the examples.
#
#   ./hack/up.sh
#   ./.venv/bin/python examples/hello.py
#
# Tear down with ./hack/down.sh (a bare `kind delete cluster` leaves the stale Flyte upload cache
# that down.sh clears, which would 404 the first submit on the next devbox).
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$(pwd)"
HACK="$ROOT/hack"
CLUSTER="${KIND_CLUSTER:-armada}"

echo "==> preflight: required tools and a running Docker"
missing=""
for t in docker kind helm kubectl python3; do
  command -v "$t" >/dev/null 2>&1 || missing="$missing $t"
done
[ -n "$missing" ] && { echo "    missing required tool(s):$missing" >&2; exit 1; }
docker info >/dev/null 2>&1 || { echo "    Docker is not running; start it and retry" >&2; exit 1; }

echo "==> kind cluster '$CLUSTER'"
if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  echo "    cluster exists (reusing; 'kind delete cluster --name $CLUSTER' to recreate)"
  # A cluster not created from hack/kind-config.yaml lacks the host port mappings, and the devbox
  # would only fail minutes later with a misleading error. Probe :30080 (the Flyte API): it is the
  # mapping other Armada setups (e.g. the armada-operator quickstart, which maps 30000-30002) lack.
  if ! docker port "${CLUSTER}-control-plane" 2>/dev/null | grep -q ":30080"; then
    echo "    cluster '$CLUSTER' is missing the kind-config.yaml port mappings (no host :30080)." >&2
    echo "    Recreate it: kind delete cluster --name $CLUSTER && ./hack/up.sh" >&2
    exit 1
  fi
else
  kind create cluster --name "$CLUSTER" --config "$HACK/kind-config.yaml"
fi

echo "==> Armada"
KIND_CLUSTER="$CLUSTER" "$HACK/setup-armada.sh"

echo "==> Python venv (for the examples)"
# pip enforces the supported range (pyproject requires-python >=3.10) if python3 is too old.
[ -d .venv ] || python3 -m venv .venv
./.venv/bin/pip install -q -e .

echo "==> Flyte + connector"
KIND_CLUSTER="$CLUSTER" ./demo/setup.sh

echo
echo "Devbox up. Run an example:"
echo "  ./.venv/bin/python examples/hello.py"
echo "  ./.venv/bin/python examples/dag.py"
