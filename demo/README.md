# Local quickstart

Run a Flyte 2 backend in the same Kind cluster Armada already schedules onto, then submit a task.
This is the one-command way to see the connector work end to end.

```
   host                             Kind cluster "armada-test"
 ┌────────────┐  localhost:30080  ┌─────────────────────────────────────────┐
 │ Flyte CLI  │ ─────────────────▶│ flyte-binary (API + console + TaskAction │
 │ (examples) │  localhost:30900  │   reconciler)                            │
 ├────────────┤ ─────────────────▶│ postgres (metadata + runs)               │
 │ connector  │◀──── :8000 ───────│ minio    (blob store)                    │
 │  c0        │ ──┐               │ armada-<jobid> pods ──in-cluster──▶ minio │
 └────────────┘   │ :50051        └─────────────────────────────────────────┘
                  └──▶ Armada control plane (docker, from `dev:full`)
```

`flyte-binary`, minio, and postgres run in the cluster. The connector `c0` runs on the host and is
the only process bridging Flyte and Armada. The Armada pods reach minio in-cluster at
`minio.flyte:9000`, and `setup.sh` port-forwards the Flyte API and minio to `localhost` so the client
reaches them without any kind port mapping.

## Prerequisites

1. **Armada**, with a real executor against the Kind cluster and the `flyte` queue created. Clone
   [armada](https://github.com/armadaproject/armada), then:
   ```
   go run github.com/magefile/mage@v1.17.2 dev:full               # kind "armada-test" + full stack
   go run cmd/armadactl/main.go create queue flyte --armadaUrl localhost:50051
   ```
   Wait for the executor to log `Reporting current free resource` before submitting.
2. **This repo's venv** (an arm64 Python on Apple Silicon, since Flyte's `obstore` wheel has no x86 build):
   ```
   python3.11 -m venv .venv && ./.venv/bin/pip install -e .
   ```
3. `docker`, `helm`, `kubectl`, `kind` on PATH.

`setup.sh` pulls the published flyte-binary chart from the flyteorg helm repo and a stock upstream
`cr.flyte.org/flyteorg/flyte-binary-v2` image, so no Flyte checkout is needed. The pinned image is the
first commit that registers the connector-service plugin in the v2 executor
([flyteorg/flyte#7565](https://github.com/flyteorg/flyte/pull/7565)). Earlier stock builds do not have
it.

## Run

```
export KUBECONFIG=<armada-checkout>/.kube/external/config   # the kubeconfig `dev:full` writes
./demo/setup.sh
```

`setup.sh` deploys minio + postgres, installs the flyte-binary chart, builds and loads the task
image, starts the connector, and port-forwards the Flyte API and minio to `localhost`. Then submit an
example. It points at `localhost:30080` and targets queue `flyte`:

```
./.venv/bin/python examples/hello.py
```

Open the printed `UI:` link to watch Armada schedule the pod and record the typed result. Other
examples work the same way (`examples/fanout.py`, `examples/gang.py`, `examples/dag.py`).

## Files

| File | What it is |
|------|-----------|
| `setup.sh` | One-command stand-up of the backend + connector. Idempotent. |
| `minio.yaml` | Blob store. Pods reach it in-cluster. The host reaches it via a port-forward `setup.sh` starts. |
| `postgres.yaml` | Flyte's `flyte` (metadata) and `runs` (run-graph) databases. |
| `flyte-binary-values.yaml` | Chart overrides. `__HOST_IP__` (the connector endpoint) is filled in by `setup.sh`. |

## Overrides

`setup.sh` reads these env vars: `KIND_CLUSTER` (default `armada-test`), `FLYTE_CHART` (default
`flyteorg/flyte-binary`, or a local directory for an unreleased chart), `FLYTE_CHART_VERSION`
(default `v2.0.27`), `FLYTE_IMAGE` (default the pinned stock `flyte-binary-v2` build), `TASK_IMAGE`
(default `armada-flyte-task:v1`), `ARMADA_URL` (default `localhost:50051`), `HOST_IP` (auto-detected).

## Teardown

```
helm -n flyte uninstall flyte-binary
kubectl delete namespace flyte
pkill -f "bin/c0 --port 8000"
pkill -f "port-forward svc/flyte-binary-http"
pkill -f "port-forward svc/minio"
```

The kind cluster and Armada come down with `go run github.com/magefile/mage@v1.17.2 dev:fullDown` in
the armada repo.

## Troubleshooting

- **`no connector found for task type [armada]`**: the connector config did not reach the running
  config. It must live under `configuration.inline.plugins.connector-service` with
  `supportedTaskTypes: [armada]`. The chart's top-level `configuration.connectorService` is not wired
  in. Confirm with `kubectl -n flyte get cm flyte-binary-config -o yaml | grep -A4 connector-service`.
- **`TaskAction` stuck `Queued`**: Armada has no executor yet, or the `flyte` queue is missing.
  Check the connector log (`/tmp/armada-flyte-c0.log`) and `armadactl get queue flyte`.
- **`localhost:30080` connection refused**: the port-forward is not running. Re-run `setup.sh`, or
  start it by hand: `kubectl -n flyte port-forward svc/flyte-binary-http 30080:8090`.
- **Pod `Completed` but the run fails resolving outputs**: the pod could not reach minio in-cluster.
  Confirm the connector was started with `FLYTE_BLOB_ENDPOINT=http://minio.flyte.svc.cluster.local:9000`
  and that the minio credentials match `minio.yaml`.
- **Pod fails with a 404 on `fast<hash>.tar.gz` after a re-run**: you recreated the blob store (a
  fresh minio) but the client cached the previous upload and skipped re-uploading the code bundle to
  the new one. Clear the cache and rerun: `rm -f ~/.flyte/local-cache/cache.db`.
