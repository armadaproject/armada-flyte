# Quickstart

`armada-flyte` runs Flyte tasks as Armada jobs, so trying it means bringing up both. This walks the
three parts end to end, all in one local kind cluster:

1. **Start Armada** - the batch scheduler that runs your tasks.
2. **Start Flyte** - the Flyte 2 backend and the `armada-flyte` connector that bridges them.
3. **Submit a task** - a normal Flyte task that Armada schedules and runs.

```
   host                          kind cluster "armada"
 ┌──────────┐  localhost:30080  ┌───────────────────────────────────────────┐
 │ examples │ ─────────────────▶│ flyte-binary (API + console)               │
 │ (client) │                   │ armada-flyte-connector ──▶ armada-server   │
 │ browser  │  localhost:5001   │ minio (blob store), postgres (metadata)    │
 │          │ ─────────────────▶│ armada (operator): server, scheduler,      │
 └──────────┘                   │   executor ──▶ job pods                    │
                                └───────────────────────────────────────────┘
```

Everything runs in the cluster. The client (the examples) and your browser reach the API and console
from the host through port-forwards `setup.sh` starts. Every hop between components is cluster DNS.

## Prerequisites

- `docker`, `helm`, `kubectl`, `kind`, `git`, `make`, and Python 3.11 on your PATH.
- This repo's virtualenv:
  ```
  python3.11 -m venv .venv && ./.venv/bin/pip install -e .
  ```

## 1. Start Armada

`armada-flyte` submits to an Armada cluster. For a local trial, the
[armada-operator](https://github.com/armadaproject/armada-operator) quickstart stands one up in kind,
with the executor and `armadactl` wired up:

```
git clone https://github.com/armadaproject/armada-operator
cd armada-operator
make kind-all
armadactl create queue flyte
```

`make kind-all` creates the `armada` kind cluster and installs Armada with its dependencies (cert-manager,
Pulsar, Postgres, Redis), so the first run pulls a lot before it settles. It also downloads `armadactl` to
`~/bin`; if the `armadactl` command is not found, add `~/bin` to your PATH. `flyte` is the queue this
integration submits to.

If `apply-armada-crs` races the operator's webhook (`connection refused`), re-run
`kubectl apply -f dev/quickstart/armada-crs.yaml` once the operator pod is Ready.

## 2. Start Flyte

Now stand up the Flyte side into that same cluster. From your `armada-flyte` checkout:

```
./demo/setup.sh
```

`setup.sh` deploys the Flyte 2 backend (`flyte-binary`, with its minio blob store and postgres), builds
the task image, and deploys the **connector** - the gRPC service that receives Flyte tasks and submits
them to Armada. It pulls a stock upstream flyte-binary chart and image, so no Flyte checkout is needed.
The pinned image is the first that registers the connector plugin in the executor
([flyteorg/flyte#7565](https://github.com/flyteorg/flyte/pull/7565)). When it prints `Flyte backend up`,
the integration is ready:

```
Flyte backend up.
  API (examples submit here):   localhost:30080
  Flyte console (run graph):    http://localhost:5001/v2
  Armada Lookout (job status):  http://localhost:30000
```

## 3. Submit a task

```
./.venv/bin/python examples/hello.py
```

The task result prints to your terminal (`hello armada, from an Armada pod`), and the run appears in the
Flyte console at the printed link. That is the whole loop: a Flyte task, scheduled and run by Armada.

## The UIs

Each example prints two links:

- **Flyte console** (`http://localhost:5001/v2`) - the run graph, status, and pod logs.
- **Armada Lookout** (`http://localhost:30000`) - the job and pod as Armada sees them.

Per-task input/output values do not render in the Flyte console yet. The Flyte 2 console is pre-release
and its I/O panel is incomplete upstream. The values are recorded correctly: each example prints its
result, and the data is in the blob store.

## Next: work through the examples

Once `hello.py` runs, the [examples](../examples/) build up in order:

1. [`function.py`](../examples/function.py) - one task doing real work (a Black-Scholes price).
2. [`fanout.py`](../examples/fanout.py) - a typed dataclass through a parallel fan-out / fan-in.
3. [`gang.py`](../examples/gang.py) - N co-scheduled workers as one Armada gang (all-or-nothing).
4. [`dag.py`](../examples/dag.py) - the full shape: generate a dataset, run a gang over it, aggregate.

Each runs the same way: `./.venv/bin/python examples/<name>.py`.

## Files

| File | What it is |
|------|-----------|
| `setup.sh` | Stands up the Flyte side (backend + connector + console) in the cluster. Idempotent. |
| `minio.yaml` | Blob store. Pods reach it in-cluster; the host reaches it via a port-forward. |
| `postgres.yaml` | Flyte's `flyte` (metadata) and `runs` (run-graph) databases. |
| `flyte-binary-values.yaml` | Chart overrides: storage and the connector routing. |
| `console-proxy.yaml` | The console's same-origin sidecar proxy and its service. |

## Overrides

`setup.sh` reads these env vars: `KIND_CLUSTER` (default `armada`), `FLYTE_CHART` (default
`flyteorg/flyte-binary`, or a local directory for an unreleased chart), `FLYTE_CHART_VERSION`
(default `v2.0.27`), `FLYTE_IMAGE` (default the pinned stock `flyte-binary-v2` build), `TASK_IMAGE`
(default `armada-flyte-task:v1`), and `CONNECTOR_IMAGE` (default `dpejcev/armada-flyte-connector:0.1.0`).

## Teardown

```
helm -n flyte uninstall flyte-binary
kubectl delete namespace flyte
pkill -f "port-forward.*flyte-binary-http"
pkill -f "port-forward.*minio"
pkill -f "port-forward.*flyte-console-ui"
```

Delete the cluster and Armada with `kind delete cluster --name armada` (or `make kind-delete-cluster` in
the armada-operator checkout).

## Troubleshooting

- **`no connector found for task type [armada]`**: the connector routing did not reach the running
  config. It must live under `configuration.inline.plugins.connector-service` with
  `supportedTaskTypes: [armada]`; the chart's top-level `configuration.connectorService` is not wired in.
  Confirm with `kubectl -n flyte get cm flyte-binary-config -o yaml | grep -A4 connector-service`.
- **Task stuck `Queued`**: Armada has no executor yet, or the `flyte` queue is missing. Check
  `kubectl -n armada get pods` and `armadactl get queue flyte`.
- **`localhost:30080` connection refused**: a port-forward is not running (laptop sleep drops them).
  Re-run `setup.sh`, or restart it by hand: `kubectl -n flyte port-forward svc/flyte-binary-http 30080:8090`.
- **Flyte console shows "No actions found"**: open the exact `http://localhost:5001/v2/...` link the
  example prints. The console's API calls go through the sidecar proxy on that same origin.
- **Pod `Completed` but the run fails resolving outputs**: the pod could not reach minio in-cluster.
  Confirm the connector's `FLYTE_BLOB_ENDPOINT` is `http://minio.flyte.svc.cluster.local:9000` and the
  minio credentials match `minio.yaml`.
- **Pod fails with a 404 on `fast<hash>.tar.gz` after a re-run**: you recreated the blob store but the
  client cached the previous upload. Clear the cache and rerun: `rm -f ~/.flyte/local-cache/cache.db`.
