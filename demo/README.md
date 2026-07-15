# Quickstart

`armada-flyte` runs Flyte tasks as Armada jobs, so trying it means bringing up both. `./hack/up.sh`
does that in one command: a local kind cluster with Armada (operator, dependencies, and the component
CRs), Traefik, the Flyte 2 backend, the connector, and the `flyte` queue, all wired together.

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

Everything runs in the cluster. `hack/kind-config.yaml` maps every host-facing port to a NodePort, so
the client (the examples) and your browser reach the API and console straight from the host with no
port-forwards. Every hop between components is cluster DNS.

## Prerequisites

- `docker` (running), `helm`, `kubectl`, `kind`, and `python3` (3.10+) on your PATH. `up.sh` checks
  these before it does any work.
- `armadactl` is fetched to `~/bin` if it is not already on your PATH; add `~/bin` to your PATH if the
  command is not found afterwards.

## Bring it up

```
./hack/up.sh
./.venv/bin/python examples/hello.py
```

`up.sh` creates the kind cluster; installs Armada and its dependencies (cert-manager, Pulsar, Postgres,
Redis) via the operator and creates the `flyte` queue; builds the repo's virtualenv; then deploys
Traefik and the Flyte 2 backend (`flyte-binary`), builds and loads the task image, and deploys the
connector. The
first run pulls a lot of images before it settles. It waits for Armada to register executor capacity, so
when it prints `Devbox up` the integration is ready to submit:

```
Devbox up. Run an example:
  ./.venv/bin/python examples/hello.py
  ./.venv/bin/python examples/dag.py
```

`hello.py` prints its result to your terminal (`hello armada, from an Armada pod`), and the run appears
in the Flyte console at the link it prints. That is the whole loop: a Flyte task, scheduled and run by
Armada.

## The UIs

Each example prints two links:

- **Flyte console** (`http://localhost:5001/v2`) - the run graph, status, and pod logs. Served by
  Traefik through the flyte-binary chart's own ingress.
- **Armada Lookout** (`http://localhost:30000`) - the job and pod as Armada sees them.

Per-task input/output values do not render in the Flyte console yet. The Flyte 2 console is pre-release
and its I/O panel is incomplete upstream. The values are recorded correctly: each example prints its
result, and the data is in the blob store.

## Work through the examples

Once `hello.py` runs, the [examples](../examples/) build up in order:

1. [`function.py`](../examples/function.py) - one task doing real work (a Black-Scholes price).
2. [`fanout.py`](../examples/fanout.py) - a typed dataclass through a parallel fan-out / fan-in.
3. [`gang.py`](../examples/gang.py) - N co-scheduled workers as one Armada gang (all-or-nothing).
4. [`dag.py`](../examples/dag.py) - the full shape: generate a dataset, run a gang over it, aggregate.

Each runs the same way: `./.venv/bin/python examples/<name>.py`.

## Teardown

```
./hack/down.sh
```

`down.sh` deletes the kind cluster and clears the Flyte client's upload cache. The cache is keyed by the
endpoint (`localhost:30080`, the same for every devbox), so clearing it here keeps the next `up.sh` from
reusing entries that point at the deleted cluster's minio and 404-ing on their code bundle.

## Overrides

`up.sh` reads `KIND_CLUSTER` (default `armada`). `demo/setup.sh`, which it calls for the Flyte side,
reads `FLYTE_CHART` (default `flyteorg/flyte-binary`, or a local directory for an unreleased chart),
`FLYTE_CHART_VERSION` (default `v2.0.27`), `FLYTE_IMAGE` (default the pinned stock `flyte-binary-v2`
build; this only pre-loads the image into kind — the tag the pod runs is pinned in
`flyte-binary-values.yaml`, so change both together), `TASK_IMAGE` (default `armada-flyte-task:v1`),
and `CONNECTOR_IMAGE` (default
`gresearch/armada-flyte-connector:0.2.0`). The pinned Flyte image is the first that registers the
connector plugin in the executor
([flyteorg/flyte#7565](https://github.com/flyteorg/flyte/pull/7565)).

## Manual setup without the devbox script

`up.sh` is the supported path. If you want the steps by hand, or to submit to an Armada cluster you
already run, the pieces are:

1. **Armada.** `hack/setup-armada.sh` installs Armada into the current kind context (operator,
   dependencies, and the CRs in `hack/armada/`). The stock
   [armada-operator](https://github.com/armadaproject/armada-operator) `make kind-all` also works, but
   its kind config maps only ports 30000-30002, so the Flyte console on 5001 will not be reachable from
   the host - create the cluster from `hack/kind-config.yaml` for that.
2. **Flyte.** `./demo/setup.sh` deploys Traefik, the `flyte-binary` backend (with its minio blob store
   and postgres), builds the task image, and deploys the connector.
3. **Queue.** `armadactl create queue flyte`.

## Files

| File | What it is |
|------|-----------|
| `setup.sh` | Stands up the Flyte side (Traefik + backend + connector) in the cluster. Idempotent. |
| `minio.yaml` | Blob store. Pods reach it in-cluster; the host reaches it via the kind NodePort mapping. |
| `postgres.yaml` | Flyte's `flyte` (metadata) and `runs` (run-graph) databases. |
| `flyte-binary-values.yaml` | Chart overrides: storage, the chart ingress, and the connector routing. |

## Troubleshooting

- **`no connector found for task type [armada]`**: the connector routing did not reach the running
  config. It must live under `configuration.inline.plugins.connector-service` with
  `supportedTaskTypes: [armada]`; the chart's top-level `configuration.connectorService` is not wired in.
  Confirm with `kubectl -n flyte get cm flyte-binary-config -o yaml | grep -A4 connector-service`.
- **Task stuck `Queued`**: Armada has no executor yet, or the `flyte` queue is missing. Check
  `kubectl -n armada get pods` and `armadactl get queue flyte`.
- **A gang (`gang.py` / `dag.py`) is `REJECTED` or stuck `Queued`**: on your own Armada, the gang's
  node-uniformity label must be tracked by the executor and indexed by the scheduler, or the scheduler
  cannot place the gang. Confirm `kubernetes.io/hostname` is in the executor's
  `kubernetes.trackedNodeLabels` and the scheduler's `scheduling.indexedNodeLabels`. The devbox CRs
  (`hack/armada/armada-crs.yaml`) set both already. Also make sure the whole gang fits: with
  `kubernetes.io/hostname` uniformity all members land on one node, so the sum of their requests must
  fit that node's free capacity.
- **`localhost:30080` connection refused**: the cluster is not up, or was created without
  `hack/kind-config.yaml`, so the NodePort is not mapped to the host. Bring the devbox up with
  `./hack/up.sh`.
- **Flyte console shows "No actions found"**: open the exact `http://localhost:5001/v2/...` link the
  example prints. The console reaches the API through Traefik on the same origin.
- **Pod `Completed` but the run fails resolving outputs**: the pod could not reach minio in-cluster.
  Confirm the connector's `FLYTE_BLOB_ENDPOINT` is `http://minio.flyte.svc.cluster.local:9000` and the
  minio credentials match `minio.yaml`.
- **Pod fails with a 404 on `fast<hash>.tar.gz` after a re-run**: you recreated the blob store but the
  client cached the previous upload. Tear down with `./hack/down.sh`, which clears the cache.
