# Architecture

Flyte owns the DAG and the data flow between nodes. Armada owns scheduling and execution. The
bridge is a single Flyte 2 connector, plus a small set of changes to the Flyte 2 executor that let
it route tasks to that connector.

This document is a reference for how the two systems fit together. Every claim is grounded in the
code that implements it.

## Big picture

Flyte authors, Armada schedules. A user writes a normal Flyte 2 `@env.task` Python function. Flyte
renders it into a container whose entrypoint is `a0` (the Flyte task runtime, which loads the code
bundle, reads typed inputs from the blob store, runs the function, and writes typed outputs back).
Instead of scheduling that container itself, Flyte routes the task to the Armada connector, which
submits it as an Armada job. Armada schedules the pod onto a cluster it manages, `a0` runs there, and
the typed result flows back through the shared blob store. Flyte reads that result directly, so its
lineage and typed-data features are untouched. The connector delegates scheduling only. It never
touches the data plane and never synthesizes an output (`connector.py`).

The pieces (`docs/getting-started.md`):

- Flyte 2 runs as `flyte-binary`: the control plane, the UI, and the `TaskAction` reconciler.
- Armada's control plane submits and schedules jobs. Its executor creates the job pods on a
  Kubernetes cluster.
- The connector runs as a gRPC service that bridges the two (in-cluster in the devbox, or anywhere a
  Flyte backend can reach it).

Where these run is up to you. The blob store is shared, so Flyte and the Armada pods address the
same bucket.

## Components

### The connector service (c0)

The connector runs as an out-of-process gRPC service launched as `c0` (Flyte's connector-runtime
binary), and the Flyte backend calls
`CreateTask` / `GetTask` / `DeleteTask` on it. It holds no per-job state in the object itself,
caching only the gRPC channel and its resolved config (`connector.py`). All per-job state
lives in `ArmadaJobMetadata`, which Flyte persists and hands back on every call.

### `ArmadaFunctionTask` and `ArmadaConfig` (authoring surface)

`src/armada_flyte/task.py` is what a workflow author touches.

- `ArmadaConfig` (`task.py`) is a dataclass of per-task Armada submission knobs: `queue`
  (default `"flyte"`), `job_set_id` (default `"flyte-dag"`), `namespace` (default `"default"`),
  `priority` (default `1`), and optional `cpu`/`memory` overrides (`None` means defer to
  `flyte.Resources`). It is deliberately minimal. Resources are normally declared the stock-Flyte way,
  and `ArmadaConfig` carries only the Armada-specific fields. Gang scheduling is expressed separately
  with `armada_flyte.Gang` (see [Gang scheduling](#gang-scheduling)), not through `ArmadaConfig`.
- `ArmadaFunctionTask(AsyncConnectorExecutorMixin, AsyncFunctionTaskTemplate)` (`task.py`) is
  the task type. `_TASK_TYPE = "armada"` (`task.py`), and `__post_init__` sets `self.task_type`
  to it (`task.py`). That string routes the task to the connector. `custom_config()`
  returns `asdict(self.plugin_config)` (`task.py`), which Flyte serializes into the task
  template's `custom` field.
- `TaskPluginRegistry.register(ArmadaConfig, ArmadaFunctionTask)` (`task.py`) wires the two
  together. Any `TaskEnvironment(plugin_config=ArmadaConfig(...))` builds Armada-typed tasks.

`ArmadaConfig` is serialized and persisted in the control plane, which is why credentials never go
here (see next section).

### `ConnectorConfig` (connector-side config, distinct from `ArmadaConfig`)

`src/armada_flyte/config.py` holds the connector's own settings.

- `ConnectorConfig` (`config.py`) is a frozen dataclass: `armada_url` (default
  `"localhost:50051"`), and `blob_endpoint` / `blob_access_key` / `blob_secret_key` (default empty).
  It is marked as the extension point for `auth_token` / `tls` / `ca_cert` (`config.py`).
- Credentials live here, on the connector, never in `ArmadaConfig` (`config.py`). `ArmadaConfig`
  is serialized into the task template and persisted, so a token there would leak into the control
  plane DB.
- Resolution has three layers (`config.py`): dataclass defaults, then environment
  (`from_env` reads `ARMADA_URL` and `FLYTE_BLOB_ENDPOINT` / `FLYTE_BLOB_ACCESS_KEY` /
  `FLYTE_BLOB_SECRET_KEY`, `config.py`), then in-code `configure(**kwargs)` which validates
  keys and stashes them in a module-level `_overrides` (`config.py`). `resolve_config()`
  composes them: `from_env()` then `replace(cfg, **_overrides)` (`config.py`).
- Resolution is lazy, on first connector use (`connector.py`). There is no
  set-env-before-import trap, but `configure()` must run before the first task, because the client
  is built once and cached.

### The blob store

A single shared S3-compatible bucket is addressed by the backend, the client, and the Armada pods
alike. The connector injects the pod-reachable blob endpoint onto each job
(`_storage_env` in `connector.py`). The `a0` container reads inputs and writes `outputs.pb` there, and
Flyte reads `outputs.pb` directly.

## End-to-end data flow for one task

1. Author. A user writes `@env.task` in a `TaskEnvironment(plugin_config=ArmadaConfig(...))`. Flyte
   renders the task into a container whose entrypoint is `a0`, and serializes `ArmadaConfig` into
   the task template's `custom` field.

2. Route. The task's `task_type` is `"armada"` (`task.py`), which matches
   `ArmadaConnector.task_type_name` (`connector.py`). The Flyte backend routes it to the
   connector service.

3. `create` (`connector.py`). The connector reads back `cfg` from `task_template.custom` via
   `json_format.MessageToDict` (`connector.py`), defaulting `queue` to `"flyte"` and
   `job_set_id` to `"flyte-dag"` (`connector.py`). It requires `container.image` or raises
   `ValueError` (`connector.py`). This container is Flyte's rendered `a0` entrypoint. It
   wraps the container into a pod (`_pod_from_flyte_container`, `connector.py`): one
   container named `armada-task`, the image and `command` taken verbatim from Flyte's rendered
   container (the connector does not synthesize the `a0` command), `args` passed through
   `_runtime_args` (see below), `imagePullPolicy="IfNotPresent"` so a `kind load`ed image is used,
   and env built from the rendered env then overlaid with `_storage_env()` so the pod-reachable blob
   endpoint wins over any in-cluster endpoint the backend baked in (`connector.py`). The pod
   spec sets `terminationGracePeriodSeconds=0` and `restartPolicy="Never"` (`connector.py`).
   The connector builds a job request item with `priority`, `namespace`, the pod spec, a
   `flyte.org/connector: armada` label, and gang annotations (`connector.py`), then calls
   `submit_jobs(queue, job_set_id, [item])` through the `_call` wrapper (`connector.py`). If
   `resp.job_response_items[0].error` is set it raises `RuntimeError` (`connector.py`). It
   returns `ArmadaJobMetadata(job_id, job_set_id, queue, output_prefix=...)` (`connector.py`).

4. Armada schedules the pod. Armada queues the job, schedules it under fair-share, and its executor
   creates the pod on the Armada cluster.

5. `a0` runs. It reads typed inputs from the blob store, runs the user function, and writes typed
   `outputs.pb` (or `error.pb` on failure) to its per-action blob prefix. Flyte reads `outputs.pb`
   directly on success (`connector.py`).

6. `get` polls (`connector.py`). The connector calls
   `get_job_status([resource_meta.job_id])` through `_call` (`connector.py`), looks up the
   state (defaulting to `UNKNOWN` if absent, `connector.py`), and maps it to a Flyte phase via
   `_ARMADA_STATE_TO_PHASE`, defaulting to `RUNNING` (`connector.py`). On a terminal
   `SUCCEEDED`/`FAILED` it checks `a0`'s error file (see the handshake section) and can override the
   result to `FAILED` (`connector.py`). Otherwise it returns the mapped phase with a message
   naming the Armada state (`connector.py`).

7. Terminal. Once the phase is terminal, the framework stops polling. On `SUCCEEDED` Flyte reads the
   typed output from the blob store. `delete` (`connector.py`) cancels the job via
   `cancel_jobs(queue, job_set_id, job_id)` using the persisted metadata (all three fields needed).

## State mapping

The connector maps Armada `JobState` onto Flyte's `TaskExecution.Phase` (`connector.py`):

| Armada JobState                 | Flyte phase        | Note                                     |
|---------------------------------|--------------------|------------------------------------------|
| `QUEUED`, `SUBMITTED`, `LEASED` | `QUEUED`           |                                          |
| `PENDING`                       | `INITIALIZING`     |                                          |
| `RUNNING`                       | `RUNNING`          |                                          |
| `UNKNOWN`                       | `RUNNING`          | transient, keep polling                  |
| `SUCCEEDED`                     | `SUCCEEDED`        |                                          |
| `FAILED`, `REJECTED`            | `FAILED`           |                                          |
| `CANCELLED`                     | `ABORTED`          |                                          |
| `PREEMPTED`                     | `RETRYABLE_FAILED` | preemption is expected, so Flyte retries |

Mapping `PREEMPTED` to `RETRYABLE_FAILED` is deliberate (`connector.py`). Armada preempts jobs as
part of normal fair-share scheduling, so the node should retry rather than fail the run. Any state
not in the table falls back to `RUNNING` (`connector.py`), which keeps the connector polling
rather than failing on an unrecognized state.

## The blob and a0 handshake

The task runs as a stock Flyte `a0` container. The connector does not supply the data plane. It only
makes sure the pod can reach the blob store and that `a0`'s arguments are complete.

### Storage env injection

`_storage_env` (`connector.py`) injects `FLYTE_AWS_ENDPOINT` / `FLYTE_AWS_ACCESS_KEY_ID` /
`FLYTE_AWS_SECRET_ACCESS_KEY` onto the pod, which `a0` reads via `flyte.storage.S3.auto()`. The name
split is deliberate: the connector reads its own `FLYTE_BLOB_*` env (via `ConnectorConfig`) but
injects `FLYTE_AWS_*`, exactly as FlytePropeller does for in-cluster task pods. These are applied
after the container's own env (`connector.py`) so the pod-reachable NodePort endpoint
overrides any in-cluster endpoint the backend baked into the container. If `blob_endpoint` is empty,
nothing is injected (`connector.py`).

### The terminal error read

`a0` can write `error.pb` while the pod still exits 0, so Armada, which only sees the pod exit, may
report the job `SUCCEEDED` even though the task failed (`connector.py`). So `get` reads the
error file on any terminal `SUCCEEDED`/`FAILED` and lets it decide (`connector.py`).
`_task_error` (`connector.py`) returns `None` if there is no output prefix, otherwise loads
the error via `flyte._internal.runtime.io.load_error` under `asyncio.wait_for(..., timeout=8)`
(`connector.py`) and returns `err.message or None`. A bare `except Exception` maps any
failure to `None` (`connector.py`). The 8-second bound matters because when the connector runs
as the `c0` service, Flyte's global storage may not point at the pods' blob store, and an unbounded
read would hang the poll loop and never report the job as terminal. On timeout it falls back to the
Armada-reported phase (and on the backend, FlytePropeller reads `error.pb` itself anyway).

## Why the connector fills the runtime args

In backend (webapi/connector) execution there is no FlytePropeller to fill the runtime template
placeholders that Flyte leaves in `a0`'s args, so the connector does that in `_runtime_args`
(`connector.py`). Every substitution is conditional, so it is a no-op on already-complete
local args (`connector.py`). It substitutes `{{.runName}}` and `{{.actionName}}` from the
task execution metadata (`connector.py`), pulling org from `meta.labels.get("organization")`.
Because `a0` requires it, it appends `--run-base-dir` if absent, computed from the output prefix by
stripping the trailing `/<run>/<action>/<attempt>` via `rsplit("/", 2)[0]` (`connector.py`).
It appends `--org` / `--project` / `--domain` if missing (`connector.py`).

`_outputs_path` (`connector.py`) scans the args for the `--outputs-path` value, the
per-action blob prefix `a0` writes `outputs.pb` / `error.pb` to. This is distinct from the base
`output_prefix` passed to `create`. It is stored on `ArmadaJobMetadata.output_prefix`
(`connector.py`) so `get` can read `error.pb` from the right place.

## Gang scheduling

A gang is a set of tasks Armada schedules all-or-nothing. `armada_flyte.Gang` (`gang.py`) is the
authoring surface: add members with `Gang.add`, then `await Gang.run()` submits them together. The
cardinality Armada needs stamped on each member is the number of members added, and the gang id is
generated per `run()`, so neither is set by hand and they cannot disagree with the fan-out.

Armada drives gangs off pod annotations: `armadaproject.io/gangId`, `armadaproject.io/gangCardinality`,
and `armadaproject.io/gangNodeUniformityLabel`. The connector sets these in `create`
(`_gang_annotations_from_env`). The count is only known at the driver's fan-out, and Flyte hands the
connector one task at a time with no group size in its `TaskExecutionMetadata`, so `Gang` carries the
values down through a channel the connector does receive: per-member container env vars, set with
`task.override(env_vars=...)`.

Those transport vars use the `ARMADAFLYTE_GANG_` prefix (`gang.py`), deliberately off Armada's reserved
`ARMADA_` namespace. Armada injects its own `ARMADA_GANG_ID` / `ARMADA_GANG_CARDINALITY` /
`ARMADA_GANG_NODE_UNIFORMITY_LABEL_{NAME,VALUE}` into each gang pod at runtime (from the annotations),
and its executor only adds them if absent, so a colliding name would silently shadow them. The connector
reads the transport vars to build the annotations and strips them from the submitted pod
(`_pod_from_flyte_container`), leaving Armada's runtime env vars authoritative for the application.

A task author could hand-stamp the `ARMADAFLYTE_GANG_*` env vars to forge gang membership rather than
go through `Gang`. The blast radius is bounded by Armada's queue permissions: you can only gang jobs you
are allowed to submit to a queue, so forging only groups your own jobs. The connector rejects a
non-integer cardinality with a clear error rather than crashing.

Migrating from the earlier API: `ArmadaConfig` no longer has `gang_id` / `gang_cardinality` /
`gang_node_uniformity_label`. Drop them from the config and express the gang with `Gang` instead, which
derives the id and cardinality from the members. Passing the removed keyword raises a `TypeError`.

## How it plugs into Flyte 2

Flyte 2 ships the webapi/connector plugin code, and as of
[flyteorg/flyte#7565](https://github.com/flyteorg/flyte/pull/7565) its executor registers the plugin
and supplies the no-op `ResourceManager`, `ResourceRegistrar`, and `SecretManager` the plugin
machinery needs. Any stock `flyte-binary-v2` build from that commit on routes `armada` tasks to the
connector. Earlier builds do not (see `docs/getting-started.md`).

### The webapi connector plugin

`flyteplugins/go/tasks/plugins/webapi/connector/plugin.go` is a single remote plugin with ID
`"connector-service"` (`plugin.go:34`). `RegisterConnectorPlugin` gob-registers `ResourceMetaWrapper`
and `ResourceWrapper` and registers the remote plugin (`plugin.go:496-500`).

- `Create` (`plugin.go:124-225`) reads the task template, renders the container args, resolves the
  connector for the task category, resolves any `Connection` secrets via `taskCtx.SecretManager().Get`
  (`plugin.go:170-184`, which nil-derefs without a SecretManager), calls `CreateTask` on the async
  connector client, and returns a `ResourceMetaWrapper` carrying the opaque `ConnectorResourceMeta`
  bytes plus `OutputPrefix`, `TaskCategory`, `Connection`, `Project`, and `Domain`
  (`plugin.go:217-224`). That wrapper is gob-registered so Flyte can persist it.
- `Get` (`plugin.go:227-261`) casts `ResourceMeta` back to the wrapper, re-resolves the connector,
  and calls `GetTask`, returning a `ResourceWrapper` with the phase, outputs, and message.
- `Delete` (`plugin.go:263-290`) round-trips the resource meta and calls `DeleteTask`.
- `Status` (`plugin.go:317+`) maps the phase, and on `SUCCEEDED` writes the connector's returned
  outputs to the output path via `writeOutput`.
- `IsTerminal` (`plugin.go:93-96`) stops polling once the phase is `SUCCEEDED`, `FAILED`,
  `RETRYABLE_FAILED`, or `ABORTED`.

The `resource_meta` persistence is what makes the connector stateless. Flyte serializes the opaque
`ConnectorResourceMeta` (the Python `ArmadaJobMetadata`) and re-supplies it on every `Get`/`Delete`,
so the connector never has to remember anything about a job between calls.

### The executor registration

`executor/setup.go` registers the plugin and supplies the managers stock v2 omits.

- `connectorplugin.RegisterConnectorPlugin(&connectorplugin.ConnectorService{})` (`setup.go:73`),
  which must run before the plugin registry is snapshotted.
- The setup context is built with `plugin.NewNoopSecretManager()` and
  `plugin.NewNoopResourceRegistrar()` (`setup.go:137-141`).
- The reconciler is given `plugin.NewNoopResourceManager()` (`setup.go:189`) and
  `plugin.NewNoopSecretManager()` (`setup.go:191-193`).

The no-op managers live in `executor/pkg/plugin`. `noopResourceManager.AllocateResource` always
returns `AllocationStatusGranted` (`resource_manager.go:32-34`), matching FlytePropeller with no
quota backend. `noopSecretManager.Get` fails loudly with an error rather than handing a task a blank
value (`secret_manager.go:19-21`). Both files carry comments noting that the v2 executor never
reimplemented what FlytePropeller v1 provided (`resource_manager.go:9-14`,
`secret_manager.go:10-14`).

### The poll loop

On the Go side, `monitor` (`flyteplugins/go/tasks/pluginmachinery/internal/webapi/monitor.go:14-45`)
drives an autorefresh cache keyed per task-execution `GeneratedName` and re-invokes the plugin's
`Get`/`GetTask` per task on the cache resync interval. Terminal items are queued for delayed
deletion (`monitor.go:63-70`). On the Python side a deployed backend polls `get` roughly every 3
seconds per task (`connector.py`). The net effect is one `GetJobStatus` gRPC per in-flight task
per resync tick, so polling is O(N). `GetJobStatus` is bulk-capable and the
connector keeps no per-job state in the object, so a future in-memory status cache keyed by `job_id`
(filled by one bulk poll or the job-set event stream) could collapse that to O(1) without changing
the Flyte contract.

## Running the connector

The connector runs as a gRPC service (`c0`), pointed at Armada via `ARMADA_URL` and at the blob
store via `FLYTE_BLOB_ENDPOINT` / `FLYTE_BLOB_ACCESS_KEY` / `FLYTE_BLOB_SECRET_KEY`. Runs appear in
the Flyte UI. See `docs/getting-started.md` and `deploy/README.md` for running it.

## Fast-register and the task image

The task image is a generic runtime: `python:3.13-slim` plus `flyte` and `armada_flyte`. The user's
own code is fast-registered and pulled by `a0` at runtime, so one image serves any example. The image
must be available to the Armada cluster, loaded with `kind load` or pushed to a registry it can pull from.
