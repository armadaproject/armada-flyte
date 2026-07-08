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
touches the data plane and never synthesizes an output (`connector.py:362-364`).

The pieces (`docs/getting-started.md`):

- Flyte 2 runs as `flyte-binary`: the control plane, the UI, and the `TaskAction` reconciler.
- Armada's control plane submits and schedules jobs. Its executor creates the job pods on a
  Kubernetes cluster.
- The connector runs as a host process that bridges the two.

Where these run is up to you. The blob store is shared, so Flyte and the Armada pods address the
same bucket.

## Components

### The connector service (c0)

The connector runs as an out-of-process gRPC service launched as `c0`, and the Flyte backend calls
`CreateTask` / `GetTask` / `DeleteTask` on it. It holds no per-job state in the object itself,
caching only the gRPC channel and its resolved config (`connector.py:145-161`). All per-job state
lives in `ArmadaJobMetadata`, which Flyte persists and hands back on every call.

### `ArmadaFunctionTask` and `ArmadaConfig` (authoring surface)

`src/armada_flyte/task.py` is what a workflow author touches.

- `ArmadaConfig` (`task.py:19-38`) is a dataclass of per-task Armada submission knobs: `queue`
  (default `"flyte"`), `job_set_id` (default `"flyte-dag"`), `namespace` (default `"default"`),
  `priority` (default `1`), optional `cpu`/`memory` overrides (`None` means defer to
  `flyte.Resources`), and the gang triple `gang_id` / `gang_cardinality` / `gang_node_uniformity_label`.
  It is deliberately minimal. Resources are normally declared the stock-Flyte way, and `ArmadaConfig`
  carries only the Armada-specific fields.
- `ArmadaFunctionTask(AsyncConnectorExecutorMixin, AsyncFunctionTaskTemplate)` (`task.py:41-64`) is
  the task type. `_TASK_TYPE = "armada"` (`task.py:57`), and `__post_init__` sets `self.task_type`
  to it (`task.py:59-61`). That string routes the task to the connector. `custom_config()`
  returns `asdict(self.plugin_config)` (`task.py:63-64`), which Flyte serializes into the task
  template's `custom` field.
- `TaskPluginRegistry.register(ArmadaConfig, ArmadaFunctionTask)` (`task.py:69`) wires the two
  together. Any `TaskEnvironment(plugin_config=ArmadaConfig(...))` builds Armada-typed tasks.

`ArmadaConfig` is serialized and persisted in the control plane, which is why credentials never go
here (see next section).

### `ConnectorConfig` (connector-side config, distinct from `ArmadaConfig`)

`src/armada_flyte/config.py` holds the connector's own settings.

- `ConnectorConfig` (`config.py:23-36`) is a frozen dataclass: `armada_url` (default
  `"localhost:50051"`), and `blob_endpoint` / `blob_access_key` / `blob_secret_key` (default empty).
  It is marked as the extension point for `auth_token` / `tls` / `ca_cert` (`config.py:34-36`).
- Credentials live here, on the connector, never in `ArmadaConfig` (`config.py:4-6`). `ArmadaConfig`
  is serialized into the task template and persisted, so a token there would leak into the control
  plane DB.
- Resolution has three layers (`config.py:8-13`): dataclass defaults, then environment
  (`from_env` reads `ARMADA_URL` and `FLYTE_BLOB_ENDPOINT` / `FLYTE_BLOB_ACCESS_KEY` /
  `FLYTE_BLOB_SECRET_KEY`, `config.py:38-46`), then in-code `configure(**kwargs)` which validates
  keys and stashes them in a module-level `_overrides` (`config.py:53-73`). `resolve_config()`
  composes them: `from_env()` then `replace(cfg, **_overrides)` (`config.py:76-79`).
- Resolution is lazy, on first connector use (`connector.py:148-152`). There is no
  set-env-before-import trap, but `configure()` must run before the first task, because the client
  is built once and cached.

### The blob store

A single shared S3-compatible bucket is addressed by the backend, the client, and the Armada pods
alike. The connector injects the pod-reachable blob endpoint onto each job
(`connector.py:_storage_env`). The `a0` container reads inputs and writes `outputs.pb` there, and
Flyte reads `outputs.pb` directly.

## End-to-end data flow for one task

1. Author. A user writes `@env.task` in a `TaskEnvironment(plugin_config=ArmadaConfig(...))`. Flyte
   renders the task into a container whose entrypoint is `a0`, and serializes `ArmadaConfig` into
   the task template's `custom` field.

2. Route. The task's `task_type` is `"armada"` (`task.py:57`), which matches
   `ArmadaConnector.task_type_name` (`connector.py:138`). The Flyte backend routes it to the
   connector service.

3. `create` (`connector.py:280-330`). The connector reads back `cfg` from `task_template.custom` via
   `json_format.MessageToDict` (`connector.py:288`), defaulting `queue` to `"flyte"` and
   `job_set_id` to `"flyte-dag"` (`connector.py:289-290`). It requires `container.image` or raises
   `ValueError` (`connector.py:292-296`). This container is Flyte's rendered `a0` entrypoint. It
   wraps the container into a pod (`_pod_from_flyte_container`, `connector.py:252-278`): one
   container named `armada-task`, the image and `command` taken verbatim from Flyte's rendered
   container (the connector does not synthesize the `a0` command), `args` passed through
   `_runtime_args` (see below), `imagePullPolicy="IfNotPresent"` so a `kind load`ed image is used,
   and env built from the rendered env then overlaid with `_storage_env()` so the pod-reachable blob
   endpoint wins over any in-cluster endpoint the backend baked in (`connector.py:256-262`). The pod
   spec sets `terminationGracePeriodSeconds=0` and `restartPolicy="Never"` (`connector.py:274-278`).
   The connector builds a job request item with `priority`, `namespace`, the pod spec, a
   `flyte.org/connector: armada` label, and gang annotations (`connector.py:305-311`), then calls
   `submit_jobs(queue, job_set_id, [item])` through the `_call` wrapper (`connector.py:312-318`). If
   `resp.job_response_items[0].error` is set it raises `RuntimeError` (`connector.py:319-321`). It
   returns `ArmadaJobMetadata(job_id, job_set_id, queue, output_prefix=...)` (`connector.py:325-330`).

4. Armada schedules the pod. Armada queues the job, schedules it under fair-share, and its executor
   creates the pod on the Armada cluster.

5. `a0` runs. It reads typed inputs from the blob store, runs the user function, and writes typed
   `outputs.pb` (or `error.pb` on failure) to its per-action blob prefix. Flyte reads `outputs.pb`
   directly on success (`connector.py:362-364`).

6. `get` polls (`connector.py:356-372`). The connector calls
   `get_job_status([resource_meta.job_id])` through `_call` (`connector.py:357-359`), looks up the
   state (defaulting to `UNKNOWN` if absent, `connector.py:360`), and maps it to a Flyte phase via
   `_ARMADA_STATE_TO_PHASE`, defaulting to `RUNNING` (`connector.py:361`). On a terminal
   `SUCCEEDED`/`FAILED` it checks `a0`'s error file (see the handshake section) and can override the
   result to `FAILED` (`connector.py:365-368`). Otherwise it returns the mapped phase with a message
   naming the Armada state (`connector.py:369-372`).

7. Terminal. Once the phase is terminal, the framework stops polling. On `SUCCEEDED` Flyte reads the
   typed output from the blob store. `delete` (`connector.py:374-382`) cancels the job via
   `cancel_jobs(queue, job_set_id, job_id)` using the persisted metadata (all three fields needed).

## State mapping

The connector maps Armada `JobState` onto Flyte's `TaskExecution.Phase` (`connector.py:48-60`):

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

Mapping `PREEMPTED` to `RETRYABLE_FAILED` is deliberate (`connector.py:47`). Armada preempts jobs as
part of normal fair-share scheduling, so the node should retry rather than fail the run. Any state
not in the table falls back to `RUNNING` (`connector.py:361`), which keeps the connector polling
rather than failing on an unrecognized state.

## The blob and a0 handshake

The task runs as a stock Flyte `a0` container. The connector does not supply the data plane. It only
makes sure the pod can reach the blob store and that `a0`'s arguments are complete.

### Storage env injection

`_storage_env` (`connector.py:183-196`) injects `FLYTE_AWS_ENDPOINT` / `FLYTE_AWS_ACCESS_KEY_ID` /
`FLYTE_AWS_SECRET_ACCESS_KEY` onto the pod, which `a0` reads via `flyte.storage.S3.auto()`. The name
split is deliberate: the connector reads its own `FLYTE_BLOB_*` env (via `ConnectorConfig`) but
injects `FLYTE_AWS_*`, exactly as FlytePropeller does for in-cluster task pods. These are applied
after the container's own env (`connector.py:260-262`) so the pod-reachable NodePort endpoint
overrides any in-cluster endpoint the backend baked into the container. If `blob_endpoint` is empty,
nothing is injected (`connector.py:190-191`).

### The terminal error read

`a0` can write `error.pb` while the pod still exits 0, so Armada, which only sees the pod exit, may
report the job `SUCCEEDED` even though the task failed (`connector.py:336-339`). So `get` reads the
error file on any terminal `SUCCEEDED`/`FAILED` and lets it decide (`connector.py:365-368`).
`_task_error` (`connector.py:332-354`) returns `None` if there is no output prefix, otherwise loads
the error via `flyte._internal.runtime.io.load_error` under `asyncio.wait_for(..., timeout=8)`
(`connector.py:349-351`) and returns `err.message or None`. A bare `except Exception` maps any
failure to `None` (`connector.py:353`). The 8-second bound matters because when the connector runs
as the `c0` service, Flyte's global storage may not point at the pods' blob store, and an unbounded
read would hang the poll loop and never report the job as terminal. On timeout it falls back to the
Armada-reported phase (and on the backend, FlytePropeller reads `error.pb` itself anyway).

## Why the connector fills the runtime args

In backend (webapi/connector) execution there is no FlytePropeller to fill the runtime template
placeholders that Flyte leaves in `a0`'s args, so the connector does that in `_runtime_args`
(`connector.py:217-250`). Every substitution is conditional, so it is a no-op on already-complete
local args (`connector.py:224-225`). It substitutes `{{.runName}}` and `{{.actionName}}` from the
task execution metadata (`connector.py:227-242`), pulling org from `meta.labels.get("organization")`.
Because `a0` requires it, it appends `--run-base-dir` if absent, computed from the output prefix by
stripping the trailing `/<run>/<action>/<attempt>` via `rsplit("/", 2)[0]` (`connector.py:243-245`).
It appends `--org` / `--project` / `--domain` if missing (`connector.py:246-249`).

`_outputs_path` (`connector.py:206-215`) scans the args for the `--outputs-path` value, the
per-action blob prefix `a0` writes `outputs.pb` / `error.pb` to. This is distinct from the base
`output_prefix` passed to `create`. It is stored on `ArmadaJobMetadata.output_prefix`
(`connector.py:329`) so `get` can read `error.pb` from the right place.

## Gang scheduling

`ArmadaConfig` exposes `gang_id`, `gang_cardinality`, and `gang_node_uniformity_label`
(`task.py:36-38`). `_gang_annotations` (`connector.py:69-82`) translates them into Armada gang
annotations `armadaproject.io/gangId` and `armadaproject.io/gangCardinality`, plus
`armadaproject.io/gangNodeUniformityLabel` when set (`connector.py:64-66`, `75-81`). A job becomes a
gang member only when `gang_id` is set and `gang_cardinality` is 2 or more. Otherwise it is an
ordinary job with no gang annotations (`connector.py:72-74`). Jobs sharing a gang are scheduled
all-or-nothing together.

The `gang_id` is scoped per run. In `create` it is rewritten to `f"{gang_id}-{run_name}"`
(`connector.py:301-303`), where `run_name` comes from
`task_execution_id.node_execution_id.execution_id.name` (`_run_name`, `connector.py:198-204`). So
each run forms its own gang and concurrent runs do not collide, while every gang member within a run
still shares the same id.

## How it plugs into Flyte 2

Stock Flyte 2 ships the webapi/connector plugin code but does not register it, and its executor does
not supply the `ResourceManager`, `ResourceRegistrar`, or `SecretManager` the plugin machinery
needs. The `dejanzele/flyte` fork's `armada` branch closes that gap. Its published
`dpejcev/flyte-binary-v2:armada` image carries the registration (`docs/getting-started.md`, upstream
issue flyteorg/flyte#7564, PR #7565).

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
seconds per task (`connector.py:9-10`). The net effect is one `GetJobStatus` gRPC per in-flight task
per resync tick, so polling is O(N). `GetJobStatus` is bulk-capable and the
connector keeps no per-job state in the object, so a future in-memory status cache keyed by `job_id`
(filled by one bulk poll or the job-set event stream) could collapse that to O(1) without changing
the Flyte contract.

## Running the connector

The connector runs as a gRPC service (`c0`), pointed at Armada via `ARMADA_URL` and at the blob
store via `FLYTE_BLOB_ENDPOINT` / `FLYTE_BLOB_ACCESS_KEY` / `FLYTE_BLOB_SECRET_KEY`. Runs appear in
the Flyte UI. See `docs/getting-started.md` and `deploy/README.md` for running it.

## Fast-register and the task image

The task image is a generic runtime: `python:3.11-slim` plus `flyte` and `armada_flyte`. The user's
own code is fast-registered and pulled by `a0` at runtime, so one image serves any example. The image
must be available to the Armada cluster, loaded with `kind load` or pushed to a registry it can pull from.
