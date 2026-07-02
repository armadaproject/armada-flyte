"""A Flyte 2 AsyncConnector that submits each task as an Armada job.

The connector implements the three-method Flyte v2 connector contract:

    create   submits an Armada job (Submit.SubmitJobs) and returns a job handle
    get      polls job status (Jobs.GetJobStatus) and maps it to a Flyte phase
    delete   cancels the job (Submit.CancelJobs)

A deployed Flyte backend drives this loop: it calls ``create`` once, then polls ``get`` every 3s
until a terminal phase.

A task is a Flyte 2 ``@env.task`` function. Flyte renders it into a container (its ``a0``
entrypoint); the connector wraps that container into an Armada job, so the function body runs in
the pod with its inputs and outputs flowing through the configured blob store.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Dict, Optional

import grpc

# Must run before any armada_client import (aliases vendored google.api to standard).
import armada_flyte._proto_compat  # noqa: F401
from armada_flyte.config import ConnectorConfig, resolve_config
from armada_client.armada import submit_pb2
from armada_client.client import ArmadaClient
from armada_client.k8s.io.api.core.v1 import generated_pb2 as core_v1
from armada_client.k8s.io.apimachinery.pkg.api.resource import generated_pb2 as api_resource
from flyte import logger
from flyte.connectors import AsyncConnector, ConnectorRegistry, Resource, ResourceMeta
from flyteidl2.core import tasks_pb2 as flyte_tasks_pb2
from flyteidl2.core.execution_pb2 import TaskExecution
from google.protobuf import json_format

# Flyte's ResourceName enum (from @env.task(resources=...)) to the k8s resource key.
_RESOURCE_NAME_TO_K8S = {
    flyte_tasks_pb2.Resources.CPU: "cpu",
    flyte_tasks_pb2.Resources.MEMORY: "memory",
    flyte_tasks_pb2.Resources.GPU: "nvidia.com/gpu",
    flyte_tasks_pb2.Resources.EPHEMERAL_STORAGE: "ephemeral-storage",
}

# Armada JobState (pkg/api/submit.proto:97) -> Flyte TaskExecution.Phase.
# PREEMPTED -> RETRYABLE_FAILED so Flyte retries jobs Armada preempted (expected, not a failure).
_ARMADA_STATE_TO_PHASE: Dict[int, "TaskExecution.Phase"] = {
    submit_pb2.QUEUED: TaskExecution.QUEUED,
    submit_pb2.SUBMITTED: TaskExecution.QUEUED,
    submit_pb2.LEASED: TaskExecution.QUEUED,
    submit_pb2.PENDING: TaskExecution.INITIALIZING,
    submit_pb2.RUNNING: TaskExecution.RUNNING,
    submit_pb2.UNKNOWN: TaskExecution.RUNNING,  # transient; keep polling
    submit_pb2.SUCCEEDED: TaskExecution.SUCCEEDED,
    submit_pb2.FAILED: TaskExecution.FAILED,
    submit_pb2.REJECTED: TaskExecution.FAILED,
    submit_pb2.CANCELLED: TaskExecution.ABORTED,
    submit_pb2.PREEMPTED: TaskExecution.RETRYABLE_FAILED,
}

# Armada gang scheduling (internal/common/constants/constants.go). Jobs sharing a gangId, all
# declaring the same gangCardinality, are scheduled all-or-nothing together.
_GANG_ID_ANNOTATION = "armadaproject.io/gangId"
_GANG_CARDINALITY_ANNOTATION = "armadaproject.io/gangCardinality"
_GANG_NODE_UNIFORMITY_ANNOTATION = "armadaproject.io/gangNodeUniformityLabel"


def _gang_annotations(cfg: Dict[str, Any]) -> Dict[str, str]:
    """Translate gang_* config into Armada gang annotations, or {} for a non-gang job."""
    gang_id = cfg.get("gang_id")
    cardinality = int(cfg.get("gang_cardinality") or 0)
    if not gang_id or cardinality < 2:
        return {}
    annotations = {
        _GANG_ID_ANNOTATION: gang_id,
        _GANG_CARDINALITY_ANNOTATION: str(cardinality),
    }
    label = cfg.get("gang_node_uniformity_label")
    if label:
        annotations[_GANG_NODE_UNIFORMITY_ANNOTATION] = label
    return annotations


@dataclass
class ArmadaJobMetadata(ResourceMeta):
    """Handle Flyte persists between create() and get()/delete()."""

    job_id: str
    job_set_id: str
    queue: str
    # The blob location a0 writes outputs/errors to; get() reads error.pb from here on a terminal job.
    output_prefix: str = ""


def _quantity(value: str) -> api_resource.Quantity:
    return api_resource.Quantity(string=value)


def _resolve_resources(cfg: Dict[str, Any], container) -> core_v1.ResourceRequirements:
    """Resolve the pod's resources, REQUIRING cpu and memory. Armada rejects a job with no
    resources, so we fail fast with a clear error instead of guessing a default.

    Precedence: an explicit ArmadaConfig cpu/memory overrides; otherwise the resources declared
    via @env.task(resources=...) on the rendered container (cpu/memory/gpu/ephemeral-storage) are
    used.
    """

    def to_map(entries):
        out = {}
        for e in entries:
            key = _RESOURCE_NAME_TO_K8S.get(e.name)
            if key and e.value:
                out[key] = _quantity(e.value)
        return out

    native = getattr(container, "resources", None)
    requests = to_map(native.requests) if native else {}
    limits = to_map(native.limits) if native else {}
    for k, v in limits.items():  # a limits-only declaration mirrors back into requests
        requests.setdefault(k, v)
    for key, val in (("cpu", cfg.get("cpu")), ("memory", cfg.get("memory"))):
        if val:
            requests[key] = limits[key] = _quantity(val)
    missing = [k for k in ("cpu", "memory") if k not in requests]
    if missing:
        raise ValueError(
            f"Armada requires resource requests for {', '.join(missing)}. Declare them with "
            "@env.task(resources=...) or ArmadaConfig(cpu=..., memory=...)."
        )
    for k, v in requests.items():  # mirror requests into limits (guaranteed QoS)
        limits.setdefault(k, v)
    return core_v1.ResourceRequirements(requests=requests, limits=limits)


class ArmadaConnector(AsyncConnector):
    name = "Armada Connector"
    task_type_name = "armada"
    metadata_type = ArmadaJobMetadata

    def __init__(self, config: Optional[ConnectorConfig] = None):
        # None resolves lazily (env + configure() overrides) on first use, so configure() works
        # after import and there is no set-env-before-import trap. Pass a config to pin it (a
        # connector service launcher, or tests).
        self._config = config
        self._client: Optional[ArmadaClient] = None

    @property
    def config(self) -> ConnectorConfig:
        if self._config is None:
            self._config = resolve_config()
        return self._config

    @property
    def client(self) -> ArmadaClient:
        # Lazily build the channel so importing the module never opens a socket. This is the one
        # place to add auth/TLS later: swap insecure_channel for a secure channel built from
        # self.config (token credentials / CA cert).
        if self._client is None:
            self._client = ArmadaClient(grpc.insecure_channel(self.config.armada_url))
        return self._client

    def _call(self, what: str, fn, *args, **kwargs):
        """Run an Armada RPC, turning a connection failure into an actionable error.

        The raw gRPC ``UNAVAILABLE`` is opaque to users, and it is the symptom of the most common
        setup mistake (wrong endpoint, cluster down, or a configure() call that landed too late).
        We name the endpoint we tried and how to change it. Any other gRPC status (a real Armada
        error) surfaces unchanged.
        """
        try:
            return fn(*args, **kwargs)
        except grpc.RpcError as e:
            if e.code() != grpc.StatusCode.UNAVAILABLE:
                raise
            url = self.config.armada_url
            raise ConnectionError(
                f"Could not reach Armada at {url} while {what}. Check the cluster is up and the "
                f"endpoint is correct, then point the connector at it with ARMADA_URL=host:port or "
                f"armada_flyte.configure(armada_url='host:port')."
            ) from e

    def _storage_env(self) -> list:
        """Stamp the platform storage config onto the pod using Flyte's own FLYTE_AWS_* convention,
        exactly as FlytePropeller does for in-cluster task pods. The a0 runtime reads these via
        flyte.storage.S3.auto(). This is a self-hosted S3 store (MinIO/RustFS), not AWS cloud: the
        endpoint differs from the backend's because the Armada pod is on another cluster, but the
        credentials are the platform's. obstore allows plain HTTP by default, so no extra flags."""
        cfg = self.config
        if not cfg.blob_endpoint:
            return []
        return [
            core_v1.EnvVar(name="FLYTE_AWS_ENDPOINT", value=cfg.blob_endpoint),
            core_v1.EnvVar(name="FLYTE_AWS_ACCESS_KEY_ID", value=cfg.blob_access_key),
            core_v1.EnvVar(name="FLYTE_AWS_SECRET_ACCESS_KEY", value=cfg.blob_secret_key),
        ]

    @staticmethod
    def _run_name(meta) -> str:
        """The Flyte run name (shared by every action in a run), or "" if unavailable."""
        try:
            return meta.task_execution_id.node_execution_id.execution_id.name
        except AttributeError:
            return ""

    @staticmethod
    def _outputs_path(args) -> str:
        """The blob prefix a0 writes outputs.pb / error.pb to. Flyte renders it into the container's
        ``--outputs-path`` arg; the output_prefix passed to create() is only the base raw-data dir,
        not this per-action location, so we read it from the args (and store it for get() to use)."""
        args = list(args)
        for flag, value in zip(args, args[1:]):
            if flag == "--outputs-path":
                return value
        return ""

    @staticmethod
    def _runtime_args(args: list, output_prefix: str, meta) -> list:
        """Fill in the runtime arguments the backend leaves for the executor to substitute.

        In local execution the SDK renders these; in backend execution FlytePropeller normally
        does, but the connector (webapi) plugin does not, so the a0 args arrive with unfilled
        ``{{.runName}}``/``{{.actionName}}`` placeholders and no ``--run-base-dir`` (which a0
        requires). We fill them from the task execution metadata. Both are conditional, so this is
        a no-op on the already-complete local args.
        """
        run_name = action_name = project = domain = org = ""
        try:
            tid = meta.task_execution_id.task_id
            project, domain = tid.project, tid.domain
            ne = meta.task_execution_id.node_execution_id
            run_name = ne.execution_id.name
            action_name = ne.node_id
        except AttributeError:
            pass
        # labels is a protobuf map<string,string>; organization carries the org.
        try:
            org = meta.labels.get("organization", "")
        except (AttributeError, TypeError):
            pass
        subs = {"{{.runName}}": run_name, "{{.actionName}}": action_name}
        args = [subs.get(a, a) for a in args]
        # output_prefix ends in /<run>/<action>/<attempt>; the run base dir drops action+attempt.
        if "--run-base-dir" not in args and output_prefix:
            args = args + ["--run-base-dir", output_prefix.rsplit("/", 2)[0]]
        # a0 requires org (and project/domain) which the backend leaves for the executor to fill.
        for flag, val in (("--org", org), ("--project", project), ("--domain", domain)):
            if flag not in args and val:
                args = args + [flag, val]
        return args

    def _pod_from_flyte_container(self, c, cfg: Dict[str, Any], output_prefix: str = "", meta=None) -> core_v1.PodSpec:
        """Wrap Flyte's rendered task container (the a0 entrypoint) into an Armada pod, so the
        user's real function runs in the pod with inputs/outputs via the blob store."""
        # Build env from the rendered container, then let our blob-store env override any matching
        # keys (e.g. a backend may bake an in-cluster storage endpoint the Armada pods can't reach).
        env_by_name: Dict[str, str] = {}
        for e in c.env:
            env_by_name[getattr(e, "name", None) or getattr(e, "key", "")] = e.value
        for ev in self._storage_env():
            env_by_name[ev.name] = ev.value
        env = [core_v1.EnvVar(name=n, value=v) for n, v in env_by_name.items()]
        container = core_v1.Container(
            name="armada-task",
            image=c.image,
            command=list(c.command),
            args=self._runtime_args(list(c.args), output_prefix, meta),
            env=env,
            # Use the image already loaded into the cluster (e.g. via `kind load`).
            imagePullPolicy="IfNotPresent",
            resources=_resolve_resources(cfg, c),
        )
        # armada_client's k8s protos use camelCase field names.
        return core_v1.PodSpec(
            terminationGracePeriodSeconds=0,
            restartPolicy="Never",
            containers=[container],
        )

    async def create(
        self,
        task_template,
        output_prefix: str = "",
        inputs: Optional[Dict[str, Any]] = None,
        task_execution_metadata=None,
        **kwargs,
    ) -> ArmadaJobMetadata:
        cfg = json_format.MessageToDict(task_template.custom) if task_template.custom else {}
        queue = cfg.get("queue", "flyte")
        job_set_id = cfg.get("job_set_id", "flyte-dag")

        container = getattr(task_template, "container", None)
        if not (container and container.image):
            raise ValueError(
                "armada_flyte runs Flyte @env.task functions; this task has no rendered container."
            )
        # Run Flyte's rendered container (the a0 entrypoint) in the pod.
        pod = self._pod_from_flyte_container(container, cfg, output_prefix, task_execution_metadata)
        # Scope the gang_id to this run so each run forms its own gang (and concurrent runs do not
        # collide), while every gang member in the run still shares the same id.
        run_name = self._run_name(task_execution_metadata)
        if run_name and cfg.get("gang_id"):
            cfg["gang_id"] = f"{cfg['gang_id']}-{run_name}"
        annotations = _gang_annotations(cfg)
        item = self.client.create_job_request_item(
            priority=float(cfg.get("priority", 1)),
            namespace=cfg.get("namespace", "default"),
            pod_spec=pod,
            labels={"flyte.org/connector": "armada"},
            annotations=annotations,
        )
        resp = self._call(
            "submitting the job",
            self.client.submit_jobs,
            queue=queue,
            job_set_id=job_set_id,
            job_request_items=[item],
        )
        first = resp.job_response_items[0]
        if first.error:
            raise RuntimeError(f"Armada rejected job submission: {first.error}")

        gang = f" gang={annotations[_GANG_ID_ANNOTATION]}" if annotations else ""
        logger.info(f"[armada] submitted job {first.job_id} to queue={queue} job_set={job_set_id}{gang}")
        return ArmadaJobMetadata(
            job_id=first.job_id,
            job_set_id=job_set_id,
            queue=queue,
            output_prefix=self._outputs_path(container.args),
        )

    async def _task_error(self, output_prefix: str) -> Optional[str]:
        """The task's real failure (its message and traceback) from a0's error file, or None.

        a0 writes error.pb on a task error but the pod can still exit 0, so Armada (which only sees
        the pod exit) may report the job SUCCEEDED while the task actually failed. We read error.pb
        and let it decide, exactly as FlytePropeller does; otherwise the failure is invisible and the
        framework later mis-reports the missing output as a type error. Best-effort: a missing file
        or unreadable storage just means there is no task error to report.

        The read is hard-bounded by a timeout. When the connector runs as a service (c0), Flyte's
        global storage may not be pointed at the pods' blob store, and an unbounded read would hang
        the poll loop and never report the job as terminal. On timeout we fall back to the
        Armada-reported phase (and on the backend, FlytePropeller reads error.pb itself anyway).
        """
        if not output_prefix:
            return None
        try:
            from flyte._internal.runtime.io import error_path, load_error

            err = await asyncio.wait_for(load_error(error_path(output_prefix)), timeout=8)
            return err.message or None
        except Exception:  # noqa: BLE001 - timeout or any read failure: never block or worsen reporting
            return None

    async def get(self, resource_meta: ArmadaJobMetadata, **kwargs) -> Resource:
        states = self._call(
            "polling job status", self.client.get_job_status, [resource_meta.job_id]
        ).job_states
        state = states.get(resource_meta.job_id, submit_pb2.UNKNOWN)
        phase = _ARMADA_STATE_TO_PHASE.get(state, TaskExecution.RUNNING)
        # On a terminal job, a0's error file decides failure (see _task_error): the task can fail
        # while Armada reports the pod as succeeded. a0 writes the task's typed output to the output
        # location otherwise, which Flyte reads on success, so there is nothing to synthesise here.
        if phase in (TaskExecution.SUCCEEDED, TaskExecution.FAILED):
            message = await self._task_error(resource_meta.output_prefix)
            if message is not None:
                return Resource(phase=TaskExecution.FAILED, message=message)
        return Resource(
            phase=phase,
            message=f"armada job {resource_meta.job_id} state={submit_pb2.JobState.Name(state)}",
        )

    async def delete(self, resource_meta: ArmadaJobMetadata, **kwargs):
        self._call(
            "cancelling the job",
            self.client.cancel_jobs,
            queue=resource_meta.queue,
            job_set_id=resource_meta.job_set_id,
            job_id=resource_meta.job_id,
        )
        logger.info(f"[armada] cancelled job {resource_meta.job_id}")


# Register a default connector instance so local execution can find it by task_type.
ConnectorRegistry.register(ArmadaConnector())
