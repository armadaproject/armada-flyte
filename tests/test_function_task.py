"""Real Python @env.task: plugin_config routing and connector container-wrapping."""

from __future__ import annotations

from types import SimpleNamespace

import flyte
from flyteidl2.core import tasks_pb2 as ftp

from armada_flyte import ArmadaConfig, ArmadaFunctionTask
from armada_flyte.connector import ArmadaConnector


def _rendered_container(reqs=None, limits=None, args=("a0", "--inputs", "s3://b/i.pb")):
    """A stand-in for Flyte's rendered a0 container, with optional declared resources."""
    def entries(d):
        return [SimpleNamespace(name=k, value=v) for k, v in (d or {}).items()]

    return SimpleNamespace(
        image="task-img:latest", command=[], args=list(args), env=[],
        resources=SimpleNamespace(requests=entries(reqs), limits=entries(limits)),
    )


def _backend_meta():
    # Mirrors the TaskExecutionMetadata a deployed backend passes to create().
    return SimpleNamespace(
        task_execution_id=SimpleNamespace(
            task_id=SimpleNamespace(project="flytesnacks", domain="development"),
            node_execution_id=SimpleNamespace(
                node_id="a0", execution_id=SimpleNamespace(name="run123")
            ),
        ),
        labels={"organization": "acme"},
    )


def test_runtime_args_fills_backend_placeholders():
    # The backend leaves {{.runName}}/{{.actionName}} unfilled and omits --run-base-dir/--org.
    prefix = "s3://b/sh/flytesnacks/development/run123/a0/1"
    args = ["a0", "--run-name", "{{.runName}}", "--name", "{{.actionName}}"]
    out = ArmadaConnector._runtime_args(args, prefix, _backend_meta())
    assert "{{.runName}}" not in out and "run123" in out
    assert "{{.actionName}}" not in out and "a0" in out
    assert out[out.index("--run-base-dir") + 1] == "s3://b/sh/flytesnacks/development/run123"
    assert out[out.index("--org") + 1] == "acme"
    assert out[out.index("--project") + 1] == "flytesnacks"


def test_runtime_args_noop_when_already_complete():
    # Local execution renders complete args; nothing to add (no placeholders, run-base-dir present).
    args = ["a0", "--run-base-dir", "s3://b/base", "--org", "o", "--inputs", "s3://b/i.pb"]
    assert ArmadaConnector._runtime_args(list(args), "", None) == args


def test_run_name_extraction():
    # The run name (shared by every action in a run) scopes the gang id so runs do not collide.
    assert ArmadaConnector._run_name(_backend_meta()) == "run123"
    assert ArmadaConnector._run_name(None) == ""


def test_plugin_config_builds_a_function_task():
    env = flyte.TaskEnvironment("e", plugin_config=ArmadaConfig(queue="q"))

    @env.task
    async def f(x: int) -> int:
        return x * x

    assert isinstance(f, ArmadaFunctionTask)
    assert f.task_type == "armada"


async def test_create_wraps_the_flyte_container(connector, mock_client):
    # A real function task carries a rendered container (the a0 entrypoint). The connector must
    # submit THAT, not the placeholder workload.
    mock_client.submit_jobs.return_value = SimpleNamespace(
        job_response_items=[SimpleNamespace(job_id="01job", error="")]
    )
    container = _rendered_container(reqs={ftp.Resources.CPU: "1", ftp.Resources.MEMORY: "1Gi"})
    task_template = SimpleNamespace(custom=None, container=container)

    await connector.create(task_template, inputs={})

    pod = mock_client.create_job_request_item.call_args.kwargs["pod_spec"]
    assert pod.containers[0].image == "task-img:latest"
    assert list(pod.containers[0].args) == ["a0", "--inputs", "s3://b/i.pb"]


async def test_native_resources_are_mapped(connector, mock_client):
    # @env.task(resources=...) declares cpu/memory/gpu on the rendered container; the connector
    # must honour them (the old code silently dropped them and produced a 1-CPU pod).
    mock_client.submit_jobs.return_value = SimpleNamespace(
        job_response_items=[SimpleNamespace(job_id="01job", error="")]
    )
    container = _rendered_container(
        reqs={ftp.Resources.CPU: "4", ftp.Resources.MEMORY: "8Gi", ftp.Resources.GPU: "2"}
    )
    await connector.create(SimpleNamespace(custom=None, container=container), inputs={})

    req = mock_client.create_job_request_item.call_args.kwargs["pod_spec"].containers[0].resources.requests
    assert req["cpu"].string == "4"
    assert req["memory"].string == "8Gi"
    assert req["nvidia.com/gpu"].string == "2"


async def test_armada_config_resources_override_native(connector, mock_client, make_custom):
    # ArmadaConfig cpu/memory is the escape hatch: it wins over the rendered container's resources.
    mock_client.submit_jobs.return_value = SimpleNamespace(
        job_response_items=[SimpleNamespace(job_id="01job", error="")]
    )
    container = _rendered_container(reqs={ftp.Resources.CPU: "4", ftp.Resources.MEMORY: "8Gi"})
    tt = SimpleNamespace(custom=make_custom(cpu="500m", memory="512Mi"), container=container)
    await connector.create(tt, inputs={})

    req = mock_client.create_job_request_item.call_args.kwargs["pod_spec"].containers[0].resources.requests
    assert req["cpu"].string == "500m"
    assert req["memory"].string == "512Mi"


async def test_function_task_requires_resources(connector, mock_client):
    # No ArmadaConfig cpu/memory and no declared resources on the container -> clear error.
    container = _rendered_container(reqs={})
    try:
        await connector.create(SimpleNamespace(custom=None, container=container), inputs={})
        assert False, "expected a resources-required error"
    except ValueError as e:
        assert "resource" in str(e).lower()
