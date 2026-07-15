"""create() builds a job from the task config and returns a handle, without a live Armada."""

from __future__ import annotations

from types import SimpleNamespace

import grpc


class _RpcError(grpc.RpcError):
    """A stand-in gRPC error with a settable status code."""

    def __init__(self, code: grpc.StatusCode):
        self._code = code

    def code(self) -> grpc.StatusCode:
        return self._code

    def details(self) -> str:
        return "boom"


def _container():
    """A stand-in for Flyte's rendered a0 container (a function task always carries one)."""
    return SimpleNamespace(
        image="task-img:latest", command=[], args=["a0", "--outputs-path", "s3://b/out"], env=[],
        resources=SimpleNamespace(requests=[], limits=[]),
    )


async def test_create_submits_and_returns_handle(connector, mock_client, make_custom):
    mock_client.submit_jobs.return_value = SimpleNamespace(
        job_response_items=[SimpleNamespace(job_id="01created", error="")]
    )

    task_template = SimpleNamespace(custom=make_custom(), container=_container())
    meta = await connector.create(task_template, inputs={"name": "world"})

    assert meta.job_id == "01created"
    assert meta.queue == "flyte"
    assert meta.job_set_id == "flyte-dag"
    assert meta.output_prefix == "s3://b/out"  # extracted from the a0 --outputs-path arg

    # The job was submitted to the default queue. (Container-wrapping is asserted in test_function_task.)
    assert mock_client.submit_jobs.call_args.kwargs["queue"] == "flyte"


async def test_create_raises_on_armada_error(connector, mock_client, make_custom):
    mock_client.submit_jobs.return_value = SimpleNamespace(
        job_response_items=[SimpleNamespace(job_id="", error="queue does not exist")]
    )
    try:
        await connector.create(SimpleNamespace(custom=make_custom(), container=_container()), inputs={})
        assert False, "expected create() to raise on submission error"
    except RuntimeError as e:
        assert "queue does not exist" in str(e)


async def test_delete_cancels_job(connector, mock_client):
    from armada_flyte.connector import ArmadaJobMetadata

    await connector.delete(ArmadaJobMetadata(job_id="01job", job_set_id="flyte-dag", queue="flyte"))
    mock_client.cancel_jobs.assert_called_once()


async def test_unreachable_armada_raises_actionable_error(connector, mock_client, make_custom):
    # A connection failure (gRPC UNAVAILABLE) becomes an error that names the endpoint and the fix.
    mock_client.submit_jobs.side_effect = _RpcError(grpc.StatusCode.UNAVAILABLE)
    try:
        await connector.create(SimpleNamespace(custom=make_custom(), container=_container()), inputs={})
        assert False, "expected a ConnectionError"
    except ConnectionError as e:
        msg = str(e)
        assert "localhost:50051" in msg                       # the endpoint it tried
        assert "ARMADA_URL" in msg and "configure(" in msg     # how to point it elsewhere


async def test_other_rpc_errors_surface_unchanged(connector, mock_client, make_custom):
    # Anything other than UNAVAILABLE (a real Armada error) is not rewritten.
    mock_client.submit_jobs.side_effect = _RpcError(grpc.StatusCode.PERMISSION_DENIED)
    try:
        await connector.create(SimpleNamespace(custom=make_custom(), container=_container()), inputs={})
        assert False, "expected the original RpcError"
    except ConnectionError:
        assert False, "non-UNAVAILABLE errors must not be rewritten as ConnectionError"
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.PERMISSION_DENIED
