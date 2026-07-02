"""The Armada-to-Flyte phase mapping, and that get() applies it correctly."""

from __future__ import annotations

import pytest
from armada_client.armada import submit_pb2
from flyteidl2.core.execution_pb2 import TaskExecution

from armada_flyte.connector import _ARMADA_STATE_TO_PHASE, ArmadaJobMetadata

EXPECTED = {
    submit_pb2.QUEUED: TaskExecution.QUEUED,
    submit_pb2.SUBMITTED: TaskExecution.QUEUED,
    submit_pb2.LEASED: TaskExecution.QUEUED,
    submit_pb2.PENDING: TaskExecution.INITIALIZING,
    submit_pb2.RUNNING: TaskExecution.RUNNING,
    submit_pb2.UNKNOWN: TaskExecution.RUNNING,
    submit_pb2.SUCCEEDED: TaskExecution.SUCCEEDED,
    submit_pb2.FAILED: TaskExecution.FAILED,
    submit_pb2.REJECTED: TaskExecution.FAILED,
    submit_pb2.CANCELLED: TaskExecution.ABORTED,
    submit_pb2.PREEMPTED: TaskExecution.RETRYABLE_FAILED,
}


@pytest.mark.parametrize("state, phase", EXPECTED.items())
def test_mapping(state, phase):
    assert _ARMADA_STATE_TO_PHASE[state] == phase


def _meta() -> ArmadaJobMetadata:
    return ArmadaJobMetadata(job_id="01job", job_set_id="flyte-dag", queue="flyte")


async def test_get_maps_state(connector, mock_client):
    mock_client.get_job_status.return_value.job_states = {"01job": submit_pb2.RUNNING}
    resource = await connector.get(_meta())
    assert resource.phase == TaskExecution.RUNNING
    assert resource.outputs is None


async def test_get_succeeded_does_not_synthesise_output(connector, mock_client):
    # a0 writes the task's real typed output to the output location; the connector returns no
    # synthesised output, so Flyte reads the real one from that location.
    mock_client.get_job_status.return_value.job_states = {"01job": submit_pb2.SUCCEEDED}
    resource = await connector.get(_meta())
    assert resource.phase == TaskExecution.SUCCEEDED
    assert resource.outputs is None


async def test_get_unknown_when_job_absent(connector, mock_client):
    # A job id the status map does not contain falls back to UNKNOWN, which maps to RUNNING.
    mock_client.get_job_status.return_value.job_states = {}
    resource = await connector.get(_meta())
    assert resource.phase == TaskExecution.RUNNING


def _meta_with_output() -> ArmadaJobMetadata:
    return ArmadaJobMetadata(job_id="01job", job_set_id="flyte-dag", queue="flyte", output_prefix="s3://b/out")


async def test_get_surfaces_task_error_on_terminal(connector, mock_client, monkeypatch):
    # a0 wrote error.pb: even though Armada reports the job SUCCEEDED (the pod exited 0), get()
    # reports FAILED with the task's real reason, not a misleading missing-output error.
    import flyte._internal.runtime.io as flyte_io
    from flyteidl2.core import execution_pb2

    async def fake_load_error(path):
        return execution_pb2.ExecutionError(message="ValueError: boom\n  ...traceback...")

    monkeypatch.setattr(flyte_io, "load_error", fake_load_error)
    mock_client.get_job_status.return_value.job_states = {"01job": submit_pb2.SUCCEEDED}
    resource = await connector.get(_meta_with_output())
    assert resource.phase == TaskExecution.FAILED
    assert "boom" in resource.message


async def test_get_succeeded_when_no_error_file(connector, mock_client, monkeypatch):
    # No error.pb (the read raises): a genuine success stays SUCCEEDED.
    import flyte._internal.runtime.io as flyte_io

    async def fake_load_error(path):
        raise FileNotFoundError(path)

    monkeypatch.setattr(flyte_io, "load_error", fake_load_error)
    mock_client.get_job_status.return_value.job_states = {"01job": submit_pb2.SUCCEEDED}
    resource = await connector.get(_meta_with_output())
    assert resource.phase == TaskExecution.SUCCEEDED
