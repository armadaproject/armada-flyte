"""Gang annotations: only emitted for a real gang, with the right Armada keys, run-scoped id."""

from __future__ import annotations

from types import SimpleNamespace

from armada_flyte.connector import (
    _GANG_CARDINALITY_ANNOTATION,
    _GANG_ID_ANNOTATION,
    _gang_annotations,
)


def test_no_gang_by_default():
    assert _gang_annotations({}) == {}
    assert _gang_annotations({"gang_id": None, "gang_cardinality": 0}) == {}


def test_cardinality_below_two_is_not_a_gang():
    # A gang of one is not a gang; do not annotate it.
    assert _gang_annotations({"gang_id": "g", "gang_cardinality": 1}) == {}


def test_gang_annotations():
    ann = _gang_annotations({"gang_id": "workers", "gang_cardinality": 3})
    assert ann[_GANG_ID_ANNOTATION] == "workers"
    assert ann[_GANG_CARDINALITY_ANNOTATION] == "3"


def _meta(run_name: str):
    return SimpleNamespace(
        task_execution_id=SimpleNamespace(
            task_id=SimpleNamespace(project="p", domain="d"),
            node_execution_id=SimpleNamespace(
                node_id="a0", execution_id=SimpleNamespace(name=run_name)
            ),
        ),
        labels={},
    )


async def _submit_gang(connector, mock_client, make_custom, run_name):
    mock_client.submit_jobs.return_value = SimpleNamespace(
        job_response_items=[SimpleNamespace(job_id="01job", error="")]
    )
    container = SimpleNamespace(
        image="img", command=[], args=["a0"], env=[],
        resources=SimpleNamespace(requests=[], limits=[]),
    )
    tt = SimpleNamespace(custom=make_custom(gang_id="calc", gang_cardinality=3), container=container)
    await connector.create(tt, inputs={}, task_execution_metadata=_meta(run_name))
    return mock_client.create_job_request_item.call_args.kwargs["annotations"]


async def test_gang_id_is_run_scoped(connector, mock_client, make_custom):
    # The connector scopes the gang id to the run so concurrent runs do not collide.
    ann = await _submit_gang(connector, mock_client, make_custom, "run111")
    assert ann[_GANG_ID_ANNOTATION] == "calc-run111"
    assert ann[_GANG_CARDINALITY_ANNOTATION] == "3"


async def test_gang_ids_differ_across_runs(connector, mock_client, make_custom):
    a = await _submit_gang(connector, mock_client, make_custom, "runAAA")
    b = await _submit_gang(connector, mock_client, make_custom, "runBBB")
    assert a[_GANG_ID_ANNOTATION] != b[_GANG_ID_ANNOTATION]
