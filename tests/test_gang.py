"""Gang scheduling: the connector translates a member's ARMADAFLYTE_GANG_* env vars into Armada gang
annotations and strips the transport from the pod; the Gang helper derives the cardinality and id."""

from __future__ import annotations

import pathlib
from types import SimpleNamespace

import flyte
import pytest
from flyte._internal.runtime.task_serde import translate_task_to_wire
from flyte.models import SerializationContext

from armada_flyte import ArmadaConfig
from armada_flyte.connector import (
    _GANG_CARDINALITY_ANNOTATION,
    _GANG_ID_ANNOTATION,
    _GANG_NODE_UNIFORMITY_ANNOTATION,
    _gang_annotations_from_env,
)
from armada_flyte.gang import (
    _GANG_ENV_PREFIX,
    GANG_CARDINALITY_ENV,
    GANG_ID_ENV,
    GANG_NODE_UNIFORMITY_ENV,
    Gang,
)


def _container(env_pairs, key_attr="name"):
    """A stand-in for the rendered container, whose env entries expose .name/.value (or .key/.value,
    the KeyValuePair shape the connector also handles)."""
    env = [SimpleNamespace(**{key_attr: k, "value": v}) for k, v in env_pairs.items()]
    return SimpleNamespace(env=env)


# --- connector: env vars -> annotations -------------------------------------------------------------

def test_no_gang_without_transport_env():
    assert _gang_annotations_from_env(SimpleNamespace(env=[])) == {}
    assert _gang_annotations_from_env(SimpleNamespace(env=None)) == {}


def test_cardinality_below_two_is_not_a_gang():
    c = _container({GANG_ID_ENV: "g", GANG_CARDINALITY_ENV: "1"})
    assert _gang_annotations_from_env(c) == {}


def test_gang_annotations_from_env():
    c = _container({
        GANG_ID_ENV: "abc",
        GANG_CARDINALITY_ENV: "3",
        GANG_NODE_UNIFORMITY_ENV: "kubernetes.io/hostname",
    })
    ann = _gang_annotations_from_env(c)
    assert ann[_GANG_ID_ANNOTATION] == "abc"
    assert ann[_GANG_CARDINALITY_ANNOTATION] == "3"
    assert ann[_GANG_NODE_UNIFORMITY_ANNOTATION] == "kubernetes.io/hostname"


def test_uniformity_label_is_optional():
    c = _container({GANG_ID_ENV: "abc", GANG_CARDINALITY_ENV: "2"})
    ann = _gang_annotations_from_env(c)
    assert _GANG_NODE_UNIFORMITY_ANNOTATION not in ann


def test_reads_keyvaluepair_shape():
    # Flyte serialises container env as KeyValuePair (.key/.value), not .name/.value.
    c = _container({GANG_ID_ENV: "abc", GANG_CARDINALITY_ENV: "2"}, key_attr="key")
    assert _gang_annotations_from_env(c)[_GANG_ID_ANNOTATION] == "abc"


# --- connector.create: annotations set, transport stripped from the pod -----------------------------

async def test_create_sets_annotations_and_strips_transport(connector, mock_client, make_custom):
    mock_client.submit_jobs.return_value = SimpleNamespace(
        job_response_items=[SimpleNamespace(job_id="01job", error="")]
    )
    container = SimpleNamespace(
        image="img", command=[], args=["a0"],
        env=[SimpleNamespace(name=k, value=v) for k, v in {
            "REGULAR": "keep",
            GANG_ID_ENV: "gid123",
            GANG_CARDINALITY_ENV: "4",
            GANG_NODE_UNIFORMITY_ENV: "kubernetes.io/hostname",
        }.items()],
        resources=SimpleNamespace(requests=[], limits=[]),
    )
    tt = SimpleNamespace(custom=make_custom(), container=container)
    await connector.create(tt, inputs={}, task_execution_metadata=None)

    kwargs = mock_client.create_job_request_item.call_args.kwargs
    ann = kwargs["annotations"]
    assert ann[_GANG_ID_ANNOTATION] == "gid123"
    assert ann[_GANG_CARDINALITY_ANNOTATION] == "4"
    assert ann[_GANG_NODE_UNIFORMITY_ANNOTATION] == "kubernetes.io/hostname"

    pod_env = {e.name for e in kwargs["pod_spec"].containers[0].env}
    assert not any(n.startswith(_GANG_ENV_PREFIX) for n in pod_env), "transport must be stripped"
    assert "REGULAR" in pod_env, "non-transport env must survive"


# --- the Gang helper --------------------------------------------------------------------------------

class _FakeTask:
    """Captures the env_vars a Gang stamps via override(), and records its call args."""

    name = "worker"
    task_type = "armada"  # Gang.add() requires Armada tasks

    def __init__(self, env_vars=None):
        self.env_vars = env_vars or {}
        self._stamped = None

    def override(self, *, env_vars):
        t = _FakeTask()
        t._stamped = env_vars
        return t

    def __call__(self, *args, **kwargs):
        stamped = self._stamped

        async def _run():
            return {"env": stamped, "args": args, "kwargs": kwargs}

        return _run()


async def test_run_derives_cardinality_and_shares_one_id():
    gang = Gang(node_uniformity_label="kubernetes.io/hostname")
    for rank in range(3):
        gang.add(_FakeTask(), rank=rank)
    envs = [m["env"] for m in await gang.run()]
    assert all(e[GANG_CARDINALITY_ENV] == "3" for e in envs)
    assert len({e[GANG_ID_ENV] for e in envs}) == 1
    assert all(e[GANG_NODE_UNIFORMITY_ENV] == "kubernetes.io/hostname" for e in envs)


async def test_run_merges_existing_env_vars():
    # override(env_vars=...) replaces rather than merges, so the task's own env must be carried forward.
    gang = Gang()
    gang.add(_FakeTask({"PRESET": "keep"}), rank=0)
    gang.add(_FakeTask({"PRESET": "keep"}), rank=1)
    envs = [m["env"] for m in await gang.run()]
    assert all(e["PRESET"] == "keep" and GANG_ID_ENV in e for e in envs)


async def test_run_requires_at_least_two_members():
    with pytest.raises(ValueError):
        await Gang().run()
    solo = Gang()
    solo.add(_FakeTask(), rank=0)
    with pytest.raises(ValueError):
        await solo.run()


async def test_each_run_gets_a_fresh_gang_id():
    def two_member_gang():
        g = Gang()
        g.add(_FakeTask(), rank=0)
        g.add(_FakeTask(), rank=1)
        return g

    a = (await two_member_gang().run())[0]["env"][GANG_ID_ENV]
    b = (await two_member_gang().run())[0]["env"][GANG_ID_ENV]
    assert a != b


async def test_map_zips_items_and_passes_shared_kwargs():
    members = await Gang.map(_FakeTask(), range(3), node_uniformity_label="L", extra="x")
    assert [m["args"] for m in members] == [(0,), (1,), (2,)]
    assert all(m["kwargs"].get("extra") == "x" for m in members)
    assert all(m["env"][GANG_CARDINALITY_ENV] == "3" for m in members)
    assert all(m["env"][GANG_NODE_UNIFORMITY_ENV] == "L" for m in members)


def test_add_rejects_non_armada_tasks():
    # A non-Armada member never reaches the connector, so the gang would deadlock waiting for it.
    class _PlainTask:
        name = "plain"
        task_type = "python"

    with pytest.raises(ValueError, match="not an Armada task"):
        Gang().add(_PlainTask())


def test_transport_prefix_stays_off_armadas_reserved_namespace():
    # The transport vars must NOT collide with Armada's own ARMADA_GANG_* runtime injection, or the
    # executor's add-if-absent would silently shadow it. This invariant is the whole point of the prefix.
    for var in (GANG_ID_ENV, GANG_CARDINALITY_ENV, GANG_NODE_UNIFORMITY_ENV):
        assert var.startswith("ARMADAFLYTE_GANG_")
        assert not var.startswith("ARMADA_")  # the whole ARMADA_ namespace is Armada's


# --- integration: the env-var transport survives flyte's REAL serialization ------------------------
# The unit tests above fake both sides of the transport. These reach through flyte's own serde to pin
# the three SDK facts the design rests on: TaskTemplate.env_vars exists, override() REPLACES env rather
# than merging (why Gang carries the task's env forward itself), and env renders as KeyValuePair. If a
# flyte release changes any of them, gangs would silently degrade to independent jobs. This fails loudly.
_gang_env = flyte.TaskEnvironment(
    "gang-itest",
    image="armada-flyte-task:v1",
    resources=flyte.Resources(cpu=1, memory="256Mi"),
    plugin_config=ArmadaConfig(queue="flyte"),
    env_vars={"PRESET": "keep"},
)


@_gang_env.task
async def _member(rank: int) -> int:
    return rank


def _serialize(task):
    # root_dir must contain the task's module. This test file lives directly under it.
    sc = SerializationContext(version="itest", root_dir=pathlib.Path(__file__).parent)
    return translate_task_to_wire(task, sc, task_context=None).task_template


def test_override_replaces_env_not_merges():
    # The fact Gang works around by merging the task's own env into the stamped vars itself.
    assert _member.override(env_vars={"ONLY": "x"}).env_vars == {"ONLY": "x"}


async def test_gang_transport_survives_real_serialization(connector, mock_client):
    # Tag the member the way Gang.run does, serialize through flyte's real serde, feed to create().
    merged = {
        "PRESET": "keep",
        GANG_ID_ENV: "gid42",
        GANG_CARDINALITY_ENV: "3",
        GANG_NODE_UNIFORMITY_ENV: "kubernetes.io/hostname",
    }
    tt = _serialize(_member.override(env_vars=merged))

    # flyte renders container env as KeyValuePair (.key/.value) — the shape the connector reads.
    rendered = {e.key: e.value for e in tt.container.env}
    assert rendered[GANG_ID_ENV] == "gid42" and rendered["PRESET"] == "keep"

    mock_client.submit_jobs.return_value = SimpleNamespace(
        job_response_items=[SimpleNamespace(job_id="01", error="")]
    )
    await connector.create(tt, inputs={}, task_execution_metadata=None)

    kwargs = mock_client.create_job_request_item.call_args.kwargs
    assert kwargs["annotations"][_GANG_ID_ANNOTATION] == "gid42"
    assert kwargs["annotations"][_GANG_CARDINALITY_ANNOTATION] == "3"
    pod_env = {e.name for e in kwargs["pod_spec"].containers[0].env}
    assert not any(n.startswith("ARMADAFLYTE_GANG_") for n in pod_env)  # transport stripped from the pod
    assert "PRESET" in pod_env  # the task's own env survives
