"""ArmadaConfig defaults and the keys it serialises (which the connector reads back)."""

from __future__ import annotations

from dataclasses import asdict

from armada_flyte import ArmadaConfig


def test_config_defaults():
    config = ArmadaConfig()
    assert config.queue == "flyte"
    assert config.job_set_id == "flyte-dag"
    assert config.namespace == "default"
    assert config.priority == 1
    # cpu/memory default to None: resources come from flyte.Resources unless explicitly overridden.
    assert config.cpu is None and config.memory is None


def test_serialised_keys():
    # asdict(ArmadaConfig) is exactly what ArmadaFunctionTask.custom_config emits and the connector
    # reads back from the task template's custom. Gang scheduling is not here. It is expressed with
    # armada_flyte.Gang (see test_gang.py).
    custom = asdict(ArmadaConfig(queue="compute", priority=5))
    assert set(custom) == {
        "queue", "job_set_id", "namespace", "priority", "cpu", "memory",
    }
    assert custom["queue"] == "compute"
    assert custom["priority"] == 5
