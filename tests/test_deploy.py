"""The connector is registered and discoverable, so it can run as a deployed service."""

from __future__ import annotations

from importlib.metadata import entry_points


def test_connector_registered_in_registry():
    import armada_flyte  # noqa: F401  (importing registers the connector)
    from flyte.connectors import ConnectorRegistry

    connector = ConnectorRegistry.get_connector("armada", 0)
    assert connector.task_type_name == "armada"
    assert connector.metadata_type.__name__ == "ArmadaJobMetadata"


def test_flyte_connectors_entry_point():
    # The entry point is what lets a deployed `c0` service load the connector with no --modules.
    eps = entry_points(group="flyte.connectors")
    assert {ep.name: ep.value for ep in eps}.get("armada") == "armada_flyte.connector"
