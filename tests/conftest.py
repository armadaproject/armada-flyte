"""Shared pytest fixtures for armada-flyte unit tests.

None of these tests talk to a live Armada. The ArmadaConnector reaches the cluster
only through its lazily-built ``client`` property, which returns ``self._client`` when
that attribute is already set. Setting ``connector._client`` to a MagicMock therefore
short-circuits the gRPC channel entirely (the ``client`` property never constructs one).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from google.protobuf import json_format, struct_pb2

# Imported for its proto-pool side effect before any armada_client import happens.
import armada_flyte._proto_compat  # noqa: F401


@pytest.fixture
def make_custom():
    """Factory for a task_template.custom Struct (a serialised ArmadaConfig). Defaults to a small
    cpu/memory because the connector requires resources (Armada rejects jobs without them)."""

    def _make(cpu: str = "100m", memory: str = "128Mi", **fields) -> struct_pb2.Struct:
        s = struct_pb2.Struct()
        json_format.ParseDict({"cpu": cpu, "memory": memory, **fields}, s)
        return s

    return _make


from armada_flyte import ConnectorConfig
from armada_flyte.connector import ArmadaConnector


@pytest.fixture(autouse=True)
def _reset_connector_overrides():
    """Isolate configure() overrides between tests (they live in module-level global state)."""
    from armada_flyte import config as _config

    _config._overrides.clear()
    yield
    _config._overrides.clear()


@pytest.fixture
def mock_client() -> MagicMock:
    """A bare MagicMock standing in for ArmadaClient."""
    return MagicMock()


@pytest.fixture
def connector(mock_client: MagicMock) -> ArmadaConnector:
    """An ArmadaConnector whose gRPC client is replaced with a MagicMock.

    Because ``_client`` is pre-set, the ``client`` property short-circuits and never
    opens a socket, so the connector's create/get/delete logic runs offline.
    """
    c = ArmadaConnector(ConnectorConfig())
    c._client = mock_client
    return c
