"""Flyte 2 to Armada connector.

Import this package to register the Armada connector with Flyte's ConnectorRegistry, then set
``plugin_config=ArmadaConfig(...)`` on a ``TaskEnvironment`` so its ``@env.task`` functions run on
Armada.
"""

# Must be first: aliases armada_client's vendored google.api to the standard one before
# any armada_client import, avoiding a duplicate google/api/http.proto registration.
import armada_flyte._proto_compat  # noqa: F401,E402

from armada_flyte.config import ConnectorConfig, configure
from armada_flyte.connector import ArmadaConnector, ArmadaJobMetadata
from armada_flyte.task import ArmadaConfig, ArmadaFunctionTask

__all__ = [
    "ArmadaConnector",
    "ArmadaJobMetadata",
    "ArmadaConfig",
    "ArmadaFunctionTask",
    "ConnectorConfig",
    "configure",
]
