"""Connector configuration: where Armada is and how the connector reaches it.

This is the *connector's* config (endpoint, blob store, and later auth/TLS), distinct from
:class:`armada_flyte.task.ArmadaConfig`, which is *per-task* submission config. Credentials belong
here, on the connector, never in ``ArmadaConfig``: task config is serialised into the task template
and persisted in the control plane, so a token there would leak.

Resolution precedence, lowest to highest::

    dataclass defaults  <  environment (from_env)  <  in-code overrides (configure)

Settings resolve lazily, on first connector use, so :func:`configure` works after
``import armada_flyte`` and there is no "set the env var before importing" ordering trap.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, fields, replace
from typing import Any


@dataclass(frozen=True)
class ConnectorConfig:
    """Immutable connector settings. Build from the environment with :meth:`from_env`, or override
    fields in code via :func:`configure`."""

    # Where the connector submits and polls jobs (the Armada submit/status gRPC endpoint).
    armada_url: str = "localhost:50051"
    # Blob store the Armada pods read inputs from and write outputs to (injected as FLYTE_AWS_*).
    blob_endpoint: str = ""
    blob_access_key: str = ""
    blob_secret_key: str = ""
    # Extension point: add auth_token / tls / ca_cert / timeouts here, then wire them in the one
    # place connector.py builds the gRPC channel. Keep credentials on this object, never in
    # ArmadaConfig (which is serialised into the task template and persisted in the control plane).

    @classmethod
    def from_env(cls) -> "ConnectorConfig":
        """Read settings from the environment, falling back to the dataclass defaults."""
        return cls(
            armada_url=os.environ.get("ARMADA_URL", "localhost:50051"),
            blob_endpoint=os.environ.get("FLYTE_BLOB_ENDPOINT", ""),
            blob_access_key=os.environ.get("FLYTE_BLOB_ACCESS_KEY", ""),
            blob_secret_key=os.environ.get("FLYTE_BLOB_SECRET_KEY", ""),
        )


# In-code overrides set via configure(); applied on top of from_env() when the config resolves.
_overrides: dict = {}


def configure(**kwargs: Any) -> None:
    """Set connector settings from code, overriding the environment.

    Call this before the first task runs (the connector resolves its config lazily, on first use,
    so a later call would not be picked up by an already-built client). For local execution, call it
    in your run script; for the backend, call it in the connector service launcher (the data
    scientist's ``@env.task`` file should not hold the platform's Armada credentials).

    Example::

        import armada_flyte
        armada_flyte.configure(armada_url="armada.example.com:50051")
    """
    valid = {f.name for f in fields(ConnectorConfig)}
    unknown = set(kwargs) - valid
    if unknown:
        raise TypeError(
            f"Unknown connector setting(s): {', '.join(sorted(unknown))}. "
            f"Valid settings: {', '.join(sorted(valid))}."
        )
    _overrides.update(kwargs)


def resolve_config() -> ConnectorConfig:
    """The effective config: environment defaults with any :func:`configure` overrides on top."""
    cfg = ConnectorConfig.from_env()
    return replace(cfg, **_overrides) if _overrides else cfg
