"""ConnectorConfig resolution: defaults, environment, and in-code configure() precedence."""

from __future__ import annotations

import pytest

import armada_flyte
from armada_flyte import ConnectorConfig
from armada_flyte.config import resolve_config
from armada_flyte.connector import ArmadaConnector

_ENV_KEYS = ("ARMADA_URL", "FLYTE_BLOB_ENDPOINT", "FLYTE_BLOB_ACCESS_KEY", "FLYTE_BLOB_SECRET_KEY")


def test_defaults():
    cfg = ConnectorConfig()
    assert cfg.armada_url == "localhost:50051"
    assert cfg.blob_endpoint == "" and cfg.blob_access_key == "" and cfg.blob_secret_key == ""


def test_from_env_reads_environment(monkeypatch):
    monkeypatch.setenv("ARMADA_URL", "armada.example.com:50051")
    monkeypatch.setenv("FLYTE_BLOB_ENDPOINT", "http://minio:9000")
    monkeypatch.setenv("FLYTE_BLOB_ACCESS_KEY", "key")
    monkeypatch.setenv("FLYTE_BLOB_SECRET_KEY", "secret")
    cfg = ConnectorConfig.from_env()
    assert cfg.armada_url == "armada.example.com:50051"
    assert cfg.blob_endpoint == "http://minio:9000"
    assert cfg.blob_access_key == "key" and cfg.blob_secret_key == "secret"


def test_from_env_falls_back_to_defaults(monkeypatch):
    for k in _ENV_KEYS:
        monkeypatch.delenv(k, raising=False)
    cfg = ConnectorConfig.from_env()
    assert cfg.armada_url == "localhost:50051"
    assert cfg.blob_endpoint == ""


def test_configure_overrides_env(monkeypatch):
    # Precedence: in-code configure() wins over the environment.
    monkeypatch.setenv("ARMADA_URL", "from-env:50051")
    armada_flyte.configure(armada_url="from-code:50051")
    assert resolve_config().armada_url == "from-code:50051"


def test_configure_unknown_setting_raises():
    with pytest.raises(TypeError, match="Unknown connector setting"):
        armada_flyte.configure(armadaurl="typo:50051")


def test_connector_picks_up_configure_lazily(monkeypatch):
    # A connector built with config=None resolves lazily, so configure() before first use applies.
    monkeypatch.delenv("ARMADA_URL", raising=False)
    armada_flyte.configure(armada_url="lazy.example.com:50051")
    assert ArmadaConnector().config.armada_url == "lazy.example.com:50051"


def test_storage_env_from_config():
    c = ArmadaConnector(
        ConnectorConfig(blob_endpoint="http://minio:9000", blob_access_key="k", blob_secret_key="s")
    )
    names = {e.name: e.value for e in c._storage_env()}
    assert names == {
        "FLYTE_AWS_ENDPOINT": "http://minio:9000",
        "FLYTE_AWS_ACCESS_KEY_ID": "k",
        "FLYTE_AWS_SECRET_ACCESS_KEY": "s",
    }


def test_storage_env_empty_without_blob():
    assert ArmadaConnector(ConnectorConfig())._storage_env() == []
