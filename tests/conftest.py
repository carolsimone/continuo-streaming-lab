import pytest

ENDPOINT_VARS = (
    "KAFKA_1_PORT",
    "KAFKA_2_PORT",
    "KAFKA_3_PORT",
    "SCHEMA_REGISTRY_PORT",
    "MINIO_API_PORT",
)


@pytest.fixture(autouse=True)
def _clean_endpoint_env(monkeypatch):
    """Defaults must not depend on the developer's shell or .env."""
    for name in ENDPOINT_VARS:
        monkeypatch.delenv(name, raising=False)
