"""Host-side service addresses, derived from the same port variables compose.yml reads.

These are the addresses a process on the host (tests, `make batch`, scripts) uses to reach the
stack. Inside Compose the `producer` service passes its own in-network addresses
(kafka-1:19092,... and http://karapace:8081) as flags, so it never relies on these defaults.
"""

import os


def _port(name: str, default: str) -> str:
    """Env value, with an empty string treated as unset (as compose's `${VAR:-default}` does)."""
    return os.environ.get(name) or default


def bootstrap() -> str:
    ports = (_port("KAFKA_1_PORT", "9092"), _port("KAFKA_2_PORT", "9192"), _port("KAFKA_3_PORT", "9292"))
    return ",".join(f"localhost:{port}" for port in ports)


def registry_url() -> str:
    return f"http://localhost:{_port('SCHEMA_REGISTRY_PORT', '8081')}"


def minio_endpoint() -> str:
    return f"localhost:{_port('MINIO_API_PORT', '9000')}"
