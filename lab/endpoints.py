"""Host-side service addresses, derived from the same port variables compose.yml reads.

These are the addresses a process on the host (tests, `make batch`, scripts) uses to reach the
stack. Inside Compose the `producer` service passes its own in-network addresses
(kafka-1:19092,... and http://karapace:8081) as flags, so it never relies on these defaults.
"""

import os


def bootstrap() -> str:
    ports = (
        os.environ.get("KAFKA_1_PORT", "9092"),
        os.environ.get("KAFKA_2_PORT", "9192"),
        os.environ.get("KAFKA_3_PORT", "9292"),
    )
    return ",".join(f"localhost:{port}" for port in ports)


def registry_url() -> str:
    return f"http://localhost:{os.environ.get('SCHEMA_REGISTRY_PORT', '8081')}"


def minio_endpoint() -> str:
    return f"localhost:{os.environ.get('MINIO_API_PORT', '9000')}"
