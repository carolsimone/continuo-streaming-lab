from lab import endpoints


def test_defaults():
    assert endpoints.bootstrap() == "localhost:9092,localhost:9192,localhost:9292"
    assert endpoints.registry_url() == "http://localhost:8081"
    assert endpoints.minio_endpoint() == "localhost:9000"
    assert endpoints.catalog_url() == "http://localhost:8181"


def test_env_overrides_ports(monkeypatch):
    monkeypatch.setenv("KAFKA_1_PORT", "19092")
    monkeypatch.setenv("KAFKA_2_PORT", "19192")
    monkeypatch.setenv("KAFKA_3_PORT", "19292")
    monkeypatch.setenv("SCHEMA_REGISTRY_PORT", "18081")
    monkeypatch.setenv("MINIO_API_PORT", "19000")
    monkeypatch.setenv("ICEBERG_REST_PORT", "18181")
    assert endpoints.bootstrap() == "localhost:19092,localhost:19192,localhost:19292"
    assert endpoints.registry_url() == "http://localhost:18081"
    assert endpoints.minio_endpoint() == "localhost:19000"
    assert endpoints.catalog_url() == "http://localhost:18181"


def test_parse_args_defaults_follow_env(monkeypatch):
    from lab.produce import parse_args

    monkeypatch.setenv("SCHEMA_REGISTRY_PORT", "18081")
    monkeypatch.setenv("KAFKA_1_PORT", "19092")
    args = parse_args([])
    assert args.registry == "http://localhost:18081"
    assert args.bootstrap.startswith("localhost:19092,")


def test_empty_env_values_fall_back_to_defaults(monkeypatch):
    # `make` exports an empty string for a port that .env does not define; compose treats that as unset too.
    for name in ("KAFKA_1_PORT", "KAFKA_2_PORT", "KAFKA_3_PORT", "SCHEMA_REGISTRY_PORT", "MINIO_API_PORT", "ICEBERG_REST_PORT"):
        monkeypatch.setenv(name, "")
    assert endpoints.bootstrap() == "localhost:9092,localhost:9192,localhost:9292"
    assert endpoints.registry_url() == "http://localhost:8081"
    assert endpoints.minio_endpoint() == "localhost:9000"
    assert endpoints.catalog_url() == "http://localhost:8181"
