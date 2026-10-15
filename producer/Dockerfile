# Build context is the repo root (see compose.yml) so the image contains the `lab` package and its schemas.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app

# Versions match uv.lock for confluent-kafka and fastavro.
COPY producer/requirements.txt .
RUN pip install -r requirements.txt

COPY lab ./lab

# Exec form so the Python process is PID 1 and receives SIGTERM from `docker stop`.
# Compose supplies the arguments (see the `producer` service).
ENTRYPOINT ["python", "-m", "lab.produce"]
