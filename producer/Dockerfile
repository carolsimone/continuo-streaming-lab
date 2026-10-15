FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY producer.py .

# Exec form so the Python process is PID 1 and receives SIGTERM from `docker stop`.
ENTRYPOINT ["python", "producer.py"]
