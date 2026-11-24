# Dex conversation Worker image for the memory experiment.
# Build context: the repository's dex/ directory, for example
#   docker build -t dvt-dex-worker -f bench/dex/Dockerfile dex
FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DEX_DEMO_RUN_DIR=/data

# The same version that dex/uv.lock pins.
RUN pip install --no-cache-dir dex-python-sdk==1.45.17

WORKDIR /app
COPY worker.py worker_metrics.py ./
COPY support_conversation ./support_conversation
COPY flow-definitions ./flow-definitions
# The BlobCache directory lives on the container's writable layer, not tmpfs.
RUN mkdir -p /data

# The Worker stops gracefully on SIGTERM, the Docker default stop signal.
# Exec form keeps the Worker as PID 1, so an OOM kill stops the container.
CMD ["python", "worker.py"]
