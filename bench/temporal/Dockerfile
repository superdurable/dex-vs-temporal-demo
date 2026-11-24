# Temporal conversation worker image for the memory experiment.
# Build context: the repository's temporal/ directory, for example
#   docker build -t dvt-temporal-worker -f bench/temporal/Dockerfile temporal
FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir temporalio==1.36.1

WORKDIR /app
# worker.py imports every workflow variant and the External Storage driver.
COPY *.py ./

# SIGINT runs the worker's graceful shutdown, which releases its sticky queue.
STOPSIGNAL SIGINT
# Exec form keeps the worker as PID 1, so an OOM kill stops the container.
CMD ["python", "worker.py"]
