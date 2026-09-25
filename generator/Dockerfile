# syntax=docker/dockerfile:1
#
# Data generator image — streaming (Wikimedia EventStreams SSE + Bluesky
# Jetstream WebSocket) and batch (GH Archive + Wikimedia pageview dumps)
# writers for the analytics datalake on RustFS, via PyIceberg.
#
# Built and pushed by .github/workflows/deploy.yaml (job: build-image) to:
#   ghcr.io/daun-gatal/analytics/generator
#
# Unlike the maintenance image, the scripts ARE baked in — they are small
# and stable, and the CronJob/Deployment command selects the mode.
#
ARG PYTHON_IMAGE=python:3.12-slim
FROM ${PYTHON_IMAGE}

# Keep the ingest stack in lockstep; bump in one change.
# PyIceberg: REST catalog client (SigV4) + S3 FileIO + Arrow writes.
ARG PYICEBERG_VERSION=0.9.1
RUN pip install --no-cache-dir \
      "pyiceberg[s3fs,pyarrow]==${PYICEBERG_VERSION}" \
      requests==2.32.4 \
      websockets==15.0.1

WORKDIR /opt/generator
COPY --chmod=0644 streaming.py batch.py ./

RUN useradd --uid 10001 --no-create-home generator
USER 10001

# Default: the streaming Deployment; the batch CronJob overrides the command.
CMD ["python", "/opt/generator/streaming.py"]
