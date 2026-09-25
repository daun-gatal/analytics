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
# boto3: required by pyiceberg's SigV4 adapter (boto3.Session for request
# signing) — NOT pulled in by the [s3fs] extra; without it the REST
# catalog cannot sign at all. Left unpinned so pip resolves the version
# matching the botocore already pinned by aiobotocore (s3fs).
# pyiceberg-core: native expression/transform evaluation (used by the
# overwrite-by-hour predicates); validated against 0.12.0.
ARG PYICEBERG_VERSION=0.12.0
RUN pip install --no-cache-dir \
      "pyiceberg[s3fs,pyarrow,pyiceberg-core]==${PYICEBERG_VERSION}" \
      boto3 \
      requests==2.32.4 \
      websockets==15.0.1

WORKDIR /opt/generator
COPY --chmod=0644 streaming.py batch.py ./

RUN useradd --uid 10001 --no-create-home generator
USER 10001

# Default: the streaming Deployment; the batch CronJob overrides the command.
CMD ["python", "/opt/generator/streaming.py"]
