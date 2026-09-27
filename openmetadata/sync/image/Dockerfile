# catalog-sync — the OpenMetadata reconciliation engine image.
#
# Deliberately small: the engine talks to the OM REST API (requests),
# lists the RustFS Iceberg REST catalog with SigV4 (botocore) and reads
# the lineage/connector contracts (yaml). No heavy OM SDK needed — the
# image stays tiny and the script ships via ConfigMap (like maintenance).
FROM python:3.12-slim

# Pinned deps; security: run as non-root
RUN pip install --no-cache-dir \
      "requests==2.32.3" \
      "urllib3==2.2.3" \
      "botocore==1.35.36" \
      "PyYAML==6.0.2"

RUN useradd -m -u 1000 catalogsync
USER 1000

WORKDIR /opt/catalog-sync
# the script is volume-mounted from the ConfigMap in production; a copy
# baked into the image makes local `docker run` testing possible.
COPY --chown=1000:1000 . /opt/catalog-sync/

CMD ["python", "/opt/catalog-sync/catalog-sync.py"]
