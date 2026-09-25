# syntax=docker/dockerfile:1
#
# Iceberg maintenance image — Spark local-mode engine for the analytics
# datalake's maintenance CronJob (compaction, manifest rewrite, snapshot
# expiry).
#
# Built and pushed by .github/workflows/deploy.yaml (job: build-image) to:
#   ghcr.io/daun-gatal/analytics/iceberg-maintenance
#
# The python3-ubuntu Spark variant ships PySpark + python3; the maintenance
# script itself is NOT baked in — it comes from ConfigMap maintenance-config
# so it can be tuned without an image rebuild.
#
ARG SPARK_IMAGE=apache/spark:4.0.4-scala2.13-java17-python3-ubuntu
FROM ${SPARK_IMAGE}

USER root

# Keep the two Iceberg versions in lockstep — the aws bundle and the spark
# runtime are released together. Bump both in one change.
ARG ICEBERG_VERSION=1.11.0
ARG MAVEN_REPO=https://repo1.maven.org/maven2

# Iceberg Spark 4.0 runtime: REST catalog client (with SigV4), procedures
# (rewrite_data_files / rewrite_manifests / expire_snapshots /
# remove_orphan_files) and the SparkCatalog implementation.
ADD --chmod=0644 ${MAVEN_REPO}/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/${ICEBERG_VERSION}/iceberg-spark-runtime-4.0_2.13-${ICEBERG_VERSION}.jar /opt/spark/jars/

# AWS SDK v2 bundle: S3FileIO with endpoint override + path-style access
# for RustFS (not real AWS).
ADD --chmod=0644 ${MAVEN_REPO}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar /opt/spark/jars/

# Back to the image's default unprivileged user.
USER 185
