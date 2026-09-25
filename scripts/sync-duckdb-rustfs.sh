#!/usr/bin/env bash
#
# sync-duckdb-rustfs.sh — keep duckdb aligned with the *deployed* rustfs.
#
# Reads the live Service/rustfs-svc in the rustfs namespace, derives the
# connection facts, patches ConfigMap/duckdb-settings in analytics, and
# restarts deployment/duckdb so init.sql re-reads the new values.
#
# Idempotent: exits 0 without a restart when nothing changed.
#
# Env overrides (all optional):
#   ANALYTICS_NS     default analytics
#   RUSTFS_NS        default rustfs
#   CLUSTER_DOMAIN   default cluster.local
#   RUSTFS_SERVICE   default rustfs-svc
#   DUCKDB_SETTINGS  default duckdb-settings
#   DUCKDB_DEPLOY    default duckdb
#
set -euo pipefail

ANALYTICS_NS="${ANALYTICS_NS:-analytics}"
RUSTFS_NS="${RUSTFS_NS:-rustfs}"
CLUSTER_DOMAIN="${CLUSTER_DOMAIN:-cluster.local}"
RUSTFS_SERVICE="${RUSTFS_SERVICE:-rustfs-svc}"
DUCKDB_SETTINGS="${DUCKDB_SETTINGS:-duckdb-settings}"
DUCKDB_DEPLOY="${DUCKDB_DEPLOY:-duckdb}"

if ! kubectl get service "$RUSTFS_SERVICE" -n "$RUSTFS_NS" >/dev/null 2>&1; then
  echo "⚠️  Service/$RUSTFS_SERVICE not found in ns $RUSTFS_NS — nothing to align (deploy rustfs first)"
  exit 0
fi

if ! kubectl get configmap "$DUCKDB_SETTINGS" -n "$ANALYTICS_NS" >/dev/null 2>&1; then
  echo "⚠️  ConfigMap/$DUCKDB_SETTINGS not found in ns $ANALYTICS_NS — nothing to align (apply duckdb first)"
  exit 0
fi

#
# Derive facts from the deployed Service
#
SVC_NAME="$(kubectl get service "$RUSTFS_SERVICE" -n "$RUSTFS_NS" -o jsonpath='{.metadata.name}')"
SVC_PORT="$(kubectl get service "$RUSTFS_SERVICE" -n "$RUSTFS_NS" -o jsonpath='{range .spec.ports[*]}{.name}={.port}{"\n"}{end}' \
  | awk -F= '$1 == "endpoint" {print $2}')"

if [ -z "$SVC_NAME" ] || [ -z "$SVC_PORT" ]; then
  echo "❌ Could not derive endpoint name/port from Service/$RUSTFS_SERVICE (no port named 'endpoint')"
  exit 1
fi

ENDPOINT_HOST="${SVC_NAME}.${RUSTFS_NS}.svc.${CLUSTER_DOMAIN}"

CURRENT_HOST="$(kubectl get configmap "$DUCKDB_SETTINGS" -n "$ANALYTICS_NS" -o jsonpath='{.data.RUSTFS_ENDPOINT_HOST}' || true)"
CURRENT_PORT="$(kubectl get configmap "$DUCKDB_SETTINGS" -n "$ANALYTICS_NS" -o jsonpath='{.data.RUSTFS_ENDPOINT_PORT}' || true)"

echo "▶ Derived from Service/$RUSTFS_SERVICE: host=${ENDPOINT_HOST} port=${SVC_PORT}"
echo "▶ Current in ConfigMap/$DUCKDB_SETTINGS: host=${CURRENT_HOST:-<unset>} port=${CURRENT_PORT:-<unset>}"

if [ "$CURRENT_HOST" = "$ENDPOINT_HOST" ] && [ "$CURRENT_PORT" = "$SVC_PORT" ]; then
  echo "✅ duckdb-settings already aligned with deployed rustfs (no restart needed)"
  exit 0
fi

echo "▶ Patching ConfigMap/$DUCKDB_SETTINGS in ns $ANALYTICS_NS"
kubectl patch configmap "$DUCKDB_SETTINGS" -n "$ANALYTICS_NS" \
  --type=merge \
  -p "{\"data\":{\"RUSTFS_ENDPOINT_HOST\":\"${ENDPOINT_HOST}\",\"RUSTFS_ENDPOINT_PORT\":\"${SVC_PORT}\"}}"

echo "▶ Restarting deployment/$DUCKDB_DEPLOY so init.sql re-reads settings"
kubectl rollout restart deployment/"$DUCKDB_DEPLOY" -n "$ANALYTICS_NS"
kubectl rollout status deployment/"$DUCKDB_DEPLOY" -n "$ANALYTICS_NS" --timeout=180s || true

echo "✅ duckdb aligned with deployed rustfs"
