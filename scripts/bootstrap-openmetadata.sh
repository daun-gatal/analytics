#!/usr/bin/env bash
#
# bootstrap-openmetadata.sh — one-time per-platform OM ramp.
#
#  1. wait for the OpenMetadata server (through kubectl port-forward, so it
#     works the same way from the CI runner and an operator laptop)
#  2. log in as admin: first with $OM_ADMIN_PASSWORD (existing install), then
#     with the default 'admin' (fresh install) — and rotate the default to
#     $OM_ADMIN_PASSWORD when needed (a tailnet-reachable admin/admin would
#     be a standing hole)
#  3. create (or update) the catalog-sync-bot user and mint a fresh bot JWT
#  4. stamp Secret/openmetadata-bot in ns openmetadata with the token
#
# Idempotent: safe on every run; the secret ends up matching the server.
#
# Env (required):
#   OM_ADMIN_PASSWORD  target admin password (GitHub Secret)
# Optional:
#   OM_NAMESPACE       default openmetadata
#   OM_SERVICE         default openmetadata
#   OM_PORT            default 8585
#   OM_LOCAL_PORT      default 18585
#   BOT_NAME           default catalog-sync-bot
#
set -euo pipefail

OM_NS="${OM_NAMESPACE:-openmetadata}"
OM_SVC="${OM_SERVICE:-openmetadata}"
OM_PORT="${OM_PORT:-8585}"
LOCAL_PORT="${OM_LOCAL_PORT:-18585}"
BOT_NAME="${BOT_NAME:-catalog-sync-bot}"

for var in OM_ADMIN_PASSWORD; do
  if [ -z "${!var:-}" ]; then
    echo "❌ $var is required (set it from the matching GitHub Secret)" >&2
    exit 2
  fi
done

BASE="http://127.0.0.1:${LOCAL_PORT}/api"

# -----------------------------------------------------------------------
# 0. port-forward the API (only port reachable from the runner reliably)
# -----------------------------------------------------------------------
kubectl port-forward "service/${OM_SVC}" "${LOCAL_PORT}:${OM_PORT}" -n "$OM_NS" >/tmp/om-pf.log 2>&1 &
PF_PID=$!
trap 'kill "$PF_PID" 2>/dev/null || true' EXIT

echo "▶ waiting for OpenMetadata /api/v1/system/health"
DEADLINE=$(( $(date +%s) + 300 ))
until curl -sf "$BASE/v1/system/health" >/dev/null 2>&1; do
  if [ "$(date +%s)" -ge "$DEADLINE" ]; then
    echo "❌ OpenMetadata did not become healthy in 5m — see /tmp/om-pf.log" >&2
    exit 1
  fi
  sleep 5
done
echo "✅ server healthy"

login() {
  curl -sf -X POST "$BASE/v1/users/login" \
    -H 'Content-Type: application/json' \
    -d "{\"email\": \"admin@open-metadata.org\", \"password\": \"$1\"}"
}

ADMIN_EMAIL="admin@open-metadata.org"

# -----------------------------------------------------------------------
# 1. login (secret password first, default 'admin' for a fresh install)
# -----------------------------------------------------------------------
TOKEN_JSON="$(login "$OM_ADMIN_PASSWORD" || true)"
USED_DEFAULT=""
if [ -z "$TOKEN_JSON" ]; then
  TOKEN_JSON="$(login admin || true)"
  if [ -z "$TOKEN_JSON" ]; then
    echo "❌ admin login failed for both \$OM_ADMIN_PASSWORD and the default 'admin'" >&2
    exit 1
  fi
  USED_DEFAULT=1
fi
ADMIN_TOKEN="$(echo "$TOKEN_JSON" | python3 -c 'import json,sys; print(json.load(sys.stdin)["accessToken"])')"
AUTH="Authorization: Bearer $ADMIN_TOKEN"

# -----------------------------------------------------------------------
# 2. rotate the default password immediately after a fresh install
# -----------------------------------------------------------------------
if [ -n "$USED_DEFAULT" ]; then
  echo "▶ fresh install detected — rotating the default admin password"
  ROTATE_CODE="$(curl -s -o /tmp/om-rotate.out -w '%{http_code}' -X PUT \
    "$BASE/v1/users/changePassword" \
    -H "$AUTH" -H 'Content-Type: application/json' \
    -d "{\"email\": \"$ADMIN_EMAIL\", \"currentPassword\": \"admin\", \"newPassword\": \"$OM_ADMIN_PASSWORD\"}")"
  if [ "$ROTATE_CODE" != "200" ]; then
    echo "❌ password rotation failed (HTTP $ROTATE_CODE): $(head -c 300 /tmp/om-rotate.out)" >&2
    echo "   set \$OM_ADMIN_PASSWORD manually or rotate via the OM UI, then re-run" >&2
    exit 1
  fi
  # keep operating with the rotated credentials
  TOKEN_JSON="$(login "$OM_ADMIN_PASSWORD")"
  ADMIN_TOKEN="$(echo "$TOKEN_JSON" | python3 -c 'import json,sys; print(json.load(sys.stdin)["accessToken"])')"
  AUTH="Authorization: Bearer $ADMIN_TOKEN"
  echo "✅ admin password set from OM_ADMIN_PASSWORD"
fi

# -----------------------------------------------------------------------
# 3. create or update the catalog-sync bot user + mint a fresh JWT
# -----------------------------------------------------------------------
USER_CODE="$(curl -s -o /tmp/om-bot.out -w '%{http_code}' "$BASE/v1/users/name/$BOT_NAME" -H "$AUTH")"
if [ "$USER_CODE" = "404" ]; then
  echo "▶ creating bot user $BOT_NAME"
  CREATE_CODE="$(curl -s -o /tmp/om-bot.out -w '%{http_code}' -X POST "$BASE/v1/users" \
    -H "$AUTH" -H 'Content-Type: application/json' \
    -d "{\"name\": \"$BOT_NAME\", \"displayName\": \"Catalog Sync Bot\", \"isBot\": true}")"
  if [ "$CREATE_CODE" != "201" ]; then
    echo "❌ bot user creation failed (HTTP $CREATE_CODE): $(head -c 300 /tmp/om-bot.out)" >&2
    exit 1
  fi
elif [ "$USER_CODE" != "200" ]; then
  echo "❌ bot user lookup failed (HTTP $USER_CODE): $(head -c 300 /tmp/om-bot.out)" >&2
  exit 1
fi

USER_ID="$(curl -sf "$BASE/v1/users/name/$BOT_NAME" -H "$AUTH" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')"
TOKEN_CODE="$(curl -s -o /tmp/om-token.out -w '%{http_code}' -X PUT \
  "$BASE/v1/users/generateToken/$USER_ID" \
  -H "$AUTH" -H 'Content-Type: application/json' \
  -d '{"JWTTokenExpiry": "1 day"}')"
if [ "$TOKEN_CODE" != "200" ]; then
  echo "❌ bot token generation failed (HTTP $TOKEN_CODE): $(head -c 300 /tmp/om-token.out)" >&2
  exit 1
fi
BOT_TOKEN="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["JWTToken"])' < /tmp/om-token.out)"

# -----------------------------------------------------------------------
# 4. stamp the bot secret (server-side rotation == secret rotation)
# -----------------------------------------------------------------------
kubectl create secret generic openmetadata-bot -n "$OM_NS" \
  --from-literal=OM_BOT_TOKEN="$BOT_TOKEN" \
  --dry-run=client -o yaml | kubectl apply -f -
echo "✅ Secret/openmetadata-bot stamped — catalog-sync can now run"
