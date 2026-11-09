#!/usr/bin/env bash
#
# build-manifests.sh — render a module, optionally with Tailscale exposure.
#
# Usage: scripts/build-manifests.sh <module-path> [tailscale_expose] > built.yaml
#   module-path       . | duckdb | rustfs | maintenance | generator | metabase
#   tailscale_expose  none (default) | duckdb | rustfs | all
#
# Metabase is always on the tailnet (metabase/service.yaml). duckdb and
# rustfs are not, unless asked: the matching tailscale/<name> component is
# layered on through a throwaway overlay. Only components whose Service is
# part of the module are added (a patch with no target fails the build);
# the rest are skipped with a warning.
#
# Exposure is declarative per apply: an apply rendered WITHOUT a component
# drops its annotations from the live Service (kubectl apply prunes fields
# it set before), and the Tailscale operator removes the proxy.
#
set -euo pipefail

MODULE_PATH="${1:?usage: build-manifests.sh <module-path> [none|duckdb|rustfs|all]}"
EXPOSE="${2:-none}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

case "$EXPOSE" in
  none) WANT=() ;;
  duckdb) WANT=(duckdb) ;;
  rustfs) WANT=(rustfs) ;;
  all) WANT=(duckdb rustfs) ;;
  *) echo "❌ invalid tailscale_expose '$EXPOSE' (allowed: none, duckdb, rustfs, all)" >&2; exit 1 ;;
esac

# which exposable Services each module renders
module_has() {
  case "$MODULE_PATH" in
    .) return 0 ;;
    "$1") return 0 ;;
    *) return 1 ;;
  esac
}

COMPONENTS=()
for c in "${WANT[@]}"; do
  if module_has "$c"; then
    COMPONENTS+=("$c")
  else
    echo "⚠️  tailscale_expose=$EXPOSE: module '$MODULE_PATH' has no $c Service — skipping that exposure" >&2
  fi
done

if [ "${#COMPONENTS[@]}" -eq 0 ]; then
  echo "▶ Tailscale: metabase only" >&2
  exec kubectl kustomize --enable-helm "$ROOT/$MODULE_PATH"
fi

# throwaway overlay OUTSIDE the repo: kustomize rejects an overlay nested
# inside a kustomization it includes ("cycle detected") — which is what
# module "." (the repo root) would be.
OVERLAY="$(mktemp -d)"
trap 'rm -rf "$OVERLAY"' EXIT

# kustomize refuses absolute paths ("new root cannot be absolute"), so
# every entry is relative to the overlay dir
rel() { realpath --relative-to="$OVERLAY" "$1"; }

{
  echo "apiVersion: kustomize.config.k8s.io/v1beta1"
  echo "kind: Kustomization"
  echo "resources:"
  echo "  - $(rel "$ROOT/$MODULE_PATH")"
  echo "components:"
  for c in "${COMPONENTS[@]}"; do
    echo "  - $(rel "$ROOT/tailscale/$c")"
  done
} > "$OVERLAY/kustomization.yaml"

echo "▶ Tailscale: metabase + ${COMPONENTS[*]}" >&2
kubectl kustomize --enable-helm "$OVERLAY"
