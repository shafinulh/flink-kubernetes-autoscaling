#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../../cluster/config/env.sh"

DRY_RUN=false

usage() {
  cat <<USAGE
Usage: $(basename "$0") [--dry-run] [--help]

Deploys ${OPERATOR_IMAGE} with the Flink Kubernetes Operator Helm chart.

  --dry-run  print commands without applying them
  --help     show this message
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true ;;
    --help|-h) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage; exit 2 ;;
  esac
  shift
done

run() {
  printf 'Command:'
  printf ' %q' "$@"
  printf '\n'
  if [[ "${DRY_RUN}" == "false" ]]; then
    "$@"
  fi
}

run helm upgrade --install flink-kubernetes-operator "${HELM_CHART}" \
  --set "image.repository=${REGISTRY}/${OPERATOR_IMAGE_NAME}" \
  --set "image.tag=${OPERATOR_IMAGE_TAG}" \
  --set "image.pullPolicy=Always" \
  -f "${OPERATOR_VALUES}"

if [[ "${DRY_RUN}" == "false" ]]; then
  kubectl rollout status deployment/flink-kubernetes-operator --timeout=180s
  kubectl get pods -l app.kubernetes.io/name=flink-kubernetes-operator
  kubectl get crd | grep flink || true
fi
