#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../../cluster/config/env.sh"

deployment="${1:-flink}"
timeout="${WAIT_SECONDS:-180}s"

pkill -f "kubectl port-forward.*flink-rest.*8081:8081" 2>/dev/null || true

if kubectl get flinkdeployment "${deployment}" >/dev/null 2>&1; then
  kubectl delete flinkdeployment "${deployment}" --wait=false
  if ! kubectl wait --for=delete "flinkdeployment/${deployment}" --timeout="${timeout}" >/dev/null 2>&1; then
    kubectl patch flinkdeployment "${deployment}" -p '{"metadata":{"finalizers":[]}}' --type=merge 2>/dev/null || true
    kubectl delete flinkdeployment "${deployment}" --ignore-not-found=true --wait=false
  fi
fi

kubectl get flinkdeployment 2>/dev/null || true
kubectl get pods -l app=flink 2>/dev/null || true
