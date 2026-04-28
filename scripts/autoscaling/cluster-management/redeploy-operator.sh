#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../../cluster/config/env.sh"

helm uninstall flink-kubernetes-operator 2>/dev/null || true
kubectl delete crd/flinkdeployments.flink.apache.org --ignore-not-found=true
sleep 5
"${SCRIPT_DIR}/04-deploy-operator.sh"
