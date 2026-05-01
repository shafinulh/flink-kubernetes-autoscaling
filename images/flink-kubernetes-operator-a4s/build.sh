#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../scripts/env.sh"

echo "Building Flink Kubernetes Operator image ${OPERATOR_A4S_IMAGE}..."
docker build \
  -t "${OPERATOR_A4S_IMAGE}" \
  -f "${REPO_ROOT}/sources/operators/flink-kubernetes-operator-a4s/Dockerfile" \
  "${REPO_ROOT}/sources/operators/flink-kubernetes-operator-a4s"

echo "Flink Kubernetes Operator image built successfully: ${OPERATOR_A4S_IMAGE}"