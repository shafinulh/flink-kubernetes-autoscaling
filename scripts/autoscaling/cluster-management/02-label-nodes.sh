#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../../cluster/config/env.sh"

kubectl label node "${CONTROL_PLANE}" tier=jobmanager --overwrite
kubectl taint node "${CONTROL_PLANE}" node-role.kubernetes.io/control-plane:NoSchedule- 2>/dev/null || true

for worker in "${WORKERS[@]}"; do
  kubectl label node "${worker}" tier=taskmanager --overwrite
done

kubectl get nodes --show-labels
