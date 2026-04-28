#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../../cluster/config/env.sh"

echo "Nodes"
kubectl get nodes -o wide || true

echo
echo "Node tiers"
for node in "${ALL_NODES[@]}"; do
  tier="$(kubectl get node "${node}" -o jsonpath='{.metadata.labels.tier}' 2>/dev/null || true)"
  printf '%-12s %s\n' "${node}" "${tier:-none}"
done

echo
echo "Helm releases"
helm list --all-namespaces || true

echo
echo "Flink operator"
kubectl get pods -l app.kubernetes.io/name=flink-kubernetes-operator || true

echo
echo "Monitoring pods"
kubectl get pods -n manager || true

echo
echo "Registry"
curl -fsS "http://${REGISTRY}/v2/_catalog" 2>/dev/null || true

echo
echo "Cluster problem pods"
kubectl get pods --all-namespaces --no-headers 2>/dev/null \
  | awk '$4 !~ /^(Running|Completed)$/ {print}' || true
