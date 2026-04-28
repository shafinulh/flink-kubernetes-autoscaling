#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../../cluster/config/env.sh"

echo "Resetting autoscaling stack; Kubernetes nodes and kubeadm state are left intact."

kubectl get flinkdeployment -o name 2>/dev/null \
  | while read -r deployment; do
      kubectl patch "${deployment}" -p '{"metadata":{"finalizers":[]}}' --type=merge 2>/dev/null || true
      kubectl delete "${deployment}" --ignore-not-found=true --wait=false 2>/dev/null || true
    done

helm uninstall flink-kubernetes-operator 2>/dev/null || true
helm uninstall prom -n manager 2>/dev/null || true
helm uninstall loki -n manager 2>/dev/null || true

kubectl delete crd/flinkdeployments.flink.apache.org --ignore-not-found=true
kubectl delete ns manager local-path-storage --ignore-not-found=true --timeout=60s 2>/dev/null || true

kubectl get pods --all-namespaces --no-headers 2>/dev/null \
  | awk '$4 == "Terminating" {print $1, $2}' \
  | while read -r ns pod; do
      kubectl delete pod "${pod}" -n "${ns}" --force --grace-period=0 2>/dev/null || true
    done

kubectl get nodes
echo
echo "Replay from scripts/autoscaling/cluster-management/03-deploy-monitoring.sh"
