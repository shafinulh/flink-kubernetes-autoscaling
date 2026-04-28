#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../../cluster/config/env.sh"

deployment="${1:-flink}"

echo "Flink deployments"
kubectl get flinkdeployment 2>/dev/null || true

echo
echo "Deployment detail: ${deployment}"
kubectl get flinkdeployment "${deployment}" -o wide 2>/dev/null || true

echo
echo "JobManager pods"
kubectl get pods -l component=jobmanager -o wide 2>/dev/null || true

echo
echo "TaskManager pods"
kubectl get pods -l component=taskmanager -o wide 2>/dev/null || true

echo
echo "Flink services"
kubectl get svc -l app=flink 2>/dev/null || true
kubectl get svc "${deployment}-rest" 2>/dev/null || true

echo
echo "Recent Flink events"
kubectl get events --sort-by=.lastTimestamp 2>/dev/null \
  | grep -E "flink|${deployment}|jobmanager|taskmanager" \
  | tail -20 || true
