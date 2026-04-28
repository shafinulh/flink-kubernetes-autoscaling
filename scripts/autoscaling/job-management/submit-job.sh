#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../../cluster/config/env.sh"

if [[ $# -ne 1 ]]; then
  echo "Usage: $(basename "$0") <flinkdeployment-yaml>" >&2
  exit 2
fi

job_yaml="$1"
if [[ ! -f "${job_yaml}" ]]; then
  job_yaml="${SCRIPT_DIR}/${job_yaml}"
fi
if [[ ! -f "${job_yaml}" ]]; then
  echo "Job YAML not found: $1" >&2
  exit 1
fi

deployment="$(kubectl create --dry-run=client -f "${job_yaml}" -o jsonpath='{.metadata.name}')"

if kubectl get flinkdeployment "${deployment}" >/dev/null 2>&1; then
  echo "FlinkDeployment ${deployment} already exists. Stop it first with scripts/autoscaling/job-management/stop-job.sh ${deployment}." >&2
  exit 1
fi

kubectl apply -f "${job_yaml}"

for _ in $(seq 1 60); do
  lifecycle="$(kubectl get flinkdeployment "${deployment}" -o jsonpath='{.status.lifecycleState}' 2>/dev/null || true)"
  job_state="$(kubectl get flinkdeployment "${deployment}" -o jsonpath='{.status.jobStatus.state}' 2>/dev/null || true)"
  if [[ "${lifecycle}" == "FAILED" || "${job_state}" == "FAILED" ]]; then
    kubectl describe flinkdeployment "${deployment}" || true
    exit 1
  fi
  if kubectl get pods -l component=jobmanager --no-headers 2>/dev/null | awk '{print $3}' | grep -q '^Running$'; then
    break
  fi
  sleep 5
done

kubectl get flinkdeployment "${deployment}"
kubectl get pods -l app=flink
