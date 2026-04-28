#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "== Cluster =="
"${SCRIPT_DIR}/cluster-management/cluster-status.sh"

echo
echo "== Job =="
"${SCRIPT_DIR}/job-management/job-status.sh" "${1:-flink}"

echo
echo "== Port-forwards =="
"${SCRIPT_DIR}/job-monitoring/port-forward.sh" status
