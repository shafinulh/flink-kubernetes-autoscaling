#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../../cluster/config/env.sh"

ACTION="${1:-start}"
RUNTIME_DIR="${CLUSTER_ROOT}/.runtime"
mkdir -p "${RUNTIME_DIR}"

stop_matching() {
  pkill -f "kubectl port-forward.*$1" 2>/dev/null || true
}

case "${ACTION}" in
  start)
    stop_matching "prom-grafana"
    stop_matching "prom-kube-prometheus-stack-prometheus"
    stop_matching "flink-rest"

    kubectl port-forward --address 0.0.0.0 -n manager svc/prom-grafana 3001:80 >"${RUNTIME_DIR}/grafana-port-forward.log" 2>&1 &
    kubectl port-forward --address 0.0.0.0 -n manager svc/prom-kube-prometheus-stack-prometheus 9091:9090 >"${RUNTIME_DIR}/prometheus-port-forward.log" 2>&1 &

    if kubectl get svc flink-rest >/dev/null 2>&1; then
      kubectl port-forward --address 0.0.0.0 svc/flink-rest 8081:8081 >"${RUNTIME_DIR}/flink-port-forward.log" 2>&1 &
      echo "Flink UI:   http://${CONTROL_PLANE_IP}:8081"
    else
      echo "Flink UI:   no flink-rest service"
    fi

    echo "Grafana:    http://${CONTROL_PLANE_IP}:3001"
    echo "Prometheus: http://${CONTROL_PLANE_IP}:9091"
    ;;
  stop)
    stop_matching "prom-grafana"
    stop_matching "prom-kube-prometheus-stack-prometheus"
    stop_matching "flink-rest"
    ;;
  status)
    pgrep -af "kubectl port-forward" || true
    ;;
  *)
    echo "Usage: $(basename "$0") [start|stop|status]" >&2
    exit 2
    ;;
esac
