#!/bin/bash
###############################################################################
# 02-apply-kafka-service.sh
#
# Render or apply a Kubernetes Service/Endpoints alias for an external Kafka
# broker running on an arbitrary target host.
#
# Usage:
#   experiments/1724-kafka-q20-unique/external-kafka/setup/02-apply-kafka-service.sh render
#   experiments/1724-kafka-q20-unique/external-kafka/setup/02-apply-kafka-service.sh apply
#   experiments/1724-kafka-q20-unique/external-kafka/setup/02-apply-kafka-service.sh delete
#   experiments/1724-kafka-q20-unique/external-kafka/setup/02-apply-kafka-service.sh write
#
# Environment:
#   TARGET_HOST=c153
#   TARGET_IP=142.150.234.153
#   KAFKA_SERVICE_NAME=kafka-external
#   KAFKA_PORT=9092
#   OUTPUT_PATH=experiments/1724-kafka-q20-unique/external-kafka/.runtime/kafka-external-service.yaml
###############################################################################
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../../../cluster/config/env.sh"

EXPERIMENT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

TARGET_HOST="${TARGET_HOST:-c153}"
KAFKA_SERVICE_NAME="${KAFKA_SERVICE_NAME:-kafka-external}"
KAFKA_PORT="${KAFKA_PORT:-9092}"
OUTPUT_PATH="${OUTPUT_PATH:-${EXPERIMENT_ROOT}/.runtime/${KAFKA_SERVICE_NAME}-external-service.yaml}"
ACTION="${1:-render}"

resolve_target_ip() {
    if [[ -n "${TARGET_IP:-}" ]]; then
        printf '%s\n' "${TARGET_IP}"
        return 0
    fi

    if command -v getent >/dev/null 2>&1; then
        local resolved_ip
        resolved_ip="$(getent ahostsv4 "${TARGET_HOST}" | awk 'NR == 1 { print $1 }')"
        if [[ -n "${resolved_ip}" ]]; then
            printf '%s\n' "${resolved_ip}"
            return 0
        fi
    fi

    if [[ -z "${TARGET_HOST}" ]]; then
        printf 'TARGET_HOST or TARGET_IP is required\n' >&2
        exit 1
    fi

    printf 'Could not resolve TARGET_IP for %s\n' "${TARGET_HOST}" >&2
    exit 1
}

TARGET_IP="$(resolve_target_ip)"

render_yaml() {
    cat <<EOF
apiVersion: v1
kind: Service
metadata:
  name: ${KAFKA_SERVICE_NAME}
spec:
  ports:
  - name: client
    port: ${KAFKA_PORT}
    targetPort: ${KAFKA_PORT}
---
apiVersion: v1
kind: Endpoints
metadata:
  name: ${KAFKA_SERVICE_NAME}
subsets:
  - addresses:
      - ip: ${TARGET_IP}
    ports:
      - port: ${KAFKA_PORT}
        name: client
EOF
}

case "${ACTION}" in
    render)
        render_yaml
        ;;
    apply)
        render_yaml | kubectl apply --validate=false -f -
        ;;
    delete)
        render_yaml | kubectl delete --ignore-not-found=true -f -
        ;;
    write)
        mkdir -p "$(dirname "${OUTPUT_PATH}")"
        render_yaml > "${OUTPUT_PATH}"
        printf 'Wrote %s for %s -> %s:%s\n' "${OUTPUT_PATH}" "${KAFKA_SERVICE_NAME}" "${TARGET_IP}" "${KAFKA_PORT}"
        ;;
    *)
        printf 'Unknown action: %s\n' "${ACTION}" >&2
        exit 1
        ;;
esac
