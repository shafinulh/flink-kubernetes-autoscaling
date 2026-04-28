#!/bin/bash
###############################################################################
# 01-prepare-generator-host.sh
#
# Prepare a remote host to run the standalone Kafka broker and Nexmark Kafka
# producer containers. This script is intended to be run from the control node
# or any machine with SSH access to the target host.
#
# This script does not install Docker. The target host must already satisfy the
# documented prerequisites in the README.
#
# Usage:
#   experiments/1724-kafka-q20-unique/external-kafka/setup/01-prepare-generator-host.sh
###############################################################################
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../../../cluster/config/env.sh"

TARGET_HOST="${TARGET_HOST:-c153}"
HOST_TAG="${HOST_TAG:-${TARGET_HOST}}"
REMOTE_WORKDIR="${REMOTE_WORKDIR:-/opt/flink-kubernetes-autoscaling}"
SQL_IMAGE="${FLINK_BENCHMARK_IMAGE}"
BROKER_IMAGE="${KAFKA_IMAGE:-apache/kafka:3.9.2}"
JOB_JAR_PATH="${JOB_JAR_PATH:-/opt/flink/lib/nexmark-flink-0.3-SNAPSHOT.jar}"
KAFKA_SQL_CONNECTOR_JAR="${KAFKA_SQL_CONNECTOR_JAR:-flink-sql-connector-kafka-3.3.0.jar}"
KAFKA_CLIENTS_JAR="${KAFKA_CLIENTS_JAR:-kafka-clients-3.4.0.jar}"

if [[ -z "${TARGET_HOST}" ]]; then
    echo "TARGET_HOST is required" >&2
    exit 1
fi

ssh_remote() {
    ssh -o BatchMode=yes "${TARGET_HOST}" "$@"
}

ensure_remote_image() {
    local image="$1"

    if ssh_remote "sudo -n docker image inspect '${image}' >/dev/null 2>&1"; then
        return 0
    fi

    if ssh_remote "sudo -n docker pull '${image}' >/dev/null"; then
        return 0
    fi

    if docker image inspect "${image}" >/dev/null 2>&1; then
        docker save "${image}" | ssh_remote "sudo -n docker load >/dev/null"
        return 0
    fi

    echo "Unable to place image ${image} on ${TARGET_HOST}" >&2
    exit 1
}

echo "── Preparing Kafka Generator Host ${TARGET_HOST} ─────────────────────"
ssh_remote "hostname"
ssh_remote "sudo -n true"
ssh_remote "command -v docker >/dev/null"
if [[ "$(ssh_remote "sudo -n docker info --format '{{.Driver}}'")" != "overlay2" ]]; then
    echo "Docker on ${TARGET_HOST} must use the overlay2 storage driver" >&2
    exit 1
fi
if ! ssh_remote "sudo -n docker info 2>/dev/null | grep -q '${REGISTRY}'"; then
    echo "Docker on ${TARGET_HOST} must trust ${REGISTRY} as an insecure registry" >&2
    exit 1
fi
ssh_remote "sudo -n mkdir -p '${REMOTE_WORKDIR}'"

ensure_remote_image "${BROKER_IMAGE}"
ensure_remote_image "${SQL_IMAGE}"

ssh_remote "sudo -n docker run --rm --entrypoint sh '${SQL_IMAGE}' -lc 'test -f ${JOB_JAR_PATH} && test -f /opt/flink/lib/${KAFKA_SQL_CONNECTOR_JAR} && test -f /opt/flink/lib/${KAFKA_CLIENTS_JAR}' >/dev/null"

echo "✓ ${TARGET_HOST} is ready for the standalone Kafka generator flow"
