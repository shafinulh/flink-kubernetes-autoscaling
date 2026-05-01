#!/bin/bash
###############################################################################
# manage-external-kafka.sh
#
# Manage a standalone Kafka broker running directly on a target host via
# Docker. This is intentionally separate from the in-cluster Kafka StatefulSet.
###############################################################################
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../../../cluster/config/env.sh"

TARGET_HOST="${TARGET_HOST:-c153}"
HOST_TAG="${HOST_TAG:-${TARGET_HOST}}"
CONTAINER_NAME="${CONTAINER_NAME:-external-kafka-${HOST_TAG}}"
DATA_DIR="${DATA_DIR:-/opt/flink-kubernetes-autoscaling/${CONTAINER_NAME}/data}"
IMAGE="${KAFKA_IMAGE:-apache/kafka:3.9.2}"
KAFKA_PORT="${KAFKA_PORT:-9092}"
CONTROLLER_PORT="${CONTROLLER_PORT:-9093}"
KAFKA_PARTITIONS="${KAFKA_PARTITIONS:-24}"
KAFKA_TOPICS="${KAFKA_TOPICS:-nexmark-person nexmark-auction nexmark-bid}"

GREEN='\033[0;32m'; RED='\033[0;31m'; NC='\033[0m'

ACTION="${1:-status}"

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
        printf 'TARGET_HOST is required\n' >&2
        exit 1
    fi

    printf 'Could not resolve TARGET_IP for %s\n' "${TARGET_HOST}" >&2
    exit 1
}

TARGET_IP="$(resolve_target_ip)"

ssh_remote() {
    ssh -o BatchMode=yes "${TARGET_HOST}" "$@"
}

wait_for_kafka() {
    for _ in $(seq 1 60); do
        if ssh_remote "sudo -n docker exec ${CONTAINER_NAME} bash -lc '/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server 127.0.0.1:${KAFKA_PORT} >/dev/null 2>&1'"; then
            return 0
        fi
        sleep 2
    done
    return 1
}

create_topics_remote() {
    local topics_literal=""
    local topic

    for topic in ${KAFKA_TOPICS}; do
        topics_literal+=" '${topic}'"
    done

    ssh_remote "sudo -n docker exec ${CONTAINER_NAME} bash -lc '
        set -euo pipefail
        topics=(${topics_literal})
        for topic in \"\${topics[@]}\"; do
            /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --bootstrap-server 127.0.0.1:${KAFKA_PORT} --topic \"\${topic}\" --partitions ${KAFKA_PARTITIONS} --replication-factor 1 >/dev/null
            current_partitions=\$(/opt/kafka/bin/kafka-topics.sh --describe --bootstrap-server 127.0.0.1:${KAFKA_PORT} --topic \"\${topic}\" | grep -o \"PartitionCount: [0-9]\\+\" | head -n1 | tr -cd \"0-9\")
            if [ -n \"\${current_partitions}\" ] && [ \"\${current_partitions}\" -lt ${KAFKA_PARTITIONS} ]; then
                /opt/kafka/bin/kafka-topics.sh --alter --bootstrap-server 127.0.0.1:${KAFKA_PORT} --topic \"\${topic}\" --partitions ${KAFKA_PARTITIONS} >/dev/null
            fi
        done
        /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server 127.0.0.1:${KAFKA_PORT}
    '"
}

case "${ACTION}" in
    start)
        echo "── Starting External Kafka On ${TARGET_HOST} ─────────────────────────"
        ssh_remote "sudo -n mkdir -p '${DATA_DIR}'"
        ssh_remote "sudo -n chown -R 1000:1000 '${DATA_DIR}'"
        ssh_remote "sudo -n docker rm -f ${CONTAINER_NAME} >/dev/null 2>&1 || true"
        ssh_remote "sudo -n docker run -d --name ${CONTAINER_NAME} --restart unless-stopped --network host \
            -v '${DATA_DIR}:/var/lib/kafka/data' \
            -e KAFKA_NODE_ID=1 \
            -e KAFKA_PROCESS_ROLES=broker,controller \
            -e KAFKA_LISTENERS='PLAINTEXT://0.0.0.0:${KAFKA_PORT},CONTROLLER://0.0.0.0:${CONTROLLER_PORT}' \
            -e KAFKA_ADVERTISED_LISTENERS='PLAINTEXT://${TARGET_IP}:${KAFKA_PORT}' \
            -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP='CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT' \
            -e KAFKA_CONTROLLER_LISTENER_NAMES='CONTROLLER' \
            -e KAFKA_CONTROLLER_QUORUM_VOTERS='1@${TARGET_IP}:${CONTROLLER_PORT}' \
            -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=false \
            -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
            -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
            -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
            -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
            -e KAFKA_LOG_DIRS='/var/lib/kafka/data' \
            ${IMAGE} >/dev/null"
        if wait_for_kafka; then
            echo -e "${GREEN}✓${NC} External Kafka is up on ${TARGET_HOST} (${TARGET_IP}:${KAFKA_PORT})"
        else
            echo -e "${RED}✗ External Kafka did not become ready in time${NC}"
            exit 1
        fi
        ;;

    create-topics)
        echo "── Creating Topics On External Kafka ───────────────────────"
        create_topics_remote
        echo -e "${GREEN}✓${NC} Topics created on ${TARGET_HOST}"
        ;;

    reset)
        echo "── Resetting External Kafka On ${TARGET_HOST} ────────────────────────"
        ssh_remote "sudo -n docker rm -f ${CONTAINER_NAME} >/dev/null 2>&1 || true"
        ssh_remote "sudo -n rm -rf '${DATA_DIR}' && sudo -n mkdir -p '${DATA_DIR}' && sudo -n chown -R 1000:1000 '${DATA_DIR}'"
        "${0}" start
        "${0}" create-topics
        ;;

    stop)
        echo "── Stopping External Kafka On ${TARGET_HOST} ─────────────────────────"
        ssh_remote "sudo -n docker rm -f ${CONTAINER_NAME} >/dev/null 2>&1 || true"
        echo -e "${GREEN}✓${NC} External Kafka stopped"
        ;;

    status)
        echo "── External Kafka Status On ${TARGET_HOST} ───────────────────────────"
        ssh_remote "sudo -n docker ps -a --filter name=${CONTAINER_NAME}"
        ;;

    logs)
        echo "── External Kafka Logs On ${TARGET_HOST} ─────────────────────────────"
        ssh_remote "sudo -n docker logs --tail 200 ${CONTAINER_NAME}"
        ;;

    *)
        echo -e "${RED}Unknown action: ${ACTION}${NC}"
        exit 1
        ;;
esac
