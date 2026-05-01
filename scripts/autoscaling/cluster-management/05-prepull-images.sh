#!/usr/bin/env bash
###############################################################################
# 05-prepull-images.sh
#
# Warm image caches on the machines used by the Kubernetes autoscaling flow.
# This avoids paying image pull/load costs when a Flink job or the standalone
# producer is started.
###############################################################################
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../../cluster/config/env.sh"

TARGET_HOST="${TARGET_HOST:-c153}"
KAFKA_IMAGE="${KAFKA_IMAGE:-apache/kafka:3.9.2}"
PREPULL_OPERATOR="${PREPULL_OPERATOR:-true}"
PREPULL_BENCHMARK="${PREPULL_BENCHMARK:-true}"
PREPULL_EXTERNAL="${PREPULL_EXTERNAL:-true}"

usage() {
  cat <<USAGE
Usage: $(basename "$0") [--target-host HOST] [--skip-operator] [--skip-benchmark] [--skip-external] [--help]

Pre-pulls configured images before launching the Flink operator, Flink jobs, or
the external Kafka/producer flow.

Kubernetes nodes:
  ${OPERATOR_IMAGE}
  ${FLINK_BENCHMARK_IMAGE}

External producer host:
  ${KAFKA_IMAGE}
  ${FLINK_BENCHMARK_IMAGE}
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target-host)
      TARGET_HOST="${2:-}"
      shift
      ;;
    --skip-operator)
      PREPULL_OPERATOR=false
      ;;
    --skip-benchmark)
      PREPULL_BENCHMARK=false
      ;;
    --skip-external)
      PREPULL_EXTERNAL=false
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
  shift
done

run_local() {
  sudo -n "$@"
}

run_remote() {
  local host="$1"
  shift
  ssh -o BatchMode=yes "${host}" "sudo -n $*"
}

pull_cri_image() {
  local node="$1"
  local image="$2"

  if [[ "${node}" == "${CONTROL_PLANE}" ]]; then
    run_local crictl --runtime-endpoint "${CRI_RUNTIME_ENDPOINT}" pull "${image}" >/dev/null
  else
    run_remote "${node}" "crictl --runtime-endpoint ${CRI_RUNTIME_ENDPOINT} pull ${image} >/dev/null"
  fi
}

ensure_external_docker_image() {
  local image="$1"
  local local_id=""
  local remote_id=""

  if docker image inspect "${image}" >/dev/null 2>&1; then
    local_id="$(docker image inspect --format '{{.Id}}' "${image}")"
  fi

  remote_id="$(ssh -o BatchMode=yes "${TARGET_HOST}" "sudo -n docker image inspect --format '{{.Id}}' '${image}' 2>/dev/null || true")"
  if [[ -n "${remote_id}" && ( -z "${local_id}" || "${remote_id}" == "${local_id}" ) ]]; then
    return 0
  fi

  if ssh -o BatchMode=yes "${TARGET_HOST}" "sudo -n docker pull '${image}' >/dev/null 2>&1"; then
    remote_id="$(ssh -o BatchMode=yes "${TARGET_HOST}" "sudo -n docker image inspect --format '{{.Id}}' '${image}' 2>/dev/null || true")"
    if [[ -z "${local_id}" || "${remote_id}" == "${local_id}" ]]; then
      return 0
    fi
  fi

  if [[ -z "${local_id}" ]]; then
    echo "Unable to pull ${image} on ${TARGET_HOST}, and the image is not present locally for docker save/load fallback." >&2
    exit 1
  fi

  docker save "${image}" | ssh -o BatchMode=yes "${TARGET_HOST}" "sudo -n docker load >/dev/null"
  remote_id="$(ssh -o BatchMode=yes "${TARGET_HOST}" "sudo -n docker image inspect --format '{{.Id}}' '${image}'")"
  if [[ "${remote_id}" != "${local_id}" ]]; then
    echo "Image ${image} on ${TARGET_HOST} does not match local image id after load." >&2
    exit 1
  fi
}

echo "── Pre-pulling Kubernetes Images ──────────────────────────"
for node in "${ALL_NODES[@]}"; do
  if [[ "${PREPULL_OPERATOR}" == "true" ]]; then
    echo "  ${node}: ${OPERATOR_IMAGE}"
    pull_cri_image "${node}" "${OPERATOR_IMAGE}"
  fi
  if [[ "${PREPULL_BENCHMARK}" == "true" ]]; then
    echo "  ${node}: ${FLINK_BENCHMARK_IMAGE}"
    pull_cri_image "${node}" "${FLINK_BENCHMARK_IMAGE}"
  fi
done

if [[ "${PREPULL_EXTERNAL}" == "true" ]]; then
  echo "── Pre-pulling External Host Images On ${TARGET_HOST} ─────"
  echo "  ${TARGET_HOST}: ${KAFKA_IMAGE}"
  ensure_external_docker_image "${KAFKA_IMAGE}"
  if [[ "${PREPULL_BENCHMARK}" == "true" ]]; then
    echo "  ${TARGET_HOST}: ${FLINK_BENCHMARK_IMAGE}"
    ensure_external_docker_image "${FLINK_BENCHMARK_IMAGE}"
  fi
fi

echo "✓ Image pre-pull complete"
