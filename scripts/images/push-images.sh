#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../cluster/config/env.sh"

DRY_RUN=false
PREPULL_BENCHMARK_IMAGE="${PREPULL_BENCHMARK_IMAGE:-true}"
TARGETS=()

usage() {
  cat <<USAGE
Usage: $(basename "$0") [runtime] [benchmark] [operator] [all] [--no-prepull] [--dry-run] [--help]

Tags and pushes configured registry image tags. Existing registry tags are not deleted.

When pushing the benchmark image, pre-pulls it on configured cluster nodes by default.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true ;;
    --no-prepull) PREPULL_BENCHMARK_IMAGE=false ;;
    runtime|benchmark|operator|all) TARGETS+=("$1") ;;
    --help|-h) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage; exit 2 ;;
  esac
  shift
done

if [[ "${#TARGETS[@]}" -eq 0 ]]; then
  TARGETS=(all)
fi

run() {
  printf 'Command:'
  printf ' %q' "$@"
  printf '\n'
  if [[ "${DRY_RUN}" == "false" ]]; then
    "$@"
  fi
}

prepull_benchmark_image() {
  local node

  if [[ "${PREPULL_BENCHMARK_IMAGE}" != "true" ]]; then
    echo "Skipping benchmark image pre-pull; PREPULL_BENCHMARK_IMAGE=${PREPULL_BENCHMARK_IMAGE}."
    return 0
  fi

  echo "Pre-pulling ${FLINK_BENCHMARK_IMAGE} on cluster nodes"
  for node in "${ALL_NODES[@]}"; do
    if [[ "${node}" == "${CONTROL_PLANE}" ]]; then
      run sudo crictl --runtime-endpoint "${CRI_RUNTIME_ENDPOINT}" pull "${FLINK_BENCHMARK_IMAGE}"
    else
      run ssh -o BatchMode=yes "${node}" "sudo crictl --runtime-endpoint ${CRI_RUNTIME_ENDPOINT} pull ${FLINK_BENCHMARK_IMAGE}"
    fi
  done
}

for target in "${TARGETS[@]}"; do
  case "${target}" in
    runtime)
      run docker tag "${FLINK_RUNTIME_LOCAL}" "${FLINK_RUNTIME_IMAGE}"
      run docker push "${FLINK_RUNTIME_IMAGE}"
      ;;
    benchmark)
      run docker tag "${FLINK_BENCHMARK_LOCAL}" "${FLINK_BENCHMARK_IMAGE}"
      run docker push "${FLINK_BENCHMARK_IMAGE}"
      prepull_benchmark_image
      ;;
    operator)
      run docker tag "${OPERATOR_LOCAL}" "${OPERATOR_IMAGE}"
      run docker push "${OPERATOR_IMAGE}"
      ;;
    all)
      run docker tag "${FLINK_RUNTIME_LOCAL}" "${FLINK_RUNTIME_IMAGE}"
      run docker push "${FLINK_RUNTIME_IMAGE}"
      run docker tag "${FLINK_BENCHMARK_LOCAL}" "${FLINK_BENCHMARK_IMAGE}"
      run docker push "${FLINK_BENCHMARK_IMAGE}"
      prepull_benchmark_image
      run docker tag "${OPERATOR_LOCAL}" "${OPERATOR_IMAGE}"
      run docker push "${OPERATOR_IMAGE}"
      ;;
  esac
done
