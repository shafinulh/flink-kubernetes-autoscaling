#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../env.sh"

WITH_RUNTIME=false

usage() {
  cat <<USAGE
Usage: $(basename "$0") [--with-runtime] [--force-datastream-build] [--no-cache] [--dry-run] [--help]

Builds the configured default benchmark runtime and operator images:
  ${FLINK_BENCHMARK_LOCAL}
  ${OPERATOR_LOCAL}
Skips ${FLINK_RUNTIME_LOCAL} unless --with-runtime is set.

  --with-runtime  also rebuild the base Flink runtime image
  --force-datastream-build
                  rebuild DataStream benchmark jars even when target jars already exist
  --no-cache      pass Docker --no-cache to images being built
  --dry-run       print commands only
USAGE
}

COMMON_ARGS=()
BENCHMARK_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --with-runtime) WITH_RUNTIME=true ;;
    --dry-run|--no-cache) COMMON_ARGS+=("$1") ;;
    --force-datastream-build) BENCHMARK_ARGS+=("$1") ;;
    --help|-h) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage; exit 2 ;;
  esac
  shift
done

if [[ "${WITH_RUNTIME}" == "true" ]]; then
  "${REPO_ROOT}/images/flink-runtime/build.sh" "${COMMON_ARGS[@]}"
else
  echo "Skipping ${FLINK_RUNTIME_LOCAL}; use --with-runtime to rebuild it."
fi

"${REPO_ROOT}/images/flink-benchmark-runtime/build.sh" "${COMMON_ARGS[@]}" "${BENCHMARK_ARGS[@]}"
"${REPO_ROOT}/images/flink-kubernetes-operator/build.sh" "${COMMON_ARGS[@]}"
