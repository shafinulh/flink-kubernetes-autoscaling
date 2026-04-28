#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../env.sh"

usage() {
  cat <<USAGE
Usage: $(basename "$0") [--help]

Checks whether the configured local image tags exist and verifies expected benchmark jars when present.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --help|-h) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage; exit 2 ;;
  esac
done

missing=0
for image in "${FLINK_RUNTIME_LOCAL}" "${FLINK_BENCHMARK_LOCAL}" "${OPERATOR_LOCAL}"; do
  if docker image inspect "${image}" >/dev/null 2>&1; then
    echo "OK: ${image}"
  else
    echo "MISSING: ${image}"
    missing=1
  fi
done

if docker image inspect "${FLINK_BENCHMARK_LOCAL}" >/dev/null 2>&1; then
  docker run --rm --entrypoint sh "${FLINK_BENCHMARK_LOCAL}" -lc \
    "test -d /opt/flink/examples/justin && \
     test -f /opt/flink/lib/nexmark-flink-0.3-SNAPSHOT.jar && \
     test -f /opt/flink/lib/${ROCKSDB_OPTIONS_JAR_NAME} && \
     test -f /opt/flink/lib/${KAFKA_SQL_CONNECTOR_JAR} && \
     test -f /opt/flink/lib/${KAFKA_CLIENTS_JAR}"
  echo "OK: benchmark runtime jars"
fi

exit "${missing}"
