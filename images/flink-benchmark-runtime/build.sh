#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../scripts/env.sh"

DRY_RUN=false
NO_CACHE="${NO_CACHE:-false}"
FORCE_DATASTREAM_BUILD="${FORCE_DATASTREAM_BUILD:-false}"
BUILD_CONTEXT=""
LOCAL_IMAGE_OVERRIDE=""
DATASTREAM_BENCHMARK_JARS=(
  Update.jar
  ReadOnly.jar
  WriteOnly.jar
  Query1.jar
  Query2.jar
  Query3.jar
  Query5.jar
  Query8.jar
  Query11.jar
)

usage() {
  cat <<USAGE
Usage: $(basename "$0") [--runtime-image IMAGE] [--datastream-source-dir DIR] [--nexmark-source-dir DIR] [--rocksdb-source-dir DIR] [--image-name NAME] [--tag TAG] [--local-image IMAGE] [--force-datastream-build] [--no-cache] [--dry-run] [--help]

Builds ${FLINK_BENCHMARK_LOCAL} on top of ${FLINK_RUNTIME_LOCAL}.

  --runtime-image IMAGE        base Flink runtime image to use
  --datastream-source-dir DIR  DataStream benchmark source tree
  --nexmark-source-dir DIR     SQL Nexmark source tree
  --rocksdb-source-dir DIR     RocksDB options source tree
  --image-name NAME            local image repository name
  --tag TAG                    local image tag
  --local-image IMAGE          full local image reference; overrides --image-name/--tag
  --force-datastream-build     rebuild DataStream benchmark jars even when target jars already exist
  --no-cache                   pass Docker --no-cache for the benchmark image
  --dry-run                    print commands only
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true ;;
    --no-cache) NO_CACHE=true ;;
    --force-datastream-build) FORCE_DATASTREAM_BUILD=true ;;
    --runtime-image) FLINK_RUNTIME_LOCAL="$2"; shift ;;
    --datastream-source-dir) DATASTREAM_BENCHMARK_SOURCE_DIR="$2"; shift ;;
    --nexmark-source-dir) NEXMARK_SOURCE_DIR="$2"; shift ;;
    --rocksdb-source-dir) ROCKSDB_OPTIONS_SOURCE_DIR="$2"; shift ;;
    --image-name) FLINK_BENCHMARK_IMAGE_NAME="$2"; shift ;;
    --tag) FLINK_BENCHMARK_IMAGE_TAG="$2"; shift ;;
    --local-image) LOCAL_IMAGE_OVERRIDE="$2"; shift ;;
    --help|-h) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage; exit 2 ;;
  esac
  shift
done

if [[ -n "${LOCAL_IMAGE_OVERRIDE}" ]]; then
  FLINK_BENCHMARK_LOCAL="${LOCAL_IMAGE_OVERRIDE}"
else
  FLINK_BENCHMARK_LOCAL="${FLINK_BENCHMARK_IMAGE_NAME}:${FLINK_BENCHMARK_IMAGE_TAG}"
fi

DATASTREAM_BENCHMARK_SOURCE="${REPO_ROOT}/${DATASTREAM_BENCHMARK_SOURCE_DIR}"
NEXMARK_SOURCE="${REPO_ROOT}/${NEXMARK_SOURCE_DIR}"
ROCKSDB_OPTIONS_SOURCE="${REPO_ROOT}/${ROCKSDB_OPTIONS_SOURCE_DIR}"
CUSTOM_KAFKA_CONNECTOR_SOURCE="${NEXMARK_SOURCE}/flink-connector-kafka-3.3.0"
export DATASTREAM_BENCHMARK_SOURCE_DIR NEXMARK_SOURCE_DIR ROCKSDB_OPTIONS_SOURCE_DIR

run() {
  printf 'Command:'
  printf ' %q' "$@"
  printf '\n'
  if [[ "${DRY_RUN}" == "false" ]]; then
    "$@"
  fi
}

datastream_benchmark_jars_exist() {
  local jar
  for jar in "${DATASTREAM_BENCHMARK_JARS[@]}"; do
    [[ -s "${DATASTREAM_BENCHMARK_SOURCE}/target/${jar}" ]] || return 1
  done
}

if [[ "${DRY_RUN}" == "false" ]] && ! docker image inspect "${FLINK_RUNTIME_LOCAL}" >/dev/null 2>&1; then
  echo "Missing base image ${FLINK_RUNTIME_LOCAL}. Build it once with images/flink-runtime/build.sh or scripts/images/build-all-default.sh --with-runtime." >&2
  exit 1
fi

BUILD_CONTEXT="$(mktemp -d)"
cleanup() {
  if [[ -n "${BUILD_CONTEXT}" && -d "${BUILD_CONTEXT}" ]]; then
    rm -rf "${BUILD_CONTEXT}"
  fi
}
trap cleanup EXIT

echo "Packaging benchmark runtime inputs"
if [[ "${FORCE_DATASTREAM_BUILD}" == "true" ]]; then
  run "${SCRIPT_DIR}/scripts/build-datastream-benchmark-jars.sh"
elif datastream_benchmark_jars_exist; then
  echo "Skipping DataStream benchmark build; jars already exist in ${DATASTREAM_BENCHMARK_SOURCE}/target"
else
  run "${SCRIPT_DIR}/scripts/build-datastream-benchmark-jars.sh"
fi
run "${SCRIPT_DIR}/scripts/build-nexmark-sql-jars.sh"
run "${SCRIPT_DIR}/scripts/build-rocksdb-options-jar.sh"

if [[ "${DRY_RUN}" == "false" ]]; then
  mkdir -p "${BUILD_CONTEXT}/datastream-jars"

  for jar in "${DATASTREAM_BENCHMARK_JARS[@]}"; do
    cp "${DATASTREAM_BENCHMARK_SOURCE}/target/${jar}" "${BUILD_CONTEXT}/datastream-jars/"
  done
  cp "${NEXMARK_SOURCE}/nexmark-flink/target/nexmark-flink-0.3-SNAPSHOT.jar" "${BUILD_CONTEXT}/nexmark-flink-0.3-SNAPSHOT.jar"
  cp "${ROCKSDB_OPTIONS_SOURCE}/target/${ROCKSDB_OPTIONS_JAR_NAME}" "${BUILD_CONTEXT}/${ROCKSDB_OPTIONS_JAR_NAME}"
  cp "${CUSTOM_KAFKA_CONNECTOR_SOURCE}/flink-sql-connector-kafka/target/${KAFKA_SQL_CONNECTOR_JAR}" "${BUILD_CONTEXT}/${KAFKA_SQL_CONNECTOR_JAR}"
fi

run mvn dependency:copy \
  -Dartifact="org.apache.kafka:kafka-clients:${KAFKA_CLIENTS_VERSION}:jar" \
  -DoutputDirectory="${BUILD_CONTEXT}" \
  -Dmdep.stripVersion=false \
  --quiet

docker_cmd=(docker build)
if [[ "${NO_CACHE}" == "true" ]]; then
  docker_cmd+=(--no-cache)
fi
docker_cmd+=(
  --build-arg "BASE_IMAGE=${FLINK_RUNTIME_LOCAL}"
  --build-arg "KAFKA_SQL_CONNECTOR_JAR=${KAFKA_SQL_CONNECTOR_JAR}"
  --build-arg "KAFKA_CLIENTS_JAR=${KAFKA_CLIENTS_JAR}"
  --build-arg "ROCKSDB_OPTIONS_JAR=${ROCKSDB_OPTIONS_JAR_NAME}"
  -f "${REPO_ROOT}/images/flink-benchmark-runtime/Dockerfile"
  "${BUILD_CONTEXT}"
  -t "${FLINK_BENCHMARK_LOCAL}"
)

echo "Building ${FLINK_BENCHMARK_LOCAL}"
run "${docker_cmd[@]}"
