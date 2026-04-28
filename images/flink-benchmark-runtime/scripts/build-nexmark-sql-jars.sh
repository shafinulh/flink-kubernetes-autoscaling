#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../../scripts/env.sh"

NEXMARK_JAR="${NEXMARK_SOURCE}/nexmark-flink/target/nexmark-flink-0.3-SNAPSHOT.jar"

set +e
mvn -f "${NEXMARK_SOURCE}/pom.xml" \
  -pl nexmark-flink -am package \
  -Dmaven.test.skip=true \
  -Dassembly.skipAssembly=true \
  -Dflink.version="${NEXMARK_FLINK_VERSION}"
maven_exit=$?
set -e

if [[ ! -f "${NEXMARK_JAR}" ]]; then
  echo "Nexmark jar not found: ${NEXMARK_JAR}" >&2
  exit 1
fi
if [[ "${maven_exit}" -ne 0 ]]; then
  echo "Maven exited ${maven_exit}, but ${NEXMARK_JAR} was produced; continuing." >&2
fi

mvn -f "${CUSTOM_KAFKA_CONNECTOR_SOURCE}/pom.xml" \
  -pl flink-sql-connector-kafka -am package \
  -Dmaven.test.skip=true \
  -DskipTests \
  -Dflink.version="${NEXMARK_FLINK_VERSION}"
