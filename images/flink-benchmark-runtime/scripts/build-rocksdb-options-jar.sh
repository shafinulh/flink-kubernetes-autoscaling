#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../../scripts/env.sh"

mvn -f "${ROCKSDB_OPTIONS_SOURCE}/pom.xml" \
  package \
  -Dmaven.test.skip=true \
  -Dflink.version="${NEXMARK_FLINK_VERSION}"
