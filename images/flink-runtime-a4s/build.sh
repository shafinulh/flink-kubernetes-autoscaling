#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../scripts/env.sh"

DRY_RUN=false
NO_CACHE="${NO_CACHE:-false}"
LOCAL_IMAGE_OVERRIDE=""

usage() {
  cat <<USAGE
Usage: $(basename "$0") [--source-dir DIR] [--image-name NAME] [--tag TAG] [--local-image IMAGE] [--no-cache] [--dry-run] [--help]

Builds ${FLINK_RUNTIME_LOCAL} from ${FLINK_RUNTIME_SOURCE_DIR}.

  --source-dir DIR    Flink source tree to copy into the Docker build context
  --image-name NAME   local image repository name
  --tag TAG           local image tag
  --local-image IMAGE full local image reference; overrides --image-name/--tag
  --no-cache          pass Docker --no-cache
  --dry-run           print the Docker command only
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true ;;
    --no-cache) NO_CACHE=true ;;
    --source-dir) FLINK_RUNTIME_SOURCE_DIR="$2"; shift ;;
    --image-name) FLINK_RUNTIME_IMAGE_NAME="$2"; shift ;;
    --tag) FLINK_RUNTIME_IMAGE_TAG="$2"; shift ;;
    --local-image) LOCAL_IMAGE_OVERRIDE="$2"; shift ;;
    --help|-h) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage; exit 2 ;;
  esac
  shift
done

if [[ -n "${LOCAL_IMAGE_OVERRIDE}" ]]; then
  FLINK_RUNTIME_A4S_LOCAL="${LOCAL_IMAGE_OVERRIDE}"
else
  FLINK_RUNTIME_A4S_LOCAL="${FLINK_RUNTIME_IMAGE_NAME}:${FLINK_RUNTIME_A4S_IMAGE_TAG}"
fi

cmd=(docker build)
if [[ "${NO_CACHE}" == "true" ]]; then
  cmd+=(--no-cache)
fi
cmd+=(
  -f "${REPO_ROOT}/images/flink-runtime-a4s/Dockerfile"
  --build-arg "FLINK_RUNTIME_SOURCE_DIR=${FLINK_RUNTIME_A4S_SOURCE_DIR}"
  -t "${FLINK_RUNTIME_A4S_LOCAL}"
  "${REPO_ROOT}"
)

echo "Building ${FLINK_RUNTIME_A4S_LOCAL}"
printf 'Command:'
printf ' %q' "${cmd[@]}"
printf '\n'

if [[ "${DRY_RUN}" == "false" ]]; then
  "${cmd[@]}"
fi
