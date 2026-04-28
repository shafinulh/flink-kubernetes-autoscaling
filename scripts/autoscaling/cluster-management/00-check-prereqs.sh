#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../../cluster/config/env.sh"

missing=()

need_local() {
  command -v "$1" >/dev/null 2>&1 || missing+=("$1")
}

need_remote() {
  ssh -o BatchMode=yes "$1" "command -v $2 >/dev/null" || missing+=("$1:$2")
}

echo "Control-plane: ${CONTROL_PLANE}"
for bin in docker kubeadm kubelet kubectl helm containerd; do
  need_local "${bin}"
done

for node in "${WORKERS[@]}"; do
  echo "Worker: ${node}"
  ssh -o BatchMode=yes -o ConnectTimeout=5 "${node}" "true" || missing+=("${node}:ssh")
  for bin in kubeadm kubelet kubectl containerd; do
    need_remote "${node}" "${bin}"
  done
done

if [[ "${#missing[@]}" -gt 0 ]]; then
  printf 'Missing prerequisites:\n'
  printf '  %s\n' "${missing[@]}"
  exit 1
fi

echo "Prerequisites found."
