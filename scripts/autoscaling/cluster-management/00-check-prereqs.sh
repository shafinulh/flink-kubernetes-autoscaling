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

if ! sudo -n true >/dev/null 2>&1; then
  missing+=("local:passwordless-sudo")
fi

if [[ "$(id -u)" -ne 0 && -r "${SHARED_KUBECONFIG}" ]]; then
  kubeconfig_gid="$(getent group "${KUBECONFIG_GROUP}" | cut -d: -f3 || true)"
  if [[ -z "${kubeconfig_gid}" ]] || ! id -G | tr ' ' '\n' | grep -qx "${kubeconfig_gid}"; then
    missing+=("local:${USER}:not-in-${KUBECONFIG_GROUP}")
  fi
fi

for node in "${WORKERS[@]}"; do
  echo "Worker: ${node}"
  ssh -o BatchMode=yes -o ConnectTimeout=5 "${node}" "true" || missing+=("${node}:ssh")
  ssh -o BatchMode=yes -o ConnectTimeout=5 "${node}" "sudo -n true" || missing+=("${node}:passwordless-sudo")
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
