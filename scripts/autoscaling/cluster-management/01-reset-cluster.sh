#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../../cluster/config/env.sh"

if [[ "$(hostname -s)" != "${CONTROL_PLANE}" ]]; then
  echo "Run this script on the control-plane host (${CONTROL_PLANE}); current host is $(hostname -s)." >&2
  exit 1
fi

echo "This will reset and recreate the Kubernetes cluster on ${ALL_NODES[*]}."
read -r -p "Type 'yes' to continue: " confirm
[[ "${confirm}" == "yes" ]] || exit 0

run_root() {
  sudo -n "$@"
}

ssh_root() {
  local host="$1"
  shift
  ssh "${host}" "sudo -n $*"
}

run_root true
for worker in "${WORKERS[@]}"; do
  ssh_root "${worker}" "true"
  ssh "${worker}" "sudo -n kubeadm reset -f --cri-socket ${CRI_RUNTIME_ENDPOINT} || true"
  ssh "${worker}" "sudo -n rm -rf /etc/cni/net.d /var/lib/etcd /var/lib/kubelet/*"
  ssh "${worker}" "sudo -n systemctl restart containerd kubelet"
done

run_root kubeadm reset -f --cri-socket "${CRI_RUNTIME_ENDPOINT}" || true
run_root rm -rf /etc/cni/net.d /var/lib/etcd
run_root systemctl restart containerd kubelet

run_root kubeadm init \
  --cri-socket "${CRI_RUNTIME_ENDPOINT}" \
  --apiserver-advertise-address="${CONTROL_PLANE_IP}" \
  --pod-network-cidr=192.168.0.0/16 \
  --node-name="${CONTROL_PLANE}" \
  --control-plane-endpoint="${CONTROL_PLANE_IP}:6443"

mkdir -p "${HOME}/.kube"
run_root cp -f /etc/kubernetes/admin.conf "${HOME}/.kube/config"
run_root chown "$(id -u):$(id -g)" "${HOME}/.kube/config"
run_root install -d -m 750 -o root -g "${KUBECONFIG_GROUP}" "$(dirname "${SHARED_KUBECONFIG}")"
run_root install -m 640 -o root -g "${KUBECONFIG_GROUP}" /etc/kubernetes/admin.conf "${SHARED_KUBECONFIG}"
export KUBECONFIG="${SHARED_KUBECONFIG}"

kubectl apply -f https://raw.githubusercontent.com/projectcalico/calico/v3.27.0/manifests/calico.yaml

join_cmd="$(kubeadm token create --print-join-command)"
for worker in "${WORKERS[@]}"; do
  ssh "${worker}" "sudo -n ${join_cmd} --cri-socket ${CRI_RUNTIME_ENDPOINT} --node-name=${worker}"
done

kubectl get nodes -o wide
