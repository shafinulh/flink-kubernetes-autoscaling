#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../../cluster/config/env.sh"

echo "This will reset and recreate the Kubernetes cluster on ${ALL_NODES[*]}."
read -r -p "Type 'yes' to continue: " confirm
[[ "${confirm}" == "yes" ]] || exit 0

for worker in "${WORKERS[@]}"; do
  ssh "${worker}" "sudo kubeadm reset -f --cri-socket ${CRI_RUNTIME_ENDPOINT} || true"
  ssh "${worker}" "sudo rm -rf /etc/cni/net.d /var/lib/etcd /var/lib/kubelet/*"
  ssh "${worker}" "sudo systemctl restart containerd kubelet"
done

sudo kubeadm reset -f --cri-socket "${CRI_RUNTIME_ENDPOINT}" || true
sudo rm -rf /etc/cni/net.d /var/lib/etcd
sudo systemctl restart containerd kubelet

sudo kubeadm init \
  --cri-socket "${CRI_RUNTIME_ENDPOINT}" \
  --apiserver-advertise-address="${CONTROL_PLANE_IP}" \
  --pod-network-cidr=192.168.0.0/16 \
  --node-name="${CONTROL_PLANE}" \
  --control-plane-endpoint="${CONTROL_PLANE_IP}:6443"

mkdir -p "${HOME}/.kube"
sudo cp -f /etc/kubernetes/admin.conf "${HOME}/.kube/config"
sudo chown "$(id -u):$(id -g)" "${HOME}/.kube/config"
sudo install -d -m 750 -o root -g users "$(dirname "${SHARED_KUBECONFIG}")"
sudo install -m 640 -o root -g users /etc/kubernetes/admin.conf "${SHARED_KUBECONFIG}"

kubectl apply -f https://raw.githubusercontent.com/projectcalico/calico/v3.27.0/manifests/calico.yaml

join_cmd="$(kubeadm token create --print-join-command)"
for worker in "${WORKERS[@]}"; do
  ssh "${worker}" "sudo ${join_cmd} --cri-socket ${CRI_RUNTIME_ENDPOINT} --node-name=${worker}"
done

kubectl get nodes -o wide
