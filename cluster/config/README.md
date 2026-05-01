# Cluster Config

`env.sh` is the shared configuration entrypoint for autoscaling operations.
It defines the control-plane host, worker hosts, registry, image tags,
kubeconfig, checkpoint paths, Helm chart path, and manifest locations.

Image names, tags, and source paths are inherited from `scripts/env.sh`.

Source it from scripts instead of duplicating paths or machine names:

```bash
source cluster/config/env.sh
```

Common overrides:

```bash
CONTROL_PLANE=c165
CONTROL_PLANE_IP=142.150.234.165
WORKER_NODES="c182 c167"
REGISTRY=142.150.234.165:5000
```

The shared kubeconfig defaults to:

```text
/etc/flink-kubernetes-autoscaling/kubeconfig
```

Cluster users should be members of the Unix group configured by
`KUBECONFIG_GROUP` in `env.sh` (`users` by default). That group can read the
shared kubeconfig, so scripts that use `kubectl` or `helm` work from any
checkout path without copying credentials into each repository.

Host-level rebuilds are different from Kubernetes API operations:
`01-reset-cluster.sh` runs `kubeadm`, restarts `containerd`/`kubelet`, and
writes `/etc/kubernetes` state. Those actions still require passwordless sudo
on the control-plane and workers. Day-to-day status, monitoring, operator, and
job scripts use the shared kubeconfig and do not require sudo.
