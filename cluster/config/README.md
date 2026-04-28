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
