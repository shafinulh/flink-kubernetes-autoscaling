# Cluster

Cluster configuration and Kubernetes manifests for the autoscaling stack.
Operational commands live under `scripts/autoscaling/`. Image definitions and
component build entrypoints live under `images/`, with aggregate image commands
under `scripts/images/`.

Contents:

- `config/env.sh`: shared machine, registry, image, kubeconfig, and path settings.
- `monitoring/`: Prometheus, Grafana, Loki, PodMonitor, and dashboard manifests.
- `operator/`: Flink Kubernetes Operator Helm values.

Machine defaults are in `config/env.sh`. Override them with environment
variables such as `CONTROL_PLANE`, `CONTROL_PLANE_IP`, `WORKER_NODES`, or
`REGISTRY`.
