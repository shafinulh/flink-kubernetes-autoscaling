# Monitoring

Monitoring manifests consumed by `scripts/autoscaling/cluster-management/03-deploy-monitoring.sh`.

- `values-prom.yaml`: kube-prometheus-stack values.
- `values-loki.yaml`: Loki stack values.
- `pod-monitor.yaml`: Flink metrics PodMonitor.
- `cluster-role-binding-default.yaml`: default service account cluster binding.
- `grafana-*.json`: dashboards provisioned into Grafana.
