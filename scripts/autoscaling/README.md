# Autoscaling Scripts

Operational scripts for the Kubernetes autoscaling stack. Static configuration
and Kubernetes manifests live under `cluster/`.

## Cluster Management

Run these from the repository root in setup order:

```bash
scripts/autoscaling/cluster-management/00-check-prereqs.sh
scripts/autoscaling/cluster-management/01-reset-cluster.sh
scripts/autoscaling/cluster-management/02-label-nodes.sh
scripts/autoscaling/cluster-management/03-deploy-monitoring.sh
scripts/autoscaling/cluster-management/04-deploy-operator.sh
```

Other cluster commands:

```bash
scripts/autoscaling/cluster-management/cluster-status.sh
scripts/autoscaling/cluster-management/02b-reset-autoscaling-stack.sh
scripts/autoscaling/cluster-management/redeploy-operator.sh
```

`02b-reset-autoscaling-stack.sh` removes the FlinkDeployment, operator,
monitoring releases, and related namespaces without rebuilding kubeadm. After
using it, replay from `03-deploy-monitoring.sh`, then `04-deploy-operator.sh`.

Preview the operator Helm command without touching the cluster:

```bash
scripts/autoscaling/cluster-management/04-deploy-operator.sh --dry-run
```

## Job Management

```bash
scripts/autoscaling/job-management/submit-job.sh experiments/1724-kafka-q20-unique/jobs/q20_unique-sql-ssd-kafka-justin-rocksdb-options.yaml
scripts/autoscaling/job-management/job-status.sh
scripts/autoscaling/job-management/stop-job.sh
```

## Job Monitoring

```bash
scripts/autoscaling/job-monitoring/port-forward.sh start
scripts/autoscaling/job-monitoring/observe-scaling.py --follow
scripts/autoscaling/status.sh
```
