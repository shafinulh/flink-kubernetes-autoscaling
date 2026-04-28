# Operator Deployment

Flink Kubernetes Operator Helm configuration.

Use `scripts/autoscaling/cluster-management/04-deploy-operator.sh` to install
the operator and `scripts/autoscaling/cluster-management/redeploy-operator.sh`
to reinstall it.

The timeout values from the old `f9b8575b` deployment commit can be recreated here later if validation shows they are needed:

```yaml
kubernetes.operator.flink.client.timeout: 5 min
kubernetes.operator.deployment.readiness.timeout: 10 min
```
