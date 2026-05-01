# A4S Implementation Summary

This is the high-level documentation of the A4S logic flow across the Flink Kubernetes Operator, Flink Runtime, and RocksDB. It includes key files where the majority of the implementation and application logic lives. For how to deploy the stack, see [README.md](README.md).

## Kubernetes Operator 1.13

The operator adds A4S-aware autoscaling logic on top of the existing autoscaler and Justin integration paths. It fetches per-vertex miss-rate curves from Flink runtime, evaluates memory/parallelism trade-offs, and publishes decisions through existing override and state-store channels instead of introducing a new realization path.

### Key Files

| File                           | Responsibility                                                                                  |
| ------------------------------ | ----------------------------------------------------------------------------------------------- |
| `AutoScalerOptions.java`       | Defines A4S and Justin configuration options that can be controlled at runtime via yaml options |
| `RestApiMetricsCollector.java` | Calls runtime A4S metrics endpoints and ingests vertex-level MRC data.                          |
| `ScalingMetricEvaluator.java`  | Converts MRC inputs into memory/parallelism evaluation curves.                                  |
| `A4S.java`                     | Implements the key logic for the A4S scaling algorithm                                          |
| `ScalingExecutor.java`         | Maps A4S decisions into Justin-compatible overrides and persists them.                          |
| `MemoryParallelismCurve.java`  | Represents the evaluated memory/parallelism frontier used by A4S.                               |

## Flink Runtime 1.18

Flink Runtime changes expose A4S metrics over REST through the Job Manager. The Job Manager aggregates RocksDB-derived histogram metrics at vertex scope on individual Task Managers via RPC and returns a standardized `scaledMrc` payload for operator-side policy evaluation. Metrics are exposed under the new `GET /jobs/:jobid/vertices/:vertexid/a4s-metrics` API which is queried by the Kubernetes Operator and can also be queried manually.

### Key Files

| File                                           | Responsibility                                                                                                                 |
| ---------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `StackDistanceHistogram.java`                  | Represents a stack distance histogram retrieved from RocksDB                                                                   |
| `MissRateCurve.java`                           | Represents a miss-rate curve generated from a stack distance histogram. Encapsulates key logic and semantics for what a MRC is |
| `RocksDBNativeMetricMonitor.java`              | Retrieves stack-distance histogram data from native RocksDB monitoring.                                                        |
| `A4SAggregatedVertexMetricsHeaders.java`       | Declares the `GET /jobs/:jobid/vertices/:vertexid/a4s-metrics` REST contract.                                                  |
| `A4SAggregatingVertexMetricsHandler.java`      | Aggregates per-subtask metrics into a vertex-level MRC response.                                                               |
| `A4SAggregatedMetricsResponseBody.java`        | Defines response payload shape with `scaledMrc`.                                                                               |
| `JustinResourceRequirementsHeaders.java`       | Declares `GET /jobs/:jobid/justin` contract.                                                                                   |
| `JustinResourceRequirementsUpdateHeaders.java` | Declares `PUT /jobs/:jobid/justin` contract.                                                                                   |
| `JustinResourceRequirementsUpdateHandler.java` | Accepts and validates Justin override updates from clients.                                                                    |
| `AdaptiveScheduler.java`                       | Applies resource requirement updates through existing adaptive scheduling flow.                                                |

## FRocksDB 6.20.3

FRocksDB provides the low-level histogram signal required by A4S. The branch depends on custom RocksDB JNI/native support so stack-distance metrics can be collected in task managers at the task slot level and surfaced to runtime metric collection.

### Key Files

| File            | Responsibility                                                                                       |
| --------------- | ---------------------------------------------------------------------------------------------------- |
| `lru_cache.cc`  | Implementation of the QuickMRC logic and exposes interfaces for retrieving stack distance histograms |
| `LRUCache.java` | Exposes the JNI bindings for Flink to call into RocksDB to retrieve stack distance histograms        |

## End-to-End Flow

1. A task manager collect stack-distance histogram metrics emitted from RocksDB from each of its task slots. This is stored in the task manager's memory. This happens periodically (on the order of seconds) without intervention
2. The task manager resets stack-distance histogram metrics in RocksDB after fetching this batch.
3. Histogram data is serialized and sent to the Job Manager via RPC when requested.
4. Job Manager gathers subtask values for each operator vertex and merges them and generate scaled miss-rate curves.
5. Miss-rate curve metrics are published via `GET /jobs/:jobid/vertices/:vertexid/a4s-metrics` at the vertex level.
6. The operator autoscaler fetches per-vertex `scaledMrc` data from runtime.
7. Evaluation converts MRC points into a memory/parallelism curve using throughput and latency assumptions.
8. A4S selects `(parallelism, memoryMB)` candidates with bounded retry/attempt logic.
9. The operator publishes decisions through Resource Profile Overrides
10. Runtime receives Justin updates via `GET/PUT /jobs/:jobid/justin` and realizes them via existing scheduler paths.

## Configuration

Enable A4S through existing autoscaler configuration keys:

```yaml
job.autoscaler.enabled: true
job.autoscaler.scaling.enabled: true
job.autoscaler.justin.enabled: true
job.autoscaler.a4s.enabled: true
job.autoscaler.a4s.max-attempts: 20
job.autoscaler.a4s.hit-latency-sec: 0.000005
job.autoscaler.a4s.miss-latency-sec: 0.005
```

Optional tuning keys:

```yaml
job.autoscaler.a4s.managed-memory-override.enabled: false
job.autoscaler.a4s.managed-memory-override.mb: 0
job.autoscaler.a4s.memory-base.mb: 0.0
```
