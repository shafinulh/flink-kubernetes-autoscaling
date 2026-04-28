# Sources

Source trees are organized by the container image build pipelines that use
them.

- `flink/`: Flink runtime sources for `images/flink-runtime`.
- `benchmarks/`: benchmark and workload sources for `images/flink-benchmark-runtime`.
- `libs/`: support libraries copied into the benchmark runtime image.
- `operators/`: Flink Kubernetes Operator sources for `images/flink-kubernetes-operator`.

Place custom forks beside the default source trees, then pass the source
directory and image tag to the relevant build script. You can also set the same
values through environment variables. For example:

```bash
images/flink-runtime/build.sh \
  --source-dir sources/flink/my-custom-flink \
  --tag custom-flink-tag

images/flink-kubernetes-operator/build.sh \
  --source-dir sources/operators/my-custom-operator \
  --tag custom-operator-tag
```

Default source selections are defined in `scripts/env.sh`:

```text
FLINK_RUNTIME_SOURCE_DIR
DATASTREAM_BENCHMARK_SOURCE_DIR
NEXMARK_SOURCE_DIR
ROCKSDB_OPTIONS_SOURCE_DIR
OPERATOR_SOURCE_DIR
```
