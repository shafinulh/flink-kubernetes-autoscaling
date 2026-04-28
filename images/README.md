# Images

## Image Commands

Build the default benchmark runtime and operator images:

```bash
scripts/images/build-all-default.sh
```

Rebuild the base Flink runtime image as part of the same run. This can take 20+
minutes:

```bash
scripts/images/build-all-default.sh --with-runtime
```

Verify that the configured local image tags exist and that the benchmark image
contains the expected jars:

```bash
scripts/images/verify-images.sh
```

Tag and push configured images to the registry:

```bash
scripts/images/push-images.sh all
```

Defaults for image names, tags, source paths, and registry values are defined in
`scripts/env.sh`. Cluster-specific registry and node settings are defined in
`cluster/config/env.sh`.

## Flink Runtime Image

Builds the base Flink runtime image from a selected Flink source tree.

```bash
images/flink-runtime/build.sh
images/flink-runtime/build.sh \
  --source-dir sources/flink/my-custom-flink \
  --tag custom-flink-tag
```

Use `--image-name`, `--tag`, or `--local-image` to control the local image
reference. Use `--source-dir` to select a different Flink source tree. Building
the runtime image can take 20+ minutes.

## Flink Benchmark Runtime Image

Builds the benchmark runtime image on top of a local Flink runtime image. The
base runtime image must exist locally before this build runs.

The build stages these artifacts into the image:

- DataStream benchmark jars in `/opt/flink/examples/justin`
- SQL Nexmark jar in `/opt/flink/lib`
- RocksDB options jar in `/opt/flink/lib`
- Kafka SQL connector and Kafka client jars in `/opt/flink/lib`

```bash
images/flink-benchmark-runtime/build.sh
images/flink-benchmark-runtime/build.sh \
  --runtime-image flink-runtime:custom-flink-tag \
  --datastream-source-dir sources/benchmarks/my-custom-datastream \
  --nexmark-source-dir sources/benchmarks/my-custom-nexmark \
  --rocksdb-source-dir sources/libs/my-custom-rocksdb-options \
  --tag custom-benchmark-tag
```

The script builds missing workload jars before running `docker build`. Existing
DataStream jars are reused when they are already present in the selected source
tree. Use `--force-datastream-build` to rebuild DataStream jars even when the
target jar files already exist. Rebuilding the DataStream benchmark jars can
take about 10 minutes.

## Flink Kubernetes Operator Image

Builds the operator image from an operator source tree. The Dockerfile is owned
by the selected operator source repository.

```bash
images/flink-kubernetes-operator/build.sh
images/flink-kubernetes-operator/build.sh \
  --source-dir sources/operators/my-custom-operator \
  --tag custom-operator-tag
```

Use `--source-dir` to select a different operator fork. Use `--image-name`,
`--tag`, or `--local-image` to control the local image reference.
