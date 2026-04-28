# Image Builds

This document explains how the Flink autoscaling images are built, tagged, and
pushed. For a shorter command reference, see [`images/README.md`](../images/README.md).

## Image Roles

- `flink-runtime`: base Flink runtime built from the selected Flink source tree.
- `flink-benchmark-runtime`: runtime image used by benchmark jobs. It adds
  Nexmark DataStream benchmarks, the custom three-table unique SQL workloads,
  RocksDB options support, and Kafka connector/client jars.
- `flink-kubernetes-operator`: operator image that manages Flink deployments
  and runs the autoscaling logic.

## Current Image Management

A registry container is running a private Docker registry. A Docker registry is
a server that stores Docker images and handles image push and pull requests.

`c165` hosts the registry on port `5000`. The cluster config sets `REGISTRY` to
`142.150.234.165:5000`, and `scripts/images/push-images.sh` tags local images
with that registry address before pushing them.

Worker nodes `c167` and `c182` pull images from the registry through
`c165:5000` / `142.150.234.165:5000`. Their containerd registry configuration is
stored at:

```text
/etc/containerd/certs.d/142.150.234.165:5000/hosts.toml
```

After pushing the benchmark runtime image, `scripts/images/push-images.sh`
pre-pulls it on the configured cluster nodes with `crictl`. By default this
includes `c165`, `c182`, and `c167`. Disable the pre-pull with `--no-prepull` or
`PREPULL_BENCHMARK_IMAGE=false`.

## Defaults

Default local image tags:

```text
flink-runtime:base-1-18-from-justin
flink-benchmark-runtime:sql-three-tables-unique
flink-kubernetes-operator:justin-modified
```

Default registry image tags:

```text
142.150.234.165:5000/flink-runtime:base-1-18-from-justin
142.150.234.165:5000/flink-benchmark-runtime:sql-three-tables-unique
142.150.234.165:5000/flink-kubernetes-operator:justin-modified
```

Default source paths:

```text
FLINK_RUNTIME_SOURCE_DIR=sources/flink/flink-1-18-src-from-justin
DATASTREAM_BENCHMARK_SOURCE_DIR=sources/benchmarks/nexmark-datastream-benchmarks-from-justin
NEXMARK_SOURCE_DIR=sources/benchmarks/nexmark-sql-benchmarks
ROCKSDB_OPTIONS_SOURCE_DIR=sources/libs/rocksdb-options
OPERATOR_SOURCE_DIR=sources/operators/flink-kubernetes-operator-justin
```

Image names, tags, source paths, and Maven artifact names are defined in
`scripts/env.sh`. Cluster-facing scripts source `cluster/config/env.sh`, which
imports those image defaults and then applies cluster-specific registry and node
settings.

## Build and Publish Commands

Build the configured default benchmark runtime and operator images:

```bash
scripts/images/build-all-default.sh
```

Rebuild the base Flink runtime image as part of the same run. This can take 20+
minutes:

```bash
scripts/images/build-all-default.sh --with-runtime
```

Build an individual image:

```bash
images/flink-runtime/build.sh
images/flink-benchmark-runtime/build.sh
images/flink-kubernetes-operator/build.sh
```

Verify local image tags and expected benchmark runtime jars:

```bash
scripts/images/verify-images.sh
```

Tag and push the configured images to the private registry:

```bash
scripts/images/push-images.sh all
```

`--no-cache` passes Docker `--no-cache` to the selected image builds.
`--force-datastream-build` rebuilds the DataStream benchmark jars during the
benchmark runtime build; that rebuild can take about 10 minutes.

## Benchmark Runtime Inputs

`images/flink-benchmark-runtime/build.sh` builds or collects the workload jars,
creates a temporary Docker build context, and then builds the benchmark runtime
image.

The benchmark runtime image contains:

- DataStream benchmark jars in `/opt/flink/examples/justin`
- SQL Nexmark jar in `/opt/flink/lib`
- RocksDB options jar in `/opt/flink/lib`
- Kafka SQL connector and Kafka client jars in `/opt/flink/lib`

Helper scripts for those inputs live under `images/flink-benchmark-runtime/scripts/`:

- `build-datastream-benchmark-jars.sh`
- `build-nexmark-sql-jars.sh`
- `build-rocksdb-options-jar.sh`

## Variant Builds

Each image build supports source and tag overrides:

```bash
images/flink-runtime/build.sh \
  --source-dir sources/flink/my-custom-flink \
  --tag custom-flink-tag

images/flink-benchmark-runtime/build.sh \
  --runtime-image flink-runtime:custom-flink-tag \
  --datastream-source-dir sources/benchmarks/my-custom-datastream \
  --nexmark-source-dir sources/benchmarks/my-custom-nexmark \
  --rocksdb-source-dir sources/libs/my-custom-rocksdb-options \
  --tag custom-benchmark-tag

images/flink-kubernetes-operator/build.sh \
  --source-dir sources/operators/my-custom-operator \
  --tag custom-operator-tag
```

Use `--image-name`, `--tag`, or `--local-image` to control the local image
reference. Use `--source-dir` options to select different source trees.
