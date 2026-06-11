<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Helm Chart for Apache Celeborn

[Apache Celeborn](https://celeborn.apache.org) is an intermediate data service for Big Data compute engines (i.e. ETL, OLAP and Streaming engines) to boost performance, stability, and flexibility. Intermediate data typically include shuffle and spilled data.

## Introduction

This chart will bootstrap an [Celeborn](https://celeborn.apache.org) deployment on a [Kubernetes](http://kubernetes.io) cluster using the [Helm](https://helm.sh) package manager.

## Requirements

Configure kubectl to connect to the Kubernetes cluster.

- [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl)
- [Helm 3.0+](https://helm.sh/docs/using_helm/#installing-helm)

## Template rendering

When you want to test the template rendering, but not actually install anything. [Debugging templates](https://helm.sh/docs/chart_template_guide/debugging/) provide a quick way of viewing the generated content without YAML parse errors blocking.

There are two ways to render templates. It will return the rendered template to you so you can see the output.

- Local rendering chart templates

```shell
helm template --debug ../celeborn
```

- Server side rendering chart templates

```shell
helm install --dry-run --debug --generate-name ../celeborn
```

More details in [Helm Install](https://helm.sh/docs/helm/helm_install/).
The chart can be customized using the following [celeborn configurations](https://celeborn.apache.org/docs/latest/configuration/#important-configurations).
Specify parameters using `--set key=value[,key=value]` argument to `helm install`.

## Configuration highlights

### Internal ports (required for Celeborn auth / SASL)

Set `internalPort.enabled=true` to create a separate internal RPC port on the
masters and workers (`celeborn.internal.port.enabled=true`). The chart wires
`celeborn.master.internal.port` / `celeborn.master.ha.node.<id>.internal.port`
(`internalPort.masterPort`, default `8097`) and `celeborn.worker.internal.port`
(`internalPort.workerPort`, default `8093`), builds the internal master endpoints,
and exposes the ports on the StatefulSets and headless Services. This is the
prerequisite for enabling `celeborn.auth.enabled`.

### Hadoop client config for DFS storage

For the HDFS / Ozone `ofs://` / S3 storage path, create a Secret holding your
Hadoop client config files and point the chart at it with `configsSecretName`:

```shell
kubectl create secret generic celeborn-hadoop-conf \
  --from-file=core-site.xml --from-file=hdfs-site.xml
helm install celeborn ../celeborn \
  --set configsSecretName=celeborn-hadoop-conf
```

The keys listed in `hadoopConfigFiles` (default `core-site.xml`) are mounted into
each master/worker pod under `/opt/celeborn/conf/`. Sample files live in `envs/`.

### Private image registry

Either reference an existing docker-registry Secret with `image.pullSecret.name`,
or let the chart render a `kubernetes.io/dockerconfigjson` Secret from
`image.pullSecret.credentials` (`registry` / `username` / `password`). Additional
secret names can still be listed under `imagePullSecrets`.

### Cluster domain

`clusterDomain` (default `cluster.local`) is used to build the in-cluster service
FQDNs; override it for clusters that use a non-default domain.

## Documentation

For additional details on deploying the Celeborn Kubernetes Helm chart, please refer to the [Celeborn on Kubernetes](https://celeborn.apache.org/docs/latest/deploy_on_k8s/) documentation.
