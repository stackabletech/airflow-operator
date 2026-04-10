<!-- markdownlint-disable MD034 -->
# Helm Chart for Stackable Operator for Apache Airflow

This Helm Chart can be used to install Custom Resource Definitions and the Operator for Apache Airflow provided by Stackable.

## Requirements

- Create a [Kubernetes Cluster](../Readme.md)
- Install [Helm](https://helm.sh/docs/intro/install/)

## Install the Stackable Operator for Apache Airflow

```bash
# From the root of the operator repository
make compile-chart

helm install airflow-operator deploy/helm/airflow-operator
```

## Publish The Chart

The Helm chart is published to two different repositories:

- oci.stackable.tech
- quay.io

Each chart version references images from it's corresponding repository.

Package and publish both variants:

```bash
make chart-package-all
make chart-publish-all
```

Package and publish for oci.stackable.tech:

```bash
make chart-package-oci
make chart-publish-oci
```

Package and publish for quay.io:

```bash
make chart-package-quay
make chart-publish-quay
```

Install from oci.stackable.tech:

```bash
helm install airflow-operator oci://oci.stackable.tech/sdp/charts/airflow-operator --version 0.0.0-dev
```

Install from quay.io:

```shell
helm install airflow-operator oci://quay.io/stackable/charts/airflow-operator --version 0.0.0-dev
```

## Usage of the CRDs

The usage of this operator and its CRDs is described in the [documentation](https://docs.stackable.tech/airflow/index.html)

The operator has example requests included in the [`/examples`](https://github.com/stackabletech/airflow-operator/tree/main/examples) directory.

## Links

<https://github.com/stackabletech/airflow-operator>
