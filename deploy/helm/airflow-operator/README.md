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

## Publish The Chart To Multiple Registries

This chart is structured so the operator image can be sourced from different registries without
duplicating templates.

- `values.yaml` contains the default image coordinates (`image.registry` and `image.repository`).
- `values.registry-oci.yaml` sets defaults for chart artifacts published to `oci.stackable.tech`.
- `values.registry-quay.yaml` sets defaults for chart artifacts published to `quay.io`.

Package each artifact with a registry-specific `values.yaml` so users of each chart registry
automatically pull images from the same source registry.

`helm package` does not accept a values overlay directly, so create a temporary chart copy per
target and merge values before packaging:

```bash
# Package chart with oci.stackable.tech defaults
tmp_oci="$(mktemp -d)"
cp -r deploy/helm/airflow-operator "${tmp_oci}/"
yq ea '. as $item ireduce ({}; . * $item )' \
    "${tmp_oci}/airflow-operator/values.yaml" \
    "${tmp_oci}/airflow-operator/values.registry-oci.yaml" \
    > "${tmp_oci}/airflow-operator/values.yaml.new"
mv "${tmp_oci}/airflow-operator/values.yaml.new" "${tmp_oci}/airflow-operator/values.yaml"
helm package "${tmp_oci}/airflow-operator" --destination /tmp/charts-oci

# Package chart with quay.io defaults
tmp_quay="$(mktemp -d)"
cp -r deploy/helm/airflow-operator "${tmp_quay}/"
yq ea '. as $item ireduce ({}; . * $item )' \
    "${tmp_quay}/airflow-operator/values.yaml" \
    "${tmp_quay}/airflow-operator/values.registry-quay.yaml" \
    > "${tmp_quay}/airflow-operator/values.yaml.new"
mv "${tmp_quay}/airflow-operator/values.yaml.new" "${tmp_quay}/airflow-operator/values.yaml"
helm package "${tmp_quay}/airflow-operator" --destination /tmp/charts-quay
```

Then push the packaged chart from `/tmp/charts-oci` to `oci.stackable.tech` and the packaged chart
from `/tmp/charts-quay` to `quay.io`.

## Usage of the CRDs

The usage of this operator and its CRDs is described in the [documentation](https://docs.stackable.tech/airflow/index.html)

The operator has example requests included in the [`/examples`](https://github.com/stackabletech/airflow-operator/tree/main/examples) directory.

## Links

<https://github.com/stackabletech/airflow-operator>
