use crate::authentication::AirflowAuthentication;
use crate::git_sync::GitSync;
use crate::{
    AirflowClusterStatus, AirflowConfigFragment, AirflowExecutor, CurrentlySupportedListenerClasses,
};
use serde::{Deserialize, Serialize};
use stackable_operator::{
    commons::{cluster_operation::ClusterOperation, product_image_selection::ProductImage},
    k8s_openapi::api::core::v1::{Volume, VolumeMount},
    kube::CustomResource,
    role_utils::Role,
    schemars::{self, JsonSchema},
};

/// An Airflow cluster stacklet. This resource is managed by the Stackable operator for Apache Airflow.
/// Find more information on how to use it and the resources that the operator generates in the
/// [operator documentation](DOCS_BASE_URL_PLACEHOLDER/airflow/).
///
/// The CRD contains three roles: webserver, scheduler and worker/celeryExecutor.
/// You can use either the celeryExecutor or the kubernetesExecutor.
#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "airflow.stackable.tech",
    version = "v1alpha1",
    kind = "AirflowCluster",
    plural = "airflowclusters",
    shortname = "airflow",
    status = "AirflowClusterStatus",
    namespaced,
    crates(
        kube_core = "stackable_operator::kube::core",
        k8s_openapi = "stackable_operator::k8s_openapi",
        schemars = "stackable_operator::schemars"
    )
)]
#[serde(rename_all = "camelCase")]
pub struct AirflowClusterSpec {
    // no doc string - See ProductImage struct
    pub image: ProductImage,

    /// Configuration that applies to all roles and role groups.
    /// This includes settings for authentication, git sync, service exposition and volumes, among other things.
    pub cluster_config: AirflowClusterConfig,

    // no doc string - See ClusterOperation struct
    #[serde(default)]
    pub cluster_operation: ClusterOperation,

    /// The `webserver` role provides the main UI for user interaction.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub webservers: Option<Role<AirflowConfigFragment>>,

    /// The `scheduler` is responsible for triggering jobs and persisting their metadata to the backend database.
    /// Jobs are scheduled on the workers/executors.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedulers: Option<Role<AirflowConfigFragment>>,

    #[serde(flatten)]
    pub executor: AirflowExecutor,
}

#[derive(Clone, Deserialize, Debug, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AirflowClusterConfig {
    #[serde(flatten)]
    pub authentication: AirflowAuthentication,
    /// The name of the Secret object containing the admin user credentials and database connection details.
    /// Read the
    /// [getting started guide first steps](DOCS_BASE_URL_PLACEHOLDER/airflow/getting_started/first_steps)
    /// to find out more.
    pub credentials_secret: String,
    /// The `gitSync` settings allow configuring DAGs to mount via `git-sync`.
    /// Learn more in the
    /// [mounting DAGs documentation](DOCS_BASE_URL_PLACEHOLDER/airflow/usage-guide/mounting-dags#_via_git_sync).
    #[serde(default)]
    pub dags_git_sync: Vec<GitSync>,
    /// for internal use only - not for production use.
    #[serde(default)]
    pub expose_config: bool,
    /// Whether to load example DAGs or not; defaults to false. The examples are used in the
    /// [getting started guide](DOCS_BASE_URL_PLACEHOLDER/airflow/getting_started/).
    #[serde(default)]
    pub load_examples: bool,
    /// This field controls which type of Service the Operator creates for this AirflowCluster:
    ///
    /// * cluster-internal: Use a ClusterIP service
    ///
    /// * external-unstable: Use a NodePort service
    ///
    /// * external-stable: Use a LoadBalancer service
    ///
    /// This is a temporary solution with the goal to keep yaml manifests forward compatible.
    /// In the future, this setting will control which [ListenerClass](DOCS_BASE_URL_PLACEHOLDER/listener-operator/listenerclass.html)
    /// will be used to expose the service, and ListenerClass names will stay the same, allowing for a non-breaking change.
    #[serde(default)]
    pub listener_class: CurrentlySupportedListenerClasses,

    /// Name of the Vector aggregator [discovery ConfigMap](DOCS_BASE_URL_PLACEHOLDER/concepts/service_discovery).
    /// It must contain the key `ADDRESS` with the address of the Vector aggregator.
    /// Follow the [logging tutorial](DOCS_BASE_URL_PLACEHOLDER/tutorials/logging-vector-aggregator)
    /// to learn how to configure log aggregation with Vector.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_aggregator_config_map_name: Option<String>,

    /// Additional volumes to define. Use together with `volumeMounts` to mount the volumes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volumes: Option<Vec<Volume>>,

    /// Additional volumes to mount. Use together with `volumes` to define volumes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_mounts: Option<Vec<VolumeMount>>,
}

impl AirflowCluster {}
