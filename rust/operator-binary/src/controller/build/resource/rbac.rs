//! Builds the RBAC resources (ServiceAccount + RoleBinding) shared by all role groups.

use stackable_operator::{
    k8s_openapi::api::{core::v1::ServiceAccount, rbac::v1::RoleBinding},
    v2::rbac,
};

use crate::controller::{ValidatedCluster, build::recommended_labels_for_cluster_resources};

/// Builds the [`ServiceAccount`] that the role-group Pods and the Kubernetes-executor Pods run
/// under.
pub fn build_service_account(cluster: &ValidatedCluster) -> ServiceAccount {
    rbac::build_service_account(
        cluster,
        &cluster.cluster_resource_names(),
        recommended_labels_for_cluster_resources(cluster),
    )
}

/// Builds the [`RoleBinding`] that binds the [`ServiceAccount`] from [`build_service_account`] to
/// the operator-deployed ClusterRole.
pub fn build_role_binding(cluster: &ValidatedCluster) -> RoleBinding {
    rbac::build_role_binding(
        cluster,
        &cluster.cluster_resource_names(),
        recommended_labels_for_cluster_resources(cluster),
    )
}
