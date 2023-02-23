use snafu::{ResultExt, Snafu};
use stackable_airflow_crd::{AirflowCluster, APP_NAME, OPERATOR_NAME};
use stackable_operator::{
    client::Client,
    cluster_resources::ClusterResources,
    kube::{runtime::controller::Action, Resource, ResourceExt},
    role_utils::RoleGroupRef,
};

use std::sync::Arc;
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::common::rbac;

use super::{types::BuiltClusterResource, AIRFLOW_CONTROLLER_NAME};

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("failed to apply global Service"))]
    ApplyRoleService {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to apply Service for {rolegroup}"))]
    ApplyRoleGroupService {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<AirflowCluster>,
    },
    #[snafu(display("failed to apply ConfigMap for {rolegroup}"))]
    ApplyRoleGroupConfig {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<AirflowCluster>,
    },
    #[snafu(display("failed to apply StatefulSet for {rolegroup}"))]
    ApplyRoleGroupStatefulSet {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<AirflowCluster>,
    },
    #[snafu(display("failed to patch service account: {source}"))]
    ApplyServiceAccount {
        name: String,
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to patch role binding: {source}"))]
    ApplyRoleBinding {
        name: String,
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to apply Airflow DB"))]
    ApplyAirflowDB {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to create cluster resources"))]
    CreateClusterResources {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to delete orphaned resources"))]
    DeleteOrphanedResources {
        source: stackable_operator::error::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub async fn apply_cluster_resources(
    client: &Client,
    airflow: Arc<AirflowCluster>,
    built_cluster_resources: Vec<BuiltClusterResource>,
) -> Result<Action> {
    let mut cluster_resources = ClusterResources::new(
        APP_NAME,
        OPERATOR_NAME,
        AIRFLOW_CONTROLLER_NAME,
        &airflow.object_ref(&()),
    )
    .context(CreateClusterResourcesSnafu)?;

    let mut delete_orphaned_found = false;

    for cluster_resource in built_cluster_resources {
        match cluster_resource {
            BuiltClusterResource::PatchRBAC => {
                let (rbac_sa, rbac_rolebinding) =
                    rbac::build_rbac_resources(airflow.as_ref(), "airflow");
                client
                    .apply_patch(AIRFLOW_CONTROLLER_NAME, &rbac_sa, &rbac_sa)
                    .await
                    .with_context(|_| ApplyServiceAccountSnafu {
                        name: rbac_sa.name_unchecked(),
                    })?;
                client
                    .apply_patch(
                        AIRFLOW_CONTROLLER_NAME,
                        &rbac_rolebinding,
                        &rbac_rolebinding,
                    )
                    .await
                    .with_context(|_| ApplyRoleBindingSnafu {
                        name: rbac_rolebinding.name_unchecked(),
                    })?;
            }
            BuiltClusterResource::PatchAirflowDB(airflow_db) => {
                client
                    .apply_patch(AIRFLOW_CONTROLLER_NAME, &airflow_db, &airflow_db)
                    .await
                    .context(ApplyAirflowDBSnafu)?;
            }
            BuiltClusterResource::RoleService(role_service) => {
                cluster_resources
                    .add(client, &role_service)
                    .await
                    .context(ApplyRoleServiceSnafu)?;
            }
            BuiltClusterResource::RolegroupService(rg_service, rolegroup) => {
                cluster_resources.add(client, &rg_service).await.context(
                    ApplyRoleGroupServiceSnafu {
                        rolegroup: rolegroup.clone(),
                    },
                )?;
            }
            BuiltClusterResource::RolegroupConfigMap(rg_configmap, rolegroup) => {
                cluster_resources
                    .add(client, &rg_configmap)
                    .await
                    .with_context(|_| ApplyRoleGroupConfigSnafu {
                        rolegroup: rolegroup.clone(),
                    })?;
            }
            BuiltClusterResource::RolegroupStatefulSet(rg_statefulset, rolegroup) => {
                cluster_resources
                    .add(client, &rg_statefulset)
                    .await
                    .context(ApplyRoleGroupStatefulSetSnafu {
                        rolegroup: rolegroup.clone(),
                    })?;
            }
            BuiltClusterResource::DeleteOrphaned => {
                delete_orphaned_found = true; // due to move semantics
            }
        }
    }

    if delete_orphaned_found {
        cluster_resources
            .delete_orphaned_resources(client)
            .await
            .context(DeleteOrphanedResourcesSnafu)?;
    }

    Ok(Action::await_change())
}
