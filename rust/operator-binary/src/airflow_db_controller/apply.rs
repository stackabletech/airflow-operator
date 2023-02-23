use stackable_airflow_crd::airflowdb::{AirflowDB, AirflowDBStatus};

use std::sync::Arc;
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{common::rbac, AIRFLOW_DB_CONTROLLER_NAME, APP_NAME, OPERATOR_NAME};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    client::Client,
    cluster_resources::ClusterResources,
    k8s_openapi::api::core::v1::Secret,
    kube::{
        runtime::{controller::Action, reflector::ObjectRef},
        Resource, ResourceExt,
    },
    role_utils::RoleGroupRef,
};

use super::types::BuiltClusterResource;

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
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
    #[snafu(display("failed to patch ConfigMap [{name}]"))]
    ApplyConfigMap {
        name: String,
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to apply Job for {}", airflow_db))]
    ApplyJob {
        source: stackable_operator::error::Error,
        airflow_db: ObjectRef<AirflowDB>,
    },
    #[snafu(display("failed to update status"))]
    ApplyStatus {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to create cluster resources"))]
    CreateClusterResources {
        source: stackable_operator::error::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub async fn apply_cluster_resources(
    client: &Client,
    airflow_db: Arc<AirflowDB>,
    built_cluster_resources: Vec<BuiltClusterResource>,
) -> Result<Action> {
    let mut cluster_resources = ClusterResources::new(
        APP_NAME,
        OPERATOR_NAME,
        AIRFLOW_DB_CONTROLLER_NAME,
        &airflow_db.object_ref(&()),
    )
    .context(CreateClusterResourcesSnafu)?;

    for cluster_resource in built_cluster_resources {
        match cluster_resource {
            BuiltClusterResource::PatchRBAC => {
                let (rbac_sa, rbac_rolebinding) =
                    rbac::build_rbac_resources(airflow_db.as_ref(), "airflow");

                client
                    .apply_patch(AIRFLOW_DB_CONTROLLER_NAME, &rbac_sa, &rbac_sa)
                    .await
                    .with_context(|_| ApplyServiceAccountSnafu {
                        name: rbac_sa.name_unchecked(),
                    })?;
                client
                    .apply_patch(
                        AIRFLOW_DB_CONTROLLER_NAME,
                        &rbac_rolebinding,
                        &rbac_rolebinding,
                    )
                    .await
                    .with_context(|_| ApplyRoleBindingSnafu {
                        name: rbac_rolebinding.name_unchecked(),
                    })?;
            }
            BuiltClusterResource::PatchConfigMap(config_map) => {
                client
                    .apply_patch(AIRFLOW_DB_CONTROLLER_NAME, &config_map, &config_map)
                    .await
                    .context(ApplyConfigMapSnafu {
                        name: config_map.name_any(),
                    })?;
            }
            BuiltClusterResource::PatchJob(job, status) => {
                client
                    .apply_patch(AIRFLOW_DB_CONTROLLER_NAME, &job, &job)
                    .await
                    .context(ApplyJobSnafu {
                        airflow_db: ObjectRef::from_obj(&*airflow_db),
                    })?;
                // The job is started, update status to reflect new state
                client
                    .apply_patch_status(
                        AIRFLOW_DB_CONTROLLER_NAME,
                        &*airflow_db,
                        &status.initializing(),
                    )
                    .await
                    .context(ApplyStatusSnafu)?;
            }
            BuiltClusterResource::PatchJobStatus(status) => {
                client
                    .apply_patch_status(AIRFLOW_DB_CONTROLLER_NAME, &*airflow_db, &status)
                    .await
                    .context(ApplyStatusSnafu)?;
            }
        }
    }

    Ok(Action::await_change())
}
