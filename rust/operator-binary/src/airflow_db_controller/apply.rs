use stackable_airflow_crd::airflowdb::AirflowDB;

use std::sync::Arc;
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{common::rbac, AIRFLOW_DB_CONTROLLER_NAME};

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    client::Client,
    kube::{
        runtime::{controller::Action, reflector::ObjectRef},
        ResourceExt,
    },
};

use super::types::BuiltClusterResource;

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("failed to patch service account: {source}"))]
    PatchServiceAccount {
        name: String,
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to patch role binding: {source}"))]
    PatchRoleBinding {
        name: String,
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to patch ConfigMap [{name}]"))]
    PatchConfigMap {
        name: String,
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to apply Job for {}", airflow_db))]
    ApplyJob {
        source: stackable_operator::error::Error,
        airflow_db: ObjectRef<AirflowDB>,
    },
    #[snafu(display("failed to update status"))]
    UpdateStatus {
        source: stackable_operator::error::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub async fn apply_cluster_resources(
    client: &Client,
    airflow_db: Arc<AirflowDB>,
    built_cluster_resources: Vec<BuiltClusterResource>,
) -> Result<Action> {
    for cluster_resource in built_cluster_resources {
        match cluster_resource {
            BuiltClusterResource::PatchRBAC => {
                let (rbac_sa, rbac_rolebinding) =
                    rbac::build_rbac_resources(airflow_db.as_ref(), "airflow");

                client
                    .apply_patch(AIRFLOW_DB_CONTROLLER_NAME, &rbac_sa, &rbac_sa)
                    .await
                    .with_context(|_| PatchServiceAccountSnafu {
                        name: rbac_sa.name_unchecked(),
                    })?;
                client
                    .apply_patch(
                        AIRFLOW_DB_CONTROLLER_NAME,
                        &rbac_rolebinding,
                        &rbac_rolebinding,
                    )
                    .await
                    .with_context(|_| PatchRoleBindingSnafu {
                        name: rbac_rolebinding.name_unchecked(),
                    })?;
            }
            BuiltClusterResource::PatchConfigMap(config_map) => {
                client
                    .apply_patch(AIRFLOW_DB_CONTROLLER_NAME, &config_map, &config_map)
                    .await
                    .context(PatchConfigMapSnafu {
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
                    .context(UpdateStatusSnafu)?;
            }
            BuiltClusterResource::PatchJobStatus(status) => {
                client
                    .apply_patch_status(AIRFLOW_DB_CONTROLLER_NAME, &*airflow_db, &status)
                    .await
                    .context(UpdateStatusSnafu)?;
            }
        }
    }

    Ok(Action::await_change())
}
