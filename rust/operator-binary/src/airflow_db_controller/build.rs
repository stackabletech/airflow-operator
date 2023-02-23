mod misc;

use crate::airflow_controller::DOCKER_IMAGE_BASE_NAME;
use crate::common::controller_commons::{
    CONFIG_VOLUME_NAME, LOG_CONFIG_VOLUME_NAME, LOG_VOLUME_NAME,
};
use crate::common::product_logging::{
    extend_config_map_with_log_config, resolve_vector_aggregator_address,
};
use crate::common::util::{env_var_from_secret, get_job_state, JobState};
use crate::common::{controller_commons, rbac};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_airflow_crd::{
    airflowdb::{
        AirflowDB, AirflowDBStatus, AirflowDBStatusCondition, AirflowDbConfig, Container,
        AIRFLOW_DB_CONTROLLER_NAME,
    },
    LOG_CONFIG_DIR, STACKABLE_LOG_DIR,
};
use stackable_operator::{
    builder::{ConfigMapBuilder, ContainerBuilder, ObjectMetaBuilder, PodSecurityContextBuilder},
    commons::product_image_selection::ResolvedProductImage,
    k8s_openapi::api::{
        batch::v1::{Job, JobSpec},
        core::v1::{ConfigMap, EnvVar, PodSpec, PodTemplateSpec, Secret},
    },
    kube::{
        runtime::{controller::Action, reflector::ObjectRef},
        ResourceExt,
    },
    logging::controller::ReconcilerError,
    product_logging::{self, spec::Logging},
    role_utils::RoleGroupRef,
};
use std::{sync::Arc, time::Duration};
use strum::{EnumDiscriminants, IntoStaticStr};

use self::misc::{build_config_map, build_init_job};

use super::types::{BuiltClusterResource, FetchedAdditionalData};

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("object has no namespace"))]
    ObjectHasNoNamespace,

    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("database state is 'initializing' but failed to find job {}", init_job))]
    GetInitializationJob {
        source: stackable_operator::error::Error,
        init_job: ObjectRef<Job>,
    },
    #[snafu(display("Failed to check whether the secret ({}) exists", secret))]
    SecretCheck {
        source: stackable_operator::error::Error,
        secret: ObjectRef<Secret>,
    },

    #[snafu(display("failed to build ConfigMap [{name}]"))]
    BuildConfig {
        name: String,
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to resolve and merge config"))]
    FailedToResolveConfig {
        source: stackable_airflow_crd::airflowdb::Error,
    },
    #[snafu(display("invalid container name"))]
    InvalidContainerName {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to resolve the Vector aggregator address"))]
    ResolveVectorAggregatorAddress {
        source: crate::common::product_logging::Error,
    },
    #[snafu(display("failed to add the logging configuration to the ConfigMap [{cm_name}]"))]
    InvalidLoggingConfig {
        source: crate::common::product_logging::Error,
        cm_name: String,
    },
    #[snafu(display("failed to build"))]
    BuildingFailure { source: misc::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub fn build_cluster_resources(
    airflow_db: Arc<AirflowDB>,
    additional_data: FetchedAdditionalData,
) -> Result<Vec<BuiltClusterResource>> {
    let mut built_cluster_resources: Vec<BuiltClusterResource> = Vec::new();

    let resolved_product_image: ResolvedProductImage =
        airflow_db.spec.image.resolve(DOCKER_IMAGE_BASE_NAME);

    let (rbac_sa, rbac_rolebinding) = rbac::build_rbac_resources(airflow_db.as_ref(), "airflow");

    built_cluster_resources.push(BuiltClusterResource::PatchRBAC);

    if let Some(ref s) = airflow_db.status {
        match s.condition {
            AirflowDBStatusCondition::Pending => {
                let secret = additional_data.initial_secret;
                if secret.is_some() {
                    let vector_aggregator_address = additional_data.vector_aggregator_address;

                    let config = airflow_db
                        .merged_config()
                        .context(FailedToResolveConfigSnafu)?;

                    let config_map = build_config_map(
                        &airflow_db,
                        &config.logging,
                        vector_aggregator_address.as_deref(),
                    )
                    .context(BuildingFailureSnafu)?;

                    built_cluster_resources
                        .push(BuiltClusterResource::PatchConfigMap(config_map.clone()));

                    let job = build_init_job(
                        &airflow_db,
                        &resolved_product_image,
                        &rbac_sa.name_unchecked(),
                        &config,
                        &config_map.name_unchecked(),
                    )
                    .context(BuildingFailureSnafu)?;

                    built_cluster_resources
                        .push(BuiltClusterResource::PatchJob(job.clone(), s.clone()));
                }
            }
            AirflowDBStatusCondition::Initializing => {
                // In here, check the associated job that is running.
                // If it is still running, do nothing. If it completed, set status to ready, if it failed, set status to failed.
                let ns = airflow_db
                    .namespace()
                    .unwrap_or_else(|| "default".to_string());
                let job_name = airflow_db.job_name();
                let job = additional_data.job;

                let new_status = match get_job_state(&job) {
                    JobState::Complete => Some(s.ready()),
                    JobState::Failed => Some(s.failed()),
                    JobState::InProgress => None,
                };

                if let Some(ns) = new_status {
                    built_cluster_resources.push(BuiltClusterResource::PatchJobStatus(ns.clone()));
                }
            }
            AirflowDBStatusCondition::Ready => (),
            AirflowDBStatusCondition::Failed => (),
        }
    } else {
        // Status is none => initialize the status object as "Provisioned"
        let new_status = AirflowDBStatus::new();

        built_cluster_resources.push(BuiltClusterResource::PatchJobStatus(new_status.clone()));
    }

    Ok(built_cluster_resources)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::super::types::FetchedAdditionalData;

    use super::build_cluster_resources;
    //use assert_json_diff::{assert_json_matches_no_panic, CompareMode, Config};
    use stackable_airflow_crd::airflowdb::AirflowDB;

    #[test]
    fn test_build_step_just_runs() {
        let cluster_cr = std::fs::File::open("test/smoke/db.yaml").unwrap();
        let deserializer = serde_yaml::Deserializer::from_reader(&cluster_cr);
        let airflow_db: AirflowDB =
            serde_yaml::with::singleton_map_recursive::deserialize(deserializer).unwrap();

        let result =
            build_cluster_resources(Arc::new(airflow_db), FetchedAdditionalData::default());

        assert!(result.is_ok(), "we want an ok, instead we got {:?}", result);
    }
}
