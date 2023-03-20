mod misc;

use crate::airflow_controller::DOCKER_IMAGE_BASE_NAME;
use crate::common::rbac;
use crate::common::util::{get_job_state, JobState};

use snafu::{ResultExt, Snafu};
use stackable_airflow_crd::airflowdb::{AirflowDB, AirflowDBStatus, AirflowDBStatusCondition};
use stackable_operator::kube::runtime::controller::Action;
use stackable_operator::{
    commons::product_image_selection::ResolvedProductImage, kube::ResourceExt,
};
use std::sync::Arc;
use strum::{EnumDiscriminants, IntoStaticStr};

use self::misc::{build_config_map, build_init_job};

use super::types::{BuiltClusterResource, FetchedAdditionalData};

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("failed to resolve and merge config"))]
    FailedToResolveConfig {
        source: stackable_airflow_crd::airflowdb::Error,
    },
    #[snafu(display("failed to build"))]
    BuildingFailure { source: misc::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub fn build_cluster_resources(
    airflow_db: Arc<AirflowDB>,
    additional_data: FetchedAdditionalData,
) -> Result<(Vec<BuiltClusterResource>, Action)> {
    let mut built_cluster_resources: Vec<BuiltClusterResource> = Vec::new();

    let resolved_product_image: ResolvedProductImage =
        airflow_db.spec.image.resolve(DOCKER_IMAGE_BASE_NAME);

    let (rbac_sa, _) = rbac::build_rbac_resources(airflow_db.as_ref(), "airflow");

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

                    built_cluster_resources.push(BuiltClusterResource::PatchJob(job, s.clone()));
                }
            }
            AirflowDBStatusCondition::Initializing => {
                // only proceed if the job already exists
                if let Some(job) = additional_data.job {
                    // In here, check the associated job that is running.
                    // If it is still running, do nothing. If it completed, set status to ready, if it failed, set status to failed.

                    let new_status = match get_job_state(&job) {
                        JobState::Complete => Some(s.ready()),
                        JobState::Failed => Some(s.failed()),
                        JobState::InProgress => None,
                    };

                    if let Some(ns) = new_status {
                        built_cluster_resources.push(BuiltClusterResource::PatchJobStatus(ns));
                    }
                }
            }
            AirflowDBStatusCondition::Ready => (),
            AirflowDBStatusCondition::Failed => (),
        }
    } else {
        // Status is none => initialize the status object as "Provisioned"
        let new_status = AirflowDBStatus::new();

        built_cluster_resources.push(BuiltClusterResource::PatchJobStatus(new_status));
    }

    Ok((built_cluster_resources, Action::await_change()))
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use crate::airflow_db_controller::types::BuiltClusterResource;

    use super::super::types::FetchedAdditionalData;

    use super::build_cluster_resources;
    //use assert_json_diff::{assert_json_matches_no_panic, CompareMode, Config};
    use stackable_airflow_crd::airflowdb::{AirflowDB, AirflowDBStatus, AirflowDBStatusCondition};
    use stackable_operator::{k8s_openapi::api::core::v1::Secret, kube::core::ObjectMeta};

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

    #[test]
    fn test_initial_create_job() {
        let cluster_cr = std::fs::File::open("test/smoke/db.yaml").unwrap();
        let deserializer = serde_yaml::Deserializer::from_reader(&cluster_cr);
        let mut airflow_db: AirflowDB =
            serde_yaml::with::singleton_map_recursive::deserialize(deserializer).unwrap();
        airflow_db.status = Some(AirflowDBStatus {
            started_at: None,
            condition: AirflowDBStatusCondition::Pending,
        });
        airflow_db.metadata.uid = Some("hello".to_string());

        let (result, _) = build_cluster_resources(
            Arc::new(airflow_db),
            FetchedAdditionalData {
                initial_secret: Some(Secret {
                    data: None,
                    immutable: None,
                    string_data: Some(BTreeMap::from([(
                        "some_key".to_string(),
                        "some_value".to_string(),
                    )])),
                    metadata: ObjectMeta::default(),
                    type_: None,
                }),
                vector_aggregator_address: None,
                job: None,
            },
        )
        .expect("should produce result");

        assert_eq!(
            result.len(),
            3,
            "we want to have a single resource entry, instead we got {:?}",
            result
        );

        if let BuiltClusterResource::PatchJob(_, _) = result[2] {
            // if let ... else is not a thing yet :( https://github.com/rust-lang/rust/pull/93628/
        } else {
            panic!("expected PatchJob entry, found {:?}", result[0]);
        }
    }
}
