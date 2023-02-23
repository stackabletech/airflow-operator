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
}

type Result<T, E = Error> = std::result::Result<T, E>;

// NEXT
// put apply stuff into apply.rs and enums
// actually fetch additional data

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
                    )?;

                    built_cluster_resources
                        .push(BuiltClusterResource::PatchConfigMap(config_map.clone()));

                    let job = build_init_job(
                        &airflow_db,
                        &resolved_product_image,
                        &rbac_sa.name_unchecked(),
                        &config,
                        &config_map.name_unchecked(),
                    )?;

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

fn build_init_job(
    airflow_db: &AirflowDB,
    resolved_product_image: &ResolvedProductImage,
    sa_name: &str,
    config: &AirflowDbConfig,
    config_map_name: &str,
) -> Result<Job> {
    let commands = vec![
        String::from("airflow db init"),
        String::from("airflow db upgrade"),
        String::from(
            "airflow users create \
                    --username \"$ADMIN_USERNAME\" \
                    --firstname \"$ADMIN_FIRSTNAME\" \
                    --lastname \"$ADMIN_LASTNAME\" \
                    --email \"$ADMIN_EMAIL\" \
                    --password \"$ADMIN_PASSWORD\" \
                    --role \"Admin\"",
        ),
        product_logging::framework::shutdown_vector_command(STACKABLE_LOG_DIR),
    ];

    let secret = &airflow_db.spec.credentials_secret;

    let env = vec![
        env_var_from_secret(
            "AIRFLOW__WEBSERVER__SECRET_KEY",
            secret,
            "connections.secretKey",
        ),
        env_var_from_secret(
            "AIRFLOW__CORE__SQL_ALCHEMY_CONN",
            secret,
            "connections.sqlalchemyDatabaseUri",
        ),
        env_var_from_secret(
            "AIRFLOW__CELERY__RESULT_BACKEND",
            secret,
            "connections.celeryResultBackend",
        ),
        env_var_from_secret("ADMIN_USERNAME", secret, "adminUser.username"),
        env_var_from_secret("ADMIN_FIRSTNAME", secret, "adminUser.firstname"),
        env_var_from_secret("ADMIN_LASTNAME", secret, "adminUser.lastname"),
        env_var_from_secret("ADMIN_EMAIL", secret, "adminUser.email"),
        env_var_from_secret("ADMIN_PASSWORD", secret, "adminUser.password"),
        EnvVar {
            name: "PYTHONPATH".into(),
            value: Some(LOG_CONFIG_DIR.into()),
            ..Default::default()
        },
        EnvVar {
            name: "AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS".into(),
            value: Some("log_config.LOGGING_CONFIG".into()),
            ..Default::default()
        },
    ];

    let mut containers = Vec::new();

    let mut cb = ContainerBuilder::new(&Container::AirflowInitDb.to_string())
        .context(InvalidContainerNameSnafu)?;

    cb.image_from_product_image(resolved_product_image)
        .command(vec!["/bin/bash".to_string()])
        .args(vec![String::from("-c"), commands.join("; ")])
        .add_env_vars(env)
        .add_volume_mount(LOG_CONFIG_VOLUME_NAME, LOG_CONFIG_DIR)
        .add_volume_mount(LOG_VOLUME_NAME, STACKABLE_LOG_DIR);

    let volumes = controller_commons::create_volumes(
        config_map_name,
        config.logging.containers.get(&Container::AirflowInitDb),
    );

    containers.push(cb.build());

    if config.logging.enable_vector_agent {
        containers.push(product_logging::framework::vector_container(
            resolved_product_image,
            CONFIG_VOLUME_NAME,
            LOG_VOLUME_NAME,
            config.logging.containers.get(&Container::Vector),
        ));
    }

    let pod = PodTemplateSpec {
        metadata: Some(
            ObjectMetaBuilder::new()
                .name(format!("{}-init", airflow_db.name_unchecked()))
                .build(),
        ),
        spec: Some(PodSpec {
            containers,
            restart_policy: Some("Never".to_string()),
            service_account: Some(sa_name.to_string()),
            image_pull_secrets: resolved_product_image.pull_secrets.clone(),
            security_context: Some(
                PodSecurityContextBuilder::new()
                    .run_as_user(rbac::AIRFLOW_UID)
                    .run_as_group(0)
                    .build(),
            ),
            volumes: Some(volumes),
            ..Default::default()
        }),
    };

    let job = Job {
        metadata: ObjectMetaBuilder::new()
            .name(airflow_db.name_unchecked())
            .namespace_opt(airflow_db.namespace())
            .ownerreference_from_resource(airflow_db, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .build(),
        spec: Some(JobSpec {
            template: pod,
            ..Default::default()
        }),
        status: None,
    };

    Ok(job)
}

fn build_config_map(
    airflow_db: &AirflowDB,
    logging: &Logging<Container>,
    vector_aggregator_address: Option<&str>,
) -> Result<ConfigMap> {
    let mut cm_builder = ConfigMapBuilder::new();

    let cm_name = format!("{cluster}-init-db", cluster = airflow_db.name_unchecked());

    cm_builder.metadata(
        ObjectMetaBuilder::new()
            .name(&cm_name)
            .namespace_opt(airflow_db.namespace())
            .ownerreference_from_resource(airflow_db, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .build(),
    );

    extend_config_map_with_log_config(
        &RoleGroupRef {
            cluster: ObjectRef::from_obj(airflow_db),
            role: String::new(),
            role_group: String::new(),
        },
        vector_aggregator_address,
        logging,
        &Container::AirflowInitDb,
        &Container::Vector,
        &mut cm_builder,
    )
    .context(InvalidLoggingConfigSnafu {
        cm_name: cm_name.to_owned(),
    })?;

    cm_builder
        .build()
        .context(BuildConfigSnafu { name: cm_name })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::super::types::{BuiltClusterResource, FetchedAdditionalData};

    use super::build_cluster_resources;
    //use assert_json_diff::{assert_json_matches_no_panic, CompareMode, Config};
    use stackable_airflow_crd::airflowdb::AirflowDB;

    #[test]
    fn test_build_step_just_runs() {
        let cluster_cr = std::fs::File::open("test/smoke/db.yaml").unwrap();
        let deserializer = serde_yaml::Deserializer::from_reader(&cluster_cr);
        let druid_cluster: AirflowDB =
            serde_yaml::with::singleton_map_recursive::deserialize(deserializer).unwrap();

        let result =
            build_cluster_resources(Arc::new(druid_cluster), FetchedAdditionalData::default());

        assert!(result.is_ok(), "we want an ok, instead we got {:?}", result);
    }
}
