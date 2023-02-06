use crate::airflow_controller::DOCKER_IMAGE_BASE_NAME;
use crate::product_logging::{extend_init_db_config_map, resolve_vector_aggregator_address};
use crate::rbac;
use crate::util::{env_var_from_secret, get_job_state, JobState};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_airflow_crd::{
    airflowdb::{
        AirflowDB, AirflowDBStatus, AirflowDBStatusCondition, AirflowDbConfig, Container,
        AIRFLOW_DB_CONTROLLER_NAME,
    },
    LOG_CONFIG_DIR, LOG_VOLUME_SIZE_IN_MIB, STACKABLE_LOG_DIR,
};
use stackable_operator::{
    builder::{ConfigMapBuilder, ContainerBuilder, ObjectMetaBuilder, PodSecurityContextBuilder},
    commons::product_image_selection::ResolvedProductImage,
    k8s_openapi::{
        api::{
            batch::v1::{Job, JobSpec},
            core::v1::{
                ConfigMap, ConfigMapVolumeSource, EmptyDirVolumeSource, EnvVar, PodSpec,
                PodTemplateSpec, Secret, Volume,
            },
        },
        apimachinery::pkg::api::resource::Quantity,
    },
    kube::{
        runtime::{controller::Action, reflector::ObjectRef},
        ResourceExt,
    },
    logging::controller::ReconcilerError,
    product_logging::{
        self,
        spec::{
            ConfigMapLogConfig, ContainerLogConfig, ContainerLogConfigChoice,
            CustomContainerLogConfig, Logging,
        },
    },
    role_utils::RoleGroupRef,
};
use std::{sync::Arc, time::Duration};
use strum::{EnumDiscriminants, IntoStaticStr};

pub struct Ctx {
    pub client: stackable_operator::client::Client,
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("object has no namespace"))]
    ObjectHasNoNamespace,
    #[snafu(display("failed to apply Job for {}", airflow_db))]
    ApplyJob {
        source: stackable_operator::error::Error,
        airflow_db: ObjectRef<AirflowDB>,
    },
    #[snafu(display("failed to update status"))]
    ApplyStatus {
        source: stackable_operator::error::Error,
    },
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
    #[snafu(display("failed to build ConfigMap [{name}]"))]
    BuildConfig {
        name: String,
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to patch ConfigMap [{name}]"))]
    ApplyConfigMap {
        name: String,
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to resolve and merge config"))]
    FailedToResolveConfig {
        source: stackable_airflow_crd::airflowdb::Error,
    },
    #[snafu(display("failed to resolve the Vector aggregator address"))]
    ResolveVectorAggregatorAddress {
        source: crate::product_logging::Error,
    },
    #[snafu(display("failed to add the logging configuration to the ConfigMap [{cm_name}]"))]
    InvalidLoggingConfig {
        source: crate::product_logging::Error,
        cm_name: String,
    },
}
type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}

pub async fn reconcile_airflow_db(airflow_db: Arc<AirflowDB>, ctx: Arc<Ctx>) -> Result<Action> {
    tracing::info!("Starting reconcile");

    let client = &ctx.client;
    let resolved_product_image: ResolvedProductImage =
        airflow_db.spec.image.resolve(DOCKER_IMAGE_BASE_NAME);

    let (rbac_sa, rbac_rolebinding) = rbac::build_rbac_resources(airflow_db.as_ref(), "airflow");
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
    if let Some(ref s) = airflow_db.status {
        match s.condition {
            AirflowDBStatusCondition::Pending => {
                let secret = client
                    .get_opt::<Secret>(
                        &airflow_db.spec.credentials_secret,
                        airflow_db
                            .namespace()
                            .as_deref()
                            .context(ObjectHasNoNamespaceSnafu)?,
                    )
                    .await
                    .with_context(|_| {
                        let mut secret_ref =
                            ObjectRef::<Secret>::new(&airflow_db.spec.credentials_secret);
                        if let Some(ns) = airflow_db.namespace() {
                            secret_ref = secret_ref.within(&ns);
                        }
                        SecretCheckSnafu { secret: secret_ref }
                    })?;
                if secret.is_some() {
                    let vector_aggregator_address = if let Some(vector_aggregator_config_map_name) =
                        &airflow_db.spec.vector_aggregator_config_map_name
                    {
                        Some(
                            resolve_vector_aggregator_address(
                                vector_aggregator_config_map_name,
                                airflow_db
                                    .namespace()
                                    .as_deref()
                                    .context(ObjectHasNoNamespaceSnafu)?,
                                client,
                            )
                            .await
                            .context(ResolveVectorAggregatorAddressSnafu)?,
                        )
                    } else {
                        None
                    };

                    let config = airflow_db
                        .merged_config()
                        .context(FailedToResolveConfigSnafu)?;

                    let config_map = build_config_map(
                        &airflow_db,
                        &config.logging,
                        vector_aggregator_address.as_deref(),
                    )?;
                    client
                        .apply_patch(AIRFLOW_DB_CONTROLLER_NAME, &config_map, &config_map)
                        .await
                        .context(ApplyConfigMapSnafu {
                            name: config_map.name_any(),
                        })?;

                    let job = build_init_job(
                        &airflow_db,
                        &resolved_product_image,
                        &rbac_sa.name_unchecked(),
                        &config,
                        &config_map.name_unchecked(),
                    )?;
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
                            &s.initializing(),
                        )
                        .await
                        .context(ApplyStatusSnafu)?;
                }
            }
            AirflowDBStatusCondition::Initializing => {
                // In here, check the associated job that is running.
                // If it is still running, do nothing. If it completed, set status to ready, if it failed, set status to failed.
                let ns = airflow_db
                    .namespace()
                    .unwrap_or_else(|| "default".to_string());
                let job_name = airflow_db.job_name();
                let job =
                    client
                        .get::<Job>(&job_name, &ns)
                        .await
                        .context(GetInitializationJobSnafu {
                            init_job: ObjectRef::<Job>::new(&job_name).within(&ns),
                        })?;

                let new_status = match get_job_state(&job) {
                    JobState::Complete => Some(s.ready()),
                    JobState::Failed => Some(s.failed()),
                    JobState::InProgress => None,
                };

                if let Some(ns) = new_status {
                    client
                        .apply_patch_status(AIRFLOW_DB_CONTROLLER_NAME, &*airflow_db, &ns)
                        .await
                        .context(ApplyStatusSnafu)?;
                }
            }
            AirflowDBStatusCondition::Ready => (),
            AirflowDBStatusCondition::Failed => (),
        }
    } else {
        // Status is none => initialize the status object as "Provisioned"
        let new_status = AirflowDBStatus::new();
        client
            .apply_patch_status(AIRFLOW_DB_CONTROLLER_NAME, &*airflow_db, &new_status)
            .await
            .context(ApplyStatusSnafu)?;
    }

    Ok(Action::await_change())
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

    let mut cb = ContainerBuilder::new("airflow-init-db").expect("ContainerBuilder not created");

    cb.image_from_product_image(resolved_product_image)
        .command(vec!["/bin/bash".to_string()])
        .args(vec![String::from("-c"), commands.join("; ")])
        .add_env_vars(env)
        .add_volume_mount("log-config", LOG_CONFIG_DIR)
        .add_volume_mount("log", STACKABLE_LOG_DIR);

    let mut volumes = Vec::new();

    volumes.push(Volume {
        name: "config".to_string(),
        config_map: Some(ConfigMapVolumeSource {
            name: Some(config_map_name.into()),
            ..ConfigMapVolumeSource::default()
        }),
        ..Volume::default()
    });
    volumes.push(Volume {
        name: "log".to_string(),
        empty_dir: Some(EmptyDirVolumeSource {
            medium: None,
            size_limit: Some(Quantity(format!("{LOG_VOLUME_SIZE_IN_MIB}Mi"))),
        }),
        ..Volume::default()
    });

    if let Some(ContainerLogConfig {
        choice:
            Some(ContainerLogConfigChoice::Custom(CustomContainerLogConfig {
                custom: ConfigMapLogConfig { config_map },
            })),
    }) = config.logging.containers.get(&Container::AirflowInitDb)
    {
        volumes.push(Volume {
            name: "log-config".to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: Some(config_map.into()),
                ..ConfigMapVolumeSource::default()
            }),
            ..Volume::default()
        });
    } else {
        volumes.push(Volume {
            name: "log-config".to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: Some(config_map_name.into()),
                ..ConfigMapVolumeSource::default()
            }),
            ..Volume::default()
        });
    }

    containers.push(cb.build());

    if config.logging.enable_vector_agent {
        containers.push(product_logging::framework::vector_container(
            resolved_product_image,
            "config",
            "log",
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

    extend_init_db_config_map(
        &RoleGroupRef {
            cluster: ObjectRef::from_obj(airflow_db),
            role: String::new(),
            role_group: String::new(),
        },
        vector_aggregator_address,
        logging,
        &mut cm_builder,
    )
    .context(InvalidLoggingConfigSnafu {
        cm_name: cm_name.to_owned(),
    })?;

    cm_builder
        .build()
        .context(BuildConfigSnafu { name: cm_name })
}

pub fn error_policy(_obj: Arc<AirflowDB>, _error: &Error, _ctx: Arc<Ctx>) -> Action {
    Action::requeue(Duration::from_secs(5))
}
