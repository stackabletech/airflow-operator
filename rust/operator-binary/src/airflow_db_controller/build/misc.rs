use crate::common::controller_commons::{
    CONFIG_VOLUME_NAME, LOG_CONFIG_VOLUME_NAME, LOG_VOLUME_NAME,
};
use crate::common::product_logging::extend_config_map_with_log_config;
use crate::common::util::env_var_from_secret;
use crate::common::{controller_commons, rbac};

use snafu::{ResultExt, Snafu};
use stackable_airflow_crd::{
    airflowdb::{AirflowDB, AirflowDbConfig, Container},
    LOG_CONFIG_DIR, STACKABLE_LOG_DIR,
};
use stackable_operator::{
    builder::{ConfigMapBuilder, ContainerBuilder, ObjectMetaBuilder, PodSecurityContextBuilder},
    commons::product_image_selection::ResolvedProductImage,
    k8s_openapi::api::{
        batch::v1::{Job, JobSpec},
        core::v1::{ConfigMap, EnvVar, PodSpec, PodTemplateSpec},
    },
    kube::{runtime::reflector::ObjectRef, ResourceExt},
    product_logging::{self, spec::Logging},
    role_utils::RoleGroupRef,
};
use strum::{EnumDiscriminants, IntoStaticStr};

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to build ConfigMap [{name}]"))]
    BuildConfig {
        name: String,
        source: stackable_operator::error::Error,
    },
    #[snafu(display("invalid container name"))]
    InvalidContainerName {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to add the logging configuration to the ConfigMap [{cm_name}]"))]
    InvalidLoggingConfig {
        source: crate::common::product_logging::Error,
        cm_name: String,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub fn build_init_job(
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

pub fn build_config_map(
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
