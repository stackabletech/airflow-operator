//! Builds the Kubernetes-executor pod-template [`ConfigMap`]: a `ConfigMap` whose single entry is
//! the serialized Pod template Airflow uses to provision one Pod per task.

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{
        configmap::ConfigMapBuilder,
        meta::ObjectMetaBuilder,
        pod::{PodBuilder, security::PodSecurityContextBuilder},
    },
    constants::RESTART_CONTROLLER_ENABLED_LABEL,
    k8s_openapi::{
        DeepMerge,
        api::core::v1::{ConfigMap, PodTemplateSpec},
    },
    v2::{
        builder::pod::container::{EnvVarSet, new_container_builder},
        product_logging::framework::STACKABLE_LOG_DIR,
    },
};

use crate::{
    controller::{
        EXECUTOR_ROLE_GROUP_NAME, EXECUTOR_ROLE_NAME, EXECUTOR_TEMPLATE_ROLE_GROUP_NAME,
        ValidatedAirflowConfig, ValidatedCluster,
        build::{
            graceful_shutdown::add_graceful_shutdown_config,
            object_meta,
            properties::env_vars::build_airflow_template_envs,
            recommended_labels_for_role_group_resources,
            resource::pod::{
                GitSyncSidecarsAddition, add_authentication_volumes_and_volume_mounts,
                add_git_sync_resources, build_logging_container,
            },
            volumes::{self, CONFIG_VOLUME_NAME, LOG_CONFIG_VOLUME_NAME, LOG_VOLUME_NAME},
        },
    },
    crd::{CONFIG_PATH, Container, LOG_CONFIG_DIR, TEMPLATE_NAME},
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to configure graceful shutdown"))]
    GracefulShutdown {
        source: crate::controller::build::graceful_shutdown::Error,
    },

    #[snafu(display("failed to add needed volume"))]
    AddVolume {
        source: stackable_operator::builder::pod::Error,
    },

    #[snafu(display("failed to add needed volumeMount"))]
    AddVolumeMount {
        source: stackable_operator::builder::pod::container::Error,
    },

    #[snafu(display("pod template serialization"))]
    PodTemplateSerde { source: serde_yaml::Error },

    #[snafu(display("failed to build shared pod resources"))]
    Pod {
        source: crate::controller::build::resource::pod::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub fn build_executor_template_config_map(
    cluster: &ValidatedCluster,
    executor_config: &ValidatedAirflowConfig,
    env_overrides: &EnvVarSet,
    pod_overrides: &PodTemplateSpec,
) -> Result<ConfigMap> {
    let resolved_product_image = &cluster.image;
    let authentication_config = &cluster.cluster_config.authentication_config;
    // The git-sync resources were resolved up-front during validation; read them off the validated
    // executor config rather than reconstructing them here.
    let git_sync_resources = &executor_config.git_sync_resources;

    let mut pb = PodBuilder::new();
    let pb_metadata = ObjectMetaBuilder::new()
        .with_labels(recommended_labels_for_role_group_resources(
            cluster,
            &EXECUTOR_ROLE_NAME,
            &EXECUTOR_TEMPLATE_ROLE_GROUP_NAME,
        ))
        .build();

    pb.metadata(pb_metadata)
        .image_pull_secrets_from_product_image(resolved_product_image)
        .affinity(&executor_config.affinity)
        .service_account_name(
            cluster
                .cluster_resource_names()
                .service_account_name()
                .to_string(),
        )
        .restart_policy("Never")
        .security_context(
            PodSecurityContextBuilder::with_stackable_defaults()
                .fs_group(1000)
                .build(),
        );

    add_graceful_shutdown_config(executor_config.graceful_shutdown_timeout, &mut pb)
        .context(GracefulShutdownSnafu)?;

    // N.B. this "base" name is an airflow requirement and should not be changed!
    // See https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/8.4.0/kubernetes_executor.html#base-image
    let mut airflow_container = new_container_builder(&Container::Base.to_container_name());

    add_authentication_volumes_and_volume_mounts(
        authentication_config,
        &mut airflow_container,
        &mut pb,
    )
    .context(PodSnafu)?;
    airflow_container
        .image_from_product_image(resolved_product_image)
        .resources(executor_config.resources.clone().into())
        .add_env_vars(build_airflow_template_envs(
            cluster,
            env_overrides,
            &executor_config.logging,
            git_sync_resources,
        ))
        // Operator-managed mounts first: their names and paths are constants, so they cannot
        // collide with each other.
        .add_volume_mount(&*CONFIG_VOLUME_NAME, CONFIG_PATH)
        .expect("The mount paths are statically defined and there should be no duplicates.")
        .add_volume_mount(&*LOG_CONFIG_VOLUME_NAME, LOG_CONFIG_DIR)
        .expect("The mount paths are statically defined and there should be no duplicates.")
        .add_volume_mount(&*LOG_VOLUME_NAME, STACKABLE_LOG_DIR)
        .expect("The mount paths are statically defined and there should be no duplicates.")
        // User-supplied mounts last: these can collide with the ones above, so this stays fallible.
        .add_volume_mounts(cluster.volume_mounts())
        .context(AddVolumeMountSnafu)?;

    add_git_sync_resources(
        &mut pb,
        &mut airflow_container,
        git_sync_resources,
        // We don't need a git-sync sidecar, an initial clone via the init-container is sufficient for
        // Kubernetes executors, as they are short-lived.
        &GitSyncSidecarsAddition::Skip,
    )
    .context(PodSnafu)?;

    cluster
        .metadata_database_connection_details()
        .add_to_container(&mut airflow_container);

    pb.add_container(airflow_container.build());
    // Operator-managed volumes first (static names), user-supplied volumes last (fallible).
    pb.add_volumes(volumes::create_volumes(
        cluster
            .role_group_resource_names(&EXECUTOR_ROLE_NAME, &EXECUTOR_ROLE_GROUP_NAME)
            .role_group_config_map()
            .as_ref(),
        &executor_config.logging.product_container,
    ))
    .expect("The volume names are statically defined and there should be no duplicates.");
    pb.add_volumes(cluster.volumes().clone())
        .context(AddVolumeSnafu)?;

    if let Some(vector_log_config) = &executor_config.logging.vector_container {
        pb.add_container(build_logging_container(
            resolved_product_image,
            vector_log_config,
            &cluster
                .role_group_resource_names(&EXECUTOR_ROLE_NAME, &EXECUTOR_TEMPLATE_ROLE_GROUP_NAME),
        ));
    }

    let mut pod_template = pb.build_template();
    pod_template.merge_from(pod_overrides.clone());

    let mut cm_builder = ConfigMapBuilder::new();

    cm_builder
        .metadata(
            object_meta(
                cluster,
                cluster.executor_template_configmap_name(),
                recommended_labels_for_role_group_resources(
                    cluster,
                    &EXECUTOR_ROLE_NAME,
                    &EXECUTOR_TEMPLATE_ROLE_GROUP_NAME,
                ),
            )
            .with_label(RESTART_CONTROLLER_ENABLED_LABEL.clone())
            .build(),
        )
        .add_data(
            TEMPLATE_NAME,
            serde_yaml::to_string(&pod_template).context(PodTemplateSerdeSnafu)?,
        );

    Ok(cm_builder
        .build()
        .expect("The ConfigMap metadata is set in this function."))
}
