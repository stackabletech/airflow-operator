//! Builds the Kubernetes-executor pod-template [`ConfigMap`]: a `ConfigMap` whose single entry is
//! the serialized Pod template Airflow uses to provision one Pod per task.

use std::collections::HashMap;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{
        configmap::ConfigMapBuilder,
        meta::ObjectMetaBuilder,
        pod::{PodBuilder, container::ContainerBuilder, security::PodSecurityContextBuilder},
    },
    crd::git_sync,
    k8s_openapi::{
        DeepMerge,
        api::core::v1::{ConfigMap, PodTemplateSpec},
    },
    kvp::{Label, LabelError},
    v2::builder::meta::ownerreference_from_resource,
};

use crate::{
    airflow_controller::{
        executor_role_group_name, executor_role_name, executor_template_role_group_name,
    },
    controller::{
        ValidatedCluster,
        build::resource::pod::{
            add_authentication_volumes_and_volume_mounts, add_git_sync_resources,
            build_logging_container,
        },
        validate,
    },
    controller_commons::{self, CONFIG_VOLUME_NAME, LOG_CONFIG_VOLUME_NAME, LOG_VOLUME_NAME},
    crd::{
        CONFIG_PATH, Container, ExecutorConfig, LOG_CONFIG_DIR, STACKABLE_LOG_DIR, TEMPLATE_NAME,
    },
    env_vars::build_airflow_template_envs,
    operations::graceful_shutdown::add_graceful_shutdown_config,
};

#[derive(Snafu, Debug)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("invalid container name"))]
    InvalidContainerName {
        source: stackable_operator::builder::pod::container::Error,
    },

    #[snafu(display("failed to configure graceful shutdown"))]
    GracefulShutdown {
        source: crate::operations::graceful_shutdown::Error,
    },

    #[snafu(display("failed to add needed volume"))]
    AddVolume {
        source: stackable_operator::builder::pod::Error,
    },

    #[snafu(display("failed to add needed volumeMount"))]
    AddVolumeMount {
        source: stackable_operator::builder::pod::container::Error,
    },

    #[snafu(display("failed to build label"))]
    BuildLabel { source: LabelError },

    #[snafu(display("pod template serialization"))]
    PodTemplateSerde { source: serde_yaml::Error },

    #[snafu(display("failed to build the pod template config map"))]
    PodTemplateConfigMap {
        source: stackable_operator::builder::configmap::Error,
    },

    #[snafu(display("failed to validate the executor logging configuration"))]
    Validate {
        source: crate::controller::validate::Error,
    },

    #[snafu(display("failed to build shared pod resources"))]
    Pod {
        source: crate::controller::build::resource::pod::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub fn build_executor_template_config_map(
    cluster: &ValidatedCluster,
    sa_name: &str,
    merged_executor_config: &ExecutorConfig,
    env_overrides: &HashMap<String, String>,
    pod_overrides: &PodTemplateSpec,
    git_sync_resources: &git_sync::v1alpha2::GitSyncResources,
) -> Result<ConfigMap> {
    let resolved_product_image = &cluster.image;
    let authentication_config = &cluster.cluster_config.authentication_config;

    let mut pb = PodBuilder::new();
    let pb_metadata =
        ObjectMetaBuilder::new()
            .with_labels(cluster.recommended_labels_for(
                &executor_role_name(),
                &executor_template_role_group_name(),
            ))
            .build();

    pb.metadata(pb_metadata)
        .image_pull_secrets_from_product_image(resolved_product_image)
        .affinity(&merged_executor_config.affinity)
        .service_account_name(sa_name)
        .restart_policy("Never")
        .security_context(PodSecurityContextBuilder::new().fs_group(1000).build());

    add_graceful_shutdown_config(merged_executor_config.graceful_shutdown_timeout, &mut pb)
        .context(GracefulShutdownSnafu)?;

    // N.B. this "base" name is an airflow requirement and should not be changed!
    // See https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/8.4.0/kubernetes_executor.html#base-image
    let mut airflow_container =
        ContainerBuilder::new(&Container::Base.to_string()).context(InvalidContainerNameSnafu)?;

    add_authentication_volumes_and_volume_mounts(
        authentication_config,
        &mut airflow_container,
        &mut pb,
    )
    .context(PodSnafu)?;
    airflow_container
        .image_from_product_image(resolved_product_image)
        .resources(merged_executor_config.resources.clone().into())
        .add_env_vars(build_airflow_template_envs(
            cluster,
            env_overrides,
            merged_executor_config,
            git_sync_resources,
        ))
        .add_volume_mounts(cluster.volume_mounts())
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(CONFIG_VOLUME_NAME, CONFIG_PATH)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(LOG_CONFIG_VOLUME_NAME, LOG_CONFIG_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(LOG_VOLUME_NAME, STACKABLE_LOG_DIR)
        .context(AddVolumeMountSnafu)?;

    add_git_sync_resources(
        &mut pb,
        &mut airflow_container,
        git_sync_resources,
        false,
        true,
    )
    .context(PodSnafu)?;

    cluster
        .cluster_config
        .metadata_database_connection_details
        .add_to_container(&mut airflow_container);

    pb.add_container(airflow_container.build());
    pb.add_volumes(cluster.volumes().clone())
        .context(AddVolumeSnafu)?;
    pb.add_volumes(controller_commons::create_volumes(
        cluster
            .resource_names(&executor_role_name(), &executor_role_group_name())
            .role_group_config_map()
            .as_ref(),
        merged_executor_config
            .logging
            .containers
            .get(&Container::Airflow),
    ))
    .context(AddVolumeSnafu)?;

    // The Kubernetes executor pod template is not an `AirflowRole` with role groups, so its logging
    // is validated here (at build time) via the shared `validate_logging`, mirroring the role-group
    // path in `validate`.
    let executor_logging = validate::validate_logging(
        &merged_executor_config.logging,
        &cluster.cluster_config.vector_aggregator_config_map_name,
    )
    .context(ValidateSnafu)?;
    if let Some(vector_log_config) = &executor_logging.vector_container {
        pb.add_container(build_logging_container(
            resolved_product_image,
            vector_log_config,
            &cluster.resource_names(&executor_role_name(), &executor_template_role_group_name()),
        ));
    }

    let mut pod_template = pb.build_template();
    pod_template.merge_from(pod_overrides.clone());

    let mut cm_builder = ConfigMapBuilder::new();

    let restarter_label =
        Label::try_from(("restarter.stackable.tech/enabled", "true")).context(BuildLabelSnafu)?;

    cm_builder
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(cluster)
                .name(cluster.executor_template_configmap_name())
                .ownerreference(ownerreference_from_resource(cluster, None, Some(true)))
                .with_labels(cluster.recommended_labels_for(
                    &executor_role_name(),
                    &executor_template_role_group_name(),
                ))
                .with_label(restarter_label)
                .build(),
        )
        .add_data(
            TEMPLATE_NAME,
            serde_yaml::to_string(&pod_template).context(PodTemplateSerdeSnafu)?,
        );

    cm_builder.build().context(PodTemplateConfigMapSnafu)
}
