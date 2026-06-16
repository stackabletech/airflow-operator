use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{
        meta::ObjectMetaBuilder,
        pod::{
            PodBuilder,
            container::ContainerBuilder,
            resources::ResourceRequirementsBuilder,
            security::PodSecurityContextBuilder,
            volume::{
                ListenerOperatorVolumeSourceBuilder, ListenerOperatorVolumeSourceBuilderError,
                ListenerReference, VolumeBuilder,
            },
        },
    },
    crd::git_sync,
    database_connections::drivers::{
        celery::CeleryDatabaseConnectionDetails, sqlalchemy::SqlAlchemyDatabaseConnectionDetails,
    },
    k8s_openapi::{
        DeepMerge,
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{PersistentVolumeClaim, Probe, ServiceAccount, TCPSocketAction},
        },
        apimachinery::pkg::{apis::meta::v1::LabelSelector, util::intstr::IntOrString},
    },
    kube::{ResourceExt, api::ObjectMeta},
    kvp::{Annotation, Label, LabelError},
    utils::COMMON_BASH_TRAP_FUNCTIONS,
    v2::{builder::meta::ownerreference_from_resource, types::operator::RoleGroupName},
};

use crate::{
    controller::{
        AirflowRoleGroupConfig, ValidatedCluster, ValidatedLogging,
        build::resource::{
            pod::{
                add_authentication_volumes_and_volume_mounts, add_git_sync_resources,
                build_logging_container,
            },
            service::stateful_set_service_name,
        },
    },
    controller_commons::{self, CONFIG_VOLUME_NAME, LOG_CONFIG_VOLUME_NAME, LOG_VOLUME_NAME},
    crd::{
        AirflowExecutor, AirflowRole, CONFIG_PATH, Container, HTTP_PORT_NAME, LISTENER_VOLUME_DIR,
        LISTENER_VOLUME_NAME, LOG_CONFIG_DIR, METRICS_PORT, METRICS_PORT_NAME, STACKABLE_LOG_DIR,
        TEMPLATE_LOCATION, TEMPLATE_VOLUME_NAME, v1alpha2,
    },
    env_vars,
    operations::graceful_shutdown::add_airflow_graceful_shutdown_config,
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

    #[snafu(display("failed to build label"))]
    BuildLabel { source: LabelError },

    #[snafu(display("failed to add needed volume"))]
    AddVolume {
        source: stackable_operator::builder::pod::Error,
    },

    #[snafu(display("failed to add needed volumeMount"))]
    AddVolumeMount {
        source: stackable_operator::builder::pod::container::Error,
    },

    #[snafu(display("failed to build Statefulset environmental variables"))]
    BuildStatefulsetEnvVars { source: env_vars::Error },

    #[snafu(display("failed to build listener volume"))]
    BuildListenerVolume {
        source: ListenerOperatorVolumeSourceBuilderError,
    },

    #[snafu(display("failed to build shared pod resources"))]
    Pod {
        source: crate::controller::build::resource::pod::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

fn build_rolegroup_metadata(
    cluster: &ValidatedCluster,
    role: &AirflowRole,
    role_group_name: &RoleGroupName,
    prometheus_label: Label,
    name: String,
) -> ObjectMeta {
    ObjectMetaBuilder::new()
        .name_and_namespace(cluster)
        .name(name)
        .ownerreference(ownerreference_from_resource(cluster, None, Some(true)))
        .with_labels(cluster.recommended_labels(role, role_group_name))
        .with_label(prometheus_label)
        .build()
}

/// The rolegroup [`StatefulSet`] runs the rolegroup, as configured by the administrator.
#[allow(clippy::too_many_arguments)]
pub fn build_server_rolegroup_statefulset(
    airflow: &v1alpha2::AirflowCluster,
    validated_cluster: &ValidatedCluster,
    airflow_role: &AirflowRole,
    role_group_name: &RoleGroupName,
    validated_rg_config: &AirflowRoleGroupConfig,
    logging: &ValidatedLogging,
    metadata_database_connection_details: &SqlAlchemyDatabaseConnectionDetails,
    celery_database_connection_details: &Option<(
        CeleryDatabaseConnectionDetails,
        CeleryDatabaseConnectionDetails,
    )>,
    service_account: &ServiceAccount,
    git_sync_resources: &git_sync::v1alpha2::GitSyncResources,
) -> Result<StatefulSet> {
    let merged_airflow_config = &validated_rg_config.config;
    let env_overrides = &validated_rg_config.env_overrides;

    let resolved_product_image = &validated_cluster.image;
    let authentication_config = &validated_cluster.cluster_config.authentication_config;
    let executor = &validated_cluster.cluster_config.executor;

    let mut pb = PodBuilder::new();
    let resource_names =
        validated_cluster.resource_names(&airflow_role.role_name(), role_group_name);

    let recommended_object_labels =
        validated_cluster.recommended_labels(airflow_role, role_group_name);
    // Used for PVC templates that cannot be modified once they are deployed (a constant "none"
    // version keeps the labels stable across version upgrades).
    let unversioned_recommended_labels =
        validated_cluster.unversioned_recommended_labels(airflow_role, role_group_name);

    let pb_metadata = ObjectMetaBuilder::new()
        .with_labels(recommended_object_labels)
        .with_annotation(
            Annotation::try_from((
                "kubectl.kubernetes.io/default-container",
                format!("{}", Container::Airflow),
            ))
            .expect("static annotation is always valid"),
        )
        .build();

    pb.metadata(pb_metadata)
        .image_pull_secrets_from_product_image(resolved_product_image)
        .affinity(&merged_airflow_config.affinity)
        .service_account_name(service_account.name_any())
        .security_context(PodSecurityContextBuilder::new().fs_group(1000).build());

    let mut airflow_container = ContainerBuilder::new(&Container::Airflow.to_string())
        .context(InvalidContainerNameSnafu)?;

    add_authentication_volumes_and_volume_mounts(
        authentication_config,
        &mut airflow_container,
        &mut pb,
    )
    .context(PodSnafu)?;

    add_airflow_graceful_shutdown_config(merged_airflow_config, &mut pb)
        .context(GracefulShutdownSnafu)?;

    let mut airflow_container_args = Vec::new();
    airflow_container_args.extend(airflow_role.get_commands(
        airflow,
        authentication_config,
        resolved_product_image,
    ));

    airflow_container
        .image_from_product_image(resolved_product_image)
        .resources(merged_airflow_config.resources.clone().into())
        .command(vec![
            "/bin/bash".to_string(),
            "-x".to_string(),
            "-euo".to_string(),
            "pipefail".to_string(),
            "-c".to_string(),
        ])
        .args(vec![airflow_container_args.join("\n")]);

    airflow_container.add_env_vars(
        env_vars::build_airflow_statefulset_envs(
            validated_cluster,
            airflow_role,
            env_overrides,
            metadata_database_connection_details,
            celery_database_connection_details,
            git_sync_resources,
        )
        .context(BuildStatefulsetEnvVarsSnafu)?,
    );

    let volume_mounts = airflow.volume_mounts();
    airflow_container
        .add_volume_mounts(volume_mounts)
        .context(AddVolumeMountSnafu)?;
    airflow_container
        .add_volume_mount(CONFIG_VOLUME_NAME, CONFIG_PATH)
        .context(AddVolumeMountSnafu)?;
    airflow_container
        .add_volume_mount(LOG_CONFIG_VOLUME_NAME, LOG_CONFIG_DIR)
        .context(AddVolumeMountSnafu)?;
    airflow_container
        .add_volume_mount(LOG_VOLUME_NAME, STACKABLE_LOG_DIR)
        .context(AddVolumeMountSnafu)?;

    if let AirflowExecutor::KubernetesExecutors { .. } = executor {
        airflow_container
            .add_volume_mount(TEMPLATE_VOLUME_NAME, TEMPLATE_LOCATION)
            .context(AddVolumeMountSnafu)?;
    }

    // for roles with an http endpoint
    if let Some(http_port) = airflow_role.get_http_port() {
        let probe = Probe {
            tcp_socket: Some(TCPSocketAction {
                port: IntOrString::Int(http_port.into()),
                ..TCPSocketAction::default()
            }),
            initial_delay_seconds: Some(60),
            period_seconds: Some(10),
            failure_threshold: Some(6),
            ..Probe::default()
        };
        airflow_container.readiness_probe(probe.clone());
        airflow_container.liveness_probe(probe);
        airflow_container.add_container_port(HTTP_PORT_NAME, http_port.into());
    }

    let mut pvcs: Option<Vec<PersistentVolumeClaim>> = None;

    if let Some(listener_group_name) = validated_cluster
        .role_configs
        .get(airflow_role)
        .and_then(|role_config| role_config.group_listener_name.clone())
    {
        // Listener endpoints for the Webserver role will use persistent volumes
        // so that load balancers can hard-code the target addresses. This will
        // be the case even when no class is set (and the value defaults to
        // cluster-internal) as the address should still be consistent.
        let pvc = ListenerOperatorVolumeSourceBuilder::new(
            &ListenerReference::ListenerName(listener_group_name),
            &unversioned_recommended_labels,
        )
        .build_pvc(LISTENER_VOLUME_NAME.to_string())
        .context(BuildListenerVolumeSnafu)?;
        pvcs = Some(vec![pvc]);

        airflow_container
            .add_volume_mount(LISTENER_VOLUME_NAME, LISTENER_VOLUME_DIR)
            .context(AddVolumeMountSnafu)?;
    }

    // If the DAG is modularized we may encounter a timing issue whereby the celery worker
    // has started *before* all modules referenced by the DAG have been fetched by gitsync
    // and registered. This will result in ModuleNotFoundError errors. This can be avoided
    // by running a one-off git-sync process in an init-container so that all DAG
    // dependencies are fully loaded. The sidecar git-sync is then used for regular updates.
    let use_git_sync_init_containers = matches!(executor, AirflowExecutor::CeleryExecutors { .. });
    add_git_sync_resources(
        &mut pb,
        &mut airflow_container,
        git_sync_resources,
        true,
        use_git_sync_init_containers,
    )
    .context(PodSnafu)?;

    metadata_database_connection_details.add_to_container(&mut airflow_container);
    if let Some((celery_result_backend, celery_broker)) = celery_database_connection_details {
        celery_result_backend.add_to_container(&mut airflow_container);
        celery_broker.add_to_container(&mut airflow_container);
    }

    pb.add_container(airflow_container.build());

    let metrics_container = ContainerBuilder::new("metrics")
        .context(InvalidContainerNameSnafu)?
        .image_from_product_image(resolved_product_image)
        .command(vec![
            "/bin/bash".to_string(),
            "-x".to_string(),
            "-euo".to_string(),
            "pipefail".to_string(),
            "-c".to_string(),
        ])
        .args(vec![
            [
                COMMON_BASH_TRAP_FUNCTIONS.to_string(),
                "prepare_signal_handlers".to_string(),
                "/stackable/statsd_exporter &".to_string(),
                "wait_for_termination $!".to_string(),
            ]
            .join("\n"),
        ])
        .add_container_port(METRICS_PORT_NAME, METRICS_PORT.into())
        .resources(
            ResourceRequirementsBuilder::new()
                .with_cpu_request("100m")
                .with_cpu_limit("200m")
                .with_memory_request("64Mi")
                .with_memory_limit("64Mi")
                .build(),
        )
        .build();
    pb.add_container(metrics_container);

    pb.add_volumes(airflow.volumes().clone())
        .context(AddVolumeSnafu)?;
    pb.add_volumes(controller_commons::create_volumes(
        resource_names.role_group_config_map().as_ref(),
        merged_airflow_config
            .logging
            .containers
            .get(&Container::Airflow),
    ))
    .context(AddVolumeSnafu)?;

    if let AirflowExecutor::KubernetesExecutors { .. } = executor {
        pb.add_volume(
            VolumeBuilder::new(TEMPLATE_VOLUME_NAME)
                .with_config_map(airflow.executor_template_configmap_name())
                .build(),
        )
        .context(AddVolumeSnafu)?;
    }

    if let Some(vector_log_config) = &logging.vector_container {
        pb.add_container(build_logging_container(
            resolved_product_image,
            vector_log_config,
            &resource_names,
        ));
    }
    let mut pod_template = pb.build_template();
    pod_template.merge_from(validated_rg_config.pod_overrides.clone());

    let restarter_label =
        Label::try_from(("restarter.stackable.tech/enabled", "true")).context(BuildLabelSnafu)?;

    let metadata = build_rolegroup_metadata(
        validated_cluster,
        airflow_role,
        role_group_name,
        restarter_label,
        resource_names.stateful_set_name().to_string(),
    );

    let statefulset_match_labels =
        validated_cluster.role_group_selector(airflow_role, role_group_name);

    let statefulset_spec = StatefulSetSpec {
        pod_management_policy: Some(
            match airflow_role {
                AirflowRole::Scheduler => {
                    "OrderedReady" // Scheduler pods should start after another, since part of their startup phase is initializing the database, see crd/src/lib.rs
                }
                AirflowRole::Webserver
                | AirflowRole::Worker
                | AirflowRole::DagProcessor
                | AirflowRole::Triggerer => "Parallel",
            }
            .to_string(),
        ),
        replicas: Some(i32::from(validated_rg_config.replicas)),
        selector: LabelSelector {
            match_labels: Some(statefulset_match_labels.into()),
            ..LabelSelector::default()
        },
        service_name: stateful_set_service_name(validated_cluster, airflow_role, role_group_name),
        template: pod_template,
        volume_claim_templates: pvcs,
        ..StatefulSetSpec::default()
    };

    Ok(StatefulSet {
        metadata,
        spec: Some(statefulset_spec),
        status: None,
    })
}
