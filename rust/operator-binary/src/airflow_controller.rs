//! Ensures that `Pod`s are configured and running for each [`v1alpha2::AirflowCluster`]
use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use const_format::concatcp;
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{
        self,
        configmap::ConfigMapBuilder,
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
    cli::OperatorEnvironmentOptions,
    cluster_resources::{ClusterResourceApplyStrategy, ClusterResources},
    commons::{
        product_image_selection::ResolvedProductImage, random_secret_creation,
        rbac::build_rbac_resources,
    },
    crd::{authentication::ldap, git_sync, listener},
    database_connections::{
        TemplatingMechanism,
        drivers::{
            celery::CeleryDatabaseConnectionDetails,
            sqlalchemy::SqlAlchemyDatabaseConnectionDetails,
        },
    },
    k8s_openapi::{
        self, DeepMerge,
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMap, EnvVar, PersistentVolumeClaim, PodTemplateSpec, Probe, ServiceAccount,
                TCPSocketAction,
            },
        },
        apimachinery::pkg::{apis::meta::v1::LabelSelector, util::intstr::IntOrString},
    },
    kube::{
        Resource, ResourceExt,
        api::ObjectMeta,
        core::{DeserializeGuard, error_boundary},
        runtime::{controller::Action, reflector::ObjectRef},
    },
    kvp::{Annotation, Label, LabelError, Labels, ObjectLabels},
    logging::controller::ReconcilerError,
    product_logging::{self, framework::LoggingError, spec::ContainerLogConfig},
    role_utils::RoleGroupRef,
    shared::time::Duration,
    status::condition::{
        compute_conditions, operations::ClusterOperationsConditionBuilder,
        statefulset::StatefulSetConditionBuilder,
    },
    utils::COMMON_BASH_TRAP_FUNCTIONS,
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{
    controller::{
        build::config_map,
        validate::{ValidatedAirflowCluster, ValidatedRoleGroupConfig},
    },
    controller_commons::{self, CONFIG_VOLUME_NAME, LOG_CONFIG_VOLUME_NAME, LOG_VOLUME_NAME},
    crd::{
        self, APP_NAME, AirflowClusterStatus, AirflowConfigOverrides, AirflowExecutor,
        AirflowExecutorCommonConfiguration, AirflowRole, CONFIG_PATH, Container, ExecutorConfig,
        HTTP_PORT, HTTP_PORT_NAME, LISTENER_VOLUME_DIR, LISTENER_VOLUME_NAME, LOG_CONFIG_DIR,
        METRICS_PORT, METRICS_PORT_NAME, OPERATOR_NAME, STACKABLE_LOG_DIR, TEMPLATE_LOCATION,
        TEMPLATE_NAME, TEMPLATE_VOLUME_NAME,
        authentication::{
            AirflowAuthenticationClassResolved, AirflowClientAuthenticationDetailsResolved,
        },
        build_recommended_labels,
        internal_secret::{
            FERNET_KEY_SECRET_KEY, INTERNAL_SECRET_SECRET_KEY, JWT_SECRET_SECRET_KEY,
        },
        v1alpha2,
    },
    env_vars::{self, build_airflow_template_envs},
    operations::{
        graceful_shutdown::{
            add_airflow_graceful_shutdown_config, add_executor_graceful_shutdown_config,
        },
        pdb::add_pdbs,
    },
    service::{
        build_rolegroup_headless_service, build_rolegroup_metrics_service,
        stateful_set_service_name,
    },
};

pub const AIRFLOW_CONTROLLER_NAME: &str = "airflowcluster";
pub const CONTAINER_IMAGE_BASE_NAME: &str = "airflow";
pub const AIRFLOW_FULL_CONTROLLER_NAME: &str =
    concatcp!(AIRFLOW_CONTROLLER_NAME, '.', OPERATOR_NAME);

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub operator_environment: OperatorEnvironmentOptions,
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("object defines no airflow config role"))]
    NoAirflowRole,

    #[snafu(display("failed to apply Service for {rolegroup}"))]
    ApplyRoleGroupService {
        source: stackable_operator::cluster_resources::Error,
        rolegroup: RoleGroupRef<v1alpha2::AirflowCluster>,
    },

    #[snafu(display("failed to apply ConfigMap for {rolegroup}"))]
    ApplyRoleGroupConfig {
        source: stackable_operator::cluster_resources::Error,
        rolegroup: RoleGroupRef<v1alpha2::AirflowCluster>,
    },

    #[snafu(display("failed to apply StatefulSet for {rolegroup}"))]
    ApplyRoleGroupStatefulSet {
        source: stackable_operator::cluster_resources::Error,
        rolegroup: RoleGroupRef<v1alpha2::AirflowCluster>,
    },

    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to patch service account"))]
    ApplyServiceAccount {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to patch role binding: {source}"))]
    ApplyRoleBinding {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to build RBAC objects"))]
    BuildRBACObjects {
        source: stackable_operator::commons::rbac::Error,
    },

    #[snafu(display("failed to build rolegroup ConfigMap"))]
    BuildConfigMap {
        source: crate::controller::build::config_map::Error,
    },

    #[snafu(display("failed to resolve and merge config for role and role group"))]
    FailedToResolveConfig { source: crd::Error },

    #[snafu(display("invalid container name"))]
    InvalidContainerName {
        source: stackable_operator::builder::pod::container::Error,
    },

    #[snafu(display("invalid git-sync specification"))]
    InvalidGitSyncSpec { source: git_sync::v1alpha2::Error },

    #[snafu(display("failed to create cluster resources"))]
    CreateClusterResources {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to delete orphaned resources"))]
    DeleteOrphanedResources {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("vector agent is enabled but vector aggregator ConfigMap is missing"))]
    VectorAggregatorConfigMapMissing,

    #[snafu(display("failed to update status"))]
    ApplyStatus {
        source: stackable_operator::client::Error,
    },

    #[snafu(display("failed to create internal secret"))]
    InternalSecret {
        source: random_secret_creation::Error,
    },

    #[snafu(display("failed to dereference cluster resources"))]
    Dereference {
        source: crate::controller::dereference::Error,
    },

    #[snafu(display("failed to validate cluster configuration"))]
    Validate {
        source: crate::controller::validate::Error,
    },

    #[snafu(display("pod template serialization"))]
    PodTemplateSerde { source: serde_yaml::Error },

    #[snafu(display("failed to build the pod template config map"))]
    PodTemplateConfigMap {
        source: stackable_operator::builder::configmap::Error,
    },

    #[snafu(display("failed to apply executor template ConfigMap"))]
    ApplyExecutorTemplateConfig {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to create PodDisruptionBudget"))]
    FailedToCreatePdb {
        source: crate::operations::pdb::Error,
    },

    #[snafu(display("failed to configure graceful shutdown"))]
    GracefulShutdown {
        source: crate::operations::graceful_shutdown::Error,
    },

    #[snafu(display("failed to build label"))]
    BuildLabel { source: LabelError },

    #[snafu(display("failed to build object meta data"))]
    ObjectMeta {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to configure logging"))]
    ConfigureLogging { source: LoggingError },

    #[snafu(display("failed to add needed volume"))]
    AddVolume { source: builder::pod::Error },

    #[snafu(display("failed to add needed volumeMount"))]
    AddVolumeMount {
        source: builder::pod::container::Error,
    },

    #[snafu(display("failed to add LDAP Volumes and VolumeMounts"))]
    AddLdapVolumesAndVolumeMounts { source: ldap::v1alpha1::Error },

    #[snafu(display("failed to add TLS Volumes and VolumeMounts"))]
    AddTlsVolumesAndVolumeMounts {
        source: stackable_operator::commons::tls_verification::TlsClientDetailsError,
    },

    #[snafu(display("AirflowCluster object is invalid"))]
    InvalidAirflowCluster {
        source: error_boundary::InvalidObject,
    },

    #[snafu(display("failed to build Statefulset environmental variables"))]
    BuildStatefulsetEnvVars { source: env_vars::Error },

    #[snafu(display("failed to build Labels"))]
    LabelBuild {
        source: stackable_operator::kvp::LabelError,
    },

    #[snafu(display("failed to build listener volume"))]
    BuildListenerVolume {
        source: ListenerOperatorVolumeSourceBuilderError,
    },

    #[snafu(display("failed to apply group listener"))]
    ApplyGroupListener {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to configure service"))]
    ServiceConfiguration { source: crate::service::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}

pub async fn reconcile_airflow(
    airflow: Arc<DeserializeGuard<v1alpha2::AirflowCluster>>,
    ctx: Arc<Ctx>,
) -> Result<Action> {
    tracing::info!("Starting reconcile");

    let airflow = airflow
        .0
        .as_ref()
        .map_err(error_boundary::InvalidObject::clone)
        .context(InvalidAirflowClusterSnafu)?;

    let client = &ctx.client;

    let dereferenced = crate::controller::dereference::dereference(client, airflow)
        .await
        .context(DereferenceSnafu)?;

    let cluster_operation_cond_builder =
        ClusterOperationsConditionBuilder::new(&airflow.spec.cluster_operation);

    let templating_mechanism = TemplatingMechanism::BashEnvSubstitution;
    let metadata_database_connection_details = airflow
        .spec
        .cluster_config
        .metadata_database
        .sqlalchemy_connection_details_with_templating("METADATA", &templating_mechanism);

    let celery_database_connection_details = if let (
        Some(celery_results_backend),
        Some(celery_broker),
    ) = (
        &airflow.spec.cluster_config.celery_results_backend,
        &airflow.spec.cluster_config.celery_broker,
    ) {
        // The celery results backend and celery broker only work with configured celeryExecutors.
        // Emit a warning if celery executors were not configured properly.
        if !matches!(
            &airflow.spec.executor,
            AirflowExecutor::CeleryExecutors { .. }
        ) {
            tracing::warn!(
                "No `spec.celeryExecutors` configured, but `spec.clusterConfig.celeryResultsBackend` and `spec.clusterConfig.celeryBroker` are provided. This only works in combination with a celery executor!"
            )
        }

        let celery_results_backend = celery_results_backend
            .celery_connection_details_with_templating(
                "CELERY_RESULT_BACKEND",
                &templating_mechanism,
            );
        let celery_broker = celery_broker
            .celery_connection_details_with_templating("CELERY_BROKER", &templating_mechanism);
        Some((celery_results_backend, celery_broker))
    } else {
        None
    };

    let validated_cluster = crate::controller::validate::validate_cluster(
        airflow,
        &ctx.operator_environment.image_repository,
        dereferenced,
    )
    .context(ValidateSnafu)?;

    // TODO: Move secret creation to a dedicated apply step once it exists.
    random_secret_creation::create_random_secret_if_not_exists(
        &airflow.shared_internal_secret_secret_name(),
        INTERNAL_SECRET_SECRET_KEY,
        256,
        airflow,
        client,
    )
    .await
    .context(InternalSecretSnafu)?;

    random_secret_creation::create_random_secret_if_not_exists(
        &airflow.shared_jwt_secret_secret_name(),
        JWT_SECRET_SECRET_KEY,
        256,
        airflow,
        client,
    )
    .await
    .context(InternalSecretSnafu)?;

    // https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/fernet.html#security-fernet
    // does not document how long the fernet key should be, but recommends using
    // python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
    // which returns `jUm21LuA76YZmrIa9u4eXRg0h0P24MDC9IDOmDvJbfw=`, which has 44 characters, which makes 32 bytes.
    random_secret_creation::create_random_secret_if_not_exists(
        &airflow.shared_fernet_key_secret_name(),
        FERNET_KEY_SECRET_KEY,
        32,
        airflow,
        client,
    )
    .await
    .context(InternalSecretSnafu)?;

    let mut cluster_resources = ClusterResources::new(
        APP_NAME,
        OPERATOR_NAME,
        AIRFLOW_CONTROLLER_NAME,
        &airflow.object_ref(&()),
        ClusterResourceApplyStrategy::from(&airflow.spec.cluster_operation),
        &airflow.spec.object_overrides,
    )
    .context(CreateClusterResourcesSnafu)?;

    let required_labels = cluster_resources
        .get_required_labels()
        .context(BuildLabelSnafu)?;

    let (rbac_sa, rbac_rolebinding) =
        build_rbac_resources(airflow, APP_NAME, required_labels).context(BuildRBACObjectsSnafu)?;

    let rbac_sa = cluster_resources
        .add(client, rbac_sa.clone())
        .await
        .context(ApplyServiceAccountSnafu)?;
    cluster_resources
        .add(client, rbac_rolebinding)
        .await
        .context(ApplyRoleBindingSnafu)?;

    let mut ss_cond_builder = StatefulSetConditionBuilder::default();

    // if the kubernetes executor is specified, in place of a worker role that will be in the role
    // collection there will be a pod template created to be used for pod provisioning
    if let AirflowExecutor::KubernetesExecutors {
        common_configuration,
    } = &validated_cluster.executor
    {
        build_executor_template(
            airflow,
            common_configuration,
            &metadata_database_connection_details,
            &validated_cluster,
            &mut cluster_resources,
            client,
            &rbac_sa,
        )
        .await?;
    }

    for (airflow_role, role_group_configs) in &validated_cluster.role_groups {
        let role_name = airflow_role.to_string();

        if let Some(role_config) = validated_cluster.role_configs.get(airflow_role) {
            if let Some(pdb) = &role_config.pdb {
                add_pdbs(pdb, airflow, airflow_role, client, &mut cluster_resources)
                    .await
                    .context(FailedToCreatePdbSnafu)?;
            }

            if let Some(listener_class) = &role_config.listener_class
                && let Some(listener_group_name) = &role_config.group_listener_name
            {
                let rg_group_listener = build_group_listener(
                    airflow,
                    build_recommended_labels(
                        airflow,
                        AIRFLOW_CONTROLLER_NAME,
                        &validated_cluster.image.app_version_label_value,
                        &role_name,
                        "none",
                    ),
                    listener_class.to_string(),
                    listener_group_name.clone(),
                )?;
                cluster_resources
                    .add(client, rg_group_listener)
                    .await
                    .context(ApplyGroupListenerSnafu)?;
            }
        }

        for (rolegroup_name, validated_rg_config) in role_group_configs {
            let rolegroup = RoleGroupRef {
                cluster: ObjectRef::from_obj(airflow),
                role: role_name.clone(),
                role_group: rolegroup_name.into(),
            };

            let git_sync_resources = git_sync::v1alpha2::GitSyncResources::new(
                &airflow.spec.cluster_config.dags_git_sync,
                &validated_cluster.image,
                &env_vars_from_overrides(&validated_rg_config.env_overrides),
                &airflow.volume_mounts(),
                LOG_VOLUME_NAME,
                &validated_rg_config
                    .merged_config
                    .logging
                    .for_container(&Container::GitSync),
            )
            .context(InvalidGitSyncSpecSnafu)?;

            let role_group_service_recommended_labels = build_recommended_labels(
                airflow,
                AIRFLOW_CONTROLLER_NAME,
                &validated_cluster.image.app_version_label_value,
                &rolegroup.role,
                &rolegroup.role_group,
            );

            let role_group_service_selector = Labels::role_group_selector(
                airflow,
                APP_NAME,
                &rolegroup.role,
                &rolegroup.role_group,
            )
            .context(LabelBuildSnafu)?;

            let rg_headless_service = build_rolegroup_headless_service(
                airflow,
                &rolegroup,
                role_group_service_recommended_labels.clone(),
                role_group_service_selector.clone().into(),
            )
            .context(ServiceConfigurationSnafu)?;

            cluster_resources
                .add(client, rg_headless_service)
                .await
                .context(ApplyRoleGroupServiceSnafu {
                    rolegroup: rolegroup.clone(),
                })?;

            let rg_metrics_service = build_rolegroup_metrics_service(
                airflow,
                &rolegroup,
                role_group_service_recommended_labels,
                role_group_service_selector.into(),
            )
            .context(ServiceConfigurationSnafu)?;
            cluster_resources
                .add(client, rg_metrics_service)
                .await
                .context(ApplyRoleGroupServiceSnafu {
                    rolegroup: rolegroup.clone(),
                })?;

            let rg_configmap = config_map::build_rolegroup_config_map(
                airflow,
                &validated_cluster,
                &rolegroup,
                &validated_rg_config.config_overrides,
                &validated_rg_config.merged_config.logging,
                &Container::Airflow,
            )
            .context(BuildConfigMapSnafu)?;
            cluster_resources
                .add(client, rg_configmap)
                .await
                .with_context(|_| ApplyRoleGroupConfigSnafu {
                    rolegroup: rolegroup.clone(),
                })?;

            let rg_statefulset = build_server_rolegroup_statefulset(
                airflow,
                &validated_cluster,
                airflow_role,
                &rolegroup,
                validated_rg_config,
                &metadata_database_connection_details,
                &celery_database_connection_details,
                &rbac_sa,
                &git_sync_resources,
            )?;

            ss_cond_builder.add(
                cluster_resources
                    .add(client, rg_statefulset)
                    .await
                    .context(ApplyRoleGroupStatefulSetSnafu {
                        rolegroup: rolegroup.clone(),
                    })?,
            );
        }
    }

    cluster_resources
        .delete_orphaned_resources(client)
        .await
        .context(DeleteOrphanedResourcesSnafu)?;

    let status = AirflowClusterStatus {
        conditions: compute_conditions(
            airflow,
            &[&ss_cond_builder, &cluster_operation_cond_builder],
        ),
    };

    client
        .apply_patch_status(OPERATOR_NAME, airflow, &status)
        .await
        .context(ApplyStatusSnafu)?;

    Ok(Action::await_change())
}

async fn build_executor_template(
    airflow: &v1alpha2::AirflowCluster,
    common_config: &AirflowExecutorCommonConfiguration,
    metadata_database_connection_details: &SqlAlchemyDatabaseConnectionDetails,
    validated_cluster: &ValidatedAirflowCluster,
    cluster_resources: &mut ClusterResources<'_>,
    client: &stackable_operator::client::Client,
    rbac_sa: &stackable_operator::k8s_openapi::api::core::v1::ServiceAccount,
) -> Result<(), Error> {
    let merged_executor_config = airflow
        .merged_executor_config(&common_config.config)
        .context(FailedToResolveConfigSnafu)?;
    let rolegroup = RoleGroupRef {
        cluster: ObjectRef::from_obj(airflow),
        role: "executor".into(),
        role_group: "kubernetes".into(),
    };

    let rg_configmap = config_map::build_rolegroup_config_map(
        airflow,
        validated_cluster,
        &rolegroup,
        // The kubernetes-executor pod template does not apply webserver_config.py overrides
        // (preserves prior behaviour, which passed an empty map here).
        &AirflowConfigOverrides::default(),
        &merged_executor_config.logging,
        &Container::Base,
    )
    .context(BuildConfigMapSnafu)?;
    cluster_resources
        .add(client, rg_configmap)
        .await
        .with_context(|_| ApplyRoleGroupConfigSnafu {
            rolegroup: rolegroup.clone(),
        })?;

    let git_sync_resources = git_sync::v1alpha2::GitSyncResources::new(
        &airflow.spec.cluster_config.dags_git_sync,
        &validated_cluster.image,
        &env_vars_from_overrides(&common_config.env_overrides),
        &airflow.volume_mounts(),
        LOG_VOLUME_NAME,
        &merged_executor_config
            .logging
            .for_container(&Container::GitSync),
    )
    .context(InvalidGitSyncSpecSnafu)?;

    let worker_pod_template_config_map = build_executor_template_config_map(
        airflow,
        &validated_cluster.image,
        &validated_cluster.authentication_config,
        metadata_database_connection_details,
        &rbac_sa.name_unchecked(),
        &merged_executor_config,
        &common_config.env_overrides,
        &common_config.pod_overrides,
        &rolegroup,
        &git_sync_resources,
    )?;
    cluster_resources
        .add(client, worker_pod_template_config_map)
        .await
        .with_context(|_| ApplyExecutorTemplateConfigSnafu {})?;
    Ok(())
}

fn build_rolegroup_metadata(
    airflow: &v1alpha2::AirflowCluster,
    resolved_product_image: &&ResolvedProductImage,
    rolegroup: &&RoleGroupRef<v1alpha2::AirflowCluster>,
    prometheus_label: Label,
    name: String,
) -> Result<ObjectMeta, Error> {
    let metadata = ObjectMetaBuilder::new()
        .name_and_namespace(airflow)
        .name(name)
        .ownerreference_from_resource(airflow, None, Some(true))
        .context(ObjectMissingMetadataForOwnerRefSnafu)?
        .with_recommended_labels(&build_recommended_labels(
            airflow,
            AIRFLOW_CONTROLLER_NAME,
            &resolved_product_image.app_version_label_value,
            &rolegroup.role,
            &rolegroup.role_group,
        ))
        .context(ObjectMetaSnafu)?
        .with_label(prometheus_label)
        .build();
    Ok(metadata)
}

pub fn build_group_listener(
    airflow: &v1alpha2::AirflowCluster,
    object_labels: ObjectLabels<v1alpha2::AirflowCluster>,
    listener_class: String,
    listener_group_name: String,
) -> Result<listener::v1alpha1::Listener> {
    Ok(listener::v1alpha1::Listener {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(airflow)
            .name(listener_group_name)
            .ownerreference_from_resource(airflow, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(&object_labels)
            .context(ObjectMetaSnafu)?
            .build(),
        spec: listener::v1alpha1::ListenerSpec {
            class_name: Some(listener_class),
            ports: Some(listener_ports()),
            ..listener::v1alpha1::ListenerSpec::default()
        },
        status: None,
    })
}

/// We only use the http port here and intentionally omit
/// the metrics one.
fn listener_ports() -> Vec<listener::v1alpha1::ListenerPort> {
    vec![listener::v1alpha1::ListenerPort {
        name: HTTP_PORT_NAME.to_string(),
        port: HTTP_PORT.into(),
        protocol: Some("TCP".to_string()),
    }]
}

/// The rolegroup [`StatefulSet`] runs the rolegroup, as configured by the administrator.
#[allow(clippy::too_many_arguments)]
fn build_server_rolegroup_statefulset(
    airflow: &v1alpha2::AirflowCluster,
    validated_cluster: &ValidatedAirflowCluster,
    airflow_role: &AirflowRole,
    rolegroup_ref: &RoleGroupRef<v1alpha2::AirflowCluster>,
    validated_rg_config: &ValidatedRoleGroupConfig,
    metadata_database_connection_details: &SqlAlchemyDatabaseConnectionDetails,
    celery_database_connection_details: &Option<(
        CeleryDatabaseConnectionDetails,
        CeleryDatabaseConnectionDetails,
    )>,
    service_account: &ServiceAccount,
    git_sync_resources: &git_sync::v1alpha2::GitSyncResources,
) -> Result<StatefulSet> {
    let merged_airflow_config = &validated_rg_config.merged_config;
    let env_overrides = &validated_rg_config.env_overrides;

    let resolved_product_image = &validated_cluster.image;
    let authentication_config = &validated_cluster.authentication_config;
    let authorization_config = &validated_cluster.authorization_config;
    let executor = &validated_cluster.executor;

    let binding = airflow.get_role(airflow_role);
    let role = binding.as_ref().context(NoAirflowRoleSnafu)?;

    let rolegroup = role.role_groups.get(&rolegroup_ref.role_group);

    let mut pb = PodBuilder::new();
    let recommended_object_labels = build_recommended_labels(
        airflow,
        AIRFLOW_CONTROLLER_NAME,
        &resolved_product_image.app_version_label_value,
        &rolegroup_ref.role,
        &rolegroup_ref.role_group,
    );
    // Used for PVC templates that cannot be modified once they are deployed
    let unversioned_recommended_labels = Labels::recommended(&build_recommended_labels(
        airflow,
        AIRFLOW_CONTROLLER_NAME,
        // A version value is required, and we do want to use the "recommended" format for the other desired labels
        "none",
        &rolegroup_ref.role,
        &rolegroup_ref.role_group,
    ))
    .context(LabelBuildSnafu)?;

    let pb_metadata = ObjectMetaBuilder::new()
        .with_recommended_labels(&recommended_object_labels)
        .context(ObjectMetaSnafu)?
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
    )?;

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
            airflow,
            airflow_role,
            env_overrides,
            executor,
            authentication_config,
            authorization_config,
            metadata_database_connection_details,
            celery_database_connection_details,
            git_sync_resources,
            resolved_product_image,
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

    if let Some(listener_group_name) = airflow.group_listener_name(airflow_role) {
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
    )?;

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
        &rolegroup_ref.object_name(),
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

    if merged_airflow_config.logging.enable_vector_agent {
        match &airflow
            .spec
            .cluster_config
            .vector_aggregator_config_map_name
        {
            Some(vector_aggregator_config_map_name) => {
                pb.add_container(build_logging_container(
                    resolved_product_image,
                    merged_airflow_config
                        .logging
                        .containers
                        .get(&Container::Vector),
                    vector_aggregator_config_map_name,
                )?);
            }
            None => {
                VectorAggregatorConfigMapMissingSnafu.fail()?;
            }
        }
    }
    let mut pod_template = pb.build_template();
    pod_template.merge_from(role.config.pod_overrides.clone());
    if let Some(rolegroup) = rolegroup {
        pod_template.merge_from(rolegroup.config.pod_overrides.clone());
    }

    let restarter_label =
        Label::try_from(("restarter.stackable.tech/enabled", "true")).context(BuildLabelSnafu)?;

    let metadata = build_rolegroup_metadata(
        airflow,
        &resolved_product_image,
        &rolegroup_ref,
        restarter_label,
        rolegroup_ref.object_name(),
    )?;

    let statefulset_match_labels = Labels::role_group_selector(
        airflow,
        APP_NAME,
        &rolegroup_ref.role,
        &rolegroup_ref.role_group,
    )
    .context(BuildLabelSnafu)?;

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
        replicas: rolegroup.and_then(|rg| rg.replicas).map(i32::from),
        selector: LabelSelector {
            match_labels: Some(statefulset_match_labels.into()),
            ..LabelSelector::default()
        },
        service_name: stateful_set_service_name(rolegroup_ref),
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

fn build_logging_container(
    resolved_product_image: &ResolvedProductImage,
    log_config: Option<&ContainerLogConfig>,
    vector_aggregator_config_map_name: &str,
) -> Result<k8s_openapi::api::core::v1::Container> {
    product_logging::framework::vector_container(
        resolved_product_image,
        CONFIG_VOLUME_NAME,
        LOG_VOLUME_NAME,
        log_config,
        ResourceRequirementsBuilder::new()
            .with_cpu_request("250m")
            .with_cpu_limit("500m")
            .with_memory_request("128Mi")
            .with_memory_limit("128Mi")
            .build(),
        vector_aggregator_config_map_name,
    )
    .context(ConfigureLoggingSnafu)
}

#[allow(clippy::too_many_arguments)]
fn build_executor_template_config_map(
    airflow: &v1alpha2::AirflowCluster,
    resolved_product_image: &ResolvedProductImage,
    authentication_config: &AirflowClientAuthenticationDetailsResolved,
    metadata_database_connection_details: &SqlAlchemyDatabaseConnectionDetails,
    sa_name: &str,
    merged_executor_config: &ExecutorConfig,
    env_overrides: &HashMap<String, String>,
    pod_overrides: &PodTemplateSpec,
    rolegroup_ref: &RoleGroupRef<v1alpha2::AirflowCluster>,
    git_sync_resources: &git_sync::v1alpha2::GitSyncResources,
) -> Result<ConfigMap> {
    let mut pb = PodBuilder::new();
    let pb_metadata = ObjectMetaBuilder::new()
        .with_recommended_labels(&build_recommended_labels(
            airflow,
            AIRFLOW_CONTROLLER_NAME,
            &resolved_product_image.app_version_label_value,
            "executor",
            "executor-template",
        ))
        .context(ObjectMetaSnafu)?
        .build();

    pb.metadata(pb_metadata)
        .image_pull_secrets_from_product_image(resolved_product_image)
        .affinity(&merged_executor_config.affinity)
        .service_account_name(sa_name)
        .restart_policy("Never")
        .security_context(PodSecurityContextBuilder::new().fs_group(1000).build());

    add_executor_graceful_shutdown_config(merged_executor_config, &mut pb)
        .context(GracefulShutdownSnafu)?;

    // N.B. this "base" name is an airflow requirement and should not be changed!
    // See https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/8.4.0/kubernetes_executor.html#base-image
    let mut airflow_container =
        ContainerBuilder::new(&Container::Base.to_string()).context(InvalidContainerNameSnafu)?;

    // Works too, had been changed
    add_authentication_volumes_and_volume_mounts(
        authentication_config,
        &mut airflow_container,
        &mut pb,
    )?;
    airflow_container
        .image_from_product_image(resolved_product_image)
        .resources(merged_executor_config.resources.clone().into())
        .add_env_vars(build_airflow_template_envs(
            airflow,
            env_overrides,
            merged_executor_config,
            metadata_database_connection_details,
            git_sync_resources,
            resolved_product_image,
        ))
        .add_volume_mounts(airflow.volume_mounts())
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
    )?;

    metadata_database_connection_details.add_to_container(&mut airflow_container);

    pb.add_container(airflow_container.build());
    pb.add_volumes(airflow.volumes().clone())
        .context(AddVolumeSnafu)?;
    pb.add_volumes(controller_commons::create_volumes(
        &rolegroup_ref.object_name(),
        merged_executor_config
            .logging
            .containers
            .get(&Container::Airflow),
    ))
    .context(AddVolumeSnafu)?;

    if merged_executor_config.logging.enable_vector_agent {
        match &airflow
            .spec
            .cluster_config
            .vector_aggregator_config_map_name
        {
            Some(vector_aggregator_config_map_name) => {
                pb.add_container(build_logging_container(
                    resolved_product_image,
                    merged_executor_config
                        .logging
                        .containers
                        .get(&Container::Vector),
                    vector_aggregator_config_map_name,
                )?);
            }
            None => {
                VectorAggregatorConfigMapMissingSnafu.fail()?;
            }
        }
    }

    let mut pod_template = pb.build_template();
    pod_template.merge_from(pod_overrides.clone());

    let mut cm_builder = ConfigMapBuilder::new();

    let restarter_label =
        Label::try_from(("restarter.stackable.tech/enabled", "true")).context(BuildLabelSnafu)?;

    cm_builder
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(airflow)
                .name(airflow.executor_template_configmap_name())
                .ownerreference_from_resource(airflow, None, Some(true))
                .context(ObjectMissingMetadataForOwnerRefSnafu)?
                .with_recommended_labels(&build_recommended_labels(
                    airflow,
                    AIRFLOW_CONTROLLER_NAME,
                    &resolved_product_image.app_version_label_value,
                    "executor",
                    "executor-template",
                ))
                .context(ObjectMetaSnafu)?
                .with_label(restarter_label)
                .build(),
        )
        .add_data(
            TEMPLATE_NAME,
            serde_yaml::to_string(&pod_template).context(PodTemplateSerdeSnafu)?,
        );

    cm_builder.build().context(PodTemplateConfigMapSnafu)
}

pub fn error_policy(
    _obj: Arc<DeserializeGuard<v1alpha2::AirflowCluster>>,
    error: &Error,
    _ctx: Arc<Ctx>,
) -> Action {
    match error {
        // root object is invalid, will be requeued when modified anyway
        Error::InvalidAirflowCluster { .. } => Action::await_change(),

        _ => Action::requeue(*Duration::from_secs(10)),
    }
}

fn add_authentication_volumes_and_volume_mounts(
    authentication_config: &AirflowClientAuthenticationDetailsResolved,
    cb: &mut ContainerBuilder,
    pb: &mut PodBuilder,
) -> Result<()> {
    // Different authentication entries can reference the same secret
    // class or TLS certificate. It must be ensured that the volumes
    // and volume mounts are only added once in such a case.

    let mut ldap_authentication_providers = BTreeSet::new();
    let mut tls_client_credentials = BTreeSet::new();

    for auth_class_resolved in &authentication_config.authentication_classes_resolved {
        match auth_class_resolved {
            AirflowAuthenticationClassResolved::Ldap { provider } => {
                ldap_authentication_providers.insert(provider);
            }
            AirflowAuthenticationClassResolved::Oidc { provider, .. } => {
                tls_client_credentials.insert(&provider.tls);
            }
        }
    }

    for provider in ldap_authentication_providers {
        provider
            .add_volumes_and_mounts(pb, vec![cb])
            .context(AddLdapVolumesAndVolumeMountsSnafu)?;
    }

    for tls in tls_client_credentials {
        tls.add_volumes_and_mounts(pb, vec![cb])
            .context(AddTlsVolumesAndVolumeMountsSnafu)?;
    }
    Ok(())
}

fn add_git_sync_resources(
    pb: &mut PodBuilder,
    cb: &mut ContainerBuilder,
    git_sync_resources: &git_sync::v1alpha2::GitSyncResources,
    add_sidecar_containers: bool,
    add_init_containers: bool,
) -> Result<()> {
    if add_sidecar_containers {
        for container in git_sync_resources.git_sync_containers.iter().cloned() {
            pb.add_container(container);
        }
    }
    if add_init_containers {
        for container in git_sync_resources.git_sync_init_containers.iter().cloned() {
            pb.add_init_container(container);
        }
    }
    pb.add_volumes(git_sync_resources.git_content_volumes.to_owned())
        .context(AddVolumeSnafu)?;
    pb.add_volumes(git_sync_resources.git_ssh_volumes.to_owned())
        .context(AddVolumeSnafu)?;
    pb.add_volumes(git_sync_resources.git_ca_cert_volumes.to_owned())
        .context(AddVolumeSnafu)?;
    cb.add_volume_mounts(git_sync_resources.git_content_volume_mounts.to_owned())
        .context(AddVolumeMountSnafu)?;

    Ok(())
}

/// Convert user-supplied `envOverrides` into a list of [`EnvVar`]s.
fn env_vars_from_overrides(env_overrides: &HashMap<String, String>) -> Vec<EnvVar> {
    env_overrides
        .iter()
        .map(|(k, v)| EnvVar {
            name: k.clone(),
            value: Some(v.clone()),
            ..EnvVar::default()
        })
        .collect()
}
