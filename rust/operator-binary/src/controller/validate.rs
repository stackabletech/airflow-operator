//! The validate step in the AirflowCluster controller
//!
//! Validates the AirflowCluster spec and produces a [`ValidatedAirflowCluster`] where
//! all optional-after-merge fields are unwrapped and logging is pre-validated.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    str::FromStr,
};

use product_config::{ProductConfigManager, types::PropertyNameKind};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{
        configmap::ConfigMapBuilder,
        meta::ObjectMetaBuilder,
        pod::{
            PodBuilder,
            container::ContainerBuilder,
            resources::ResourceRequirementsBuilder,
            security::PodSecurityContextBuilder,
            volume::{ListenerOperatorVolumeSourceBuilder, ListenerReference},
        },
    },
    commons::product_image_selection::ResolvedProductImage,
    crd::git_sync,
    database_connections::drivers::{
        celery::CeleryDatabaseConnectionDetails, sqlalchemy::SqlAlchemyDatabaseConnectionDetails,
    },
    k8s_openapi::{
        DeepMerge,
        api::core::v1::{ConfigMap, PodTemplateSpec, Volume, VolumeMount},
    },
    kube::{Resource, ResourceExt},
    kvp::{Label, Labels, ObjectLabels},
    product_config_utils::{
        env_vars_from, env_vars_from_rolegroup_config, transform_all_roles_to_config,
        validate_all_roles_and_groups_config,
    },
    product_logging::{self, framework::LoggingError, spec::Logging},
    role_utils::RoleGroupRef,
};
use strum::{EnumDiscriminants, IntoEnumIterator, IntoStaticStr};

use super::{
    AIRFLOW_CONTROLLER_NAME, PrecomputedPodData, ValidatedAirflowCluster, ValidatedLogging,
    ValidatedRoleConfig, ValidatedRoleGroupConfig, dereference::DereferencedObjects,
};
use crate::{
    controller_commons::{self, CONFIG_VOLUME_NAME, LOG_CONFIG_VOLUME_NAME, LOG_VOLUME_NAME},
    crd::{
        AIRFLOW_CONFIG_FILENAME, APP_NAME, AirflowConfig, AirflowExecutor, AirflowRole,
        CONFIG_PATH, Container, ExecutorConfig, LISTENER_VOLUME_NAME, LOG_CONFIG_DIR,
        OPERATOR_NAME, STACKABLE_LOG_DIR, TEMPLATE_NAME,
        authentication::{
            AirflowAuthenticationClassResolved, AirflowClientAuthenticationDetailsResolved,
        },
        authorization::AirflowAuthorizationResolved,
        v1alpha2,
    },
    env_vars,
    framework::{
        product_logging::framework::{
            VectorContainerLogConfig, validate_logging_configuration_for_container,
        },
        types::{
            kubernetes::{ConfigMapName, NamespaceName, Uid},
            operator::ClusterName,
        },
    },
    product_logging::extend_config_map_with_log_config,
};

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("failed to validate cluster name"))]
    InvalidClusterName {
        source: crate::framework::macros::attributed_string_type::Error,
    },

    #[snafu(display("object has no associated namespace"))]
    ObjectHasNoNamespace,

    #[snafu(display("failed to validate cluster namespace"))]
    InvalidClusterNamespace {
        source: crate::framework::macros::attributed_string_type::Error,
    },

    #[snafu(display("object has no UID"))]
    ObjectHasNoUid,

    #[snafu(display("failed to validate cluster UID"))]
    InvalidClusterUid {
        source: crate::framework::macros::attributed_string_type::Error,
    },

    #[snafu(display("failed to validate logging configuration"))]
    ValidateLoggingConfig {
        source: crate::framework::product_logging::framework::Error,
    },

    #[snafu(display("vectorAggregatorConfigMapName must be set when vector agent is enabled"))]
    MissingVectorAggregatorConfigMapName,

    #[snafu(display("failed to parse vector aggregator ConfigMap name"))]
    ParseVectorAggregatorConfigMapName {
        source: crate::framework::macros::attributed_string_type::Error,
    },

    #[snafu(display("graceful shutdown timeout is not configured"))]
    MissingGracefulShutdownTimeout,

    #[snafu(display("failed to resolve and merge config for role and role group"))]
    FailedToResolveConfig { source: crate::crd::Error },

    #[snafu(display("failed to construct Airflow configuration"))]
    ConstructConfig { source: crate::config::Error },

    #[snafu(display("failed to write config file"))]
    BuildConfigFile {
        source: product_config::flask_app_config_writer::FlaskAppConfigWriterError,
    },

    #[snafu(display("Failed to transform configs"))]
    ProductConfigTransform {
        source: stackable_operator::product_config_utils::Error,
    },

    #[snafu(display("invalid product config"))]
    InvalidProductConfig {
        source: stackable_operator::product_config_utils::Error,
    },

    #[snafu(display("could not parse Airflow role [{role}]"))]
    UnidentifiedAirflowRole {
        source: strum::ParseError,
        role: String,
    },

    #[snafu(display("object defines no airflow config role"))]
    NoAirflowRole,

    #[snafu(display("failed to build environment variables"))]
    BuildEnvVars { source: crate::env_vars::Error },

    #[snafu(display("invalid git-sync specification"))]
    InvalidGitSyncSpec { source: git_sync::v1alpha2::Error },

    #[snafu(display("failed to configure logging"))]
    ConfigureLogging { source: LoggingError },

    #[snafu(display("failed to add LDAP volumes and volume mounts"))]
    AddLdapVolumesAndVolumeMounts {
        source: stackable_operator::crd::authentication::ldap::v1alpha1::Error,
    },

    #[snafu(display("failed to add TLS volumes and volume mounts"))]
    AddTlsVolumesAndVolumeMounts {
        source: stackable_operator::commons::tls_verification::TlsClientDetailsError,
    },

    #[snafu(display("failed to build listener volume"))]
    BuildListenerVolume {
        source: stackable_operator::builder::pod::volume::ListenerOperatorVolumeSourceBuilderError,
    },

    #[snafu(display("failed to build labels"))]
    BuildLabels {
        source: stackable_operator::kvp::LabelError,
    },

    #[snafu(display("invalid container name"))]
    InvalidContainerName {
        source: stackable_operator::builder::pod::container::Error,
    },

    #[snafu(display("failed to add volume mount"))]
    AddVolumeMount {
        source: stackable_operator::builder::pod::container::Error,
    },

    #[snafu(display("failed to add volume"))]
    AddVolume {
        source: stackable_operator::builder::pod::Error,
    },

    #[snafu(display("failed to serialize pod template"))]
    PodTemplateSerde { source: serde_yaml::Error },

    #[snafu(display("failed to build pod template ConfigMap"))]
    PodTemplateConfigMap {
        source: stackable_operator::builder::configmap::Error,
    },

    #[snafu(display("failed to build object metadata"))]
    ObjectMeta {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to build graceful shutdown config"))]
    GracefulShutdown {
        source: stackable_operator::builder::pod::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub fn validate_logging(
    logging: &Logging<Container>,
    main_container: Container,
    vector_aggregator_config_map_name: Option<&str>,
) -> Result<ValidatedLogging> {
    let airflow_container = validate_logging_configuration_for_container(logging, main_container)
        .context(ValidateLoggingConfigSnafu)?;

    let vector_container = if logging.enable_vector_agent {
        let aggregator_name =
            vector_aggregator_config_map_name.context(MissingVectorAggregatorConfigMapNameSnafu)?;
        ConfigMapName::from_str(aggregator_name)
            .context(ParseVectorAggregatorConfigMapNameSnafu)?;
        let log_config = validate_logging_configuration_for_container(logging, Container::Vector)
            .context(ValidateLoggingConfigSnafu)?;
        Some(VectorContainerLogConfig { log_config })
    } else {
        None
    };

    let git_sync_container_log_config = logging.for_container(&Container::GitSync).into_owned();

    Ok(ValidatedLogging {
        airflow_container,
        vector_container,
        git_sync_container_log_config,
    })
}

pub fn validate_airflow_config(
    config: &AirflowConfig,
    vector_aggregator_config_map_name: Option<&str>,
    config_file_content: String,
) -> Result<ValidatedRoleGroupConfig> {
    let logging = validate_logging(
        &config.logging,
        Container::Airflow,
        vector_aggregator_config_map_name,
    )?;

    let graceful_shutdown_timeout = config
        .graceful_shutdown_timeout
        .context(MissingGracefulShutdownTimeoutSnafu)?;

    Ok(ValidatedRoleGroupConfig {
        resources: config.resources.clone(),
        logging,
        affinity: config.affinity.clone(),
        graceful_shutdown_timeout,
        config_file_content,
    })
}

pub fn validate_executor_config(
    config: &ExecutorConfig,
    vector_aggregator_config_map_name: Option<&str>,
    config_file_content: String,
) -> Result<ValidatedRoleGroupConfig> {
    let logging = validate_logging(
        &config.logging,
        Container::Base,
        vector_aggregator_config_map_name,
    )?;

    let graceful_shutdown_timeout = config
        .graceful_shutdown_timeout
        .context(MissingGracefulShutdownTimeoutSnafu)?;

    Ok(ValidatedRoleGroupConfig {
        resources: config.resources.clone(),
        logging,
        affinity: config.affinity.clone(),
        graceful_shutdown_timeout,
        config_file_content,
    })
}

/// Generates the `webserver_config.py` content for a role group.
///
/// This function is called during the validate stage so that the build stage can
/// construct ConfigMaps infallibly.
pub fn generate_config_file_content(
    authentication_config: &crate::crd::authentication::AirflowClientAuthenticationDetailsResolved,
    authorization_config: &crate::crd::authorization::AirflowAuthorizationResolved,
    product_version: &str,
    rolegroup_config_overrides: &std::collections::HashMap<
        product_config::types::PropertyNameKind,
        BTreeMap<String, String>,
    >,
) -> Result<String> {
    use std::io::Write;

    use product_config::flask_app_config_writer;
    use stackable_operator::product_config_utils::{
        CONFIG_OVERRIDE_FILE_FOOTER_KEY, CONFIG_OVERRIDE_FILE_HEADER_KEY,
    };

    use crate::{
        config::{self, PYTHON_IMPORTS},
        crd::{AIRFLOW_CONFIG_FILENAME, AirflowConfigOptions},
    };

    let mut config = BTreeMap::new();
    config::add_airflow_config(
        &mut config,
        authentication_config,
        authorization_config,
        product_version,
    )
    .context(ConstructConfigSnafu)?;

    let mut file_overrides = rolegroup_config_overrides
        .get(&product_config::types::PropertyNameKind::File(
            AIRFLOW_CONFIG_FILENAME.to_string(),
        ))
        .cloned()
        .unwrap_or_default();

    config.append(&mut file_overrides);

    let mut config_file = Vec::new();

    if let Some(header) = config.remove(CONFIG_OVERRIDE_FILE_HEADER_KEY) {
        writeln!(config_file, "{}", header).expect("writing to Vec<u8> is infallible");
    }

    let temp_file_footer: Option<String> = config.remove(CONFIG_OVERRIDE_FILE_FOOTER_KEY);

    flask_app_config_writer::write::<AirflowConfigOptions, _, _>(
        &mut config_file,
        config.iter(),
        PYTHON_IMPORTS,
    )
    .context(BuildConfigFileSnafu)?;

    if let Some(footer) = temp_file_footer {
        writeln!(config_file, "{}", footer).expect("writing to Vec<u8> is infallible");
    }

    Ok(String::from_utf8(config_file).expect("flask_app_config_writer produces valid UTF-8"))
}

/// Top-level validation: runs product_config, merges/validates per-rolegroup configs,
/// generates config file contents, and assembles a [`ValidatedAirflowCluster`].
pub fn validate_cluster(
    airflow: &v1alpha2::AirflowCluster,
    dereferenced: &DereferencedObjects,
    product_config_manager: &ProductConfigManager,
) -> Result<ValidatedAirflowCluster> {
    let vector_aggregator_config_map_name = airflow
        .spec
        .cluster_config
        .vector_aggregator_config_map_name
        .as_deref();

    // --- product_config transform + validate ---
    let mut roles = HashMap::new();
    for role in AirflowRole::iter() {
        if let Some(resolved_role) = airflow.get_role(&role) {
            roles.insert(
                role.to_string(),
                (
                    vec![
                        PropertyNameKind::Env,
                        PropertyNameKind::File(AIRFLOW_CONFIG_FILENAME.into()),
                    ],
                    resolved_role.clone(),
                ),
            );
        }
    }

    let role_config = transform_all_roles_to_config(airflow, &roles);
    let validated_role_config = validate_all_roles_and_groups_config(
        &dereferenced.resolved_product_image.product_version,
        &role_config.context(ProductConfigTransformSnafu)?,
        product_config_manager,
        false,
        false,
    )
    .context(InvalidProductConfigSnafu)?;

    // --- compute database connection details (infallible) ---
    let templating_mechanism =
        stackable_operator::database_connections::TemplatingMechanism::BashEnvSubstitution;
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

    // --- compute auth volumes/mounts (fallible) ---
    let (auth_volumes, auth_volume_mounts) =
        compute_auth_volumes_and_mounts(&dereferenced.authentication_config)?;

    // --- service account name (matches build_rbac_resources output: "{cluster}-serviceaccount") ---
    let service_account_name = format!("{}-serviceaccount", airflow.name_any());

    // --- per-role/rolegroup validation ---
    let mut validated_role_groups = BTreeMap::new();
    let mut all_precomputed_pod_data = BTreeMap::new();

    for (role_name, role_config) in validated_role_config.iter() {
        let airflow_role =
            AirflowRole::from_str(role_name).context(UnidentifiedAirflowRoleSnafu {
                role: role_name.to_string(),
            })?;

        let mut validated_groups = BTreeMap::new();
        let mut pod_data_groups = BTreeMap::new();

        for (rolegroup_name, rolegroup_config) in role_config.iter() {
            let rolegroup_ref = RoleGroupRef {
                cluster: stackable_operator::kube::runtime::reflector::ObjectRef::from_obj(airflow),
                role: role_name.into(),
                role_group: rolegroup_name.into(),
            };

            let merged_airflow_config = airflow
                .merged_config(&airflow_role, &rolegroup_ref)
                .context(FailedToResolveConfigSnafu)?;

            let config_file_content = generate_config_file_content(
                &dereferenced.authentication_config,
                &dereferenced.authorization_config,
                &dereferenced.resolved_product_image.product_version,
                rolegroup_config,
            )?;

            let validated_config = validate_airflow_config(
                &merged_airflow_config,
                vector_aggregator_config_map_name,
                config_file_content,
            )?;

            let pod_data = compute_precomputed_pod_data(
                airflow,
                &airflow_role,
                &rolegroup_ref,
                rolegroup_config,
                &dereferenced.resolved_product_image,
                &dereferenced.authentication_config,
                &dereferenced.authorization_config,
                &metadata_database_connection_details,
                &celery_database_connection_details,
                &validated_config.logging,
                &auth_volumes,
                &auth_volume_mounts,
                &service_account_name,
            )?;

            validated_groups.insert(rolegroup_name.clone(), validated_config);
            pod_data_groups.insert(rolegroup_name.clone(), pod_data);
        }

        validated_role_groups.insert(airflow_role.clone(), validated_groups);
        all_precomputed_pod_data.insert(airflow_role, pod_data_groups);
    }

    // --- per-role config (PDB, listeners) ---
    let mut validated_role_configs_map = BTreeMap::new();
    for role in AirflowRole::iter() {
        if let Some(role_config) = airflow.role_config(&role) {
            let pdb = &role_config.pod_disruption_budget;
            let listener_class = role.listener_class_name(airflow);
            let group_listener_name = airflow.group_listener_name(&role);
            validated_role_configs_map.insert(
                role,
                ValidatedRoleConfig {
                    pdb_enabled: pdb.enabled,
                    pdb_max_unavailable: pdb.max_unavailable,
                    listener_class,
                    group_listener_name,
                },
            );
        }
    }

    // --- executor template config maps ---
    let executor_template_config_maps = if let AirflowExecutor::KubernetesExecutors {
        common_configuration,
    } = &airflow.spec.executor
    {
        let merged_executor_config = airflow
            .merged_executor_config(&common_configuration.config)
            .context(FailedToResolveConfigSnafu)?;

        let config_file_content = generate_config_file_content(
            &dereferenced.authentication_config,
            &dereferenced.authorization_config,
            &dereferenced.resolved_product_image.product_version,
            &HashMap::new(),
        )?;

        let validated_config = validate_executor_config(
            &merged_executor_config,
            vector_aggregator_config_map_name,
            config_file_content,
        )?;

        build_executor_template_config_maps(
            airflow,
            &dereferenced.resolved_product_image,
            &dereferenced.authentication_config,
            &metadata_database_connection_details,
            &service_account_name,
            &validated_config,
            common_configuration,
        )?
    } else {
        Vec::new()
    };

    // --- assemble ---
    validate(
        airflow,
        &dereferenced.resolved_product_image,
        validated_role_groups,
        all_precomputed_pod_data,
        executor_template_config_maps,
        validated_role_configs_map,
    )
}

/// Validates the AirflowCluster and produces a [`ValidatedAirflowCluster`] containing
/// all role groups with their validated configs.
pub fn validate(
    airflow: &v1alpha2::AirflowCluster,
    resolved_product_image: &stackable_operator::commons::product_image_selection::ResolvedProductImage,
    validated_role_configs: BTreeMap<AirflowRole, BTreeMap<String, ValidatedRoleGroupConfig>>,
    precomputed_pod_data: BTreeMap<AirflowRole, BTreeMap<String, PrecomputedPodData>>,
    executor_template_config_maps: Vec<ConfigMap>,
    role_configs: BTreeMap<AirflowRole, ValidatedRoleConfig>,
) -> Result<ValidatedAirflowCluster> {
    let cluster_name =
        ClusterName::from_str(&airflow.name_any()).context(InvalidClusterNameSnafu)?;
    let namespace =
        NamespaceName::from_str(&airflow.namespace().context(ObjectHasNoNamespaceSnafu)?)
            .context(InvalidClusterNamespaceSnafu)?;
    let uid = Uid::from_str(airflow.meta().uid.as_deref().context(ObjectHasNoUidSnafu)?)
        .context(InvalidClusterUidSnafu)?;

    Ok(ValidatedAirflowCluster::new(
        resolved_product_image.clone(),
        cluster_name,
        namespace,
        uid,
        validated_role_configs,
        precomputed_pod_data,
        executor_template_config_maps,
        role_configs,
        airflow.spec.executor.clone(),
    ))
}

/// Extracts auth volumes and volume mounts using temporary builders.
///
/// The upstream LDAP/TLS provider APIs require `PodBuilder`/`ContainerBuilder` references.
/// We create temporary builders, call the auth methods, then extract the raw volumes and mounts.
fn compute_auth_volumes_and_mounts(
    authentication_config: &AirflowClientAuthenticationDetailsResolved,
) -> Result<(Vec<Volume>, Vec<VolumeMount>)> {
    let mut pb = PodBuilder::new();
    let mut cb = ContainerBuilder::new("dummy").expect("'dummy' is a valid container name");

    let mut ldap_providers = BTreeSet::new();
    let mut tls_credentials = BTreeSet::new();

    for auth_class in &authentication_config.authentication_classes_resolved {
        match auth_class {
            AirflowAuthenticationClassResolved::Ldap { provider } => {
                ldap_providers.insert(provider);
            }
            AirflowAuthenticationClassResolved::Oidc { provider, .. } => {
                tls_credentials.insert(&provider.tls);
            }
        }
    }

    for provider in ldap_providers {
        provider
            .add_volumes_and_mounts(&mut pb, vec![&mut cb])
            .context(AddLdapVolumesAndVolumeMountsSnafu)?;
    }
    for tls in tls_credentials {
        tls.add_volumes_and_mounts(&mut pb, vec![&mut cb])
            .context(AddTlsVolumesAndVolumeMountsSnafu)?;
    }

    let container = cb.build();
    let pod_template = pb.build_template();

    let volumes = pod_template
        .spec
        .and_then(|s| s.volumes)
        .unwrap_or_default();
    let mounts = container.volume_mounts.unwrap_or_default();

    Ok((volumes, mounts))
}

/// Builds the executor template ConfigMaps for KubernetesExecutor mode.
///
/// Produces two ConfigMaps:
/// 1. A logging/config ConfigMap for the executor pods (equivalent to a rolegroup ConfigMap)
/// 2. A pod template ConfigMap containing a serialised PodTemplate that Airflow uses to
///    launch executor pods
///
/// This is done in the validate stage because it uses PodBuilder/ContainerBuilder which
/// are fallible. The build stage then just passes these through to KubernetesResources.
#[allow(clippy::too_many_arguments)]
fn build_executor_template_config_maps(
    airflow: &v1alpha2::AirflowCluster,
    resolved_product_image: &ResolvedProductImage,
    authentication_config: &AirflowClientAuthenticationDetailsResolved,
    metadata_database_connection_details: &SqlAlchemyDatabaseConnectionDetails,
    service_account_name: &str,
    validated_config: &super::ValidatedRoleGroupConfig,
    common_configuration: &crate::crd::AirflowExecutorCommonConfiguration,
) -> Result<Vec<ConfigMap>> {
    let executor_rolegroup_ref = RoleGroupRef {
        cluster: stackable_operator::kube::runtime::reflector::ObjectRef::from_obj(airflow),
        role: "executor".into(),
        role_group: "kubernetes".into(),
    };

    // 1. Build the executor logging/config ConfigMap
    let executor_config_cm = {
        let metadata = ObjectMetaBuilder::new()
            .name(executor_rolegroup_ref.object_name())
            .namespace_opt(airflow.namespace())
            .ownerreference_from_resource(airflow, None, Some(true))
            .context(ObjectMetaSnafu)?
            .with_recommended_labels(&build_object_labels(
                airflow,
                resolved_product_image,
                "executor",
                "executor-template",
            ))
            .context(ObjectMetaSnafu)?
            .build();

        let mut cm_builder = ConfigMapBuilder::new();
        cm_builder.metadata(metadata);
        cm_builder.add_data(
            AIRFLOW_CONFIG_FILENAME,
            validated_config.config_file_content.clone(),
        );

        extend_config_map_with_log_config(
            &executor_rolegroup_ref,
            &Container::Base,
            &validated_config.logging.airflow_container,
            validated_config.logging.vector_container.as_ref(),
            &mut cm_builder,
            resolved_product_image,
        );

        cm_builder.build().context(PodTemplateConfigMapSnafu)?
    };

    // 2. Build the executor pod template ConfigMap
    let executor_template_cm = {
        // git-sync resources for the executor template
        let git_sync_resources = git_sync::v1alpha2::GitSyncResources::new(
            &airflow.spec.cluster_config.dags_git_sync,
            resolved_product_image,
            &env_vars_from(&common_configuration.env_overrides),
            &airflow.volume_mounts(),
            LOG_VOLUME_NAME,
            &validated_config.logging.git_sync_container_log_config,
        )
        .context(InvalidGitSyncSpecSnafu)?;

        let mut pb = PodBuilder::new();
        let pb_metadata = ObjectMetaBuilder::new()
            .with_recommended_labels(&build_object_labels(
                airflow,
                resolved_product_image,
                "executor",
                "executor-template",
            ))
            .context(ObjectMetaSnafu)?
            .build();

        pb.metadata(pb_metadata)
            .image_pull_secrets_from_product_image(resolved_product_image)
            .affinity(&validated_config.affinity)
            .service_account_name(service_account_name)
            .restart_policy("Never")
            .security_context(PodSecurityContextBuilder::new().fs_group(1000).build());

        pb.termination_grace_period(&validated_config.graceful_shutdown_timeout)
            .context(GracefulShutdownSnafu)?;

        // Container name "base" is an Airflow requirement
        let mut airflow_container = ContainerBuilder::new(&Container::Base.to_string())
            .context(InvalidContainerNameSnafu)?;

        // Auth volumes and mounts
        add_authentication_volumes_and_volume_mounts(
            authentication_config,
            &mut airflow_container,
            &mut pb,
        )?;

        airflow_container
            .image_from_product_image(resolved_product_image)
            .resources(validated_config.resources.clone().into())
            .add_env_vars(env_vars::build_airflow_template_envs(
                airflow,
                &common_configuration.env_overrides,
                validated_config.logging.is_vector_agent_enabled(),
                metadata_database_connection_details,
                &git_sync_resources,
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

        // Git-sync resources (init containers only, no sidecars for executor template)
        for container in git_sync_resources.git_sync_init_containers.iter().cloned() {
            pb.add_init_container(container);
        }
        pb.add_volumes(git_sync_resources.git_content_volumes.clone())
            .context(AddVolumeSnafu)?;
        pb.add_volumes(git_sync_resources.git_ssh_volumes.clone())
            .context(AddVolumeSnafu)?;
        pb.add_volumes(git_sync_resources.git_ca_cert_volumes.clone())
            .context(AddVolumeSnafu)?;
        airflow_container
            .add_volume_mounts(git_sync_resources.git_content_volume_mounts.clone())
            .context(AddVolumeMountSnafu)?;

        // Database connection env vars
        metadata_database_connection_details.add_to_container(&mut airflow_container);

        pb.add_container(airflow_container.build());
        pb.add_volumes(airflow.volumes().clone())
            .context(AddVolumeSnafu)?;
        pb.add_volumes(controller_commons::create_volumes(
            &executor_rolegroup_ref.object_name(),
            &validated_config.logging.airflow_container,
        ))
        .context(AddVolumeSnafu)?;

        if let Some(vector_config) = &validated_config.logging.vector_container {
            let vector_aggregator_config_map_name = airflow
                .spec
                .cluster_config
                .vector_aggregator_config_map_name
                .as_deref()
                .context(MissingVectorAggregatorConfigMapNameSnafu)?;
            pb.add_container(build_logging_container(
                resolved_product_image,
                vector_config,
                vector_aggregator_config_map_name,
            )?);
        }

        let mut pod_template = pb.build_template();
        pod_template.merge_from(common_configuration.pod_overrides.clone());

        let restarter_label = Label::try_from(("restarter.stackable.tech/enabled", "true"))
            .expect("static label is always valid");

        let mut cm_builder = ConfigMapBuilder::new();
        cm_builder
            .metadata(
                ObjectMetaBuilder::new()
                    .name_and_namespace(airflow)
                    .name(airflow.executor_template_configmap_name())
                    .ownerreference_from_resource(airflow, None, Some(true))
                    .context(ObjectMetaSnafu)?
                    .with_recommended_labels(&build_object_labels(
                        airflow,
                        resolved_product_image,
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

        cm_builder.build().context(PodTemplateConfigMapSnafu)?
    };

    Ok(vec![executor_config_cm, executor_template_cm])
}

/// Helper to add authentication volumes and volume mounts directly to builders.
/// Used by the executor template where we build a PodTemplate using PodBuilder/ContainerBuilder.
fn add_authentication_volumes_and_volume_mounts(
    authentication_config: &AirflowClientAuthenticationDetailsResolved,
    cb: &mut ContainerBuilder,
    pb: &mut PodBuilder,
) -> Result<()> {
    let mut ldap_providers = BTreeSet::new();
    let mut tls_credentials = BTreeSet::new();

    for auth_class in &authentication_config.authentication_classes_resolved {
        match auth_class {
            AirflowAuthenticationClassResolved::Ldap { provider } => {
                ldap_providers.insert(provider);
            }
            AirflowAuthenticationClassResolved::Oidc { provider, .. } => {
                tls_credentials.insert(&provider.tls);
            }
        }
    }

    for provider in ldap_providers {
        provider
            .add_volumes_and_mounts(pb, vec![cb])
            .context(AddLdapVolumesAndVolumeMountsSnafu)?;
    }
    for tls in tls_credentials {
        tls.add_volumes_and_mounts(pb, vec![cb])
            .context(AddTlsVolumesAndVolumeMountsSnafu)?;
    }
    Ok(())
}

fn build_object_labels<'a>(
    airflow: &'a v1alpha2::AirflowCluster,
    resolved_product_image: &'a ResolvedProductImage,
    role: &'a str,
    role_group: &'a str,
) -> ObjectLabels<'a, v1alpha2::AirflowCluster> {
    ObjectLabels {
        owner: airflow,
        app_name: APP_NAME,
        app_version: &resolved_product_image.app_version_label_value,
        operator_name: OPERATOR_NAME,
        controller_name: AIRFLOW_CONTROLLER_NAME,
        role,
        role_group,
    }
}

fn build_logging_container(
    resolved_product_image: &ResolvedProductImage,
    vector_config: &VectorContainerLogConfig,
    vector_aggregator_config_map_name: &str,
) -> Result<stackable_operator::k8s_openapi::api::core::v1::Container> {
    let raw_log_config = vector_config.log_config.to_raw_container_log_config();

    product_logging::framework::vector_container(
        resolved_product_image,
        CONFIG_VOLUME_NAME,
        LOG_VOLUME_NAME,
        Some(&raw_log_config),
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

/// Computes all pod-level data needed by the build stage to construct StatefulSets infallibly.
#[allow(clippy::too_many_arguments)]
fn compute_precomputed_pod_data(
    airflow: &v1alpha2::AirflowCluster,
    airflow_role: &AirflowRole,
    rolegroup_ref: &RoleGroupRef<v1alpha2::AirflowCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    resolved_product_image: &ResolvedProductImage,
    authentication_config: &AirflowClientAuthenticationDetailsResolved,
    authorization_config: &AirflowAuthorizationResolved,
    metadata_database_connection_details: &SqlAlchemyDatabaseConnectionDetails,
    celery_database_connection_details: &Option<(
        CeleryDatabaseConnectionDetails,
        CeleryDatabaseConnectionDetails,
    )>,
    validated_logging: &ValidatedLogging,
    auth_volumes: &[Volume],
    auth_volume_mounts: &[VolumeMount],
    service_account_name: &str,
) -> Result<PrecomputedPodData> {
    let executor = &airflow.spec.executor;

    // --- git-sync resources ---
    let git_sync_resources = git_sync::v1alpha2::GitSyncResources::new(
        &airflow.spec.cluster_config.dags_git_sync,
        resolved_product_image,
        &env_vars_from_rolegroup_config(rolegroup_config),
        &airflow.volume_mounts(),
        LOG_VOLUME_NAME,
        &validated_logging.git_sync_container_log_config,
    )
    .context(InvalidGitSyncSpecSnafu)?;

    // --- env vars ---
    let mut env_vars = env_vars::build_airflow_statefulset_envs(
        airflow,
        airflow_role,
        rolegroup_config,
        executor,
        authentication_config,
        authorization_config,
        metadata_database_connection_details,
        celery_database_connection_details,
        &git_sync_resources,
        resolved_product_image,
    )
    .context(BuildEnvVarsSnafu)?;

    // Database connection details add secret-referenced env vars via ContainerBuilder.
    // Extract them using a temp builder.
    let db_env_vars = {
        let mut cb = ContainerBuilder::new("dummy").expect("'dummy' is a valid container name");
        metadata_database_connection_details.add_to_container(&mut cb);
        if let Some((celery_result_backend, celery_broker)) = celery_database_connection_details {
            celery_result_backend.add_to_container(&mut cb);
            celery_broker.add_to_container(&mut cb);
        }
        cb.build().env.unwrap_or_default()
    };
    env_vars.extend(db_env_vars);

    // --- commands ---
    let airflow_commands =
        airflow_role.get_commands(airflow, authentication_config, resolved_product_image);

    // --- git-sync containers/volumes ---
    let use_git_sync_init_containers = matches!(executor, AirflowExecutor::CeleryExecutors { .. });
    let git_sync_containers = git_sync_resources.git_sync_containers.clone();
    let git_sync_init_containers = if use_git_sync_init_containers {
        git_sync_resources.git_sync_init_containers.clone()
    } else {
        Vec::new()
    };
    let mut git_sync_volumes = git_sync_resources.git_content_volumes.clone();
    git_sync_volumes.extend(git_sync_resources.git_ssh_volumes.clone());
    git_sync_volumes.extend(git_sync_resources.git_ca_cert_volumes.clone());
    let git_sync_volume_mounts = git_sync_resources.git_content_volume_mounts.clone();

    // --- vector container ---
    let vector_container = if let Some(vector_config) = &validated_logging.vector_container {
        let vector_aggregator_config_map_name = airflow
            .spec
            .cluster_config
            .vector_aggregator_config_map_name
            .as_deref()
            .context(MissingVectorAggregatorConfigMapNameSnafu)?;
        Some(build_logging_container(
            resolved_product_image,
            vector_config,
            vector_aggregator_config_map_name,
        )?)
    } else {
        None
    };

    // --- replicas ---
    let binding = airflow.get_role(airflow_role);
    let role = binding.as_ref().context(NoAirflowRoleSnafu)?;
    let rolegroup = role.role_groups.get(&rolegroup_ref.role_group);
    let replicas = rolegroup.and_then(|rg| rg.replicas);

    // --- pod overrides ---
    let mut pod_overrides = PodTemplateSpec::default();
    pod_overrides.merge_from(role.config.pod_overrides.clone());
    if let Some(rg) = rolegroup {
        pod_overrides.merge_from(rg.config.pod_overrides.clone());
    }

    // --- executor template configmap name ---
    let executor_template_configmap_name =
        if matches!(executor, AirflowExecutor::KubernetesExecutors { .. }) {
            Some(airflow.executor_template_configmap_name())
        } else {
            None
        };

    // --- listener PVC ---
    let listener_volume_claim_template = if airflow_role.get_http_port().is_some() {
        if let Some(listener_group_name) = airflow.group_listener_name(airflow_role) {
            let unversioned_labels = Labels::recommended(&ObjectLabels {
                owner: airflow,
                app_name: APP_NAME,
                app_version: "none",
                operator_name: OPERATOR_NAME,
                controller_name: AIRFLOW_CONTROLLER_NAME,
                role: &rolegroup_ref.role,
                role_group: &rolegroup_ref.role_group,
            })
            .context(BuildLabelsSnafu)?;

            let pvc = ListenerOperatorVolumeSourceBuilder::new(
                &ListenerReference::ListenerName(listener_group_name),
                &unversioned_labels,
            )
            .build_pvc(LISTENER_VOLUME_NAME.to_string())
            .context(BuildListenerVolumeSnafu)?;
            Some(pvc)
        } else {
            None
        }
    } else {
        None
    };

    // --- user-defined extra volumes/mounts from CRD ---
    let extra_volumes = airflow.volumes().clone();
    let extra_volume_mounts = airflow.volume_mounts();

    Ok(PrecomputedPodData {
        env_vars,
        airflow_commands,
        auth_volumes: auth_volumes.to_vec(),
        auth_volume_mounts: auth_volume_mounts.to_vec(),
        extra_volumes,
        extra_volume_mounts,
        git_sync_containers,
        git_sync_init_containers,
        git_sync_volumes,
        git_sync_volume_mounts,
        vector_container,
        service_account_name: service_account_name.to_string(),
        replicas,
        pod_overrides,
        executor: executor.clone(),
        executor_template_configmap_name,
        listener_volume_claim_template,
    })
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, str::FromStr};

    use stackable_operator::{
        commons::product_image_selection::ResolvedProductImage,
        kvp::LabelValue,
        product_logging::spec::{
            AutomaticContainerLogConfig, ContainerLogConfig, ContainerLogConfigChoice, Logging,
        },
        shared::time::Duration,
    };

    use super::*;
    use crate::crd::{AirflowConfig, Container};

    fn airflow_config_with_logging(enable_vector: bool) -> AirflowConfig {
        let mut containers = BTreeMap::new();
        containers.insert(
            Container::Airflow,
            ContainerLogConfig {
                choice: Some(ContainerLogConfigChoice::Automatic(
                    AutomaticContainerLogConfig::default(),
                )),
            },
        );
        if enable_vector {
            containers.insert(
                Container::Vector,
                ContainerLogConfig {
                    choice: Some(ContainerLogConfigChoice::Automatic(
                        AutomaticContainerLogConfig::default(),
                    )),
                },
            );
        }
        AirflowConfig {
            resources: Default::default(),
            logging: Logging {
                enable_vector_agent: enable_vector,
                containers,
            },
            affinity: Default::default(),
            graceful_shutdown_timeout: Some(Duration::from_secs(120)),
        }
    }

    #[test]
    fn test_validate_airflow_config_without_vector() {
        let config = airflow_config_with_logging(false);
        let result = validate_airflow_config(&config, None, String::new());
        assert!(result.is_ok());
        let validated = result.unwrap();
        assert!(validated.logging.vector_container.is_none());
        assert!(!validated.logging.is_vector_agent_enabled());
        assert_eq!(
            validated.graceful_shutdown_timeout,
            Duration::from_secs(120)
        );
    }

    #[test]
    fn test_validate_airflow_config_with_vector() {
        let config = airflow_config_with_logging(true);
        let result =
            validate_airflow_config(&config, Some("vector-aggregator-discovery"), String::new());
        assert!(result.is_ok());
        let validated = result.unwrap();
        assert!(validated.logging.vector_container.is_some());
        assert!(validated.logging.is_vector_agent_enabled());
    }

    #[test]
    fn test_validate_vector_enabled_missing_config_map() {
        let config = airflow_config_with_logging(true);
        let result = validate_airflow_config(&config, None, String::new());
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_missing_graceful_shutdown() {
        let mut config = airflow_config_with_logging(false);
        config.graceful_shutdown_timeout = None;
        let result = validate_airflow_config(&config, None, String::new());
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_ok() {
        let (airflow, image) = test_objects();
        let result = validate(
            &airflow,
            &image,
            BTreeMap::new(),
            BTreeMap::new(),
            vec![],
            BTreeMap::new(),
        );
        assert!(result.is_ok());
        let validated = result.unwrap();
        assert_eq!(validated.name.to_string(), "my-airflow");
        assert_eq!(validated.namespace.to_string(), "default");
    }

    #[test]
    fn test_validate_err_missing_name() {
        test_validate_err(
            |airflow, _| airflow.metadata.name = None,
            ErrorDiscriminants::InvalidClusterName,
        );
    }

    #[test]
    fn test_validate_err_missing_namespace() {
        test_validate_err(
            |airflow, _| airflow.metadata.namespace = None,
            ErrorDiscriminants::ObjectHasNoNamespace,
        );
    }

    #[test]
    fn test_validate_err_missing_uid() {
        test_validate_err(
            |airflow, _| airflow.metadata.uid = None,
            ErrorDiscriminants::ObjectHasNoUid,
        );
    }

    #[test]
    fn test_validate_err_invalid_cluster_name() {
        test_validate_err(
            |airflow, _| {
                airflow.metadata.name =
                    Some("THIS-IS-NOT-A-VALID-DNS-LABEL-NAME-BECAUSE-UPPERCASE".to_string())
            },
            ErrorDiscriminants::InvalidClusterName,
        );
    }

    #[test]
    fn test_validate_err_invalid_namespace() {
        test_validate_err(
            |airflow, _| airflow.metadata.namespace = Some("INVALID NAMESPACE".to_string()),
            ErrorDiscriminants::InvalidClusterNamespace,
        );
    }

    #[test]
    fn test_validate_err_invalid_uid() {
        test_validate_err(
            |airflow, _| airflow.metadata.uid = Some("not-a-uuid".to_string()),
            ErrorDiscriminants::InvalidClusterUid,
        );
    }

    fn test_validate_err(
        mutate: fn(&mut v1alpha2::AirflowCluster, &mut ResolvedProductImage),
        expected: ErrorDiscriminants,
    ) {
        let (mut airflow, mut image) = test_objects();
        mutate(&mut airflow, &mut image);
        let result = validate(
            &airflow,
            &image,
            BTreeMap::new(),
            BTreeMap::new(),
            vec![],
            BTreeMap::new(),
        );
        match result {
            Err(err) => assert_eq!(expected, ErrorDiscriminants::from(err)),
            Ok(_) => panic!("validate should have failed with {expected:?}"),
        }
    }

    fn test_objects() -> (v1alpha2::AirflowCluster, ResolvedProductImage) {
        use stackable_operator::kube::api::ObjectMeta;

        let airflow = v1alpha2::AirflowCluster {
            metadata: ObjectMeta {
                name: Some("my-airflow".to_string()),
                namespace: Some("default".to_string()),
                uid: Some("e6ac237d-a6d4-43a1-8135-f36506110912".to_string()),
                ..ObjectMeta::default()
            },
            spec: serde_json::from_value(serde_json::json!({
                "image": { "productVersion": "2.10.4" },
                "clusterConfig": {
                    "credentialsSecretName": "airflow-admin-credentials",
                    "metadataDatabase": {
                        "postgresql": {
                            "host": "airflow-postgresql",
                            "database": "airflow",
                            "credentialsSecretName": "airflow-postgresql-credentials"
                        }
                    }
                },
                "kubernetesExecutors": { "config": {} },
                "webservers": { "roleGroups": { "default": { "config": {} } } },
                "schedulers": { "roleGroups": { "default": { "config": {} } } }
            }))
            .expect("test spec JSON should be valid"),
            status: None,
        };

        let image = ResolvedProductImage {
            product_version: "2.10.4".to_owned(),
            app_version_label_value: LabelValue::from_str("2.10.4-stackable0.0.0-dev")
                .expect("valid label value"),
            image: "oci.stackable.tech/sdp/airflow:2.10.4-stackable0.0.0-dev".to_string(),
            image_pull_policy: "Always".to_owned(),
            pull_secrets: None,
        };

        (airflow, image)
    }
}
