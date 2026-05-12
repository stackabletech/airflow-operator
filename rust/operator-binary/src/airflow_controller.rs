//! Ensures that `Pod`s are configured and running for each [`v1alpha2::AirflowCluster`]
//!
//! Pipeline architecture: dereference -> validate -> build -> apply -> update_status

// ---------------------------------------------------------------------------
// Imports
// ---------------------------------------------------------------------------

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    marker::PhantomData,
    str::FromStr,
    sync::Arc,
};

use const_format::concatcp;
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
    cli::OperatorEnvironmentOptions,
    cluster_resources::{ClusterResource, ClusterResourceApplyStrategy, ClusterResources},
    commons::{
        affinity::StackableAffinity,
        product_image_selection::ResolvedProductImage,
        rbac::build_rbac_resources,
        resources::{NoRuntimeLimits, Resources},
    },
    crd::{git_sync, listener},
    database_connections::drivers::{
        celery::CeleryDatabaseConnectionDetails, sqlalchemy::SqlAlchemyDatabaseConnectionDetails,
    },
    k8s_openapi::{
        DeepMerge,
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                Affinity, ConfigMap, Container as K8sContainer, ContainerPort, EnvVar,
                PersistentVolumeClaim, PodSecurityContext, PodSpec, PodTemplateSpec, Probe,
                ResourceRequirements, Service, ServiceAccount, ServicePort, ServiceSpec,
                TCPSocketAction, Volume, VolumeMount,
            },
            policy::v1::PodDisruptionBudget,
            rbac::v1::RoleBinding,
        },
        apimachinery::pkg::{
            api::resource::Quantity, apis::meta::v1::LabelSelector, util::intstr::IntOrString,
        },
    },
    kube::{
        Resource, ResourceExt,
        api::ObjectMeta,
        core::{DeserializeGuard, error_boundary},
        runtime::{controller::Action, reflector::ObjectRef},
    },
    kvp::{Annotation, Annotations, Label, Labels, ObjectLabels},
    logging::controller::ReconcilerError,
    product_config_utils::{
        env_vars_from, env_vars_from_rolegroup_config, transform_all_roles_to_config,
        validate_all_roles_and_groups_config,
    },
    product_logging::{self, framework::LoggingError, spec::Logging},
    role_utils::RoleGroupRef,
    shared::time::Duration,
    status::condition::{
        compute_conditions, operations::ClusterOperationsConditionBuilder,
        statefulset::StatefulSetConditionBuilder,
    },
    utils::COMMON_BASH_TRAP_FUNCTIONS,
};
use stackable_operator::commons::random_secret_creation;
use stackable_operator::product_logging::spec::ContainerLogConfig;
use strum::{EnumDiscriminants, IntoEnumIterator, IntoStaticStr};

use crate::{
    crd::{
        AIRFLOW_CONFIG_FILENAME, APP_NAME, AirflowClusterStatus, AirflowConfig, AirflowExecutor,
        AirflowRole, AirflowStorageConfig, CONFIG_PATH, Container, ExecutorConfig, HTTP_PORT,
        HTTP_PORT_NAME, LISTENER_VOLUME_DIR, LISTENER_VOLUME_NAME, LOG_CONFIG_DIR, METRICS_PORT,
        METRICS_PORT_NAME, OPERATOR_NAME, STACKABLE_LOG_DIR, TEMPLATE_LOCATION, TEMPLATE_NAME,
        TEMPLATE_VOLUME_NAME,
        authentication::{
            AirflowAuthenticationClassResolved, AirflowClientAuthenticationDetailsResolved,
        },
        authorization::AirflowAuthorizationResolved,
        internal_secret::{FERNET_KEY_SECRET_KEY, INTERNAL_SECRET_SECRET_KEY, JWT_SECRET_SECRET_KEY},
        v1alpha2,
    },
    controller_commons::{self, CONFIG_VOLUME_NAME, LOG_CONFIG_VOLUME_NAME, LOG_VOLUME_NAME},
    env_vars,
    framework::{
        self, HasName, HasUid, NameIsValidLabelValue,
        builder::meta::ownerreference_from_resource,
        product_logging::framework::{
            ValidatedContainerLogConfigChoice, VectorContainerLogConfig,
            validate_logging_configuration_for_container,
        },
        types::{
            kubernetes::{ConfigMapName, NamespaceName, Uid},
            operator::ClusterName,
        },
    },
    product_logging::extend_config_map_with_log_config,
    service::stateful_set_service_name,
};

// ---------------------------------------------------------------------------
// Constants and context
// ---------------------------------------------------------------------------

pub const AIRFLOW_CONTROLLER_NAME: &str = "airflowcluster";
pub const CONTAINER_IMAGE_BASE_NAME: &str = "airflow";
pub const AIRFLOW_FULL_CONTROLLER_NAME: &str =
    concatcp!(AIRFLOW_CONTROLLER_NAME, '.', OPERATOR_NAME);

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub product_config: ProductConfigManager,
    pub operator_environment: OperatorEnvironmentOptions,
}

// ---------------------------------------------------------------------------
// Validated types
// ---------------------------------------------------------------------------

pub(crate) struct Prepared;
pub(crate) struct Applied;

pub(crate) struct KubernetesResources<T> {
    pub stateful_sets: Vec<StatefulSet>,
    pub config_maps: Vec<ConfigMap>,
    pub services: Vec<Service>,
    pub service_accounts: Vec<ServiceAccount>,
    pub role_bindings: Vec<RoleBinding>,
    pub pod_disruption_budgets: Vec<PodDisruptionBudget>,
    pub listeners: Vec<listener::v1alpha1::Listener>,
    pub _status: PhantomData<T>,
}

#[derive(Clone, Debug)]
pub struct ValidatedRoleConfig {
    pub pdb_enabled: bool,
    pub pdb_max_unavailable: Option<u16>,
    pub listener_class: Option<String>,
    pub group_listener_name: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ValidatedRoleGroupConfig {
    pub resources: Resources<AirflowStorageConfig, NoRuntimeLimits>,
    pub logging: ValidatedLogging,
    pub affinity: StackableAffinity,
    pub graceful_shutdown_timeout: Duration,
    pub config_file_content: String,
}

#[derive(Clone)]
pub struct PrecomputedPodData {
    pub env_vars: Vec<EnvVar>,
    pub airflow_commands: Vec<String>,
    pub auth_volumes: Vec<Volume>,
    pub auth_volume_mounts: Vec<VolumeMount>,
    pub extra_volumes: Vec<Volume>,
    pub extra_volume_mounts: Vec<VolumeMount>,
    pub git_sync_containers: Vec<K8sContainer>,
    pub git_sync_init_containers: Vec<K8sContainer>,
    pub git_sync_volumes: Vec<Volume>,
    pub git_sync_volume_mounts: Vec<VolumeMount>,
    pub vector_container: Option<K8sContainer>,
    pub service_account_name: String,
    pub replicas: Option<u16>,
    pub pod_overrides: PodTemplateSpec,
    pub executor: AirflowExecutor,
    pub executor_template_configmap_name: Option<String>,
    pub listener_volume_claim_template: Option<PersistentVolumeClaim>,
}

#[derive(Clone, Debug)]
pub struct ValidatedLogging {
    pub airflow_container: ValidatedContainerLogConfigChoice,
    pub vector_container: Option<VectorContainerLogConfig>,
    pub git_sync_container_log_config: ContainerLogConfig,
}

impl ValidatedLogging {
    pub fn is_vector_agent_enabled(&self) -> bool {
        self.vector_container.is_some()
    }
}

// REVIEW: ValidatedAirflowCluster is the central validated type. It holds all data needed
// by the build stage so that build() can be infallible. All optional-after-merge fields
// are unwrapped during validation, and logging is pre-validated into ValidatedLogging.
#[derive(Clone)]
pub struct ValidatedAirflowCluster {
    metadata: ObjectMeta,
    pub image: ResolvedProductImage,
    pub name: ClusterName,
    pub namespace: NamespaceName,
    pub uid: Uid,
    pub role_groups: BTreeMap<AirflowRole, BTreeMap<String, ValidatedRoleGroupConfig>>,
    pub precomputed_pod_data: BTreeMap<AirflowRole, BTreeMap<String, PrecomputedPodData>>,
    pub executor_template_config_maps: Vec<ConfigMap>,
    pub role_configs: BTreeMap<AirflowRole, ValidatedRoleConfig>,
    pub executor: AirflowExecutor,
}

impl ValidatedAirflowCluster {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        image: ResolvedProductImage,
        name: ClusterName,
        namespace: NamespaceName,
        uid: Uid,
        role_groups: BTreeMap<AirflowRole, BTreeMap<String, ValidatedRoleGroupConfig>>,
        precomputed_pod_data: BTreeMap<AirflowRole, BTreeMap<String, PrecomputedPodData>>,
        executor_template_config_maps: Vec<ConfigMap>,
        role_configs: BTreeMap<AirflowRole, ValidatedRoleConfig>,
        executor: AirflowExecutor,
    ) -> Self {
        Self {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some(namespace.to_string()),
                uid: Some(uid.to_string()),
                ..ObjectMeta::default()
            },
            image,
            name,
            namespace,
            uid,
            role_groups,
            precomputed_pod_data,
            executor_template_config_maps,
            role_configs,
            executor,
        }
    }

    pub fn rolegroup_ref(&self, role: &AirflowRole, role_group: &str) -> RoleGroupRef<Self> {
        RoleGroupRef {
            cluster: ObjectRef::from_obj(self),
            role: role.to_string(),
            role_group: role_group.to_string(),
        }
    }
}

impl HasName for ValidatedAirflowCluster {
    fn to_name(&self) -> String {
        self.name.to_string()
    }
}

impl HasUid for ValidatedAirflowCluster {
    fn to_uid(&self) -> Uid {
        self.uid.clone()
    }
}

impl Resource for ValidatedAirflowCluster {
    type DynamicType =
        <v1alpha2::AirflowCluster as stackable_operator::kube::Resource>::DynamicType;
    type Scope = <v1alpha2::AirflowCluster as stackable_operator::kube::Resource>::Scope;

    fn kind(dt: &Self::DynamicType) -> std::borrow::Cow<'_, str> {
        v1alpha2::AirflowCluster::kind(dt)
    }

    fn group(dt: &Self::DynamicType) -> std::borrow::Cow<'_, str> {
        v1alpha2::AirflowCluster::group(dt)
    }

    fn version(dt: &Self::DynamicType) -> std::borrow::Cow<'_, str> {
        v1alpha2::AirflowCluster::version(dt)
    }

    fn plural(dt: &Self::DynamicType) -> std::borrow::Cow<'_, str> {
        v1alpha2::AirflowCluster::plural(dt)
    }

    fn meta(&self) -> &ObjectMeta {
        &self.metadata
    }

    fn meta_mut(&mut self) -> &mut ObjectMeta {
        &mut self.metadata
    }
}

impl NameIsValidLabelValue for ValidatedAirflowCluster {
    fn to_label_value(&self) -> String {
        self.name.to_label_value()
    }
}

// ---------------------------------------------------------------------------
// Error types and reconcile
// ---------------------------------------------------------------------------

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("AirflowCluster object is invalid"))]
    InvalidAirflowCluster {
        source: error_boundary::InvalidObject,
    },

    #[snafu(display("failed to dereference resources"))]
    Dereference { source: DereferenceError },

    #[snafu(display("failed to validate cluster"))]
    Validate { source: ValidateError },

    #[snafu(display("failed to create cluster resources"))]
    CreateClusterResources {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to apply resources"))]
    Apply { source: ApplyError },

    #[snafu(display("failed to update status"))]
    UpdateStatus { source: UpdateStatusError },
}

type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}

// REVIEW: The reconcile pipeline is structured as five sequential stages:
// 1. dereference — async, fallible: resolve external references
// 2. validate — sync, fallible: validate and merge configs
// 3. build — sync, infallible: construct Kubernetes resources
// 4. apply — async, fallible: apply resources to the cluster
// 5. update_status — async, fallible: patch status on the CRD
pub async fn reconcile(
    airflow: Arc<DeserializeGuard<v1alpha2::AirflowCluster>>,
    ctx: Arc<Ctx>,
) -> Result<Action> {
    tracing::info!("Starting reconcile");

    let airflow = airflow
        .0
        .as_ref()
        .map_err(error_boundary::InvalidObject::clone)
        .context(InvalidAirflowClusterSnafu)?;

    // --- dereference (async, fallible) ---
    let dereferenced = dereference(
        &ctx.client,
        airflow,
        CONTAINER_IMAGE_BASE_NAME,
        &ctx.operator_environment.image_repository,
        crate::built_info::PKG_VERSION,
    )
    .await
    .context(DereferenceSnafu)?;

    // --- validate (sync, fallible) ---
    let validated =
        validate_cluster(airflow, &dereferenced, &ctx.product_config).context(ValidateSnafu)?;

    // REVIEW: build() is infallible — all validation and fallible operations (config
    // generation, PodBuilder/ContainerBuilder usage, logging validation) happen in the
    // validate stage. The build stage purely assembles Kubernetes resource structs.
    // --- build (sync, infallible) ---
    let prepared = build(&validated);

    // --- apply (async, fallible) ---
    let cluster_resources = ClusterResources::new(
        APP_NAME,
        OPERATOR_NAME,
        AIRFLOW_CONTROLLER_NAME,
        &airflow.object_ref(&()),
        ClusterResourceApplyStrategy::from(&airflow.spec.cluster_operation),
        &airflow.spec.object_overrides,
    )
    .context(CreateClusterResourcesSnafu)?;

    let applied = Applier::new(&ctx.client, cluster_resources)
        .apply(prepared)
        .await
        .context(ApplySnafu)?;

    // --- update status (async, fallible) ---
    update_status(&ctx.client, airflow, applied)
        .await
        .context(UpdateStatusSnafu)?;

    Ok(Action::await_change())
}

pub fn error_policy(
    _obj: Arc<DeserializeGuard<v1alpha2::AirflowCluster>>,
    error: &Error,
    _ctx: Arc<Ctx>,
) -> Action {
    match error {
        Error::InvalidAirflowCluster { .. } => Action::await_change(),
        _ => Action::requeue(*Duration::from_secs(10)),
    }
}

// ---------------------------------------------------------------------------
// Dereference
// ---------------------------------------------------------------------------

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum DereferenceError {
    #[snafu(display("failed to resolve product image"))]
    ResolveProductImage {
        source: stackable_operator::commons::product_image_selection::Error,
    },

    #[snafu(display("failed to apply authentication configuration"))]
    InvalidAuthenticationConfig {
        source: crate::crd::authentication::Error,
    },

    #[snafu(display("invalid authorization config"))]
    InvalidAuthorizationConfig {
        source: stackable_operator::commons::opa::Error,
    },

    #[snafu(display("failed to create internal secret"))]
    InvalidInternalSecret {
        source: random_secret_creation::Error,
    },
}

pub struct DereferencedObjects {
    pub resolved_product_image: ResolvedProductImage,
    pub authentication_config: AirflowClientAuthenticationDetailsResolved,
    pub authorization_config: AirflowAuthorizationResolved,
}

pub async fn dereference(
    client: &stackable_operator::client::Client,
    airflow: &v1alpha2::AirflowCluster,
    image_base_name: &str,
    image_repository: &str,
    pkg_version: &str,
) -> std::result::Result<DereferencedObjects, DereferenceError> {
    let resolved_product_image = airflow
        .spec
        .image
        .resolve(image_base_name, image_repository, pkg_version)
        .context(deref_err::ResolveProductImageSnafu)?;

    let authentication_config = AirflowClientAuthenticationDetailsResolved::from(
        &airflow.spec.cluster_config.authentication,
        client,
    )
    .await
    .context(deref_err::InvalidAuthenticationConfigSnafu)?;

    let authorization_config = AirflowAuthorizationResolved::from_authorization_config(
        client,
        airflow,
        &airflow.spec.cluster_config.authorization,
    )
    .await
    .context(deref_err::InvalidAuthorizationConfigSnafu)?;

    random_secret_creation::create_random_secret_if_not_exists(
        &airflow.shared_internal_secret_secret_name(),
        INTERNAL_SECRET_SECRET_KEY,
        256,
        airflow,
        client,
    )
    .await
    .context(deref_err::InvalidInternalSecretSnafu)?;

    random_secret_creation::create_random_secret_if_not_exists(
        &airflow.shared_jwt_secret_secret_name(),
        JWT_SECRET_SECRET_KEY,
        256,
        airflow,
        client,
    )
    .await
    .context(deref_err::InvalidInternalSecretSnafu)?;

    random_secret_creation::create_random_secret_if_not_exists(
        &airflow.shared_fernet_key_secret_name(),
        FERNET_KEY_SECRET_KEY,
        32,
        airflow,
        client,
    )
    .await
    .context(deref_err::InvalidInternalSecretSnafu)?;

    Ok(DereferencedObjects {
        resolved_product_image,
        authentication_config,
        authorization_config,
    })
}

/// Module-like namespace for Snafu context selectors for DereferenceError,
/// avoiding name collisions with the top-level and validate error selectors.
mod deref_err {
    pub(super) use super::{
        InvalidAuthenticationConfigSnafu, InvalidAuthorizationConfigSnafu,
        InvalidInternalSecretSnafu, ResolveProductImageSnafu,
    };
}

// ---------------------------------------------------------------------------
// Validate
// ---------------------------------------------------------------------------

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum ValidateError {
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

type ValidateResult<T> = std::result::Result<T, ValidateError>;

fn validate_logging(
    logging: &Logging<Container>,
    main_container: Container,
    vector_aggregator_config_map_name: Option<&str>,
) -> ValidateResult<ValidatedLogging> {
    let airflow_container = validate_logging_configuration_for_container(logging, main_container)
        .context(validate_err::ValidateLoggingConfigSnafu)?;

    let vector_container = if logging.enable_vector_agent {
        let aggregator_name = vector_aggregator_config_map_name
            .context(validate_err::MissingVectorAggregatorConfigMapNameSnafu)?;
        ConfigMapName::from_str(aggregator_name)
            .context(validate_err::ParseVectorAggregatorConfigMapNameSnafu)?;
        let log_config = validate_logging_configuration_for_container(logging, Container::Vector)
            .context(validate_err::ValidateLoggingConfigSnafu)?;
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

fn validate_airflow_config(
    config: &AirflowConfig,
    vector_aggregator_config_map_name: Option<&str>,
    config_file_content: String,
) -> ValidateResult<ValidatedRoleGroupConfig> {
    let logging = validate_logging(
        &config.logging,
        Container::Airflow,
        vector_aggregator_config_map_name,
    )?;

    let graceful_shutdown_timeout = config
        .graceful_shutdown_timeout
        .context(validate_err::MissingGracefulShutdownTimeoutSnafu)?;

    Ok(ValidatedRoleGroupConfig {
        resources: config.resources.clone(),
        logging,
        affinity: config.affinity.clone(),
        graceful_shutdown_timeout,
        config_file_content,
    })
}

fn validate_executor_config(
    config: &ExecutorConfig,
    vector_aggregator_config_map_name: Option<&str>,
    config_file_content: String,
) -> ValidateResult<ValidatedRoleGroupConfig> {
    let logging = validate_logging(
        &config.logging,
        Container::Base,
        vector_aggregator_config_map_name,
    )?;

    let graceful_shutdown_timeout = config
        .graceful_shutdown_timeout
        .context(validate_err::MissingGracefulShutdownTimeoutSnafu)?;

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
fn generate_config_file_content(
    authentication_config: &AirflowClientAuthenticationDetailsResolved,
    authorization_config: &AirflowAuthorizationResolved,
    product_version: &str,
    rolegroup_config_overrides: &HashMap<
        PropertyNameKind,
        BTreeMap<String, String>,
    >,
) -> ValidateResult<String> {
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
    .context(validate_err::ConstructConfigSnafu)?;

    let mut file_overrides = rolegroup_config_overrides
        .get(&PropertyNameKind::File(AIRFLOW_CONFIG_FILENAME.to_string()))
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
    .context(validate_err::BuildConfigFileSnafu)?;

    if let Some(footer) = temp_file_footer {
        writeln!(config_file, "{}", footer).expect("writing to Vec<u8> is infallible");
    }

    Ok(String::from_utf8(config_file).expect("flask_app_config_writer produces valid UTF-8"))
}

/// Top-level validation: runs product_config, merges/validates per-rolegroup configs,
/// generates config file contents, and assembles a [`ValidatedAirflowCluster`].
fn validate_cluster(
    airflow: &v1alpha2::AirflowCluster,
    dereferenced: &DereferencedObjects,
    product_config_manager: &ProductConfigManager,
) -> ValidateResult<ValidatedAirflowCluster> {
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
        &role_config.context(validate_err::ProductConfigTransformSnafu)?,
        product_config_manager,
        false,
        false,
    )
    .context(validate_err::InvalidProductConfigSnafu)?;

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
            AirflowRole::from_str(role_name).context(validate_err::UnidentifiedAirflowRoleSnafu {
                role: role_name.to_string(),
            })?;

        let mut validated_groups = BTreeMap::new();
        let mut pod_data_groups = BTreeMap::new();

        for (rolegroup_name, rolegroup_config) in role_config.iter() {
            let rolegroup_ref = RoleGroupRef {
                cluster: ObjectRef::from_obj(airflow),
                role: role_name.into(),
                role_group: rolegroup_name.into(),
            };

            let merged_airflow_config = airflow
                .merged_config(&airflow_role, &rolegroup_ref)
                .context(validate_err::FailedToResolveConfigSnafu)?;

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
            .context(validate_err::FailedToResolveConfigSnafu)?;

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
    validate_and_assemble(
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
fn validate_and_assemble(
    airflow: &v1alpha2::AirflowCluster,
    resolved_product_image: &ResolvedProductImage,
    validated_role_configs: BTreeMap<AirflowRole, BTreeMap<String, ValidatedRoleGroupConfig>>,
    precomputed_pod_data: BTreeMap<AirflowRole, BTreeMap<String, PrecomputedPodData>>,
    executor_template_config_maps: Vec<ConfigMap>,
    role_configs: BTreeMap<AirflowRole, ValidatedRoleConfig>,
) -> ValidateResult<ValidatedAirflowCluster> {
    let cluster_name =
        ClusterName::from_str(&airflow.name_any()).context(validate_err::InvalidClusterNameSnafu)?;
    let namespace = NamespaceName::from_str(
        &airflow
            .namespace()
            .context(validate_err::ObjectHasNoNamespaceSnafu)?,
    )
    .context(validate_err::InvalidClusterNamespaceSnafu)?;
    let uid = Uid::from_str(
        airflow
            .meta()
            .uid
            .as_deref()
            .context(validate_err::ObjectHasNoUidSnafu)?,
    )
    .context(validate_err::InvalidClusterUidSnafu)?;

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
) -> ValidateResult<(Vec<Volume>, Vec<VolumeMount>)> {
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
            .context(validate_err::AddLdapVolumesAndVolumeMountsSnafu)?;
    }
    for tls in tls_credentials {
        tls.add_volumes_and_mounts(&mut pb, vec![&mut cb])
            .context(validate_err::AddTlsVolumesAndVolumeMountsSnafu)?;
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
    validated_config: &ValidatedRoleGroupConfig,
    common_configuration: &crate::crd::AirflowExecutorCommonConfiguration,
) -> ValidateResult<Vec<ConfigMap>> {
    let executor_rolegroup_ref = RoleGroupRef {
        cluster: ObjectRef::from_obj(airflow),
        role: "executor".into(),
        role_group: "kubernetes".into(),
    };

    // 1. Build the executor logging/config ConfigMap
    let executor_config_cm = {
        let metadata = ObjectMetaBuilder::new()
            .name(executor_rolegroup_ref.object_name())
            .namespace_opt(airflow.namespace())
            .ownerreference_from_resource(airflow, None, Some(true))
            .context(validate_err::ObjectMetaSnafu)?
            .with_recommended_labels(&build_object_labels(
                airflow,
                resolved_product_image,
                "executor",
                "executor-template",
            ))
            .context(validate_err::ObjectMetaSnafu)?
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

        cm_builder
            .build()
            .context(validate_err::PodTemplateConfigMapSnafu)?
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
        .context(validate_err::InvalidGitSyncSpecSnafu)?;

        let mut pb = PodBuilder::new();
        let pb_metadata = ObjectMetaBuilder::new()
            .with_recommended_labels(&build_object_labels(
                airflow,
                resolved_product_image,
                "executor",
                "executor-template",
            ))
            .context(validate_err::ObjectMetaSnafu)?
            .build();

        pb.metadata(pb_metadata)
            .image_pull_secrets_from_product_image(resolved_product_image)
            .affinity(&validated_config.affinity)
            .service_account_name(service_account_name)
            .restart_policy("Never")
            .security_context(PodSecurityContextBuilder::new().fs_group(1000).build());

        pb.termination_grace_period(&validated_config.graceful_shutdown_timeout)
            .context(validate_err::GracefulShutdownSnafu)?;

        // Container name "base" is an Airflow requirement
        let mut airflow_container = ContainerBuilder::new(&Container::Base.to_string())
            .context(validate_err::InvalidContainerNameSnafu)?;

        // Auth volumes and mounts
        add_authentication_volumes_and_volume_mounts_to_builders(
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
            .context(validate_err::AddVolumeMountSnafu)?
            .add_volume_mount(CONFIG_VOLUME_NAME, CONFIG_PATH)
            .context(validate_err::AddVolumeMountSnafu)?
            .add_volume_mount(LOG_CONFIG_VOLUME_NAME, LOG_CONFIG_DIR)
            .context(validate_err::AddVolumeMountSnafu)?
            .add_volume_mount(LOG_VOLUME_NAME, STACKABLE_LOG_DIR)
            .context(validate_err::AddVolumeMountSnafu)?;

        // Git-sync resources (init containers only, no sidecars for executor template)
        for container in git_sync_resources.git_sync_init_containers.iter().cloned() {
            pb.add_init_container(container);
        }
        pb.add_volumes(git_sync_resources.git_content_volumes.clone())
            .context(validate_err::AddVolumeSnafu)?;
        pb.add_volumes(git_sync_resources.git_ssh_volumes.clone())
            .context(validate_err::AddVolumeSnafu)?;
        pb.add_volumes(git_sync_resources.git_ca_cert_volumes.clone())
            .context(validate_err::AddVolumeSnafu)?;
        airflow_container
            .add_volume_mounts(git_sync_resources.git_content_volume_mounts.clone())
            .context(validate_err::AddVolumeMountSnafu)?;

        // Database connection env vars
        metadata_database_connection_details.add_to_container(&mut airflow_container);

        pb.add_container(airflow_container.build());
        pb.add_volumes(airflow.volumes().clone())
            .context(validate_err::AddVolumeSnafu)?;
        // REVIEW: controller_commons::create_volumes now takes &ValidatedContainerLogConfigChoice
        // instead of Option<&ContainerLogConfig>. The validated type ensures logging config
        // has already been checked, so the build stage can use it directly.
        pb.add_volumes(controller_commons::create_volumes(
            &executor_rolegroup_ref.object_name(),
            &validated_config.logging.airflow_container,
        ))
        .context(validate_err::AddVolumeSnafu)?;

        if let Some(vector_config) = &validated_config.logging.vector_container {
            let vector_aggregator_config_map_name = airflow
                .spec
                .cluster_config
                .vector_aggregator_config_map_name
                .as_deref()
                .context(validate_err::MissingVectorAggregatorConfigMapNameSnafu)?;
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
                    .context(validate_err::ObjectMetaSnafu)?
                    .with_recommended_labels(&build_object_labels(
                        airflow,
                        resolved_product_image,
                        "executor",
                        "executor-template",
                    ))
                    .context(validate_err::ObjectMetaSnafu)?
                    .with_label(restarter_label)
                    .build(),
            )
            .add_data(
                TEMPLATE_NAME,
                serde_yaml::to_string(&pod_template)
                    .context(validate_err::PodTemplateSerdeSnafu)?,
            );

        cm_builder
            .build()
            .context(validate_err::PodTemplateConfigMapSnafu)?
    };

    Ok(vec![executor_config_cm, executor_template_cm])
}

/// Helper to add authentication volumes and volume mounts directly to builders.
/// Used by the executor template where we build a PodTemplate using PodBuilder/ContainerBuilder.
fn add_authentication_volumes_and_volume_mounts_to_builders(
    authentication_config: &AirflowClientAuthenticationDetailsResolved,
    cb: &mut ContainerBuilder,
    pb: &mut PodBuilder,
) -> ValidateResult<()> {
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
            .context(validate_err::AddLdapVolumesAndVolumeMountsSnafu)?;
    }
    for tls in tls_credentials {
        tls.add_volumes_and_mounts(pb, vec![cb])
            .context(validate_err::AddTlsVolumesAndVolumeMountsSnafu)?;
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
) -> ValidateResult<K8sContainer> {
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
    .context(validate_err::ConfigureLoggingSnafu)
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
) -> ValidateResult<PrecomputedPodData> {
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
    .context(validate_err::InvalidGitSyncSpecSnafu)?;

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
    .context(validate_err::BuildEnvVarsSnafu)?;

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
            .context(validate_err::MissingVectorAggregatorConfigMapNameSnafu)?;
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
    let role = binding.as_ref().context(validate_err::NoAirflowRoleSnafu)?;
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
            .context(validate_err::BuildLabelsSnafu)?;

            let pvc = ListenerOperatorVolumeSourceBuilder::new(
                &ListenerReference::ListenerName(listener_group_name),
                &unversioned_labels,
            )
            .build_pvc(LISTENER_VOLUME_NAME.to_string())
            .context(validate_err::BuildListenerVolumeSnafu)?;
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

/// Module-like namespace for Snafu context selectors for ValidateError,
/// avoiding name collisions with the top-level error selectors.
mod validate_err {
    pub(super) use super::{
        AddLdapVolumesAndVolumeMountsSnafu, AddTlsVolumesAndVolumeMountsSnafu, AddVolumeMountSnafu,
        AddVolumeSnafu, BuildConfigFileSnafu, BuildEnvVarsSnafu, BuildLabelsSnafu,
        BuildListenerVolumeSnafu, ConfigureLoggingSnafu, ConstructConfigSnafu,
        FailedToResolveConfigSnafu, GracefulShutdownSnafu, InvalidClusterNameSnafu,
        InvalidClusterNamespaceSnafu, InvalidClusterUidSnafu, InvalidContainerNameSnafu,
        InvalidGitSyncSpecSnafu, InvalidProductConfigSnafu,
        MissingGracefulShutdownTimeoutSnafu, MissingVectorAggregatorConfigMapNameSnafu,
        NoAirflowRoleSnafu, ObjectHasNoNamespaceSnafu, ObjectHasNoUidSnafu, ObjectMetaSnafu,
        ParseVectorAggregatorConfigMapNameSnafu, PodTemplateConfigMapSnafu,
        PodTemplateSerdeSnafu, ProductConfigTransformSnafu, UnidentifiedAirflowRoleSnafu,
        ValidateLoggingConfigSnafu,
    };
}

// ---------------------------------------------------------------------------
// Build (including RoleGroupBuilder)
// ---------------------------------------------------------------------------

fn main_container_for_role(_role: &AirflowRole) -> Container {
    Container::Airflow
}

// REVIEW: build() is infallible. All validation and fallible operations (config generation,
// PodBuilder/ContainerBuilder usage, logging validation) are performed in the validate
// stage. The build stage purely assembles Kubernetes resource structs from validated data.
fn build(validated: &ValidatedAirflowCluster) -> KubernetesResources<Prepared> {
    let mut stateful_sets = Vec::new();
    let mut config_maps = Vec::new();
    let mut services = Vec::new();
    let mut pod_disruption_budgets = Vec::new();
    let mut listeners = Vec::new();

    // --- RBAC ---
    let rbac_labels = build_recommended_labels(validated, "rbac", "rbac");

    let (rbac_sa, rbac_rolebinding) = build_rbac_resources(validated, APP_NAME, rbac_labels)
        .expect(
            "RBAC resources should be created because the validated cluster has valid metadata",
        );

    // --- Executor template ConfigMaps (pre-built in validate stage) ---
    config_maps.extend(validated.executor_template_config_maps.clone());

    // --- Per-role/rolegroup resources ---
    for (airflow_role, role_groups) in &validated.role_groups {
        // PDBs
        if let Some(role_config) = validated.role_configs.get(airflow_role) {
            if let Some(pdb) = build_pdb(validated, airflow_role, role_config) {
                pod_disruption_budgets.push(pdb);
            }
        }

        // Group listeners (only Webserver)
        if let Some(role_config) = validated.role_configs.get(airflow_role) {
            if let (Some(listener_class), Some(listener_name)) = (
                &role_config.listener_class,
                &role_config.group_listener_name,
            ) {
                listeners.push(build_group_listener(
                    validated,
                    airflow_role,
                    listener_class.clone(),
                    listener_name.clone(),
                ));
            }
        }

        for (rolegroup_name, role_group_config) in role_groups {
            let rolegroup_ref = validated.rolegroup_ref(airflow_role, rolegroup_name);

            let main_container = main_container_for_role(airflow_role);

            // Services
            services.push(build_headless_service(validated, &rolegroup_ref));
            services.push(build_metrics_service(validated, &rolegroup_ref));

            // ConfigMap + StatefulSet via RoleGroupBuilder
            let pod_data = validated
                .precomputed_pod_data
                .get(airflow_role)
                .and_then(|groups| groups.get(rolegroup_name))
                .expect(
                    "PrecomputedPodData should exist for every role group \
                    because validate_cluster computes it for each one",
                );

            let builder = RoleGroupBuilder::new(
                validated,
                role_group_config,
                rolegroup_ref,
                airflow_role.clone(),
                main_container,
                pod_data,
            );

            config_maps.push(builder.build_config_map());
            stateful_sets.push(builder.build_stateful_set());
        }
    }

    KubernetesResources {
        stateful_sets,
        config_maps,
        services,
        service_accounts: vec![rbac_sa],
        role_bindings: vec![rbac_rolebinding],
        pod_disruption_budgets,
        listeners,
        _status: PhantomData,
    }
}

fn build_pdb(
    cluster: &ValidatedAirflowCluster,
    role: &AirflowRole,
    role_config: &ValidatedRoleConfig,
) -> Option<PodDisruptionBudget> {
    if !role_config.pdb_enabled {
        return None;
    }

    let max_unavailable = role_config.pdb_max_unavailable.unwrap_or(match role {
        AirflowRole::Worker => match &cluster.executor {
            AirflowExecutor::KubernetesExecutors { .. } => return None,
            _ => 1,
        },
        _ => 1,
    });

    // REVIEW: from_str_unsafe is used here because the values come from constants (APP_NAME,
    // OPERATOR_NAME, AIRFLOW_CONTROLLER_NAME) or validated role names — they are known to be
    // valid at compile time or have been validated during the validate stage.
    Some({
        use crate::framework::types::operator::*;
        framework::builder::pdb::pod_disruption_budget_builder_with_role(
            cluster,
            &ProductName::from_str_unsafe(APP_NAME),
            &RoleName::from_str_unsafe(&role.to_string()),
            &OperatorName::from_str_unsafe(OPERATOR_NAME),
            &ControllerName::from_str_unsafe(AIRFLOW_CONTROLLER_NAME),
        )
        .with_max_unavailable(max_unavailable)
        .build()
    })
}

fn build_headless_service(
    cluster: &ValidatedAirflowCluster,
    rolegroup_ref: &RoleGroupRef<ValidatedAirflowCluster>,
) -> Service {
    let metadata = ObjectMetaBuilder::new()
        .name(format!("{}-headless", rolegroup_ref.object_name()))
        .namespace(&cluster.namespace)
        .ownerreference(ownerreference_from_resource(cluster, None, Some(true)))
        .with_labels(build_recommended_labels(
            cluster,
            &rolegroup_ref.role,
            &rolegroup_ref.role_group,
        ))
        .build();

    Service {
        metadata,
        spec: Some(ServiceSpec {
            type_: Some("ClusterIP".to_string()),
            cluster_ip: Some("None".to_string()),
            ports: Some(vec![ServicePort {
                name: Some(HTTP_PORT_NAME.to_string()),
                port: HTTP_PORT.into(),
                protocol: Some("TCP".to_string()),
                ..ServicePort::default()
            }]),
            selector: Some(
                build_role_group_selector_labels(
                    cluster,
                    &rolegroup_ref.role,
                    &rolegroup_ref.role_group,
                )
                .into(),
            ),
            publish_not_ready_addresses: Some(true),
            ..ServiceSpec::default()
        }),
        status: None,
    }
}

fn build_metrics_service(
    cluster: &ValidatedAirflowCluster,
    rolegroup_ref: &RoleGroupRef<ValidatedAirflowCluster>,
) -> Service {
    let metadata = ObjectMetaBuilder::new()
        .name(format!("{}-metrics", rolegroup_ref.object_name()))
        .namespace(&cluster.namespace)
        .ownerreference(ownerreference_from_resource(cluster, None, Some(true)))
        .with_labels(build_recommended_labels(
            cluster,
            &rolegroup_ref.role,
            &rolegroup_ref.role_group,
        ))
        .with_labels(prometheus_labels())
        .with_annotations(prometheus_annotations())
        .build();

    Service {
        metadata,
        spec: Some(ServiceSpec {
            type_: Some("ClusterIP".to_string()),
            cluster_ip: Some("None".to_string()),
            ports: Some(vec![ServicePort {
                name: Some(METRICS_PORT_NAME.to_string()),
                port: METRICS_PORT.into(),
                protocol: Some("TCP".to_string()),
                ..ServicePort::default()
            }]),
            selector: Some(
                build_role_group_selector_labels(
                    cluster,
                    &rolegroup_ref.role,
                    &rolegroup_ref.role_group,
                )
                .into(),
            ),
            publish_not_ready_addresses: Some(true),
            ..ServiceSpec::default()
        }),
        status: None,
    }
}

// REVIEW: from_str_unsafe is used for label construction throughout these helpers because
// the inputs are either compile-time constants (APP_NAME, OPERATOR_NAME, etc.) or values
// that have already been validated during the validate stage (role names, role group names,
// product versions). Using from_str_unsafe avoids redundant re-validation in the infallible
// build stage.
fn build_recommended_labels(
    cluster: &ValidatedAirflowCluster,
    role: &str,
    role_group: &str,
) -> Labels {
    use crate::framework::types::operator::*;
    framework::kvp::label::recommended_labels(
        cluster,
        &ProductName::from_str_unsafe(APP_NAME),
        &ProductVersion::from_str_unsafe(&cluster.image.app_version_label_value.to_string()),
        &OperatorName::from_str_unsafe(OPERATOR_NAME),
        &ControllerName::from_str_unsafe(AIRFLOW_CONTROLLER_NAME),
        &RoleName::from_str_unsafe(role),
        &RoleGroupName::from_str_unsafe(role_group),
    )
}

fn build_role_group_selector_labels(
    cluster: &ValidatedAirflowCluster,
    role: &str,
    role_group: &str,
) -> Labels {
    use crate::framework::types::operator::*;
    framework::kvp::label::role_group_selector(
        cluster,
        &ProductName::from_str_unsafe(APP_NAME),
        &RoleName::from_str_unsafe(role),
        &RoleGroupName::from_str_unsafe(role_group),
    )
}

fn prometheus_labels() -> Labels {
    Labels::try_from([("prometheus.io/scrape", "true")]).expect("should be a valid label")
}

fn prometheus_annotations() -> Annotations {
    Annotations::try_from([
        ("prometheus.io/path".to_owned(), "/metrics".to_owned()),
        ("prometheus.io/port".to_owned(), METRICS_PORT.to_string()),
        ("prometheus.io/scheme".to_owned(), "http".to_owned()),
        ("prometheus.io/scrape".to_owned(), "true".to_owned()),
    ])
    .expect("should be valid annotations")
}

fn build_group_listener(
    cluster: &ValidatedAirflowCluster,
    role: &AirflowRole,
    listener_class: String,
    listener_group_name: String,
) -> listener::v1alpha1::Listener {
    let metadata = ObjectMetaBuilder::new()
        .name(&listener_group_name)
        .namespace(&cluster.namespace)
        .ownerreference(ownerreference_from_resource(cluster, None, Some(true)))
        .with_labels(build_recommended_labels(cluster, &role.to_string(), "none"))
        .build();

    listener::v1alpha1::Listener {
        metadata,
        spec: listener::v1alpha1::ListenerSpec {
            class_name: Some(listener_class),
            ports: Some(listener_ports()),
            ..listener::v1alpha1::ListenerSpec::default()
        },
        status: None,
    }
}

fn listener_ports() -> Vec<listener::v1alpha1::ListenerPort> {
    vec![listener::v1alpha1::ListenerPort {
        name: HTTP_PORT_NAME.to_string(),
        port: HTTP_PORT.into(),
        protocol: Some("TCP".to_string()),
    }]
}

// ---------------------------------------------------------------------------
// RoleGroupBuilder
// ---------------------------------------------------------------------------

struct RoleGroupBuilder<'a> {
    cluster: &'a ValidatedAirflowCluster,
    role_group_config: &'a ValidatedRoleGroupConfig,
    rolegroup_ref: RoleGroupRef<ValidatedAirflowCluster>,
    airflow_role: AirflowRole,
    main_container: Container,
    pod_data: &'a PrecomputedPodData,
}

impl<'a> RoleGroupBuilder<'a> {
    fn new(
        cluster: &'a ValidatedAirflowCluster,
        role_group_config: &'a ValidatedRoleGroupConfig,
        rolegroup_ref: RoleGroupRef<ValidatedAirflowCluster>,
        airflow_role: AirflowRole,
        main_container: Container,
        pod_data: &'a PrecomputedPodData,
    ) -> Self {
        Self {
            cluster,
            role_group_config,
            rolegroup_ref,
            airflow_role,
            main_container,
            pod_data,
        }
    }

    fn build_config_map(&self) -> ConfigMap {
        let metadata = self
            .common_metadata(self.rolegroup_ref.object_name())
            .build();

        let mut cm_builder = ConfigMapBuilder::new();
        cm_builder.metadata(metadata);

        cm_builder.add_data(
            AIRFLOW_CONFIG_FILENAME,
            self.role_group_config.config_file_content.clone(),
        );

        extend_config_map_with_log_config(
            &self.rolegroup_ref,
            &self.main_container,
            &self.role_group_config.logging.airflow_container,
            self.role_group_config.logging.vector_container.as_ref(),
            &mut cm_builder,
            &self.cluster.image,
        );

        cm_builder
            .build()
            .expect("ConfigMap should build because metadata is set")
    }

    fn build_stateful_set(&self) -> StatefulSet {
        let restarter_label = Label::try_from(("restarter.stackable.tech/enabled", "true"))
            .expect("static label is always valid");

        let metadata = self
            .common_metadata(self.rolegroup_ref.object_name())
            .with_label(restarter_label)
            .build();

        let template = self.build_pod_template();

        let match_labels = {
            use crate::framework::types::operator::*;
            framework::kvp::label::role_group_selector(
                self.cluster,
                &ProductName::from_str_unsafe(APP_NAME),
                &RoleName::from_str_unsafe(&self.rolegroup_ref.role),
                &RoleGroupName::from_str_unsafe(&self.rolegroup_ref.role_group),
            )
        };

        let pod_management_policy = match self.airflow_role {
            AirflowRole::Scheduler => "OrderedReady",
            AirflowRole::Webserver
            | AirflowRole::Worker
            | AirflowRole::DagProcessor
            | AirflowRole::Triggerer => "Parallel",
        }
        .to_string();

        let spec = StatefulSetSpec {
            pod_management_policy: Some(pod_management_policy),
            replicas: self.pod_data.replicas.map(i32::from),
            selector: LabelSelector {
                match_labels: Some(match_labels.into()),
                ..LabelSelector::default()
            },
            service_name: stateful_set_service_name(&self.rolegroup_ref),
            template,
            volume_claim_templates: self
                .pod_data
                .listener_volume_claim_template
                .clone()
                .map(|pvc| vec![pvc]),
            ..StatefulSetSpec::default()
        };

        StatefulSet {
            metadata,
            spec: Some(spec),
            status: None,
        }
    }

    fn build_pod_template(&self) -> PodTemplateSpec {
        let pod_metadata = ObjectMetaBuilder::new()
            .with_labels(self.recommended_labels())
            .with_annotation(
                Annotation::try_from((
                    "kubectl.kubernetes.io/default-container",
                    format!("{}", self.main_container),
                ))
                .expect("static annotation is always valid"),
            )
            .build();

        let airflow_container = self.build_airflow_container();
        let metrics_container = self.build_metrics_container();

        let mut containers = vec![airflow_container, metrics_container];
        containers.extend(self.pod_data.git_sync_containers.clone());
        if let Some(vector_container) = &self.pod_data.vector_container {
            containers.push(vector_container.clone());
        }

        let init_containers = if self.pod_data.git_sync_init_containers.is_empty() {
            None
        } else {
            Some(self.pod_data.git_sync_init_containers.clone())
        };

        let volumes = self.build_volumes();

        let termination_grace_period_seconds = self
            .role_group_config
            .graceful_shutdown_timeout
            .as_secs()
            .try_into()
            .ok();

        let mut pod_template = PodTemplateSpec {
            metadata: Some(pod_metadata),
            spec: Some(PodSpec {
                affinity: {
                    let a = &self.role_group_config.affinity;
                    if a.pod_affinity.is_some()
                        || a.pod_anti_affinity.is_some()
                        || a.node_affinity.is_some()
                    {
                        Some(Affinity {
                            pod_affinity: a.pod_affinity.clone(),
                            pod_anti_affinity: a.pod_anti_affinity.clone(),
                            node_affinity: a.node_affinity.clone(),
                        })
                    } else {
                        None
                    }
                },
                containers,
                init_containers,
                service_account_name: Some(self.pod_data.service_account_name.clone()),
                termination_grace_period_seconds,
                security_context: Some(PodSecurityContext {
                    fs_group: Some(1000),
                    ..PodSecurityContext::default()
                }),
                image_pull_secrets: self.cluster.image.pull_secrets.clone(),
                volumes: if volumes.is_empty() {
                    None
                } else {
                    Some(volumes)
                },
                ..PodSpec::default()
            }),
        };

        pod_template.merge_from(self.pod_data.pod_overrides.clone());
        pod_template
    }

    fn build_airflow_container(&self) -> K8sContainer {
        let mut volume_mounts = vec![
            VolumeMount {
                name: CONFIG_VOLUME_NAME.to_string(),
                mount_path: CONFIG_PATH.to_string(),
                ..VolumeMount::default()
            },
            VolumeMount {
                name: LOG_CONFIG_VOLUME_NAME.to_string(),
                mount_path: LOG_CONFIG_DIR.to_string(),
                ..VolumeMount::default()
            },
            VolumeMount {
                name: LOG_VOLUME_NAME.to_string(),
                mount_path: STACKABLE_LOG_DIR.to_string(),
                ..VolumeMount::default()
            },
        ];

        volume_mounts.extend(self.pod_data.extra_volume_mounts.clone());
        volume_mounts.extend(self.pod_data.auth_volume_mounts.clone());
        volume_mounts.extend(self.pod_data.git_sync_volume_mounts.clone());

        if matches!(
            self.pod_data.executor,
            AirflowExecutor::KubernetesExecutors { .. }
        ) {
            volume_mounts.push(VolumeMount {
                name: TEMPLATE_VOLUME_NAME.to_string(),
                mount_path: TEMPLATE_LOCATION.to_string(),
                ..VolumeMount::default()
            });
        }

        if self.airflow_role.get_http_port().is_some()
            && self.pod_data.listener_volume_claim_template.is_some()
        {
            volume_mounts.push(VolumeMount {
                name: LISTENER_VOLUME_NAME.to_string(),
                mount_path: LISTENER_VOLUME_DIR.to_string(),
                ..VolumeMount::default()
            });
        }

        let mut ports = Vec::new();
        if let Some(http_port) = self.airflow_role.get_http_port() {
            ports.push(ContainerPort {
                name: Some(HTTP_PORT_NAME.to_string()),
                container_port: http_port.into(),
                ..ContainerPort::default()
            });
        }

        let (readiness_probe, liveness_probe) =
            if let Some(http_port) = self.airflow_role.get_http_port() {
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
                (Some(probe.clone()), Some(probe))
            } else {
                (None, None)
            };

        K8sContainer {
            name: self.main_container.to_string(),
            image: Some(self.cluster.image.image.clone()),
            image_pull_policy: Some(self.cluster.image.image_pull_policy.clone()),
            command: Some(vec![
                "/bin/bash".to_string(),
                "-x".to_string(),
                "-euo".to_string(),
                "pipefail".to_string(),
                "-c".to_string(),
            ]),
            args: Some(vec![self.pod_data.airflow_commands.join("\n")]),
            env: Some(self.pod_data.env_vars.clone()),
            ports: if ports.is_empty() { None } else { Some(ports) },
            volume_mounts: Some(volume_mounts),
            resources: Some(self.role_group_config.resources.clone().into()),
            readiness_probe,
            liveness_probe,
            ..K8sContainer::default()
        }
    }

    fn build_metrics_container(&self) -> K8sContainer {
        let args = [
            COMMON_BASH_TRAP_FUNCTIONS.to_string(),
            "prepare_signal_handlers".to_string(),
            "/stackable/statsd_exporter &".to_string(),
            "wait_for_termination $!".to_string(),
        ]
        .join("\n");

        K8sContainer {
            name: "metrics".to_string(),
            image: Some(self.cluster.image.image.clone()),
            image_pull_policy: Some(self.cluster.image.image_pull_policy.clone()),
            command: Some(vec![
                "/bin/bash".to_string(),
                "-x".to_string(),
                "-euo".to_string(),
                "pipefail".to_string(),
                "-c".to_string(),
            ]),
            args: Some(vec![args]),
            ports: Some(vec![ContainerPort {
                name: Some(METRICS_PORT_NAME.to_string()),
                container_port: METRICS_PORT.into(),
                ..ContainerPort::default()
            }]),
            resources: Some(ResourceRequirements {
                requests: Some(BTreeMap::from([
                    ("cpu".to_string(), Quantity("100m".to_string())),
                    ("memory".to_string(), Quantity("64Mi".to_string())),
                ])),
                limits: Some(BTreeMap::from([
                    ("cpu".to_string(), Quantity("200m".to_string())),
                    ("memory".to_string(), Quantity("64Mi".to_string())),
                ])),
                ..ResourceRequirements::default()
            }),
            ..K8sContainer::default()
        }
    }

    fn build_volumes(&self) -> Vec<Volume> {
        // REVIEW: controller_commons::create_volumes is called with the new validated type
        // (&ValidatedContainerLogConfigChoice) instead of the old Option<&ContainerLogConfig>.
        // This is safe because the logging config has already been validated during the
        // validate stage.
        let mut volumes = controller_commons::create_volumes(
            &self.rolegroup_ref.object_name(),
            &self.role_group_config.logging.airflow_container,
        );

        volumes.extend(self.pod_data.extra_volumes.clone());
        volumes.extend(self.pod_data.auth_volumes.clone());
        volumes.extend(self.pod_data.git_sync_volumes.clone());

        if let Some(template_cm_name) = &self.pod_data.executor_template_configmap_name {
            volumes.push(Volume {
                name: TEMPLATE_VOLUME_NAME.to_string(),
                config_map: Some(
                    stackable_operator::k8s_openapi::api::core::v1::ConfigMapVolumeSource {
                        name: template_cm_name.clone(),
                        ..Default::default()
                    },
                ),
                ..Volume::default()
            });
        }

        volumes
    }

    fn common_metadata(&self, resource_name: impl Into<String>) -> ObjectMetaBuilder {
        let mut builder = ObjectMetaBuilder::new();

        builder
            .name(resource_name)
            .namespace(&self.cluster.namespace)
            .ownerreference(ownerreference_from_resource(self.cluster, None, Some(true)))
            .with_labels(self.recommended_labels());

        builder
    }

    fn recommended_labels(&self) -> Labels {
        use crate::framework::types::operator::*;
        framework::kvp::label::recommended_labels(
            self.cluster,
            &ProductName::from_str_unsafe(APP_NAME),
            &ProductVersion::from_str_unsafe(
                &self.cluster.image.app_version_label_value.to_string(),
            ),
            &OperatorName::from_str_unsafe(OPERATOR_NAME),
            &ControllerName::from_str_unsafe(AIRFLOW_CONTROLLER_NAME),
            &RoleName::from_str_unsafe(&self.rolegroup_ref.role),
            &RoleGroupName::from_str_unsafe(&self.rolegroup_ref.role_group),
        )
    }
}

// ---------------------------------------------------------------------------
// Apply
// ---------------------------------------------------------------------------

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum ApplyError {
    #[snafu(display("failed to apply resource"))]
    ApplyResource {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to delete orphaned resources"))]
    DeleteOrphanedResources {
        source: stackable_operator::cluster_resources::Error,
    },
}

struct Applier<'a> {
    client: &'a stackable_operator::client::Client,
    cluster_resources: ClusterResources<'a>,
}

impl<'a> Applier<'a> {
    fn new(
        client: &'a stackable_operator::client::Client,
        cluster_resources: ClusterResources<'a>,
    ) -> Self {
        Applier {
            client,
            cluster_resources,
        }
    }

    async fn apply(
        mut self,
        resources: KubernetesResources<Prepared>,
    ) -> std::result::Result<KubernetesResources<Applied>, ApplyError> {
        let config_maps = self.add_resources(resources.config_maps).await?;
        let service_accounts = self.add_resources(resources.service_accounts).await?;
        let services = self.add_resources(resources.services).await?;
        let role_bindings = self.add_resources(resources.role_bindings).await?;
        let listeners = self.add_resources(resources.listeners).await?;
        let stateful_sets = self.add_resources(resources.stateful_sets).await?;
        let pod_disruption_budgets = self.add_resources(resources.pod_disruption_budgets).await?;

        self.cluster_resources
            .delete_orphaned_resources(self.client)
            .await
            .context(apply_err::DeleteOrphanedResourcesSnafu)?;

        Ok(KubernetesResources {
            stateful_sets,
            config_maps,
            services,
            service_accounts,
            role_bindings,
            pod_disruption_budgets,
            listeners,
            _status: PhantomData,
        })
    }

    async fn add_resources<T: ClusterResource + Sync>(
        &mut self,
        resources: Vec<T>,
    ) -> std::result::Result<Vec<T>, ApplyError> {
        let mut applied = vec![];
        for resource in resources {
            let applied_resource = self
                .cluster_resources
                .add(self.client, resource)
                .await
                .context(apply_err::ApplyResourceSnafu)?;
            applied.push(applied_resource);
        }
        Ok(applied)
    }
}

/// Module-like namespace for Snafu context selectors for ApplyError.
mod apply_err {
    pub(super) use super::{ApplyResourceSnafu, DeleteOrphanedResourcesSnafu};
}

// ---------------------------------------------------------------------------
// Update status
// ---------------------------------------------------------------------------

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum UpdateStatusError {
    #[snafu(display("failed to update status"))]
    PatchStatus {
        source: stackable_operator::client::Error,
    },
}

async fn update_status(
    client: &stackable_operator::client::Client,
    airflow: &v1alpha2::AirflowCluster,
    applied_resources: KubernetesResources<Applied>,
) -> std::result::Result<(), UpdateStatusError> {
    let mut ss_cond_builder = StatefulSetConditionBuilder::default();
    for stateful_set in applied_resources.stateful_sets {
        ss_cond_builder.add(stateful_set);
    }

    let cluster_operation_cond_builder =
        ClusterOperationsConditionBuilder::new(&airflow.spec.cluster_operation);

    let status = AirflowClusterStatus {
        conditions: compute_conditions(
            airflow,
            &[&ss_cond_builder, &cluster_operation_cond_builder],
        ),
    };

    client
        .apply_patch_status(OPERATOR_NAME, airflow, &status)
        .await
        .context(PatchStatusSnafu)?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, str::FromStr};

    use stackable_operator::{
        commons::{
            affinity::StackableAffinity,
            product_image_selection::ResolvedProductImage,
            resources::{NoRuntimeLimits, Resources},
        },
        k8s_openapi::api::core::v1::PodTemplateSpec,
        kube::Resource,
        kvp::LabelValue,
        product_logging::spec::{
            AutomaticContainerLogConfig, ContainerLogConfig, ContainerLogConfigChoice, Logging,
        },
        shared::time::Duration,
    };

    use super::*;
    use crate::crd::{AirflowConfig, AirflowStorageConfig, Container};

    // --- validate tests ---

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
        let result = validate_and_assemble(
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
            ValidateErrorDiscriminants::InvalidClusterName,
        );
    }

    #[test]
    fn test_validate_err_missing_namespace() {
        test_validate_err(
            |airflow, _| airflow.metadata.namespace = None,
            ValidateErrorDiscriminants::ObjectHasNoNamespace,
        );
    }

    #[test]
    fn test_validate_err_missing_uid() {
        test_validate_err(
            |airflow, _| airflow.metadata.uid = None,
            ValidateErrorDiscriminants::ObjectHasNoUid,
        );
    }

    #[test]
    fn test_validate_err_invalid_cluster_name() {
        test_validate_err(
            |airflow, _| {
                airflow.metadata.name =
                    Some("THIS-IS-NOT-A-VALID-DNS-LABEL-NAME-BECAUSE-UPPERCASE".to_string())
            },
            ValidateErrorDiscriminants::InvalidClusterName,
        );
    }

    #[test]
    fn test_validate_err_invalid_namespace() {
        test_validate_err(
            |airflow, _| airflow.metadata.namespace = Some("INVALID NAMESPACE".to_string()),
            ValidateErrorDiscriminants::InvalidClusterNamespace,
        );
    }

    #[test]
    fn test_validate_err_invalid_uid() {
        test_validate_err(
            |airflow, _| airflow.metadata.uid = Some("not-a-uuid".to_string()),
            ValidateErrorDiscriminants::InvalidClusterUid,
        );
    }

    fn test_validate_err(
        mutate: fn(&mut v1alpha2::AirflowCluster, &mut ResolvedProductImage),
        expected: ValidateErrorDiscriminants,
    ) {
        let (mut airflow, mut image) = test_objects();
        mutate(&mut airflow, &mut image);
        let result = validate_and_assemble(
            &airflow,
            &image,
            BTreeMap::new(),
            BTreeMap::new(),
            vec![],
            BTreeMap::new(),
        );
        match result {
            Err(err) => assert_eq!(expected, ValidateErrorDiscriminants::from(err)),
            Ok(_) => panic!("validate should have failed with {expected:?}"),
        }
    }

    fn test_objects() -> (v1alpha2::AirflowCluster, ResolvedProductImage) {
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

    // --- build tests ---

    #[test]
    fn test_build() {
        let validated = validated_cluster();

        let resources = build(&validated);

        assert_eq!(
            vec![
                "my-airflow-scheduler-default",
                "my-airflow-webserver-default",
            ],
            extract_resource_names(&resources.stateful_sets)
        );
        assert_eq!(
            vec![
                "my-airflow-scheduler-default-headless",
                "my-airflow-scheduler-default-metrics",
                "my-airflow-webserver-default-headless",
                "my-airflow-webserver-default-metrics",
            ],
            extract_resource_names(&resources.services)
        );
        assert_eq!(
            vec![
                "my-airflow-scheduler-default",
                "my-airflow-webserver-default",
            ],
            extract_resource_names(&resources.config_maps)
        );
        assert_eq!(
            vec!["my-airflow-serviceaccount"],
            extract_resource_names(&resources.service_accounts)
        );
        assert_eq!(
            vec!["my-airflow-rolebinding"],
            extract_resource_names(&resources.role_bindings)
        );
        assert_eq!(
            vec!["my-airflow-scheduler", "my-airflow-webserver"],
            extract_resource_names(&resources.pod_disruption_budgets)
        );
        assert_eq!(
            vec!["my-airflow-webserver"],
            extract_resource_names(&resources.listeners)
        );
    }

    fn extract_resource_names(resources: &[impl Resource]) -> Vec<&str> {
        let mut names: Vec<&str> = resources
            .iter()
            .filter_map(|r| r.meta().name.as_ref())
            .map(|n| n.as_str())
            .collect();
        names.sort();
        names
    }

    fn validated_cluster() -> ValidatedAirflowCluster {
        let image = ResolvedProductImage {
            product_version: "2.10.4".to_owned(),
            app_version_label_value: LabelValue::from_str("2.10.4-stackable0.0.0-dev")
                .expect("valid label value"),
            image: "oci.stackable.tech/sdp/airflow:2.10.4-stackable0.0.0-dev".to_string(),
            image_pull_policy: "Always".to_owned(),
            pull_secrets: None,
        };

        let logging = ValidatedLogging {
            airflow_container: ValidatedContainerLogConfigChoice::Automatic(
                AutomaticContainerLogConfig::default(),
            ),
            vector_container: None,
            git_sync_container_log_config: ContainerLogConfig {
                choice: Some(ContainerLogConfigChoice::Automatic(
                    AutomaticContainerLogConfig::default(),
                )),
            },
        };

        let role_group_config = ValidatedRoleGroupConfig {
            resources: Resources::<AirflowStorageConfig, NoRuntimeLimits>::default(),
            logging: logging.clone(),
            affinity: StackableAffinity::default(),
            graceful_shutdown_timeout: Duration::from_secs(120),
            config_file_content: String::new(),
        };

        let pod_data = PrecomputedPodData {
            env_vars: vec![],
            airflow_commands: vec!["airflow webserver".to_string()],
            auth_volumes: vec![],
            auth_volume_mounts: vec![],
            extra_volumes: vec![],
            extra_volume_mounts: vec![],
            git_sync_containers: vec![],
            git_sync_init_containers: vec![],
            git_sync_volumes: vec![],
            git_sync_volume_mounts: vec![],
            vector_container: None,
            service_account_name: "my-airflow-serviceaccount".to_string(),
            replicas: Some(1),
            pod_overrides: PodTemplateSpec::default(),
            executor: AirflowExecutor::KubernetesExecutors {
                common_configuration: Box::default(),
            },
            executor_template_configmap_name: None,
            listener_volume_claim_template: None,
        };

        let role_groups = BTreeMap::from([
            (
                AirflowRole::Webserver,
                BTreeMap::from([("default".to_string(), role_group_config.clone())]),
            ),
            (
                AirflowRole::Scheduler,
                BTreeMap::from([("default".to_string(), role_group_config)]),
            ),
        ]);

        let precomputed_pod_data = BTreeMap::from([
            (
                AirflowRole::Webserver,
                BTreeMap::from([("default".to_string(), pod_data.clone())]),
            ),
            (
                AirflowRole::Scheduler,
                BTreeMap::from([("default".to_string(), pod_data)]),
            ),
        ]);

        // Role configs: PDB enabled for both roles; Webserver also gets a listener
        let role_configs = BTreeMap::from([
            (
                AirflowRole::Scheduler,
                ValidatedRoleConfig {
                    pdb_enabled: true,
                    pdb_max_unavailable: None,
                    listener_class: None,
                    group_listener_name: None,
                },
            ),
            (
                AirflowRole::Webserver,
                ValidatedRoleConfig {
                    pdb_enabled: true,
                    pdb_max_unavailable: None,
                    listener_class: Some("cluster-internal".to_string()),
                    group_listener_name: Some("my-airflow-webserver".to_string()),
                },
            ),
        ]);

        ValidatedAirflowCluster::new(
            image,
            ClusterName::from_str_unsafe("my-airflow"),
            NamespaceName::from_str_unsafe("default"),
            Uid::from_str_unsafe("e6ac237d-a6d4-43a1-8135-f36506110912"),
            role_groups,
            precomputed_pod_data,
            vec![],
            role_configs,
            AirflowExecutor::KubernetesExecutors {
                common_configuration: Box::default(),
            },
        )
    }
}
