use std::collections::{BTreeMap, BTreeSet};

use product_config::flask_app_config_writer::{FlaskAppConfigOptions, PythonType};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    commons::{
        affinity::StackableAffinity,
        cache::UserInformationCache,
        cluster_operation::ClusterOperation,
        opa::OpaConfig,
        product_image_selection::{ProductImage, ResolvedProductImage},
        resources::{
            CpuLimitsFragment, MemoryLimitsFragment, NoRuntimeLimits, NoRuntimeLimitsFragment,
            Resources, ResourcesFragment,
        },
    },
    config::{
        fragment::{self, Fragment, ValidationError},
        merge::Merge,
    },
    crd::git_sync,
    deep_merger::ObjectOverrides,
    k8s_openapi::{
        api::core::v1::{Volume, VolumeMount},
        apimachinery::pkg::api::resource::Quantity,
    },
    kube::{CustomResource, ResourceExt},
    kvp::ObjectLabels,
    memory::{BinaryMultiple, MemoryQuantity},
    product_config_utils::{self, Configuration},
    product_logging::{
        self,
        framework::{create_vector_shutdown_file_command, remove_vector_shutdown_file_command},
        spec::Logging,
    },
    role_utils::{
        CommonConfiguration, GenericProductSpecificCommonConfig, GenericRoleConfig, Role,
        RoleGroup, RoleGroupRef,
    },
    schemars::{self, JsonSchema},
    shared::time::Duration,
    status::condition::{ClusterCondition, HasStatusCondition},
    utils::{COMMON_BASH_TRAP_FUNCTIONS, crds::raw_object_list_schema},
    versioned::versioned,
};
use strum::{Display, EnumIter, EnumString, IntoEnumIterator};

use crate::{
    connections::database::DbType,
    crd::{
        affinity::{get_affinity, get_executor_affinity},
        authentication::{
            AirflowAuthenticationClassResolved, AirflowClientAuthenticationDetails,
            AirflowClientAuthenticationDetailsResolved,
        },
        v1alpha2::WebserverRoleConfig,
    },
    util::role_service_name,
};

pub mod affinity;
pub mod authentication;
pub mod authorization;
pub mod internal_secret;

pub const APP_NAME: &str = "airflow";
pub const FIELD_MANAGER: &str = "airflow-operator";
pub const OPERATOR_NAME: &str = "airflow.stackable.tech";
pub const CONFIG_PATH: &str = "/stackable/app/config";
pub const STACKABLE_LOG_DIR: &str = "/stackable/log";
pub const LOG_CONFIG_DIR: &str = "/stackable/app/log_config";
pub const AIRFLOW_HOME: &str = "/stackable/airflow";
pub const AIRFLOW_CONFIG_FILENAME: &str = "webserver_config.py";

pub const TEMPLATE_VOLUME_NAME: &str = "airflow-executor-pod-template";
pub const TEMPLATE_LOCATION: &str = "/templates";
pub const TEMPLATE_NAME: &str = "airflow_executor_pod_template.yaml";

pub const LISTENER_VOLUME_NAME: &str = "listener";
pub const LISTENER_VOLUME_DIR: &str = "/stackable/listener";

pub const HTTP_PORT_NAME: &str = "http";
pub const HTTP_PORT: u16 = 8080;
pub const METRICS_PORT_NAME: &str = "metrics";
pub const METRICS_PORT: u16 = 9102;

const DEFAULT_AIRFLOW_GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_minutes_unchecked(2);
const DEFAULT_WORKER_GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_minutes_unchecked(5);

pub const MAX_LOG_FILES_SIZE: MemoryQuantity = MemoryQuantity {
    value: 10.0,
    unit: BinaryMultiple::Mebi,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Unknown Airflow role found {role}. Should be one of {roles:?}"))]
    UnknownAirflowRole { role: String, roles: Vec<String> },

    #[snafu(display("fragment validation failure"))]
    FragmentValidationFailure { source: ValidationError },

    #[snafu(display("Configuration/Executor conflict!"))]
    NoRoleForExecutorFailure,

    #[snafu(display("object has no associated namespace"))]
    NoNamespace,
}

#[derive(Display, EnumIter, EnumString)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum AirflowConfigOptions {
    AuthType,
    OauthProviders,
    AuthLdapSearch,
    AuthLdapSearchFilter,
    AuthLdapServer,
    AuthLdapUidField,
    AuthLdapBindUser,
    AuthLdapBindPassword,
    AuthUserRegistration,
    AuthUserRegistrationRole,
    AuthLdapFirstnameField,
    AuthLdapLastnameField,
    AuthLdapEmailField,
    AuthLdapGroupField,
    AuthRolesSyncAtLogin,
    AuthLdapTlsDemand,
    AuthLdapTlsCertfile,
    AuthLdapTlsKeyfile,
    AuthLdapTlsCacertfile,
    AuthLdapAllowSelfSigned,
    // OPA configs for Airflow 2
    // Airflow 3 configs need to be passed via env variables!
    // See `env_vars::authorization_env_vars` for details
    AuthOpaCacheMaxsize,
    AuthOpaCacheTtlInSec,
    AuthOpaRequestUrl,
    AuthOpaRequestTimeout,
}

impl FlaskAppConfigOptions for AirflowConfigOptions {
    fn python_type(&self) -> PythonType {
        match self {
            AirflowConfigOptions::AuthType => PythonType::Expression,
            AirflowConfigOptions::OauthProviders => PythonType::Expression,
            AirflowConfigOptions::AuthUserRegistration => PythonType::BoolLiteral,
            AirflowConfigOptions::AuthUserRegistrationRole => PythonType::StringLiteral,
            AirflowConfigOptions::AuthRolesSyncAtLogin => PythonType::BoolLiteral,
            AirflowConfigOptions::AuthLdapServer => PythonType::StringLiteral,
            AirflowConfigOptions::AuthLdapBindUser => PythonType::Expression,
            AirflowConfigOptions::AuthLdapBindPassword => PythonType::Expression,
            AirflowConfigOptions::AuthLdapSearch => PythonType::StringLiteral,
            AirflowConfigOptions::AuthLdapSearchFilter => PythonType::StringLiteral,
            AirflowConfigOptions::AuthLdapUidField => PythonType::StringLiteral,
            AirflowConfigOptions::AuthLdapGroupField => PythonType::StringLiteral,
            AirflowConfigOptions::AuthLdapFirstnameField => PythonType::StringLiteral,
            AirflowConfigOptions::AuthLdapLastnameField => PythonType::StringLiteral,
            AirflowConfigOptions::AuthLdapEmailField => PythonType::StringLiteral,
            AirflowConfigOptions::AuthLdapTlsDemand => PythonType::BoolLiteral,
            AirflowConfigOptions::AuthLdapTlsCertfile => PythonType::StringLiteral,
            AirflowConfigOptions::AuthLdapTlsKeyfile => PythonType::StringLiteral,
            AirflowConfigOptions::AuthLdapTlsCacertfile => PythonType::StringLiteral,
            AirflowConfigOptions::AuthLdapAllowSelfSigned => PythonType::BoolLiteral,
            AirflowConfigOptions::AuthOpaCacheMaxsize => PythonType::IntLiteral,
            AirflowConfigOptions::AuthOpaCacheTtlInSec => PythonType::IntLiteral,
            AirflowConfigOptions::AuthOpaRequestUrl => PythonType::StringLiteral,
            AirflowConfigOptions::AuthOpaRequestTimeout => PythonType::IntLiteral,
        }
    }
}

#[versioned(
    version(name = "v1alpha1"),
    version(name = "v1alpha2"),
    crates(
        kube_core = "stackable_operator::kube::core",
        kube_client = "stackable_operator::kube::client",
        k8s_openapi = "stackable_operator::k8s_openapi",
        schemars = "stackable_operator::schemars",
        versioned = "stackable_operator::versioned"
    )
)]
pub mod versioned {
    /// An Airflow cluster stacklet. This resource is managed by the Stackable operator for Apache Airflow.
    /// Find more information on how to use it and the resources that the operator generates in the
    /// [operator documentation](DOCS_BASE_URL_PLACEHOLDER/airflow/).
    ///
    /// The CRD contains three roles: webserver, scheduler and worker/celeryExecutor.
    /// You can use either the celeryExecutor or the kubernetesExecutor.
    #[versioned(crd(
        group = "airflow.stackable.tech",
        plural = "airflowclusters",
        shortname = "airflow",
        status = "AirflowClusterStatus",
        namespaced,
    ))]
    #[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct AirflowClusterSpec {
        // no doc string - See ProductImage struct
        pub image: ProductImage,

        /// Configuration that applies to all roles and role groups.
        /// This includes settings for authentication, git sync, service exposition and volumes, among other things.
        pub cluster_config: AirflowClusterConfig,

        // no doc string - See ClusterOperation struct
        #[serde(default)]
        pub cluster_operation: ClusterOperation,

        #[serde(default)]
        pub object_overrides: ObjectOverrides,

        /// The `webservers` role provides the main UI for user interaction.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub webservers: Option<Role<AirflowConfigFragment, v1alpha2::WebserverRoleConfig>>,

        /// The `schedulers` is responsible for triggering jobs and persisting their metadata to the backend database.
        /// Jobs are scheduled on the workers/executors.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub schedulers: Option<Role<AirflowConfigFragment>>,

        #[serde(flatten)]
        pub executor: AirflowExecutor,

        /// The `dagProcessors` role runs the DAG processor routine for DAG preparation.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub dag_processors: Option<Role<AirflowConfigFragment>>,

        /// The `triggerers` role runs the triggerer process for use with deferrable DAG operators.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub triggerers: Option<Role<AirflowConfigFragment>>,
    }

    #[derive(Clone, Deserialize, Debug, JsonSchema, PartialEq, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct AirflowClusterConfig {
        #[serde(default)]
        pub authentication: Vec<AirflowClientAuthenticationDetails>,

        /// Authorization options.
        /// Learn more in the [Airflow authorization usage guide](DOCS_BASE_URL_PLACEHOLDER/airflow/usage-guide/security#_authorization).
        #[serde(skip_serializing_if = "Option::is_none")]
        pub authorization: Option<AirflowAuthorization>,

        /// The name of the Secret object containing the admin user credentials and database connection details.
        /// Read the
        /// [getting started guide first steps](DOCS_BASE_URL_PLACEHOLDER/airflow/getting_started/first_steps)
        /// to find out more.
        pub credentials_secret: String,

        /// Connection information needed to construct a connection for the
        /// mandatory backend metadata database.
        pub metadata_database: DbType,

        /// The `gitSync` settings allow configuring DAGs to mount via `git-sync`.
        /// Learn more in the
        /// [mounting DAGs documentation](DOCS_BASE_URL_PLACEHOLDER/airflow/usage-guide/mounting-dags#_via_git_sync).
        #[serde(default)]
        #[versioned(
            changed(since = "v1alpha2", from_type = "Vec<git_sync::v1alpha1::GitSync>"),
            hint(vec)
        )]
        pub dags_git_sync: Vec<git_sync::v1alpha2::GitSync>,

        /// for internal use only - not for production use.
        #[serde(default)]
        pub expose_config: bool,

        /// Whether to load example DAGs or not; defaults to false. The examples are used in the
        /// [getting started guide](DOCS_BASE_URL_PLACEHOLDER/airflow/getting_started/).
        #[serde(default)]
        pub load_examples: bool,

        /// Settings related to the database initialization routines (which are always executed by default).
        #[serde(default)]
        pub database_initialization: DatabaseInitializationConfig,

        /// Name of the Vector aggregator [discovery ConfigMap](DOCS_BASE_URL_PLACEHOLDER/concepts/service_discovery).
        /// It must contain the key `ADDRESS` with the address of the Vector aggregator.
        /// Follow the [logging tutorial](DOCS_BASE_URL_PLACEHOLDER/tutorials/logging-vector-aggregator)
        /// to learn how to configure log aggregation with Vector.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub vector_aggregator_config_map_name: Option<String>,

        /// Additional volumes to define. Use together with `volumeMounts` to mount the volumes.
        #[serde(default)]
        #[schemars(schema_with = "raw_object_list_schema")]
        pub volumes: Vec<Volume>,

        /// Additional volumes to mount. Use together with `volumes` to define volumes.
        #[serde(default)]
        #[schemars(schema_with = "raw_object_list_schema")]
        pub volume_mounts: Vec<VolumeMount>,
    }
    // TODO: move generic version to op-rs?
    #[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct WebserverRoleConfig {
        #[serde(flatten)]
        pub common: GenericRoleConfig,

        /// This field controls which [ListenerClass](https://docs.stackable.tech/home/nightly/listener-operator/listenerclass.html) is used to expose the webserver.
        #[serde(default = "webserver_default_listener_class")]
        pub listener_class: String,
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseInitializationConfig {
    /// Whether to execute the database initialization routines (a combination of database initialization, upgrade and migration depending on the Airflow version). Defaults to true to be backwards-compatible.
    /// WARNING: setting this to false is *unsupported* as subsequent updates to the Airflow cluster may result in broken behaviour due to inconsistent metadata!
    /// Do not change the default unless you know what you are doing!
    #[serde(default = "default_db_init")]
    pub enabled: bool,
}

impl Default for DatabaseInitializationConfig {
    fn default() -> Self {
        Self {
            enabled: default_db_init(),
        }
    }
}

pub fn default_db_init() -> bool {
    true
}

impl Default for v1alpha2::WebserverRoleConfig {
    fn default() -> Self {
        v1alpha2::WebserverRoleConfig {
            listener_class: webserver_default_listener_class(),
            common: Default::default(),
        }
    }
}

fn webserver_default_listener_class() -> String {
    "cluster-internal".to_string()
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AirflowClusterStatus {
    #[serde(default)]
    pub conditions: Vec<ClusterCondition>,
}

impl HasStatusCondition for v1alpha2::AirflowCluster {
    fn conditions(&self) -> Vec<ClusterCondition> {
        match &self.status {
            Some(status) => status.conditions.clone(),
            None => vec![],
        }
    }
}

impl v1alpha2::AirflowCluster {
    /// The name of the group-listener provided for a specific role.
    /// Webservers will use this group listener so that only one load balancer
    /// is needed for that role.
    pub fn group_listener_name(&self, role: &AirflowRole) -> Option<String> {
        match role {
            AirflowRole::Webserver => Some(role_service_name(&self.name_any(), &role.to_string())),
            AirflowRole::Scheduler
            | AirflowRole::Worker
            | AirflowRole::DagProcessor
            | AirflowRole::Triggerer => None,
        }
    }

    /// the worker role will not be returned if airflow provisions pods as needed (i.e. when
    /// the kubernetes executor is specified)
    pub fn get_role(&self, role: &AirflowRole) -> Option<Role<AirflowConfigFragment>> {
        match role {
            AirflowRole::Webserver => self
                .spec
                .webservers
                .to_owned()
                .map(extract_role_from_webserver_config),
            AirflowRole::Scheduler => self.spec.schedulers.to_owned(),
            AirflowRole::DagProcessor => self.spec.dag_processors.to_owned(),
            AirflowRole::Triggerer => self.spec.triggerers.to_owned(),
            AirflowRole::Worker => {
                if let AirflowExecutor::CeleryExecutor { config } = &self.spec.executor {
                    Some(config.clone())
                } else {
                    None
                }
            }
        }
    }

    pub fn role_config(&self, role: &AirflowRole) -> Option<GenericRoleConfig> {
        self.get_role(role).map(|r| r.role_config)
    }

    pub fn volumes(&self) -> &Vec<Volume> {
        &self.spec.cluster_config.volumes
    }

    pub fn volume_mounts(&self) -> Vec<VolumeMount> {
        self.spec.cluster_config.volume_mounts.clone()
    }

    pub fn executor_template_configmap_name(&self) -> String {
        format!("{}-executor-pod-template", self.name_any())
    }

    /// Retrieve and merge resource configs for role and role groups
    pub fn merged_config(
        &self,
        role: &AirflowRole,
        rolegroup_ref: &RoleGroupRef<v1alpha2::AirflowCluster>,
    ) -> Result<AirflowConfig, Error> {
        // Initialize the result with all default values as baseline
        let conf_defaults = AirflowConfig::default_config(&self.name_any(), role);

        let role_config = role.role_config(self)?;

        // Retrieve role resource config
        let mut conf_role = role_config.config.config;

        // Retrieve rolegroup specific resource config
        let mut conf_rolegroup = role_config
            .role_groups
            .get(&rolegroup_ref.role_group)
            .map(|rg| rg.config.config.clone())
            .unwrap_or_default();

        // Merge more specific configs into default config
        // Hierarchy is:
        // 1. RoleGroup
        // 2. Role
        // 3. Default
        conf_role.merge(&conf_defaults);
        conf_rolegroup.merge(&conf_role);

        tracing::debug!("Merged config: {:?}", conf_rolegroup);
        fragment::validate(conf_rolegroup).context(FragmentValidationFailureSnafu)
    }

    /// Retrieve and merge resource configs for the executor template
    pub fn merged_executor_config(
        &self,
        config: &ExecutorConfigFragment,
    ) -> Result<ExecutorConfig, Error> {
        // use the worker defaults for executor pods
        let resources = default_resources(&AirflowRole::Worker);
        let logging = product_logging::spec::default_logging();
        let affinity = get_executor_affinity(&self.name_any());
        let graceful_shutdown_timeout = Some(DEFAULT_WORKER_GRACEFUL_SHUTDOWN_TIMEOUT);

        let executor_defaults = ExecutorConfigFragment {
            resources,
            logging,
            affinity,
            graceful_shutdown_timeout,
        };

        let mut conf_executor = config.to_owned();
        conf_executor.merge(&executor_defaults);

        tracing::debug!("Merged executor config: {:?}", conf_executor);
        fragment::validate(conf_executor).context(FragmentValidationFailureSnafu)
    }

    pub fn shared_internal_secret_secret_name(&self) -> String {
        format!("{}-internal-secret", &self.name_any())
    }

    pub fn shared_jwt_secret_secret_name(&self) -> String {
        format!("{}-jwt-secret", &self.name_any())
    }

    pub fn shared_fernet_key_secret_name(&self) -> String {
        format!("{}-fernet-key", &self.name_any())
    }
}

fn extract_role_from_webserver_config(
    fragment: Role<AirflowConfigFragment, WebserverRoleConfig>,
) -> Role<AirflowConfigFragment> {
    Role {
        config: CommonConfiguration {
            config: fragment.config.config,
            config_overrides: fragment.config.config_overrides,
            env_overrides: fragment.config.env_overrides,
            cli_overrides: fragment.config.cli_overrides,
            pod_overrides: fragment.config.pod_overrides,
            product_specific_common_config: fragment.config.product_specific_common_config,
        },
        role_config: fragment.role_config.common,
        role_groups: fragment
            .role_groups
            .into_iter()
            .map(|(k, v)| {
                (
                    k,
                    RoleGroup {
                        config: CommonConfiguration {
                            config: v.config.config,
                            config_overrides: v.config.config_overrides,
                            env_overrides: v.config.env_overrides,
                            cli_overrides: v.config.cli_overrides,
                            pod_overrides: v.config.pod_overrides,
                            product_specific_common_config: v.config.product_specific_common_config,
                        },
                        replicas: v.replicas,
                    },
                )
            })
            .collect(),
    }
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AirflowAuthorization {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub opa: Option<AirflowOpaConfig>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AirflowOpaConfig {
    #[serde(flatten)]
    pub opa: OpaConfig,
    #[serde(default)]
    pub cache: UserInformationCache,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AirflowCredentials {
    pub admin_user: AdminUserCredentials,
    pub connections: Connections,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminUserCredentials {
    pub username: String,
    pub firstname: String,
    pub lastname: String,
    pub email: String,
    pub password: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Connections {
    pub secret_key: String,
    pub sqlalchemy_database_uri: String,
}

#[derive(
    Clone,
    Debug,
    Deserialize,
    Display,
    EnumIter,
    Eq,
    Hash,
    JsonSchema,
    PartialEq,
    Serialize,
    EnumString,
)]
pub enum AirflowRole {
    #[strum(serialize = "webserver")]
    Webserver,

    #[strum(serialize = "scheduler")]
    Scheduler,

    #[strum(serialize = "worker")]
    Worker,

    #[strum(serialize = "dagprocessor")]
    DagProcessor,

    #[strum(serialize = "triggerer")]
    Triggerer,
}

impl AirflowRole {
    /// Returns the start commands for the different airflow components. Airflow expects all
    /// components to have the same image/configuration (e.g. DAG folder location), even if not all
    /// configuration settings are used everywhere. For this reason we ensure that the webserver
    /// config file is in the Airflow home directory on all pods.
    /// Only the webserver needs to know about authentication CA's which is added via python's certify
    /// if authentication is enabled.
    pub fn get_commands(
        &self,
        airflow: &v1alpha2::AirflowCluster,
        auth_config: &AirflowClientAuthenticationDetailsResolved,
        resolved_product_image: &ResolvedProductImage,
    ) -> Vec<String> {
        let mut command = vec![
            format!(
                "cp -RL {CONFIG_PATH}/{AIRFLOW_CONFIG_FILENAME} {AIRFLOW_HOME}/{AIRFLOW_CONFIG_FILENAME}"
            ),
            // graceful shutdown part
            COMMON_BASH_TRAP_FUNCTIONS.to_string(),
            remove_vector_shutdown_file_command(STACKABLE_LOG_DIR),
        ];

        if resolved_product_image.product_version.starts_with("3.") {
            // Start-up commands have changed in 3.x.
            // See https://airflow.apache.org/docs/apache-airflow/3.0.1/installation/upgrading_to_airflow3.html#step-6-changes-to-your-startup-scripts and
            // https://airflow.apache.org/docs/apache-airflow/3.0.1/installation/setting-up-the-database.html#setting-up-the-database.
            // `airflow db migrate` is not run for each role so there may be
            // re-starts of webserver and/or workers (which require the DB).
            // DB-migrations should be eventually be optional:
            // See https://github.com/stackabletech/airflow-operator/issues/589.
            match &self {
                AirflowRole::Webserver => {
                    command.extend(Self::authentication_start_commands(auth_config));
                    command.extend(vec![
                        "prepare_signal_handlers".to_string(),
                        container_debug_command(),
                        "airflow api-server &".to_string(),
                    ]);
                }
                AirflowRole::Scheduler => {
                    if airflow.spec.cluster_config.database_initialization.enabled {
                        tracing::info!("Database initialization has been enabled.");
                        command.extend(vec![
                            "airflow db migrate".to_string(),
                            "airflow users create \
                            --username \"$ADMIN_USERNAME\" \
                            --firstname \"$ADMIN_FIRSTNAME\" \
                            --lastname \"$ADMIN_LASTNAME\" \
                            --email \"$ADMIN_EMAIL\" \
                            --password \"$ADMIN_PASSWORD\" \
                            --role \"Admin\""
                                .to_string(),
                        ]);
                    } else {
                        tracing::info!("Database initialization routines have been skipped!")
                    }
                    command.extend(vec![
                        "prepare_signal_handlers".to_string(),
                        container_debug_command(),
                        "airflow scheduler &".to_string(),
                    ]);
                    if airflow.spec.dag_processors.is_none() {
                        // If no dag_processors role has been specified, the
                        // process needs to be included with the scheduler
                        // (with 3.x there is no longer the possibility of
                        // starting it as a subprocess, so it has to be
                        // explicitly started *somewhere*)
                        command.extend(vec!["airflow dag-processor &".to_string()]);
                    }
                }
                AirflowRole::DagProcessor => command.extend(vec![
                    "prepare_signal_handlers".to_string(),
                    container_debug_command(),
                    "airflow dag-processor &".to_string(),
                ]),
                AirflowRole::Triggerer => command.extend(vec![
                    "prepare_signal_handlers".to_string(),
                    container_debug_command(),
                    "airflow triggerer &".to_string(),
                ]),
                AirflowRole::Worker => command.extend(vec![
                    "prepare_signal_handlers".to_string(),
                    container_debug_command(),
                    "airflow celery worker &".to_string(),
                ]),
            }
        } else {
            match &self {
                AirflowRole::Webserver => {
                    // Getting auth commands for AuthClass
                    command.extend(Self::authentication_start_commands(auth_config));
                    command.extend(vec![
                        "prepare_signal_handlers".to_string(),
                        container_debug_command(),
                        "airflow webserver &".to_string(),
                    ]);
                }
                AirflowRole::Scheduler => {
                    if airflow.spec.cluster_config.database_initialization.enabled {
                        tracing::info!("Database initialization has been enabled.");
                        command.extend(vec![
                            // Database initialization is limited to the scheduler, see https://github.com/stackabletech/airflow-operator/issues/259
                            "airflow db init".to_string(),
                            "airflow db upgrade".to_string(),
                            "airflow users create \
                            --username \"$ADMIN_USERNAME\" \
                            --firstname \"$ADMIN_FIRSTNAME\" \
                            --lastname \"$ADMIN_LASTNAME\" \
                            --email \"$ADMIN_EMAIL\" \
                            --password \"$ADMIN_PASSWORD\" \
                            --role \"Admin\""
                                .to_string(),
                        ]);
                    } else {
                        tracing::info!("Database initialization routines have been skipped!")
                    }
                    command.extend(vec![
                        "prepare_signal_handlers".to_string(),
                        container_debug_command(),
                        "airflow scheduler &".to_string(),
                    ]);
                }
                AirflowRole::DagProcessor => command.extend(vec![
                    "prepare_signal_handlers".to_string(),
                    container_debug_command(),
                    "airflow dag-processor &".to_string(),
                ]),
                AirflowRole::Triggerer => command.extend(vec![
                    "prepare_signal_handlers".to_string(),
                    container_debug_command(),
                    "airflow triggerer &".to_string(),
                ]),
                AirflowRole::Worker => command.extend(vec![
                    "prepare_signal_handlers".to_string(),
                    container_debug_command(),
                    "airflow celery worker &".to_string(),
                ]),
            }
        }

        // graceful shutdown part
        command.extend(vec![
            "wait_for_termination $!".to_string(),
            create_vector_shutdown_file_command(STACKABLE_LOG_DIR),
        ]);

        command
    }

    fn authentication_start_commands(
        auth_config: &AirflowClientAuthenticationDetailsResolved,
    ) -> Vec<String> {
        let mut commands = Vec::new();

        let mut tls_client_credentials = BTreeSet::new();

        for auth_class_resolved in &auth_config.authentication_classes_resolved {
            match auth_class_resolved {
                AirflowAuthenticationClassResolved::Oidc { provider, .. } => {
                    tls_client_credentials.insert(&provider.tls);

                    // WebPKI will be handled implicitly
                }
                AirflowAuthenticationClassResolved::Ldap { .. } => {}
            }
        }

        for tls in tls_client_credentials {
            commands.push(tls.tls_ca_cert_mount_path().map(|tls_ca_cert_mount_path| {
                Self::add_cert_to_python_certifi_command(&tls_ca_cert_mount_path)
            }));
        }

        commands.iter().flatten().cloned().collect::<Vec<_>>()
    }

    // Adding certificate to the mount path for airflow startup commands
    fn add_cert_to_python_certifi_command(cert_file: &str) -> String {
        format!("cat {cert_file} >> \"$(python -c 'import certifi; print(certifi.where())')\"")
    }

    /// Will be used to expose service ports and - by extension - which roles should be
    /// created as services.
    pub fn get_http_port(&self) -> Option<u16> {
        match &self {
            AirflowRole::Webserver => Some(HTTP_PORT),
            AirflowRole::Scheduler => None,
            AirflowRole::Worker => None,
            AirflowRole::DagProcessor => None,
            AirflowRole::Triggerer => None,
        }
    }

    pub fn roles() -> Vec<String> {
        let mut roles = vec![];
        for role in Self::iter() {
            roles.push(role.to_string())
        }
        roles
    }

    pub fn listener_class_name(&self, airflow: &v1alpha2::AirflowCluster) -> Option<String> {
        match self {
            Self::Webserver => airflow
                .spec
                .webservers
                .to_owned()
                .map(|webserver| webserver.role_config.listener_class),
            Self::Worker | Self::Scheduler | Self::DagProcessor | Self::Triggerer => None,
        }
    }

    pub fn role_config(
        &self,
        airflow: &v1alpha2::AirflowCluster,
    ) -> Result<Role<AirflowConfigFragment>, Error> {
        let role = self.to_string();
        let roles = AirflowRole::roles();

        let role_config = match self {
            AirflowRole::Webserver => &extract_role_from_webserver_config(
                airflow
                    .spec
                    .webservers
                    .to_owned()
                    .context(UnknownAirflowRoleSnafu { role, roles })?,
            ),
            AirflowRole::Worker => {
                if let AirflowExecutor::CeleryExecutor { config } = &airflow.spec.executor {
                    config
                } else {
                    return Err(Error::NoRoleForExecutorFailure);
                }
            }
            AirflowRole::Scheduler => airflow
                .spec
                .schedulers
                .as_ref()
                .context(UnknownAirflowRoleSnafu { role, roles })?,
            AirflowRole::DagProcessor => airflow
                .spec
                .dag_processors
                .as_ref()
                .context(UnknownAirflowRoleSnafu { role, roles })?,
            AirflowRole::Triggerer => airflow
                .spec
                .triggerers
                .as_ref()
                .context(UnknownAirflowRoleSnafu { role, roles })?,
        };
        Ok(role_config.clone())
    }
}

fn container_debug_command() -> String {
    format!("containerdebug --output={STACKABLE_LOG_DIR}/containerdebug-state.json --loop &")
}

#[derive(Clone, Debug, Deserialize, Display, JsonSchema, PartialEq, Serialize)]
pub enum AirflowExecutor {
    /// The celery executor.
    /// Deployed with an explicit number of replicas.
    #[serde(rename = "celeryExecutors")]
    CeleryExecutor {
        #[serde(flatten)]
        config: Role<AirflowConfigFragment>,
    },

    /// With the Kuberentes executor, executor Pods are created on demand.
    #[serde(rename = "kubernetesExecutors")]
    KubernetesExecutor {
        #[serde(flatten)]
        common_configuration:
            CommonConfiguration<ExecutorConfigFragment, GenericProductSpecificCommonConfig>,
    },
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, Debug, Default, JsonSchema, PartialEq, Fragment)]
#[fragment_attrs(
    allow(clippy::derive_partial_eq_without_eq),
    derive(
        Clone,
        Debug,
        Default,
        Deserialize,
        Merge,
        JsonSchema,
        PartialEq,
        Serialize
    ),
    serde(rename_all = "camelCase")
)]
pub struct AirflowStorageConfig {}

#[derive(
    Clone,
    Debug,
    Deserialize,
    Display,
    Eq,
    EnumIter,
    JsonSchema,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
)]
#[serde(rename_all = "kebab-case")]
#[strum(serialize_all = "kebab-case")]
pub enum Container {
    Airflow,
    Vector,
    Base,
    GitSync,
}

#[derive(Clone, Debug, Default, Fragment, JsonSchema, PartialEq)]
#[fragment_attrs(
    derive(
        Clone,
        Debug,
        Default,
        Deserialize,
        Merge,
        JsonSchema,
        PartialEq,
        Serialize
    ),
    serde(rename_all = "camelCase")
)]
pub struct ExecutorConfig {
    #[fragment_attrs(serde(default))]
    pub resources: Resources<AirflowStorageConfig, NoRuntimeLimits>,

    #[fragment_attrs(serde(default))]
    pub logging: Logging<Container>,

    #[fragment_attrs(serde(default))]
    pub affinity: StackableAffinity,

    /// Time period Pods have to gracefully shut down, e.g. `30m`, `1h` or `2d`. Consult the operator documentation for details.
    #[fragment_attrs(serde(default))]
    pub graceful_shutdown_timeout: Option<Duration>,
}

#[derive(Clone, Debug, Default, Fragment, JsonSchema, PartialEq)]
#[fragment_attrs(
    derive(
        Clone,
        Debug,
        Default,
        Deserialize,
        Merge,
        JsonSchema,
        PartialEq,
        Serialize
    ),
    serde(rename_all = "camelCase")
)]
pub struct AirflowConfig {
    #[fragment_attrs(serde(default))]
    pub resources: Resources<AirflowStorageConfig, NoRuntimeLimits>,

    #[fragment_attrs(serde(default))]
    pub logging: Logging<Container>,

    #[fragment_attrs(serde(default))]
    pub affinity: StackableAffinity,

    /// Time period Pods have to gracefully shut down, e.g. `30m`, `1h` or `2d`. Consult the operator documentation for details.
    #[fragment_attrs(serde(default))]
    pub graceful_shutdown_timeout: Option<Duration>,
}

impl AirflowConfig {
    pub const CREDENTIALS_SECRET_PROPERTY: &'static str = "credentialsSecret";
    pub const GIT_CREDENTIALS_SECRET_PROPERTY: &'static str = "gitCredentialsSecret";

    fn default_config(cluster_name: &str, role: &AirflowRole) -> AirflowConfigFragment {
        AirflowConfigFragment {
            resources: default_resources(role),
            logging: product_logging::spec::default_logging(),
            affinity: get_affinity(cluster_name, role),
            graceful_shutdown_timeout: Some(match role {
                AirflowRole::Webserver
                | AirflowRole::Scheduler
                | AirflowRole::DagProcessor
                | AirflowRole::Triggerer => DEFAULT_AIRFLOW_GRACEFUL_SHUTDOWN_TIMEOUT,
                AirflowRole::Worker => DEFAULT_WORKER_GRACEFUL_SHUTDOWN_TIMEOUT,
            }),
        }
    }
}

impl Configuration for AirflowConfigFragment {
    type Configurable = v1alpha2::AirflowCluster;

    fn compute_env(
        &self,
        cluster: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, product_config_utils::Error> {
        let mut env: BTreeMap<String, Option<String>> = BTreeMap::new();
        env.insert(
            AirflowConfig::CREDENTIALS_SECRET_PROPERTY.to_string(),
            Some(cluster.spec.cluster_config.credentials_secret.clone()),
        );
        Ok(env)
    }

    fn compute_cli(
        &self,
        _cluster: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, product_config_utils::Error> {
        Ok(BTreeMap::new())
    }

    fn compute_files(
        &self,
        _cluster: &Self::Configurable,
        _role_name: &str,
        _file: &str,
    ) -> Result<BTreeMap<String, Option<String>>, product_config_utils::Error> {
        Ok(BTreeMap::new())
    }
}

fn default_resources(role: &AirflowRole) -> ResourcesFragment<AirflowStorageConfig> {
    let (cpu, memory) = match role {
        AirflowRole::Worker => (
            CpuLimitsFragment {
                min: Some(Quantity("1".into())),
                max: Some(Quantity("2".into())),
            },
            MemoryLimitsFragment {
                limit: Some(Quantity("3Gi".into())),
                runtime_limits: NoRuntimeLimitsFragment {},
            },
        ),
        AirflowRole::Webserver => (
            CpuLimitsFragment {
                min: Some(Quantity("1".into())),
                max: Some(Quantity("2".into())),
            },
            MemoryLimitsFragment {
                limit: Some(Quantity("3Gi".into())),
                runtime_limits: NoRuntimeLimitsFragment {},
            },
        ),
        AirflowRole::Scheduler => (
            CpuLimitsFragment {
                min: Some(Quantity("1".to_owned())),
                max: Some(Quantity("2".to_owned())),
            },
            MemoryLimitsFragment {
                limit: Some(Quantity("1Gi".to_owned())),
                runtime_limits: NoRuntimeLimitsFragment {},
            },
        ),
        AirflowRole::DagProcessor => (
            CpuLimitsFragment {
                min: Some(Quantity("1".to_owned())),
                max: Some(Quantity("2".to_owned())),
            },
            MemoryLimitsFragment {
                limit: Some(Quantity("1Gi".to_owned())),
                runtime_limits: NoRuntimeLimitsFragment {},
            },
        ),
        AirflowRole::Triggerer => (
            CpuLimitsFragment {
                min: Some(Quantity("1".to_owned())),
                max: Some(Quantity("2".to_owned())),
            },
            MemoryLimitsFragment {
                limit: Some(Quantity("1Gi".to_owned())),
                runtime_limits: NoRuntimeLimitsFragment {},
            },
        ),
    };

    ResourcesFragment {
        cpu,
        memory,
        storage: AirflowStorageConfigFragment {},
    }
}

/// Creates recommended `ObjectLabels` to be used in deployed resources
pub fn build_recommended_labels<'a, T>(
    owner: &'a T,
    controller_name: &'a str,
    app_version: &'a str,
    role: &'a str,
    role_group: &'a str,
) -> ObjectLabels<'a, T> {
    ObjectLabels {
        owner,
        app_name: APP_NAME,
        app_version,
        operator_name: OPERATOR_NAME,
        controller_name,
        role,
        role_group,
    }
}

#[cfg(test)]
mod tests {
    use stackable_operator::commons::product_image_selection::ResolvedProductImage;

    use crate::v1alpha2::AirflowCluster;

    #[test]
    fn test_cluster_config() {
        let cluster = "
        apiVersion: airflow.stackable.tech/v1alpha2
        kind: AirflowCluster
        metadata:
          name: airflow
        spec:
          image:
            productVersion: 3.0.6
          clusterConfig:
            loadExamples: true
            exposeConfig: true
            credentialsSecret: simple-airflow-credentials
            metadataDatabase:
              postgresql:
                host: airflow-postgresql
                databaseName: airflow
                credentialsSecret: postgresql-credentials
          webservers:
            roleGroups:
              default:
                config: {}
          kubernetesExecutors:
            config: {}
          schedulers:
            roleGroups:
              default:
                config: {}
          ";

        let deserializer = serde_yaml::Deserializer::from_str(cluster);
        let cluster: AirflowCluster =
            serde_yaml::with::singleton_map_recursive::deserialize(deserializer).unwrap();

        let resolved_airflow_image: ResolvedProductImage = cluster
            .spec
            .image
            .resolve("airflow", "0.0.0-dev")
            .expect("test: resolved product image is always valid");

        assert_eq!("3.0.6", &resolved_airflow_image.product_version);

        assert_eq!("KubernetesExecutor", cluster.spec.executor.to_string());
        assert!(cluster.spec.cluster_config.load_examples);
        assert!(cluster.spec.cluster_config.expose_config);
        // defaults to true
        assert!(cluster.spec.cluster_config.database_initialization.enabled);
    }
}
