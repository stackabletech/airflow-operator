use std::collections::{BTreeMap, BTreeSet, HashMap};

use product_config::flask_app_config_writer::{FlaskAppConfigOptions, PythonType};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    commons::{
        affinity::StackableAffinity,
        cache::UserInformationCache,
        cluster_operation::ClusterOperation,
        opa::OpaConfig,
        product_image_selection::ProductImage,
        resources::{
            CpuLimitsFragment, MemoryLimitsFragment, NoRuntimeLimits, NoRuntimeLimitsFragment,
            Resources, ResourcesFragment,
        },
    },
    config::{
        fragment::{self, Fragment, ValidationError},
        merge::{Atomic, Merge},
    },
    k8s_openapi::{
        api::core::v1::{Volume, VolumeMount},
        apimachinery::pkg::api::resource::Quantity,
    },
    kube::{CustomResource, ResourceExt, runtime::reflector::ObjectRef},
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
        RoleGroupRef,
    },
    schemars::{self, JsonSchema},
    status::condition::{ClusterCondition, HasStatusCondition},
    time::Duration,
    utils::{COMMON_BASH_TRAP_FUNCTIONS, crds::raw_object_list_schema},
    versioned::versioned,
};
use strum::{Display, EnumIter, EnumString, IntoEnumIterator};
use utils::{PodRef, get_persisted_listener_podrefs};

use crate::crd::{
    affinity::{get_affinity, get_executor_affinity},
    authentication::{
        AirflowAuthenticationClassResolved, AirflowClientAuthenticationDetails,
        AirflowClientAuthenticationDetailsResolved,
    },
    git_sync::{GIT_SYNC_CONTENT, GIT_SYNC_DIR, GitSync},
};

pub mod affinity;
pub mod authentication;
pub mod authorization;
pub mod git_sync;
pub mod utils;

pub const AIRFLOW_UID: i64 = 1000;
pub const APP_NAME: &str = "airflow";
pub const OPERATOR_NAME: &str = "airflow.stackable.tech";
pub const CONFIG_PATH: &str = "/stackable/app/config";
pub const STACKABLE_LOG_DIR: &str = "/stackable/log";
pub const LOG_CONFIG_DIR: &str = "/stackable/app/log_config";
pub const AIRFLOW_HOME: &str = "/stackable/airflow";
pub const AIRFLOW_CONFIG_FILENAME: &str = "webserver_config.py";

pub const TEMPLATE_VOLUME_NAME: &str = "airflow-executor-pod-template";
pub const TEMPLATE_CONFIGMAP_NAME: &str = "airflow-executor-pod-template";
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

    #[snafu(display("listener podrefs could not be resolved"))]
    ListenerPodRef { source: utils::Error },
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

#[versioned(version(name = "v1alpha1"))]
pub mod versioned {
    /// An Airflow cluster stacklet. This resource is managed by the Stackable operator for Apache Airflow.
    /// Find more information on how to use it and the resources that the operator generates in the
    /// [operator documentation](DOCS_BASE_URL_PLACEHOLDER/airflow/).
    ///
    /// The CRD contains three roles: webserver, scheduler and worker/celeryExecutor.
    /// You can use either the celeryExecutor or the kubernetesExecutor.
    #[versioned(k8s(
        group = "airflow.stackable.tech",
        kind = "AirflowCluster",
        plural = "airflowclusters",
        shortname = "airflow",
        status = "AirflowClusterStatus",
        namespaced,
        crates(
            kube_core = "stackable_operator::kube::core",
            k8s_openapi = "stackable_operator::k8s_openapi",
            schemars = "stackable_operator::schemars"
        )
    ))]
    #[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct AirflowClusterSpec {
        // no doc string - See ProductImage struct
        pub image: ProductImage,

        /// Configuration that applies to all roles and role groups.
        /// This includes settings for authentication, git sync, service exposition and volumes, among other things.
        pub cluster_config: v1alpha1::AirflowClusterConfig,

        // no doc string - See ClusterOperation struct
        #[serde(default)]
        pub cluster_operation: ClusterOperation,

        /// The `webserver` role provides the main UI for user interaction.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub webservers: Option<Role<AirflowConfigFragment>>,

        /// The `scheduler` is responsible for triggering jobs and persisting their metadata to the backend database.
        /// Jobs are scheduled on the workers/executors.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub schedulers: Option<Role<AirflowConfigFragment>>,

        #[serde(flatten)]
        pub executor: AirflowExecutor,
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

        /// The `gitSync` settings allow configuring DAGs to mount via `git-sync`.
        /// Learn more in the
        /// [mounting DAGs documentation](DOCS_BASE_URL_PLACEHOLDER/airflow/usage-guide/mounting-dags#_via_git_sync).
        #[serde(default)]
        pub dags_git_sync: Vec<GitSync>,

        /// for internal use only - not for production use.
        #[serde(default)]
        pub expose_config: bool,

        /// Whether to load example DAGs or not; defaults to false. The examples are used in the
        /// [getting started guide](DOCS_BASE_URL_PLACEHOLDER/airflow/getting_started/).
        #[serde(default)]
        pub load_examples: bool,

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
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AirflowClusterStatus {
    #[serde(default)]
    pub conditions: Vec<ClusterCondition>,
}

impl HasStatusCondition for v1alpha1::AirflowCluster {
    fn conditions(&self) -> Vec<ClusterCondition> {
        match &self.status {
            Some(status) => status.conditions.clone(),
            None => vec![],
        }
    }
}

impl v1alpha1::AirflowCluster {
    /// the worker role will not be returned if airflow provisions pods as needed (i.e. when
    /// the kubernetes executor is specified)
    pub fn get_role(&self, role: &AirflowRole) -> Option<Role<AirflowConfigFragment>> {
        match role {
            AirflowRole::Webserver => self.spec.webservers.to_owned(),
            AirflowRole::Scheduler => self.spec.schedulers.to_owned(),
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
        let mut mounts = self.spec.cluster_config.volume_mounts.clone();
        if self.git_sync().is_some() {
            mounts.push(VolumeMount {
                name: GIT_SYNC_CONTENT.into(),
                mount_path: GIT_SYNC_DIR.into(),
                ..VolumeMount::default()
            });
        }
        mounts
    }

    pub fn git_sync(&self) -> Option<&GitSync> {
        let dags_git_sync = &self.spec.cluster_config.dags_git_sync;
        // dags_git_sync is a list but only the first element is considered
        // (this avoids a later breaking change when all list elements are processed)
        if dags_git_sync.len() > 1 {
            tracing::warn!(
                "{:?} git-sync elements: only first will be considered...",
                dags_git_sync.len()
            );
        }
        dags_git_sync.first()
    }

    /// The name of the role-level load-balanced Kubernetes `Service`
    pub fn node_role_service_name(&self) -> Option<String> {
        self.metadata.name.clone()
    }

    /// Retrieve and merge resource configs for role and role groups
    pub fn merged_config(
        &self,
        role: &AirflowRole,
        rolegroup_ref: &RoleGroupRef<v1alpha1::AirflowCluster>,
    ) -> Result<AirflowConfig, Error> {
        // Initialize the result with all default values as baseline
        let conf_defaults = AirflowConfig::default_config(&self.name_any(), role);

        let role = match role {
            AirflowRole::Webserver => {
                self.spec
                    .webservers
                    .as_ref()
                    .context(UnknownAirflowRoleSnafu {
                        role: role.to_string(),
                        roles: AirflowRole::roles(),
                    })?
            }
            AirflowRole::Worker => {
                if let AirflowExecutor::CeleryExecutor { config } = &self.spec.executor {
                    config
                } else {
                    return Err(Error::NoRoleForExecutorFailure);
                }
            }
            AirflowRole::Scheduler => {
                self.spec
                    .schedulers
                    .as_ref()
                    .context(UnknownAirflowRoleSnafu {
                        role: role.to_string(),
                        roles: AirflowRole::roles(),
                    })?
            }
        };

        // Retrieve role resource config
        let mut conf_role = role.config.config.to_owned();

        // Retrieve rolegroup specific resource config
        let mut conf_rolegroup = role
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

    pub fn merged_listener_class(
        &self,
        role: &AirflowRole,
        rolegroup_name: &String,
    ) -> Result<Option<SupportedListenerClasses>, Error> {
        let listener_class_default = Some(SupportedListenerClasses::ClusterInternal);

        let role = match role {
            AirflowRole::Webserver => {
                self.spec
                    .webservers
                    .as_ref()
                    .context(UnknownAirflowRoleSnafu {
                        role: role.to_string(),
                        roles: AirflowRole::roles(),
                    })?
            }
            AirflowRole::Worker => {
                if let AirflowExecutor::CeleryExecutor { config } = &self.spec.executor {
                    config
                } else {
                    return Err(Error::NoRoleForExecutorFailure);
                }
            }
            AirflowRole::Scheduler => {
                self.spec
                    .schedulers
                    .as_ref()
                    .context(UnknownAirflowRoleSnafu {
                        role: role.to_string(),
                        roles: AirflowRole::roles(),
                    })?
            }
        };

        let mut listener_class_role = role.config.config.listener_class.to_owned();
        let mut listener_class_rolegroup = role
            .role_groups
            .get(rolegroup_name)
            .map(|rg| rg.config.config.listener_class.clone())
            .unwrap_or_default();
        listener_class_role.merge(&listener_class_default);
        listener_class_rolegroup.merge(&listener_class_role);
        tracing::debug!("Merged listener-class: {:?}", listener_class_rolegroup);
        Ok(listener_class_rolegroup)
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

    pub fn rolegroup_ref(
        &self,
        role_name: impl Into<String>,
        group_name: impl Into<String>,
    ) -> RoleGroupRef<v1alpha1::AirflowCluster> {
        RoleGroupRef {
            cluster: ObjectRef::from_obj(self),
            role: role_name.into(),
            role_group: group_name.into(),
        }
    }

    pub fn rolegroup_ref_and_replicas(
        &self,
        role: &AirflowRole,
    ) -> Vec<(RoleGroupRef<v1alpha1::AirflowCluster>, u16)> {
        match role {
            AirflowRole::Webserver => self
                .spec
                .webservers
                .iter()
                .flat_map(|role| &role.role_groups)
                // Order rolegroups consistently, to avoid spurious downstream rewrites
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .filter(|(rolegroup_name, _)| {
                    self.resolved_listener_class_discoverable(role, rolegroup_name)
                })
                .map(|(rolegroup_name, role_group)| {
                    (
                        self.rolegroup_ref(role.to_string(), rolegroup_name),
                        role_group.replicas.unwrap_or_default(),
                    )
                })
                .collect(),
            AirflowRole::Scheduler => self
                .spec
                .schedulers
                .iter()
                .flat_map(|role| &role.role_groups)
                // Order rolegroups consistently, to avoid spurious downstream rewrites
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .filter(|(rolegroup_name, _)| {
                    self.resolved_listener_class_discoverable(role, rolegroup_name)
                })
                .map(|(rolegroup_name, role_group)| {
                    (
                        self.rolegroup_ref(AirflowRole::Scheduler.to_string(), rolegroup_name),
                        role_group.replicas.unwrap_or_default(),
                    )
                })
                .collect(),
            AirflowRole::Worker => {
                if let AirflowExecutor::CeleryExecutor { config } = &self.spec.executor {
                    config
                        .role_groups
                        .iter()
                        // Order rolegroups consistently, to avoid spurious downstream rewrites
                        .collect::<BTreeMap<_, _>>()
                        .into_iter()
                        .filter(|(rolegroup_name, _)| {
                            self.resolved_listener_class_discoverable(role, rolegroup_name)
                        })
                        .map(|(rolegroup_name, role_group)| {
                            (
                                self.rolegroup_ref(AirflowRole::Worker.to_string(), rolegroup_name),
                                role_group.replicas.unwrap_or_default(),
                            )
                        })
                        .collect()
                } else {
                    vec![]
                }
            }
        }
    }

    fn resolved_listener_class_discoverable(
        &self,
        role: &AirflowRole,
        rolegroup_name: &&String,
    ) -> bool {
        if let Ok(Some(listener_class)) = self.merged_listener_class(role, rolegroup_name) {
            listener_class.discoverable()
        } else {
            // merged_listener_class returns an error is one of the roles was not found:
            // all roles are mandatory for airflow to work, but a missing role will by
            // definition not have a listener class
            false
        }
    }

    pub fn pod_refs(&self, role: &AirflowRole) -> Result<Vec<PodRef>, Error> {
        let ns = self.metadata.namespace.clone().context(NoNamespaceSnafu)?;
        let rolegroup_ref_and_replicas = self.rolegroup_ref_and_replicas(role);

        Ok(rolegroup_ref_and_replicas
            .iter()
            .flat_map(|(rolegroup_ref, replicas)| {
                let ns = ns.clone();
                (0..*replicas).map(move |i| PodRef {
                    namespace: ns.clone(),
                    role_group_service_name: rolegroup_ref.object_name(),
                    pod_name: format!("{}-{}", rolegroup_ref.object_name(), i),
                    ports: HashMap::from([
                        (HTTP_PORT_NAME.to_owned(), HTTP_PORT),
                        (METRICS_PORT_NAME.to_owned(), METRICS_PORT),
                    ]),
                    fqdn_override: None,
                })
            })
            .collect())
    }

    pub async fn listener_refs(
        &self,
        client: &stackable_operator::client::Client,
        role: &AirflowRole,
    ) -> Result<Vec<PodRef>, Error> {
        let pod_refs = self.pod_refs(role)?;

        tracing::debug!("Pod references for role {role}: {:#?}", pod_refs);
        get_persisted_listener_podrefs(client, pod_refs, LISTENER_VOLUME_NAME)
            .await
            .context(ListenerPodRefSnafu)
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

#[derive(Clone, Debug, Default, Display, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub enum SupportedListenerClasses {
    #[default]
    #[serde(rename = "cluster-internal")]
    #[strum(serialize = "cluster-internal")]
    ClusterInternal,

    #[serde(rename = "external-unstable")]
    #[strum(serialize = "external-unstable")]
    ExternalUnstable,

    #[serde(rename = "external-stable")]
    #[strum(serialize = "external-stable")]
    ExternalStable,
}

impl Atomic for SupportedListenerClasses {}

impl SupportedListenerClasses {
    pub fn discoverable(&self) -> bool {
        match self {
            SupportedListenerClasses::ClusterInternal => false,
            SupportedListenerClasses::ExternalUnstable => true,
            SupportedListenerClasses::ExternalStable => true,
        }
    }
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
        auth_config: &AirflowClientAuthenticationDetailsResolved,
    ) -> Vec<String> {
        let mut command = vec![
            format!(
                "cp -RL {CONFIG_PATH}/{AIRFLOW_CONFIG_FILENAME} {AIRFLOW_HOME}/{AIRFLOW_CONFIG_FILENAME}"
            ),
            // graceful shutdown part
            COMMON_BASH_TRAP_FUNCTIONS.to_string(),
            remove_vector_shutdown_file_command(STACKABLE_LOG_DIR),
        ];

        match &self {
            AirflowRole::Webserver => {
                // Getting auth commands for AuthClass
                command.extend(Self::authentication_start_commands(auth_config));
                command.extend(vec![
                    "prepare_signal_handlers".to_string(),
                    format!("containerdebug --output={STACKABLE_LOG_DIR}/containerdebug-state.json --loop &"),
                    "airflow webserver &".to_string(),
                ]);
            }

            AirflowRole::Scheduler => command.extend(vec![
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
                "prepare_signal_handlers".to_string(),
                format!(
                    "containerdebug --output={STACKABLE_LOG_DIR}/containerdebug-state.json --loop &"
                ),
                "airflow scheduler &".to_string(),
            ]),
            AirflowRole::Worker => command.extend(vec![
                "prepare_signal_handlers".to_string(),
                format!(
                    "containerdebug --output={STACKABLE_LOG_DIR}/containerdebug-state.json --loop &"
                ),
                "airflow celery worker &".to_string(),
            ]),
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
        }
    }

    pub fn roles() -> Vec<String> {
        let mut roles = vec![];
        for role in Self::iter() {
            roles.push(role.to_string())
        }
        roles
    }
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

    /// This field controls which [ListenerClass](DOCS_BASE_URL_PLACEHOLDER/listener-operator/listenerclass.html) is used to expose the airflow services.
    #[serde(default)]
    pub listener_class: SupportedListenerClasses,
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
                AirflowRole::Webserver | AirflowRole::Scheduler => {
                    DEFAULT_AIRFLOW_GRACEFUL_SHUTDOWN_TIMEOUT
                }
                AirflowRole::Worker => DEFAULT_WORKER_GRACEFUL_SHUTDOWN_TIMEOUT,
            }),
            listener_class: Some(SupportedListenerClasses::ClusterInternal),
        }
    }
}

impl Configuration for AirflowConfigFragment {
    type Configurable = v1alpha1::AirflowCluster;

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
                min: Some(Quantity("500m".into())),
                max: Some(Quantity("2".into())),
            },
            MemoryLimitsFragment {
                limit: Some(Quantity("2Gi".into())),
                runtime_limits: NoRuntimeLimitsFragment {},
            },
        ),
        AirflowRole::Webserver => (
            CpuLimitsFragment {
                min: Some(Quantity("500m".into())),
                max: Some(Quantity("2".into())),
            },
            MemoryLimitsFragment {
                limit: Some(Quantity("3Gi".into())),
                runtime_limits: NoRuntimeLimitsFragment {},
            },
        ),
        AirflowRole::Scheduler => (
            CpuLimitsFragment {
                min: Some(Quantity("500m".to_owned())),
                max: Some(Quantity("2".to_owned())),
            },
            MemoryLimitsFragment {
                limit: Some(Quantity("512Mi".to_owned())),
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

    use crate::v1alpha1::AirflowCluster;

    #[test]
    fn test_cluster_config() {
        let cluster = "
        apiVersion: airflow.stackable.tech/v1alpha1
        kind: AirflowCluster
        metadata:
          name: airflow
        spec:
          image:
            productVersion: 2.10.4
          clusterConfig:
            loadExamples: true
            exposeConfig: true
            credentialsSecret: simple-airflow-credentials
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

        let resolved_airflow_image: ResolvedProductImage =
            cluster.spec.image.resolve("airflow", "0.0.0-dev");

        assert_eq!("2.10.4", &resolved_airflow_image.product_version);

        assert_eq!("KubernetesExecutor", cluster.spec.executor.to_string());
        assert!(cluster.spec.cluster_config.load_examples);
        assert!(cluster.spec.cluster_config.expose_config);
    }
}
