pub mod affinity;
pub mod airflowdb;

use crate::affinity::get_affinity;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::commons::affinity::StackableAffinity;
use stackable_operator::commons::product_image_selection::ProductImage;
use stackable_operator::kube::ResourceExt;
use stackable_operator::role_utils::RoleGroup;
use stackable_operator::{
    commons::cluster_operation::ClusterOperation,
    commons::resources::{
        CpuLimitsFragment, MemoryLimitsFragment, NoRuntimeLimits, NoRuntimeLimitsFragment,
        Resources, ResourcesFragment,
    },
    config::{fragment, fragment::Fragment, fragment::ValidationError, merge::Merge},
    k8s_openapi::{
        api::core::v1::{Volume, VolumeMount},
        apimachinery::pkg::api::resource::Quantity,
    },
    kube::CustomResource,
    labels::ObjectLabels,
    product_config::flask_app_config_writer::{FlaskAppConfigOptions, PythonType},
    product_config_utils::{ConfigError, Configuration},
    product_logging::{self, spec::Logging},
    role_utils::{Role, RoleGroupRef},
    schemars::{self, JsonSchema},
    status::condition::{ClusterCondition, HasStatusCondition},
};

use std::collections::BTreeMap;
use std::ops::Deref;
use strum::{Display, EnumIter, EnumString, IntoEnumIterator};

pub const AIRFLOW_UID: i64 = 1000;
pub const APP_NAME: &str = "airflow";
pub const OPERATOR_NAME: &str = "airflow.stackable.tech";
pub const CONFIG_PATH: &str = "/stackable/app/config";
pub const STACKABLE_LOG_DIR: &str = "/stackable/log";
pub const LOG_CONFIG_DIR: &str = "/stackable/app/log_config";
pub const AIRFLOW_HOME: &str = "/stackable/airflow";
pub const AIRFLOW_CONFIG_FILENAME: &str = "webserver_config.py";
pub const GIT_SYNC_DIR: &str = "/stackable/app/git";
pub const GIT_CONTENT: &str = "content-from-git";
pub const GIT_ROOT: &str = "/tmp/git";
pub const GIT_LINK: &str = "current";
pub const GIT_SYNC_NAME: &str = "gitsync";

const GIT_SYNC_DEPTH: u8 = 1u8;
const GIT_SYNC_WAIT: u16 = 20u16;

pub const LOG_VOLUME_SIZE_IN_MIB: u32 = 10;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Unknown Airflow role found {role}. Should be one of {roles:?}"))]
    UnknownAirflowRole { role: String, roles: Vec<String> },
    #[snafu(display("fragment validation failure"))]
    FragmentValidationFailure { source: ValidationError },
}

#[derive(Display, EnumIter, EnumString)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum AirflowConfigOptions {
    AuthType,
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
}

impl FlaskAppConfigOptions for AirflowConfigOptions {
    fn python_type(&self) -> PythonType {
        match self {
            AirflowConfigOptions::AuthType => PythonType::Expression,
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
        }
    }
}

#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "airflow.stackable.tech",
    version = "v1alpha1",
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
)]
#[serde(rename_all = "camelCase")]
pub struct AirflowClusterSpec {
    /// The Airflow image to use
    pub image: ProductImage,
    /// Global cluster configuration that applies to all roles and role groups
    #[serde(default)]
    pub cluster_config: AirflowClusterConfig,
    /// Cluster operations like pause reconciliation or cluster stop.
    #[serde(default)]
    pub cluster_operation: ClusterOperation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub webservers: Option<Role<AirflowConfigFragment>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedulers: Option<Role<AirflowConfigFragment>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workers: Option<Role<AirflowConfigFragment>>,
}

#[derive(Clone, Deserialize, Debug, Default, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AirflowClusterConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authentication_config: Option<AirflowClusterAuthenticationConfig>,
    pub credentials_secret: String,
    #[serde(default)]
    pub dags_git_sync: Vec<GitSync>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database_initialization: Option<airflowdb::AirflowDbConfigFragment>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expose_config: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub load_examples: Option<bool>,
    /// In the future this setting will control, which ListenerClass <https://docs.stackable.tech/home/stable/listener-operator/listenerclass.html>
    /// will be used to expose the service.
    /// Currently only a subset of the ListenerClasses are supported by choosing the type of the created Services
    /// by looking at the ListenerClass name specified,
    /// In a future release support for custom ListenerClasses will be introduced without a breaking change:
    ///
    /// * cluster-internal: Use a ClusterIP service
    ///
    /// * external-unstable: Use a NodePort service
    ///
    /// * external-stable: Use a LoadBalancer service
    #[serde(default)]
    pub listener_class: CurrentlySupportedListenerClasses,
    /// Name of the Vector aggregator discovery ConfigMap.
    /// It must contain the key `ADDRESS` with the address of the Vector aggregator.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_aggregator_config_map_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volumes: Option<Vec<Volume>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_mounts: Option<Vec<VolumeMount>>,
}

// TODO: Temporary solution until listener-operator is finished
#[derive(Clone, Debug, Default, Display, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub enum CurrentlySupportedListenerClasses {
    #[default]
    #[serde(rename = "cluster-internal")]
    ClusterInternal,
    #[serde(rename = "external-unstable")]
    ExternalUnstable,
    #[serde(rename = "external-stable")]
    ExternalStable,
}

impl CurrentlySupportedListenerClasses {
    pub fn k8s_service_type(&self) -> String {
        match self {
            CurrentlySupportedListenerClasses::ClusterInternal => "ClusterIP".to_string(),
            CurrentlySupportedListenerClasses::ExternalUnstable => "NodePort".to_string(),
            CurrentlySupportedListenerClasses::ExternalStable => "LoadBalancer".to_string(),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GitSync {
    pub repo: String,
    pub branch: Option<String>,
    pub git_folder: Option<String>,
    pub depth: Option<u8>,
    pub wait: Option<u16>,
    pub credentials_secret: Option<String>,
    pub git_sync_conf: Option<BTreeMap<String, String>>,
}

impl GitSync {
    pub fn get_args(&self) -> Vec<String> {
        let mut args: Vec<String> = vec![];
        args.extend(vec![
            "/stackable/git-sync".to_string(),
            format!("--repo={}", self.repo.clone()),
            format!(
                "--branch={}",
                self.branch.clone().unwrap_or_else(|| "main".to_string())
            ),
            format!("--depth={}", self.depth.unwrap_or(GIT_SYNC_DEPTH)),
            format!("--wait={}", self.wait.unwrap_or(GIT_SYNC_WAIT)),
            format!("--dest={GIT_LINK}"),
            format!("--root={GIT_ROOT}"),
            format!("--git-config=safe.directory:{GIT_ROOT}"),
        ]);
        if let Some(git_sync_conf) = self.git_sync_conf.as_ref() {
            for (key, value) in git_sync_conf {
                // config options that are internal details have
                // constant values and will be ignored here
                if key.eq_ignore_ascii_case("--dest")
                    || key.eq_ignore_ascii_case("--root")
                    || key.eq_ignore_ascii_case("--git-config")
                {
                    tracing::warn!("Config option {:?} will be ignored...", key);
                } else {
                    args.push(format!("{key}={value}"));
                }
            }
        }
        args
    }
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AirflowClusterAuthenticationConfig {
    /// Name of the AuthenticationClass used to authenticate the users.
    /// At the moment only LDAP is supported.
    /// If not specified the default authentication (AUTH_DB) will be used.
    pub authentication_class: Option<String>,

    /// Allow users who are not already in the FAB DB.
    /// Gets mapped to `AUTH_USER_REGISTRATION`
    #[serde(default = "default_user_registration")]
    pub user_registration: bool,

    /// This role will be given in addition to any AUTH_ROLES_MAPPING.
    /// Gets mapped to `AUTH_USER_REGISTRATION_ROLE`
    #[serde(default = "default_user_registration_role")]
    pub user_registration_role: String,

    /// If we should replace ALL the user's roles each login, or only on registration.
    /// Gets mapped to `AUTH_ROLES_SYNC_AT_LOGIN`
    #[serde(default = "default_sync_roles_at")]
    pub sync_roles_at: LdapRolesSyncMoment,
}

pub fn default_user_registration() -> bool {
    true
}

pub fn default_user_registration_role() -> String {
    "Public".to_string()
}

/// Matches Flask's default mode of syncing at registration
pub fn default_sync_roles_at() -> LdapRolesSyncMoment {
    LdapRolesSyncMoment::Registration
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub enum LdapRolesSyncMoment {
    Registration,
    Login,
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
    pub fn get_commands(&self) -> Vec<String> {
        let copy_config = format!(
            "cp -RL {CONFIG_PATH}/{AIRFLOW_CONFIG_FILENAME} \
            {AIRFLOW_HOME}/{AIRFLOW_CONFIG_FILENAME}"
        );
        match &self {
            AirflowRole::Webserver => vec![copy_config, "airflow webserver".to_string()],
            AirflowRole::Scheduler => vec![copy_config, "airflow scheduler".to_string()],
            AirflowRole::Worker => vec![copy_config, "airflow celery worker".to_string()],
        }
    }

    /// Will be used to expose service ports and - by extension - which roles should be
    /// created as services.
    pub fn get_http_port(&self) -> Option<u16> {
        match &self {
            AirflowRole::Webserver => Some(8080),
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

impl AirflowCluster {
    pub fn get_role(&self, role: AirflowRole) -> &Option<Role<AirflowConfigFragment>> {
        match role {
            AirflowRole::Webserver => &self.spec.webservers,
            AirflowRole::Scheduler => &self.spec.schedulers,
            AirflowRole::Worker => &self.spec.workers,
        }
    }

    /// this will extract a `Vec<Volume>` from `Option<Vec<Volume>>`
    pub fn volumes(&self) -> Vec<Volume> {
        let tmp = self.spec.cluster_config.volumes.as_ref();
        tmp.iter().flat_map(|v| v.deref().clone()).collect()
    }

    pub fn volume_mounts(&self) -> Vec<VolumeMount> {
        let tmp = self.spec.cluster_config.volume_mounts.as_ref();
        let mut mounts: Vec<VolumeMount> = tmp.iter().flat_map(|v| v.deref().clone()).collect();
        if self.git_sync().is_some() {
            mounts.push(VolumeMount {
                name: GIT_CONTENT.into(),
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
}

impl AirflowConfig {
    pub const CREDENTIALS_SECRET_PROPERTY: &'static str = "credentialsSecret";
    pub const GIT_CREDENTIALS_SECRET_PROPERTY: &'static str = "gitCredentialsSecret";

    fn default_config(cluster_name: &str, role: &AirflowRole) -> AirflowConfigFragment {
        AirflowConfigFragment {
            resources: ResourcesFragment {
                cpu: CpuLimitsFragment {
                    min: Some(Quantity("200m".to_owned())),
                    max: Some(Quantity("4".to_owned())),
                },
                memory: MemoryLimitsFragment {
                    limit: Some(Quantity("2Gi".to_owned())),
                    runtime_limits: NoRuntimeLimitsFragment {},
                },
                storage: AirflowStorageConfigFragment {},
            },
            logging: product_logging::spec::default_logging(),
            affinity: get_affinity(cluster_name, role),
        }
    }
}

impl Configuration for AirflowConfigFragment {
    type Configurable = AirflowCluster;

    fn compute_env(
        &self,
        cluster: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let mut env: BTreeMap<String, Option<String>> = BTreeMap::new();
        env.insert(
            AirflowConfig::CREDENTIALS_SECRET_PROPERTY.to_string(),
            Some(cluster.spec.cluster_config.credentials_secret.clone()),
        );
        if let Some(git_sync) = &cluster.git_sync() {
            if let Some(credentials_secret) = &git_sync.credentials_secret {
                env.insert(
                    AirflowConfig::GIT_CREDENTIALS_SECRET_PROPERTY.to_string(),
                    Some(credentials_secret.to_string()),
                );
            }
        }
        Ok(env)
    }

    fn compute_cli(
        &self,
        _cluster: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        Ok(BTreeMap::new())
    }

    fn compute_files(
        &self,
        _cluster: &Self::Configurable,
        _role_name: &str,
        _file: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        Ok(BTreeMap::new())
    }
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AirflowClusterStatus {
    #[serde(default)]
    pub conditions: Vec<ClusterCondition>,
}

impl HasStatusCondition for AirflowCluster {
    fn conditions(&self) -> Vec<ClusterCondition> {
        match &self.status {
            Some(status) => status.conditions.clone(),
            None => vec![],
        }
    }
}

impl AirflowCluster {
    /// The name of the role-level load-balanced Kubernetes `Service`
    pub fn node_role_service_name(&self) -> Option<String> {
        self.metadata.name.clone()
    }

    /// Retrieve and merge resource configs for role and role groups
    pub fn merged_config(
        &self,
        role: &AirflowRole,
        rolegroup_ref: &RoleGroupRef<AirflowCluster>,
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
            AirflowRole::Worker => self
                .spec
                .workers
                .as_ref()
                .context(UnknownAirflowRoleSnafu {
                    role: role.to_string(),
                    roles: AirflowRole::roles(),
                })?,
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

        if let Some(RoleGroup {
            selector: Some(selector),
            ..
        }) = role.role_groups.get(&rolegroup_ref.role_group)
        {
            // Migrate old `selector` attribute, see ADR 26 affinities.
            // TODO Can be removed after support for the old `selector` field is dropped.
            #[allow(deprecated)]
            conf_rolegroup.affinity.add_legacy_selector(selector);
        }

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

/// A reference to a [`AirflowCluster`]
#[derive(Clone, Default, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AirflowClusterRef {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

#[cfg(test)]
mod tests {
    use crate::airflowdb::AirflowDB;
    use crate::AirflowCluster;
    use stackable_operator::commons::product_image_selection::ResolvedProductImage;

    #[test]
    fn test_cluster_config() {
        let cluster: AirflowCluster = serde_yaml::from_str::<AirflowCluster>(
            "
        apiVersion: airflow.stackable.tech/v1alpha1
        kind: AirflowCluster
        metadata:
          name: airflow
        spec:
          image:
            productVersion: 2.6.1
            stackableVersion: 0.0.0-dev
          clusterConfig:
            executor: KubernetesExecutor
            loadExamples: true
            exposeConfig: true
            credentialsSecret: simple-airflow-credentials
          webservers:
            roleGroups:
              default:
                config: {}
          workers:
            roleGroups:
              default:
                config: {}
          schedulers:
            roleGroups:
              default:
                config: {}
          ",
        )
        .unwrap();

        let resolved_airflow_image: ResolvedProductImage = cluster.spec.image.resolve("airflow");

        let airflow_db = AirflowDB::for_airflow(&cluster, &resolved_airflow_image).unwrap();
        let resolved_airflow_db_image: ResolvedProductImage =
            airflow_db.spec.image.resolve("airflow");

        assert_eq!("2.6.1", &resolved_airflow_db_image.product_version);
        assert_eq!("2.6.1", &resolved_airflow_image.product_version);
        assert_eq!(
            "KubernetesExecutor",
            cluster.spec.cluster_config.executor.unwrap_or_default()
        );
        assert!(cluster.spec.cluster_config.load_examples.unwrap_or(false));
        assert!(cluster.spec.cluster_config.expose_config.unwrap_or(false));
    }

    #[test]
    fn test_git_sync() {
        let cluster: AirflowCluster = serde_yaml::from_str::<AirflowCluster>(
            "
        apiVersion: airflow.stackable.tech/v1alpha1
        kind: AirflowCluster
        metadata:
          name: airflow
        spec:
          image:
            productVersion: 2.6.1
            stackableVersion: 0.0.0-dev
          clusterConfig:
            executor: CeleryExecutor
            loadExamples: false
            exposeConfig: false
            credentialsSecret: simple-airflow-credentials
            dagsGitSync:
              - name: git-sync
                repo: https://github.com/stackabletech/airflow-operator
                branch: feat/git-sync
                wait: 20
                gitSyncConf: {}
                gitFolder: tests/templates/kuttl/mount-dags-gitsync/dags
          webservers:
            roleGroups:
              default:
                config: {}
          workers:
            roleGroups:
              default:
                config: {}
          schedulers:
            roleGroups:
              default:
                config: {}
          ",
        )
        .unwrap();

        assert!(cluster.git_sync().is_some(), "git_sync was not Some!");
        assert_eq!(
            Some("tests/templates/kuttl/mount-dags-gitsync/dags".to_string()),
            cluster.git_sync().unwrap().git_folder
        );
    }

    #[test]
    fn test_git_sync_config() {
        let cluster: AirflowCluster = serde_yaml::from_str::<AirflowCluster>(
            "
        apiVersion: airflow.stackable.tech/v1alpha1
        kind: AirflowCluster
        metadata:
          name: airflow
        spec:
          image:
            productVersion: 2.6.1
            stackableVersion: 0.0.0-dev
          clusterConfig:
            executor: CeleryExecutor
            loadExamples: false
            exposeConfig: false
            credentialsSecret: simple-airflow-credentials
            dagsGitSync:
              - name: git-sync
                repo: https://github.com/stackabletech/airflow-operator
                branch: feat/git-sync
                wait: 20
                gitSyncConf:
                  --rev: c63921857618a8c392ad757dda13090fff3d879a
                gitFolder: tests/templates/kuttl/mount-dags-gitsync/dags
          webservers:
            roleGroups:
              default:
                config: {}
          workers:
            roleGroups:
              default:
                config: {}
          schedulers:
            roleGroups:
              default:
                config: {}
          ",
        )
        .unwrap();

        assert!(cluster
            .git_sync()
            .unwrap()
            .get_args()
            .iter()
            .any(|c| c == "--rev=c63921857618a8c392ad757dda13090fff3d879a"));
    }
}
