pub mod airflowdb;

use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::commons::product_image_selection::ProductImage;
use stackable_operator::{
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
    role_utils::{Role, RoleGroupRef},
    schemars::{self, JsonSchema},
};
use std::collections::BTreeMap;
use strum::{Display, EnumIter, EnumString, IntoEnumIterator};

pub const APP_NAME: &str = "airflow";
pub const OPERATOR_NAME: &str = "airflow.stackable.tech";
pub const CONFIG_PATH: &str = "/stackable/app/config";
pub const AIRFLOW_HOME: &str = "/stackable/airflow";
pub const AIRFLOW_CONFIG_FILENAME: &str = "webserver_config.py";

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
    /// Emergency stop button, if `true` then all pods are stopped without affecting configuration (as setting `replicas` to `0` would)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stopped: Option<bool>,
    /// The Airflow image to use
    pub image: ProductImage,
    pub credentials_secret: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub load_examples: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expose_config: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volumes: Option<Vec<Volume>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_mounts: Option<Vec<VolumeMount>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authentication_config: Option<AirflowClusterAuthenticationConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub webservers: Option<Role<AirflowConfig>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedulers: Option<Role<AirflowConfig>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workers: Option<Role<AirflowConfig>>,
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
    pub fn get_role(&self, role: AirflowRole) -> &Option<Role<AirflowConfig>> {
        match role {
            AirflowRole::Webserver => &self.spec.webservers,
            AirflowRole::Scheduler => &self.spec.schedulers,
            AirflowRole::Worker => &self.spec.workers,
        }
    }

    pub fn volumes(&self) -> Vec<Volume> {
        let tmp = self.spec.volumes.as_ref();
        tmp.iter().flat_map(|v| v.iter()).cloned().collect()
    }

    pub fn volume_mounts(&self) -> Vec<VolumeMount> {
        let tmp = self.spec.volume_mounts.as_ref();
        tmp.iter().flat_map(|v| v.iter()).cloned().collect()
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

#[derive(Clone, Debug, Deserialize, Default, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AirflowConfig {
    pub resources: Option<ResourcesFragment<AirflowStorageConfig, NoRuntimeLimits>>,
}

impl AirflowConfig {
    pub const CREDENTIALS_SECRET_PROPERTY: &'static str = "credentialsSecret";

    fn default_resources() -> ResourcesFragment<AirflowStorageConfig, NoRuntimeLimits> {
        ResourcesFragment {
            cpu: CpuLimitsFragment {
                min: Some(Quantity("200m".to_owned())),
                max: Some(Quantity("4".to_owned())),
            },
            memory: MemoryLimitsFragment {
                limit: Some(Quantity("2Gi".to_owned())),
                runtime_limits: NoRuntimeLimitsFragment {},
            },
            storage: AirflowStorageConfigFragment {},
        }
    }
}

impl Configuration for AirflowConfig {
    type Configurable = AirflowCluster;

    fn compute_env(
        &self,
        cluster: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        Ok([(
            Self::CREDENTIALS_SECRET_PROPERTY.to_string(),
            Some(cluster.spec.credentials_secret.clone()),
        )]
        .into())
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
pub struct AirflowClusterStatus {}

impl AirflowCluster {
    /// The name of the role-level load-balanced Kubernetes `Service`
    pub fn node_role_service_name(&self) -> Option<String> {
        self.metadata.name.clone()
    }

    /// Retrieve and merge resource configs for role and role groups
    pub fn resolve_resource_config_for_role_and_rolegroup(
        &self,
        role: &AirflowRole,
        rolegroup_ref: &RoleGroupRef<AirflowCluster>,
    ) -> Result<Resources<AirflowStorageConfig, NoRuntimeLimits>, Error> {
        // Initialize the result with all default values as baseline
        let conf_defaults = AirflowConfig::default_resources();

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
        let mut conf_role: ResourcesFragment<AirflowStorageConfig, NoRuntimeLimits> =
            role.config.config.resources.clone().unwrap_or_default();

        // Retrieve rolegroup specific resource config
        let mut conf_rolegroup: ResourcesFragment<AirflowStorageConfig, NoRuntimeLimits> = role
            .role_groups
            .get(&rolegroup_ref.role_group)
            .and_then(|rg| rg.config.config.resources.clone())
            .unwrap_or_default();

        // Merge more specific configs into default config
        // Hierarchy is:
        // 1. RoleGroup
        // 2. Role
        // 3. Default
        conf_role.merge(&conf_defaults);
        conf_rolegroup.merge(&conf_role);

        tracing::debug!("Merged resource config: {:?}", conf_rolegroup);
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
            productVersion: 2.2.4
            stackableVersion: 0.5.0
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

        assert_eq!("2.2.4", &resolved_airflow_db_image.product_version);
        assert_eq!("2.2.4", &resolved_airflow_image.product_version);
        assert_eq!(
            "KubernetesExecutor",
            cluster.spec.executor.unwrap_or_default()
        );
        assert!(cluster.spec.load_examples.unwrap_or(false));
        assert!(cluster.spec.expose_config.unwrap_or(false));
    }
}
