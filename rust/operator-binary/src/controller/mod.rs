use std::{collections::BTreeMap, marker::PhantomData, str::FromStr};

use stackable_operator::{
    commons::{
        affinity::StackableAffinity,
        pdb::PdbConfig,
        product_image_selection::ResolvedProductImage,
        resources::{NoRuntimeLimits, Resources},
    },
    constant,
    crd::{git_sync, listener},
    database_connections::{
        TemplatingMechanism,
        drivers::{
            celery::CeleryDatabaseConnectionDetails,
            sqlalchemy::SqlAlchemyDatabaseConnectionDetails,
        },
    },
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, PodTemplateSpec, Service, ServiceAccount, Volume, VolumeMount},
        policy::v1::PodDisruptionBudget,
        rbac::v1::RoleBinding,
    },
    kube::{Resource, ResourceExt, api::ObjectMeta},
    product_logging::spec::ContainerLogConfig,
    shared::time::Duration,
    v2::{
        HasName, HasUid, NameIsValidLabelValue,
        builder::pod::container::EnvVarSet,
        product_logging::framework::{ValidatedContainerLogConfigChoice, VectorContainerLogConfig},
        role_group_utils::ResourceNames,
        role_utils,
        types::{
            kubernetes::{
                ConfigMapName, ListenerClassName, ListenerName, NamespaceName, SecretName, Uid,
            },
            operator::{
                ClusterName, ControllerName, OperatorName, ProductName, ProductVersion,
                RoleGroupName, RoleName,
            },
        },
    },
};

use crate::{
    airflow_controller::AIRFLOW_CONTROLLER_NAME,
    crd::{
        AIRFLOW_OPERATOR_NAME, APP_NAME, AirflowConfig, AirflowConfigOverrides, AirflowExecutor,
        AirflowRole, AirflowStorageConfig, ExecutorConfig,
        authentication::AirflowClientAuthenticationDetailsResolved,
        authorization::AirflowAuthorizationResolved,
        databases::{
            CeleryBrokerConnection, CeleryResultBackendConnection, MetadataDatabaseConnection,
        },
        trusted_proxies::TrustedProxy,
        v1alpha2,
    },
};

pub mod apply;
pub mod build;
pub mod dereference;
pub mod update_status;
pub mod validate;

constant!(PRODUCT_NAME: ProductName = APP_NAME);
constant!(OPERATOR_NAME: OperatorName = AIRFLOW_OPERATOR_NAME);
constant!(CONTROLLER_NAME: ControllerName = AIRFLOW_CONTROLLER_NAME);

/// Marker for prepared Kubernetes resources which are not applied yet.
pub struct Prepared;
/// Marker for applied Kubernetes resources.
pub struct Applied;

/// Every Kubernetes resource produced by the build step.
///
/// `T` is a marker that indicates if these resources are only [`Prepared`] or already [`Applied`].
/// The marker is useful e.g. to ensure that the cluster status is updated based on the applied
/// resources.
pub struct KubernetesResources<T> {
    pub stateful_sets: Vec<StatefulSet>,
    pub services: Vec<Service>,
    pub listeners: Vec<listener::v1alpha1::Listener>,
    pub config_maps: Vec<ConfigMap>,
    pub pod_disruption_budgets: Vec<PodDisruptionBudget>,
    pub service_accounts: Vec<ServiceAccount>,
    pub role_bindings: Vec<RoleBinding>,
    pub status: PhantomData<T>,
}
// Webserver role only — all non-Option
#[derive(Clone, Debug)]
pub struct ValidatedWebserverRoleConfig {
    pub pdb: PdbConfig,
    pub listener_class: ListenerClassName,
    pub group_listener_name: ListenerName,
    pub trusted_proxies: Vec<TrustedProxy>,
}

// Other roles: scheduler, worker, dagprocessor, triggerer
#[derive(Clone, Debug)]
pub struct ValidatedRoleConfig {
    pub pdb: PdbConfig,
}

/// Per-rolegroup configuration: the merged CRD config plus overrides.
pub type AirflowRoleGroupConfig = stackable_operator::v2::role_utils::RoleGroupConfig<
    ValidatedAirflowConfig,
    stackable_operator::v2::role_utils::GenericCommonConfig,
    AirflowConfigOverrides,
>;

/// A validated, merged Airflow role-group config: the merged [`AirflowConfig`] with its raw
/// `logging` replaced by the up-front-validated [`ValidatedLogging`].
pub struct ValidatedAirflowConfig {
    pub resources: Resources<AirflowStorageConfig, NoRuntimeLimits>,
    pub logging: ValidatedLogging,
    pub affinity: StackableAffinity,
    pub graceful_shutdown_timeout: Option<Duration>,
    pub git_sync_resources: git_sync::v1alpha2::GitSyncResources,
}

impl ValidatedAirflowConfig {
    /// Builds the validated config from the merged [`AirflowConfig`], swapping in the
    /// already-validated logging.
    pub(crate) fn from_merged(
        merged: AirflowConfig,
        logging: ValidatedLogging,
        git_sync_resources: git_sync::v1alpha2::GitSyncResources,
    ) -> Self {
        Self {
            resources: merged.resources,
            logging,
            affinity: merged.affinity,
            graceful_shutdown_timeout: merged.graceful_shutdown_timeout,
            git_sync_resources,
        }
    }

    /// Builds the validated config from the merged [`ExecutorConfig`] (Kubernetes-executor pod
    /// template), swapping in the already-validated logging. [`ExecutorConfig`] is field-identical
    /// to [`AirflowConfig`].
    pub(crate) fn from_merged_executor(
        merged: ExecutorConfig,
        logging: ValidatedLogging,
        git_sync_resources: git_sync::v1alpha2::GitSyncResources,
    ) -> Self {
        Self {
            resources: merged.resources,
            logging,
            affinity: merged.affinity,
            graceful_shutdown_timeout: merged.graceful_shutdown_timeout,
            git_sync_resources,
        }
    }
}

/// The validated Kubernetes-executor pod-template config, computed during validation so the build
/// step never merges or validates the raw cluster's executor config itself.
pub struct ValidatedExecutorTemplate {
    /// The merged + validated executor config (resources, affinity, logging, …).
    pub config: ValidatedAirflowConfig,
    /// Env-var overrides for the executor pod template (`spec.kubernetesExecutors.envOverrides`).
    pub env_overrides: EnvVarSet,
    /// Pod overrides for the executor pod template (`spec.kubernetesExecutors.podOverrides`).
    pub pod_overrides: PodTemplateSpec,
}

/// Validated logging configuration for the containers of a role-group (or Kubernetes-executor) Pod.
#[derive(Clone, Debug, PartialEq)]
pub struct ValidatedLogging {
    pub product_container: ValidatedContainerLogConfigChoice,
    pub vector_container: Option<VectorContainerLogConfig>,
    pub git_sync_container: ContainerLogConfig,
    pub enable_vector_agent: bool,
}

/// Cluster-wide configuration that applies to every role and role group.
///
/// Carries the dereferenced external references, so every downstream build step reads them from
/// here rather than from the raw cluster object.
pub struct ValidatedClusterConfig {
    pub executor: AirflowExecutor,
    /// The validated Kubernetes-executor pod-template config (`None` for the Celery executor),
    /// merged and logging-validated up-front so the build step does not touch the raw cluster.
    pub executor_template: Option<ValidatedExecutorTemplate>,
    pub authentication_config: AirflowClientAuthenticationDetailsResolved,
    pub authorization_config: AirflowAuthorizationResolved,
    pub credentials_secret_name: SecretName,
    pub load_examples: bool,
    pub expose_config: bool,
    pub database_initialization_enabled: bool,
    /// The metadata database connection (`spec.clusterConfig.metadataDatabase`), as taken from the
    /// CRD. The templated connection details are derived on demand, see
    /// [`ValidatedCluster::metadata_database_connection_details`].
    pub metadata_database: MetadataDatabaseConnection,
    /// The Celery result-backend connection (`spec.clusterConfig.celeryResultsBackend`), when
    /// configured. See [`ValidatedCluster::celery_database_connection_details`].
    pub celery_results_backend: Option<CeleryResultBackendConnection>,
    /// The Celery broker connection (`spec.clusterConfig.celeryBroker`), when configured. See
    /// [`ValidatedCluster::celery_database_connection_details`].
    pub celery_broker: Option<CeleryBrokerConnection>,
    /// User-supplied extra Volumes (`spec.clusterConfig.volumes`).
    pub volumes: Vec<Volume>,
    /// User-supplied extra VolumeMounts (`spec.clusterConfig.volumeMounts`).
    pub volume_mounts: Vec<VolumeMount>,
}

/// The validated cluster: proves that config merging succeeded for every role and
/// role group before any resources are created.
pub struct ValidatedCluster {
    /// `ObjectMeta` carrying `name`, `namespace` and `uid`, captured during validation, so this
    /// struct can stand in as the owner [`Resource`] for child objects.
    metadata: ObjectMeta,
    /// The cluster name as a type-safe value, used to build resource names and labels.
    pub name: ClusterName,
    /// The cluster's namespace as a type-safe value, captured during validation.
    pub namespace: NamespaceName,
    /// The cluster's UID as a type-safe value, captured during validation (used for owner
    /// references).
    pub uid: Uid,
    /// The product version as a valid label value, for the recommended `app.kubernetes.io/version`
    /// label. Derived from the resolved image's app-version label value.
    pub product_version: ProductVersion,
    pub image: ResolvedProductImage,
    pub cluster_config: ValidatedClusterConfig,
    pub webserver_config: Option<ValidatedWebserverRoleConfig>,
    pub webserver_role_group_configs: BTreeMap<RoleGroupName, AirflowRoleGroupConfig>,
    pub scheduler_config: Option<ValidatedRoleConfig>,
    pub scheduler_role_group_configs: BTreeMap<RoleGroupName, AirflowRoleGroupConfig>,
    pub dagprocessor_config: Option<ValidatedRoleConfig>,
    pub dagprocessor_role_group_configs: BTreeMap<RoleGroupName, AirflowRoleGroupConfig>,
    pub triggerer_config: Option<ValidatedRoleConfig>,
    pub triggerer_role_group_configs: BTreeMap<RoleGroupName, AirflowRoleGroupConfig>,
    pub worker_config: Option<ValidatedRoleConfig>,
    pub worker_role_group_configs: BTreeMap<RoleGroupName, AirflowRoleGroupConfig>,
}

/// The non-derived inputs to [`ValidatedCluster::new`].
///
/// Named fields, so the five same-typed role-group maps — and the four
/// `Option<ValidatedRoleConfig>` — cannot be swapped silently.
pub struct ValidatedClusterParams {
    pub name: ClusterName,
    pub namespace: NamespaceName,
    pub uid: Uid,
    pub image: ResolvedProductImage,
    pub cluster_config: ValidatedClusterConfig,
    pub webserver_config: Option<ValidatedWebserverRoleConfig>,
    pub webserver_role_group_configs: BTreeMap<RoleGroupName, AirflowRoleGroupConfig>,
    pub scheduler_config: Option<ValidatedRoleConfig>,
    pub scheduler_role_group_configs: BTreeMap<RoleGroupName, AirflowRoleGroupConfig>,
    pub dagprocessor_config: Option<ValidatedRoleConfig>,
    pub dagprocessor_role_group_configs: BTreeMap<RoleGroupName, AirflowRoleGroupConfig>,
    pub triggerer_config: Option<ValidatedRoleConfig>,
    pub triggerer_role_group_configs: BTreeMap<RoleGroupName, AirflowRoleGroupConfig>,
    pub worker_config: Option<ValidatedRoleConfig>,
    pub worker_role_group_configs: BTreeMap<RoleGroupName, AirflowRoleGroupConfig>,
}

impl ValidatedCluster {
    pub fn new(params: ValidatedClusterParams) -> Self {
        let ValidatedClusterParams {
            name,
            namespace,
            uid,
            image,
            cluster_config,
            webserver_config,
            webserver_role_group_configs,
            scheduler_config,
            scheduler_role_group_configs,
            dagprocessor_config,
            dagprocessor_role_group_configs,
            triggerer_config,
            triggerer_role_group_configs,
            worker_config,
            worker_role_group_configs,
        } = params;

        // `app_version_label_value` is constructed to be a valid label value, so it is also a valid
        // `ProductVersion`.
        let product_version = ProductVersion::from_str(&image.app_version_label_value)
            .expect("the app version label value is a valid product version");
        Self {
            // Capture only the identity fields needed to own child objects.
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some(namespace.to_string()),
                uid: Some(uid.to_string()),
                ..ObjectMeta::default()
            },
            name,
            namespace,
            uid,
            product_version,
            image,
            cluster_config,
            webserver_config,
            webserver_role_group_configs,
            scheduler_config,
            scheduler_role_group_configs,
            dagprocessor_config,
            dagprocessor_role_group_configs,
            triggerer_config,
            triggerer_role_group_configs,
            worker_config,
            worker_role_group_configs,
        }
    }

    /// Whether the cluster declares the given role.
    pub fn has_role(&self, role: &AirflowRole) -> bool {
        match role {
            AirflowRole::Webserver => self.webserver_config.is_some(),
            AirflowRole::Scheduler => self.scheduler_config.is_some(),
            AirflowRole::Worker => self.worker_config.is_some(),
            AirflowRole::DagProcessor => self.dagprocessor_config.is_some(),
            AirflowRole::Triggerer => self.triggerer_config.is_some(),
        }
    }

    /// The PodDisruptionBudget config of `role`, or `None` if the cluster does not declare it.
    pub(crate) fn pdb(&self, role: &AirflowRole) -> Option<&PdbConfig> {
        match role {
            AirflowRole::Webserver => self.webserver_config.as_ref().map(|config| &config.pdb),
            AirflowRole::Scheduler => self.scheduler_config.as_ref().map(|config| &config.pdb),
            AirflowRole::Worker => self.worker_config.as_ref().map(|config| &config.pdb),
            AirflowRole::DagProcessor => {
                self.dagprocessor_config.as_ref().map(|config| &config.pdb)
            }
            AirflowRole::Triggerer => self.triggerer_config.as_ref().map(|config| &config.pdb),
        }
    }

    /// The name of the group Listener provided for `role`, if the role serves the web UI.
    pub(crate) fn group_listener_name(&self, role: &AirflowRole) -> Option<&ListenerName> {
        match role {
            AirflowRole::Webserver => self
                .webserver_config
                .as_ref()
                .map(|config| &config.group_listener_name),
            AirflowRole::Scheduler
            | AirflowRole::Worker
            | AirflowRole::DagProcessor
            | AirflowRole::Triggerer => None,
        }
    }

    /// The reverse proxies `role` trusts `X-Forwarded-*` headers from.
    ///
    /// Empty for every role but the webserver, which alone serves the web UI — and empty for the
    /// webserver too when the cluster declares no webserver role, or it trusts no proxies.
    pub(crate) fn trusted_proxies(&self, role: &AirflowRole) -> &[TrustedProxy] {
        match role {
            AirflowRole::Webserver => self
                .webserver_config
                .as_ref()
                .map(|config| config.trusted_proxies.as_slice())
                .unwrap_or_default(),
            AirflowRole::Scheduler
            | AirflowRole::Worker
            | AirflowRole::DagProcessor
            | AirflowRole::Triggerer => &[],
        }
    }

    /// The Secret holding the shared internal secret (`<cluster>-internal-secret`).
    pub fn internal_secret_name(&self) -> SecretName {
        const SUFFIX: &str = "-internal-secret";
        const _: () = assert!(
            ClusterName::MAX_LENGTH + SUFFIX.len() <= SecretName::MAX_LENGTH,
            "The string `<cluster_name>-internal-secret` must not exceed the limit of Secret names."
        );
        // A ClusterName is an RFC 1035 label, so appending an alphanumeric-terminated suffix keeps
        // it a valid RFC 1123 subdomain.
        let _ = ClusterName::IS_RFC_1123_SUBDOMAIN_NAME;

        SecretName::from_str(&format!("{}{SUFFIX}", self.name))
            .expect("the internal secret name is a valid Secret name")
    }

    /// The Secret holding the shared JWT secret (`<cluster>-jwt-secret`).
    pub fn jwt_secret_name(&self) -> SecretName {
        const SUFFIX: &str = "-jwt-secret";
        const _: () = assert!(
            ClusterName::MAX_LENGTH + SUFFIX.len() <= SecretName::MAX_LENGTH,
            "The string `<cluster_name>-jwt-secret` must not exceed the limit of Secret names."
        );
        let _ = ClusterName::IS_RFC_1123_SUBDOMAIN_NAME;

        SecretName::from_str(&format!("{}{SUFFIX}", self.name))
            .expect("the JWT secret name is a valid Secret name")
    }

    /// The Secret holding the shared Fernet key (`<cluster>-fernet-key`).
    pub fn fernet_key_name(&self) -> SecretName {
        const SUFFIX: &str = "-fernet-key";
        const _: () = assert!(
            ClusterName::MAX_LENGTH + SUFFIX.len() <= SecretName::MAX_LENGTH,
            "The string `<cluster_name>-fernet-key` must not exceed the limit of Secret names."
        );
        let _ = ClusterName::IS_RFC_1123_SUBDOMAIN_NAME;

        SecretName::from_str(&format!("{}{SUFFIX}", self.name))
            .expect("the Fernet key secret name is a valid Secret name")
    }

    /// The ConfigMap holding the Kubernetes-executor pod template (`<cluster>-executor-pod-template`).
    pub fn executor_template_configmap_name(&self) -> ConfigMapName {
        const SUFFIX: &str = "-executor-pod-template";
        const _: () = assert!(
            ClusterName::MAX_LENGTH + SUFFIX.len() <= ConfigMapName::MAX_LENGTH,
            "The string `<cluster_name>-executor-pod-template` must not exceed the limit of \
            ConfigMap names."
        );
        let _ = ClusterName::IS_RFC_1123_SUBDOMAIN_NAME;

        ConfigMapName::from_str(&format!("{}{SUFFIX}", self.name))
            .expect("the executor pod-template ConfigMap name is a valid ConfigMap name")
    }

    /// User-supplied extra Volumes (`spec.clusterConfig.volumes`).
    pub fn volumes(&self) -> &Vec<Volume> {
        &self.cluster_config.volumes
    }

    /// User-supplied extra VolumeMounts (`spec.clusterConfig.volumeMounts`).
    pub fn volume_mounts(&self) -> Vec<VolumeMount> {
        self.cluster_config.volume_mounts.clone()
    }

    /// The templated SQLAlchemy connection details for the metadata database, derived from the
    /// CRD connection ([`ValidatedClusterConfig::metadata_database`]).
    pub fn metadata_database_connection_details(&self) -> SqlAlchemyDatabaseConnectionDetails {
        self.cluster_config
            .metadata_database
            .sqlalchemy_connection_details_with_templating(
                "METADATA",
                &TemplatingMechanism::BashEnvSubstitution,
            )
    }

    /// The templated Celery result-backend and broker connection details, derived from the CRD
    /// connections ([`ValidatedClusterConfig::celery_results_backend`] /
    /// [`ValidatedClusterConfig::celery_broker`]). `Some` only when both are configured, as the
    /// Celery executor needs both.
    pub fn celery_database_connection_details(
        &self,
    ) -> Option<(
        CeleryDatabaseConnectionDetails,
        CeleryDatabaseConnectionDetails,
    )> {
        let templating_mechanism = TemplatingMechanism::BashEnvSubstitution;
        match (
            &self.cluster_config.celery_results_backend,
            &self.cluster_config.celery_broker,
        ) {
            (Some(celery_results_backend), Some(celery_broker)) => {
                let celery_results_backend = celery_results_backend
                    .celery_connection_details_with_templating(
                        "CELERY_RESULT_BACKEND",
                        &templating_mechanism,
                    );
                let celery_broker = celery_broker.celery_connection_details_with_templating(
                    "CELERY_BROKER",
                    &templating_mechanism,
                );
                Some((celery_results_backend, celery_broker))
            }
            _ => None,
        }
    }

    /// Type-safe names for the per-cluster RBAC resources: the ServiceAccount shared by all
    /// Pods, its (namespaced) RoleBinding, and the operator-deployed ClusterRole it binds.
    pub fn cluster_resource_names(&self) -> role_utils::ResourceNames {
        role_utils::ResourceNames {
            cluster_name: self.name.clone(),
            product_name: PRODUCT_NAME.clone(),
        }
    }

    /// Type-safe names for the resources of a role group.
    pub fn role_group_resource_names(
        &self,
        role_name: &RoleName,
        role_group_name: &RoleGroupName,
    ) -> ResourceNames {
        ResourceNames {
            cluster_name: self.name.clone(),
            role_name: role_name.clone(),
            role_group_name: role_group_name.clone(),
        }
    }
}

// Pseudo role/role-group names for the Kubernetes executor's resources (it is not a real
// AirflowRole). Used to derive its labels and ConfigMap name.
constant!(pub EXECUTOR_ROLE_NAME: RoleName = "executor");
constant!(pub EXECUTOR_ROLE_GROUP_NAME: RoleGroupName = "kubernetes");

// The executor *pod-template* role-group name, used for the template ConfigMap/pod labels.
constant!(pub EXECUTOR_TEMPLATE_ROLE_GROUP_NAME: RoleGroupName = "executor-template");

/// Lets [`ValidatedCluster`] stand in for the raw [`v1alpha2::AirflowCluster`] when building owner
/// references and metadata for child objects. Kind/group/version are delegated to the CRD; the
/// `metadata` (name, namespace, uid) is captured during validation.
impl Resource for ValidatedCluster {
    type DynamicType = <v1alpha2::AirflowCluster as Resource>::DynamicType;
    type Scope = <v1alpha2::AirflowCluster as Resource>::Scope;

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

impl HasName for ValidatedCluster {
    fn to_name(&self) -> String {
        self.name_any()
    }
}

impl NameIsValidLabelValue for ValidatedCluster {
    fn to_label_value(&self) -> String {
        self.name.to_label_value()
    }
}

impl HasUid for ValidatedCluster {
    fn to_uid(&self) -> Uid {
        self.uid.clone()
    }
}

#[cfg(test)]
mod tests {
    use indoc::formatdoc;

    use super::*;
    use crate::controller::{
        build::test_support::dereferenced_objects, validate::validate_cluster,
    };

    #[test]
    fn test_constants() {
        // Test that dereferencing the constants does not panic.
        let _ = *PRODUCT_NAME;
        let _ = *OPERATOR_NAME;
        let _ = *CONTROLLER_NAME;
        let _ = *EXECUTOR_ROLE_NAME;
        let _ = *EXECUTOR_ROLE_GROUP_NAME;
        let _ = *EXECUTOR_TEMPLATE_ROLE_GROUP_NAME;
    }

    #[test]
    fn webserver_trusted_proxies_are_parsed() {
        let cluster = validated_cluster_with_webserver_role_config(
            "      trustedProxies:\n        - 10.244.0.0/16\n        - 192.168.1.1",
        );

        let trusted_proxies = cluster.trusted_proxies(&AirflowRole::Webserver);

        let rendered: Vec<String> = trusted_proxies
            .iter()
            .map(TrustedProxy::to_string)
            .collect();
        assert_eq!(rendered, ["10.244.0.0/16", "192.168.1.1"]);
    }

    /// Only the webserver serves HTTP, so no other role may pick the setting up even if a
    /// webserver configured it.
    #[test]
    fn non_webserver_roles_have_no_trusted_proxies() {
        let cluster = validated_cluster_with_webserver_role_config(
            "      trustedProxies:\n        - 10.244.0.0/16",
        );

        for role in [
            AirflowRole::Scheduler,
            AirflowRole::Worker,
            AirflowRole::DagProcessor,
            AirflowRole::Triggerer,
        ] {
            assert!(
                cluster.trusted_proxies(&role).is_empty(),
                "role {role:?} must not have trusted proxies"
            );
        }
    }

    #[test]
    fn a_webserver_without_trusted_proxies_yields_an_empty_list() {
        let cluster =
            validated_cluster_with_webserver_role_config("      listenerClass: external-stable");

        assert!(cluster.trusted_proxies(&AirflowRole::Webserver).is_empty());
    }

    /// The validated cluster for a CR with the given `webservers.roleConfig` block spliced in.
    ///
    /// The `roleConfig` must be one the webserver accepts: the trusted proxies are parsed and
    /// checked by `validate_cluster`, not by [`ValidatedCluster::trusted_proxies`], which only
    /// hands back what validation already accepted. The rejection cases live in
    /// [`crate::crd::trusted_proxies`], next to the parsing they exercise.
    fn validated_cluster_with_webserver_role_config(role_config: &str) -> ValidatedCluster {
        validate_cluster(
            &test_cluster_with_webserver_role_config(role_config),
            "oci.stackable.tech/sdp",
            dereferenced_objects(),
        )
        .expect("test cluster validates")
    }

    /// A cluster CR with the given `webservers.roleConfig` block spliced in.
    fn test_cluster_with_webserver_role_config(role_config: &str) -> v1alpha2::AirflowCluster {
        let cluster = formatdoc! {"
            apiVersion: airflow.stackable.tech/v1alpha2
            kind: AirflowCluster
            metadata:
              name: airflow
              namespace: default
              uid: e6ac237d-a6d4-43a1-8135-f36506110912
            spec:
              image:
                productVersion: 3.3.1
              clusterConfig:
                credentialsSecretName: airflow-admin-credentials
                metadataDatabase:
                  postgresql:
                    host: airflow-postgresql
                    database: airflow
                    credentialsSecretName: airflow-postgresql-credentials
              webservers:
                roleConfig:
            {role_config}
                roleGroups:
                  default:
                    config: {{}}
              kubernetesExecutors:
                config: {{}}
        "};

        let deserializer = serde_yaml::Deserializer::from_str(&cluster);
        serde_yaml::with::singleton_map_recursive::deserialize(deserializer)
            .expect("the test CR deserialises")
    }
}
