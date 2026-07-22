use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
};

use stackable_operator::{
    builder::meta::ObjectMetaBuilder,
    commons::{
        affinity::StackableAffinity,
        product_image_selection::ResolvedProductImage,
        resources::{NoRuntimeLimits, Resources},
    },
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
        core::v1::{ConfigMap, PodTemplateSpec, Service, Volume, VolumeMount},
        policy::v1::PodDisruptionBudget,
    },
    kube::{Resource, ResourceExt, api::ObjectMeta},
    kvp::Labels,
    product_logging::spec::ContainerLogConfig,
    shared::time::Duration,
    v2::{
        HasName, HasUid, NameIsValidLabelValue,
        builder::meta::ownerreference_from_resource,
        kvp::label::{recommended_labels, role_group_selector},
        product_logging::framework::{ValidatedContainerLogConfigChoice, VectorContainerLogConfig},
        role_group_utils::ResourceNames,
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
    controller::build::openlineage::ResolvedOpenLineageConfig,
    crd::{
        APP_NAME, AirflowConfig, AirflowConfigOverrides, AirflowExecutor, AirflowRole,
        AirflowStorageConfig, ExecutorConfig, OPERATOR_NAME,
        authentication::AirflowClientAuthenticationDetailsResolved,
        authorization::AirflowAuthorizationResolved,
        databases::{
            CeleryBrokerConnection, CeleryResultBackendConnection, MetadataDatabaseConnection,
        },
        v1alpha2,
    },
};

pub mod build;
pub mod dereference;
pub mod validate;

/// Every Kubernetes resource produced by the build step.
pub struct KubernetesResources {
    pub stateful_sets: Vec<StatefulSet>,
    pub services: Vec<Service>,
    pub listeners: Vec<listener::v1alpha1::Listener>,
    pub config_maps: Vec<ConfigMap>,
    pub pod_disruption_budgets: Vec<PodDisruptionBudget>,
}

/// Per-role configuration extracted during validation.
#[derive(Clone, Debug)]
pub struct ValidatedRoleConfig {
    pub pdb: Option<stackable_operator::commons::pdb::PdbConfig>,
    pub listener_class: Option<ListenerClassName>,
    pub group_listener_name: Option<ListenerName>,
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
    pub env_overrides: HashMap<String, String>,
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
    /// The resolved OpenLineage configuration (`spec.clusterConfig.openLineage`), when configured.
    pub open_lineage: Option<ResolvedOpenLineageConfig>,
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
    pub role_groups: BTreeMap<AirflowRole, BTreeMap<RoleGroupName, AirflowRoleGroupConfig>>,
    pub role_configs: BTreeMap<AirflowRole, ValidatedRoleConfig>,
}

impl ValidatedCluster {
    pub fn new(
        name: ClusterName,
        namespace: NamespaceName,
        uid: Uid,
        image: ResolvedProductImage,
        cluster_config: ValidatedClusterConfig,
        role_groups: BTreeMap<AirflowRole, BTreeMap<RoleGroupName, AirflowRoleGroupConfig>>,
        role_configs: BTreeMap<AirflowRole, ValidatedRoleConfig>,
    ) -> Self {
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
            role_groups,
            role_configs,
        }
    }

    /// Whether the cluster has the given role configured (i.e. it has role groups for it).
    pub fn has_role(&self, role: &AirflowRole) -> bool {
        self.role_groups.contains_key(role)
    }

    /// The Secret holding the shared internal secret (`<cluster>-internal-secret`).
    pub fn internal_secret_name(&self) -> SecretName {
        SecretName::from_str(&format!("{}-internal-secret", self.name_any()))
            .expect("the internal secret name is a valid Secret name")
    }

    /// The Secret holding the shared JWT secret (`<cluster>-jwt-secret`).
    pub fn jwt_secret_name(&self) -> SecretName {
        SecretName::from_str(&format!("{}-jwt-secret", self.name_any()))
            .expect("the JWT secret name is a valid Secret name")
    }

    /// The Secret holding the shared Fernet key (`<cluster>-fernet-key`).
    pub fn fernet_key_name(&self) -> SecretName {
        SecretName::from_str(&format!("{}-fernet-key", self.name_any()))
            .expect("the Fernet key secret name is a valid Secret name")
    }

    /// The ConfigMap holding the Kubernetes-executor pod template (`<cluster>-executor-pod-template`).
    pub fn executor_template_configmap_name(&self) -> ConfigMapName {
        ConfigMapName::from_str(&format!("{}-executor-pod-template", self.name_any()))
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

    /// Type-safe names for the resources of a role group.
    pub fn resource_names(
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

    /// Recommended labels for a role-group resource.
    pub fn recommended_labels(
        &self,
        role: &AirflowRole,
        role_group_name: &RoleGroupName,
    ) -> Labels {
        self.recommended_labels_for(&role.role_name(), role_group_name)
    }

    /// Recommended labels for a resource that is not tied to a concrete [`AirflowRole`] (e.g. the
    /// Kubernetes executor pod template), using a free-form role/role-group label value.
    pub fn recommended_labels_for(
        &self,
        role_name: &RoleName,
        role_group_name: &RoleGroupName,
    ) -> Labels {
        self.recommended_labels_with(&self.product_version, role_name, role_group_name)
    }

    /// Recommended labels with a constant `none` version, for PVC templates that cannot be modified
    /// after deployment (keeps the labels stable across version upgrades).
    pub fn unversioned_recommended_labels(
        &self,
        role: &AirflowRole,
        role_group_name: &RoleGroupName,
    ) -> Labels {
        self.recommended_labels_with(
            &ProductVersion::from_str("none").expect("'none' is a valid product version"),
            &role.role_name(),
            role_group_name,
        )
    }

    fn recommended_labels_with(
        &self,
        product_version: &ProductVersion,
        role_name: &RoleName,
        role_group_name: &RoleGroupName,
    ) -> Labels {
        recommended_labels(
            self,
            &product_name(),
            product_version,
            &operator_name(),
            &controller_name(),
            role_name,
            role_group_name,
        )
    }

    /// Selector labels matching the pods of a role group.
    pub fn role_group_selector(
        &self,
        role: &AirflowRole,
        role_group_name: &RoleGroupName,
    ) -> Labels {
        role_group_selector(self, &product_name(), &role.role_name(), role_group_name)
    }

    /// Returns an [`ObjectMetaBuilder`] pre-filled with the namespace, the resource `name`, an owner
    /// reference back to this cluster, and the given recommended `labels`.
    pub(crate) fn object_meta(&self, name: impl Into<String>, labels: Labels) -> ObjectMetaBuilder {
        let mut builder = ObjectMetaBuilder::new();
        builder
            .name_and_namespace(self)
            .name(name)
            .ownerreference(ownerreference_from_resource(self, None, Some(true)))
            .with_labels(labels);
        builder
    }
}

/// The product name (`airflow`) as a type-safe label value.
pub(crate) fn product_name() -> ProductName {
    ProductName::from_str(APP_NAME).expect("'airflow' is a valid product name")
}

/// The operator name as a type-safe label value.
pub(crate) fn operator_name() -> OperatorName {
    OperatorName::from_str(OPERATOR_NAME).expect("the operator name is a valid label value")
}

/// The controller name as a type-safe label value.
pub(crate) fn controller_name() -> ControllerName {
    ControllerName::from_str(AIRFLOW_CONTROLLER_NAME)
        .expect("the controller name is a valid label value")
}

/// Pseudo role/role-group names for the Kubernetes executor's resources (it is not a real
/// AirflowRole). Used to derive its labels and ConfigMap name.
pub const EXECUTOR_ROLE_NAME: &str = "executor";
pub const EXECUTOR_ROLE_GROUP_NAME: &str = "kubernetes";

/// The executor pseudo-role name (`executor`) as a type-safe value.
pub fn executor_role_name() -> RoleName {
    EXECUTOR_ROLE_NAME
        .parse()
        .expect("'executor' is a valid role name")
}

/// The executor's role-group name (`kubernetes`), used for its role-group ConfigMap.
pub fn executor_role_group_name() -> RoleGroupName {
    EXECUTOR_ROLE_GROUP_NAME
        .parse()
        .expect("'kubernetes' is a valid role group name")
}

/// The executor *pod-template* role-group name (`executor-template`), used for the template
/// ConfigMap/pod labels.
pub fn executor_template_role_group_name() -> RoleGroupName {
    "executor-template"
        .parse()
        .expect("'executor-template' is a valid role group name")
}

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
