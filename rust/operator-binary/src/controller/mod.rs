use std::{collections::BTreeMap, str::FromStr};

use stackable_operator::{
    commons::product_image_selection::ResolvedProductImage,
    k8s_openapi::api::core::v1::{Volume, VolumeMount},
    kube::{Resource, ResourceExt, api::ObjectMeta},
    kvp::Labels,
    v2::{
        HasName, HasUid, NameIsValidLabelValue,
        kvp::label::{recommended_labels, role_group_selector},
        product_logging::framework::VectorContainerLogConfig,
        role_group_utils::ResourceNames,
        types::{
            kubernetes::{ConfigMapName, Uid},
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
        APP_NAME, AirflowConfig, AirflowConfigOverrides, AirflowExecutor, AirflowRole,
        OPERATOR_NAME, authentication::AirflowClientAuthenticationDetailsResolved,
        authorization::AirflowAuthorizationResolved, v1alpha2,
    },
};

pub mod build;
pub mod dereference;
pub mod validate;

/// Per-role configuration extracted during validation.
#[derive(Clone, Debug)]
pub struct ValidatedRoleConfig {
    pub pdb: Option<stackable_operator::commons::pdb::PdbConfig>,
    pub listener_class: Option<String>,
    pub group_listener_name: Option<String>,
}

/// Per-rolegroup configuration: the merged CRD config plus overrides.
///
/// This is the generic [`stackable_operator::v2::role_utils::RoleGroupConfig`]: the merged config
/// fragment in `config`, the typed `config_overrides` (role-group merged over role) and the merged
/// `env_overrides`/`cli_overrides`/`pod_overrides`. The config overrides are kept typed
/// ([`AirflowConfigOverrides`]) and flattened into the rendered config file later, in the build step.
pub type AirflowRoleGroupConfig = stackable_operator::v2::role_utils::RoleGroupConfig<
    AirflowConfig,
    stackable_operator::v2::role_utils::GenericCommonConfig,
    AirflowConfigOverrides,
>;

/// A validated role group: the merged [`AirflowRoleGroupConfig`] paired with its up-front-validated
/// [`ValidatedLogging`], so the build step reads both from here rather than re-deriving from the raw
/// cluster. (Superset folds logging into a single rolegroup struct; airflow keeps the generic merged
/// config as-is — retaining all its fields without bespoke `#[allow(dead_code)]` — and carries
/// logging alongside it.)
#[derive(Clone, Debug)]
pub struct AirflowRoleGroup {
    pub config: AirflowRoleGroupConfig,
    pub logging: ValidatedLogging,
}

/// Validated logging configuration for the (optional) Vector container.
///
/// Produced up-front by [`validate::validate_logging`] (mirroring the superset-operator) so that an
/// invalid custom log ConfigMap name or a missing Vector aggregator discovery ConfigMap name fails
/// reconciliation during validation rather than at resource-build time.
///
/// Unlike superset's equivalent this carries no product-container field: superset's is never read,
/// and airflow's `airflow` container log config is consumed leniently in the build, so validating
/// it eagerly here could reject configurations the build currently tolerates.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidatedLogging {
    pub vector_container: Option<VectorContainerLogConfig>,
    pub enable_vector_agent: bool,
}

/// Cluster-wide configuration that applies to every role and role group.
///
/// Carries the dereferenced external references, so every downstream build step reads them from
/// here rather than from the raw cluster object.
#[derive(Clone, Debug)]
pub struct ValidatedClusterConfig {
    pub executor: AirflowExecutor,
    pub authentication_config: AirflowClientAuthenticationDetailsResolved,
    pub authorization_config: AirflowAuthorizationResolved,
    pub credentials_secret_name: String,
    pub load_examples: bool,
    pub expose_config: bool,
    pub database_initialization_enabled: bool,
    /// User-supplied extra Volumes (`spec.clusterConfig.volumes`).
    pub volumes: Vec<Volume>,
    /// User-supplied extra VolumeMounts (`spec.clusterConfig.volumeMounts`).
    pub volume_mounts: Vec<VolumeMount>,
    /// The validated Vector aggregator discovery ConfigMap name (`None` when no aggregator is set).
    pub vector_aggregator_config_map_name: Option<ConfigMapName>,
}

/// The validated cluster: proves that config merging succeeded for every role and
/// role group before any resources are created.
#[derive(Clone, Debug)]
pub struct ValidatedCluster {
    /// `ObjectMeta` carrying `name`, `namespace` and `uid`, captured during validation, so this
    /// struct can stand in as the owner [`Resource`] for child objects.
    metadata: ObjectMeta,
    /// The cluster name as a type-safe value, used to build resource names and labels.
    pub name: ClusterName,
    /// The product version as a valid label value, for the recommended `app.kubernetes.io/version`
    /// label. Derived from the resolved image's app-version label value.
    pub product_version: ProductVersion,
    pub image: ResolvedProductImage,
    pub cluster_config: ValidatedClusterConfig,
    pub role_groups: BTreeMap<AirflowRole, BTreeMap<RoleGroupName, AirflowRoleGroup>>,
    pub role_configs: BTreeMap<AirflowRole, ValidatedRoleConfig>,
}

impl ValidatedCluster {
    pub fn new(
        airflow: &v1alpha2::AirflowCluster,
        name: ClusterName,
        image: ResolvedProductImage,
        cluster_config: ValidatedClusterConfig,
        role_groups: BTreeMap<AirflowRole, BTreeMap<RoleGroupName, AirflowRoleGroup>>,
        role_configs: BTreeMap<AirflowRole, ValidatedRoleConfig>,
    ) -> Self {
        // `app_version_label_value` is constructed to be a valid label value, so it is also a valid
        // `ProductVersion`.
        let product_version = ProductVersion::from_str(&image.app_version_label_value)
            .expect("the app version label value is a valid product version");
        Self {
            // Capture only the identity fields needed to own child objects.
            metadata: ObjectMeta {
                name: Some(airflow.name_any()),
                namespace: airflow.namespace(),
                uid: airflow.uid(),
                ..ObjectMeta::default()
            },
            name,
            product_version,
            image,
            cluster_config,
            role_groups,
            role_configs,
        }
    }

    /// The cluster's namespace, captured during validation.
    pub fn namespace(&self) -> Option<String> {
        self.metadata.namespace.clone()
    }

    /// Whether the cluster has the given role configured (i.e. it has role groups for it).
    pub fn has_role(&self, role: &AirflowRole) -> bool {
        self.role_groups.contains_key(role)
    }

    /// The Secret holding the shared internal secret (`<cluster>-internal-secret`).
    pub fn internal_secret_name(&self) -> String {
        format!("{}-internal-secret", self.name_any())
    }

    /// The Secret holding the shared JWT secret (`<cluster>-jwt-secret`).
    pub fn jwt_secret_name(&self) -> String {
        format!("{}-jwt-secret", self.name_any())
    }

    /// The Secret holding the shared Fernet key (`<cluster>-fernet-key`).
    pub fn fernet_key_name(&self) -> String {
        format!("{}-fernet-key", self.name_any())
    }

    /// The ConfigMap holding the Kubernetes-executor pod template (`<cluster>-executor-pod-template`).
    pub fn executor_template_configmap_name(&self) -> String {
        format!("{}-executor-pod-template", self.name_any())
    }

    /// User-supplied extra Volumes (`spec.clusterConfig.volumes`).
    pub fn volumes(&self) -> &Vec<Volume> {
        &self.cluster_config.volumes
    }

    /// User-supplied extra VolumeMounts (`spec.clusterConfig.volumeMounts`).
    pub fn volume_mounts(&self) -> Vec<VolumeMount> {
        self.cluster_config.volume_mounts.clone()
    }

    /// Type-safe names for the resources of a role group.
    ///
    /// Infallible: the combined name length was validated during cluster validation
    /// (see `validate::validate_cluster`).
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
}

/// The product name (`airflow`) as a type-safe label value.
fn product_name() -> ProductName {
    ProductName::from_str(APP_NAME).expect("'airflow' is a valid product name")
}

/// The operator name as a type-safe label value.
fn operator_name() -> OperatorName {
    OperatorName::from_str(OPERATOR_NAME).expect("the operator name is a valid label value")
}

/// The controller name as a type-safe label value.
fn controller_name() -> ControllerName {
    ControllerName::from_str(AIRFLOW_CONTROLLER_NAME)
        .expect("the controller name is a valid label value")
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
        Uid::from_str(
            &self
                .metadata
                .uid
                .clone()
                .expect("the uid is captured during validation"),
        )
        .expect("the uid is a valid Kubernetes UID")
    }
}
