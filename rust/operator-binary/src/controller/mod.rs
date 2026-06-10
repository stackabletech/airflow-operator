use std::collections::BTreeMap;

use stackable_operator::{
    commons::product_image_selection::ResolvedProductImage,
    kube::{Resource, ResourceExt, api::ObjectMeta},
};

use crate::crd::{
    AirflowConfig, AirflowConfigOverrides, AirflowExecutor, AirflowRole,
    authentication::AirflowClientAuthenticationDetailsResolved,
    authorization::AirflowAuthorizationResolved, v1alpha2,
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

/// Cluster-wide configuration that applies to every role and role group.
///
/// Carries the dereferenced external references, so every downstream build step reads them from
/// here rather than from the raw cluster object.
#[derive(Clone, Debug)]
pub struct ValidatedClusterConfig {
    pub executor: AirflowExecutor,
    pub authentication_config: AirflowClientAuthenticationDetailsResolved,
    pub authorization_config: AirflowAuthorizationResolved,
}

/// The validated cluster: proves that config merging succeeded for every role and
/// role group before any resources are created.
#[derive(Clone, Debug)]
pub struct ValidatedCluster {
    /// `ObjectMeta` carrying `name`, `namespace` and `uid`, captured during validation, so this
    /// struct can stand in as the owner [`Resource`] for child objects.
    metadata: ObjectMeta,
    pub image: ResolvedProductImage,
    pub cluster_config: ValidatedClusterConfig,
    pub role_groups: BTreeMap<AirflowRole, BTreeMap<String, AirflowRoleGroupConfig>>,
    pub role_configs: BTreeMap<AirflowRole, ValidatedRoleConfig>,
}

impl ValidatedCluster {
    pub fn new(
        airflow: &v1alpha2::AirflowCluster,
        image: ResolvedProductImage,
        cluster_config: ValidatedClusterConfig,
        role_groups: BTreeMap<AirflowRole, BTreeMap<String, AirflowRoleGroupConfig>>,
        role_configs: BTreeMap<AirflowRole, ValidatedRoleConfig>,
    ) -> Self {
        Self {
            // Capture only the identity fields needed to own child objects.
            metadata: ObjectMeta {
                name: Some(airflow.name_any()),
                namespace: airflow.namespace(),
                uid: airflow.uid(),
                ..ObjectMeta::default()
            },
            image,
            cluster_config,
            role_groups,
            role_configs,
        }
    }
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
