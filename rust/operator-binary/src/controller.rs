use std::{collections::BTreeMap, marker::PhantomData, sync::Arc};

use const_format::concatcp;
use product_config::ProductConfigManager;
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    cli::OperatorEnvironmentOptions,
    cluster_resources::{ClusterResourceApplyStrategy, ClusterResources},
    commons::{
        affinity::StackableAffinity,
        product_image_selection::ResolvedProductImage,
        resources::{NoRuntimeLimits, Resources},
    },
    crd::listener,
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{
            ConfigMap, Container as K8sContainer, EnvVar, PersistentVolumeClaim, PodTemplateSpec,
            Service, ServiceAccount, Volume, VolumeMount,
        },
        policy::v1::PodDisruptionBudget,
        rbac::v1::RoleBinding,
    },
    kube::{
        Resource,
        api::ObjectMeta,
        core::{DeserializeGuard, error_boundary},
        runtime::{controller::Action, reflector::ObjectRef},
    },
    logging::controller::ReconcilerError,
    product_logging::spec::ContainerLogConfig,
    role_utils::RoleGroupRef,
    shared::time::Duration,
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{
    crd::{APP_NAME, AirflowExecutor, AirflowRole, AirflowStorageConfig, OPERATOR_NAME, v1alpha2},
    framework::{
        HasName, HasUid, NameIsValidLabelValue,
        product_logging::framework::{ValidatedContainerLogConfigChoice, VectorContainerLogConfig},
        types::{
            kubernetes::{NamespaceName, Uid},
            operator::ClusterName,
        },
    },
};

pub mod apply;
pub mod build;
pub mod dereference;
pub mod update_status;
pub mod validate;

pub const AIRFLOW_CONTROLLER_NAME: &str = "airflowcluster";
pub const CONTAINER_IMAGE_BASE_NAME: &str = "airflow";
pub const AIRFLOW_FULL_CONTROLLER_NAME: &str =
    concatcp!(AIRFLOW_CONTROLLER_NAME, '.', OPERATOR_NAME);

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub product_config: ProductConfigManager,
    pub operator_environment: OperatorEnvironmentOptions,
}

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
// Reconcile
// ---------------------------------------------------------------------------

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("AirflowCluster object is invalid"))]
    InvalidAirflowCluster {
        source: error_boundary::InvalidObject,
    },

    #[snafu(display("failed to dereference resources"))]
    Dereference { source: dereference::Error },

    #[snafu(display("failed to validate cluster"))]
    Validate { source: validate::Error },

    #[snafu(display("failed to create cluster resources"))]
    CreateClusterResources {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to apply resources"))]
    Apply { source: apply::Error },

    #[snafu(display("failed to update status"))]
    UpdateStatus { source: update_status::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}

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
    let dereferenced = dereference::dereference(
        &ctx.client,
        airflow,
        CONTAINER_IMAGE_BASE_NAME,
        &ctx.operator_environment.image_repository,
        crate::built_info::PKG_VERSION,
    )
    .await
    .context(DereferenceSnafu)?;

    // --- validate (sync, fallible) ---
    let validated = validate::validate_cluster(airflow, &dereferenced, &ctx.product_config)
        .context(ValidateSnafu)?;

    // --- build (sync, infallible) ---
    let prepared = build::build(&validated);

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

    let applied = apply::Applier::new(&ctx.client, cluster_resources)
        .apply(prepared)
        .await
        .context(ApplySnafu)?;

    // --- update status (async, fallible) ---
    update_status::update_status(&ctx.client, airflow, applied)
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
