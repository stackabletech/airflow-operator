//! Ensures that `Pod`s are configured and running for each [`v1alpha2::AirflowCluster`]
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use const_format::concatcp;
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    cli::OperatorEnvironmentOptions,
    cluster_resources::{ClusterResourceApplyStrategy, ClusterResources},
    commons::{random_secret_creation, rbac::build_rbac_resources},
    crd::git_sync,
    k8s_openapi::api::core::v1::EnvVar,
    kube::{
        Resource, ResourceExt,
        core::{DeserializeGuard, error_boundary},
        runtime::controller::Action,
    },
    kvp::LabelError,
    logging::controller::ReconcilerError,
    shared::time::Duration,
    status::condition::{
        compute_conditions, operations::ClusterOperationsConditionBuilder,
        statefulset::StatefulSetConditionBuilder,
    },
    v2::types::operator::{RoleGroupName, RoleName},
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{
    controller::{
        ValidatedCluster,
        build::{
            config_map,
            resource::{
                executor::build_executor_template_config_map,
                listener::build_group_listener,
                pdb::build_pdb,
                service::{build_rolegroup_headless_service, build_rolegroup_metrics_service},
                statefulset::build_server_rolegroup_statefulset,
            },
        },
    },
    controller_commons::LOG_VOLUME_NAME,
    crd::{
        self, APP_NAME, AirflowClusterStatus, AirflowConfigOverrides, AirflowExecutor,
        AirflowExecutorCommonConfiguration, Container, OPERATOR_NAME,
        internal_secret::{
            FERNET_KEY_SECRET_KEY, INTERNAL_SECRET_SECRET_KEY, JWT_SECRET_SECRET_KEY,
        },
        v1alpha2,
    },
};

pub const AIRFLOW_CONTROLLER_NAME: &str = "airflowcluster";
pub const CONTAINER_IMAGE_BASE_NAME: &str = "airflow";

/// Pseudo role/role-group names for the Kubernetes executor's resources (it is not a real
/// [`AirflowRole`]). Used to derive its labels and ConfigMap name.
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
pub const AIRFLOW_FULL_CONTROLLER_NAME: &str =
    concatcp!(AIRFLOW_CONTROLLER_NAME, '.', OPERATOR_NAME);

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub operator_environment: OperatorEnvironmentOptions,
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("failed to apply Service for role group {role_group}"))]
    ApplyRoleGroupService {
        source: stackable_operator::cluster_resources::Error,
        role_group: RoleGroupName,
    },

    #[snafu(display("failed to apply ConfigMap for role group {role_group}"))]
    ApplyRoleGroupConfig {
        source: stackable_operator::cluster_resources::Error,
        role_group: RoleGroupName,
    },

    #[snafu(display("failed to apply StatefulSet for role group {role_group}"))]
    ApplyRoleGroupStatefulSet {
        source: stackable_operator::cluster_resources::Error,
        role_group: RoleGroupName,
    },

    #[snafu(display("failed to patch service account"))]
    ApplyServiceAccount {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to patch role binding: {source}"))]
    ApplyRoleBinding {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to build RBAC objects"))]
    BuildRBACObjects {
        source: stackable_operator::commons::rbac::Error,
    },

    #[snafu(display("failed to build rolegroup ConfigMap"))]
    BuildConfigMap {
        source: crate::controller::build::config_map::Error,
    },

    #[snafu(display("failed to resolve and merge config for role and role group"))]
    FailedToResolveConfig { source: crd::Error },

    #[snafu(display("invalid git-sync specification"))]
    InvalidGitSyncSpec { source: git_sync::v1alpha2::Error },

    #[snafu(display("failed to create cluster resources"))]
    CreateClusterResources {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to delete orphaned resources"))]
    DeleteOrphanedResources {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to update status"))]
    ApplyStatus {
        source: stackable_operator::client::Error,
    },

    #[snafu(display("failed to create internal secret"))]
    InternalSecret {
        source: random_secret_creation::Error,
    },

    #[snafu(display("failed to dereference cluster resources"))]
    Dereference {
        source: crate::controller::dereference::Error,
    },

    #[snafu(display("failed to validate cluster configuration"))]
    Validate {
        source: crate::controller::validate::Error,
    },

    #[snafu(display("failed to apply executor template ConfigMap"))]
    ApplyExecutorTemplateConfig {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to build the executor pod-template ConfigMap"))]
    BuildExecutorTemplate {
        source: crate::controller::build::resource::executor::Error,
    },

    #[snafu(display("failed to apply PodDisruptionBudget"))]
    ApplyPdb {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to build label"))]
    BuildLabel { source: LabelError },

    #[snafu(display("AirflowCluster object is invalid"))]
    InvalidAirflowCluster {
        source: error_boundary::InvalidObject,
    },

    #[snafu(display("failed to build the rolegroup StatefulSet"))]
    BuildStatefulSet {
        source: crate::controller::build::resource::statefulset::Error,
    },

    #[snafu(display("failed to apply group listener"))]
    ApplyGroupListener {
        source: stackable_operator::cluster_resources::Error,
    },
}

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}

pub async fn reconcile_airflow(
    airflow: Arc<DeserializeGuard<v1alpha2::AirflowCluster>>,
    ctx: Arc<Ctx>,
) -> Result<Action> {
    tracing::info!("Starting reconcile");

    let airflow = airflow
        .0
        .as_ref()
        .map_err(error_boundary::InvalidObject::clone)
        .context(InvalidAirflowClusterSnafu)?;

    let client = &ctx.client;

    let dereferenced = crate::controller::dereference::dereference(client, airflow)
        .await
        .context(DereferenceSnafu)?;

    let cluster_operation_cond_builder =
        ClusterOperationsConditionBuilder::new(&airflow.spec.cluster_operation);

    let validated_cluster = crate::controller::validate::validate_cluster(
        airflow,
        &ctx.operator_environment.image_repository,
        dereferenced,
    )
    .context(ValidateSnafu)?;

    // TODO: Move secret creation to a dedicated apply step once it exists.
    random_secret_creation::create_random_secret_if_not_exists(
        &validated_cluster.internal_secret_name(),
        INTERNAL_SECRET_SECRET_KEY,
        256,
        airflow,
        client,
    )
    .await
    .context(InternalSecretSnafu)?;

    random_secret_creation::create_random_secret_if_not_exists(
        &validated_cluster.jwt_secret_name(),
        JWT_SECRET_SECRET_KEY,
        256,
        airflow,
        client,
    )
    .await
    .context(InternalSecretSnafu)?;

    // https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/fernet.html#security-fernet
    // does not document how long the fernet key should be, but recommends using
    // python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
    // which returns `jUm21LuA76YZmrIa9u4eXRg0h0P24MDC9IDOmDvJbfw=`, which has 44 characters, which makes 32 bytes.
    random_secret_creation::create_random_secret_if_not_exists(
        &validated_cluster.fernet_key_name(),
        FERNET_KEY_SECRET_KEY,
        32,
        airflow,
        client,
    )
    .await
    .context(InternalSecretSnafu)?;

    let mut cluster_resources = ClusterResources::new(
        APP_NAME,
        OPERATOR_NAME,
        AIRFLOW_CONTROLLER_NAME,
        &airflow.object_ref(&()),
        ClusterResourceApplyStrategy::from(&airflow.spec.cluster_operation),
        &airflow.spec.object_overrides,
    )
    .context(CreateClusterResourcesSnafu)?;

    let required_labels = cluster_resources
        .get_required_labels()
        .context(BuildLabelSnafu)?;

    let (rbac_sa, rbac_rolebinding) =
        build_rbac_resources(airflow, APP_NAME, required_labels).context(BuildRBACObjectsSnafu)?;

    let rbac_sa = cluster_resources
        .add(client, rbac_sa.clone())
        .await
        .context(ApplyServiceAccountSnafu)?;
    cluster_resources
        .add(client, rbac_rolebinding)
        .await
        .context(ApplyRoleBindingSnafu)?;

    let mut ss_cond_builder = StatefulSetConditionBuilder::default();

    // if the kubernetes executor is specified, in place of a worker role that will be in the role
    // collection there will be a pod template created to be used for pod provisioning
    if let AirflowExecutor::KubernetesExecutors {
        common_configuration,
    } = &validated_cluster.cluster_config.executor
    {
        build_executor_template(
            airflow,
            common_configuration,
            &validated_cluster,
            &mut cluster_resources,
            client,
            &rbac_sa,
        )
        .await?;
    }

    for (airflow_role, role_group_configs) in &validated_cluster.role_groups {
        if let Some(role_config) = validated_cluster.role_configs.get(airflow_role) {
            if let Some(pdb_config) = &role_config.pdb
                && let Some(pdb) = build_pdb(pdb_config, &validated_cluster, airflow_role)
            {
                cluster_resources
                    .add(client, pdb)
                    .await
                    .context(ApplyPdbSnafu)?;
            }

            if let Some(listener_class) = &role_config.listener_class
                && let Some(listener_group_name) = &role_config.group_listener_name
            {
                let rg_group_listener = build_group_listener(
                    &validated_cluster,
                    airflow_role,
                    listener_class.to_string(),
                    listener_group_name.clone(),
                );
                cluster_resources
                    .add(client, rg_group_listener)
                    .await
                    .context(ApplyGroupListenerSnafu)?;
            }
        }

        for (rolegroup_name, validated_rg) in role_group_configs {
            let validated_rg_config = &validated_rg.config;
            let logging = &validated_rg.logging;

            let git_sync_resources = git_sync::v1alpha2::GitSyncResources::new(
                &validated_cluster.cluster_config.dags_git_sync,
                &validated_cluster.image,
                &Vec::<EnvVar>::from(validated_rg_config.env_overrides.clone()),
                &airflow.volume_mounts(),
                LOG_VOLUME_NAME,
                &validated_rg_config
                    .config
                    .logging
                    .for_container(&Container::GitSync),
            )
            .context(InvalidGitSyncSpecSnafu)?;

            let rg_headless_service =
                build_rolegroup_headless_service(&validated_cluster, airflow_role, rolegroup_name);

            cluster_resources
                .add(client, rg_headless_service)
                .await
                .context(ApplyRoleGroupServiceSnafu {
                    role_group: rolegroup_name.clone(),
                })?;

            let rg_metrics_service =
                build_rolegroup_metrics_service(&validated_cluster, airflow_role, rolegroup_name);
            cluster_resources
                .add(client, rg_metrics_service)
                .await
                .context(ApplyRoleGroupServiceSnafu {
                    role_group: rolegroup_name.clone(),
                })?;

            let rg_configmap = config_map::build_rolegroup_config_map(
                &validated_cluster,
                &airflow_role.role_name(),
                rolegroup_name,
                &validated_rg_config.config_overrides,
                &validated_rg_config.config.logging,
                &Container::Airflow,
            )
            .context(BuildConfigMapSnafu)?;
            cluster_resources
                .add(client, rg_configmap)
                .await
                .with_context(|_| ApplyRoleGroupConfigSnafu {
                    role_group: rolegroup_name.clone(),
                })?;

            let rg_statefulset = build_server_rolegroup_statefulset(
                &validated_cluster,
                airflow_role,
                rolegroup_name,
                validated_rg_config,
                logging,
                &rbac_sa,
                &git_sync_resources,
            )
            .context(BuildStatefulSetSnafu)?;

            ss_cond_builder.add(
                cluster_resources
                    .add(client, rg_statefulset)
                    .await
                    .context(ApplyRoleGroupStatefulSetSnafu {
                        role_group: rolegroup_name.clone(),
                    })?,
            );
        }
    }

    cluster_resources
        .delete_orphaned_resources(client)
        .await
        .context(DeleteOrphanedResourcesSnafu)?;

    let status = AirflowClusterStatus {
        conditions: compute_conditions(
            airflow,
            &[&ss_cond_builder, &cluster_operation_cond_builder],
        ),
    };

    client
        .apply_patch_status(OPERATOR_NAME, airflow, &status)
        .await
        .context(ApplyStatusSnafu)?;

    Ok(Action::await_change())
}

async fn build_executor_template(
    airflow: &v1alpha2::AirflowCluster,
    common_config: &AirflowExecutorCommonConfiguration,
    validated_cluster: &ValidatedCluster,
    cluster_resources: &mut ClusterResources<'_>,
    client: &stackable_operator::client::Client,
    rbac_sa: &stackable_operator::k8s_openapi::api::core::v1::ServiceAccount,
) -> Result<(), Error> {
    let merged_executor_config = airflow
        .merged_executor_config(&common_config.config)
        .context(FailedToResolveConfigSnafu)?;
    let rg_configmap = config_map::build_rolegroup_config_map(
        validated_cluster,
        &executor_role_name(),
        &executor_role_group_name(),
        // The kubernetes-executor pod template does not apply webserver_config.py overrides
        // (preserves prior behaviour, which passed an empty map here).
        &AirflowConfigOverrides::default(),
        &merged_executor_config.logging,
        &Container::Base,
    )
    .context(BuildConfigMapSnafu)?;
    cluster_resources
        .add(client, rg_configmap)
        .await
        .with_context(|_| ApplyRoleGroupConfigSnafu {
            role_group: executor_role_group_name(),
        })?;

    let git_sync_resources = git_sync::v1alpha2::GitSyncResources::new(
        &validated_cluster.cluster_config.dags_git_sync,
        &validated_cluster.image,
        &env_vars_from_overrides(&common_config.env_overrides),
        &airflow.volume_mounts(),
        LOG_VOLUME_NAME,
        &merged_executor_config
            .logging
            .for_container(&Container::GitSync),
    )
    .context(InvalidGitSyncSpecSnafu)?;

    let worker_pod_template_config_map = build_executor_template_config_map(
        validated_cluster,
        &rbac_sa.name_unchecked(),
        &merged_executor_config,
        &common_config.env_overrides,
        &common_config.pod_overrides,
        &git_sync_resources,
    )
    .context(BuildExecutorTemplateSnafu)?;
    cluster_resources
        .add(client, worker_pod_template_config_map)
        .await
        .with_context(|_| ApplyExecutorTemplateConfigSnafu {})?;
    Ok(())
}

pub fn error_policy(
    _obj: Arc<DeserializeGuard<v1alpha2::AirflowCluster>>,
    error: &Error,
    _ctx: Arc<Ctx>,
) -> Action {
    match error {
        // root object is invalid, will be requeued when modified anyway
        Error::InvalidAirflowCluster { .. } => Action::await_change(),

        _ => Action::requeue(*Duration::from_secs(10)),
    }
}

/// Convert user-supplied `envOverrides` into a list of [`EnvVar`]s.
fn env_vars_from_overrides(env_overrides: &HashMap<String, String>) -> Vec<EnvVar> {
    // Collect into a `BTreeMap` first so the env vars come out in a deterministic (sorted) order;
    // `HashMap` iteration order is randomised per instance and would otherwise churn the containers
    // this feeds between reconciles. Mirrors the override handling in `env_vars.rs`.
    env_overrides
        .iter()
        .collect::<BTreeMap<_, _>>()
        .into_iter()
        .map(|(k, v)| EnvVar {
            name: k.clone(),
            value: Some(v.clone()),
            ..EnvVar::default()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::env_vars_from_overrides;

    /// The env vars must come out in a deterministic (sorted-by-name) order. `env_overrides` is a
    /// `HashMap`, whose iteration order is randomised per instance, so iterating it directly would
    /// vary the rendered env array between reconciles and churn the git-sync containers it feeds.
    #[test]
    fn env_vars_from_overrides_are_sorted_by_name() {
        let overrides = HashMap::from([
            ("CHARLIE".to_string(), "3".to_string()),
            ("ALPHA".to_string(), "1".to_string()),
            ("ECHO".to_string(), "5".to_string()),
            ("BRAVO".to_string(), "2".to_string()),
            ("DELTA".to_string(), "4".to_string()),
        ]);

        let names: Vec<String> = env_vars_from_overrides(&overrides)
            .into_iter()
            .map(|env_var| env_var.name)
            .collect();

        assert_eq!(names, ["ALPHA", "BRAVO", "CHARLIE", "DELTA", "ECHO"]);
    }
}
