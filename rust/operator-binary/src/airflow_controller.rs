//! Ensures that `Pod`s are configured and running for each [`v1alpha2::AirflowCluster`]
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use const_format::concatcp;
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    cli::OperatorEnvironmentOptions,
    cluster_resources::ClusterResourceApplyStrategy,
    commons::{random_secret_creation, rbac::build_rbac_resources},
    k8s_openapi::api::core::v1::EnvVar,
    kube::{
        ResourceExt,
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
    v2::cluster_resources::cluster_resources_new,
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{
    controller::{ValidatedCluster, build, controller_name, operator_name, product_name},
    crd::{
        APP_NAME, AirflowClusterStatus, OPERATOR_NAME,
        internal_secret::{
            FERNET_KEY_SECRET_KEY, INTERNAL_SECRET_SECRET_KEY, JWT_SECRET_SECRET_KEY,
        },
        v1alpha2,
    },
};

pub const AIRFLOW_CONTROLLER_NAME: &str = "airflowcluster";
pub const CONTAINER_IMAGE_BASE_NAME: &str = "airflow";

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
    #[snafu(display("failed to apply Kubernetes resource"))]
    ApplyResource {
        source: stackable_operator::cluster_resources::Error,
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

    #[snafu(display("failed to build the Kubernetes resources"))]
    BuildResources { source: build::Error },

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

    #[snafu(display("failed to build label"))]
    BuildLabel { source: LabelError },

    #[snafu(display("AirflowCluster object is invalid"))]
    InvalidAirflowCluster {
        source: error_boundary::InvalidObject,
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

    ensure_random_secrets(client, &validated_cluster).await?;

    let mut cluster_resources = cluster_resources_new(
        &product_name(),
        &operator_name(),
        &controller_name(),
        &validated_cluster.name,
        &validated_cluster.namespace,
        &validated_cluster.uid,
        ClusterResourceApplyStrategy::from(&airflow.spec.cluster_operation),
        &airflow.spec.object_overrides,
    );

    let required_labels = cluster_resources
        .get_required_labels()
        .context(BuildLabelSnafu)?;

    let (rbac_sa, rbac_rolebinding) =
        build_rbac_resources(airflow, APP_NAME, required_labels).context(BuildRBACObjectsSnafu)?;

    // The ServiceAccount name is deterministic on the built object, so the build step does not
    // depend on the applied ServiceAccount.
    let service_account_name = rbac_sa.name_any();

    cluster_resources
        .add(client, rbac_sa)
        .await
        .context(ApplyServiceAccountSnafu)?;
    cluster_resources
        .add(client, rbac_rolebinding)
        .await
        .context(ApplyRoleBindingSnafu)?;

    let resources =
        build::build(&validated_cluster, &service_account_name).context(BuildResourcesSnafu)?;

    let mut ss_cond_builder = StatefulSetConditionBuilder::default();

    // Apply order is: StatefulSets last (a changed mounted ConfigMap/Secret
    // must exist first, else Pods restart -- commons-operator#111).
    for service in resources.services {
        cluster_resources
            .add(client, service)
            .await
            .context(ApplyResourceSnafu)?;
    }
    for listener in resources.listeners {
        cluster_resources
            .add(client, listener)
            .await
            .context(ApplyResourceSnafu)?;
    }
    for config_map in resources.config_maps {
        cluster_resources
            .add(client, config_map)
            .await
            .context(ApplyResourceSnafu)?;
    }
    for pdb in resources.pod_disruption_budgets {
        cluster_resources
            .add(client, pdb)
            .await
            .context(ApplyResourceSnafu)?;
    }
    for statefulset in resources.stateful_sets {
        ss_cond_builder.add(
            cluster_resources
                .add(client, statefulset)
                .await
                .context(ApplyResourceSnafu)?,
        );
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

/// Ensures the three shared random Secrets (internal / JWT / Fernet) exist, creating any that are
/// missing. These are read-or-create client operations, so they cannot be part of the client-free
/// `build()` step.
async fn ensure_random_secrets(
    client: &stackable_operator::client::Client,
    cluster: &ValidatedCluster,
) -> Result<(), Error> {
    random_secret_creation::create_random_secret_if_not_exists(
        cluster.internal_secret_name().as_ref(),
        INTERNAL_SECRET_SECRET_KEY,
        256,
        cluster,
        client,
    )
    .await
    .context(InternalSecretSnafu)?;

    random_secret_creation::create_random_secret_if_not_exists(
        cluster.jwt_secret_name().as_ref(),
        JWT_SECRET_SECRET_KEY,
        256,
        cluster,
        client,
    )
    .await
    .context(InternalSecretSnafu)?;

    // https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/fernet.html#security-fernet
    // does not document how long the fernet key should be, but recommends using
    // python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
    // which returns 32 bytes.
    random_secret_creation::create_random_secret_if_not_exists(
        cluster.fernet_key_name().as_ref(),
        FERNET_KEY_SECRET_KEY,
        32,
        cluster,
        client,
    )
    .await
    .context(InternalSecretSnafu)?;

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
pub(crate) fn env_vars_from_overrides(env_overrides: &HashMap<String, String>) -> Vec<EnvVar> {
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
