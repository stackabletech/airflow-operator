//! Ensures that `Pod`s are configured and running for each [`v1alpha2::AirflowCluster`]
use std::sync::Arc;

use const_format::concatcp;
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    cli::OperatorEnvironmentOptions,
    cluster_resources::ClusterResourceApplyStrategy,
    kube::{
        Resource,
        core::{DeserializeGuard, error_boundary},
        runtime::controller::Action,
    },
    logging::controller::ReconcilerError,
    shared::time::Duration,
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{
    controller::{
        apply::{self, Applier, ensure_random_secrets},
        build,
        update_status::{self, update_status},
    },
    crd::{AIRFLOW_OPERATOR_NAME, v1alpha2},
};

pub const AIRFLOW_CONTROLLER_NAME: &str = "airflowcluster";
pub const CONTAINER_IMAGE_BASE_NAME: &str = "airflow";

pub const AIRFLOW_FULL_CONTROLLER_NAME: &str =
    concatcp!(AIRFLOW_CONTROLLER_NAME, '.', AIRFLOW_OPERATOR_NAME);

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub operator_environment: OperatorEnvironmentOptions,
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("failed to apply the Kubernetes resources"))]
    ApplyResources { source: apply::Error },

    #[snafu(display("failed to build the Kubernetes resources"))]
    BuildResources { source: build::Error },

    #[snafu(display("failed to ensure the shared random Secrets exist"))]
    EnsureSecrets { source: apply::Error },

    #[snafu(display("failed to update the cluster status"))]
    UpdateStatus { source: update_status::Error },

    #[snafu(display("failed to dereference cluster resources"))]
    Dereference {
        source: crate::controller::dereference::Error,
    },

    #[snafu(display("failed to validate cluster configuration"))]
    Validate {
        source: crate::controller::validate::Error,
    },

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

    if airflow.meta().deletion_timestamp.is_some() {
        return Ok(Action::await_change());
    }

    let airflow = airflow
        .0
        .as_ref()
        .map_err(error_boundary::InvalidObject::clone)
        .context(InvalidAirflowClusterSnafu)?;

    let client = &ctx.client;

    let dereferenced = crate::controller::dereference::dereference(client, airflow)
        .await
        .context(DereferenceSnafu)?;

    let validated_cluster = crate::controller::validate::validate_cluster(
        airflow,
        &ctx.operator_environment.image_repository,
        dereferenced,
    )
    .context(ValidateSnafu)?;

    let resources = build::build(&validated_cluster).context(BuildResourcesSnafu)?;

    ensure_random_secrets(client, &validated_cluster)
        .await
        .context(EnsureSecretsSnafu)?;
    let applied = Applier::new(
        client,
        &validated_cluster,
        ClusterResourceApplyStrategy::from(&airflow.spec.cluster_operation),
        &airflow.spec.object_overrides,
    )
    .apply(resources)
    .await
    .context(ApplyResourcesSnafu)?;

    update_status(client, airflow, &applied)
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
        // root object is invalid, will be requeued when modified anyway
        Error::InvalidAirflowCluster { .. } => Action::await_change(),

        _ => Action::requeue(*Duration::from_secs(10)),
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use stackable_operator::{
        client::Client,
        commons::networking::DomainName,
        kube::{Client as KubeClient, Config},
        utils::cluster_info::KubernetesClusterInfo,
    };

    use super::*;

    /// The client points at a closed port, so any API call would fail the reconciliation: an `Ok`
    /// proves that a cluster being deleted returns before the reconciler touches the Kubernetes
    /// API, and because the spec is invalid, before the [`DeserializeGuard`] is unwrapped.
    #[test]
    fn reconcile_exits_early_for_deleted_cluster() {
        let airflow = serde_yaml::from_str(
            r#"
apiVersion: airflow.stackable.tech/v1alpha2
kind: AirflowCluster
metadata:
  name: airflow
  namespace: default
  deletionTimestamp: "2026-08-14T12:00:00Z"
spec: {}
"#,
        )
        .expect("YAML parses; the invalid spec is captured inside the DeserializeGuard");

        let action = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("current-thread tokio runtime")
            .block_on(async {
                let ctx = Arc::new(Ctx {
                    client: Client::new(
                        KubeClient::try_from(Config::new(
                            "http://127.0.0.1:1".parse().expect("valid static URI"),
                        ))
                        .expect("client from static config"),
                        None,
                        "default".to_owned(),
                        KubernetesClusterInfo {
                            cluster_domain: DomainName::from_str("cluster.local")
                                .expect("valid cluster domain"),
                        },
                    ),
                    operator_environment: OperatorEnvironmentOptions {
                        operator_namespace: "stackable-operators".to_owned(),
                        operator_service_name: "airflow-operator".to_owned(),
                        image_repository: "oci.stackable.tech/sdp".to_owned(),
                    },
                });

                reconcile_airflow(Arc::new(airflow), ctx).await
            })
            .expect("a deleted cluster reconciles without any API call");

        assert_eq!(action, Action::await_change());
    }
}
