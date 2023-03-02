mod apply;
mod build;
mod fetch;
mod types;

use snafu::{ResultExt, Snafu};
use stackable_airflow_crd::AirflowCluster;
use stackable_operator::product_config::ProductConfigManager;
use stackable_operator::{kube::runtime::controller::Action, logging::controller::ReconcilerError};
use std::{sync::Arc, time::Duration};

use strum::{EnumDiscriminants, IntoStaticStr};

use crate::airflow_controller::{
    apply::apply_cluster_resources, build::build_cluster_resources, fetch::fetch_additional_data,
};

pub const AIRFLOW_CONTROLLER_NAME: &str = "airflowcluster";
pub const DOCKER_IMAGE_BASE_NAME: &str = "airflow";

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub product_config: ProductConfigManager,
}

type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("failed to fetch additional information"))]
    Fetch { source: fetch::Error },
    #[snafu(display("failed to build cluster resources"))]
    Build { source: build::Error },
    #[snafu(display("failed to apply cluster resources"))]
    Apply { source: apply::Error },
}

pub async fn reconcile_airflow(airflow: Arc<AirflowCluster>, ctx: Arc<Ctx>) -> Result<Action> {
    tracing::info!("Starting AirflowCluster reconcile");

    let fetched_additional_data = fetch_additional_data(&airflow, &ctx.client)
        .await
        .context(FetchSnafu)?;
    let built_cluster_resources = build_cluster_resources(
        airflow.clone(),
        fetched_additional_data,
        &ctx.product_config,
    )
    .context(BuildSnafu)?;

    apply_cluster_resources(&ctx.client, airflow, built_cluster_resources)
        .await
        .context(ApplySnafu)
}

pub fn error_policy(_obj: Arc<AirflowCluster>, _error: &Error, _ctx: Arc<Ctx>) -> Action {
    Action::requeue(Duration::from_secs(5))
}
