mod apply;
mod build;
mod fetch;
mod types;

use snafu::{ResultExt, Snafu};
use stackable_airflow_crd::airflowdb::AirflowDB;
use stackable_operator::{kube::runtime::controller::Action, logging::controller::ReconcilerError};
use std::{sync::Arc, time::Duration};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::airflow_db_controller::{
    apply::apply_cluster_resources, build::build_cluster_resources, fetch::fetch_additional_data,
};

pub struct Ctx {
    pub client: stackable_operator::client::Client,
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

type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}

pub async fn reconcile_airflow_db(airflow_db: Arc<AirflowDB>, ctx: Arc<Ctx>) -> Result<Action> {
    tracing::info!("Starting AirflowDB reconcile");

    let fetched_additional_data = fetch_additional_data(&airflow_db, &ctx.client)
        .await
        .context(FetchSnafu)?;
    let built_cluster_resources =
        build_cluster_resources(airflow_db.clone(), fetched_additional_data).context(BuildSnafu)?;

    apply_cluster_resources(&ctx.client, airflow_db, built_cluster_resources)
        .await
        .context(ApplySnafu)
}

pub fn error_policy(_obj: Arc<AirflowDB>, _error: &Error, _ctx: Arc<Ctx>) -> Action {
    Action::requeue(Duration::from_secs(5))
}
