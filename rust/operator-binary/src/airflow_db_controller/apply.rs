use snafu::Snafu;
use stackable_airflow_crd::airflowdb::AirflowDB;
use stackable_operator::{client::Client, kube::runtime::controller::Action};

use std::sync::Arc;
use strum::{EnumDiscriminants, IntoStaticStr};

use super::types::BuiltClusterResource;

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("placeholder"))]
    Placeholder,
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub async fn apply_cluster_resources(
    client: &Client,
    airflow_db: &Arc<AirflowDB>,
    built_cluster_resources: Vec<BuiltClusterResource>,
) -> Result<Action> {
    Ok(Action::await_change())
}
