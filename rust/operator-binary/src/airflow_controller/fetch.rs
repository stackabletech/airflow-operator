use snafu::Snafu;
use stackable_airflow_crd::AirflowCluster;
use stackable_operator::client::Client;

use std::sync::Arc;
use strum::{EnumDiscriminants, IntoStaticStr};

use super::types::FetchedAdditionalData;

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("placeholder"))]
    Placeholder,
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub async fn fetch_additional_data(
    airflow_db: &Arc<AirflowCluster>,
    client: &Client,
) -> Result<FetchedAdditionalData> {
    Ok(FetchedAdditionalData {})
}
