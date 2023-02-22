use snafu::Snafu;
use stackable_airflow_crd::AirflowCluster;
use stackable_operator::product_config::ProductConfigManager;

use std::sync::Arc;
use strum::{EnumDiscriminants, IntoStaticStr};

use super::types::{BuiltClusterResource, FetchedAdditionalData};

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("placegholder"))]
    Placeholder,
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub fn build_cluster_resources(
    druid: Arc<AirflowCluster>,
    additional_data: FetchedAdditionalData,
    product_config: &ProductConfigManager,
) -> Result<Vec<BuiltClusterResource>> {
    Ok(vec![])
}
