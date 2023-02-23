use snafu::Snafu;
use stackable_airflow_crd::AirflowCluster;
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
    airflow_db: &Arc<AirflowCluster>,
    built_cluster_resources: Vec<BuiltClusterResource>,
) -> Result<Action> {
    /*
       let mut cluster_resources = ClusterResources::new(
           APP_NAME,
           OPERATOR_NAME,
           AIRFLOW_CONTROLLER_NAME,
           &airflow.object_ref(&()),
       )
       .context(CreateClusterResourcesSnafu)?;
    */

    // TODO
    /*
    cluster_resources
        .delete_orphaned_resources(client)
        .await
        .context(DeleteOrphanedResourcesSnafu)?;
     */

    Ok(Action::await_change())
}
