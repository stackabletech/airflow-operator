//! The update status step in the AirflowCluster controller
//!
//! Computes the cluster status from the applied resources and patches it onto
//! the AirflowCluster object.

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    client::Client,
    status::condition::{
        compute_conditions, operations::ClusterOperationsConditionBuilder,
        statefulset::StatefulSetConditionBuilder,
    },
};
use strum::{EnumDiscriminants, IntoStaticStr};

use super::{Applied, KubernetesResources};
use crate::crd::{AirflowClusterStatus, OPERATOR_NAME, v1alpha2};

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("failed to update status"))]
    UpdateStatus {
        source: stackable_operator::client::Error,
    },
}

pub async fn update_status(
    client: &Client,
    airflow: &v1alpha2::AirflowCluster,
    applied_resources: KubernetesResources<Applied>,
) -> Result<(), Error> {
    let mut ss_cond_builder = StatefulSetConditionBuilder::default();
    for stateful_set in applied_resources.stateful_sets {
        ss_cond_builder.add(stateful_set);
    }

    let cluster_operation_cond_builder =
        ClusterOperationsConditionBuilder::new(&airflow.spec.cluster_operation);

    let status = AirflowClusterStatus {
        conditions: compute_conditions(
            airflow,
            &[&ss_cond_builder, &cluster_operation_cond_builder],
        ),
    };

    client
        .apply_patch_status(OPERATOR_NAME, airflow, &status)
        .await
        .context(UpdateStatusSnafu)?;

    Ok(())
}
