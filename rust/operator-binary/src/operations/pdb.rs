use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::pdb::PodDisruptionBudgetBuilder, client::Client, cluster_resources::ClusterResources,
    commons::pdb::PdbConfig, kube::ResourceExt,
};

use crate::{
    airflow_controller::AIRFLOW_CONTROLLER_NAME,
    crd::{APP_NAME, AirflowExecutor, AirflowRole, OPERATOR_NAME, v1alpha1},
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Cannot create PodDisruptionBudget for role [{role}]"))]
    CreatePdb {
        source: stackable_operator::builder::pdb::Error,
        role: String,
    },
    #[snafu(display("Cannot apply PodDisruptionBudget [{name}]"))]
    ApplyPdb {
        source: stackable_operator::cluster_resources::Error,
        name: String,
    },
}

pub async fn add_pdbs(
    pdb: &PdbConfig,
    airflow: &v1alpha1::AirflowCluster,
    role: &AirflowRole,
    client: &Client,
    cluster_resources: &mut ClusterResources,
) -> Result<(), Error> {
    if !pdb.enabled {
        return Ok(());
    }

    let max_unavailable = pdb.max_unavailable.unwrap_or(match role {
        AirflowRole::Scheduler => max_unavailable_schedulers(),
        AirflowRole::Webserver => max_unavailable_webservers(),
        AirflowRole::Processor => max_unavailable_processors(),
        AirflowRole::Triggerer => max_unavailable_triggerers(),
        AirflowRole::Worker => match airflow.spec.executor {
            AirflowExecutor::CeleryExecutor { .. } => max_unavailable_workers(),
            AirflowExecutor::KubernetesExecutor { .. } => {
                // In case Airflow creates the Pods, we don't want to influence that.
                return Ok(());
            }
        },
    });
    let pdb = PodDisruptionBudgetBuilder::new_with_role(
        airflow,
        APP_NAME,
        &role.to_string(),
        OPERATOR_NAME,
        AIRFLOW_CONTROLLER_NAME,
    )
    .with_context(|_| CreatePdbSnafu {
        role: role.to_string(),
    })?
    .with_max_unavailable(max_unavailable)
    .build();
    let pdb_name = pdb.name_any();
    cluster_resources
        .add(client, pdb)
        .await
        .with_context(|_| ApplyPdbSnafu { name: pdb_name })?;

    Ok(())
}

fn max_unavailable_schedulers() -> u16 {
    1
}

fn max_unavailable_workers() -> u16 {
    1
}

fn max_unavailable_webservers() -> u16 {
    1
}

fn max_unavailable_processors() -> u16 {
    1
}

fn max_unavailable_triggerers() -> u16 {
    1
}
