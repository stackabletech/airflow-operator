use stackable_operator::{
    commons::pdb::PdbConfig, k8s_openapi::api::policy::v1::PodDisruptionBudget,
    v2::builder::pdb::pod_disruption_budget_builder_with_role,
};

use crate::{
    controller::{ValidatedCluster, controller_name, operator_name, product_name},
    crd::{AirflowExecutor, AirflowRole},
};

/// Builds the [`PodDisruptionBudget`] for the given `role`, or `None` if PDBs are disabled.
///
/// Returns `None` for the worker role under the Kubernetes executor as well: in that case Airflow
/// creates the Pods itself, so we don't want to influence them with a PDB.
pub fn build_pdb(
    pdb: &PdbConfig,
    cluster: &ValidatedCluster,
    role: &AirflowRole,
) -> Option<PodDisruptionBudget> {
    if !pdb.enabled {
        return None;
    }

    let max_unavailable = pdb.max_unavailable.unwrap_or(match role {
        AirflowRole::Scheduler => max_unavailable_schedulers(),
        AirflowRole::Webserver => max_unavailable_webservers(),
        AirflowRole::DagProcessor => max_unavailable_dag_processors(),
        AirflowRole::Triggerer => max_unavailable_triggerers(),
        AirflowRole::Worker => match cluster.cluster_config.executor {
            AirflowExecutor::CeleryExecutors { .. } => max_unavailable_workers(),
            AirflowExecutor::KubernetesExecutors { .. } => {
                // In case Airflow creates the Pods, we don't want to influence that.
                return None;
            }
        },
    });

    let pdb = pod_disruption_budget_builder_with_role(
        cluster,
        &product_name(),
        &ValidatedCluster::role_name(role),
        &operator_name(),
        &controller_name(),
    )
    .with_max_unavailable(max_unavailable)
    .build();

    Some(pdb)
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

fn max_unavailable_dag_processors() -> u16 {
    1
}

fn max_unavailable_triggerers() -> u16 {
    1
}
