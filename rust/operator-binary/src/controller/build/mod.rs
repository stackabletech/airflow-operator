//! Builders that assemble Kubernetes resources from the validated cluster.

use snafu::{ResultExt, Snafu};
use stackable_operator::v2::types::operator::RoleGroupName;

use crate::{
    controller::{
        KubernetesResources, ValidatedCluster,
        build::resource::{
            config_map,
            executor::build_executor_template_config_map,
            listener::build_group_listener,
            pdb::build_pdb,
            service::{build_rolegroup_headless_service, build_rolegroup_metrics_service},
            statefulset::build_server_rolegroup_statefulset,
        },
        executor_role_group_name, executor_role_name,
    },
    crd::{AirflowConfigOverrides, Container},
};

pub mod graceful_shutdown;
pub mod lineage;
pub mod properties;
pub mod resource;
pub mod volumes;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build ConfigMap for role group {role_group}"))]
    ConfigMap {
        source: resource::config_map::Error,
        role_group: RoleGroupName,
    },

    #[snafu(display("failed to build StatefulSet for role group {role_group}"))]
    StatefulSet {
        source: resource::statefulset::Error,
        role_group: RoleGroupName,
    },

    #[snafu(display("failed to build the Kubernetes-executor pod-template ConfigMap"))]
    ExecutorTemplate { source: resource::executor::Error },
}

/// Builds every Kubernetes resource for the given validated cluster.
///
/// Does not need a Kubernetes client: every reference to another Kubernetes resource is already
/// dereferenced and validated by this point. Cluster configuration is likewise already validated,
/// so the errors returned here are resource-assembly failures only.
///
/// `service_account_name` is the name of the RBAC `ServiceAccount` the role-group Pods and the
/// Kubernetes-executor pod template run under (RBAC resources are built and applied separately,
/// in the reconcile step).
pub fn build(
    cluster: &ValidatedCluster,
    service_account_name: &str,
) -> Result<KubernetesResources, Error> {
    let mut stateful_sets = vec![];
    let mut services = vec![];
    let mut listeners = vec![];
    let mut config_maps = vec![];
    let mut pod_disruption_budgets = vec![];

    // The Kubernetes-executor pod template (only built for the Kubernetes executor; the Celery
    // executor's workers are a regular role with its own role groups instead).
    if let Some(executor_template) = &cluster.cluster_config.executor_template {
        let executor_role_group = executor_role_group_name();
        let executor_config_map = config_map::build_rolegroup_config_map(
            cluster,
            &executor_role_name(),
            &executor_role_group,
            // The Kubernetes-executor pod template does not apply webserver_config.py overrides.
            &AirflowConfigOverrides::default(),
            &executor_template.config.logging,
            &Container::Base,
        )
        .context(ConfigMapSnafu {
            role_group: executor_role_group,
        })?;
        config_maps.push(executor_config_map);

        let executor_template_config_map = build_executor_template_config_map(
            cluster,
            service_account_name,
            &executor_template.config,
            &executor_template.env_overrides,
            &executor_template.pod_overrides,
        )
        .context(ExecutorTemplateSnafu)?;
        config_maps.push(executor_template_config_map);
    }

    for (role, role_group_configs) in &cluster.role_groups {
        if let Some(role_config) = cluster.role_configs.get(role) {
            if let Some(pdb_config) = &role_config.pdb {
                pod_disruption_budgets.extend(build_pdb(pdb_config, cluster, role));
            }
            if let Some(listener_class) = &role_config.listener_class
                && let Some(group_listener_name) = &role_config.group_listener_name
            {
                listeners.push(build_group_listener(
                    cluster,
                    role,
                    listener_class.clone(),
                    group_listener_name.clone(),
                ));
            }
        }

        for (role_group_name, rg_config) in role_group_configs {
            let logging = &rg_config.config.logging;

            services.push(build_rolegroup_headless_service(
                cluster,
                role,
                role_group_name,
            ));
            services.push(build_rolegroup_metrics_service(
                cluster,
                role,
                role_group_name,
            ));
            config_maps.push(
                config_map::build_rolegroup_config_map(
                    cluster,
                    &role.role_name(),
                    role_group_name,
                    &rg_config.config_overrides,
                    logging,
                    &Container::Airflow,
                )
                .context(ConfigMapSnafu {
                    role_group: role_group_name.clone(),
                })?,
            );
            stateful_sets.push(
                build_server_rolegroup_statefulset(
                    cluster,
                    role,
                    role_group_name,
                    rg_config,
                    logging,
                    service_account_name,
                )
                .context(StatefulSetSnafu {
                    role_group: role_group_name.clone(),
                })?,
            );
        }
    }

    Ok(KubernetesResources {
        stateful_sets,
        services,
        listeners,
        config_maps,
        pod_disruption_budgets,
    })
}

#[cfg(test)]
mod tests {
    use stackable_operator::kube::Resource;

    use super::build;
    use crate::{
        controller::{
            ValidatedCluster, dereference::DereferencedObjects, validate::validate_cluster,
        },
        crd::{
            authentication::{AirflowClientAuthenticationDetailsResolved, FlaskRolesSyncMoment},
            authorization::AirflowAuthorizationResolved,
            v1alpha2,
        },
    };

    /// A validated cluster with default `webserver`/`scheduler` role groups and the given executor
    /// (its `spec` key plus config, as standalone YAML), built via `validate_cluster` from a
    /// minimal test CR (mirroring `validate::tests::test_cluster`), since `ValidatedCluster`
    /// carries several resolved types (git-sync resources, validated logging, …) that are
    /// impractical to construct by hand.
    fn validated_cluster(executor_key: &str, executor_config: &str) -> ValidatedCluster {
        let cluster_yaml = r#"
        apiVersion: airflow.stackable.tech/v1alpha2
        kind: AirflowCluster
        metadata:
          name: airflow
          namespace: default
          uid: e6ac237d-a6d4-43a1-8135-f36506110912
        spec:
          image:
            productVersion: 3.1.6
          clusterConfig:
            loadExamples: false
            exposeConfig: false
            credentialsSecretName: airflow-admin-credentials
            metadataDatabase:
              postgresql:
                host: airflow-postgresql
                database: airflow
                credentialsSecretName: airflow-postgresql-credentials
          webservers:
            config: {}
            roleGroups:
              default:
                config: {}
          schedulers:
            config: {}
            roleGroups:
              default:
                config: {}
        "#;
        // The executor block is inserted into the parsed document rather than spliced into the
        // YAML text, so the fixture does not depend on matching the template's indentation.
        let mut cluster_value: serde_yaml::Value =
            serde_yaml::from_str(cluster_yaml).expect("the test CR is valid YAML");
        cluster_value["spec"]
            .as_mapping_mut()
            .expect("the test CR has a spec mapping")
            .insert(
                executor_key.into(),
                serde_yaml::from_str(executor_config).expect("the executor config is valid YAML"),
            );
        let cluster: v1alpha2::AirflowCluster =
            serde_yaml::with::singleton_map_recursive::deserialize(cluster_value)
                .expect("the test CR deserialises");

        let dereferenced = DereferencedObjects {
            authentication_config: AirflowClientAuthenticationDetailsResolved {
                authentication_classes_resolved: vec![],
                user_registration: true,
                user_registration_role: "Public".to_string(),
                sync_roles_at: FlaskRolesSyncMoment::default(),
            },
            authorization_config: AirflowAuthorizationResolved { opa: None },
            resolved_lineage_config: None,
        };

        validate_cluster(&cluster, "oci.stackable.tech/sdp", dereferenced)
            .expect("test cluster validates")
    }

    /// Validated cluster with a Celery executor (its workers are provisioned via the queue, so no
    /// executor pod template is built).
    fn celery_executor_cluster() -> ValidatedCluster {
        validated_cluster("celeryExecutors", "{config: {}, roleGroups: {}}")
    }

    /// Validated cluster with a Kubernetes executor, which builds an executor pod-template
    /// ConfigMap instead of a worker role.
    fn kubernetes_executor_cluster() -> ValidatedCluster {
        validated_cluster("kubernetesExecutors", "{config: {}}")
    }

    fn sorted_names(resources: &[impl Resource]) -> Vec<&str> {
        let mut names: Vec<&str> = resources
            .iter()
            .filter_map(|resource| resource.meta().name.as_deref())
            .collect();
        names.sort();
        names
    }

    #[test]
    fn build_produces_expected_resource_names() {
        let cluster = celery_executor_cluster();
        let resources = build(&cluster, "airflow-serviceaccount").expect("build succeeds");

        assert_eq!(
            sorted_names(&resources.stateful_sets),
            ["airflow-scheduler-default", "airflow-webserver-default"]
        );
        // One headless and one metrics Service per role group.
        assert_eq!(resources.services.len(), 4);
        assert_eq!(
            sorted_names(&resources.config_maps),
            ["airflow-scheduler-default", "airflow-webserver-default"]
        );
        // The webserver is the only role with a group Listener.
        assert_eq!(sorted_names(&resources.listeners), ["airflow-webserver"]);
        // A default PDB per role (the Celery worker included).
        assert_eq!(
            sorted_names(&resources.pod_disruption_budgets),
            ["airflow-scheduler", "airflow-webserver", "airflow-worker"]
        );
    }

    /// The Kubernetes-executor branch of `build()` (moved here from `reconcile`) additionally emits
    /// the executor role-group ConfigMap and the executor pod-template ConfigMap; the Celery case
    /// does not.
    #[test]
    fn build_kubernetes_executor_adds_pod_template_config_maps() {
        let cluster = kubernetes_executor_cluster();
        let resources = build(&cluster, "airflow-serviceaccount").expect("build succeeds");

        assert_eq!(
            sorted_names(&resources.config_maps),
            [
                "airflow-executor-kubernetes",
                "airflow-executor-pod-template",
                "airflow-scheduler-default",
                "airflow-webserver-default",
            ]
        );
    }
}
