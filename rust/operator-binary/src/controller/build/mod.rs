//! Builders that assemble Kubernetes resources from the validated cluster.

use std::marker::PhantomData;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::meta::ObjectMetaBuilder,
    kvp::Labels,
    v2::{builder::meta::ownerreference_from_resource, types::operator::RoleGroupName},
};

use crate::{
    controller::{
        KubernetesResources, Prepared, ValidatedCluster,
        build::resource::{
            config_map::build_rolegroup_config_map,
            executor::build_executor_template_config_map,
            listener::build_group_listener,
            pdb::build_pdb,
            rbac::{build_role_binding, build_service_account},
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
pub fn build(cluster: &ValidatedCluster) -> Result<KubernetesResources<Prepared>, Error> {
    let mut stateful_sets = vec![];
    let mut services = vec![];
    let mut listeners = vec![];
    let mut config_maps = vec![];
    let mut pod_disruption_budgets = vec![];

    // The Kubernetes-executor pod template (only built for the Kubernetes executor; the Celery
    // executor's workers are a regular role with its own role groups instead).
    if let Some(executor_template) = &cluster.cluster_config.executor_template {
        let executor_role_group = executor_role_group_name();
        let executor_config_map = build_rolegroup_config_map(
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
                build_rolegroup_config_map(
                    cluster,
                    &ValidatedCluster::role_name(role),
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
        service_accounts: vec![build_service_account(cluster)],
        role_bindings: vec![build_role_binding(cluster)],
        status: PhantomData,
    })
}

/// Returns an [`ObjectMetaBuilder`] pre-filled with the cluster's namespace, the resource
/// `name`, an owner reference back to the cluster, and the given recommended `labels`.
///
/// Consolidates the metadata chain repeated by the child-resource builders. Call sites that
/// need extra labels/annotations chain them onto the returned builder.
pub(crate) fn object_meta(
    cluster: &ValidatedCluster,
    name: impl Into<String>,
    labels: Labels,
) -> ObjectMetaBuilder {
    let mut builder = ObjectMetaBuilder::new();
    builder
        .name_and_namespace(cluster)
        .name(name)
        .ownerreference(ownerreference_from_resource(cluster, None, Some(true)))
        .with_labels(labels);
    builder
}

#[cfg(test)]
pub(crate) mod test_support {
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

    /// The expected `app.kubernetes.io/version` label value for the given product version.
    ///
    /// The `-stackable` suffix carries the operator's own version, which is `0.0.0-dev` on main
    /// but rewritten by the release process — so tests must derive it rather than hardcode it,
    /// or they fail on release branches.
    pub fn app_version_label(product_version: &str) -> String {
        format!(
            "{product_version}-stackable{}",
            crate::built_info::PKG_VERSION
        )
    }

    /// A validated cluster with default `webserver`/`scheduler` role groups and the given executor
    /// (its `spec` key plus config, as standalone YAML), built via `validate_cluster` from a
    /// minimal test CR (mirroring `validate::tests::test_cluster`), since `ValidatedCluster`
    /// carries several resolved types (git-sync resources, validated logging, …) that are
    /// impractical to construct by hand.
    pub fn validated_cluster(executor_key: &str, executor_config: &str) -> ValidatedCluster {
        let cluster_yaml = r#"
        apiVersion: airflow.stackable.tech/v1alpha2
        kind: AirflowCluster
        metadata:
          name: my-airflow
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
    pub fn celery_executor_cluster() -> ValidatedCluster {
        validated_cluster("celeryExecutors", "{config: {}, roleGroups: {}}")
    }

    /// Validated cluster with a Kubernetes executor, which builds an executor pod-template
    /// ConfigMap instead of a worker role.
    pub fn kubernetes_executor_cluster() -> ValidatedCluster {
        validated_cluster("kubernetesExecutors", "{config: {}}")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use stackable_operator::kube::Resource;

    use super::{
        build,
        test_support::{app_version_label, celery_executor_cluster, kubernetes_executor_cluster},
    };

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
        let resources = build(&cluster).expect("build succeeds");

        assert_eq!(
            sorted_names(&resources.stateful_sets),
            [
                "my-airflow-scheduler-default",
                "my-airflow-webserver-default"
            ]
        );
        // One headless and one metrics Service per role group.
        assert_eq!(resources.services.len(), 4);
        assert_eq!(
            sorted_names(&resources.config_maps),
            [
                "my-airflow-scheduler-default",
                "my-airflow-webserver-default"
            ]
        );
        // The webserver is the only role with a group Listener.
        assert_eq!(sorted_names(&resources.listeners), ["my-airflow-webserver"]);
        // A default PDB per role (the Celery worker included).
        assert_eq!(
            sorted_names(&resources.pod_disruption_budgets),
            [
                "my-airflow-scheduler",
                "my-airflow-webserver",
                "my-airflow-worker"
            ]
        );
    }

    /// Locks the RBAC resource names, the roleRef, and the recommended label set against
    /// accidental drift. The fixture's cluster name deliberately differs from the product name so
    /// that swapped `name`/`instance` label values cannot pass unnoticed.
    #[test]
    fn build_produces_rbac() {
        let cluster = celery_executor_cluster();
        let resources = build(&cluster).expect("build succeeds");

        assert_eq!(
            sorted_names(&resources.service_accounts),
            ["my-airflow-serviceaccount"]
        );
        assert_eq!(
            sorted_names(&resources.role_bindings),
            ["my-airflow-rolebinding"]
        );

        let expected_labels = BTreeMap::from(
            [
                ("app.kubernetes.io/component", "none"),
                ("app.kubernetes.io/instance", "my-airflow"),
                (
                    "app.kubernetes.io/managed-by",
                    "airflow.stackable.tech_airflowcluster",
                ),
                ("app.kubernetes.io/name", "airflow"),
                ("app.kubernetes.io/role-group", "none"),
                ("app.kubernetes.io/version", &app_version_label("3.1.6")),
                ("stackable.tech/vendor", "Stackable"),
            ]
            .map(|(key, value)| (key.to_string(), value.to_string())),
        );
        let service_account = resources
            .service_accounts
            .first()
            .expect("a ServiceAccount is built");
        assert_eq!(
            service_account.metadata.labels,
            Some(expected_labels.clone())
        );

        let role_binding = resources
            .role_bindings
            .first()
            .expect("a RoleBinding is built");
        assert_eq!(role_binding.metadata.labels, Some(expected_labels));
        assert_eq!(role_binding.role_ref.name, "airflow-clusterrole");
    }

    /// The Kubernetes-executor branch of `build()` (moved here from `reconcile`) additionally emits
    /// the executor role-group ConfigMap and the executor pod-template ConfigMap; the Celery case
    /// does not.
    #[test]
    fn build_kubernetes_executor_adds_pod_template_config_maps() {
        let cluster = kubernetes_executor_cluster();
        let resources = build(&cluster).expect("build succeeds");

        assert_eq!(
            sorted_names(&resources.config_maps),
            [
                "my-airflow-executor-kubernetes",
                "my-airflow-executor-pod-template",
                "my-airflow-scheduler-default",
                "my-airflow-webserver-default",
            ]
        );
    }
}
