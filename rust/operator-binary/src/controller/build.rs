pub mod role_group_builder;

use std::marker::PhantomData;

use stackable_operator::{
    builder::meta::ObjectMetaBuilder,
    commons::rbac::build_rbac_resources,
    crd::listener,
    k8s_openapi::api::core::v1::{ServicePort, ServiceSpec},
    kvp::{Annotations, Labels},
    role_utils::RoleGroupRef,
};

use crate::{
    crd::{
        APP_NAME, AirflowExecutor, AirflowRole, Container, HTTP_PORT, HTTP_PORT_NAME,
        METRICS_PORT, METRICS_PORT_NAME, OPERATOR_NAME,
    },
    framework::{
        self,
        builder::meta::ownerreference_from_resource,
        types::operator::*,
    },
};

use super::{
    AIRFLOW_CONTROLLER_NAME, KubernetesResources, Prepared, ValidatedAirflowCluster,
    ValidatedRoleConfig,
};
use self::role_group_builder::RoleGroupBuilder;

fn main_container_for_role(_role: &AirflowRole) -> Container {
    Container::Airflow
}

pub(crate) fn build(validated: &ValidatedAirflowCluster) -> KubernetesResources<Prepared> {
    let mut stateful_sets = Vec::new();
    let mut config_maps = Vec::new();
    let mut services = Vec::new();
    let mut pod_disruption_budgets = Vec::new();
    let mut listeners = Vec::new();

    // --- RBAC ---
    let rbac_labels = build_recommended_labels(validated, "rbac", "rbac");

    let (rbac_sa, rbac_rolebinding) = build_rbac_resources(validated, APP_NAME, rbac_labels)
        .expect(
            "RBAC resources should be created because the validated cluster has valid metadata",
        );

    // --- Executor template ConfigMaps (pre-built in validate stage) ---
    config_maps.extend(validated.executor_template_config_maps.clone());

    // --- Per-role/rolegroup resources ---
    for (airflow_role, role_groups) in &validated.role_groups {
        // PDBs
        if let Some(role_config) = validated.role_configs.get(airflow_role) {
            if let Some(pdb) = build_pdb(validated, airflow_role, role_config) {
                pod_disruption_budgets.push(pdb);
            }
        }

        // Group listeners (only Webserver)
        if let Some(role_config) = validated.role_configs.get(airflow_role) {
            if let (Some(listener_class), Some(listener_name)) = (
                &role_config.listener_class,
                &role_config.group_listener_name,
            ) {
                listeners.push(build_group_listener(
                    validated,
                    airflow_role,
                    listener_class.clone(),
                    listener_name.clone(),
                ));
            }
        }

        for (rolegroup_name, role_group_config) in role_groups {
            let rolegroup_ref = validated.rolegroup_ref(airflow_role, rolegroup_name);

            let main_container = main_container_for_role(airflow_role);

            // Services
            services.push(build_headless_service(validated, &rolegroup_ref));
            services.push(build_metrics_service(validated, &rolegroup_ref));

            // ConfigMap + StatefulSet via RoleGroupBuilder
            let pod_data = validated
                .precomputed_pod_data
                .get(airflow_role)
                .and_then(|groups| groups.get(rolegroup_name))
                .expect(
                    "PrecomputedPodData should exist for every role group \
                    because validate_cluster computes it for each one",
                );

            let builder = RoleGroupBuilder::new(
                validated,
                role_group_config,
                rolegroup_ref,
                airflow_role.clone(),
                main_container,
                pod_data,
            );

            config_maps.push(builder.build_config_map());
            stateful_sets.push(builder.build_stateful_set());
        }
    }

    KubernetesResources {
        stateful_sets,
        config_maps,
        services,
        service_accounts: vec![rbac_sa],
        role_bindings: vec![rbac_rolebinding],
        pod_disruption_budgets,
        listeners,
        _status: PhantomData,
    }
}

fn build_pdb(
    cluster: &ValidatedAirflowCluster,
    role: &AirflowRole,
    role_config: &ValidatedRoleConfig,
) -> Option<stackable_operator::k8s_openapi::api::policy::v1::PodDisruptionBudget> {
    if !role_config.pdb_enabled {
        return None;
    }

    let max_unavailable = role_config.pdb_max_unavailable.unwrap_or(match role {
        AirflowRole::Worker => match &cluster.executor {
            AirflowExecutor::KubernetesExecutors { .. } => return None,
            _ => 1,
        },
        _ => 1,
    });

    Some({
        framework::builder::pdb::pod_disruption_budget_builder_with_role(
            cluster,
            &ProductName::from_str_unsafe(APP_NAME),
            &RoleName::from_str_unsafe(&role.to_string()),
            &OperatorName::from_str_unsafe(OPERATOR_NAME),
            &ControllerName::from_str_unsafe(AIRFLOW_CONTROLLER_NAME),
        )
        .with_max_unavailable(max_unavailable)
        .build()
    })
}

fn build_headless_service(
    cluster: &ValidatedAirflowCluster,
    rolegroup_ref: &RoleGroupRef<ValidatedAirflowCluster>,
) -> stackable_operator::k8s_openapi::api::core::v1::Service {
    let metadata = ObjectMetaBuilder::new()
        .name(format!("{}-headless", rolegroup_ref.object_name()))
        .namespace(&cluster.namespace)
        .ownerreference(ownerreference_from_resource(cluster, None, Some(true)))
        .with_labels(build_recommended_labels(
            cluster,
            &rolegroup_ref.role,
            &rolegroup_ref.role_group,
        ))
        .build();

    stackable_operator::k8s_openapi::api::core::v1::Service {
        metadata,
        spec: Some(ServiceSpec {
            type_: Some("ClusterIP".to_string()),
            cluster_ip: Some("None".to_string()),
            ports: Some(vec![ServicePort {
                name: Some(HTTP_PORT_NAME.to_string()),
                port: HTTP_PORT.into(),
                protocol: Some("TCP".to_string()),
                ..ServicePort::default()
            }]),
            selector: Some(
                build_role_group_selector_labels(
                    cluster,
                    &rolegroup_ref.role,
                    &rolegroup_ref.role_group,
                )
                .into(),
            ),
            publish_not_ready_addresses: Some(true),
            ..ServiceSpec::default()
        }),
        status: None,
    }
}

fn build_metrics_service(
    cluster: &ValidatedAirflowCluster,
    rolegroup_ref: &RoleGroupRef<ValidatedAirflowCluster>,
) -> stackable_operator::k8s_openapi::api::core::v1::Service {
    let metadata = ObjectMetaBuilder::new()
        .name(format!("{}-metrics", rolegroup_ref.object_name()))
        .namespace(&cluster.namespace)
        .ownerreference(ownerreference_from_resource(cluster, None, Some(true)))
        .with_labels(build_recommended_labels(
            cluster,
            &rolegroup_ref.role,
            &rolegroup_ref.role_group,
        ))
        .with_labels(prometheus_labels())
        .with_annotations(prometheus_annotations())
        .build();

    stackable_operator::k8s_openapi::api::core::v1::Service {
        metadata,
        spec: Some(ServiceSpec {
            type_: Some("ClusterIP".to_string()),
            cluster_ip: Some("None".to_string()),
            ports: Some(vec![ServicePort {
                name: Some(METRICS_PORT_NAME.to_string()),
                port: METRICS_PORT.into(),
                protocol: Some("TCP".to_string()),
                ..ServicePort::default()
            }]),
            selector: Some(
                build_role_group_selector_labels(
                    cluster,
                    &rolegroup_ref.role,
                    &rolegroup_ref.role_group,
                )
                .into(),
            ),
            publish_not_ready_addresses: Some(true),
            ..ServiceSpec::default()
        }),
        status: None,
    }
}

pub(super) fn build_recommended_labels(
    cluster: &ValidatedAirflowCluster,
    role: &str,
    role_group: &str,
) -> Labels {
    framework::kvp::label::recommended_labels(
        cluster,
        &ProductName::from_str_unsafe(APP_NAME),
        &ProductVersion::from_str_unsafe(&cluster.image.app_version_label_value.to_string()),
        &OperatorName::from_str_unsafe(OPERATOR_NAME),
        &ControllerName::from_str_unsafe(AIRFLOW_CONTROLLER_NAME),
        &RoleName::from_str_unsafe(role),
        &RoleGroupName::from_str_unsafe(role_group),
    )
}

pub(super) fn build_role_group_selector_labels(
    cluster: &ValidatedAirflowCluster,
    role: &str,
    role_group: &str,
) -> Labels {
    framework::kvp::label::role_group_selector(
        cluster,
        &ProductName::from_str_unsafe(APP_NAME),
        &RoleName::from_str_unsafe(role),
        &RoleGroupName::from_str_unsafe(role_group),
    )
}

fn prometheus_labels() -> Labels {
    Labels::try_from([("prometheus.io/scrape", "true")]).expect("should be a valid label")
}

fn prometheus_annotations() -> Annotations {
    Annotations::try_from([
        ("prometheus.io/path".to_owned(), "/metrics".to_owned()),
        ("prometheus.io/port".to_owned(), METRICS_PORT.to_string()),
        ("prometheus.io/scheme".to_owned(), "http".to_owned()),
        ("prometheus.io/scrape".to_owned(), "true".to_owned()),
    ])
    .expect("should be valid annotations")
}

fn build_group_listener(
    cluster: &ValidatedAirflowCluster,
    role: &AirflowRole,
    listener_class: String,
    listener_group_name: String,
) -> listener::v1alpha1::Listener {
    let metadata = ObjectMetaBuilder::new()
        .name(&listener_group_name)
        .namespace(&cluster.namespace)
        .ownerreference(ownerreference_from_resource(cluster, None, Some(true)))
        .with_labels(build_recommended_labels(cluster, &role.to_string(), "none"))
        .build();

    listener::v1alpha1::Listener {
        metadata,
        spec: listener::v1alpha1::ListenerSpec {
            class_name: Some(listener_class),
            ports: Some(listener_ports()),
            ..listener::v1alpha1::ListenerSpec::default()
        },
        status: None,
    }
}

fn listener_ports() -> Vec<listener::v1alpha1::ListenerPort> {
    vec![listener::v1alpha1::ListenerPort {
        name: HTTP_PORT_NAME.to_string(),
        port: HTTP_PORT.into(),
        protocol: Some("TCP".to_string()),
    }]
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, str::FromStr};

    use stackable_operator::{
        commons::{
            affinity::StackableAffinity,
            product_image_selection::ResolvedProductImage,
            resources::{NoRuntimeLimits, Resources},
        },
        k8s_openapi::api::core::v1::PodTemplateSpec,
        kube::Resource,
        kvp::LabelValue,
        product_logging::spec::{
            AutomaticContainerLogConfig, ContainerLogConfig, ContainerLogConfigChoice,
        },
        shared::time::Duration,
    };

    use super::*;
    use crate::{
        controller::{
            PrecomputedPodData, ValidatedAirflowCluster, ValidatedLogging, ValidatedRoleConfig,
            ValidatedRoleGroupConfig,
        },
        crd::{AirflowExecutor, AirflowRole, AirflowStorageConfig},
        framework::{
            product_logging::framework::ValidatedContainerLogConfigChoice,
            types::{
                kubernetes::{NamespaceName, Uid},
                operator::ClusterName,
            },
        },
    };

    #[test]
    fn test_build() {
        let validated = validated_cluster();

        let resources = build(&validated);

        assert_eq!(
            vec![
                "my-airflow-scheduler-default",
                "my-airflow-webserver-default",
            ],
            extract_resource_names(&resources.stateful_sets)
        );
        assert_eq!(
            vec![
                "my-airflow-scheduler-default-headless",
                "my-airflow-scheduler-default-metrics",
                "my-airflow-webserver-default-headless",
                "my-airflow-webserver-default-metrics",
            ],
            extract_resource_names(&resources.services)
        );
        assert_eq!(
            vec![
                "my-airflow-scheduler-default",
                "my-airflow-webserver-default",
            ],
            extract_resource_names(&resources.config_maps)
        );
        assert_eq!(
            vec!["my-airflow-serviceaccount"],
            extract_resource_names(&resources.service_accounts)
        );
        assert_eq!(
            vec!["my-airflow-rolebinding"],
            extract_resource_names(&resources.role_bindings)
        );
        assert_eq!(
            vec!["my-airflow-scheduler", "my-airflow-webserver"],
            extract_resource_names(&resources.pod_disruption_budgets)
        );
        assert_eq!(
            vec!["my-airflow-webserver"],
            extract_resource_names(&resources.listeners)
        );
    }

    fn extract_resource_names(resources: &[impl Resource]) -> Vec<&str> {
        let mut names: Vec<&str> = resources
            .iter()
            .filter_map(|r| r.meta().name.as_ref())
            .map(|n| n.as_str())
            .collect();
        names.sort();
        names
    }

    fn validated_cluster() -> ValidatedAirflowCluster {
        let image = ResolvedProductImage {
            product_version: "2.10.4".to_owned(),
            app_version_label_value: LabelValue::from_str("2.10.4-stackable0.0.0-dev")
                .expect("valid label value"),
            image: "oci.stackable.tech/sdp/airflow:2.10.4-stackable0.0.0-dev".to_string(),
            image_pull_policy: "Always".to_owned(),
            pull_secrets: None,
        };

        let logging = ValidatedLogging {
            airflow_container: ValidatedContainerLogConfigChoice::Automatic(
                AutomaticContainerLogConfig::default(),
            ),
            vector_container: None,
            git_sync_container_log_config: ContainerLogConfig {
                choice: Some(ContainerLogConfigChoice::Automatic(
                    AutomaticContainerLogConfig::default(),
                )),
            },
        };

        let role_group_config = ValidatedRoleGroupConfig {
            resources: Resources::<AirflowStorageConfig, NoRuntimeLimits>::default(),
            logging: logging.clone(),
            affinity: StackableAffinity::default(),
            graceful_shutdown_timeout: Duration::from_secs(120),
            config_file_content: String::new(),
        };

        let pod_data = PrecomputedPodData {
            env_vars: vec![],
            airflow_commands: vec!["airflow webserver".to_string()],
            auth_volumes: vec![],
            auth_volume_mounts: vec![],
            extra_volumes: vec![],
            extra_volume_mounts: vec![],
            git_sync_containers: vec![],
            git_sync_init_containers: vec![],
            git_sync_volumes: vec![],
            git_sync_volume_mounts: vec![],
            vector_container: None,
            service_account_name: "my-airflow-serviceaccount".to_string(),
            replicas: Some(1),
            pod_overrides: PodTemplateSpec::default(),
            executor: AirflowExecutor::KubernetesExecutors {
                common_configuration: Box::default(),
            },
            executor_template_configmap_name: None,
            listener_volume_claim_template: None,
        };

        let role_groups = BTreeMap::from([
            (
                AirflowRole::Webserver,
                BTreeMap::from([("default".to_string(), role_group_config.clone())]),
            ),
            (
                AirflowRole::Scheduler,
                BTreeMap::from([("default".to_string(), role_group_config)]),
            ),
        ]);

        let precomputed_pod_data = BTreeMap::from([
            (
                AirflowRole::Webserver,
                BTreeMap::from([("default".to_string(), pod_data.clone())]),
            ),
            (
                AirflowRole::Scheduler,
                BTreeMap::from([("default".to_string(), pod_data)]),
            ),
        ]);

        // Role configs: PDB enabled for both roles; Webserver also gets a listener
        let role_configs = BTreeMap::from([
            (
                AirflowRole::Scheduler,
                ValidatedRoleConfig {
                    pdb_enabled: true,
                    pdb_max_unavailable: None,
                    listener_class: None,
                    group_listener_name: None,
                },
            ),
            (
                AirflowRole::Webserver,
                ValidatedRoleConfig {
                    pdb_enabled: true,
                    pdb_max_unavailable: None,
                    listener_class: Some("cluster-internal".to_string()),
                    group_listener_name: Some("my-airflow-webserver".to_string()),
                },
            ),
        ]);

        ValidatedAirflowCluster::new(
            image,
            ClusterName::from_str_unsafe("my-airflow"),
            NamespaceName::from_str_unsafe("default"),
            Uid::from_str_unsafe("e6ac237d-a6d4-43a1-8135-f36506110912"),
            role_groups,
            precomputed_pod_data,
            vec![],
            role_configs,
            AirflowExecutor::KubernetesExecutors {
                common_configuration: Box::default(),
            },
        )
    }
}
