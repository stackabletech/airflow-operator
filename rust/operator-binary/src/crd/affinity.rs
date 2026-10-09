use stackable_operator::{
    commons::{
        affinity::{
            StackableAffinityFragment, affinity_between_cluster_pods, affinity_between_role_pods,
        },
        opa::OpaConfig,
    },
    k8s_openapi::api::core::v1::{PodAffinity, PodAntiAffinity},
};

use crate::crd::{APP_NAME, AirflowRole};

/// Used for all [`AirflowRole`]s besides executors.
pub fn get_affinity(
    cluster_name: &str,
    role: &AirflowRole,
    opa_config: Option<&OpaConfig>,
) -> StackableAffinityFragment {
    let opa_config = match role {
        // Only the webserver is configured with the OPA auth manager, so only the webserver is
        // co-located with the OPA Pods.
        AirflowRole::Webserver => opa_config,
        AirflowRole::Scheduler
        | AirflowRole::Worker
        | AirflowRole::DagProcessor
        | AirflowRole::Triggerer => None,
    };
    get_affinity_for_role(cluster_name, &role.to_string(), opa_config)
}

/// There is no [`AirflowRole`] for executors (only for workers), so let's have a special case here.
pub fn get_executor_affinity(cluster_name: &str) -> StackableAffinityFragment {
    get_affinity_for_role(cluster_name, "executor", None)
}

fn get_affinity_for_role(
    cluster_name: &str,
    role: &str,
    opa_config: Option<&OpaConfig>,
) -> StackableAffinityFragment {
    // Built before the `let`s below, which shadow the helper functions with their results.
    let affinity_to_opa_pods = opa_config.map(|opa_config| {
        affinity_between_role_pods(
            "opa",
            &opa_config.config_map_name, // The discovery cm has the same name as the OpaCluster itself
            "server",
            50,
        )
    });
    let affinity_between_cluster_pods = affinity_between_cluster_pods(APP_NAME, cluster_name, 20);
    let affinity_between_role_pods = affinity_between_role_pods(APP_NAME, cluster_name, role, 70);

    let mut pod_affinities = vec![affinity_between_cluster_pods];
    pod_affinities.extend(affinity_to_opa_pods);

    StackableAffinityFragment {
        pod_affinity: Some(PodAffinity {
            preferred_during_scheduling_ignored_during_execution: Some(pod_affinities),
            required_during_scheduling_ignored_during_execution: None,
        }),
        pod_anti_affinity: Some(PodAntiAffinity {
            preferred_during_scheduling_ignored_during_execution: Some(vec![
                affinity_between_role_pods,
            ]),
            required_during_scheduling_ignored_during_execution: None,
        }),
        node_affinity: None,
        node_selector: None,
    }
}
#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use rstest::rstest;
    use stackable_operator::{
        commons::affinity::StackableAffinity,
        k8s_openapi::{
            api::core::v1::{
                PodAffinity, PodAffinityTerm, PodAntiAffinity, WeightedPodAffinityTerm,
            },
            apimachinery::pkg::apis::meta::v1::LabelSelector,
        },
        kube::ResourceExt,
        role_utils::GenericRoleConfig,
        v2::role_utils::{GenericCommonConfig, with_validated_config},
    };

    use crate::crd::{
        AirflowConfig, AirflowConfigFragment, AirflowConfigOverrides, AirflowExecutor, AirflowRole,
        v1alpha2,
    };

    #[rstest]
    #[case(AirflowRole::Worker)]
    #[case(AirflowRole::Scheduler)]
    #[case(AirflowRole::Webserver)]
    fn test_affinity_defaults(#[case] role: AirflowRole) {
        let cluster = "
        apiVersion: airflow.stackable.tech/v1alpha2
        kind: AirflowCluster
        metadata:
          name: airflow
        spec:
          image:
            productVersion: 3.3.1
          clusterConfig:
            credentialsSecretName: airflow-admin-credentials
            metadataDatabase:
              postgresql:
                host: airflow-postgresql
                database: airflow
                credentialsSecretName: airflow-postgresql-credentials
            celeryResultsBackend:
              postgresql:
                host: airflow-postgresql
                database: airflow
                credentialsSecretName: airflow-postgresql-credentials
            celeryBroker:
              redis:
                host: airflow-redis-master
                credentialsSecretName: airflow-redis-credentials
            authorization:
              opa:
                configMapName: simple-opa
                package: airflow
          webservers:
            roleGroups:
              default:
                replicas: 1
          celeryExecutors:
            roleGroups:
              default:
                replicas: 2
          schedulers:
            roleGroups:
              default:
                replicas: 1
        ";

        let deserializer = serde_yaml::Deserializer::from_str(cluster);
        let airflow: v1alpha2::AirflowCluster =
            serde_yaml::with::singleton_map_recursive::deserialize(deserializer).unwrap();

        let resolved_role = airflow
            .get_role(&role)
            .expect("the role is defined in the test cluster");
        let default_config =
            AirflowConfig::default_config(&airflow.name_any(), &role, airflow.get_opa_config());
        let rolegroup = resolved_role
            .role_groups
            .get("default")
            .expect("the 'default' role group is defined in the test cluster");

        let mut expected_pod_affinities = vec![WeightedPodAffinityTerm {
            pod_affinity_term: PodAffinityTerm {
                label_selector: Some(LabelSelector {
                    match_expressions: None,
                    match_labels: Some(BTreeMap::from([
                        ("app.kubernetes.io/name".to_string(), "airflow".to_string()),
                        (
                            "app.kubernetes.io/instance".to_string(),
                            "airflow".to_string(),
                        ),
                    ])),
                }),
                topology_key: "kubernetes.io/hostname".to_string(),
                ..PodAffinityTerm::default()
            },
            weight: 20,
        }];
        // Only the webserver is configured with the OPA auth manager.
        if role == AirflowRole::Webserver {
            expected_pod_affinities.push(WeightedPodAffinityTerm {
                pod_affinity_term: PodAffinityTerm {
                    label_selector: Some(LabelSelector {
                        match_expressions: None,
                        match_labels: Some(BTreeMap::from([
                            ("app.kubernetes.io/name".to_string(), "opa".to_string()),
                            (
                                "app.kubernetes.io/instance".to_string(),
                                "simple-opa".to_string(),
                            ),
                            (
                                "app.kubernetes.io/component".to_string(),
                                "server".to_string(),
                            ),
                        ])),
                    }),
                    topology_key: "kubernetes.io/hostname".to_string(),
                    ..PodAffinityTerm::default()
                },
                weight: 50,
            });
        }

        let expected: StackableAffinity = StackableAffinity {
            node_affinity: None,
            node_selector: None,
            pod_affinity: Some(PodAffinity {
                required_during_scheduling_ignored_during_execution: None,
                preferred_during_scheduling_ignored_during_execution: Some(expected_pod_affinities),
            }),
            pod_anti_affinity: Some(PodAntiAffinity {
                required_during_scheduling_ignored_during_execution: None,
                preferred_during_scheduling_ignored_during_execution: Some(vec![
                    WeightedPodAffinityTerm {
                        pod_affinity_term: PodAffinityTerm {
                            label_selector: Some(LabelSelector {
                                match_expressions: None,
                                match_labels: Some(BTreeMap::from([
                                    ("app.kubernetes.io/name".to_string(), "airflow".to_string()),
                                    (
                                        "app.kubernetes.io/instance".to_string(),
                                        "airflow".to_string(),
                                    ),
                                    ("app.kubernetes.io/component".to_string(), role.to_string()),
                                ])),
                            }),
                            topology_key: "kubernetes.io/hostname".to_string(),
                            ..PodAffinityTerm::default()
                        },
                        weight: 70,
                    },
                ]),
            }),
        };

        let affinity = with_validated_config::<
            AirflowConfig,
            GenericCommonConfig,
            AirflowConfigFragment,
            GenericRoleConfig,
            AirflowConfigOverrides,
        >(rolegroup, &resolved_role, &default_config)
        .expect("the config should merge and validate")
        .config
        .config
        .affinity;

        assert_eq!(affinity, expected);
    }

    #[test]
    fn test_executor_affinity_defaults() {
        let cluster = "
        apiVersion: airflow.stackable.tech/v1alpha2
        kind: AirflowCluster
        metadata:
          name: airflow
        spec:
          image:
            productVersion: 3.3.1
          clusterConfig:
            credentialsSecretName: airflow-admin-credentials
            metadataDatabase:
              postgresql:
                host: airflow-postgresql
                database: airflow
                credentialsSecretName: airflow-postgresql-credentials
          webservers:
            roleGroups:
              default:
                replicas: 1
          schedulers:
            roleGroups:
              default:
                replicas: 1
          kubernetesExecutors: {}
          ";

        let deserializer = serde_yaml::Deserializer::from_str(cluster);
        let airflow: v1alpha2::AirflowCluster =
            serde_yaml::with::singleton_map_recursive::deserialize(deserializer).unwrap();

        let expected: StackableAffinity = StackableAffinity {
            node_affinity: None,
            node_selector: None,
            pod_affinity: Some(PodAffinity {
                required_during_scheduling_ignored_during_execution: None,
                preferred_during_scheduling_ignored_during_execution: Some(vec![
                    WeightedPodAffinityTerm {
                        pod_affinity_term: PodAffinityTerm {
                            label_selector: Some(LabelSelector {
                                match_expressions: None,
                                match_labels: Some(BTreeMap::from([
                                    ("app.kubernetes.io/name".to_string(), "airflow".to_string()),
                                    (
                                        "app.kubernetes.io/instance".to_string(),
                                        "airflow".to_string(),
                                    ),
                                ])),
                            }),
                            topology_key: "kubernetes.io/hostname".to_string(),
                            ..PodAffinityTerm::default()
                        },
                        weight: 20,
                    },
                ]),
            }),
            pod_anti_affinity: Some(PodAntiAffinity {
                required_during_scheduling_ignored_during_execution: None,
                preferred_during_scheduling_ignored_during_execution: Some(vec![
                    WeightedPodAffinityTerm {
                        pod_affinity_term: PodAffinityTerm {
                            label_selector: Some(LabelSelector {
                                match_expressions: None,
                                match_labels: Some(BTreeMap::from([
                                    ("app.kubernetes.io/name".to_string(), "airflow".to_string()),
                                    (
                                        "app.kubernetes.io/instance".to_string(),
                                        "airflow".to_string(),
                                    ),
                                    (
                                        "app.kubernetes.io/component".to_string(),
                                        "executor".to_string(),
                                    ),
                                ])),
                            }),
                            topology_key: "kubernetes.io/hostname".to_string(),
                            ..PodAffinityTerm::default()
                        },
                        weight: 70,
                    },
                ]),
            }),
        };

        let executor_config = match &airflow.spec.executor {
            AirflowExecutor::CeleryExecutors { .. } => unreachable!(),
            AirflowExecutor::KubernetesExecutors {
                common_configuration,
            } => &common_configuration.config,
        };
        let affinity = airflow
            .merged_executor_config(executor_config)
            .unwrap()
            .affinity;

        assert_eq!(affinity, expected);
    }
}
