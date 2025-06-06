use stackable_operator::{
    commons::affinity::{
        StackableAffinityFragment, affinity_between_cluster_pods, affinity_between_role_pods,
    },
    k8s_openapi::api::core::v1::{PodAffinity, PodAntiAffinity},
};

use crate::crd::{APP_NAME, AirflowRole};

/// Used for all [`AirflowRole`]s besides executors.
pub fn get_affinity(cluster_name: &str, role: &AirflowRole) -> StackableAffinityFragment {
    get_affinity_for_role(cluster_name, &role.to_string())
}

/// There is no [`AirflowRole`] for executors (only for workers), so let's have a special case here.
pub fn get_executor_affinity(cluster_name: &str) -> StackableAffinityFragment {
    get_affinity_for_role(cluster_name, "executor")
}

fn get_affinity_for_role(cluster_name: &str, role: &str) -> StackableAffinityFragment {
    let affinity_between_cluster_pods = affinity_between_cluster_pods(APP_NAME, cluster_name, 20);
    let affinity_between_role_pods = affinity_between_role_pods(APP_NAME, cluster_name, role, 70);

    StackableAffinityFragment {
        pod_affinity: Some(PodAffinity {
            preferred_during_scheduling_ignored_during_execution: Some(vec![
                affinity_between_cluster_pods,
            ]),
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
        kube::runtime::reflector::ObjectRef,
        role_utils::RoleGroupRef,
    };

    use crate::crd::{AirflowExecutor, AirflowRole, v1alpha1};

    #[rstest]
    #[case(AirflowRole::Worker)]
    #[case(AirflowRole::Scheduler)]
    #[case(AirflowRole::Webserver)]
    fn test_affinity_defaults(#[case] role: AirflowRole) {
        let cluster = "
        apiVersion: airflow.stackable.tech/v1alpha1
        kind: AirflowCluster
        metadata:
          name: airflow
        spec:
          image:
            productVersion: 2.10.5
          clusterConfig:
            credentialsSecret: airflow-credentials
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
        let airflow: v1alpha1::AirflowCluster =
            serde_yaml::with::singleton_map_recursive::deserialize(deserializer).unwrap();

        let rolegroup_ref = RoleGroupRef {
            cluster: ObjectRef::from_obj(&airflow),
            role: role.to_string(),
            role_group: "default".to_string(),
        };

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

        let affinity = airflow
            .merged_config(&role, &rolegroup_ref)
            .unwrap()
            .affinity;

        assert_eq!(affinity, expected);
    }

    #[test]
    fn test_executor_affinity_defaults() {
        let cluster = "
        apiVersion: airflow.stackable.tech/v1alpha1
        kind: AirflowCluster
        metadata:
          name: airflow
        spec:
          image:
            productVersion: 2.10.5
          clusterConfig:
            credentialsSecret: airflow-credentials
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
        let airflow: v1alpha1::AirflowCluster =
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
            AirflowExecutor::CeleryExecutor { .. } => unreachable!(),
            AirflowExecutor::KubernetesExecutor {
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
