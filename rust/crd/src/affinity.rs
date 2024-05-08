use stackable_operator::{
    commons::affinity::{
        affinity_between_cluster_pods, affinity_between_role_pods, StackableAffinityFragment,
    },
    k8s_openapi::api::core::v1::{PodAffinity, PodAntiAffinity},
};

use crate::{AirflowRole, APP_NAME};

pub fn get_affinity(cluster_name: &str, role: &AirflowRole) -> StackableAffinityFragment {
    let affinity_between_cluster_pods = affinity_between_cluster_pods(APP_NAME, cluster_name, 20);
    let affinity_between_role_pods =
        affinity_between_role_pods(APP_NAME, cluster_name, &role.to_string(), 70);

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

    use rstest::rstest;
    use std::collections::BTreeMap;

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

    use crate::{AirflowCluster, AirflowRole};

    #[rstest]
    // #[case(AirflowRole::Worker)]
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
            productVersion: 2.8.1
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
        let airflow: AirflowCluster =
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
                            match_label_keys: None,
                            mismatch_label_keys: None,
                            namespace_selector: None,
                            namespaces: None,
                            topology_key: "kubernetes.io/hostname".to_string(),
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
                            match_label_keys: None,
                            mismatch_label_keys: None,
                            namespace_selector: None,
                            namespaces: None,
                            topology_key: "kubernetes.io/hostname".to_string(),
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
}
