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
        commons::affinity::{StackableAffinity, StackableNodeSelector},
        config::fragment::validate,
        k8s_openapi::api::core::v1::{
            NodeAffinity, NodeSelector, NodeSelectorRequirement, NodeSelectorTerm,
        },
        kube::runtime::reflector::ObjectRef,
        role_utils::RoleGroupRef,
    };

    use crate::{AirflowCluster, AirflowRole};

    use super::get_affinity;

    #[rstest]
    #[case(AirflowRole::Worker)]
    #[case(AirflowRole::Scheduler)]
    #[case(AirflowRole::Webserver)]
    fn test_affinity_defaults(#[case] role: AirflowRole) {
        let input = r#"
        apiVersion: airflow.stackable.tech/v1alpha1
        kind: AirflowCluster
        metadata:
          name: airflow
        spec:
          image:
            productVersion: 2.4.1
            stackableVersion: 23.4.0-rc2
          executor: CeleryExecutor
          loadExamples: true
          exposeConfig: false
          credentialsSecret: simple-airflow-credentials
          webservers:
            roleGroups:
              default:
                replicas: 1
          workers:
            roleGroups:
              default:
                replicas: 2
          schedulers:
            roleGroups:
              default:
                replicas: 1
        "#;
        let airflow: AirflowCluster = serde_yaml::from_str(input).expect("illegal test input");

        let rolegroup_ref = RoleGroupRef {
            cluster: ObjectRef::from_obj(&airflow),
            role: role.to_string(),
            role_group: "default".to_string(),
        };

        let expected: StackableAffinity = validate(get_affinity("airflow", &role)).unwrap();

        let affinity = airflow
            .merged_config(&role, &rolegroup_ref)
            .unwrap()
            .affinity;

        assert_eq!(affinity, expected);
    }

    #[test]
    fn test_affinity_legacy_node_selector() {
        let input = r#"
        apiVersion: airflow.stackable.tech/v1alpha1
        kind: AirflowCluster
        metadata:
          name: airflow
        spec:
          image:
            productVersion: 2.4.1
            stackableVersion: 23.4.0-rc2
          executor: CeleryExecutor
          loadExamples: true
          exposeConfig: false
          credentialsSecret: simple-airflow-credentials
          webservers:
            roleGroups:
              default:
                replicas: 1
          workers:
            roleGroups:
              default:
                replicas: 2
          schedulers:
            roleGroups:
              default:
                replicas: 1
                selector:
                  matchLabels:
                    disktype: ssd
                  matchExpressions:
                    - key: topology.kubernetes.io/zone
                      operator: In
                      values:
                        - antarctica-east1
                        - antarctica-west1
        "#;

        let airflow: AirflowCluster = serde_yaml::from_str(input).expect("illegal test input");

        let expected: StackableAffinity = StackableAffinity {
            node_affinity: Some(NodeAffinity {
                preferred_during_scheduling_ignored_during_execution: None,
                required_during_scheduling_ignored_during_execution: Some(NodeSelector {
                    node_selector_terms: vec![NodeSelectorTerm {
                        match_expressions: Some(vec![NodeSelectorRequirement {
                            key: "topology.kubernetes.io/zone".to_string(),
                            operator: "In".to_string(),
                            values: Some(vec![
                                "antarctica-east1".to_string(),
                                "antarctica-west1".to_string(),
                            ]),
                        }]),
                        match_fields: None,
                    }],
                }),
            }),
            node_selector: Some(StackableNodeSelector {
                node_selector: BTreeMap::from([("disktype".to_string(), "ssd".to_string())]),
            }),
            ..validate(get_affinity("airflow", &AirflowRole::Scheduler)).unwrap()
        };

        let rolegroup_ref = RoleGroupRef {
            cluster: ObjectRef::from_obj(&airflow),
            role: AirflowRole::Scheduler.to_string(),
            role_group: "default".to_string(),
        };

        let affinity = airflow
            .merged_config(&AirflowRole::Scheduler, &rolegroup_ref)
            .unwrap()
            .affinity;

        assert_eq!(affinity, expected);
    }
}
