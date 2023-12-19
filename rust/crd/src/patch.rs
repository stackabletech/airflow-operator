use crate::{AirflowCluster, AirflowClusterConfig, AirflowClusterSpec};

trait ConvertCrdFrom<T> {
    fn convert_crd_from(cluster: T) -> Self;
}

impl ConvertCrdFrom<crate::v1alpha1::lib::AirflowCluster> for AirflowCluster {
    fn convert_crd_from(alpha: crate::v1alpha1::lib::AirflowCluster) -> Self {
        AirflowCluster {
            metadata: alpha.metadata,
            spec: AirflowClusterSpec {
                image: alpha.spec.image,
                cluster_config: AirflowClusterConfig {
                    authentication: alpha.spec.cluster_config.authentication,
                    credentials_secret: alpha.spec.cluster_config.credentials_secret,
                    dags_git_sync: alpha.spec.cluster_config.dags_git_sync,
                    load_examples: alpha.spec.cluster_config.load_examples,
                    listener_class: alpha.spec.cluster_config.listener_class,
                    vector_aggregator_config_map_name: alpha
                        .spec
                        .cluster_config
                        .vector_aggregator_config_map_name,
                    volumes: alpha.spec.cluster_config.volumes,
                    volume_mounts: alpha.spec.cluster_config.volume_mounts,
                },
                cluster_operation: alpha.spec.cluster_operation,
                webservers: alpha.spec.webservers,
                schedulers: alpha.spec.schedulers,
                executor: alpha.spec.executor,
            },
            status: alpha.status,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::patch::ConvertCrdFrom;
    use crate::AirflowCluster;
    use json_patch;
    use json_patch::{patch, Patch};
    use serde_json::from_value;
    use stackable_operator::kube::core::crd::merge_crds;
    use stackable_operator::kube::CustomResourceExt;

    #[test]
    fn test_cluster_patch() {
        let cluster = r#"
apiVersion: airflow.stackable.tech/v1alpha1
kind: AirflowCluster
metadata:
  name: airflow
spec:
  image:
    productVersion: 2.7.2
  clusterConfig:
    loadExamples: false
    credentialsSecret: simple-airflow-credentials
  webservers:
    roleGroups:
      default: {}
  celeryExecutors:
    roleGroups:
      default: {}
  schedulers:
    roleGroups:
      default: {}
          "#;

        let deserializer = serde_yaml::Deserializer::from_str(cluster);
        let mut airflow: AirflowCluster =
            serde_yaml::with::singleton_map_recursive::deserialize(deserializer).unwrap();
        let original = serde_json::json!(&airflow);

        // change something
        airflow.spec.cluster_config.load_examples = true;

        let patched = serde_json::json!(&airflow);
        let p = json_patch::diff(&original, &patched);
        println!("{:#?}", p);

        assert_eq!(
            p,
            from_value::<Patch>(serde_json::json!([
              { "op": "replace", "path": "/spec/clusterConfig/loadExamples", "value": true },
            ]))
            .unwrap()
        );

        let mut doc = original.clone();
        patch(&mut doc, &p).unwrap();
        assert_eq!(doc, patched);
    }

    #[test]
    fn test_cluster_conversion() {
        let cluster = r#"
apiVersion: airflow.stackable.tech/v1alpha1
kind: AirflowCluster
metadata:
  name: airflow
spec:
  image:
    productVersion: 2.7.2
  clusterConfig:
    loadExamples: false
    exposeConfig: false
    credentialsSecret: simple-airflow-credentials
  webservers:
    roleGroups:
      default: {}
  celeryExecutors:
    roleGroups:
      default: {}
  schedulers:
    roleGroups:
      default: {}
          "#;

        let deserializer = serde_yaml::Deserializer::from_str(cluster);
        let airflow_alpha: crate::v1alpha1::lib::AirflowCluster =
            serde_yaml::with::singleton_map_recursive::deserialize(deserializer).unwrap();
        let original = serde_json::json!(&airflow_alpha);
        let airflow_beta = AirflowCluster::convert_crd_from(airflow_alpha);
        let patched = serde_json::json!(&airflow_beta);
        let p = json_patch::diff(&original, &patched);

        assert_eq!(
            p,
            from_value::<Patch>(serde_json::json!([
              { "op": "replace", "path": "/apiVersion" , "value": "airflow.stackable.tech/v1beta1"},
                { "op": "remove", "path": "/spec/clusterConfig/exposeConfig" },
            ]))
            .unwrap()
        );
    }

    #[test]
    fn test_crd_merge() {
        let crd_alpha = crate::v1alpha1::lib::AirflowCluster::crd();
        let crd_beta = AirflowCluster::crd();
        let crd_all_versions = vec![crd_alpha.clone(), crd_beta.clone()];

        // get multi-version schema where v1beta1 is the stored version
        let crd_composite = merge_crds(crd_all_versions.clone(), "v1beta1").unwrap();
        let yaml = serde_yaml::to_string(&crd_composite).unwrap().to_string();
        println!("{}", yaml);
    }
}
