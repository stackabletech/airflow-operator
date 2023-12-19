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

    #[derive(Clone, Debug, PartialEq)]
    struct TestStruct {
        test_field: String,
    }
    #[derive(Clone, Debug)]
    struct CustomResourceV1Alpha1 {
        field_alpha: TestStruct,
    }
    #[derive(Clone, Debug)]
    struct CustomResourceV1Beta1 {
        field_alpha: Vec<TestStruct>,
    }
    #[derive(Debug)]
    struct CustomResourceV1 {
        field_alpha: Option<Vec<TestStruct>>,
    }

    trait TestConvertibleToV1Beta {
        fn v1alpha1_to_v1beta1(&self) -> CustomResourceV1Beta1;
    }

    trait TestConvertibleToV1 {
        fn v1beta1_to_v1(&self) -> CustomResourceV1;
    }

    impl TestConvertibleToV1Beta for CustomResourceV1Alpha1 {
        fn v1alpha1_to_v1beta1(&self) -> CustomResourceV1Beta1 {
            CustomResourceV1Beta1 {
                field_alpha: vec![self.field_alpha.clone()],
            }
        }
    }

    impl TestConvertibleToV1 for CustomResourceV1Beta1 {
        fn v1beta1_to_v1(&self) -> CustomResourceV1 {
            CustomResourceV1 {
                field_alpha: Some(self.field_alpha.clone()),
            }
        }
    }

    #[test]
    fn test_conversions() {
        let s = "Sample v1alpha1 data".to_string();
        let v1alpha1 = CustomResourceV1Alpha1 {
            field_alpha: TestStruct {
                test_field: s.clone(),
            },
        };

        let v1beta1 = v1alpha1.v1alpha1_to_v1beta1();
        assert_eq!(
            vec![TestStruct {
                test_field: s.clone()
            }],
            v1beta1.field_alpha
        );

        let v1 = v1beta1.v1beta1_to_v1();
        assert_eq!(
            Some(vec![TestStruct {
                test_field: s.clone()
            }]),
            v1.field_alpha
        );
    }

    #[test]
    fn test_patch() {
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
    fn test_conversion() {
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
}
