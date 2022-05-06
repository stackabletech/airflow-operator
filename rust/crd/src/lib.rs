pub mod airflowdb;

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use stackable_operator::k8s_openapi::api::core::v1::{Volume, VolumeMount};
use stackable_operator::kube::CustomResource;
use stackable_operator::product_config_utils::{ConfigError, Configuration};
use stackable_operator::role_utils::Role;
use stackable_operator::schemars::{self, JsonSchema};
use strum::{Display, EnumIter, EnumString};

pub const APP_NAME: &str = "airflow";

#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "airflow.stackable.tech",
    version = "v1alpha1",
    kind = "AirflowCluster",
    plural = "airflowclusters",
    shortname = "airflow",
    status = "AirflowClusterStatus",
    namespaced,
    crates(
        kube_core = "stackable_operator::kube::core",
        k8s_openapi = "stackable_operator::k8s_openapi",
        schemars = "stackable_operator::schemars"
    )
)]
#[serde(rename_all = "camelCase")]
pub struct AirflowClusterSpec {
    /// Emergency stop button, if `true` then all pods are stopped without affecting configuration (as setting `replicas` to `0` would)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stopped: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    pub credentials_secret: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub statsd_exporter_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub load_examples: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expose_config: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volumes: Option<Vec<Volume>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_mounts: Option<Vec<VolumeMount>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub webservers: Option<Role<AirflowConfig>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedulers: Option<Role<AirflowConfig>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workers: Option<Role<AirflowConfig>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AirflowCredentials {
    pub admin_user: AdminUserCredentials,
    pub connections: Connections,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminUserCredentials {
    pub username: String,
    pub firstname: String,
    pub lastname: String,
    pub email: String,
    pub password: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Connections {
    pub secret_key: String,
    pub sqlalchemy_database_uri: String,
}

#[derive(
    Clone,
    Debug,
    Deserialize,
    Display,
    EnumIter,
    Eq,
    Hash,
    JsonSchema,
    PartialEq,
    Serialize,
    EnumString,
)]
pub enum AirflowRole {
    #[strum(serialize = "webserver")]
    Webserver,
    #[strum(serialize = "scheduler")]
    Scheduler,
    #[strum(serialize = "worker")]
    Worker,
}

impl AirflowRole {
    /// Returns the start commands for the different server types.
    pub fn get_commands(&self) -> Vec<String> {
        match &self {
            AirflowRole::Webserver => vec!["airflow webserver".to_string()],
            AirflowRole::Scheduler => vec!["airflow scheduler".to_string()],
            AirflowRole::Worker => vec!["airflow celery worker".to_string()],
        }
    }

    /// Will be used to expose service ports and - by extension - which roles should be
    /// created as services.
    pub fn get_http_port(&self) -> Option<u16> {
        match &self {
            AirflowRole::Webserver => Some(8080),
            AirflowRole::Scheduler => None,
            AirflowRole::Worker => None,
        }
    }
}

impl AirflowCluster {
    pub fn get_role(&self, role: AirflowRole) -> &Option<Role<AirflowConfig>> {
        match role {
            AirflowRole::Webserver => &self.spec.webservers,
            AirflowRole::Scheduler => &self.spec.schedulers,
            AirflowRole::Worker => &self.spec.workers,
        }
    }

    pub fn volumes(&self) -> Vec<Volume> {
        let tmp = self.spec.volumes.as_ref();
        tmp.iter().flat_map(|v| v.iter()).cloned().collect()
    }

    pub fn volume_mounts(&self) -> Vec<VolumeMount> {
        let tmp = self.spec.volume_mounts.as_ref();
        tmp.iter().flat_map(|v| v.iter()).cloned().collect()
    }
}

#[derive(Clone, Debug, Deserialize, Default, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AirflowConfig {}

impl AirflowConfig {
    pub const CREDENTIALS_SECRET_PROPERTY: &'static str = "credentialsSecret";
}

impl Configuration for AirflowConfig {
    type Configurable = AirflowCluster;

    fn compute_env(
        &self,
        cluster: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        Ok([(
            Self::CREDENTIALS_SECRET_PROPERTY.to_string(),
            Some(cluster.spec.credentials_secret.clone()),
        )]
        .into())
    }

    fn compute_cli(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        Ok(BTreeMap::new())
    }

    fn compute_files(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
        _file: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        Ok(BTreeMap::new())
    }
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AirflowClusterStatus {}

impl AirflowCluster {
    /// The name of the role-level load-balanced Kubernetes `Service`
    pub fn node_role_service_name(&self) -> Option<String> {
        self.metadata.name.clone()
    }
}

/// A reference to a [`AirflowCluster`]
#[derive(Clone, Default, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AirflowClusterRef {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

#[cfg(test)]
mod tests {
    use crate::airflowdb::AirflowDB;
    use crate::AirflowCluster;

    #[test]
    fn test_cluster_config() {
        let cluster: AirflowCluster = serde_yaml::from_str::<AirflowCluster>(
            "
        apiVersion: airflow.stackable.tech/v1alpha1
        kind: AirflowCluster
        metadata:
          name: airflow
        spec:
          version: 2.2.4
          executor: KubernetesExecutor
          loadExamples: true
          exposeConfig: true
          credentialsSecret: simple-airflow-credentials
          webservers:
            roleGroups:
              default:
                config: {}
          workers:
            roleGroups:
              default:
                config: {}
          schedulers:
            roleGroups:
              default:
                config: {}
          ",
        )
        .unwrap();

        let airflow_db = AirflowDB::for_airflow(&cluster);

        assert_eq!("2.2.4", airflow_db.unwrap().spec.airflow_version);
        assert_eq!("2.2.4", cluster.spec.version.unwrap_or_default());
        assert_eq!(
            "KubernetesExecutor",
            cluster.spec.executor.unwrap_or_default()
        );
        assert!(cluster.spec.load_examples.unwrap_or(false));
        assert!(cluster.spec.expose_config.unwrap_or(false));
    }
}
