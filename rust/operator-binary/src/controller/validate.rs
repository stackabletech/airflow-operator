use std::collections::{BTreeMap, HashMap};

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    commons::product_image_selection::{self, ResolvedProductImage},
    config::merge::Merge,
    role_utils::RoleGroupRef,
};
use strum::IntoEnumIterator;

use super::dereference::DereferencedObjects;
use crate::{
    airflow_controller::CONTAINER_IMAGE_BASE_NAME,
    crd::{
        AirflowConfig, AirflowConfigOverrides, AirflowExecutor, AirflowRole, AirflowRoleType,
        authentication::AirflowClientAuthenticationDetailsResolved,
        authorization::AirflowAuthorizationResolved, v1alpha2,
    },
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to resolve product image"))]
    ResolveProductImage {
        source: product_image_selection::Error,
    },

    #[snafu(display("failed to resolve and merge config for role and role group"))]
    FailedToResolveConfig { source: crate::crd::Error },
}

/// Per-role configuration extracted during validation.
#[derive(Clone, Debug)]
pub struct ValidatedRoleConfig {
    pub pdb: Option<stackable_operator::commons::pdb::PdbConfig>,
    pub listener_class: Option<String>,
    pub group_listener_name: Option<String>,
}

/// Per-rolegroup configuration: the merged CRD config plus overrides.
///
/// `config_overrides` is kept as the typed [`AirflowConfigOverrides`] (role-group merged over
/// role); it is flattened into the rendered config file later, in the build step. This mirrors
/// hdfs-operator. `env_overrides` is already a flat map.
#[derive(Clone, Debug)]
pub struct ValidatedRoleGroupConfig {
    pub merged_config: AirflowConfig,
    pub config_overrides: AirflowConfigOverrides,
    pub env_overrides: HashMap<String, String>,
}

/// The validated cluster: proves that config merging succeeded for every role and
/// role group before any resources are created. It also carries the dereferenced
/// external references, so every downstream build step reads them from here.
#[derive(Clone, Debug)]
pub struct ValidatedAirflowCluster {
    pub image: ResolvedProductImage,
    pub role_groups: BTreeMap<AirflowRole, BTreeMap<String, ValidatedRoleGroupConfig>>,
    pub role_configs: BTreeMap<AirflowRole, ValidatedRoleConfig>,
    pub executor: AirflowExecutor,
    pub authentication_config: AirflowClientAuthenticationDetailsResolved,
    pub authorization_config: AirflowAuthorizationResolved,
}

pub fn validate_cluster(
    airflow: &v1alpha2::AirflowCluster,
    image_repository: &str,
    dereferenced: DereferencedObjects,
) -> Result<ValidatedAirflowCluster, Error> {
    let resolved_product_image = airflow
        .spec
        .image
        .resolve(
            CONTAINER_IMAGE_BASE_NAME,
            image_repository,
            crate::built_info::PKG_VERSION,
        )
        .context(ResolveProductImageSnafu)?;

    let mut role_groups = BTreeMap::new();
    let mut role_configs = BTreeMap::new();

    // if the kubernetes executor is specified there will be no worker role as the pods
    // are provisioned by airflow as defined by the task (default: one pod per task)
    for role in AirflowRole::iter() {
        let Some(resolved_role) = airflow.get_role(&role) else {
            continue;
        };

        role_configs.insert(
            role.clone(),
            ValidatedRoleConfig {
                pdb: airflow
                    .role_config(&role)
                    .map(|rc| rc.pod_disruption_budget),
                listener_class: role.listener_class_name(airflow).map(|s| s.to_string()),
                group_listener_name: airflow.group_listener_name(&role),
            },
        );

        let mut group_configs = BTreeMap::new();
        for rolegroup_name in resolved_role.role_groups.keys() {
            let rolegroup_ref = RoleGroupRef {
                cluster: stackable_operator::kube::runtime::reflector::ObjectRef::from_obj(airflow),
                role: role.to_string(),
                role_group: rolegroup_name.into(),
            };

            let merged_config = airflow
                .merged_config(&role, &rolegroup_ref)
                .context(FailedToResolveConfigSnafu)?;

            let (config_overrides, env_overrides) =
                merge_role_group_overrides(&resolved_role, rolegroup_name);

            group_configs.insert(
                rolegroup_name.clone(),
                ValidatedRoleGroupConfig {
                    merged_config,
                    config_overrides,
                    env_overrides,
                },
            );
        }

        role_groups.insert(role, group_configs);
    }

    let DereferencedObjects {
        authentication_config,
        authorization_config,
    } = dereferenced;

    Ok(ValidatedAirflowCluster {
        image: resolved_product_image,
        role_groups,
        role_configs,
        executor: airflow.spec.executor.clone(),
        authentication_config,
        authorization_config,
    })
}

/// Merge a role group's config overrides over the role-level ones (role-group wins per key) via
/// the `Merge` impl on [`AirflowConfigOverrides`], and combine env overrides (role first, then
/// role-group on top). Mirrors hdfs-operator's `validate_role_group_config`.
///
/// The merged overrides are returned *typed*; flattening into the rendered `webserver_config.py`
/// happens later, in the build step. Note the `Merge` semantics: a role-group `null` inherits the
/// role-level value rather than unsetting it.
fn merge_role_group_overrides(
    role: &AirflowRoleType,
    rolegroup_name: &str,
) -> (AirflowConfigOverrides, HashMap<String, String>) {
    let rolegroup = role.role_groups.get(rolegroup_name);

    let mut config_overrides = rolegroup
        .map(|rg| rg.config.config_overrides.clone())
        .unwrap_or_default();
    config_overrides.merge(&role.config.config_overrides);

    let mut env_overrides = role.config.env_overrides.clone();
    if let Some(rg) = rolegroup {
        env_overrides.extend(rg.config.env_overrides.clone());
    }

    (config_overrides, env_overrides)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::merge_role_group_overrides;
    use crate::crd::{AirflowRole, v1alpha2};

    fn test_cluster() -> v1alpha2::AirflowCluster {
        let cluster_yaml = r#"
        apiVersion: airflow.stackable.tech/v1alpha2
        kind: AirflowCluster
        metadata:
          name: airflow
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
            configOverrides:
              webserver_config.py:
                AUTH_TYPE: "AUTH_OID"
                ROLE_ONLY_KEY: "role-value"
            envOverrides:
              ROLE_ENV_VAR: "role-env-value"
            roleGroups:
              default:
                config: {}
                configOverrides:
                  webserver_config.py:
                    AUTH_TYPE: "AUTH_DB"
                    GROUP_ONLY_KEY: "group-value"
                envOverrides:
                  GROUP_ENV_VAR: "group-env-value"
          schedulers:
            config: {}
            roleGroups:
              default:
                config: {}
          kubernetesExecutors:
            config: {}
        "#;
        let deserializer = serde_yaml::Deserializer::from_str(cluster_yaml);
        serde_yaml::with::singleton_map_recursive::deserialize(deserializer).unwrap()
    }

    #[test]
    fn role_group_overrides_merge_over_role_overrides() {
        let cluster = test_cluster();
        let role = cluster
            .get_role(&AirflowRole::Webserver)
            .expect("webserver role");

        let (config_overrides, env_overrides) = merge_role_group_overrides(&role, "default");

        // configOverrides are kept typed (values are `Option<String>`). The role-group AUTH_TYPE
        // overrides the role-level one; both role-only and group-only keys are kept.
        assert_eq!(
            config_overrides.webserver_config_py.overrides,
            BTreeMap::from([
                ("AUTH_TYPE".to_string(), Some("AUTH_DB".to_string())),
                ("ROLE_ONLY_KEY".to_string(), Some("role-value".to_string())),
                (
                    "GROUP_ONLY_KEY".to_string(),
                    Some("group-value".to_string())
                ),
            ])
        );

        assert_eq!(env_overrides.len(), 2);
        assert_eq!(env_overrides.get("ROLE_ENV_VAR").unwrap(), "role-env-value");
        assert_eq!(
            env_overrides.get("GROUP_ENV_VAR").unwrap(),
            "group-env-value"
        );
    }

    /// A role-group `null` override inherits the role-level value instead of unsetting it — the
    /// behavioural consequence of merging with `Merge` rather than `.extend()`. `main`'s
    /// product-config used `.extend()`, where the same input would have *removed* the key. This
    /// test pins that choice (mirrors the kafka-operator test).
    #[test]
    fn role_group_null_inherits_role_value_rather_than_unsetting_it() {
        let cluster_yaml = r#"
        apiVersion: airflow.stackable.tech/v1alpha2
        kind: AirflowCluster
        metadata:
          name: airflow
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
            configOverrides:
              webserver_config.py:
                AUTH_TYPE: "AUTH_OID"
            roleGroups:
              default:
                config: {}
                configOverrides:
                  webserver_config.py:
                    AUTH_TYPE: null
          schedulers:
            config: {}
            roleGroups:
              default:
                config: {}
          kubernetesExecutors:
            config: {}
        "#;
        let deserializer = serde_yaml::Deserializer::from_str(cluster_yaml);
        let cluster: v1alpha2::AirflowCluster =
            serde_yaml::with::singleton_map_recursive::deserialize(deserializer).unwrap();
        let role = cluster
            .get_role(&AirflowRole::Webserver)
            .expect("webserver role");

        // For contrast: under the old `.extend()` layering the role-group `null` overwrites the
        // role value, and the key is dropped on flatten — i.e. AUTH_TYPE is unset entirely.
        let old_extend_behaviour: BTreeMap<String, String> = {
            let mut combined = role
                .config
                .config_overrides
                .webserver_config_py
                .overrides
                .clone();
            let rg = role.role_groups.get("default").expect("default role group");
            combined.extend(
                rg.config
                    .config_overrides
                    .webserver_config_py
                    .overrides
                    .clone(),
            );
            combined
                .into_iter()
                .filter_map(|(key, value)| value.map(|value| (key, value)))
                .collect()
        };
        assert!(
            !old_extend_behaviour.contains_key("AUTH_TYPE"),
            "under the old `.extend()` behaviour the role-group `null` unsets AUTH_TYPE"
        );

        // What we do now (Merge): the role-group `null` inherits the role-level value, so
        // AUTH_TYPE survives as the role's "AUTH_OID".
        let (config_overrides, _env_overrides) = merge_role_group_overrides(&role, "default");
        assert_eq!(
            config_overrides
                .webserver_config_py
                .overrides
                .get("AUTH_TYPE"),
            Some(&Some("AUTH_OID".to_string())),
            "role-group `null` should inherit the role-level AUTH_TYPE under Merge semantics"
        );
    }

    #[test]
    fn role_without_overrides_yields_empty() {
        let cluster = test_cluster();
        let role = cluster
            .get_role(&AirflowRole::Scheduler)
            .expect("scheduler role");

        let (config_overrides, env_overrides) = merge_role_group_overrides(&role, "default");

        assert!(config_overrides.webserver_config_py.overrides.is_empty());
        assert!(env_overrides.is_empty());
    }
}
