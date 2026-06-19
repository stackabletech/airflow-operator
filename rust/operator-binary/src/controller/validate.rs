use std::{collections::BTreeMap, str::FromStr};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    commons::product_image_selection,
    config::fragment,
    database_connections::{
        TemplatingMechanism,
        drivers::{
            celery::CeleryDatabaseConnectionDetails,
            sqlalchemy::SqlAlchemyDatabaseConnectionDetails,
        },
    },
    kube::ResourceExt,
    product_logging::spec::Logging,
    role_utils::{GenericRoleConfig, RoleGroup},
    v2::{
        builder::pod::container::{EnvVarName, EnvVarSet},
        controller_utils::{get_namespace, get_uid},
        product_logging::framework::{
            VectorContainerLogConfig, validate_logging_configuration_for_container,
        },
        role_utils::{GenericCommonConfig, RoleGroupConfig, with_validated_config},
        types::{
            kubernetes::ConfigMapName,
            operator::{ClusterName, RoleGroupName},
        },
    },
};
use strum::IntoEnumIterator;

use super::{
    AirflowRoleGroup, AirflowRoleGroupConfig, ValidatedCluster, ValidatedClusterConfig,
    ValidatedLogging, ValidatedRoleConfig, dereference::DereferencedObjects,
};
use crate::{
    airflow_controller::CONTAINER_IMAGE_BASE_NAME,
    crd::{
        AirflowConfig, AirflowConfigFragment, AirflowConfigOverrides, AirflowExecutor, AirflowRole,
        AirflowRoleType, Container, v1alpha2,
    },
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to resolve product image"))]
    ResolveProductImage {
        source: product_image_selection::Error,
    },

    #[snafu(display("invalid cluster name {cluster_name}"))]
    ParseClusterName {
        source: stackable_operator::v2::macros::attributed_string_type::Error,
        cluster_name: String,
    },

    #[snafu(display("failed to resolve namespace"))]
    ResolveNamespace {
        source: stackable_operator::v2::controller_utils::Error,
    },

    #[snafu(display("failed to resolve uid"))]
    ResolveUid {
        source: stackable_operator::v2::controller_utils::Error,
    },

    #[snafu(display("invalid role group name {role_group}"))]
    ParseRoleGroupName {
        source: stackable_operator::v2::macros::attributed_string_type::Error,
        role_group: String,
    },

    #[snafu(display("failed to resolve and merge config for role group {role_group}"))]
    FailedToResolveConfig {
        source: fragment::ValidationError,
        role_group: RoleGroupName,
    },

    #[snafu(display("failed to parse an environment variable override name"))]
    ParseEnvVarName {
        source: stackable_operator::v2::macros::attributed_string_type::Error,
    },

    #[snafu(display("failed to validate the logging configuration"))]
    ValidateLoggingConfig {
        source: stackable_operator::v2::product_logging::framework::Error,
    },

    #[snafu(display(
        "the Vector aggregator discovery ConfigMap name must be set when the Vector agent is enabled"
    ))]
    MissingVectorAggregatorConfigMapName,
}

pub fn validate_cluster(
    airflow: &v1alpha2::AirflowCluster,
    image_repository: &str,
    dereferenced: DereferencedObjects,
) -> Result<ValidatedCluster, Error> {
    let resolved_product_image = airflow
        .spec
        .image
        .resolve(
            CONTAINER_IMAGE_BASE_NAME,
            image_repository,
            crate::built_info::PKG_VERSION,
        )
        .context(ResolveProductImageSnafu)?;

    let cluster_name =
        ClusterName::from_str(&airflow.name_any()).with_context(|_| ParseClusterNameSnafu {
            cluster_name: airflow.name_any(),
        })?;
    let namespace = get_namespace(airflow).context(ResolveNamespaceSnafu)?;
    let uid = get_uid(airflow).context(ResolveUidSnafu)?;

    // The Vector aggregator discovery ConfigMap name. Validity is already enforced by the
    // `ConfigMapName` type on the CRD; it is only required when the Vector agent is enabled.
    let vector_aggregator_config_map_name = airflow
        .spec
        .cluster_config
        .vector_aggregator_config_map_name
        .clone();

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
                listener_class: role.listener_class_name(airflow),
                group_listener_name: airflow.group_listener_name(&role),
            },
        );

        let default_config = AirflowConfig::default_config(&airflow.name_any(), &role);

        let mut group_configs = BTreeMap::new();
        for (rolegroup_name, rolegroup) in &resolved_role.role_groups {
            let role_group_name = RoleGroupName::from_str(rolegroup_name).with_context(|_| {
                ParseRoleGroupNameSnafu {
                    role_group: rolegroup_name.clone(),
                }
            })?;
            let config =
                validate_role_group(&resolved_role, &role_group_name, rolegroup, &default_config)?;
            let logging = validate_logging(
                &config.config.logging,
                &Container::Airflow,
                &vector_aggregator_config_map_name,
            )?;

            group_configs.insert(role_group_name, AirflowRoleGroup { config, logging });
        }

        role_groups.insert(role, group_configs);
    }

    let DereferencedObjects {
        authentication_config,
        authorization_config,
    } = dereferenced;

    let (metadata_database_connection_details, celery_database_connection_details) =
        database_connection_details(airflow);

    Ok(ValidatedCluster::new(
        cluster_name,
        namespace,
        uid,
        resolved_product_image,
        ValidatedClusterConfig {
            executor: airflow.spec.executor.clone(),
            authentication_config,
            authorization_config,
            dags_git_sync: airflow.spec.cluster_config.dags_git_sync.clone(),
            credentials_secret_name: airflow.spec.cluster_config.credentials_secret_name.clone(),
            metadata_database_connection_details,
            celery_database_connection_details,
            load_examples: airflow.spec.cluster_config.load_examples,
            expose_config: airflow.spec.cluster_config.expose_config,
            database_initialization_enabled: airflow
                .spec
                .cluster_config
                .database_initialization
                .enabled,
            volumes: airflow.spec.cluster_config.volumes.clone(),
            volume_mounts: airflow.spec.cluster_config.volume_mounts.clone(),
            vector_aggregator_config_map_name: vector_aggregator_config_map_name.clone(),
        },
        role_groups,
        role_configs,
    ))
}

/// Builds the templated metadata-database (and optional Celery result-backend/broker) connection
/// details from the cluster spec.
fn database_connection_details(
    airflow: &v1alpha2::AirflowCluster,
) -> (
    SqlAlchemyDatabaseConnectionDetails,
    Option<(
        CeleryDatabaseConnectionDetails,
        CeleryDatabaseConnectionDetails,
    )>,
) {
    let templating_mechanism = TemplatingMechanism::BashEnvSubstitution;

    let metadata_database_connection_details = airflow
        .spec
        .cluster_config
        .metadata_database
        .sqlalchemy_connection_details_with_templating("METADATA", &templating_mechanism);

    let celery_database_connection_details = if let (
        Some(celery_results_backend),
        Some(celery_broker),
    ) = (
        &airflow.spec.cluster_config.celery_results_backend,
        &airflow.spec.cluster_config.celery_broker,
    ) {
        // The celery results backend and celery broker only work with configured celeryExecutors.
        // Emit a warning if celery executors were not configured properly.
        if !matches!(
            &airflow.spec.executor,
            AirflowExecutor::CeleryExecutors { .. }
        ) {
            tracing::warn!(
                "No `spec.celeryExecutors` configured, but `spec.clusterConfig.celeryResultsBackend` and `spec.clusterConfig.celeryBroker` are provided. This only works in combination with a celery executor!"
            )
        }

        let celery_results_backend = celery_results_backend
            .celery_connection_details_with_templating(
                "CELERY_RESULT_BACKEND",
                &templating_mechanism,
            );
        let celery_broker = celery_broker
            .celery_connection_details_with_templating("CELERY_BROKER", &templating_mechanism);
        Some((celery_results_backend, celery_broker))
    } else {
        None
    };

    (
        metadata_database_connection_details,
        celery_database_connection_details,
    )
}

/// Validate and merge one role group against its role, via the shared
/// [`with_validated_config`] from `operator-rs`, returning the generic
/// [`stackable_operator::v2::role_utils::RoleGroupConfig`].
///
/// This performs the full `default → role → role-group` merge of the config fragment (then
/// validates it) *and* the role←role-group merge of the overrides in one step. The config
/// overrides are kept *typed* ([`AirflowConfigOverrides`]); flattening into the rendered
/// `webserver_config.py` happens later, in the build step.
///
/// Note the override `Merge` semantics: a role-group `null` inherits the role-level value rather
/// than unsetting it (config overrides), and env overrides layer role-group on top of role.
fn validate_role_group(
    role: &AirflowRoleType,
    role_group_name: &RoleGroupName,
    rolegroup: &RoleGroup<AirflowConfigFragment, GenericCommonConfig, AirflowConfigOverrides>,
    default_config: &AirflowConfigFragment,
) -> Result<AirflowRoleGroupConfig, Error> {
    let validated = with_validated_config::<
        AirflowConfig,
        GenericCommonConfig,
        AirflowConfigFragment,
        GenericRoleConfig,
        AirflowConfigOverrides,
    >(rolegroup, role, default_config)
    .with_context(|_| FailedToResolveConfigSnafu {
        role_group: role_group_name.clone(),
    })?;

    let mut env_overrides = EnvVarSet::new();
    for (env_var_name, env_var_value) in validated.config.env_overrides {
        env_overrides = env_overrides.with_value(
            &EnvVarName::from_str(&env_var_name).context(ParseEnvVarNameSnafu)?,
            env_var_value,
        );
    }

    Ok(RoleGroupConfig {
        replicas: validated.replicas,
        config: validated.config.config,
        config_overrides: validated.config.config_overrides,
        env_overrides,
        cli_overrides: validated.config.cli_overrides,
        pod_overrides: validated.config.pod_overrides,
        product_specific_common_config: validated.config.product_specific_common_config,
    })
}

/// Validates the logging configuration for the product container and the (optional) Vector
/// container.
///
/// `product_container` selects the product's main container (`Container::Airflow` for the role
/// groups, `Container::Base` for the Kubernetes-executor pod template).
///
/// `vector_aggregator_config_map_name` is the discovery ConfigMap name of the Vector aggregator;
/// it is required (and validated) only when the Vector agent is enabled. Mirrors hive's
/// `validate_logging`. Used both per-role-group (here) and for the Kubernetes executor pod template
/// (which is not a [`AirflowRole`] with role groups, so it is validated from the build step).
pub(crate) fn validate_logging(
    logging: &Logging<Container>,
    product_container: &Container,
    vector_aggregator_config_map_name: &Option<ConfigMapName>,
) -> Result<ValidatedLogging, Error> {
    let product_container =
        validate_logging_configuration_for_container(logging, product_container)
            .context(ValidateLoggingConfigSnafu)?;

    let vector_container = if logging.enable_vector_agent {
        let vector_aggregator_config_map_name = vector_aggregator_config_map_name
            .clone()
            .context(MissingVectorAggregatorConfigMapNameSnafu)?;
        Some(VectorContainerLogConfig {
            log_config: validate_logging_configuration_for_container(logging, &Container::Vector)
                .context(ValidateLoggingConfigSnafu)?,
            vector_aggregator_config_map_name,
        })
    } else {
        None
    };

    Ok(ValidatedLogging {
        product_container,
        vector_container,
        enable_vector_agent: logging.enable_vector_agent,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use stackable_operator::k8s_openapi::api::core::v1::EnvVar;

    use super::validate_role_group;
    use crate::crd::{AirflowConfig, AirflowRole, v1alpha2};

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
        let default_config = AirflowConfig::default_config("airflow", &AirflowRole::Webserver);
        let rolegroup = role.role_groups.get("default").expect("default role group");

        let validated = validate_role_group(
            &role,
            &"default".parse().expect("valid role group name"),
            rolegroup,
            &default_config,
        )
        .expect("validated role group");
        let config_overrides = validated.config_overrides;

        // configOverrides are kept typed. The role-group AUTH_TYPE overrides the role-level one;
        // both role-only and group-only keys are kept.
        assert_eq!(
            config_overrides.webserver_config_py.overrides,
            BTreeMap::from([
                ("AUTH_TYPE".to_string(), "AUTH_DB".to_string()),
                ("ROLE_ONLY_KEY".to_string(), "role-value".to_string()),
                ("GROUP_ONLY_KEY".to_string(), "group-value".to_string()),
            ])
        );

        // env overrides layer role-group on top of role.
        let env_overrides: BTreeMap<String, Option<String>> =
            Vec::<EnvVar>::from(validated.env_overrides)
                .into_iter()
                .map(|env_var| (env_var.name, env_var.value))
                .collect();
        assert_eq!(env_overrides.len(), 2);
        assert_eq!(
            env_overrides.get("ROLE_ENV_VAR").unwrap().as_deref(),
            Some("role-env-value")
        );
        assert_eq!(
            env_overrides.get("GROUP_ENV_VAR").unwrap().as_deref(),
            Some("group-env-value")
        );
    }

    /// A `null` override value is rejected by the CRD. `configOverrides` values are typed as
    /// `String` (operator-rs `KeyValueConfigOverrides`, since the `Option<String>` was removed in
    /// operator-rs #1219), so there is no longer a way to express "unset this key" via `null` —
    /// the previous `null`-means-inherit/unset semantics no longer exist at the type level.
    #[test]
    fn role_group_null_override_value_is_rejected() {
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
        let result: Result<v1alpha2::AirflowCluster, _> =
            serde_yaml::with::singleton_map_recursive::deserialize(deserializer);
        assert!(
            result.is_err(),
            "a `null` configOverrides value should be rejected: values are typed as `String`"
        );
    }

    #[test]
    fn role_without_overrides_yields_empty() {
        let cluster = test_cluster();
        let role = cluster
            .get_role(&AirflowRole::Scheduler)
            .expect("scheduler role");
        let default_config = AirflowConfig::default_config("airflow", &AirflowRole::Scheduler);
        let rolegroup = role.role_groups.get("default").expect("default role group");

        let validated = validate_role_group(
            &role,
            &"default".parse().expect("valid role group name"),
            rolegroup,
            &default_config,
        )
        .expect("validated role group");

        assert!(
            validated
                .config_overrides
                .webserver_config_py
                .overrides
                .is_empty()
        );
        assert!(Vec::<EnvVar>::from(validated.env_overrides).is_empty());
    }

    /// `replicas` and the role←role-group merged `pod_overrides` are produced by
    /// `with_validated_config` and must be carried on `AirflowRoleGroupConfig`, so the build
    /// step reads them from here rather than re-deriving from the raw cluster.
    #[test]
    fn role_group_carries_merged_pod_overrides_and_replicas() {
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
            podOverrides:
              metadata:
                labels:
                  role-label: role
                  shared: role
            roleGroups:
              default:
                replicas: 3
                config: {}
                podOverrides:
                  metadata:
                    labels:
                      rg-label: rg
                      shared: rg
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
        let default_config = AirflowConfig::default_config("airflow", &AirflowRole::Webserver);
        let rolegroup = role.role_groups.get("default").expect("default role group");

        let validated = validate_role_group(
            &role,
            &"default".parse().expect("valid role group name"),
            rolegroup,
            &default_config,
        )
        .expect("validated role group");

        // replicas is carried through from the role group.
        assert_eq!(validated.replicas, Some(3));

        // pod_overrides is merged role←role-group (role-group wins on shared keys, both levels'
        // unique keys survive).
        let labels = validated
            .pod_overrides
            .metadata
            .expect("pod override metadata")
            .labels
            .expect("pod override labels");
        assert_eq!(labels.get("role-label"), Some(&"role".to_string()));
        assert_eq!(labels.get("rg-label"), Some(&"rg".to_string()));
        assert_eq!(labels.get("shared"), Some(&"rg".to_string()));
    }
}
