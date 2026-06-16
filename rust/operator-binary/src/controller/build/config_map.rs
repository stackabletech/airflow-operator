//! Builds the rolegroup [`ConfigMap`]: the rendered `webserver_config.py` plus the
//! logging/vector configuration.

use std::collections::BTreeMap;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, meta::ObjectMetaBuilder},
    k8s_openapi::api::core::v1::ConfigMap,
    kube::runtime::reflector::ObjectRef,
    product_logging::{
        self,
        spec::{ContainerLogConfig, ContainerLogConfigChoice, Logging},
    },
    role_utils::RoleGroupRef,
    v2::{
        builder::meta::ownerreference_from_resource,
        types::operator::{RoleGroupName, RoleName},
    },
};

use crate::{
    config::webserver_config,
    controller::ValidatedCluster,
    crd::{AIRFLOW_CONFIG_FILENAME, AirflowConfigOverrides, Container, STACKABLE_LOG_DIR},
    product_logging::{LOG_CONFIG_FILE, create_airflow_config},
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build webserver config for role group {role_group}"))]
    BuildWebserverConfig {
        source: webserver_config::Error,
        role_group: RoleGroupName,
    },

    #[snafu(display("failed to add the logging configuration to the ConfigMap [{cm_name}]"))]
    InvalidLoggingConfig {
        source: crate::product_logging::Error,
        cm_name: String,
    },

    #[snafu(display("failed to build ConfigMap for role group {role_group}"))]
    BuildConfigMap {
        source: stackable_operator::builder::configmap::Error,
        role_group: RoleGroupName,
    },
}

/// The rolegroup [`ConfigMap`] configures the rolegroup based on the configuration given by the administrator
pub fn build_rolegroup_config_map(
    validated_cluster: &ValidatedCluster,
    role_name: &RoleName,
    role_group_name: &RoleGroupName,
    config_overrides: &AirflowConfigOverrides,
    logging: &Logging<Container>,
    container: &Container,
    // The Vector agent config (`vector.yaml`), built by the caller (where a `RoleGroupRef` is
    // available via [`build_vector_config`]); `None` when the agent is disabled.
    vector_config: Option<String>,
) -> Result<ConfigMap, Error> {
    // Flatten the typed `webserver_config.py` overrides into a plain map for the file writer.
    let config_file_overrides: BTreeMap<String, String> =
        config_overrides.webserver_config_py.overrides.clone();

    let config_file = webserver_config::build(validated_cluster, &config_file_overrides)
        .with_context(|_| BuildWebserverConfigSnafu {
            role_group: role_group_name.clone(),
        })?;

    let mut cm_builder = ConfigMapBuilder::new();

    cm_builder
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(validated_cluster)
                .name(
                    validated_cluster
                        .resource_names(role_name, role_group_name)
                        .role_group_config_map()
                        .to_string(),
                )
                .ownerreference(ownerreference_from_resource(
                    validated_cluster,
                    None,
                    Some(true),
                ))
                .with_labels(validated_cluster.recommended_labels_for(role_name, role_group_name))
                .build(),
        )
        .add_data(AIRFLOW_CONFIG_FILENAME, config_file);

    // Log config for the main container, when it uses an Automatic log config.
    if let Some(ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    }) = logging.containers.get(container)
    {
        let log_dir = format!("{STACKABLE_LOG_DIR}/{container}");
        cm_builder.add_data(
            LOG_CONFIG_FILE,
            create_airflow_config(log_config, &log_dir, &validated_cluster.image),
        );
    }

    // Vector agent config, built by the caller (where a `RoleGroupRef` is available).
    if let Some(vector_config) = vector_config {
        cm_builder.add_data(
            product_logging::framework::VECTOR_CONFIG_FILE,
            vector_config,
        );
    }

    cm_builder.build().with_context(|_| BuildConfigMapSnafu {
        role_group: role_group_name.clone(),
    })
}

/// Builds the Vector agent config (`vector.yaml`) for a role group, when the agent is enabled.
///
/// This is the one remaining place that needs a [`RoleGroupRef`]: the upstream v1
/// `create_vector_config` API still takes one (only as a type witness — it does not read the role
/// group). It is constructed locally here so no `RoleGroupRef` leaks into the rest of the operator.
/// This goes away entirely once logging is migrated to the v2/superset mechanism.
pub fn build_vector_config(
    validated_cluster: &ValidatedCluster,
    role_name: &RoleName,
    role_group_name: &RoleGroupName,
    logging: &Logging<Container>,
) -> Option<String> {
    if logging.enable_vector_agent {
        let vector_log_config = if let Some(ContainerLogConfig {
            choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
        }) = logging.containers.get(&Container::Vector)
        {
            Some(log_config)
        } else {
            None
        };
        let rolegroup_ref = RoleGroupRef {
            cluster: ObjectRef::from_obj(validated_cluster),
            role: role_name.to_string(),
            role_group: role_group_name.to_string(),
        };
        Some(product_logging::framework::create_vector_config(
            &rolegroup_ref,
            vector_log_config,
        ))
    } else {
        None
    }
}
