//! Builds the rolegroup [`ConfigMap`]: the rendered `webserver_config.py` plus the
//! logging/vector configuration.

use std::collections::BTreeMap;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::configmap::ConfigMapBuilder,
    k8s_openapi::api::core::v1::ConfigMap,
    product_logging::framework::VECTOR_CONFIG_FILE,
    v2::types::operator::{RoleGroupName, RoleName},
};

use crate::{
    controller::{
        ValidatedCluster, ValidatedLogging,
        build::properties::{
            product_logging::{LOG_CONFIG_FILE, create_airflow_config, vector_config_file_content},
            webserver_config,
        },
    },
    crd::{AIRFLOW_CONFIG_FILENAME, AirflowConfigOverrides, Container, STACKABLE_LOG_DIR},
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build webserver config for role group {role_group}"))]
    BuildWebserverConfig {
        source: webserver_config::Error,
        role_group: RoleGroupName,
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
    logging: &ValidatedLogging,
    container: &Container,
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
            validated_cluster
                .object_meta(
                    validated_cluster
                        .resource_names(role_name, role_group_name)
                        .role_group_config_map()
                        .to_string(),
                    validated_cluster.recommended_labels_for(role_name, role_group_name),
                )
                .build(),
        )
        .add_data(AIRFLOW_CONFIG_FILENAME, config_file);

    // Log config for the main container, when it uses an Automatic log config.
    let log_dir = format!("{STACKABLE_LOG_DIR}/{container}");
    if let Some(log_config) = create_airflow_config(
        &logging.product_container,
        &log_dir,
        &validated_cluster.image,
    ) {
        cm_builder.add_data(LOG_CONFIG_FILE, log_config);
    }

    // Vector agent config
    if logging.enable_vector_agent {
        cm_builder.add_data(VECTOR_CONFIG_FILE, vector_config_file_content());
    }

    cm_builder.build().with_context(|_| BuildConfigMapSnafu {
        role_group: role_group_name.clone(),
    })
}
