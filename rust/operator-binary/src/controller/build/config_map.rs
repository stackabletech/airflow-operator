//! Builds the rolegroup [`ConfigMap`]: the rendered `webserver_config.py` plus the
//! logging/vector configuration.

use std::collections::BTreeMap;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, meta::ObjectMetaBuilder},
    k8s_openapi::api::core::v1::ConfigMap,
    product_logging::{
        self,
        spec::{ContainerLogConfig, ContainerLogConfigChoice, Logging},
    },
    role_utils::RoleGroupRef,
};

use crate::{
    airflow_controller::{AIRFLOW_CONTROLLER_NAME, ValidatedCluster},
    config::webserver_config,
    crd::{
        AIRFLOW_CONFIG_FILENAME, AirflowConfigOverrides, Container, STACKABLE_LOG_DIR,
        build_recommended_labels, v1alpha2,
    },
    product_logging::{LOG_CONFIG_FILE, create_airflow_config},
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build webserver config for {rolegroup}"))]
    BuildWebserverConfig {
        source: webserver_config::Error,
        rolegroup: RoleGroupRef<v1alpha2::AirflowCluster>,
    },

    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to build object meta"))]
    ObjectMeta {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to add the logging configuration to the ConfigMap [{cm_name}]"))]
    InvalidLoggingConfig {
        source: crate::product_logging::Error,
        cm_name: String,
    },

    #[snafu(display("failed to build ConfigMap for {rolegroup}"))]
    BuildConfigMap {
        source: stackable_operator::builder::configmap::Error,
        rolegroup: RoleGroupRef<v1alpha2::AirflowCluster>,
    },
}

/// The rolegroup [`ConfigMap`] configures the rolegroup based on the configuration given by the administrator
pub fn build_rolegroup_config_map(
    airflow: &v1alpha2::AirflowCluster,
    validated_cluster: &ValidatedCluster,
    rolegroup: &RoleGroupRef<v1alpha2::AirflowCluster>,
    config_overrides: &AirflowConfigOverrides,
    logging: &Logging<Container>,
    container: &Container,
) -> Result<ConfigMap, Error> {
    // Flatten the typed `webserver_config.py` overrides into a plain map for the file writer.
    let config_file_overrides: BTreeMap<String, String> =
        config_overrides.webserver_config_py.overrides.clone();

    let config_file = webserver_config::build(validated_cluster, &config_file_overrides)
        .with_context(|_| BuildWebserverConfigSnafu {
            rolegroup: rolegroup.clone(),
        })?;

    let mut cm_builder = ConfigMapBuilder::new();

    cm_builder
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(airflow)
                .name(rolegroup.object_name())
                .ownerreference_from_resource(airflow, None, Some(true))
                .context(ObjectMissingMetadataForOwnerRefSnafu)?
                .with_recommended_labels(&build_recommended_labels(
                    airflow,
                    AIRFLOW_CONTROLLER_NAME,
                    &validated_cluster.image.app_version_label_value,
                    &rolegroup.role,
                    &rolegroup.role_group,
                ))
                .context(ObjectMetaSnafu)?
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

    // Vector agent config, when enabled.
    if logging.enable_vector_agent {
        let vector_log_config = if let Some(ContainerLogConfig {
            choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
        }) = logging.containers.get(&Container::Vector)
        {
            Some(log_config)
        } else {
            None
        };
        cm_builder.add_data(
            product_logging::framework::VECTOR_CONFIG_FILE,
            product_logging::framework::create_vector_config(rolegroup, vector_log_config),
        );
    }

    cm_builder.build().with_context(|_| BuildConfigMapSnafu {
        rolegroup: rolegroup.clone(),
    })
}
